from pathlib import Path
import tempfile
import unittest

from compat_lib import checks, corpus, report
import harness
from tests.task_storage import ModuleStorage

_STORAGE = ModuleStorage()

def setUpModule():
    _STORAGE.start()

def tearDownModule():
    _STORAGE.finish()

class ShapeOwnerTests(unittest.TestCase):
    def parse(self, headers='', sql='SELECT 1;'):
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as folder:
            path = Path(folder) / 'case.sql'
            path.write_text('-- case: example\n-- title: Example\n-- owner: unowned\n'+headers+sql, encoding='utf-8')
            return corpus.parse_case(path)

    def record(self, output, count=1, state='00000'):
        return dict(sqlstate=state, output_lines=output, row_count=count)

    def test_shape(self):
        p = self.record(['"a,b","line', 'name"', '1,2'])
        g = self.record(['"a,b","line', 'name"', '9,8'])
        self.assertIsNone(corpus.statement_difference(p, g, 'shape'))
        self.assertEqual(corpus.statement_difference(p, g), 'result_differs')
        self.assertEqual(corpus.statement_difference(p, dict(g, row_count=2), 'shape'), 'result_differs')
        self.assertEqual(corpus.statement_difference(p, self.record(['a,b', '9,8']), 'shape'), 'result_differs')
        self.assertEqual(corpus.statement_difference(p, dict(g, sqlstate='42601'), 'shape'), 'giggles_error')
        self.assertIsNone(corpus.statement_difference(dict(p, sqlstate='42601'), dict(g, sqlstate='42601'), 'shape'))
        self.assertIsNone(corpus.statement_difference(self.record([], 0), self.record([], 0), 'shape'))
        self.assertEqual(corpus.statement_difference(self.record(['UPDATE 1']), self.record(['UPDATE 2']), 'shape'), 'result_differs')

    def test_metadata_and_comments(self):
        self.assertTrue({'DD-0068', 'unowned'} <= checks.OWNERS)
        self.assertEqual(harness.parse_owners('DD-0068,unowned'), {'DD-0068', 'unowned'})
        defaults = harness.parser().parse_args(['run'])
        self.assertTrue({'DD-0068', 'unowned'} <= harness.parse_owners(defaults.owners))
        self.assertEqual(self.parse()['compare'], 'full')
        sql = '/* semicolon ;\n nested /* ; */ comment */\nSELECT 1 /* ; */; -- trailing ;\n'
        case = self.parse('-- compare: shape\n-- source: DataGrip capture\n', sql)
        self.assertEqual(case['compare'], 'shape')
        self.assertEqual(case['source'], 'DataGrip capture')
        self.assertEqual(case['statements'][0]['sql'], sql.rstrip())
        for mode in ('', 'stdout', 'SHAPE', 'other'):
            with self.assertRaises(ValueError):
                self.parse('-- compare: '+mode+'\n')

    def test_report_unowned_and_compatible_shape(self):
        cases = [dict(id='shape_case', owner='unowned', outcome='COMPATIBLE', compare='shape'),
                 dict(id='failure', owner='unowned', outcome='DIVERGENT', compare='full')]
        text = '\n'.join(report.differential_markdown(dict(cases=cases)))
        self.assertEqual(text.count('### Unowned (needs an owner)'), 1)
        self.assertNotIn('### unowned', text)
        self.assertIn('shape_case | unowned | compare: shape | COMPATIBLE', text)
        data = dict(run_id='x', started_at='', finished_at='', summary=dict(clients={}),
                    checks=[dict(client='psql', id='x', owner='unowned', outcome='FAIL')],
                    failures_by_owner={'unowned':['psql/x']})
        self.assertIn('### Unowned (needs an owner)', report.markdown(data))

    def test_manifest_and_corpus_owners(self):
        import json
        root = Path(__file__).resolve().parents[1]
        for path in (root/'probes').glob('*/cases.json'):
            cases = {case['id']:case for case in json.loads(path.read_text())}
            for key, owner in {'update_delete_params':'E3', 'param_no_from_aggregate':'DD-0068', 'savepoint':'unowned'}.items():
                self.assertEqual(cases[key]['owner'], owner, str(path))
            copy_case = {'psycopg':'copy_from_stdin', 'pgx':'copy_from', 'npgsql':'binary_import'}.get(path.parent.name)
            if copy_case:
                self.assertEqual(cases[copy_case]['owner'], 'unowned', str(path))
            if path.parent.name == 'pgx':
                self.assertEqual(cases['exec_mode_simple']['owner'], 'D')
                self.assertTrue(cases['exec_mode_simple']['title'].endswith('(GD-0055)'))
        for name, owner in {'dml_predicates/update_set_expression.sql':'E3', 'known_gaps/no_from_aggregate.sql':'DD-0068', 'transactions/savepoint_release.sql':'unowned', 'transactions/savepoint_rollback_to.sql':'unowned'}.items():
            self.assertEqual(corpus.parse_case(root/'corpus'/name)['owner'], owner)
