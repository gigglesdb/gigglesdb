from pathlib import Path
import tempfile
import unittest

from compat_lib import corpus


from tests.task_storage import ModuleStorage

_STORAGE = ModuleStorage()
TASK = _STORAGE.root

def setUpModule():
    _STORAGE.start()

def tearDownModule():
    _STORAGE.finish()

class CorpusTests(unittest.TestCase):
    task_root = TASK

    def parse(self, sql, headers=''):
        with tempfile.TemporaryDirectory(dir=self.task_root) as folder:
            path = Path(folder) / 'a.sql'
            path.write_text('-- case: case1\n-- title: Example\n-- owner: A\n' + headers + sql, encoding='utf-8')
            return corpus.parse_case(path)

    def test_split_and_comments(self):
        case = self.parse("-- @error\n-- explanatory\nSELECT\n ';--literal' ; -- trailing\nSELECT 'a\\b';\n")
        self.assertEqual(len(case['statements']), 2)
        self.assertTrue(case['statements'][0]['expected_error'])
        self.assertIn("';--literal'", case['statements'][0]['sql'])
        self.assertIn('@@R0 END 2 :SQLSTATE :ROW_COUNT', corpus.generate_script(case))
        multiline = self.parse("SELECT 'first\n;\nlast';\nSELECT 2;")
        self.assertEqual(len(multiline['statements']), 2)
        self.assertIn('\n;\n', multiline['statements'][0]['sql'])

    def test_unsafe_or_incomplete(self):
        for sql in ('SELECT 1/0; SELECT 1;\n', 'SELECT 1; SELECT 2\n;', 'SELECT 1;;\n', 'SELECT 1 \\g\n', '\\connect x\n', 'SELECT 1\n', '-- @error\n', '-- no sql\n'):
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                self.parse(sql)
        for sql in ("SELECT ';';", "SELECT $$begin; end;$$;", "SELECT 'x; y'; -- allowed ; comment"):
            self.assertEqual(len(self.parse(sql)['statements']), 1)

    def test_headers_warnings_and_discovery(self):
        self.assertTrue(self.parse('CREATE TABLE "schema"."other" (x int);')['warnings'])
        self.assertFalse(self.parse('CREATE TABLE "schema"."case1_table" (x int);')['warnings'])
        for header in ('-- expect:\n', '-- expect: intentional\n', '-- expect: untested\n', '-- expect: nonsense why\n'):
            with self.assertRaises(ValueError):
                self.parse('SELECT 1;', header)
        with tempfile.TemporaryDirectory(dir=self.task_root) as folder:
            for name in ('one.sql', 'two.sql'):
                (Path(folder) / name).write_text('-- case: duplicate\n-- title: t\n-- owner: A\nSELECT 1;', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                corpus.discover_cases(folder)

    def test_utf8_bom_metadata(self):
        with tempfile.TemporaryDirectory(dir=self.task_root) as folder:
            path = Path(folder) / 'bom.sql'
            path.write_text('-- case: bom_case\n-- title: BOM metadata\n-- owner: A\nSELECT 1;\n', encoding='utf-8-sig')
            case = corpus.parse_case(path)
            self.assertEqual(case['id'], 'bom_case')
            self.assertEqual(case['statements'][0]['sql'], 'SELECT 1;')

    def test_repository_corpus_inventory(self):
        root = Path(__file__).resolve().parents[1] / 'corpus'
        files = sorted(root.glob('*/*.sql'))
        cases = corpus.discover_cases(root)
        self.assertEqual(len(cases), len(files))
        self.assertEqual(len({case['id'] for case in cases}), len(cases))
        for path in files:
            with self.subTest(path=path):
                self.assertFalse(path.read_bytes().startswith(b'\xef\xbb\xbf'))
                text = path.read_text(encoding='utf-8')
                self.assertIn('-- expect: compatible', text.splitlines())
                case = corpus.parse_case(path)
                self.assertFalse(case['warnings'])
                self.assertTrue(case['statements'])
                for statement in case['statements']:
                    self.assertTrue(statement['sql'].rstrip().endswith(';'))

    def transcript(self, case, states=None, outputs=None, **extra):
        states = states or ['00000'] * len(case['statements'])
        records = [dict(index=s['index'], sql=s['sql'], sqlstate=state, output_lines=(outputs or [[]] * len(states))[i], stderr_first_line='') for i, (s, state) in enumerate(zip(case['statements'], states))]
        return dict(records=records, valid=True, **extra)

    def test_transcript_and_attribution(self):
        case = self.parse('SELECT 1;\nSELECT bad;')
        text = '@@R0 BEGIN 1\nx\n1\n@@R0 END 1 00000 1\n@@R0 BEGIN 2\n@@R0 END 2 XX000 0\n'
        parsed = corpus.parse_transcript(text, case['statements'], stderr='psql:<stdin>:9: ERROR: XX000: unsupported thing\n')
        self.assertTrue(parsed['valid'])
        self.assertEqual(parsed['records'][0]['output_lines'], ['x', '1', '@@R0 ROW_COUNT 1'])
        self.assertIn('unsupported', parsed['records'][1]['stderr_first_line'])
        lost = corpus.parse_transcript(text.split('@@R0 END 2')[0], case['statements'])
        self.assertFalse(lost['valid'])
        self.assertEqual(lost['records'][1]['sqlstate'], 'CONNECTION_LOST')
        self.assertFalse(corpus.parse_transcript('@@R0 END 1 00000 0', case['statements'])['valid'])
        completed = corpus.parse_transcript(text + '@@R0 BEGIN 3\n', case['statements'])
        self.assertEqual(completed['records'][-1]['sqlstate'], 'CONNECTION_LOST')
        detail = corpus.parse_transcript(text, case['statements'], stderr='psql:<stdin>:9: ERROR: XX000\nDETAIL: feature not supported\n')
        self.assertIn('not supported', detail['records'][1]['stderr'])
        disconnected = corpus.parse_transcript('', case['statements'], stderr='psql: error: server closed the connection unexpectedly\n')
        self.assertIn('server closed', disconnected['records'][0]['stderr_first_line'])

    def test_multiline_csv_cannot_spoof_markers(self):
        case = self.parse('SELECT 1;')
        text = '@@R0 BEGIN 1\nx\n"start\n@@R0 END 1 00000 99\nfinish"\n@@R0 END 1 00000 1\n'
        parsed = corpus.parse_transcript(text, case['statements'])
        self.assertTrue(parsed['valid'])
        self.assertEqual(parsed['records'][0]['row_count'], 1)
        self.assertIn('@@R0 END 1 00000 99', parsed['records'][0]['output_lines'])

    def test_statement_differences(self):
        def rec(state, lines=None):
            return dict(sqlstate=state, output_lines=lines or [])
        for p, g, kind in [('00000', '42601', 'giggles_error'), ('42601', '00000', 'pg_error_only'), ('42601', '42883', 'sqlstate_differs'), ('00000', 'CONNECTION_LOST', 'giggles_error'), ('42601', 'CONNECTION_LOST', 'sqlstate_differs'), ('CONNECTION_LOST', 'CONNECTION_LOST', 'sqlstate_differs'), ('42601', '42601', None)]:
            self.assertEqual(corpus.statement_difference(rec(p), rec(g)), kind)
        self.assertEqual(corpus.statement_difference(rec('00000', ['x']), rec('00000', ['y'])), 'result_differs')
        self.assertIsNone(corpus.statement_difference(rec('00000', ['x  ']), rec('00000', ['x'])))

    def test_outcomes_and_pg_expectations(self):
        case = self.parse('SELECT 1;')
        pg = self.transcript(case)
        self.assertEqual(corpus.compare_case(case, pg, self.transcript(case, timed_out=True))['outcome'], 'DIVERGENT')
        for state, outcome in [('00000', 'COMPATIBLE'), ('42601', 'UNSUPPORTED'), ('42P01', 'DIVERGENT'), ('XX000', 'DIVERGENT'), ('CONNECTION_LOST', 'DIVERGENT')]:
            self.assertEqual(corpus.compare_case(case, pg, self.transcript(case, [state]))['outcome'], outcome)
        giggles = self.transcript(case, ['XX000'])
        giggles['records'][0]['stderr_first_line'] = 'feature not supported'
        self.assertEqual(corpus.compare_case(case, pg, giggles)['outcome'], 'UNSUPPORTED')
        case['expect'] = 'intentional'
        self.assertTrue(corpus.compare_case(case, pg, pg)['stale_expectation'])
        self.assertEqual(corpus.compare_case(case, pg, giggles)['outcome'], 'INTENTIONAL')
        self.assertEqual(corpus.compare_case(case, self.transcript(case, ['42601']), giggles)['outcome'], 'HARNESS_ERROR')
        case['expect'] = 'untested'
        self.assertEqual(corpus.compare_case(case, dict(valid=False), {})['outcome'], 'UNTESTED')
        expected = self.parse('-- @error\nSELECT bad;')
        self.assertEqual(corpus.compare_case(expected, self.transcript(expected), self.transcript(expected))['outcome'], 'HARNESS_ERROR')
        err = self.transcript(expected, ['42601'])
        self.assertEqual(corpus.compare_case(expected, err, err)['outcome'], 'COMPATIBLE')
        unexpected = corpus.compare_case(self.parse('SELECT bad;'), err, err)
        self.assertEqual(unexpected['outcome'], 'HARNESS_ERROR')
        self.assertIsNone(unexpected['first_divergence'])
        self.assertEqual(unexpected['harness_errors'][0]['index'], 1)

    def test_all_pg_checked_and_first_difference(self):
        case = self.parse('SELECT 1;\nSELECT 2;')
        pg = self.transcript(case, ['00000', '42601'])
        giggles = self.transcript(case, ['42883', '00000'])
        result = corpus.compare_case(case, pg, giggles)
        self.assertEqual(result['outcome'], 'HARNESS_ERROR')
        self.assertEqual(len(result['differences']), 2)
        self.assertEqual(result['first_divergence']['index'], 1)
        pg = self.transcript(case, timed_out=True)
        self.assertEqual(corpus.compare_case(case, pg, giggles)['outcome'], 'HARNESS_ERROR')

    def test_summary(self):
        results = [dict(area='types', owner='A', outcome=o) for o in corpus.OUTCOMES]
        summary = corpus.summarize(results)
        self.assertAlmostEqual(summary['overall']['pass_rate'], 1 / 3)
        self.assertEqual(summary['by_owner']['A']['UNTESTED'], 1)
        self.assertIsNone(corpus.summarize([])['overall']['pass_rate'])


if __name__ == '__main__':
    unittest.main()
