from pathlib import Path
import sys
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import harness
from compat_lib import areas


def report(*rows):
    cases = [dict(area=a, id=i, outcome=o, owner='E1') for a, i, o in rows]
    return dict(checks=[dict(client='psql', id='c', outcome='PASS')], differential=dict(cases=cases))


class FocusedSelectionTests(unittest.TestCase):
    case = dict(id='case_a', area='types', path='compat/corpus/types/filename.sql')

    def test_comma_separated_patterns(self):
        for pattern in ('case_a', 'other_case,case_a', 'other/*, types/case_a', 'x,,types', 'types/file*.sql,zzz'):
            self.assertTrue(harness.corpus_matches(self.case, pattern), pattern)
        for pattern in ('other_case,case_b', 'other/*', '', ','):
            self.assertFalse(harness.corpus_matches(self.case, pattern), pattern)

    def test_exclusions(self):
        for pattern in ('!other', '!pg_types,!other/*', 'types,!other_case'):
            self.assertTrue(harness.corpus_matches(self.case, pattern), pattern)
        for pattern in ('!types', '!pg_types,!types', 'types,!case_a', '*,!types/file*.sql', '!'):
            self.assertFalse(harness.corpus_matches(self.case, pattern), pattern)

    def test_halves_partition_the_corpus(self):
        from compat_lib import corpus
        cases = corpus.discover_cases(Path(__file__).resolve().parents[1] / 'corpus')
        half_a = 'pg_types,pg_functions,pg_queries,pg_silent'
        half_b = '!pg_types,!pg_functions,!pg_queries,!pg_silent'
        a = {c['id'] for c in cases if harness.corpus_matches(c, half_a)}
        b = {c['id'] for c in cases if harness.corpus_matches(c, half_b)}
        self.assertFalse(a & b)
        self.assertEqual(a | b, {c['id'] for c in cases})
        self.assertTrue(a and b)

    def test_parser_keeps_patterns_verbatim(self):
        args = harness.parser().parse_args(['run', '--no-checks', '--corpus', 'a_case,b/*'])
        self.assertEqual(args.corpus, 'a_case,b/*')


class AreaTableTests(unittest.TestCase):
    def test_counts_ignore_checks(self):
        result = areas.counts(report(('types', 'a', 'COMPATIBLE'), ('types', 'b', 'DIVERGENT'), ('ddl', 'c', 'UNSUPPORTED')))
        self.assertEqual(result['types']['COMPATIBLE'], 1)
        self.assertEqual(result['types']['DIVERGENT'], 1)
        self.assertEqual(result['ddl']['UNSUPPORTED'], 1)
        self.assertEqual(set(result), {'types', 'ddl'})

    def test_table_with_previous(self):
        old = report(('types', 'a', 'UNSUPPORTED'), ('gone', 'g', 'COMPATIBLE'))
        new = report(('types', 'a', 'COMPATIBLE'), ('types', 'b', 'COMPATIBLE'))
        lines = areas.table(new, old).splitlines()
        self.assertIn('COMPATIBLE before', lines[0])
        self.assertEqual(lines[2].split('|')[1].strip(), 'gone')
        self.assertTrue(lines[2].rstrip(' |').endswith('-1'))
        self.assertEqual([c.strip() for c in lines[3].split('|')][1:3], ['types', '2'])
        self.assertTrue(lines[3].rstrip(' |').endswith('+2'))
        self.assertTrue(lines[-1].startswith('| **total** | 2 |'))
        self.assertTrue(lines[-1].rstrip(' |').endswith('+1'))

    def test_merge_parts(self):
        a = dict(report(('types', 'a', 'COMPATIBLE')), run_id='r1', build=dict(commit='c' * 40),
                 started_at='2026-01-01T00:00:00Z', finished_at='2026-01-01T01:00:00Z')
        b = dict(report(('ddl', 'b', 'DIVERGENT')), run_id='r2', build=dict(commit='c' * 40),
                 started_at='2026-01-01T01:00:00Z', finished_at='2026-01-01T02:00:00Z')
        b['checks'] = []
        merged = areas.merge([a, b])
        self.assertEqual(merged['run_id'], 'r1+r2')
        self.assertEqual((merged['started_at'], merged['finished_at']),
                         ('2026-01-01T00:00:00Z', '2026-01-01T02:00:00Z'))
        self.assertEqual(merged['differential']['summary']['overall']['COMPATIBLE'], 1)
        self.assertEqual(merged['differential']['summary']['overall']['DIVERGENT'], 1)
        self.assertEqual(set(areas.counts(merged)), {'types', 'ddl'})
        self.assertEqual(len(a['differential']['cases']), 1)
        with self.assertRaises(ValueError):
            areas.merge([a, dict(b, build=dict(commit='d' * 40))])
        with self.assertRaises(ValueError):
            areas.merge([a, a])

    def test_table_without_previous(self):
        lines = areas.table(report(('types', 'a', 'HARNESS_ERROR'))).splitlines()
        self.assertNotIn('change', lines[0])
        self.assertEqual(len(lines), 4)


if __name__ == '__main__':
    unittest.main()
