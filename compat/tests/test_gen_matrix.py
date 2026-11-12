"""Matrix rendering checks; run with the compat unittest discovery suite."""
import json
from pathlib import Path
import unittest

import gen_matrix


class MatrixTests(unittest.TestCase):
    def report(self):
        root = Path(__file__).resolve().parents[1]
        return json.loads((root / 'reports/c76276aa/report.json').read_text())

    def test_report_driven_divergences(self):
        report = self.report()
        result = gen_matrix.generate(report)
        section = result.split('## Deliberate divergences')[1]
        rows = [line for line in section.splitlines() if line.startswith('| ')][2:]
        cases = [case for case in report['differential']['cases'] if case['outcome'] == 'INTENTIONAL']
        self.assertEqual(len(rows), 3)
        self.assertEqual(len(rows), len(cases))
        for row, case in zip(rows, cases):
            self.assertIn(case['title'], row)
            self.assertIn('`' + case['first_divergence']['giggles']['sqlstate'] + '`', row)
            self.assertIn(f"[{case['id']}](corpus/{case['area']}/{case['id']}.sql)", row)
        self.assertNotIn('domain_cast', section)
        self.assertNotIn('drop_type_cascade', section)

    def test_missing_case_and_state(self):
        report = self.report()
        report['differential']['cases'] = [dict(area='missing', id='missing', title='A | B',
                                               outcome='INTENTIONAL', first_divergence=None)]
        result = gen_matrix.generate(report)
        self.assertIn('| A &#124; B | unrecorded | missing |', result)

    def test_paths_command_and_commit_fallback(self):
        root = Path(__file__).resolve().parents[1]
        output = root / 'nested output' / 'matrix.md'
        report = self.report()
        result = gen_matrix.generate(report, dict(results=[]), dict(results=[]),
                    report_path=root / 'reports/c76276aa/report.json',
                    orm_path=root / 'reports/c76276aa/orm-report.json',
                    gui_path=root / 'reports/c76276aa/gui-report.json',
                    output=output, server_commit='fallback-sha')
        self.assertIn('python -I ../gen_matrix.py ../reports/c76276aa/report.json --output matrix.md', result)
        self.assertIn('--orm-report ../reports/c76276aa/orm-report.json', result)
        self.assertIn('--gui-report ../reports/c76276aa/gui-report.json', result)
        self.assertIn('--server-commit fallback-sha', result)
        self.assertIn('[report](../reports/c76276aa/report.md)', result)
        self.assertIn('[JSON](../reports/c76276aa/report.json)', result)
        self.assertIn('Server commit: `fallback-sha`', result)
        self.assertNotIn('adb1f1e5', result)
        self.assertIn('Server commit: `unrecorded`', gen_matrix.generate(report))
        report['build']['commit'] = 'recorded-sha'
        self.assertIn('Server commit: `recorded-sha`',
                      gen_matrix.generate(report, server_commit='fallback-sha'))

    def test_orm_counts_and_diagnostics(self):
        rows = [dict(orm='tiny', target=target, step='read', status='PASS')
                for target in ('pg', 'giggles')]
        rows += [dict(orm='tiny', target='giggles', step='migrate', status='FAIL',
                      sqlstate='42601', error='bad | syntax\nnext line'),
                 dict(orm='tiny', target='giggles', step='write', status='FAIL',
                      classification='blocked', reason='migration failed')]
        result = '\n'.join(gen_matrix.orm_section(dict(results=rows),
                          Path('compat/reports/abc/orm-report.json'), Path('compat/MATRIX.md')))
        self.assertIn('| tiny | 1/1 | 1/3 | 1 | 1 |', result)
        self.assertIn('| migrate | FAIL | 42601 | bad &#124; syntax next line |', result)
        self.assertIn('| write | BLOCKED |  | migration failed |', result)
        self.assertIn('(reports/abc/orm-report.json)', result)

    def test_gui_counts_and_state_fallback(self):
        rows = [dict(tool='browser', node='tables', outcome='PASS'),
                dict(tool='browser', node='columns', outcome='FAIL',
                     giggles=dict(sqlstate='42703'), owner='catalog'),
                dict(tool='browser', node='connect', outcome='FAIL',
                     sqlstate='08006', giggles=dict(sqlstate='XX000'), owner='server'),
                dict(tool='browser', node='oracle', outcome='HARNESS_ERROR')]
        result = '\n'.join(gen_matrix.gui_section(dict(results=rows, limitations=['Source only']),
                          Path('compat/reports/abc/gui-report.json'), Path('compat/MATRIX.md')))
        self.assertIn('| browser | 1 | 2 | 1 | 4 |', result)
        self.assertIn('| browser | columns | FAIL | 42703 | catalog |', result)
        self.assertIn('| browser | connect | FAIL | 08006 | server |', result)
        self.assertIn('(reports/abc/gui-report.json)', result)
        self.assertIn('Source only', result)

    def test_empty_lanes(self):
        for render in (gen_matrix.orm_section, gen_matrix.gui_section):
            result = '\n'.join(render(dict(results=[]), Path('empty.json'), Path('MATRIX.md')))
            self.assertIn('Evidence: [JSON](empty.json).', result)


if __name__ == '__main__':
    unittest.main()
