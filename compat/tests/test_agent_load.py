"""R3-404: application latency bound harness (compat/agent_load.py)."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
import uuid

import agent_load as al

SCRIPT = Path(al.__file__).resolve()


def phase(select, update, insert, errors=None, agents=0):
    def op(p99, n=1000):
        return dict(count=n, p50_ms=p99 / 2, p99_ms=p99, p999_ms=p99 * 1.5, max_ms=p99 * 2)
    return dict(seconds=5.0, operations=dict(select=op(select), update=op(update), insert=op(insert)),
                application_errors=errors or {}, application_error_samples=[],
                agents=dict(tasks=agents, sessions=agents, statements=agents * 10, outcomes={}))


class PercentileMath(unittest.TestCase):
    def test_percentile_math(self):
        values = list(range(1, 101))
        self.assertEqual(al.percentile(values, 50), 50)
        self.assertEqual(al.percentile(values, 99), 99)
        self.assertEqual(al.percentile(values, 99.9), 100)
        self.assertEqual(al.percentile(values, 100), 100)
        # Nearest rank with exact arithmetic: 0.999 * 1000 is rank 999, not 1000.
        thousand = list(range(1, 1001))
        self.assertEqual(al.percentile(thousand, 99.9), 999)
        self.assertEqual(al.percentile(thousand, 99), 990)
        self.assertEqual(al.percentile([7.5], 99), 7.5)
        self.assertIsNone(al.percentile([], 99))
        summary = al.summarize([3.0, 1.0, 2.0, 4.0], seconds=2)
        self.assertEqual(summary, dict(count=4, p50_ms=2.0, p99_ms=4.0, p999_ms=4.0, max_ms=4.0, per_second=2.0))
        self.assertEqual(al.summarize([])['p99_ms'], None)


class BoundCheck(unittest.TestCase):
    def setUp(self):
        self.bound = al.load_bound()

    def test_checked_in_bound_is_the_design_constant(self):
        b = self.bound
        self.assertEqual((b['percentile'], b['ratio'], b['slack_ms'], b['max_application_errors']), (99, 1.25, 2.0, 0))
        self.assertEqual(b['checked_phases'], ['B', 'C'])
        self.assertEqual(b['agent_max_concurrent_statements'], 8)
        self.assertEqual((b['full']['rows'], b['full']['application_connections'], b['full']['phase_seconds']), (1000000, 16, 120))
        self.assertEqual(b['full']['mix'], dict(select=80, update=15, insert=5))
        self.assertEqual((b['smoke']['rows'], b['smoke']['phase_seconds']), (10000, 5))

    def test_bound_check_pass_and_fail(self):
        a = phase(10.0, 20.0, 1.0)
        # Ratio side: 12.5 <= 1.25 * 10; slack side: 2.9 <= max(1.25, 1 + 2).
        passing = dict(A=a, B=phase(12.5, 25.0, 2.9, agents=8), C=phase(12.0, 24.0, 3.0, agents=32))
        check = al.check_bound(passing, self.bound)
        self.assertTrue(check['passed'], check)
        self.assertEqual(al.exit_code(check), al.EXIT_PASS)
        self.assertEqual(len(check['results']), 6)
        self.assertEqual(check['max_ratio'], 3.0)
        insert_b = next(r for r in check['results'] if (r['phase'], r['operation']) == ('B', 'insert'))
        self.assertEqual((insert_b['limit_ms'], insert_b['ratio'], insert_b['passed']), (3.0, 2.9, True))

        slow = copy.deepcopy(passing)
        slow['C']['operations']['update']['p99_ms'] = 25.01
        check = al.check_bound(slow, self.bound)
        self.assertFalse(check['passed'])
        self.assertTrue(check['application_errors_passed'])
        self.assertEqual(al.exit_code(check), al.EXIT_BOUND)
        failed = [(r['phase'], r['operation']) for r in check['results'] if not r['passed']]
        self.assertEqual(failed, [('C', 'update')])

        errors = copy.deepcopy(passing)
        errors['B']['application_errors'] = {'update:53300': 1}
        check = al.check_bound(errors, self.bound)
        self.assertFalse(check['passed'])
        self.assertTrue(check['latency_passed'])
        self.assertEqual(check['application_errors'], 1)
        self.assertEqual(al.exit_code(check), al.EXIT_APP_ERRORS)
        # Errors in phase A count too.
        errors = copy.deepcopy(passing)
        errors['A']['application_errors'] = {'select:connection': 2}
        self.assertEqual(al.check_bound(errors, self.bound)['application_errors'], 2)

        empty = copy.deepcopy(passing)
        empty['B']['operations']['insert'] = al.summarize([])
        check = al.check_bound(empty, self.bound)
        self.assertFalse(check['passed'])
        self.assertIn('no samples', [r.get('reason') for r in check['results']])


class ReportSchema(unittest.TestCase):
    def test_report_schema(self):
        bound = al.load_bound()
        phases = dict(A=phase(10.0, 20.0, 1.0), B=phase(11.0, 21.0, 1.5, agents=8), C=phase(30.0, 21.0, 1.5, agents=32))
        report = al.assemble_report('smoke', 'a' * 40, '2026-11-17T00:00:00Z', dict(rows=10000), bound, phases,
                                    work_dir='J:/gigglesdb-work/x')
        self.assertEqual(al.validate_report(report), [])
        self.assertEqual(report['schema'], al.SCHEMA_ID)
        self.assertEqual(report['exit'], al.EXIT_BOUND)
        self.assertEqual(report['check']['max_ratio'], 3.0)
        again = json.loads(json.dumps(report))
        self.assertEqual(al.validate_report(again), [])
        broken = copy.deepcopy(again)
        del broken['phases']['C']['operations']['insert']['p99_ms']
        del broken['check']['passed']
        broken['schema'] = 'other'
        problems = al.validate_report(broken)
        self.assertIn('C.insert.p99_ms missing', problems)
        self.assertIn('check.passed missing', problems)
        self.assertIn('report.schema is not ' + al.SCHEMA_ID, problems)


class SmokeRun(unittest.TestCase):
    """The --smoke mode against a built giggles-server (GIGGLES_SERVER_BIN).

    AGENT_LOAD_TASK_TOOL, when set, is passed as --task-tool for servers that do
    not yet run CREATE TASK over SQL (R3-110). Data and logs go under
    AGENT_LOAD_WORK (default J:/gigglesdb-work/agent-load/pytest).
    """

    def test_smoke_run_against_server(self):
        binary = os.environ.get('GIGGLES_SERVER_BIN')
        if not binary or not Path(binary).is_file():
            self.skipTest('no server binary: set GIGGLES_SERVER_BIN to a built giggles-server')
        work = Path(os.environ.get('AGENT_LOAD_WORK', 'J:/gigglesdb-work/agent-load/pytest')) / uuid.uuid4().hex[:12]
        work.mkdir(parents=True)
        report_path = work / 'agent-load.json'
        args = [sys.executable, '-I', str(SCRIPT), '--smoke', '--server-bin', binary, '--work', str(work),
                '--report', str(report_path)]
        if os.environ.get('AGENT_LOAD_TASK_TOOL'):
            args += ['--task-tool', os.environ['AGENT_LOAD_TASK_TOOL']]
        env = dict(os.environ, TEMP=str(work), TMP=str(work))
        result = subprocess.run(args, capture_output=True, text=True, env=env, timeout=900)
        (work / 'pytest-stdout.log').write_text(result.stdout + result.stderr, encoding='utf-8')
        # A smoke run is too short and too small to judge the bound; it must
        # finish, report, and see no application errors (exit 0 or 3).
        self.assertIn(result.returncode, (al.EXIT_PASS, al.EXIT_BOUND), result.stdout[-3000:] + result.stderr[-2000:])
        report = json.loads(report_path.read_text(encoding='utf-8'))
        self.assertEqual(al.validate_report(report), [])
        self.assertEqual(report['mode'], 'smoke')
        self.assertEqual(report['config']['rows'], 10000)
        self.assertEqual(report['check']['application_errors'], 0, report['phases'])
        for name in al.PHASES:
            for op in al.OPERATIONS:
                self.assertGreater(report['phases'][name]['operations'][op]['count'], 0, (name, op))
        b, c = report['phases']['B']['agents'], report['phases']['C']['agents']
        self.assertEqual((b['tasks'], b['sessions']), (4, 8))
        self.assertEqual((c['tasks'], c['sessions']), (32, 32))
        self.assertEqual(report['phases']['A']['agents']['statements'], 0)
        # Every agent step reaches its limit in B: rows scanned and the
        # rows-returned cap (54T01), the held row lock (55P03). In C the agent
        # statement ceiling (R3-403) refuses the sessions beyond it.
        for key in ('scan:54T01/rows_scanned', 'lock:55P03', 'page:54T01/rows_returned'):
            self.assertGreater(b['outcomes'].get(key, 0), 0, (key, b['outcomes']))
        self.assertGreater(sum(n for k, n in c['outcomes'].items() if k.endswith(':54T01/agent_ceiling')), 0, c['outcomes'])
        allowed = ('ok', 'block_end', '55P03', '54T01/rows_scanned', '54T01/rows_returned', '54T01/agent_ceiling')
        unexpected = {k: v for k, v in {**b['outcomes'], **c['outcomes']}.items() if k.split(':', 1)[1] not in allowed}
        self.assertEqual(unexpected, {}, (b['messages'], c['messages']))

if __name__ == '__main__':
    unittest.main()
