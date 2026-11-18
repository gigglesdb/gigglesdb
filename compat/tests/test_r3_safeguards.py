import json
import unittest
from pathlib import Path
from unittest.mock import patch
import r3_safeguards as r


class SafeguardsTests(unittest.TestCase):
    def test_default_drivers(self):
        args = r.argument_parser().parse_args(['--server-bin', 'server', '--work', 'work'])
        self.assertEqual(args.drivers, 'psycopg,pgx')
        self.assertEqual(args.go, str(r.PINNED_GO) if r.PINNED_GO.exists() else 'go')

    def test_go_environment(self):
        work = Path('J:/gigglesdb-work/r3/S16/trial')
        root = work/'run-example'
        with patch.object(Path, 'mkdir') as mkdir:
            env = r.go_environment(work, root, {'KEEP': 'value', 'GOFLAGS': 'old'})
        self.assertEqual(mkdir.call_count, 4)
        self.assertEqual(env['KEEP'], 'value')
        self.assertEqual(env['GOFLAGS'], '-mod=readonly')
        for key, folder in [('GOPATH', 'path'), ('GOMODCACHE', 'mod'), ('GOCACHE', 'cache')]:
            self.assertEqual(Path(env[key]), work/'go'/folder)
        self.assertEqual(Path(env['TEMP']), root/'tmp')
        self.assertEqual(env['TEMP'], env['TMP'])

    def test_pgx_build(self):
        root = Path('J:/gigglesdb-work/r3/S16/trial/run-example')
        from unittest.mock import mock_open
        with patch.object(Path, 'open', mock_open()), patch.object(r.subprocess, 'run') as run:
            binary = r.build_pgx('go.exe', root, {'GOFLAGS': '-mod=readonly'})
        self.assertEqual(binary, root/'pgx-scenario.exe')
        self.assertEqual(run.call_args.args[0], ['go.exe', 'build', '-o', str(binary), './'])
        self.assertEqual(run.call_args.kwargs['cwd'], Path(r.__file__).parent/'r3'/'pgx')

    def test_config(self):
        config = r.make_config(Path('J:/gigglesdb-work/data'), Path('J:/gigglesdb-work/pki'), 1234, 2345)
        self.assertEqual(config['agent']['bindings'], [])
        self.assertEqual(config['agent']['postgres'], '127.0.0.1:2345')
        self.assertEqual(config['postgresql']['listen'], '127.0.0.1:1234')
        self.assertFalse(config['postgresql']['loopback_trust'])
        self.assertEqual(config['postgresql']['tls'], 'disabled')
        self.assertEqual(config['skew_seconds'], 0)
        for key, suffix in [('certificate','node1.pem'),('private_key','node1.key'),('client_ca','ca.pem')]:
            self.assertTrue(config[key].replace('\\','/').endswith('/pki/'+suffix))

    def test_work_root(self):
        self.assertEqual(r.under_work_base('J:/gigglesdb-work/r3/S16/test'),
                         Path('J:/gigglesdb-work/r3/S16/test').resolve())
        for path in ['K:/Projects/gigglesdb', 'J:/elsewhere', 'J:/gigglesdb-work/../elsewhere']:
            with self.assertRaises(ValueError):
                r.under_work_base(path)

    def test_report(self):
        rows = [dict(step=s,status='PASS',attempts=3,detail='checked | value') for s in r.STEPS]
        report = {'drivers': {'psycopg': {'steps':rows, 'workload':{'unexpected_errors':{}}, 'deviations':['timing']}}}
        self.assertEqual(r.validate_steps(rows), [])
        self.assertEqual(r.report_exit(report), 0)
        text = r.render_report(report)
        self.assertIn('| psycopg | 01 | PASS | 3 (2) |', text)
        self.assertIn('- timing', text)
        rows[0]['status'] = 'FAIL'
        self.assertEqual(r.report_exit(report), 2)
        self.assertIn('| 01 | FAIL |', r.render_report(report))
        rows[0]['status'] = 'PASS'
        report['drivers']['psycopg']['workload']['unexpected_errors'] = {'XX000':1}
        self.assertEqual(r.report_exit(report), 2)

    def test_invalid_steps(self):
        self.assertTrue(r.validate_steps([]))
        self.assertTrue(r.validate_steps([dict(step='01',status='SKIP',attempts=0,detail=None)]))
        self.assertTrue(r.validate_steps([None]))

    def test_handshake(self):
        self.assertEqual(r.parse_handshake('RESTART\n'), 'restart')
        self.assertEqual(r.parse_handshake('RESTARTED\n'), 'restarted')
        with self.assertRaises(ValueError):
            r.parse_handshake('READY')

    def test_spec(self):
        spec = dict(pg_port=1234,agent_port=2345,database='postgres',ca='ca',
                    secret_file='secret',passwords={},expires_ms=123,results_path='out',task_sql='sql')
        with patch.object(Path,'read_text',return_value=json.dumps(spec)):
            self.assertEqual(r.parse_spec('not-created'), spec)
        del spec['secret_file']
        with patch.object(Path,'read_text',return_value=json.dumps(spec)), self.assertRaises(ValueError):
            r.parse_spec('not-created')

    def test_seed(self):
        sql = r.seed_sql()
        self.assertEqual(sum(s.count('@example.com') for s in sql), 2071)
        self.assertIn('AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS', r.task_sql(1800000000000))


if __name__ == '__main__':
    unittest.main()
