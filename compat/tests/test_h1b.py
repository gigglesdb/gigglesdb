import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock
from types import SimpleNamespace
import harness
from compat_lib import checks, pins, psql_client, report


from tests.task_storage import ModuleStorage

_STORAGE = ModuleStorage()
TASK = _STORAGE.root

def setUpModule():
    _STORAGE.start()

def tearDownModule():
    _STORAGE.finish()

class H1bTests(unittest.TestCase):
    def parse(self, headers):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            path = Path(directory)/'check.sql'
            path.write_text('-- id: a\n-- title: A\n-- owner: A\n'+headers+'\nSELECT 1;')
            return checks.parse_check(path)

    def test_environment_headers_preserve_values(self):
        check = self.parse('-- env: PGTZ=UTC\n-- env: PGOPTIONS=-c x=a=b  \n-- compare: stdout+stderr')
        self.assertEqual(check['env'], {'PGTZ':'UTC','PGOPTIONS':'-c x=a=b  '})
        self.assertEqual(check['compare'], 'stdout+stderr')
        for header in ('-- env: PGPASSWORD=secret', '-- env: PGTZ', '-- env: PGTZ=x\n-- env: PGTZ=y', '-- compare: stderr'):
            with self.subTest(header=header), self.assertRaises(ValueError):
                self.parse(header)

    def test_stream_comparison(self):
        good = dict(exit=0,timed_out=False,stdout='one\r\n',stderr='NOTICE: a\r\n')
        changed = dict(good, stderr='NOTICE: b\n')
        self.assertEqual(checks.classify(good,changed),'PASS')
        self.assertEqual(checks.classify(good,changed,compare='stdout+stderr'),'FAIL')
        self.assertEqual(checks.classify(good,dict(good,stdout='two'),compare='stdout+stderr'),'FAIL')
        self.assertEqual(checks.classify(good,dict(good,stderr='NOTICE: a  \n'),compare='stdout+stderr'),'PASS')
        diff = checks.output_diff(good,changed,'stdout+stderr')
        self.assertIn('=== stdout ===',diff)
        self.assertIn('=== stderr ===',diff)
        self.assertIn('-NOTICE: a',diff)
        self.assertIn('+NOTICE: b',diff)

    def test_client_environment_is_opt_in(self):
        engine = Mock(containers=[])
        engine.call.return_value = dict(exit=0,timed_out=False)
        pin = pins.load()['clients']['psql']
        psql_client.run(engine,pin,'net','name','host',123,'sql',1)
        self.assertNotIn('PGTZ=UTC',engine.call.call_args.args)
        psql_client.run(engine,pin,'net','name','host',123,'sql',1,{'PGTZ':'UTC'})
        self.assertIn('PGTZ=UTC',engine.call.call_args.args)
        with self.assertRaises(ValueError):
            psql_client.run(engine,pin,'net','name','host',123,'sql',1,{'PGPASSWORD':'secret'})

    def test_registry_exposes_version_setup(self):
        self.assertTrue(callable(psql_client.REGISTRY['psql']['run']))
        self.assertTrue(callable(psql_client.REGISTRY['psql']['version']))

    def test_run_check_uses_registered_runner_for_both_sides(self):
        output = dict(exit=0,timed_out=False,stdout='one',stderr='',duration_ms=1)
        runner = Mock(return_value=output)
        engine = Mock(prefix='test-',containers=[])
        data = dict(networking={},checks=[])
        check = dict(id='a',title='A',owner='A',sql='SELECT 1',env={'PGTZ':'UTC'})
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            psql_client.REGISTRY['other'] = {'run':runner,'version':Mock()}
            try:
                harness.run_check(engine,'other',{},check,True,'',SimpleNamespace(client_timeout=2),data,Path(directory),'net','pg-host',123,'secret')
            finally:
                del psql_client.REGISTRY['other']
        self.assertEqual(runner.call_count,2)
        self.assertEqual([c.args[4] for c in runner.call_args_list],['pg-host','host.docker.internal'])
        self.assertTrue(all(c.kwargs['client_env'] == {'PGTZ':'UTC'} for c in runner.call_args_list))
        self.assertEqual(data['checks'][0]['outcome'],'PASS')
        self.assertEqual(data['checks'][0]['env'], {'PGTZ':'UTC'})
        self.assertEqual(data['checks'][0]['compare'], 'stdout')
