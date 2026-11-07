import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import harness
from compat_lib import corpus, pins, psql_client, report


from tests.task_storage import ModuleStorage

_STORAGE = ModuleStorage()
TASK = _STORAGE.root

def setUpModule():
    _STORAGE.start()

def tearDownModule():
    _STORAGE.finish()

class DifferentialRunnerTests(unittest.TestCase):
    def case(self, expect='compatible'):
        return dict(id='case_a', area='types', title='A', owner='A', expect=expect,
                    reason='planned', warnings=[], statements=[dict(index=1,sql='SELECT 1;',expected_error=False)])

    def test_flags_and_session_command(self):
        args = harness.parser().parse_args(['run','--no-checks','--no-corpus','--corpus','types/*'])
        self.assertTrue(args.no_checks and args.no_corpus)
        self.assertEqual(args.corpus,'types/*')
        engine = Mock(containers=[])
        engine.call.return_value = dict(exit=None,timed_out=True)
        psql_client.run_session(engine,pins.load()['clients']['psql'],'net','name','host',123,'script')
        command = engine.call.call_args.args
        self.assertNotIn('--echo-all',command)
        self.assertNotIn('-q',command)
        self.assertIn('ON_ERROR_STOP=0',command)
        self.assertEqual(engine.call.call_args.kwargs['timeout'],120)
        engine.remove_client.assert_called_once_with('name')
        self.assertIn('run_session',psql_client.REGISTRY['psql'])

    def test_one_session_per_server_and_artifacts(self):
        output = dict(exit=0,timed_out=False,stdout='@@R0 BEGIN 1\n1\n1\n@@R0 END 1 00000 1\n',stderr='',duration_ms=1)
        runner = Mock(return_value=output)
        engine = Mock(prefix='task-',containers=[])
        data = dict(networking={}, differential=dict(cases=[]))
        with tempfile.TemporaryDirectory(dir=TASK) as folder, patch.dict(psql_client.REGISTRY['psql'],run_session=runner):
            harness.run_corpus_case(engine,'psql',{},self.case(),True,'',data,Path(folder),'net','pg',5433,'secret')
            directory=Path(folder)/'corpus/types/case_a'
            self.assertTrue((directory/'script.sql').exists())
            self.assertTrue((directory/'result.json').exists())
            self.assertTrue((directory/'pg.stdout.txt').exists())
        self.assertEqual(runner.call_count,2)
        self.assertEqual([call.args[4] for call in runner.call_args_list],['pg','host.docker.internal'])
        self.assertTrue(all(call.args[7] == 120 for call in runner.call_args_list))
        self.assertEqual(data['differential']['cases'][0]['outcome'],'COMPATIBLE')
        self.assertNotIn('pg',data['differential']['cases'][0])
        self.assertNotIn('differences',data['differential']['cases'][0])

    def test_untested_does_not_invoke(self):
        runner = Mock()
        data = dict(differential=dict(cases=[]))
        with tempfile.TemporaryDirectory(dir=TASK) as folder, patch.dict(psql_client.REGISTRY['psql'],run_session=runner):
            harness.run_corpus_case(Mock(),'psql',{},self.case('untested'),True,'',data,Path(folder),'net','pg',1,'secret')
        runner.assert_not_called()
        self.assertEqual(data['differential']['cases'][0]['outcome'],'UNTESTED')
        self.assertFalse(data['differential']['cases'][0]['stale_expectation'])

    def test_corpus_glob_forms(self):
        case = dict(self.case(),path='compat/corpus/types/filename.sql')
        for pattern in ('case_a','types','types/case_a','types/*.sql','types/filename.sql'):
            self.assertTrue(harness.corpus_matches(case,pattern),pattern)
        self.assertFalse(harness.corpus_matches(case,'other/*'))

    def test_connection_error_does_not_claim_reached_server(self):
        output = dict(exit=2,timed_out=False,stdout='',stderr='connection refused',duration_ms=1)
        runner = Mock(return_value=output)
        data = dict(networking=dict(status='unverified'), differential=dict(cases=[]))
        with tempfile.TemporaryDirectory(dir=TASK) as folder, patch.dict(psql_client.REGISTRY['psql'],run_session=runner):
            harness.run_corpus_case(Mock(prefix='task-',containers=[]),'psql',{},self.case(),True,'',data,Path(folder),'net','pg',1,'secret')
        self.assertEqual(data['networking']['status'],'unverified')

    def test_differential_markdown_and_owner_rates(self):
        case = dict(self.case(),outcome='DIVERGENT',stale_expectation=True,
                    first_divergence=dict(sql='SELECT 1;',pg=dict(sqlstate='00000'),giggles=dict(sqlstate='XX000',stderr_first_line='ERROR unsupported')))
        summary = corpus.summarize([case])
        self.assertEqual(summary['by_owner']['A']['pass_rate'],0)
        text='\n'.join(report.differential_markdown(dict(cases=[case],summary=summary)))
        self.assertIn('| Area | Cases | COMPATIBLE | UNSUPPORTED | DIVERGENT | INTENTIONAL | UNTESTED | HARNESS_ERROR | Pass rate |',text)
        self.assertIn('### A',text)
        self.assertIn('PostgreSQL 00000 vs GigglesDB XX000',text)
        self.assertIn('stale_expectation',text)
        self.assertIn('ERROR unsupported',text)

    def test_intentional_difference_is_listed_by_owner(self):
        case = dict(self.case('intentional'),outcome='INTENTIONAL',stale_expectation=False,
                    first_divergence=dict(sql='SELECT 1;',pg=dict(sqlstate='00000'),giggles=dict(sqlstate='0A000',stderr_first_line='ERROR unsupported')))
        text='\n'.join(report.differential_markdown(dict(cases=[case],summary=corpus.summarize([case]))))
        self.assertIn('### A',text)
        self.assertIn('case_a: INTENTIONAL',text)
        self.assertIn('PostgreSQL 00000 vs GigglesDB 0A000',text)

    def test_harness_error_owner_fallback(self):
        case = dict(self.case(),outcome='HARNESS_ERROR',first_divergence=None,
                    harness_errors=[dict(index=1,sql='SELECT missing;',reason='unexpected PostgreSQL error')])
        text='\n'.join(report.differential_markdown(dict(cases=[case],summary=corpus.summarize([case]))))
        self.assertIn('SELECT missing;',text)
        self.assertIn('Harness reason: unexpected PostgreSQL error',text)
        self.assertIn('PostgreSQL N/A vs GigglesDB N/A',text)
