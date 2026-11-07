import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import harness
from compat_lib import oracle, probes, psql_client
from tests.task_storage import ModuleStorage

_STORAGE = ModuleStorage()
def setUpModule(): _STORAGE.start()
def tearDownModule(): _STORAGE.finish()

class H3dRunnerTests(unittest.TestCase):
    def test_owner_validation(self):
        self.assertEqual(harness.parse_owners(' E1,H,E1 '), {'E1','H'})
        for value in ('', 'Z', 'E1,Z'):
            with self.assertRaises(ValueError): harness.parse_owners(value)
        with self.assertRaises(SystemExit):
            harness.main(['run','--owners','Z'])

    def test_log_cap_and_redaction(self):
        engine = Mock(prefix='owned-', env={})
        for payload, limit in [(b'password postgresql://user:password@host/db\n', 200),
                               (b'password\n' + b'x'*1000, 128),
                               (b'x'*125+b'password'+b'y'*100,128)]:
            process = Mock(stdout=io.BytesIO(payload))
            process.poll.return_value = 0
            process.wait.return_value = 0
            with tempfile.TemporaryDirectory(dir=_STORAGE.root) as folder, patch('subprocess.Popen',return_value=process) as popen:
                path = Path(folder)/'oracle.log'
                oracle.capture_log(engine,path,['password'],limit)
                output = path.read_bytes()
                self.assertLessEqual(len(output),limit)
                self.assertNotIn(b'password',output)
                self.assertNotIn(b'postgresql://',output)
                if len(payload)>limit: self.assertTrue(output.endswith(oracle.TRUNCATION_MARKER))
                self.assertEqual(popen.call_args.kwargs['stderr'], __import__('subprocess').STDOUT)

    def test_probe_env_rejects_connection_override(self):
        engine = Mock(prefix='owned-', env={}, containers=[])
        for env in ({'PGPASSWORD':'secret'}, {'PGHOST':'other'}, {'PGAPPNAME':'bad\nvalue'}):
            with self.assertRaises(ValueError):
                probes.run_probe_side(engine,'npgsql',{},'net','owned-client','host',5432,[],client_env=env)

    def test_profile_reuses_image_context(self):
        normal, notypes = (psql_client.REGISTRY[name] for name in ('npgsql','npgsql-notypes'))
        self.assertEqual(normal['driver'],notypes['driver'])
        self.assertEqual(normal['probe_dir'],notypes['probe_dir'])
        self.assertNotEqual(normal['r0_prefix'],notypes['r0_prefix'])

    def test_chunk_boundary_secret_and_utf8(self):
        payload = b'x'*65533 + b'password' + ('\u00e9'*300).encode()
        engine = Mock(prefix='owned-', env={})
        process = Mock(stdout=io.BytesIO(payload))
        process.poll.return_value = 0
        process.wait.return_value = 0
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as folder, patch('subprocess.Popen',return_value=process):
            path = Path(folder)/'oracle.log'
            oracle.capture_log(engine,path,['password'],65601)
            output = path.read_bytes()
            output.decode('utf-8', 'strict')
            self.assertLessEqual(len(output),65601)
            self.assertNotIn(b'password',output)
            self.assertIn(b'[REDACTED]',output)
            self.assertTrue(output.endswith(oracle.TRUNCATION_MARKER))

    def test_readiness_failure_captures_before_cleanup(self):
        from compat_lib import docker, report, util
        events = []
        engine = Mock(prefix='owned-', containers=[], networks=[])
        engine.image.return_value = True
        engine.cleanup.side_effect = lambda: events.append('cleanup')
        def fail_start(*args):
            engine.containers.append('owned-pg')
            raise RuntimeError('readiness failed')
        def capture(*args): events.append('capture')
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as folder, \
             patch.object(docker,'Docker',return_value=engine), \
             patch.object(oracle,'start',side_effect=fail_start), \
             patch.object(oracle,'capture_log',side_effect=capture), \
             patch.object(util,'command',return_value={'stdout':''}), \
             patch.object(report,'write') as write, patch('builtins.print'):
            result = harness.main(['run','--server-bin','unused','--no-corpus',
                                   '--out-root',folder,'--build-root',folder,'--cargo-target',folder])
            self.assertEqual(result,2)
            self.assertEqual(events,['capture','cleanup'])
            self.assertEqual(write.call_args.args[1]['oracle_log'],'oracle.log')

    def test_failed_docker_log_capture_does_not_create_artifact(self):
        engine = Mock(prefix='owned-', env={})
        process = Mock(stdout=io.BytesIO(b'No such container'))
        process.poll.return_value = 1
        process.wait.return_value = 1
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as folder, patch('subprocess.Popen',return_value=process):
            path = Path(folder)/'oracle.log'
            with self.assertRaisesRegex(RuntimeError, 'capture failed'):
                oracle.capture_log(engine,path,['password'])
            self.assertFalse(path.exists())
