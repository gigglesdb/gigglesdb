import base64
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from compat_lib import probes
from tests.task_storage import ModuleStorage

_STORAGE = ModuleStorage()
TASK = _STORAGE.root


def setUpModule():
    _STORAGE.start()


def tearDownModule():
    _STORAGE.finish()


MANIFEST = [dict(id='a', title='A', owner='A', expect_error=False)]


def record(value=None, status='OK', case_id='a'):
    encoded = base64.b64encode(json.dumps(value or {'n': 1}).encode()).decode()
    return '\t'.join(['CASE', case_id, status, encoded])


def stream(case=None):
    return 'VERSION\tjdbc\t42.7.3\n' + (case or record()) + '\nDONE\t1\n'


class ProbeTests(unittest.TestCase):
    def parse(self, text):
        return probes.parse_records(text, MANIFEST, 'jdbc', '42.7.3')

    def test_strict_protocol(self):
        self.assertTrue(self.parse(stream())['valid'])
        malformed = [
            stream().replace('VERSION', 'OTHER'), stream().replace('42.7.3', 'wrong'),
            stream().replace('jdbc', 'other'), stream() + '\n', stream() + record(),
            stream().replace('DONE\t1', 'DONE\t01'), stream().replace('DONE\t1', 'DONE\t2'),
            stream().replace('DONE\t1', ''), stream(record(case_id='unknown')),
            stream(record() + '\n' + record()), stream().replace('OK', 'PASS'),
            stream('CASE\ta\tOK\t!!!!'), stream('CASE\ta\tOK\t/w=='),
            stream('CASE\ta\tOK\te30==='),
            stream('CASE\ta\tOK\t' + base64.b64encode(b'[]').decode()),
            stream('CASE\ta\tOK\t' + base64.b64encode(b'{"a":NaN}').decode()),
            stream('CASE\ta\tOK\t' + base64.b64encode(b'{"a":1e999}').decode()),
            stream('CASE\ta\tOK\t' + base64.b64encode(b'{"a":1,"a":2}').decode()),
            stream(record({'exception_class': 'AssertionError'}, 'ERR')),
            stream(record({'sqlstate': 'bad', 'exception_class': 'SQLException'}, 'ERR')),
            stream(record({'sqlstate': '23505'}, 'ERR')),
            stream().replace('DONE\t1', 'VERSION\tjdbc\t42.7.3\nDONE\t1'),
        ]
        for text in malformed:
            with self.subTest(text=text):
                self.assertFalse(self.parse(text)['valid'])
        self.assertTrue(self.parse(stream(record({'sqlstate': None, 'exception_class': 'java.lang.AssertionError', 'reason': 'value_mismatch'}, 'ERR')))['valid'])
        self.assertTrue(self.parse(stream(record({'sqlstate': None, 'exception_class': 'NullPointerException'}, 'ERR')))['valid'])

    def test_version_metadata(self):
        def parse(header, driver='psycopg', fields=('libpq_version',)):
            return probes.parse_records(header + '\n' + record() + '\nDONE\t1\n',
                                        MANIFEST, driver, '3.2.9', fields)
        good = parse('VERSION\tpsycopg\t3.2.9\t180000')
        self.assertTrue(good['valid'])
        self.assertEqual(good['libpq_version'], 180000)
        for extra in ('', '\t0', '\t01', '\t-1', '\t1.0', '\t+1', '\t123\t123', '\tNaN'):
            with self.subTest(extra=extra):
                self.assertFalse(parse('VERSION\tpsycopg\t3.2.9' + extra)['valid'])
        self.assertFalse(parse('VERSION\tjdbc\t3.2.9\t180000', 'jdbc')['valid'])
        self.assertFalse(parse('VERSION\tpsycopg\t3.2.9\t180000', fields=('libpq_version', 'libpq_version'))['valid'])
        self.assertFalse(self.parse(stream().replace('42.7.3', '42.7.3\t180000'))['valid'])
        self.assertFalse(probes.parse_records('VERSION\tnode-pg\t8.16.3\t180000\n' + record() + '\nDONE\t1\n', MANIFEST, 'node-pg', '8.16.3')['valid'])

    def test_assertion_reasons(self):
        for exception in ('JdbcCompatProbe$ProbeAssertion', 'ProbeAssertion', 'AssertionError'):
            error = dict(exception_class=exception, sqlstate=None)
            for nested in (False, True):
                for reason in (None, '', 'bad reason', 'Upper', 'a' * 65, 1):
                    value = dict(error)
                    if reason is not None:
                        value['reason'] = reason
                    observation = {'steps': [dict(value, status='ERR')]} if nested else value
                    with self.subTest(exception=exception, nested=nested, reason=reason):
                        self.assertFalse(self.parse(stream(record(observation, 'OK' if nested else 'ERR')))['valid'])
                good = dict(error, reason='value_mismatch')
                observation = {'steps': [dict(good, status='ERR')]} if nested else good
                self.assertTrue(self.parse(stream(record(observation, 'OK' if nested else 'ERR')))['valid'])
        self.assertTrue(self.parse(stream(record({'reason': 'ordinary row text'})))['valid'])
        self.assertFalse(self.parse(stream(record({'status': 'ERR', 'reason': 'invalid reason'})))['valid'])

    def test_missing_records_classified_individually(self):
        manifest = MANIFEST + [dict(id='b', title='B', owner='B', expect_error=False)]
        side = probes.parse_records(stream(), manifest, 'jdbc', '42.7.3')
        self.assertTrue(side['valid'])
        self.assertEqual(side['missing'], ['b'])
        self.assertEqual([row['status'] for row in probes.classify_cases(manifest, side, side)], ['PASS', 'HARNESS_ERROR'])
        complete = dict(side, records=dict(side['records'], b=side['records']['a']))
        self.assertEqual(probes.classify_cases(manifest, complete, side)[1]['status'], 'FAIL')

    def test_classification(self):
        good = self.parse(stream())
        different = self.parse(stream(record({'n': 2})))
        self.assertEqual(probes.classify_cases(MANIFEST, good, good)[0]['status'], 'PASS')
        failure = probes.classify_cases(MANIFEST, good, different)[0]
        self.assertEqual(failure['status'], 'FAIL')
        self.assertIn('-    "n": 1', failure['diff'])
        for side in (dict(good, valid=False), dict(good, exit=1), dict(good, timed_out=True)):
            self.assertEqual(probes.classify_cases(MANIFEST, side, good)[0]['status'], 'HARNESS_ERROR')
            self.assertEqual(probes.classify_cases(MANIFEST, good, side)[0]['status'], 'FAIL')
        error = self.parse(stream(record({'sqlstate': '23505', 'exception_class': 'SQLException'}, 'ERR')))
        self.assertEqual(probes.classify_cases(MANIFEST, error, error)[0]['status'], 'HARNESS_ERROR')
        expected = [dict(MANIFEST[0], expect_error=True)]
        self.assertEqual(probes.classify_cases(expected, error, error)[0]['status'], 'PASS')
        self.assertEqual(probes.classify_cases(expected, good, good)[0]['status'], 'HARNESS_ERROR')
        marked = [dict(MANIFEST[0], untested_reason='later')]
        self.assertEqual(probes.classify_cases(marked, {}, {})[0]['status'], 'UNTESTED')

    def test_manifest_validation(self):
        for manifest in ([], {}, [dict(MANIFEST[0], id='../escape')], MANIFEST * 2,
                         [dict(MANIFEST[0], expect_error=1)], [dict(MANIFEST[0], owner='Z')],
                         [dict(MANIFEST[0], untested_reason='')], [dict(MANIFEST[0], extra=1)]):
            with self.subTest(manifest=manifest), self.assertRaises(ValueError):
                probes.validate_manifest(manifest)

    def test_hash_and_build(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            root = Path(directory)
            (root / 'Dockerfile').write_text('FROM scratch')
            first = probes.probe_image_tag('jdbc', root)
            self.assertEqual(first, probes.probe_image_tag('jdbc', root))
            (root / 'cases.json').write_text('[]')
            self.assertNotEqual(first, probes.probe_image_tag('jdbc', root))
            ok = dict(exit=0, timed_out=False, stdout=json.dumps([{'Id': 'sha256:a', 'RepoDigests': []}]), stderr='')
            bad = dict(exit=1, timed_out=False, stdout='', stderr='secret')
            engine = Mock(env={'PGPASSWORD': 'secret'})
            engine.call.side_effect = [bad, bad]
            result = probes.prepare_image(engine, 'jdbc', root, root / 'run')
            self.assertFalse(result['available'])
            self.assertEqual(result['image_build_cache'], 'miss')
            self.assertGreaterEqual(result['image_build_duration_s'], 0)
            self.assertNotIn('secret', (root / 'run/checks/jdbc/build.stderr.txt').read_text())
            self.assertEqual(engine.call.call_args_list[1].args[:3], ('build', '--tag', result['tag']))
            engine.call.reset_mock(side_effect=True)
            engine.call.return_value = ok
            cached = probes.prepare_image(engine, 'jdbc', root, root / 'run')
            self.assertTrue(cached['available'])
            self.assertEqual(cached['image_build_cache'], 'hit')
            self.assertEqual(cached['image_build_duration_s'], 0)
            self.assertEqual(engine.call.call_count, 1)
            engine.call.side_effect = OSError('Docker unavailable')
            self.assertFalse(probes.prepare_image(engine, 'jdbc', root, root / 'run')['available'])

    def test_client_secret_and_timeout_cleanup(self):
        engine = Mock(prefix='gd-test-', env={'PGPASSWORD': 'secret'}, containers=[])
        raw = dict(exit=None, timed_out=True, stdout=stream(), stderr='secret')
        with patch.object(probes, 'command', return_value=raw) as command:
            result = probes.run_probe_side(engine, 'jdbc', 'image', 'network', 'gd-test-client', 'host', 5432, MANIFEST)
        args, env = command.call_args.args
        self.assertNotIn('secret', str(args))
        self.assertEqual(env['PGPASSWORD'], 'secret')
        self.assertNotIn('PGHOST', engine.env)
        self.assertEqual(command.call_args.kwargs['timeout'], 180)
        self.assertEqual(result['stderr'], '[REDACTED]')
        engine.remove_client.assert_called_once_with('gd-test-client')
