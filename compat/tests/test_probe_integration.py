import json
import re
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import harness
from compat_lib import probes, psql_client, report
from tests.task_storage import ModuleStorage
_STORAGE = ModuleStorage()
TASK = _STORAGE.root
def setUpModule(): _STORAGE.start()
def tearDownModule(): _STORAGE.finish()

class ProbeIntegrationTests(unittest.TestCase):
    def invoke(self, image=None, pg=None, gg=None, pattern='*', client='jdbc'):
        manifest = [dict(id='one', title='First', owner='H', expect_error=False),
                    dict(id='two', title='Second', owner='E1', expect_error=False)]
        image = image or dict(available=True, tag='gd-r0-probe-jdbc:abc', image_id='sha256:abc', digest='sha256:abc', version=None)
        good = dict(exit=0, timed_out=False, stdout='password', stderr='password', valid=True, version='42.7.3',
                    records={case['id']:dict(status='OK', observation={'value':1}) for case in manifest})
        data = dict(clients={},checks=[],networking={})
        engine = SimpleNamespace(prefix='gd-r0-unit-', containers=[])
        with tempfile.TemporaryDirectory(dir=TASK) as temp:
            run = Path(temp)
            with patch.object(probes,'load_manifest',return_value=manifest), patch.object(probes,'prepare_image',return_value=image), patch.object(probes,'run_probe_side',side_effect=[dict(good, **(pg or {})),dict(good, **(gg or {}))]) as runner:
                harness.run_probe_client(engine,client,psql_client.REGISTRY[client],SimpleNamespace(checks=pattern),data,run,'network','pg',1234,'password')
                artifacts = {path.relative_to(run).as_posix():path.read_text() for path in run.rglob('*') if path.is_file()}
                calls = runner.call_args_list
        return data, artifacts, calls

    def test_registered_probe_manifests_and_sql_prefixes(self):
        root = Path(harness.__file__).parent
        for client, registration in psql_client.REGISTRY.items():
            if registration.get('kind') != 'probe':
                continue
            with self.subTest(client=client):
                manifest = probes.load_manifest(root / registration['probe_dir'] / 'cases.json')
                self.assertTrue(manifest)
                self.assertTrue(all(case['id'] for case in manifest))
                prefix = registration.get('r0_prefix', 'r0d_' + registration['driver'])
                self.assertIsNotNone(re.fullmatch(r'[a-z][a-z0-9_]{0,47}', prefix))

    def test_registry_and_success_artifacts(self):
        self.assertNotIn('run',psql_client.REGISTRY['jdbc'])
        data, artifacts, calls = self.invoke()
        self.assertEqual([r['outcome'] for r in data['checks']],['PASS','PASS'])
        self.assertEqual(data['clients']['jdbc']['version'],'42.7.3')
        self.assertEqual(data['clients']['jdbc']['digest'],'sha256:abc')
        self.assertEqual(data['clients']['jdbc']['image'],'gd-r0-probe-jdbc:abc')
        self.assertEqual(len(calls),2)
        self.assertEqual([call.args[4] for call in calls],['gd-r0-unit-c-0','gd-r0-unit-c-1'])
        self.assertEqual(calls[0].args[5:7],('pg',5432))
        self.assertEqual(calls[1].args[5:7],('host.docker.internal',1234))
        self.assertTrue(all(c.kwargs['timeout']==180 for c in calls))
        self.assertIn('checks/jdbc/pg.stdout.txt',artifacts)
        self.assertNotIn('password',''.join(artifacts.values()))
        summary,_=report.summarize(data['checks'],['jdbc'])
        self.assertIn('| jdbc | 2 |',report.client_table(summary['clients']))

    def test_psycopg_runtime_metadata_and_node_prefix(self):
        data, _, calls = self.invoke(pg=dict(version='3.2.9', libpq_version=180000), client='psycopg')
        self.assertEqual(data['clients']['psycopg']['version'], '3.2.9')
        self.assertEqual(data['clients']['psycopg']['libpq_version'], 180000)
        self.assertEqual(calls[0].kwargs['version_fields'], ('libpq_version',))
        _, _, calls = self.invoke(client='node-pg')
        self.assertEqual(calls[0].kwargs['r0_prefix'], 'r0d_nodepg')
        self.assertEqual(calls[0].kwargs['version_fields'], ())

    def test_build_failure_is_untested(self):
        data, artifacts, calls=self.invoke(image=dict(available=False,reason='probe image build failed',version=None))
        self.assertFalse(calls)
        self.assertIsNone(data['clients']['jdbc']['version'])
        self.assertEqual([r['outcome'] for r in data['checks']],['UNTESTED','UNTESTED'])
        self.assertTrue(all(r['reason']=='probe image build failed' for r in data['checks']))

    def test_invalid_oracle_is_harness_error(self):
        data,_,_=self.invoke(pg=dict(valid=False,error='missing DONE',records={},exit=0))
        self.assertTrue(all(r['outcome']=='HARNESS_ERROR' for r in data['checks']))

    def test_missing_giggles_case_and_filter(self):
        data,_,calls=self.invoke(gg=dict(valid=True,records={},exit=0),pattern='two')
        self.assertEqual(len(data['checks']),1)
        self.assertEqual(data['checks'][0]['outcome'],'FAIL')
        self.assertEqual(len(calls[0].args[7]),2)

    def test_observation_difference_and_sqlstate(self):
        gg=dict(valid=True,exit=0,stdout='',stderr='',records={'one':dict(status='ERR',observation={'exception_class':'SQLException','sqlstate':'42601'})})
        data, artifacts,_=self.invoke(gg=gg,pattern='one')
        self.assertEqual(data['checks'][0]['outcome'],'FAIL')
        self.assertEqual(data['checks'][0]['first_error'],'42601')
        self.assertIn('checks/jdbc/one/diff.txt',artifacts)


