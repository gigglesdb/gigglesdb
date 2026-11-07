import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid

sys.dont_write_bytecode = True
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from compat_lib import checks, docker, gigglesdb, pins, psql_client, report, util

TASK = Path('J:/gigglesdb-work')/('h1a-unit-'+uuid.uuid4().hex)
TASK.mkdir(parents=True,exist_ok=True)
os.environ.update(TEMP=str(TASK),TMP=str(TASK),CARGO_TARGET_DIR=str(TASK/'target'))

class HarnessTests(unittest.TestCase):
    def test_normalization(self):
        self.assertEqual(checks.normalize(' x  \r\n\r\n'), ' x')
        self.assertNotEqual(checks.normalize('2\n1'),checks.normalize('1\n2'))

    def test_headers(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            path=Path(directory)/'a.sql'
            path.write_text('-- id: a\n-- title: A\n-- owner: E1\nSELECT 1;')
            self.assertEqual(checks.parse_check(path)['owner'],'E1')
            path.write_text('-- id: a\n-- title: A\n-- owner: Z\nSELECT 1;')
            with self.assertRaises(ValueError): checks.parse_check(path)
            path.write_text('-- id: a\n-- id: b\n-- title: A\n-- owner: A')
            with self.assertRaises(ValueError): checks.parse_check(path)

    def test_classification(self):
        good=dict(exit=0,timed_out=False,stdout='1 \r\n')
        self.assertEqual(checks.classify(good,dict(good,stdout='1')),'PASS')
        self.assertEqual(checks.classify(good,dict(good,exit=1)),'FAIL')
        self.assertEqual(checks.classify(dict(good,timed_out=True),good),'HARNESS_ERROR')
        self.assertEqual(checks.classify(good,dict(good,stdout='2')),'FAIL')
        self.assertEqual(checks.classify(None,None,available=False),'UNTESTED')
        self.assertEqual(checks.classify(None,None,marked=True),'UNTESTED')

    def test_cleanup_prefix(self):
        d=docker.Docker('r0-20261106T000000Z-abcdef',{})
        calls=[]
        d.call=lambda *args,**kw: calls.append(args)
        d.containers=[d.prefix+'c-1']; d.networks=[d.prefix+'net']
        d.cleanup()
        self.assertEqual(calls,[('rm','-f',d.prefix+'c-1'),('network','rm',d.prefix+'net')])
        with self.assertRaises(ValueError): d.remove_client('other')
        with self.assertRaises(ValueError): d.remove_client('gd-r0-r0-20261106T000000Z-123456-c-1')
        self.assertEqual(len(calls),2)
        with self.assertRaises(ValueError): docker.Docker('bad',{})

    def test_k_refusal(self):
        for path in ('K:/outside','k:/Projects/gigglesdb',r'\\?\K:\Projects\gigglesdb\compat',r'\\.\K:\Projects\gigglesdb\compat'):
            with self.assertRaises(ValueError): util.output_root(path)

    def test_banner(self):
        self.assertEqual(gigglesdb.parse_banner('postgresql=127.0.0.1:1234 preview=single-host\n','postgres://postgres:secret@127.0.0.1:1234/postgres\n'),1234)
        for bad in ('postgresql=127.0.0.1:0 preview=single-host','postgresql=0.0.0.0:1234 preview=single-host','extra postgresql=127.0.0.1:1234 preview=single-host'):
            with self.assertRaises(ValueError): gigglesdb.parse_banner(bad,'postgres://postgres@127.0.0.1:1234/postgres')

    def test_reports_redact(self):
        secret='synthetic-secret'
        item=dict(client='psql',id='a',title='A',owner='A',outcome='FAIL',diff_excerpt=[secret],first_error='ERROR '+secret)
        summary,owners=report.summarize([item],['psql'])
        data=dict(schema=1,run_id='test',started_at='now',finished_at='later',harness={'commit':'abc','dirty':True},build={'commit':None,'build_identity':'unverified-binary','binary_sha256':'a'*64,'binary_size':10,'url':'postgres://u:'+secret+'@host/db'},oracle={'server_version':'18.6','collation':'C.UTF-8','timezone':'UTC','encoding':'UTF8'},clients={'psql':{'image':'postgres:18.6','digest':'sha256:test','version':'psql 18.6'}},checks=[item],summary=summary,failures_by_owner=owners,normalization_rules=checks.NORMALIZATION_RULES)
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            report.write(Path(directory),data,[secret])
            for name in ('report.json','report.md'):
                content=(Path(directory)/name).read_text()
                self.assertNotIn(secret,content)
                self.assertNotIn('postgres://',content)
            saved=json.loads((Path(directory)/'report.json').read_text())
            self.assertEqual(saved['summary']['clients']['psql']['giggles_pass_rate'],0)
            self.assertEqual(saved['build']['build_identity'],'unverified-binary')
            self.assertEqual(saved['build']['binary_size'],10)
            self.assertEqual(len(saved['build']['binary_sha256']),64)
            self.assertEqual(saved['summary']['totals']['FAIL'],1)
            self.assertEqual(saved['summary']['giggles_pass_rate'],0)
            self.assertEqual(saved['oracle']['encoding'],'UTF8')
            self.assertEqual(saved['clients']['psql']['version'],'psql 18.6')
            self.assertIn('| Client | Check | Owner | Outcome |',(Path(directory)/'report.md').read_text())
            self.assertIn('First GigglesDB error:',(Path(directory)/'report.md').read_text())
            markdown = (Path(directory)/'report.md').read_text()
            self.assertLess(markdown.index('Server binary SHA-256:'), markdown.index('Identity:'))
            self.assertLess(markdown.index('Oracle: PostgreSQL 18.6'), markdown.index('```json'))
            self.assertIn('psql -> psql 18.6', markdown)

    def test_client_command_and_timeout_cleanup(self):
        d=docker.Docker('r0-20261106T000000Z-abcdef',{'PGPASSWORD':'private-secret'})
        calls=[]
        def call(*args,**kwargs):
            calls.append((args,kwargs))
            return dict(exit=None,timed_out=True,stdout='',stderr='',duration_ms=1000)
        d.call=call
        removed=[]
        d.remove_client=removed.append
        pin=pins.load()['clients']['psql']
        name=d.prefix+'c-1'
        result=psql_client.run(d,pin,'network',name,'host.docker.internal',1234,'SELECT 1;',1)
        self.assertTrue(result['timed_out'])
        self.assertEqual(removed,[name])
        args,kwargs=calls[0]
        self.assertNotIn('private-secret',str(args))
        self.assertIn('PGPASSWORD',args)
        self.assertIn('--tmpfs',args)
        self.assertEqual(kwargs['input'],'SELECT 1;')

if __name__ == '__main__':
    unittest.main()
