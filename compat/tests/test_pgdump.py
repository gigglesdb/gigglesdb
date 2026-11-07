import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from tests.task_storage import ModuleStorage

from compat_lib import pgdump, util

_STORAGE = ModuleStorage()


def setUpModule():
    _STORAGE.start()


def tearDownModule():
    _STORAGE.finish()


class PgDumpTests(unittest.TestCase):
    def test_no_checks_skips_all_client_work(self):
        engine = Mock()
        data = dict(clients={}, checks=[], networking={})
        pgdump.run_client(engine, 'pg_dump', {}, SimpleNamespace(no_checks=True), data,
                          _STORAGE.root/'disabled', 'network', 'oracle', 15432, 'secret')
        self.assertEqual(engine.mock_calls, [])
        self.assertEqual(data['checks'], [])
        self.assertEqual(data['clients']['pg_dump']['reason'], 'checks disabled')
        self.assertFalse((_STORAGE.root/'disabled').exists())

    def test_command_preserves_requested_newlines(self):
        with patch('subprocess.run', return_value=SimpleNamespace(returncode=0, stdout=b'a\r\nb\n', stderr=b'')) as process:
            result = util.command(['client'], {}, input='SELECT é;', preserve_newlines=True)
            self.assertEqual(result['stdout'], 'a\r\nb\n')
            self.assertEqual(process.call_args.kwargs['input'], 'SELECT é;'.encode('utf-8'))
            self.assertNotIn('text', process.call_args.kwargs)
        with patch('subprocess.run', return_value=SimpleNamespace(returncode=0, stdout='a\nb\n', stderr='')) as process:
            self.assertEqual(util.command(['client'], {}, input='SELECT 1;')['stdout'], 'a\nb\n')
            self.assertTrue(process.call_args.kwargs['text'])
            self.assertEqual(process.call_args.kwargs['input'], 'SELECT 1;')

    def test_normalization_preserves_structure(self):
        a = '-- comment\n\\restrict random\nSET x = 1;\n\nCREATE TABLE x (i integer);\n\\unrestrict random\n'
        self.assertEqual(pgdump.normalize_schema(a), 'CREATE TABLE x (i integer);')
        self.assertNotEqual(pgdump.normalize_schema(a), pgdump.normalize_schema(a.replace('integer', 'bigint')))

    def test_statement_locations(self):
        sql = "SET x = 1;\nCREATE TABLE x (s text DEFAULT ';');\nDO $$ BEGIN PERFORM ';'; END $$;\nCOPY x FROM stdin;\na;b\n\\.\nSET unsupported = 1;\n"
        self.assertEqual(pgdump.failing_statement(sql, 'psql:<stdin>:7: ERROR: bad'), 'SET unsupported = 1;')
        self.assertIn('a;b', pgdump.failing_statement(sql, 'psql:<stdin>:6: ERROR: bad'))
        self.assertIn('DO $$', pgdump.failing_statement(sql, 'psql:<stdin>:3: ERROR: bad'))

    def test_errors_and_classification(self):
        good = dict(exit=0, timed_out=False)
        bad = dict(exit=1, timed_out=False)
        self.assertEqual(pgdump.classify(bad, good), 'HARNESS_ERROR')
        self.assertEqual(pgdump.classify(good, bad), 'FAIL')
        self.assertEqual(pgdump.classify(good, good, 'dump failed'), 'FAIL')
        self.assertEqual(pgdump.first_error('warning\npsql: FATAL: broken\nERROR: second'), 'psql: FATAL: broken')

    def test_orchestration_and_failed_dump_dependencies(self):
        engine = Mock(prefix='gd-r0-test-', containers=[])
        engine.image.return_value = True

        def call(*args, **kwargs):
            failed = 'pg_dump' in args and 'host.docker.internal' in args and '--version' not in args
            stdout = 'CREATE TABLE public.r0p_users (id integer);\n' if 'pg_dump' in args else ''
            return dict(exit=1 if failed else 0, stdout=stdout, stderr='ERROR: unsupported' if failed else '', timed_out=False, duration_ms=1)

        engine.call.side_effect = call
        data = dict(clients={}, checks=[], networking={})
        root = _STORAGE.root/'failed-dump'
        root.mkdir()
        pgdump.run_client(engine, 'pg_dump', {}, SimpleNamespace(client_timeout=60, pull=False, owners=' G, E1 ', checks='*'), data, root, 'network', 'oracle', 15432, 'secret')
        checks = {row['id']: row for row in data['checks']}
        self.assertEqual(len(checks), 7)
        self.assertEqual(checks['dump_schema_runs']['outcome'], 'FAIL')
        self.assertEqual(checks['dump_schema_restores_into_pg']['outcome'], 'FAIL')
        self.assertEqual(checks['dump_full_restores_into_pg']['outcome'], 'FAIL')
        self.assertEqual(sum('--version' in c.args for c in engine.call.call_args_list), 1)
        for c in engine.call.call_args_list:
            self.assertIn('--rm', c.args)
            self.assertIn('--network', c.args)
            self.assertIn('PGPASSWORD', c.args)
            self.assertNotIn('PGPASSWORD=secret', c.args)
            self.assertEqual(c.kwargs['timeout'], 60)
        self.assertEqual(len(engine.containers), len(set(engine.containers)))

    def test_partial_fixture_still_dumps_and_redumps_restore(self):
        engine = Mock(prefix='gd-r0-partial-', containers=[])
        engine.image.return_value = True

        def call(*args, **kwargs):
            fixture_failed = 'psql' in args and 'host.docker.internal' in args and 'CREATE TABLE r0p_users' in (kwargs.get('input') or '')
            token = 'restored' if 'r0_restore_schema' in args else 'original'
            stdout = '-- comment\n\\restrict '+token+'\nCREATE TABLE public.r0p_users (id integer);\n\\unrestrict '+token+'\n' if 'pg_dump' in args else ''
            if (kwargs.get('input') or '').startswith('SELECT * FROM r0p_users'):
                stdout = 'id\r\n1\r\n' if 'r0_restore_full' in args else 'id\n1\n'
            return dict(exit=1 if fixture_failed else 0, stdout=stdout, stderr='ERROR: fixture partial' if fixture_failed else '', timed_out=False, duration_ms=1)

        engine.call.side_effect = call
        data = dict(clients={}, checks=[], networking={})
        root = _STORAGE.root/'partial-fixture'
        root.mkdir()
        pgdump.run_client(engine, 'pg_dump', {}, SimpleNamespace(client_timeout=60, pull=False, owners='G,E1', checks='*'), data, root, 'network', 'oracle', 15432, 'secret')
        checks = {row['id']: row for row in data['checks']}
        self.assertEqual(checks['fixture_apply']['outcome'], 'FAIL')
        for cid in ('dump_schema_runs', 'dump_data_runs', 'dump_schema_restores_into_pg', 'dump_schema_equivalent'):
            self.assertEqual(checks[cid]['outcome'], 'PASS', cid)
        self.assertTrue(any('pg_dump' in c.args and 'r0_restore_schema' in c.args for c in engine.call.call_args_list))
        self.assertEqual(checks['dump_schema_equivalent']['diff_excerpt'], [])
        self.assertEqual(checks['dump_full_restores_into_pg']['outcome'], 'FAIL')
        self.assertIn('rows differ', checks['dump_full_restores_into_pg']['reason'])
        artifact = root/'checks'/'pg_dump'/'dump_full_restores_into_pg'/'giggles.stdout.txt'
        self.assertEqual(artifact.read_bytes(), b'id\r\n1\r\n')


if __name__ == '__main__':
    unittest.main()
