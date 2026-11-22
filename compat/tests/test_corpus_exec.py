import os
import random
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock
import time
from unittest.mock import Mock, patch

import pytest

import harness
from compat_lib import corpus, docker, pins, psql_client


RUN_ID = 'r0-20261106T000000Z-abcdef'
PIN = pins.load()['clients']['psql']


def case(case_id='sample', *sql, area='pg_queries'):
    return dict(id=case_id, area=area, title='Sample', owner='A', expect='compatible',
                statements=[dict(index=index, sql=text, expected_error=False)
                            for index, text in enumerate(sql or ('SELECT 1;',), 1)])


def result(exit=0, *, stderr='', timed_out=False):
    return dict(exit=exit, stdout='', stderr=stderr, timed_out=timed_out, duration_ms=15)


@pytest.mark.parametrize('area', ['pg_limits', 'pg_roles', 'pg_txn', 'known_gaps', 'session',
                                  'transactions', 'datagrip'])
def test_serial_areas(area):
    assert corpus.needs_serial(case(area=area)) == (True, 'area ' + area)


@pytest.mark.parametrize('sql', [
    'gRaNt SELECT ON r0c_sample TO guest;',
    'SELECT * FROM pg_catalog.pg_class;',
    'SELECT * FROM "pg_stat_activity";',
    'SELECT pg_try_advisory_lock(1);',
    "PREPARE /* split */ TRANSACTION 'sample';",
    'SELECT pg_current_xact_id();',
    "SELECT nextval('sequence_shared_by_cases');",
    'CREATE SEQUENCE r0c_sample;',
    'CREATE TABLE someone_elses_table (id int PRIMARY KEY);',
    'CREATE INDEX r0c_sample_ix ON someone_elses_table (id);',
    'DROP TABLE r0c_sample, shared_table;',
    "COPY r0c_sample FROM PROGRAM 'echo 1';",
    "COPY r0c_sample TO '/tmp/sample';",
    'VACUUM r0c_sample;',
    'DELETE FROM shared_table;',
    'SELECT * FROM shared_sequence;',
    'CREATE TABLE r0c_sample (id int CONSTRAINT shared_index PRIMARY KEY);',
    "COPY r0c_sample TO E'/tmp/sample';",
])
def test_serial_statements(sql):
    serial, reason = corpus.needs_serial(case('sample', sql))
    assert serial and reason


def test_parallel_table_case():
    assert corpus.needs_serial(case(
        'sample', 'CREATE TABLE r0c_sample (id int PRIMARY KEY);',
        'INSERT INTO r0c_sample VALUES (1);', 'CREATE INDEX r0c_sample_idx ON r0c_sample (id);',
        'CREATE VIEW r0c_sample_view AS SELECT id FROM r0c_sample;',
        'COPY (SELECT id FROM r0c_sample) TO STDOUT;',
        'DROP VIEW IF EXISTS r0c_sample_view;', 'DROP TABLE r0c_sample;')) == (False, None)
    assert corpus.needs_serial(case('sample', 'SELECT user_value, role_preferences;')) == (False, None)


def mock_engine(effects):
    engine = docker.Docker(RUN_ID, {})
    engine.call = Mock(side_effect=effects)
    return engine


def exec_session(engine, app='r0:corpus:pg_queries/sample'):
    return psql_client.run_session(engine, PIN, 'net', 'unused', 'host', 5432,
                                   'SELECT 1;', 120, client_env={'PGAPPNAME': app})


@pytest.mark.parametrize('exit', [137, 124])
def test_exec_timeout_keeps_client_for_later_sessions(exit):
    engine = mock_engine([result(), result(exit), result()])
    assert exec_session(engine) == dict(result(exit), timed_out=True)
    assert exec_session(engine) == result()
    start, run, subsequent = engine.call.call_args_list
    assert len(engine.containers) == 1
    name, = engine.containers
    assert name.startswith(engine.prefix)
    assert start.args == ('run', '-d', '--name', name, '--network', 'net', '--tmpfs',
                          '/var/lib/postgresql', '--add-host', 'host.docker.internal:host-gateway',
                          '-e', 'PGPASSWORD', '-e', 'PGSSLMODE=disable', '-e', 'PGCONNECT_TIMEOUT=10',
                          '--entrypoint', 'sleep', pins.reference(PIN), 'infinity')
    assert run.args == ('exec', '-i', '-e', 'PGAPPNAME=r0:corpus:pg_queries/sample', name,
                        'timeout', '-s', 'KILL', '120', 'psql', '-X', '-v', 'ON_ERROR_STOP=0',
                        '-h', 'host', '-p', '5432', '-U', 'postgres', '-d', 'postgres', '-f', '-')
    assert run.kwargs == dict(input='SELECT 1;', timeout=135)
    assert subsequent.args == run.args


@pytest.mark.parametrize('failure', [result(None, timed_out=True),
                                     result(1, stderr='Error response from daemon: No such container: old'),
                                     result(1, stderr='Error response from daemon: Container old is not running')])
def test_exec_replaces_unusable_container_without_replaying_sql(failure):
    engine = mock_engine([result(), failure, result(), result(), result()])
    assert exec_session(engine) == failure
    assert exec_session(engine) == result()
    start, first, remove, replacement, second = engine.call.call_args_list
    old, new = start.args[3], replacement.args[3]
    assert old != new
    assert remove.args == ('rm', '-f', old)
    assert first.args[4] == old and second.args[4] == new
    assert engine.containers == [new]


def test_jobs_validation():
    assert harness.parser().parse_args(['run']).jobs == 1
    with pytest.raises(SystemExit) as exc:
        harness.parser().parse_args(['run', '--jobs', '0'])
    assert exc.value.code == 2


@pytest.mark.parametrize('jobs', [1, 4])
def test_corpus_report_order(jobs):
    cases = [case('first'), case('role', area='pg_roles'), case('second'), case('third')]
    expected = [item['id'] for item in cases]
    delays = dict(zip(expected, random.Random(71).sample(range(1, 10), len(cases))))
    finished, lock, second_done = [], Lock(), Event()

    def execute(_engine, _client, _pin, item, _available, _reason, local, *_,
                cleanup_per_side):
        assert cleanup_per_side
        case_id = item['id']
        time.sleep(delays[case_id] / 1000)
        if jobs > 1 and case_id == 'first':
            assert second_done.wait(3)
        with lock:
            finished.append(case_id)
        if case_id == 'second':
            second_done.set()
            local['networking']['status'] = 'reached-server'
        local['differential']['cases'].append(dict(id=case_id))

    data = dict(networking=dict(status='unverified'), differential=dict(cases=[]))
    with patch.object(harness, 'run_corpus_case', side_effect=execute):
        harness.run_corpus(Mock(), 'psql', PIN, cases, True, data, Path('.'), 'net', 'pg', 1,
                           'secret', jobs)
    assert [item['id'] for item in data['differential']['cases']] == expected
    assert data['networking']['status'] == 'reached-server'
    schedule = data['differential']['schedule']
    assert schedule['jobs'] == jobs
    if jobs == 1:
        assert finished == schedule['serial'] == expected
        assert schedule['parallel'] == []
    else:
        assert finished[-1] == 'role'
        assert finished.index('second') < finished.index('first')
        assert schedule['serial'] == ['role']
        assert schedule['parallel'] == ['first', 'second', 'third']
        assert schedule['reasons'] == {'role': 'area pg_roles'}


def test_cleanup_runs_immediately_on_each_side():
    item = case('sample', 'CREATE TABLE r0c_sample (id int PRIMARY KEY);')
    output = dict(result(), stdout='@@R0 BEGIN 1\n@@R0 END 1 00000 0\n')
    runner = Mock(return_value=output)
    data = dict(networking={}, differential=dict(cases=[]))
    with (TemporaryDirectory(dir=os.environ['TEMP']) as tmp,
          patch.dict(psql_client.REGISTRY['psql'], run_session=runner)):
        harness.run_corpus(mock_engine([]), 'psql', PIN, [item], True, data, Path(tmp),
                           'net', 'pg', 1, 'secret', 4)
    assert [(call.args[4], call.kwargs['client_env']['PGAPPNAME'].split(':')[1])
            for call in runner.call_args_list] == [
                ('pg', 'corpus'), ('pg', 'cleanup'),
                ('host.docker.internal', 'corpus'), ('host.docker.internal', 'cleanup')]


@pytest.mark.parametrize('failure', [RuntimeError('failed case'), KeyboardInterrupt()])
def test_run_always_removes_exec_client_even_with_keep(failure):
    engine = mock_engine(lambda *_args, **_kwargs: result())
    server = Mock(process=None, meta={})

    def fail_during_exec(*_args):
        engine.corpus_client(pins.reference(PIN), 'net')
        raise failure

    with (TemporaryDirectory(dir=os.environ['TEMP']) as tmp,
          patch.object(harness.docker, 'Docker', return_value=engine),
          patch.object(harness.oracle, 'start', return_value=('net', 'pg', {})),
          patch.object(harness.gigglesdb, 'validate_build_worktree'),
          patch.object(harness.gigglesdb, 'Server', return_value=server),
          patch.object(harness.corpus, 'discover_cases', return_value=[dict(case(), path='sample.sql')]),
          patch.object(harness, 'run_corpus', side_effect=fail_during_exec)):
        args = harness.parser().parse_args(['run', '--server-bin', sys.executable,
                                           '--out-root', tmp, '--clients', 'psql', '--no-checks', '--keep'])
        assert harness._run(args, []) == 2
    assert engine.containers == [engine.prefix + 'version-psql']
    engine.call.assert_any_call('rm', '-f', engine.prefix + 'corpus-client-1')

