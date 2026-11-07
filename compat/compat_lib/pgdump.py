"""Pinned plain-SQL pg_dump qualification, including independent PG baselines."""
import difflib
import fnmatch
import json
from pathlib import Path
import re

from . import pins, util

CASES = (
    ('fixture_apply', 'E1'), ('dump_schema_runs', 'G'),
    ('dump_schema_restores_into_pg', 'G'), ('dump_schema_equivalent', 'G'),
    ('dump_data_runs', 'G'), ('dump_full_restores_into_pg', 'G'),
    ('pg_dump_restores_into_giggles', 'E1'),
)


def normalize_schema(sql):
    """Ignore dump comments, SET prologue, whitespace-only and PG18 guard lines.

    Guard tokens are random per invocation. SQL definitions stay byte-for-byte.
    """
    return '\n'.join(line.rstrip() for line in sql.replace('\r\n', '\n').splitlines()
                     if line.strip() and not re.match(r'^\s*(--|SET\s|\\(?:un)?restrict\s)', line))


def first_error(stderr):
    return next((line for line in stderr.splitlines() if re.search(r'\b(?:ERROR|FATAL)\b', line, re.I)), None)


def sql_statements(sql):
    """Yield (start line, end line, SQL), respecting strings/comments/COPY data."""
    start = 0
    i = 0
    quote = None
    block = 0
    while i < len(sql):
        if quote:
            if sql.startswith(quote, i):
                if quote in ("'", '"') and sql.startswith(quote * 2, i):
                    i += 2
                    continue
                i += len(quote)
                quote = None
                continue
            if quote == "'" and sql[i] == '\\':
                i += 2
            else:
                i += 1
            continue
        if block:
            if sql.startswith('/*', i):
                block += 1
                i += 2
            elif sql.startswith('*/', i):
                block -= 1
                i += 2
            else:
                i += 1
            continue
        if sql.startswith('--', i):
            end = sql.find('\n', i)
            i = len(sql) if end < 0 else end + 1
            continue
        if sql.startswith('/*', i):
            block = 1
            i += 2
            continue
        if sql[i] in "'\"":
            quote = sql[i]
            i += 1
            continue
        dollar = re.match(r'\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$', sql[i:])
        if dollar:
            quote = dollar[0]
            i += len(quote)
            continue
        if sql[i] == ';':
            end = i + 1
            text = sql[start:end]
            if re.search(r'\bCOPY\b[\s\S]*\bFROM\s+stdin\s*;', text, re.I):
                terminator = re.search(r'(?m)^\\\.\r?$', sql[end:])
                if terminator:
                    end += terminator.end()
            yield sql.count('\n', 0, start) + 1, sql.count('\n', 0, end) + 1, sql[start:end].strip()
            start = end
            i = end
            continue
        # pg_dump psql guard commands are standalone, not SQL statements.
        if sql[i] == '\\' and (i == 0 or sql[i-1] == '\n'):
            end = sql.find('\n', i)
            end = len(sql) if end < 0 else end + 1
            yield sql.count('\n', 0, start) + 1, sql.count('\n', 0, end), sql[start:end].strip()
            start = end
            i = end
            continue
        i += 1
    if sql[start:].strip():
        yield sql.count('\n', 0, start) + 1, sql.count('\n') + 1, sql[start:].strip()


def failing_statement(sql, stderr):
    match = re.search(r'psql:(?:[^\n]*?):(\d+):\s*(?:ERROR|FATAL)', stderr)
    if not match:
        return None
    line = int(match[1])
    return next((text for start, end, text in sql_statements(sql) if start <= line <= end), None)


def ok(result):
    return result is not None and result['exit'] == 0 and not result['timed_out']


def classify(pg, giggles, dependency=None):
    if not ok(pg):
        return 'HARNESS_ERROR'
    if dependency or not ok(giggles):
        return 'FAIL'
    return 'PASS'


def run_client(engine, client, registration, args, data, run, network, pg_host, port, password):
    if getattr(args, 'no_checks', False):
        data['clients'][client] = dict(available=False, version=None, reason='checks disabled')
        return
    root = run/'checks'/client
    root.mkdir(parents=True, exist_ok=True)
    pin = pins.load()['clients']['psql']
    timeout = args.client_timeout
    available = engine.image(pins.reference(pin), args.pull)
    data['clients'][client] = dict(pin, available=available, version=None)
    calls = {}

    def invoke(label, check, side='pg', sql=None, database='postgres', mode=None, table_prefix='r0p_'):
        name = engine.prefix+'c-'+str(len(engine.containers))
        engine.containers.append(name)
        command = ['run', '--rm', '-i', '--name', name, '--network', network,
                   '--tmpfs', '/var/lib/postgresql', '--add-host', 'host.docker.internal:host-gateway',
                   '-e', 'PGPASSWORD', '-e', 'PGSSLMODE=disable', '-e', 'PGCONNECT_TIMEOUT=10',
                   '-e', 'PGAPPNAME=r0:pg_dump:'+check, pins.reference(pin)]
        if mode == 'version':
            command += ['pg_dump', '--version']
        else:
            command += ['pg_dump' if mode else 'psql', '-h', pg_host if side == 'pg' else 'host.docker.internal',
                        '-p', '5432' if side == 'pg' else str(port), '-U', 'postgres', '-d', database]
            if mode:
                command += ['--no-owner', '--no-privileges', '--table', 'public.'+table_prefix+'*']
                if mode in ('schema', 'data'):
                    command += ['--schema-only' if mode == 'schema' else '--data-only']
            else:
                command += ['-X', '--csv', '-v', 'ON_ERROR_STOP=1', '-f', '-']
        result = engine.call(*command, input=sql, timeout=timeout, preserve_newlines=True)
        if result['timed_out']:
            engine.remove_client(name)
        calls[label] = (result, sql)
        artifact = root if mode == 'version' else root/check
        artifact.mkdir(parents=True, exist_ok=True)
        for stream in ('stdout', 'stderr'):
            (artifact/(label+'.'+stream+'.txt')).write_bytes(util.redact(result[stream], [password]).encode('utf-8'))
        return result

    if not available:
        rows = [dict(id=cid, title=cid, owner=owner, outcome='UNTESTED', reason='pinned pg_dump image unavailable') for cid, owner in CASES]
    else:
        version = invoke('version', 'version', mode='version')
        data['clients'][client]['version'] = version['stdout'].strip() if ok(version) else None
        fixture = (Path(__file__).parent.parent/'pgdump'/'fixture.sql').read_text(encoding='utf-8')
        pg_fixture = invoke('fixture.pg', 'fixture_apply', sql=fixture)
        gd_fixture = invoke('fixture.giggles', 'fixture_apply', 'giggles', sql=fixture)
        dumps = {}
        for mode, cid in [('schema', 'dump_schema_runs'), ('data', 'dump_data_runs'), ('full', 'dump_full_restores_into_pg')]:
            for side in ('pg', 'giggles'):
                dumps[mode, side] = invoke(mode+'.'+side, cid, side, mode=mode)
        rows = []

        def add(cid, pg, gd, reason=None, different=False, statement_sql=None):
            outcome = classify(pg, gd, reason)
            if outcome == 'PASS' and different:
                outcome = 'FAIL'
                reason = 'restored output differs from PostgreSQL baseline'
            row = dict(id=cid, title=cid, owner=dict(CASES)[cid], outcome=outcome,
                       pg=pg, giggles=gd, first_error=first_error((gd or {}).get('stderr', '')),
                       first_failing_statement=failing_statement(statement_sql or '', (gd or {}).get('stderr', '')))
            if reason:
                row['reason'] = reason
            rows.append(row)

        add('fixture_apply', pg_fixture, gd_fixture, statement_sql=fixture)
        for mode, cid in [('schema', 'dump_schema_runs'), ('data', 'dump_data_runs')]:
            add(cid, dumps[mode, 'pg'], dumps[mode, 'giggles'])
        schema_restore = None
        for mode, cid in [('schema', 'dump_schema_restores_into_pg'), ('full', 'dump_full_restores_into_pg')]:
            restores = {}
            setup_failure = None
            for side in ('pg', 'giggles'):
                database = 'r0_restore_'+mode+('_baseline' if side == 'pg' else '')
                created = invoke(mode+'.create.'+side, cid, sql='CREATE DATABASE '+database+';')
                if not ok(created):
                    setup_failure = created
                dump = dumps[mode, side]
                if ok(created) and ok(dump) and dump['stdout'].strip():
                    restores[side] = invoke(mode+'.restore.'+side, cid, sql=dump['stdout'], database=database)
                else:
                    restores[side] = created if not ok(created) else None
            reason = None if ok(dumps[mode, 'giggles']) else 'dependency GigglesDB dump failed; restore skipped'
            add(cid, restores['pg'], restores['giggles'], reason, statement_sql=dumps[mode, 'giggles']['stdout'])
            if reason:
                rows[-1]['first_error'] = first_error(dumps[mode, 'giggles']['stderr'])
            if setup_failure is not None:
                rows[-1].update(outcome='HARNESS_ERROR', reason='PostgreSQL restore database setup failed', pg=setup_failure)
            if mode == 'schema':
                schema_restore = dict(rows[-1])
            if mode == 'full' and rows[-1]['outcome'] == 'PASS' and all(ok(r) for r in restores.values()):
                query = '\n'.join('SELECT * FROM '+table+' ORDER BY 1;' for table in ('r0p_users', 'r0p_orders', 'r0p_blobs'))
                compared = {side: invoke('full.rows.'+side, cid, sql=query, database='postgres' if side == 'pg' else 'r0_restore_full') for side in ('pg', 'giggles')}
                rows[-1].update(outcome=classify(compared['pg'], compared['giggles']), pg=compared['pg'], giggles=compared['giggles'],
                                first_error=first_error(compared['giggles']['stderr']), first_failing_statement=failing_statement(query, compared['giggles']['stderr']))
                if rows[-1]['outcome'] == 'PASS' and compared['pg']['stdout'] != compared['giggles']['stdout']:
                    rows[-1].update(outcome='FAIL', reason='restored rows differ from PostgreSQL baseline')
        if schema_restore['outcome'] == 'PASS':
            restored_schema = invoke('schema.redump.pg', 'dump_schema_equivalent', database='r0_restore_schema', mode='schema')
            add('dump_schema_equivalent', dumps['schema', 'pg'], restored_schema,
                different=normalize_schema(dumps['schema', 'pg']['stdout']) != normalize_schema(restored_schema['stdout']))
        else:
            add('dump_schema_equivalent', schema_restore['pg'], None, 'dependency dump_schema_restores_into_pg failed')
            rows[-1].update(outcome=schema_restore['outcome'], first_error=schema_restore['first_error'],
                            first_failing_statement=schema_restore['first_failing_statement'])
        # A distinct PG source namespace avoids colliding with the GD fixture.
        source = fixture.replace('r0p_', 'r0q_')
        baseline_create = invoke('source.create', 'pg_dump_restores_into_giggles', sql='CREATE DATABASE r0_src;')
        src_apply = invoke('source.fixture', 'pg_dump_restores_into_giggles', sql=source, database='r0_src') if ok(baseline_create) else baseline_create
        src_dump = invoke('source.dump', 'pg_dump_restores_into_giggles', database='r0_src', mode='full', table_prefix='r0q_')
        restore_sql = src_dump['stdout']
        baseline_create = invoke('source.validation.create', 'pg_dump_restores_into_giggles', sql='CREATE DATABASE r0_src_validation;')
        baseline = invoke('source.restore.pg', 'pg_dump_restores_into_giggles', sql=restore_sql, database='r0_src_validation') if ok(baseline_create) and ok(src_dump) and restore_sql.strip() else None
        gd_restore = invoke('source.restore.giggles', 'pg_dump_restores_into_giggles', 'giggles', sql=restore_sql) if ok(src_dump) and restore_sql.strip() else None
        add('pg_dump_restores_into_giggles', baseline if ok(src_apply) else src_apply, gd_restore, statement_sql=restore_sql)
        if ok(baseline) and ok(gd_restore):
            query = '\n'.join('SELECT * FROM '+table+' ORDER BY 1;' for table in ('r0q_users', 'r0q_orders', 'r0q_blobs'))
            pg_rows = invoke('source.rows.pg', 'pg_dump_restores_into_giggles', sql=query, database='r0_src')
            gd_rows = invoke('source.rows.giggles', 'pg_dump_restores_into_giggles', 'giggles', sql=query)
            rows[-1].update(outcome=classify(pg_rows, gd_rows), pg=pg_rows, giggles=gd_rows,
                            first_error=first_error(gd_rows['stderr']), first_failing_statement=failing_statement(query, gd_rows['stderr']))
            if rows[-1]['outcome'] == 'PASS' and pg_rows['stdout'] != gd_rows['stdout']:
                rows[-1].update(outcome='FAIL', reason='restored rows differ from PostgreSQL source')
        if not ok(version):
            for row in rows:
                row.update(outcome='HARNESS_ERROR', reason='pg_dump --version failed')
        if not ok(pg_fixture):
            for row in rows:
                if row['id'] != 'pg_dump_restores_into_giggles':
                    row.update(outcome='HARNESS_ERROR', reason='PostgreSQL fixture baseline failed', pg=pg_fixture)
    by_id = {row['id']: row for row in rows}
    owners = {part.strip() for part in args.owners.split(',') if part.strip()}
    for cid, _ in CASES:
        row = by_id[cid]
        directory = root/cid
        directory.mkdir(parents=True, exist_ok=True)
        row.update(client=client, artifact_dir=str(directory))
        row.setdefault('first_error', None)
        row.setdefault('first_failing_statement', None)
        row.setdefault('pg', None)
        row.setdefault('giggles', None)
        pg_text = (row['pg'] or {}).get('stdout', '')
        gd_text = (row['giggles'] or {}).get('stdout', '')
        if cid == 'dump_schema_equivalent':
            pg_text, gd_text = normalize_schema(pg_text), normalize_schema(gd_text)
        difference = '\n'.join(difflib.unified_diff(pg_text.splitlines(), gd_text.splitlines(), fromfile='pg', tofile='giggles', lineterm=''))
        row['diff_excerpt'] = difference.splitlines()[:40]
        clean = util.redact(row, [password])
        (directory/'result.json').write_text(json.dumps(clean, indent=2)+'\n', encoding='utf-8')
        (directory/'diff.txt').write_text(util.redact(difference, [password]), encoding='utf-8')
        for side in ('pg', 'giggles'):
            for stream in ('stdout', 'stderr'):
                (directory/(side+'.'+stream+'.txt')).write_bytes(util.redact((row[side] or {}).get(stream, ''), [password]).encode('utf-8'))
        if row['owner'] in owners and (fnmatch.fnmatch(cid, args.checks) or fnmatch.fnmatch(cid+'.json', args.checks)):
            data['checks'].append(clean)
        if ok(row['giggles']) or row['first_error']:
            data['networking']['status'] = 'reached-server'
