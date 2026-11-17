#!/usr/bin/env python3
"""Mirror cluster_isolation.rs: queued session steps, bounded waits, text outcomes.

The JSON is the shared contract for the JDBC runner. Keep SQL as one simple query
per step (including batches); collect every result set before encoding the step.
Findings are measured separately and never change the overall pass result.
"""
import argparse
import json
import queue
import re
import secrets
import threading
import time
from pathlib import Path

import psycopg


def admitted(spec, level, strict=False):
    parts = spec.split(';')
    assert len(parts) in (1, 3), spec
    choices = parts[level if len(parts) == 3 else 0].split(' or ')
    return choices[:1] if strict else choices


def suffixed(sql, tables, suffix):
    # Same word/quote scanner as Rust, including its literal-quote semantics.
    out, word, quoted = [], '', False
    for char in sql + ' ':
        if not quoted and (char.isascii() and (char.isalnum() or char == '_')):
            word += char
            continue
        out.append(word + (suffix if word in tables else ''))
        word = ''
        quoted ^= char == "'"
        out.append(char)
    return ''.join(out)[:-1]


def simple(conn, sql):
    """ClientCursor uses PQsendQuery; pgresult values retain server text spelling."""
    rows, count, first = [], 0, True
    with conn.cursor() as cursor:
        cursor.execute(sql, prepare=False)
        while True:
            result = cursor.pgresult
            if result is not None:
                for row in range(result.ntuples):
                    rows.append('|'.join(
                        (result.get_value(row, col) or b'').decode('utf-8')
                        for col in range(result.nfields)))
                if first:
                    count = result.command_tuples or 0
                    first = False
            if not cursor.nextset():
                break
    return rows, count


class Session:
    def __init__(self, conn, form):
        self.conn, self.form = conn, form
        self.block = self.failed = False

    def step(self, sql):
        verb = sql.split()[0] if sql.split() else ''
        queries = [sql]
        if sql.startswith('BEGIN ISOLATION LEVEL ') and self.form != 'begin':
            rest = sql.removeprefix('BEGIN ISOLATION LEVEL ')
            level = rest.removesuffix(' READ ONLY')
            transaction = f'SET TRANSACTION ISOLATION LEVEL {rest}'
            if self.form == 'session':
                queries = [f'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}',
                           'BEGIN READ ONLY' if rest.endswith(' READ ONLY') else 'BEGIN']
            elif self.form == 'set_transaction':
                queries = ['BEGIN', transaction]
            else:
                queries = [f'BEGIN; {transaction}']
        ok = True
        try:
            for query in queries:
                rows, count = simple(self.conn, query)
            if verb == 'SELECT':
                out = ','.join(rows)
            elif verb == 'BEGIN':
                out = 'BEGIN'
            elif verb == 'COMMIT':
                out = 'ROLLBACK' if self.failed else 'COMMIT'
            elif verb == 'INSERT':
                out = f'INSERT 0 {count}'
            else:
                out = f'{verb} {count}'
        except psycopg.Error as exc:
            if not exc.sqlstate or exc.sqlstate.startswith('08') or exc.sqlstate in {
                '57P01', '57P02', '57P03',
            }:
                raise
            ok, out = False, f'E:{exc.sqlstate}'
        if verb == 'BEGIN':
            self.block, self.failed = ok, False
        elif verb in ('COMMIT', 'ROLLBACK'):
            self.block = self.failed = False
        else:
            self.failed |= self.block and not ok
        return out


def compare(result, label, spec, ours, level, strict):
    choices = admitted(spec, level, strict)
    result['transcript'].append(
        f'{label}: pg {json.dumps(choices[0])} ours {json.dumps(ours)}')
    if ours not in choices:
        result['mismatches'].append(f'{label}: admitted {json.dumps(choices)}, got {json.dumps(ours)}')


def record(name, level, form):
    return dict(case=name, level=level, form=form, transcript=[], mismatches=[])


def run_case(connect, case, level, form, data, run_id, strict, keep_tables=False):
    started = time.monotonic()
    result = record(case['name'], data['levels'][level], form)
    tables = [part.split()[0] for part in case['setup'].split('CREATE TABLE ')[1:]]
    suffix = f"_{run_id}_{case['name']}_{['rc', 'rr', 'ser'][level]}_{data['forms'].index(form)}"
    rewrite = lambda sql: suffixed(sql.replace('@LEVEL@', data['levels'][level]), tables, suffix)
    with connect(0) as conn:
        simple(conn, rewrite(case['setup']))
    done, inputs, connections, threads = queue.Queue(), {}, [], []
    outcomes, durations = {}, {}
    waited = [False] * len(case['steps'])

    def worker(conn, inbox):
        session = Session(conn, form)
        while (item := inbox.get()) is not None:
            index, sql = item
            begin = time.monotonic()
            try:
                out = session.step(sql)
            except Exception as exc:
                done.put((index, exc, time.monotonic() - begin))
                return
            done.put((index, out, time.monotonic() - begin))

    def receive(deadline):
        try:
            index, out, duration = done.get(timeout=max(0, deadline - time.monotonic()))
        except queue.Empty:
            return False
        if isinstance(out, Exception):
            raise out
        outcomes[index], durations[index] = out, duration
        return True

    try:
        for node, name in enumerate('ABC'):
            conn, inbox = connect(node), queue.Queue()
            connections.append(conn)
            inputs[name] = inbox
            thread = threading.Thread(target=worker, args=(conn, inbox), daemon=True)
            threads.append(thread)
            thread.start()
        for index, (name, sql, _) in enumerate(case['steps']):
            inputs[name].put((index, rewrite(sql)))
            deadline = time.monotonic() + data['step_wait_s']
            while index not in outcomes:
                if not receive(deadline):
                    waited[index] = True
                    break
        for inbox in inputs.values():
            inbox.put(None)
        deadline = time.monotonic() + data['end_wait_s']
        while len(outcomes) < len(case['steps']) and receive(deadline):
            pass
    finally:
        for inbox in inputs.values():
            inbox.put(None)
        for conn, thread in zip(connections, threads):
            if thread.is_alive():
                conn.cancel_safe(timeout=2)
            thread.join(timeout=5)
            conn.close()
            if thread.is_alive():
                raise RuntimeError('session worker did not stop after cancellation')
        for conn in connections[len(threads):]:
            conn.close()
    for index, (name, sql, spec) in enumerate(case['steps']):
        ours = ('~' if waited[index] else '') + outcomes.get(index, 'TIMEOUT')
        compare(result, f'[{index + 1}] {name}: {sql}', spec, ours, level, strict)
    with connect(0) as conn:
        try:
            ours = Session(conn, form).step(rewrite(case['fin'][0]))
        finally:
            if not keep_tables:
                for table in reversed(tables):
                    simple(conn, f'DROP TABLE IF EXISTS {table}{suffix} CASCADE')
    compare(result, f"final: {case['fin'][0]}", case['fin'][1], ours, level, strict)
    result.update(seconds=time.monotonic() - started,
                  step_seconds=[durations.get(i) for i in range(len(case['steps']))], waited=waited)
    return result


def reports_each_level(connect, data):
    result = record('reports_each_level', None, None)
    started = time.monotonic()
    current, default = 'SHOW transaction_isolation', 'SHOW default_transaction_isolation'
    with connect(1) as conn:
        def check(query, expected, context):
            rows, _ = simple(conn, query)
            compare(result, f'{context}: {query}', expected, rows[0] if rows else '', 0, True)
        check(current, 'read committed', 'initial')
        for level in data['levels']:
            for begin in [f'BEGIN ISOLATION LEVEL {level}',
                          f'BEGIN; SET TRANSACTION ISOLATION LEVEL {level}']:
                simple(conn, begin)
                check(current, level.lower(), begin)
                check(default, 'read committed', begin)
                simple(conn, 'ROLLBACK')
                check(current, 'read committed', f'{begin}; ROLLBACK')
            statement = f'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}'
            simple(conn, statement)
            check(current, level.lower(), statement)
            check(default, level.lower(), statement)
            simple(conn, 'BEGIN')
            check(current, level.lower(), f'{statement}; BEGIN')
            simple(conn, 'COMMIT')
            simple(conn, 'RESET default_transaction_isolation')
    result['seconds'] = time.monotonic() - started
    return result


def union_finding(connect, data, run_id, strict, keep_tables=False):
    result = record('foreign_key_union_all', None, 'begin')
    started = time.monotonic()
    finding = data['union_finding']
    rewrite = lambda sql: suffixed(sql, ['p', 'c'], f'_{run_id}_foreign_key_union_all')
    with connect(0) as conn:
        simple(conn, rewrite(finding['setup']))
        try:
            ours = Session(conn, 'begin').step(rewrite(finding['sql']))
        finally:
            if not keep_tables:
                for table in ('c', 'p'):
                    simple(conn, rewrite(f'DROP TABLE IF EXISTS {table} CASCADE'))
    compare(result, finding['sql'], finding['spec'], ours, 0, strict)
    result['seconds'] = time.monotonic() - started
    return result


def arguments(data):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--cluster', type=Path)
    source.add_argument('--target', choices=['pg'])
    parser.add_argument('--dsn')
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--cases', help='comma-separated case names')
    parser.add_argument('--forms', default=','.join(data['forms']))
    parser.add_argument('--nodes', default='1,2,3', help='three distinct cluster node ids for A,B,C')
    parser.add_argument('--run-id', default=secrets.token_hex(3))
    parser.add_argument('--keep-tables', action='store_true', help='retain case tables after final queries')
    parser.add_argument('--strict-pg', action='store_true', help='accept only the PostgreSQL alternative')
    args = parser.parse_args()
    args.cases = args.cases.split(',') if args.cases else [c['name'] for c in data['cases']]
    args.forms = args.forms.split(',')
    try:
        args.nodes = [int(node) for node in args.nodes.split(',')]
    except ValueError:
        parser.error('--nodes must contain integer ids')
    if len(args.nodes) != 3 or len(set(args.nodes)) != 3:
        parser.error('--nodes requires three distinct ids')
    if set(args.cases) - {c['name'] for c in data['cases']}:
        parser.error('unknown case name')
    if set(args.forms) - set(data['forms']) or len(set(args.forms)) != len(args.forms):
        parser.error('unknown or duplicate form')
    if not re.fullmatch(r'[a-zA-Z0-9_]{1,16}', args.run_id):
        parser.error('--run-id must be 1..16 ASCII letters, digits or underscores')
    if args.target == 'pg' and not args.dsn:
        parser.error('--target pg requires --dsn')
    return args


def connector(args):
    common = dict(autocommit=True, cursor_factory=psycopg.ClientCursor, connect_timeout=10)
    if args.target == 'pg':
        return lambda node: psycopg.connect(args.dsn, **common)
    cluster = json.loads(args.cluster.read_text(encoding='utf-8'))
    if cluster['version'] != 1:
        raise ValueError('unsupported cluster.json version')
    nodes = {node['id']: node for node in cluster['nodes']}
    selected = [nodes[node] for node in args.nodes]

    def connect(index):
        node = selected[index]
        # Keyword conninfo avoids URI quoting bugs, including IPv6 and passwords.
        return psycopg.connect(host=node['host'], port=node['pg_port'], user=cluster['owner'],
                               password=cluster['password'], dbname=cluster['database'], **common)
    return connect


def main():
    data = json.loads(Path(__file__).with_name('anomaly_cases.json').read_text(encoding='utf-8'))
    args = arguments(data)
    args.out.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    output = dict(source=data['source'], run_id=args.run_id, strict_pg=args.strict_pg,
                  target=args.target or 'cluster', nodes=args.nodes, psycopg=psycopg.__version__,
                  runs=[], findings=[], reports_each_level=None, infrastructure=[])
    try:
        connect = connector(args)
        output['reports_each_level'] = reports_each_level(connect, data)
        findings = {(name, level) for name, level in data['findings']}
        for form in args.forms:
            for case in data['cases']:
                if case['name'] not in args.cases or (form != 'begin' and case['name'] not in data['again']):
                    continue
                for level in range(len(data['levels'])):
                    result = run_case(connect, case, level, form, data, args.run_id, args.strict_pg, args.keep_tables)
                    bucket = 'findings' if (case['name'], level) in findings else 'runs'
                    output[bucket].append(result)
                    print(f"{case['name']} / {result['level']} / {form}: "
                          f"{len(result['mismatches'])} mismatch(es), {result['seconds']:.3f}s", flush=True)
        if 'begin' in args.forms and any(name in args.cases for name, _ in findings):
            output['findings'].append(union_finding(connect, data, args.run_id, args.strict_pg, args.keep_tables))
    except Exception as exc:
        output['infrastructure'].append(f'{type(exc).__name__}: {exc}')
    output['seconds'] = time.monotonic() - started
    checks = output['runs'] + ([output['reports_each_level']] if output['reports_each_level'] else [])
    mismatches = sum(len(result['mismatches']) for result in checks)
    finding_mismatches = sum(len(result['mismatches']) for result in output['findings'])
    output['pass'] = not output['infrastructure'] and mismatches == 0
    status = 2 if output['infrastructure'] else (0 if output['pass'] else 1)
    output['exit_code'] = status
    (args.out / 'anomaly-psycopg.json').write_text(
        json.dumps(output, indent=2) + '\n', encoding='utf-8', newline='\n')
    summary = (f"anomaly-psycopg {args.run_id}: {'PASS' if output['pass'] else 'FAIL'}; "
               f"{len(output['runs'])} matrix runs, {mismatches} mismatches; "
               f"{len(output['findings'])} findings, {finding_mismatches} finding mismatches; "
               f"{len(output['infrastructure'])} infrastructure errors; {output['seconds']:.3f}s")
    with (args.out / 'summary.md').open('a', encoding='utf-8', newline='\n') as stream:
        stream.write(summary + '\n')
    print(summary)
    return status


if __name__ == '__main__':
    raise SystemExit(main())
