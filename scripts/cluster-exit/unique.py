"""Concurrent secondary uniqueness through persistent gateway sessions."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
import secrets
import sys
import threading

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib import Cluster, write_result

LEVELS = dict(rc='READ COMMITTED', rr='REPEATABLE READ', ser='SERIALIZABLE')


def attempt(conn, barrier, table, level, variant, key, value):
    phase = 'begin'
    barrier.wait(timeout=30)
    try:
        conn.execute(f'BEGIN ISOLATION LEVEL {LEVELS[level]}')
        phase = 'insert'
        suffix = ' ON CONFLICT (v) DO NOTHING' if variant == 'nothing' else ''
        cur = conn.execute(f'INSERT INTO {table} VALUES ({key}, {value}){suffix}')
        inserted = cur.rowcount
        phase = 'commit'
        commit = conn.execute('COMMIT').statusmessage
        return dict(inserted=inserted, commit=commit)
    except psycopg.Error as exc:
        result = dict(sqlstate=exc.sqlstate or 'connection_lost', phase=phase, error=str(exc))
        try:
            conn.execute('ROLLBACK')
        except psycopg.Error:
            pass
        return result


def run(args, result):
    cluster = Cluster.load(args.cluster) if args.target == 'cluster' else None
    nodes = [n['id'] for n in cluster.nodes] if cluster else [1, 2, 3]
    if args.nodes != 'all':
        nodes = [int(n) for n in args.nodes.split(',')]
    if len(nodes) < 2 or len(set(nodes)) != len(nodes):
        raise ValueError('--nodes must select at least two distinct sessions/nodes')
    if cluster and not set(nodes) <= {n['id'] for n in cluster.nodes}:
        raise ValueError('unknown node')
    def connect(n):
        kw = dict(autocommit=True, connect_timeout=5, cursor_factory=psycopg.ClientCursor)
        conn = cluster.connect(n, **kw) if cluster else psycopg.connect(args.dsn, **kw)
        conn.execute('SET statement_timeout = 20000')
        return conn
    table = 'uq_' + secrets.token_hex(6)
    result.update(table=table, nodes=nodes, levels={}, violations=[])
    with ExitStack() as stack:
        setup = stack.enter_context(connect(1))
        setup.execute(f'CREATE TABLE {table} (id bigint primary key, v bigint not null, UNIQUE (v))')
        conns = [stack.enter_context(connect(n)) for n in nodes]
        observer = stack.enter_context(connect(next(n for n in nodes if n != 1)))
        total, value = 0, 0
        with ThreadPoolExecutor(len(conns)) as pool:
            for level in args.levels.split(','):
                variants = result['levels'][level] = {}
                for variant in ('insert', 'nothing'):
                    stats = variants[variant] = dict(rounds=args.rounds, successes=0,
                                                     errors={}, violations=[])
                    errors = Counter()
                    for r in range(args.rounds):
                        value += 1
                        barrier = threading.Barrier(len(conns))
                        futures = [pool.submit(attempt, c, barrier, table, level, variant,
                                               value * len(conns) + k, value)
                                   for k, c in enumerate(conns)]
                        outcomes = [f.result() for f in futures]
                        wins, invalid = 0, []
                        for n, outcome in zip(nodes, outcomes):
                            state = outcome.get('sqlstate')
                            if state:
                                errors[state] += 1
                                allowed = ('23505', '40001') if variant == 'insert' else ('40001',)
                                if state not in allowed or outcome['phase'] not in ('insert', 'commit'):
                                    invalid.append(dict(node=n, **outcome))
                            elif outcome['commit'] != 'COMMIT' or outcome['inserted'] not in (0, 1):
                                invalid.append(dict(node=n, **outcome))
                            elif outcome['inserted'] == 1:
                                wins += 1
                            elif variant == 'insert':
                                invalid.append(dict(node=n, **outcome))
                        stats['successes'] += wins
                        if wins != 1 or invalid:
                            stats['violations'].append(dict(round=r, value=value, wins=wins,
                                                            invalid=invalid, outcomes=outcomes))
                    stats['errors'] = dict(errors)
                    total += stats['successes']
        duplicates = observer.execute(f'SELECT v, count(*) FROM {table} GROUP BY v HAVING count(*) > 1').fetchall()
        count = observer.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
        result.update(successes=total, row_count=count, duplicates=duplicates)
        if duplicates or count != total:
            result['violations'].append(dict(expected_count=total, row_count=count, duplicates=duplicates))
    return int(bool(result['violations'] or any(s['violations'] for v in result['levels'].values() for s in v.values())))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--target', choices=['cluster', 'pg'], default='cluster')
    p.add_argument('--cluster')
    p.add_argument('--dsn')
    p.add_argument('--out', required=True)
    p.add_argument('--rounds', type=int, default=200)
    p.add_argument('--levels', default='rc,rr,ser')
    p.add_argument('--nodes', default='all', help='comma-separated node/session ids; PG defaults to 1,2,3')
    args = p.parse_args()
    if args.rounds < 1 or not set(args.levels.split(',')) <= LEVELS.keys():
        p.error('positive rounds and levels rc,rr,ser required')
    if not (args.cluster if args.target == 'cluster' else args.dsn):
        p.error('--cluster or --target pg --dsn required')
    result = dict(target=args.target)
    try:
        rc = run(args, result)
    except Exception as exc:
        result['infrastructure_error'] = str(exc)
        rc = 2
    result['exit_code'] = rc
    write_result(args.out, 'unique', result)
    print(f'unique: {"PASS" if rc == 0 else "VIOLATION" if rc == 1 else "INFRASTRUCTURE"}; '
          f'successes={result.get("successes", 0)} rows={result.get("row_count", "?")}')
    return rc


if __name__ == '__main__':
    sys.exit(main())
