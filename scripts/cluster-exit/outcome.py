"""Resolve cross-range transactions after losing their gateway around COMMIT."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import random
import secrets
import sys
import threading
import time

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib import Cluster, wait_for, write_result


def connect(cluster, node):
    conn = cluster.connect(node, connect_timeout=2, cursor_factory=psycopg.ClientCursor)
    # R5-62 allows 45 s per statement, above the runtime's 5 s range-call timeout.
    conn.execute('SET statement_timeout = 45000')
    return conn


def restart(cluster, gateway):
    begin = time.monotonic()
    cluster.agent(gateway).start(cluster.mode)
    wait_for(lambda: cluster.agent(gateway).status()['ready'], 60)
    def ready():
        with connect(cluster, gateway) as conn:
            return conn.execute('SELECT 1').fetchone() == (1,)
    wait_for(ready, 60)
    return time.monotonic() - begin


def commit(conn, sent):
    sent.set()
    try:
        message = conn.execute('COMMIT').statusmessage
        return dict(view='success' if message == 'COMMIT' else 'error', message=message)
    except psycopg.Error as exc:
        return dict(view='error', sqlstate=exc.sqlstate, connection_error=isinstance(
            exc, (psycopg.OperationalError, psycopg.InterfaceError)), error=str(exc))


def trial(cluster, a, b, number, point, gateway, observer):
    begin = time.monotonic()
    row = dict(trial=number, point=point, gateway=gateway, observer=observer,
               violations=[], timings={}, restart_before_resolution=False)
    killed, restarted = False, False
    conn = connect(cluster, gateway)
    try:
        conn.execute('BEGIN')
        conn.execute(f"INSERT INTO {a} VALUES ({number}, 'x')")
        conn.execute(f'INSERT INTO {b} VALUES ({number}, 1)')
        xid = int(conn.execute('SELECT pg_current_xact_id()::text').fetchone()[0])
        row['xid'] = xid
        sent = threading.Event()
        if point == 'before':
            cluster.agent(gateway).kill()
            killed = True
            client = commit(conn, sent)
        elif point == 'during':
            with ThreadPoolExecutor(1) as pool:
                future = pool.submit(commit, conn, sent)
                if not sent.wait(5):
                    raise TimeoutError('commit thread did not start')
                row['kill_delay_ms'] = random.uniform(0, 30)
                time.sleep(row['kill_delay_ms'] / 1000)
                cluster.agent(gateway).kill()
                killed = True
                client = future.result(timeout=10)
        else:
            client = commit(conn, sent)
            cluster.agent(gateway).kill()
            killed = True
        row['client'] = client
        row['timings']['kill_seconds'] = time.monotonic() - begin
        deadline = time.monotonic() + 60
        poll_begin, status, last, observed = time.monotonic(), None, None, None
        try:
            while time.monotonic() < deadline:
                try:
                    if observed is None or observed.closed:
                        observed = connect(cluster, observer)
                    remaining_ms = max(1, int(min(45, deadline - time.monotonic()) * 1000))
                    observed.execute(f'SET statement_timeout = {remaining_ms}')
                    status = observed.execute(f'SELECT pg_xact_status({xid})').fetchone()[0]
                    if status in ('committed', 'aborted'):
                        break
                    if status != 'in progress':
                        last = f'unexpected status {status!r}'
                except psycopg.Error as exc:
                    last = str(exc)
                    if observed:
                        observed.close()
                    observed = None
                # R5-62: an open transaction can need its owner restarted to resolve.
                if status == 'in progress' and not restarted and time.monotonic() - poll_begin >= 10:
                    if point == 'after':
                        row['violations'].append('acknowledged commit unresolved while owner down')
                    row['restart_before_resolution'] = True
                    row['timings']['restart_seconds'] = restart(cluster, gateway)
                    restarted = True
                time.sleep(.2)
            row.update(status=status, last_poll_error=last)
            row['timings']['resolution_seconds'] = time.monotonic() - poll_begin
            if status not in ('committed', 'aborted') or time.monotonic() > deadline:
                row['violations'].append('no definite outcome within 60 seconds')
            if observed is None or observed.closed:
                observed = connect(cluster, observer)
            rows = [observed.execute(f'SELECT * FROM {table} WHERE id = {number}').fetchall()
                    for table in (a, b)]
            row['rows'] = rows
            expected = [[(number, 'x')], [(number, 1)]] if status == 'committed' else [[], []]
            if rows != expected:
                row['violations'].append('rows disagree with outcome or are partial')
            if client['view'] == 'success' and status != 'committed':
                row['violations'].append('acknowledged commit not committed')
            if point == 'before' and (status != 'aborted' or not client.get('connection_error')):
                row['violations'].append('before must abort with a client connection error')
            if point == 'after' and client['view'] != 'success':
                row['violations'].append('after requires acknowledged commit before kill')
        finally:
            if observed:
                observed.close()
    finally:
        conn.close()
        if killed and not restarted:
            row['timings']['restart_seconds'] = restart(cluster, gateway)
        row['timings']['total_seconds'] = time.monotonic() - begin
    return row


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cluster', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--trials', type=int, default=20, help='trials per kill point')
    p.add_argument('--points', default='before,during,after')
    args = p.parse_args()
    points = args.points.split(',')
    if args.trials < 1 or not set(points) <= {'before', 'during', 'after'}:
        p.error('positive trials and points before,during,after required')
    result = dict(trials=[], counts={})
    rc = 0
    try:
        cluster = Cluster.load(args.cluster)
        gateways = [n['id'] for n in cluster.nodes if n['id'] != 1]
        runid = secrets.token_hex(6)
        a, b = 'oa_' + runid, 'ob_' + runid
        result.update(tables=[a, b], mode=cluster.mode)
        with connect(cluster, 1) as conn:
            conn.execute(f'CREATE TABLE {a} (id bigint primary key, note text)')
            conn.execute(f'CREATE TABLE {b} (id bigint primary key, n bigint)')
        for point in points:
            for _ in range(args.trials):
                number = len(result['trials']) + 1
                gateway = gateways[(number - 1) % len(gateways)]
                row = trial(cluster, a, b, number, point, gateway, 1)
                result['trials'].append(row)
                write_result(args.out, 'outcome', result)
        result['counts'] = {point: dict(Counter('fail' if t['violations'] else 'pass'
                            for t in result['trials'] if t['point'] == point)) for point in points}
        rc = int(any(t['violations'] for t in result['trials']))
    except Exception as exc:
        result['infrastructure_error'] = str(exc)
        rc = 2
    result['exit_code'] = rc
    write_result(args.out, 'outcome', result)
    print(f'outcome: {"PASS" if rc == 0 else "VIOLATION" if rc == 1 else "INFRASTRUCTURE"}; '
          f'trials={len(result["trials"])} counts={result["counts"]}')
    return rc


if __name__ == '__main__':
    sys.exit(main())
