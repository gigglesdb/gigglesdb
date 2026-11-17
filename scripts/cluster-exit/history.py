#!/usr/bin/env python3
"""Bounded transfer histories; independent checker verdicts and negative controls."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import secrets
import subprocess
import sys
import threading
import time

import psycopg

# Isolated Python omits the script directory; import only our sibling kit helper.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib import Cluster


def tick():
    return time.monotonic_ns() // 1000


class TransferWorkload:
    """Splitmix64 and draw order match record.rs (64-bit platforms)."""
    def __init__(self, seed, accounts, level):
        self.seed, self.accounts, self.level = seed, accounts, level

    def random(self):
        mask = (1 << 64) - 1
        self.seed = (self.seed + 0x9e3779b97f4a7c15) & mask
        z = self.seed
        z = ((z ^ (z >> 30)) * 0xbf58476d1ce4e5b9) & mask
        z = ((z ^ (z >> 27)) * 0x94d049bb133111eb) & mask
        return z ^ (z >> 31)

    def scan(self):
        return dict(op='scan', start=self.accounts[0], end=self.accounts[-1] + 1)

    def next_ops(self):
        if self.random() % 4 == 0:
            return [self.scan()]
        n = len(self.accounts)
        src = self.random() % n
        dst = (src + 1 + self.random() % (n - 1)) % n
        src, dst, amount = self.accounts[src], self.accounts[dst], self.random() % 5 + 1
        op = {'op': 'transfer', 'from': src, 'to': dst, 'amount': amount}
        return [op, self.scan()] if self.level == 'serializable' else [
            op, dict(op='read', key=src), dict(op='read', key=dst)]


class Target:
    def __init__(self, cluster=None, dsn=None):
        self.cluster, self.dsn = cluster, dsn
        self.nodes = [n['id'] for n in cluster.nodes] if cluster else [1]

    def connect(self, node):
        kw = dict(autocommit=True, cursor_factory=psycopg.ClientCursor, connect_timeout=2)
        conn = self.cluster.connect(node, **kw) if self.cluster else psycopg.connect(self.dsn, **kw)
        try:
            conn.execute('SET statement_timeout = 20000')
            return conn
        except Exception:
            conn.close()
            raise

    def reconnect(self, preferred, exclude=None):
        nodes = [n for n in self.nodes if n != exclude] if self.cluster else self.nodes
        order = sorted(nodes, key=lambda n: (self.nodes.index(n) - self.nodes.index(preferred)) % len(self.nodes))
        begin, last = time.monotonic(), None
        while time.monotonic() - begin < 60:
            node = order[min(int((time.monotonic() - begin) // 5), len(order) - 1)]
            try:
                return self.connect(node), node
            except psycopg.Error as exc:
                last = exc
                time.sleep(.1)
        raise TimeoutError(f'no gateway within 60s: {last}')


def rollback(conn):
    try:
        conn.execute('ROLLBACK')
    except psycopg.Error:
        conn.close()


def attempt(conn, table, ident, ops, level, on_start=lambda: None):
    a = dict(id=ident, logical_id=ident, invoke=tick(), complete=None, outcome='aborted',
             durable=None, decision_at=None, ops=ops, observations=[], receipt=None)
    on_start()
    xid, committing, error = None, False, None
    try:
        isolation = 'SERIALIZABLE' if level in ('serializable', 'serializable-pairs') else 'READ COMMITTED'
        conn.execute(f'BEGIN ISOLATION LEVEL {isolation}')
        transfer = ops[0] if ops[0]['op'] == 'transfer' else None
        if transfer:
            amount, src, dst = transfer['amount'], transfer['from'], transfer['to']
            debited = conn.execute(f'UPDATE {table} SET bal = bal - %s WHERE id = %s AND bal >= %s',
                                   (amount, src, amount)).rowcount
            if debited != 1:
                rollback(conn)
                a['complete'] = tick()
                return a, xid, 'insufficient funds'
            if conn.execute(f'UPDATE {table} SET bal = bal + %s WHERE id = %s', (amount, dst)).rowcount != 1:
                raise RuntimeError('destination account missing')
            xid = str(conn.execute('SELECT pg_current_xact_id()').fetchone()[0])
        for op in ops:
            if op['op'] == 'scan':
                rows = conn.execute(f'SELECT id, bal FROM {table} ORDER BY id').fetchall()
                a['observations'].append(dict(observation='scan', rows=rows))
            elif op['op'] == 'read':
                row = conn.execute(f'SELECT bal FROM {table} WHERE id = %s', (op['key'],)).fetchone()
                a['observations'].append(dict(observation='read', value=row[0] if row else None))
        committing = True
        result = conn.execute('COMMIT')
        a['outcome'] = 'committed' if result.statusmessage == 'COMMIT' else 'aborted'
    except psycopg.Error as exc:
        error = f'{exc.sqlstate}: {exc}'[:500]
        if committing and exc.sqlstate not in {'40001', '40P01'}:
            a['outcome'] = 'indeterminate'
            conn.close()  # Never leave a possibly live transaction in the next attempt.
        else:
            rollback(conn)
    if a['outcome'] != 'indeterminate':
        a['complete'] = tick()
    return a, xid, error


def resolve(target, attempt_record, xid, source):
    if xid is None:
        return  # Read-only lost replies remain unknown, with no invented evidence.
    preferred = target.nodes[(target.nodes.index(source) + 1) % len(target.nodes)]
    conn, node = target.reconnect(preferred, exclude=source)
    with conn:
        deadline = time.monotonic() + 10
        while True:
            status = conn.execute('SELECT pg_xact_status(%s)', (xid,)).fetchone()[0]
            if status in {'committed', 'aborted'}:
                attempt_record['durable'] = status
                return
            if status != 'in progress' or time.monotonic() >= deadline:
                raise RuntimeError(f'xid {xid} on node {node}: unresolved status {status!r}')
            time.sleep(.1)


def no_fault():
    pass


def run_window(target, table, workloads, initial, args, seed, win, faults=no_fault, window_done=None):
    """Run and quiesce one window. faults() fires once at attempts//3 starts.

    Return (JSONL header, attempts including closing scan, next state, metrics).
    Fault callers can capture their Cluster in the callable; no CLI state needed.
    """
    started, lock, counter, records, unknown, errors = time.monotonic(), threading.Lock(), 0, [], [], []
    invoked, finished_clients = 0, 0
    window_done = window_done if window_done is not None else threading.Event()
    threshold, ready = max(1, args.attempts // 3), threading.Event()

    def on_start():
        nonlocal invoked
        with lock:
            invoked += 1
            if invoked == threshold:
                ready.set()

    def worker(client):
        nonlocal counter, finished_clients
        conn, node = None, target.nodes[client % len(target.nodes)]
        try:
            while True:
                with lock:
                    if counter >= args.attempts:
                        return
                    index, counter = counter, counter + 1
                if conn is None or conn.closed:
                    conn, node = target.reconnect(node)
                ops, ident = workloads[client].next_ops(), f's{seed}-w{win}-a{index}'
                a, xid, error = attempt(conn, table, ident, ops, args.level, on_start)
                with lock:
                    records.append(a)
                    if error:
                        errors.append(dict(attempt=ident, node=node, error=error))
                    if a['outcome'] == 'indeterminate':
                        unknown.append((a, xid, node))
        finally:
            if conn is not None:
                conn.close()
            with lock:
                finished_clients += 1
                if finished_clients == args.clients:
                    window_done.set()

    with ThreadPoolExecutor(args.clients + 1) as pool:
        futures = [pool.submit(worker, c) for c in range(args.clients)]
        def inject():
            if not ready.wait(timeout=65):
                raise TimeoutError('fault threshold was not reached')
            faults()
        futures.append(pool.submit(inject))
        for future in as_completed(futures):
            future.result()
    workload_seconds = time.monotonic() - started
    for a, xid, node in unknown:
        resolve(target, a, xid, node)
    resolved = time.monotonic()
    conn, _ = target.reconnect(target.nodes[0])
    with conn:
        final, _, error = attempt(conn, table, f's{seed}-w{win}-final', [workloads[0].scan()], args.level)
    records.append(final)
    if final['outcome'] != 'committed':
        raise RuntimeError(f'closing scan failed: {error}')
    rows = final['observations'][0]['rows']
    header = dict(record='header', version=1, horizon=tick(), initial=initial,
                  invariants=dict(conserved_keys=workloads[0].accounts, nonnegative_keys=workloads[0].accounts,
                                  budgets={}, require_receipts=False))
    counts = Counter(a['outcome'] for a in records[:-1])
    metrics = dict(seed=seed, window=win, attempts=args.attempts, recorded_attempts=len(records),
                   committed_transfers=sum(a['outcome'] == 'committed' and a['ops'][0]['op'] == 'transfer'
                                           for a in records[:-1]),
                   **{name: counts[name] for name in ('committed', 'aborted', 'indeterminate')},
                   resolved=sum(a['durable'] is not None for a, _, _ in unknown), errors=errors,
                   seconds=dict(workload=workload_seconds, resolution=resolved - started - workload_seconds,
                                closing_scan=time.monotonic() - resolved))
    return header, sorted(records, key=lambda a: a['invoke']), rows, metrics


def checked(command, data=None):
    start = time.monotonic()
    result = subprocess.run(command, input=data, capture_output=True, timeout=120)
    return dict(exit_code=result.returncode, stdout=result.stdout.decode('utf-8', errors='replace'),
                stderr=result.stderr.decode('utf-8', errors='replace'), seconds=time.monotonic() - start)


def check_window(checker, path):
    result = checked([str(checker), 'check', str(path)])
    result['verdict'] = {0: 'PASS', 1: 'FAIL', 2: 'INCONCLUSIVE'}.get(result['exit_code'], 'ERROR')
    controls = {}
    for name in ('stale-read', 'aborted-but-visible', 'duplicate-apply'):
        mutation = checked([str(checker), 'control', name, str(path)])
        code = mutation['exit_code']
        if code == 0:
            data = mutation.pop('stdout')
            path.with_name(path.stem + '-' + name + '.jsonl').write_text(data, encoding='utf-8', newline='\n')
            control = checked([str(checker), 'check', '-'], data.encode('utf-8'))
            control['verdict'] = {0: 'PASS', 1: 'FAIL', 2: 'INCONCLUSIVE'}.get(control['exit_code'], 'ERROR')
            control['mutation'] = mutation
        else:
            control = dict(verdict='NOT_APPLICABLE' if code == 3 else 'ERROR', mutation=mutation)
        controls[name] = control
    return result, controls


def arguments():
    p = argparse.ArgumentParser(description=__doc__)
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument('--cluster', type=Path)
    source.add_argument('--target', choices=['pg'])
    p.add_argument('--dsn')
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--checker', type=Path, required=True)
    p.add_argument('--seeds', default='31,32')
    for name, default in [('windows', 4), ('attempts', 60), ('clients', 4), ('accounts', 8)]:
        p.add_argument('--' + name, type=int, default=default)
    p.add_argument('--level', choices=['serializable', 'serializable-pairs', 'read-committed-safe'],
                   default='serializable')
    args = p.parse_args()
    try:
        args.seeds = [int(s) for s in args.seeds.split(',')]
        if not args.seeds or any(not 0 <= s < 2**64 for s in args.seeds) or len(set(args.seeds)) != len(args.seeds):
            raise ValueError()
    except ValueError:
        p.error('--seeds requires distinct u64 integers')
    if not 2 <= args.attempts <= 120 or not 2 <= args.accounts <= 64 or not 1 <= args.clients <= 32 or args.windows < 1:
        p.error('bounds: attempts 2..120, accounts 2..64, clients 1..32, windows >= 1')
    if args.target == 'pg' and not args.dsn:
        p.error('--target pg requires --dsn')
    return args


def main():
    args, started = arguments(), time.monotonic()
    args.out.mkdir(parents=True, exist_ok=True)
    history_dir = args.out / 'history'
    history_dir.mkdir(exist_ok=True)
    output = dict(target=args.target or 'cluster', level=args.level, run_id=secrets.token_hex(4),
                  windows=[], infrastructure=[])
    try:
        target = Target(Cluster.load(args.cluster) if args.cluster else None, args.dsn)
        accounts = [k * 1_000_000 + 1 for k in range(args.accounts)]
        for seed in args.seeds:
            table = f"acct_{output['run_id']}_{seed}"
            conn, _ = target.reconnect(target.nodes[0])
            with conn:
                conn.execute(f'CREATE TABLE {table} (id bigint PRIMARY KEY, bal bigint NOT NULL)')
                for key in accounts:
                    conn.execute(f'INSERT INTO {table} (id, bal) VALUES (%s, 100)', (key,))
            initial = [(key, 100) for key in accounts]
            workloads = [TransferWorkload((seed * 16 + c) % 2**64, accounts, args.level) for c in range(args.clients)]
            for win in range(args.windows):
                header, records, initial, metrics = run_window(target, table, workloads, initial, args, seed, win)
                path = history_dir / f'seed{seed}-w{win}.jsonl'
                with path.open('w', encoding='utf-8', newline='\n') as stream:
                    for row in [header, *[dict(record='attempt', attempt=a) for a in records]]:
                        stream.write(json.dumps(row, separators=(',', ':')) + '\n')
                result, controls = check_window(args.checker, path)
                metrics.update(history=str(path), verdict=result['verdict'], check=result, controls=controls)
                output['windows'].append(metrics)
                print(f"seed {seed} window {win}: {result['verdict']}; controls " +
                      ', '.join(f"{n}={c['verdict']}" for n, c in controls.items()), flush=True)
    except Exception as exc:
        output['infrastructure'].append(f'{type(exc).__name__}: {exc}')
    windows = output['windows']
    output['not_applicable'] = sum(c['verdict'] == 'NOT_APPLICABLE' for w in windows for c in w['controls'].values())
    output['pass'] = not output['infrastructure'] and len(windows) == len(args.seeds) * args.windows and all(
        w['verdict'] == 'PASS' and all(c['verdict'] == 'FAIL' for c in w['controls'].values()) for w in windows)
    infra = output['infrastructure'] or any(w['verdict'] == 'ERROR' or any(
        c['verdict'] == 'ERROR' for c in w['controls'].values()) for w in windows)
    inconclusive = any(w['verdict'] == 'INCONCLUSIVE' or any(
        c['verdict'] == 'INCONCLUSIVE' for c in w['controls'].values()) for w in windows)
    code = 0 if output['pass'] else (2 if infra or inconclusive else 1)
    output.update(exit_code=code, seconds=time.monotonic() - started)
    (args.out / 'history.json').write_text(json.dumps(output, indent=2) + '\n', encoding='utf-8', newline='\n')
    summary = (f"history {output['run_id']}: {'PASS' if output['pass'] else 'INCONCLUSIVE' if inconclusive else 'FAIL'}; "
               f"{len(windows)} windows; {output['not_applicable']} controls not applicable; exit {code}")
    with (args.out / 'summary.md').open('a', encoding='utf-8', newline='\n') as stream:
        stream.write(summary + '\n')
    print(summary)
    return code


if __name__ == '__main__':
    raise SystemExit(main())
