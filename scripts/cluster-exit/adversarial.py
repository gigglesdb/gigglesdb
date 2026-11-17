#!/usr/bin/env python3
"""Check transfer histories and READ COMMITTED observers during range faults."""
import argparse
from collections import Counter, deque
import json
from pathlib import Path
import random
import re
import secrets
import sys
import threading
import time
from urllib.error import HTTPError

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent))
from history import Target, TransferWorkload, check_window, run_window, tick
from lib import Cluster, wait_for


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8', newline='\n')


def insert_pad(conn, table, ident, size, account=False):
    pad = secrets.token_hex((size + 1) // 2)[:size]
    columns, values = ('id, bal, pad', '%s, 100, %s') if account else ('id, pad', '%s, %s')
    conn.execute(f'INSERT INTO {table} ({columns}) VALUES ({values})', (ident, pad))
    return len(pad)


def retryable(exc):
    state = exc.sqlstate or ''
    return state == '40001' or state.startswith(('08', '57P')) or isinstance(
        exc, (psycopg.OperationalError, psycopg.InterfaceError)) and not state


def observed(events, node, kind, operation):
    event = dict(kind=kind, node=node, at=tick(), outcome='ok')
    try:
        return operation()
    except Exception as exc:
        event['outcome'] = getattr(exc, 'sqlstate', None) or type(exc).__name__
        raise
    finally:
        events.append(event)


class Observers:
    def __init__(self, target, table, fill, accounts, args):
        self.target, self.table, self.fill, self.accounts, self.args = target, table, fill, accounts, args
        self.stop, self.lock, self.threads = threading.Event(), threading.Lock(), {}
        self.successes = []
        self.results = {name: dict(answers=0, retries=0, errors=[], violations=[])
                        for name in ('total', 'scan', 'filler')}

    def start(self, name):
        if name not in self.threads:
            self.threads[name] = threading.Thread(target=self.loop, args=(name,), name=name)
            self.threads[name].start()

    def loop(self, name):
        rng, conns, ident, rings = random.Random(41 + len(name)), {}, 0, {}
        result = self.results[name]
        fixed = self.target.nodes[-2 if name == 'total' else -1]
        try:
            while not self.stop.is_set():
                node = fixed if name in ('total', 'scan') else rng.choice(self.target.nodes)
                try:
                    if node not in conns or conns[node].closed:
                        rings[node] = deque(maxlen=5)
                        conns[node] = self.target.connect(node)
                        observed(rings[node], node, 'SET', lambda: conns[node].execute(
                            "SET default_transaction_isolation = 'read committed'"))
                    conn = conns[node]
                    if name == 'total':
                        row = observed(rings[node], node, 'SELECT total', lambda: conn.execute(
                            f'SELECT count(*), sum(bal) FROM {self.table} WHERE id = ANY(%s)',
                            (self.accounts,)).fetchone())
                        if tuple(row) != (len(self.accounts), 100 * len(self.accounts)):
                            result['violations'].append(dict(at=tick(), row=list(row)))
                    elif name == 'scan':
                        sent = tick()
                        ids = observed(rings[node], node, 'SELECT scan', lambda: [r[0] for r in
                            conn.execute(f'SELECT id FROM {self.fill} ORDER BY id')])
                        with self.lock:
                            required = {key for key, completed in self.successes if completed < sent}
                        missing = required - set(ids)
                        if missing or len(ids) != len(set(ids)) or ids != sorted(ids):
                            result['violations'].append(dict(sent=sent, missing=sorted(missing),
                                duplicates=len(ids) - len(set(ids)), ordered=ids == sorted(ids)))
                    elif name == 'filler':
                        ident += 1  # Never retry an unknown INSERT under the same id.
                        observed(rings[node], node, 'INSERT filler', lambda:
                                 insert_pad(conn, self.fill, ident, self.args.fill_bytes))
                        completed = tick()
                        with self.lock:
                            self.successes.append((ident, completed))
                    result['answers'] += 1
                except psycopg.Error as exc:
                    if retryable(exc):
                        result['retries'] += 1
                    else:
                        result['errors'].append(dict(node=node, at=tick(), error=f'{exc.sqlstate}: {exc}',
                                                     events=list(rings[node])))
                    if exc.sqlstate == '25P02' and node in conns:
                        recovery = result['errors'][-1]['recovery'] = dict(rollback_ok=False, next_statement_ok=False)
                        try:
                            observed(rings[node], node, 'ROLLBACK', lambda: conns[node].execute('ROLLBACK'))
                            recovery['rollback_ok'] = True
                            observed(rings[node], node, 'SELECT probe', lambda:
                                     conns[node].execute('SELECT 1').fetchone())
                            recovery['next_statement_ok'] = True
                        except psycopg.Error as recovery_exc:
                            recovery['error'] = f'{recovery_exc.sqlstate}: {recovery_exc}'
                        recovery['events'] = list(rings[node])
                        if recovery['next_statement_ok']:
                            continue
                    if node in conns:
                        conns.pop(node).close()
                self.stop.wait(.02 if name == 'filler' else .05)
        except Exception as exc:
            result['errors'].append(dict(node=node, at=tick(), error=f'{type(exc).__name__}: {exc}',
                                         events=list(rings.get(node, []))))
        finally:
            for conn in conns.values():
                conn.close()

    def close(self):
        self.stop.set()
        for thread in self.threads.values():
            thread.join()
        self.results['filler']['completed_inserts'] = self.successes
        return self.results


class BulkWriter:
    def __init__(self, target, table, args):
        self.target, self.table, self.args = target, table, args
        self.stop = threading.Event()
        self.result = dict(inserts=0, bytes=0, errors=0, errors_by_sqlstate={})
        self.thread = threading.Thread(target=self.loop, name='bulk')

    def loop(self):
        rng, conns, ident, due = random.Random(75), {}, 0, time.monotonic()
        try:
            while not self.stop.is_set():
                node = rng.choice(self.target.nodes)
                try:
                    if node not in conns or conns[node].closed:
                        conns[node] = self.target.connect(node)
                    ident += 1  # A lost reply may have committed; retry with a fresh sequential id.
                    size = insert_pad(conns[node], self.table, ident, self.args.bulk_row_bytes)
                    self.result['inserts'] += 1
                    self.result['bytes'] += size
                    due = max(due + size / self.args.bulk_bytes_per_sec, time.monotonic())
                    self.stop.wait(max(0, due - time.monotonic()))
                except psycopg.Error as exc:
                    self.result['errors'] += 1
                    state = exc.sqlstate or 'unknown'
                    counts = self.result['errors_by_sqlstate']
                    counts[state] = counts.get(state, 0) + 1
                    if node in conns:
                        conns.pop(node).close()
                    self.stop.wait(.1)
        finally:
            for conn in conns.values():
                conn.close()

    def close(self):
        self.stop.set()
        self.thread.join()
        return self.result


def admin(cluster, args):
    result = cluster.agent(1).admin(args)
    if result['rc']:
        raise RuntimeError(f'{args}: {result}')
    return result


def ranges(cluster):
    return json.loads(admin(cluster, ['cluster', 'status', '--json'])['stdout'])['ranges']


def account_ranges(cluster, keys):
    bounds = ranges(cluster)
    raw = admin(cluster, ['range', 'status'])
    reports = {}
    for line in raw['stdout'].splitlines():
        match = re.fullmatch(r'range=(\d+) epoch=\d+ voters=\[([\d,]*)\] learners=\[([\d,]*)\] '
                             r'leader=(\d+|none) move_step=\S+', line)
        if not match:
            raise ValueError(f'unrecognized range status: {line}')
        ident, voters, learners, leader = match.groups()
        reports[int(ident)] = dict(replicas=[int(n) for n in (voters + ',' + learners).split(',') if n],
                                  leader=None if leader == 'none' else int(leader))
    selected = [dict(r, **reports[r['id']]) for r in bounds if any(
        bytes.fromhex(r['start']) <= key and (r['end'] is None or key < bytes.fromhex(r['end']))
        for key in keys)]
    if not selected:
        raise RuntimeError('no account ranges found')
    return selected, raw


def fault(cluster, target, name, keys, actions, clients, ranges_at_start, window_done,
          split_wait=120, accounts_at_start=1, run_started=None):
    action = dict(name=name, started=tick(), status='failed')
    actions.append(action)
    try:
        if name == 'split':
            action['ranges_before'] = ranges_at_start
            deadline = time.monotonic() + split_wait
            while True:
                action['ranges_after'] = len(ranges(cluster))
                selected, _ = account_ranges(cluster, keys)
                action['account_ranges_after'] = len(selected)
                if action['ranges_after'] > ranges_at_start or len(selected) > accounts_at_start:
                    action['result'] = 'range or account range count grew since run start'
                    action['first_growth_seconds'] = (tick() - (run_started or action['started'])) / 1e6
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError(f'no range growth within {split_wait} seconds')
                time.sleep(.1)
        elif name in ('move', 'isolate', 'kill-leader'):
            selected, action['range_status'] = account_ranges(cluster, keys)
            action['account_ranges'] = selected
            if name == 'move':
                choices = [(r, n) for r in selected for n in target.nodes if n not in r['replicas']]
                if not choices:
                    action.update(status='skipped', reason='no target')
                    return
                chosen, node = choices[0]
                source = chosen['replicas'][0]
                action.update(range=chosen['id'], source=source, target=node)
                action['result'] = admin(cluster, ['range', 'move', str(chosen['id']), str(source), str(node), '--wait'])
            else:
                leaders = Counter(r['leader'] for r in selected if r['leader'] is not None)
                if not leaders:
                    raise RuntimeError('no account range leader')
                node = leaders.most_common(1)[0][0]
                action['node'] = node
                if name == 'isolate':
                    try:
                        action['isolate'] = cluster.agent(node).isolate()
                    except RuntimeError as exc:
                        if isinstance(exc.__cause__, HTTPError) and exc.__cause__.code == 501:
                            action.update(status='skipped', reason='no isolation', result=str(exc))
                            return
                        raise
                    try:
                        time.sleep(10)
                    finally:
                        action['heal'] = cluster.agent(node).heal()
                else:
                    restart(cluster, node, action)
        elif name == 'kill':
            connected = target.nodes[:min(clients, len(target.nodes))]
            node = next((n for n in connected if n != 1), connected[0])
            action['node'] = node
            restart(cluster, node, action)
        action['status'] = 'done'
    except Exception as exc:
        action['error'] = f'{type(exc).__name__}: {exc}'
    finally:
        action.update(completed=tick(), seconds=(tick() - action['started']) / 1e6)


def restart(cluster, node, action):
    agent = cluster.agent(node)
    action['kill'] = agent.kill()
    try:
        time.sleep(5)
    finally:
        action['start'] = agent.start(cluster.mode)
    wait_for(lambda: agent.status()['ready'], 90)
    def sql_ready():
        with cluster.connect(node, connect_timeout=2) as conn:
            return conn.execute('SELECT 1').fetchone() == (1,)
    wait_for(sql_ready, 60)
    action['ready'] = agent.status()


def arguments():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('cluster', 'out', 'checker'):
        p.add_argument('--' + name, required=True, type=Path)
    p.add_argument('--seeds', default='41')
    p.add_argument('--schedule', default='none,split,move,kill,isolate,kill-leader')
    p.add_argument('--split-wait', type=float, default=120)
    for name, default in [('attempts', 60), ('clients', 6), ('accounts', 8), ('fill-bytes', 4096),
                          ('account-pad-bytes', 16384), ('bulk-bytes-per-sec', 4194304), ('bulk-row-bytes', 32768)]:
        p.add_argument('--' + name, type=int, default=default)
    p.add_argument('--level', choices=['serializable', 'serializable-pairs', 'read-committed-safe'],
                   default='serializable-pairs')
    args = p.parse_args()
    try:
        args.seeds = [int(s) for s in args.seeds.split(',')]
        if not args.seeds or len(set(args.seeds)) != len(args.seeds) or any(not 0 <= s < 2**64 for s in args.seeds):
            raise ValueError()
    except ValueError:
        p.error('--seeds requires distinct u64 integers')
    args.schedule = args.schedule.split(',')
    if any(s not in ('none', 'split', 'move', 'kill', 'isolate', 'kill-leader') for s in args.schedule):
        p.error('unknown schedule entry')
    if (not 2 <= args.attempts <= 120 or not 1 <= args.clients <= 32 or not 2 <= args.accounts <= 64
            or args.fill_bytes < 1 or args.account_pad_bytes < 0
            or args.bulk_bytes_per_sec < 1 or args.bulk_row_bytes < 1 or not 0 < args.split_wait < float('inf')):
        p.error('bounds: attempts 2..120, clients 1..32, accounts 2..64, fill-bytes >= 1, account-pad-bytes >= 0, bulk bytes/sec and row bytes >= 1, split-wait finite and > 0')
    return args


def main():
    args, started = arguments(), time.monotonic()
    args.out.mkdir(parents=True, exist_ok=True)
    history_dir = args.out / 'history'
    history_dir.mkdir(exist_ok=True)
    output = dict(run_id=secrets.token_hex(4), level=args.level, account_pad_bytes=args.account_pad_bytes,
                  windows=[], infrastructure=[], observers={})
    observers = bulk = None
    try:
        cluster = Cluster.load(args.cluster)
        output['ranges_at_start'] = len(ranges(cluster))
        run_started = tick()
        target = Target(cluster)
        if len(target.nodes) < 3:
            raise ValueError('at least three nodes required for observers')
        table, fill, bulk_table = [prefix + output['run_id'] for prefix in ('acct_', 'fill_', 'bulk_')]
        accounts = [k * 1_000_000 + 1 for k in range(args.accounts)]
        conn, _ = target.reconnect(1)
        with conn:
            conn.execute(f"CREATE TABLE {table} (id bigint PRIMARY KEY, bal bigint NOT NULL, pad text NOT NULL DEFAULT '')")
            conn.execute(f'CREATE TABLE {fill} (id bigint PRIMARY KEY, pad text)')
            conn.execute(f'CREATE TABLE {bulk_table} (id bigint PRIMARY KEY, pad text)')
            for key in accounts:
                insert_pad(conn, table, key, args.account_pad_bytes, account=True)
            oid = int(conn.execute('SELECT oid FROM pg_class WHERE relname = %s', (table,)).fetchone()[0])
        # table_oid(id) = 65536 + 4*id; row range key = Rows || table BE8 || encoded bigint PK.
        table_id = (oid - 65536) // 4
        keys = [b'\x80' + table_id.to_bytes(8, 'big') + b'\x02' + (key ^ (1 << 63)).to_bytes(8, 'big') for key in accounts]
        output['account_ranges_at_start'] = len(account_ranges(cluster, keys)[0])
        output.update(account_table=table, fill_table=fill, bulk_table=bulk_table,
                      bulk_bytes_per_sec=args.bulk_bytes_per_sec, bulk_row_bytes=args.bulk_row_bytes, account_keys=[k.hex() for k in keys])
        observers = Observers(target, table, fill, accounts, args)
        for name in ('total', 'scan', 'filler'):
            observers.start(name)
        bulk = BulkWriter(target, bulk_table, args)
        bulk.thread.start()
        initial = [(key, 100) for key in accounts]
        for seed in args.seeds:
            workloads = [TransferWorkload((seed * 16 + c) % 2**64, accounts, args.level) for c in range(args.clients)]
            for win, name in enumerate(args.schedule):
                before, actions = len(ranges(cluster)), []
                accounts_before, _ = account_ranges(cluster, keys)
                window_done = threading.Event()
                header, records, initial, metrics = run_window(target, table, workloads, initial, args, seed, win,
                    lambda: fault(cluster, target, name, keys, actions, args.clients,
                                  output['ranges_at_start'], window_done, args.split_wait,
                                  output['account_ranges_at_start'], run_started), window_done)
                path = history_dir / f'seed{seed}-w{win}-{name}.jsonl'
                with path.open('w', encoding='utf-8', newline='\n') as stream:
                    for row in [header, *[dict(record='attempt', attempt=a) for a in records]]:
                        stream.write(json.dumps(row, separators=(',', ':')) + '\n')
                result, controls = check_window(args.checker, path)
                statuses = {str(n): cluster.agent(n).status() for n in target.nodes}
                accounts_after, _ = account_ranges(cluster, keys)
                metrics.update(account_ranges_before=len(accounts_before), account_ranges_after=len(accounts_after),
                    account_range_ids=[r['id'] for r in accounts_after], schedule=name, actions=actions, ranges_before=before, ranges_after=len(ranges(cluster)),
                    history=str(path), verdict=result['verdict'], check=result, controls=controls, nodes=statuses)
                output['windows'].append(metrics)
                if any(not s['ready'] for s in statuses.values()):
                    output['infrastructure'].append(f'seed {seed} window {win}: node stopped unexpectedly')
                save(args.out / 'adversarial.json', output)
    except Exception as exc:
        output['infrastructure'].append(f'{type(exc).__name__}: {exc}')
    finally:
        if bulk:
            output['bulk'] = bulk.close()
        if observers:
            output['observers'] = observers.close()
    windows, obs = output['windows'], output['observers']
    output['pass'] = not output['infrastructure'] and len(windows) == len(args.seeds) * len(args.schedule) and all(
        w['verdict'] == 'PASS' and all(c['verdict'] == 'FAIL' for c in w['controls'].values()) and
        all(a['status'] in ('done', 'skipped') for a in w['actions']) for w in windows) and bool(obs) and all(
        not r['violations'] and not r['errors'] for r in obs.values()) and all(obs[n]['answers'] for n in ('total', 'scan'))
    infra = output['infrastructure'] or any(w['verdict'] in ('ERROR', 'INCONCLUSIVE') or any(
        c['verdict'] in ('ERROR', 'INCONCLUSIVE') for c in w['controls'].values()) for w in windows)
    code = 0 if output['pass'] else 2 if infra else 1
    output.update(exit_code=code, seconds=time.monotonic() - started)
    save(args.out / 'adversarial.json', output)
    skips = [f"{w['schedule']}: {a['reason']}" for w in windows for a in w['actions'] if a['status'] == 'skipped']
    summary = (f"adversarial {output['run_id']}: {'PASS' if output['pass'] else 'FAIL'}; {len(windows)} windows; "
               f"skips={'; '.join(skips) or 'none'}; exit {code}")
    with (args.out / 'summary.md').open('a', encoding='utf-8', newline='\n') as stream:
        stream.write(summary + '\n')
    print(summary)
    return code


if __name__ == '__main__':
    raise SystemExit(main())
