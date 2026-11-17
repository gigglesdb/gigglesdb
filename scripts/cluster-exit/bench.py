"""pgbench 18 cluster measurements; prefix commands must mount --out as their cwd."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
import math
import os
from pathlib import Path
import re
import secrets
import shlex
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib import Cluster

LOG_FORMAT = ('PostgreSQL 18: whitespace fields client_id transaction_no time script_no '
              'time_epoch time_us [schedule_lag]. Scheduled latency ms = field 3 / 1000; '
              'field 3 ALREADY includes field 7 under --rate; do not add it again. '
              'https://www.postgresql.org/docs/18/pgbench.html#PGBENCH-TRANSACTION-LOGGING')


def percentile(sorted_values, percent):
    """Nearest rank, applied once to pooled gateway transactions."""
    return sorted_values[max(0, math.ceil(len(sorted_values) * percent / 100) - 1)] if sorted_values else None


def parse_log(lines, rate=False):
    values, errors = [], Counter()
    for line in lines:
        fields = line.split()
        if len(fields) != (7 if rate else 6):
            raise ValueError(f'invalid pgbench 18 log: {line!r}')
        token = fields[2]
        if token in ('serialization', 'deadlock', 'failed', 'skipped'):
            errors[{'serialization': '40001', 'deadlock': '40P01'}.get(token, token)] += 1
        else:
            value = float(token) / 1000
            if not math.isfinite(value) or value < 0:
                raise ValueError('invalid latency')
            values.append(value)
    return values, errors


def summarize(values, errors, tps):
    values.sort()
    return dict(committed=len(values), tps=tps, mean_ms=statistics.fmean(values) if values else None,
                **{f'p{p}_ms': percentile(values, p) for p in (50, 95, 99)}, errors=dict(errors))


def stop_reason(point, offered, target, maximum):
    if point['errors'] or point.get('process_errors'):
        return 'errors'
    if point['tps'] < .95 * offered:
        return 'achieved_below_95_percent'
    if point['p99_ms'] is None or point['p99_ms'] > target:
        return 'p99_exceeded'
    return 'max_rate' if offered >= maximum else None


class Bench:
    def __init__(self, args):
        self.a, self.out = args, Path(args.out).resolve()
        self.out.mkdir(parents=True, exist_ok=True)
        self.work = Path(tempfile.mkdtemp(prefix='bench-', dir=self.out))
        self.prefix = shlex.split(args.pgbench_prefix) + [args.pgbench]
        self.cluster = Cluster.load(args.cluster) if args.target == 'cluster' else None
        self.calls = []
        self.version = subprocess.run(self.prefix + ['--version'], cwd=self.out, check=True,
                                      capture_output=True, text=True, timeout=30).stdout.strip()
        if not re.search(r'\b18\.', self.version):
            raise ValueError('pgbench 18 required')

    def connect(self):
        import psycopg
        return (self.cluster.connect(self.ids[0]) if self.cluster else
                psycopg.connect(self.a.dsn, autocommit=True))

    def info(self, k):
        from psycopg.conninfo import conninfo_to_dict
        return conninfo_to_dict(self.cluster.dsn(k) if self.cluster else self.a.dsn)

    def prepare(self):
        import psycopg
        from psycopg.types.numeric import Int8
        fill = {}
        with self.connect() as conn:
            for table in ('bench_w', 'bench_ix'):
                if not self.cluster:  # Cluster.reset already wiped every node.
                    conn.execute(f'DROP TABLE IF EXISTS {table}')
                conn.execute(f'CREATE TABLE {table} (id bigint PRIMARY KEY, v bigint, pad text)')
                if table == 'bench_ix':
                    conn.execute('CREATE INDEX ON bench_ix (v)')
                started, form = time.monotonic(), 'generate_series'
                for start in range(1, self.a.rows + 1, self.a.fill_chunk):
                    end = min(start + self.a.fill_chunk - 1, self.a.rows)
                    if form == 'generate_series':
                        try:
                            conn.execute(f"INSERT INTO {table} (id, v, pad) "
                                         "SELECT g, 0, repeat('x', 100) FROM generate_series(%s, %s) AS g",
                                         (Int8(start), Int8(end)))
                            continue
                        except psycopg.Error as exc:
                            if exc.sqlstate not in ('0A000', '42601', '42883', '42P01'):
                                raise
                            form = 'values'
                    for first in range(start, end + 1, 500):
                        rows = range(first, min(first + 500, end + 1))
                        conn.execute(f'INSERT INTO {table} (id, v, pad) VALUES ' +
                                     ', '.join(['(%s, %s, %s)'] * len(rows)),
                                     [v for i in rows for v in (i, 0, 'x' * 100)])
                elapsed = time.monotonic() - started
                fill[table] = dict(rows=self.a.rows, seconds=elapsed,
                                   rows_per_second=self.a.rows / elapsed, form=form)
                print(f'fill {table}: {self.a.rows} rows, {fill[table]["rows_per_second"]:.1f} rows/s ({form})',
                      flush=True)
        return fill

    def script(self, kind):
        sql = {'write': 'UPDATE bench_w SET v = v + 1 WHERE id = :id;',
               'indexed': 'UPDATE bench_ix SET v = v + 1 WHERE id = :id;',
               'read': 'SELECT v FROM bench_w WHERE id = :id;',
               'hotkey': 'UPDATE bench_w SET v = v + 1 WHERE id = 1;'}[kind]
        path = self.work / f'{kind}.sql'
        path.write_text(f'\\set id random(1, {self.a.rows})\n{sql}\n', newline='\n')
        return str(path.relative_to(self.out)).replace('\\', '/')

    def gateway(self, k, kind, duration, clients, rate, role):
        from psycopg.conninfo import make_conninfo
        info = self.info(k)
        if role:
            info.update(user=role[0], password=role[1])
        if self.a.pgbench_host_override:
            info['host'] = self.a.pgbench_host_override
        stem = self.work / secrets.token_hex(8)
        relative = stem.relative_to(self.out).as_posix()
        command = self.prefix + ['-n', '-h', info.get('host', 'localhost'), '-p', info.get('port', '5432'),
            '-U', info.get('user', 'postgres'), '-d', make_conninfo(dbname=info.get('dbname', 'postgres'),
                sslmode=info.get('sslmode', 'disable'), connect_timeout=10),
            '-f', (self.work / f'{kind}.sql').relative_to(self.out).as_posix(),
            '-c', str(clients), '-j', str(min(4, clients)), '-T', str(duration),
            '-l', '--log-prefix', relative, '--failures-detailed', '--max-tries', '1']
        if rate is not None:
            command += ['-R', str(rate)]
        env = dict(os.environ, PGPASSWORD=info.get('password', ''), PGSSLMODE=info.get('sslmode', 'disable'))
        started = time.monotonic()
        with stem.with_suffix('.stdout').open('w') as stdout, stem.with_suffix('.stderr').open('w') as stderr:
            result = subprocess.run(command, cwd=self.out, env=env, stdout=stdout, stderr=stderr,
                                    timeout=duration + 180)
        output = stem.with_suffix('.stdout').read_text()
        error = stem.with_suffix('.stderr').read_text()
        values, errors = [], Counter()
        for path in self.work.glob(stem.name + '.*'):
            if path.suffix in ('.stdout', '.stderr'):
                continue
            with path.open() as log:
                samples, failures = parse_log(log, rate is not None)
            values.extend(samples); errors.update(failures)
            if self.a.keep_raw:
                raw = self.out / 'raw'; raw.mkdir(exist_ok=True)
                with path.open('rb') as src, gzip.open(raw / (path.name + '.gz'), 'wb') as dst:
                    shutil.copyfileobj(src, dst)
            path.unlink()
        processed = re.search(r'number of transactions actually processed: (\d+)', output)
        tps = re.search(r'^tps = ([\d.]+)', output, re.M)
        call = dict(gateway=k, command=command, returncode=result.returncode, stdout=output,
                    stderr=error, wall_seconds=time.monotonic() - started,
                    processed=int(processed[1]) if processed else None, samples=len(values))
        self.calls.append(call)
        # Aborted clients can leave pgbench exit status zero; its stderr is evidence too.
        bad = result.returncode != 0 or 'aborted' in error or not processed or int(processed[1]) != len(values)
        return values, errors, float(tps[1]) if tps else 0, bad

    def run(self, kind, clients=None, offered=None, duration=None, role=None, ids=None):
        ids = ids or self.ids
        self.script(kind)
        with ThreadPoolExecutor(len(ids)) as pool:
            results = list(pool.map(lambda k: self.gateway(k, kind, duration or self.a.duration,
                clients or self.a.clients_per_gateway, None if offered is None else offered / len(ids), role), ids))
        values, errors = [], Counter()
        for samples, failures, _, _ in results:
            values.extend(samples); errors.update(failures)
        point = summarize(values, errors, sum(r[2] for r in results))
        point['process_errors'] = sum(r[3] for r in results)
        return point

    def sweep(self, kind):
        curve, best, rate = [], None, self.a.start_rate
        while True:
            warmup = self.run(kind, offered=rate, duration=self.a.warmup) if self.a.warmup else None
            point = self.run(kind, offered=rate)
            reason = stop_reason(point, rate, self.a.p99_ms, self.a.max_rate)
            point.update(offered_tps=rate, stop=reason, warmup=warmup)
            curve.append(point)
            if reason in (None, 'max_rate'):
                best = {key: val for key, val in point.items() if key != 'warmup'}
            if reason:
                break
            rate = min(rate * self.a.step, self.a.max_rate)
        return dict(highest_passing=best, curve=curve)

    def hotkey(self):
        with self.connect() as conn:
            conn.execute('UPDATE bench_w SET v = 0 WHERE id = 1')
        result = self.run('hotkey')
        with self.connect() as conn:
            result['final_v'] = conn.execute('SELECT v FROM bench_w WHERE id = 1').fetchone()[0]
        result['count_matches'] = result['final_v'] == result['committed']
        return result

    def admission(self):
        import psycopg
        from psycopg import sql
        roles = [(name, secrets.token_hex(24)) for name in ('bench_a', 'bench_b')]
        with self.connect() as conn:
            for name, password in roles:
                conn.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(sql.Identifier(name), sql.Literal(password)))
                conn.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(sql.Identifier(name)))
                conn.execute(sql.SQL('GRANT SELECT ON bench_w TO {}').format(sql.Identifier(name)))
        baseline = self.run('read', offered=100 * len(self.ids), role=roles[1])
        query = 'SELECT pg_sleep(0.05)'
        with self.connect() as conn:
            try:
                conn.execute(query)
            except psycopg.Error as exc:
                if exc.sqlstate not in ('42883', '0A000'):
                    raise
                query = 'SELECT sum(v) FROM bench_w'
                conn.execute(query)
        errors, executions, latencies = Counter(), Counter(), []
        stop, lock = threading.Event(), threading.Lock()
        limits = {}
        for k in self.ids:
            limit = dict(concurrent=16, queue=256, wait_ms=30000)
            source = 'server defaults (custom remote config requires --role-statement-limit)'
            if self.cluster:
                path = Path(self.cluster.node(k)['dir']) / f'{self.cluster.mode}.json'
                if path.is_file():
                    limit.update(json.loads(path.read_text()).get('role_statements', {}))
                    source = 'node config role_statements'
            if self.a.role_statement_limit is not None:
                limit['concurrent'] = self.a.role_statement_limit
                source = '--role-statement-limit'
            limits[k] = dict(**limit, source=source)
        clients = {k: 4 * limits[k]['concurrent'] for k in self.ids}
        def flood(k):
            info = self.info(k); info.update(user=roles[0][0], password=roles[0][1])
            local_errors, count, samples = Counter(), 0, []
            try:
                with psycopg.connect(**info, autocommit=True, connect_timeout=10) as conn:
                    while not stop.is_set():
                        started = time.monotonic()
                        try:
                            conn.execute(query); count += 1
                            samples.append((time.monotonic() - started) * 1000)
                        except psycopg.Error as exc:
                            local_errors[exc.sqlstate or 'connection'] += 1
                            if not exc.sqlstate:
                                break
            except psycopg.Error as exc:
                local_errors[exc.sqlstate or 'connection'] += 1
            with lock:
                errors.update(local_errors); executions.update(committed=count); latencies.extend(samples)
        try:
            with ThreadPoolExecutor(sum(clients.values())) as pool:
                futures = [pool.submit(flood, k) for k in self.ids for _ in range(clients[k])]
                # Let connections settle while A is already executing; B then measures the overlap.
                stop.wait(1)
                try:
                    during = self.run('read', offered=100 * len(self.ids), role=roles[1])
                finally:
                    stop.set()
                for future in futures:
                    future.result()
            return dict(baseline=baseline, during_flood=during, flood_query=query,
                        clients_per_gateway=clients, role_statement_limits=limits,
                        flood=summarize(latencies, errors, None))
        finally:
            if not self.cluster:  # Cluster reset removes roles along with the data.
                with self.connect() as conn:
                    for name, _ in roles:
                        conn.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(name)))
                        conn.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(name)))

    def execute(self):
        failed = False
        try:
            for mode in self.a.modes.split(',') if self.cluster else ['pg']:
                if self.cluster:
                    self.cluster.reset(mode)
                self.ids = ([self.cluster.leader()] if mode == 'replication' else
                            [n['id'] for n in self.cluster.nodes]) if self.cluster else [1]
                fill = self.prepare()
                result = dict(mode=mode, pgbench_version=self.version, log_format=LOG_FORMAT,
                              settings=vars(self.a) | {'dsn': '<redacted>'}, fill=fill, tests={})
                self.calls = []
                for test in self.a.tests.split(','):
                    if test == 'latency':
                        value = {kind: self.run(kind, clients=1, ids=self.ids[:1]) for kind in ('write', 'indexed')}
                    elif test == 'throughput':
                        value = {kind: self.sweep(kind) for kind in ('write', 'indexed', 'read')}
                    else:
                        value = getattr(self, test)()
                    result['tests'][test] = value
                    failed |= test == 'hotkey' and not value['count_matches']
                result['calls'] = self.calls
                failed |= any(c['returncode'] or 'aborted' in c['stderr'] or c['processed'] != c['samples'] for c in self.calls)
                result['passed'] = not failed
                (self.out / f'bench-{mode}.json').write_text(json.dumps(result, indent=2) + '\n', newline='\n')
                with (self.out / 'summary.md').open('a', newline='\n') as summary:
                    summary.write(f'- bench-{mode}: {json.dumps(result["tests"])}; passed={result["passed"]}\n')
            return int(failed)
        finally:
            shutil.rmtree(self.work)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--target', choices=['cluster', 'pg'], default='cluster')
    for name in ('cluster', 'dsn', 'out', 'pgbench'):
        p.add_argument('--' + name, required=name in ('out', 'pgbench'))
    p.add_argument('--pgbench-prefix', default='')
    p.add_argument('--pgbench-host-override')
    p.add_argument('--tests', default='latency,throughput,hotkey,admission')
    p.add_argument('--modes', default='ranges,single_range,replication')
    for name, default in [('duration', 60), ('warmup', 15), ('rows', 100000), ('fill-chunk', 10000), ('clients-per-gateway', 16), ('role-statement-limit', None)]:
        p.add_argument('--' + name, type=int, default=default)
    for name, default in [('p99-ms', 20), ('start-rate', 200), ('step', 1.5), ('max-rate', 50000)]:
        p.add_argument('--' + name, type=float, default=default)
    p.add_argument('--keep-raw', action='store_true')
    a = p.parse_args()
    if (not (a.cluster if a.target == 'cluster' else a.dsn) or a.duration < 1 or a.warmup < 0 or
        min(a.rows, a.fill_chunk, a.clients_per_gateway, a.start_rate, a.p99_ms) <= 0 or
        (a.role_statement_limit is not None and a.role_statement_limit < 1) or
        a.step <= 1 or a.max_rate < a.start_rate or
        not set(a.tests.split(',')) <= {'latency', 'throughput', 'hotkey', 'admission'} or
        not set(a.modes.split(',')) <= {'ranges', 'single_range', 'replication'}):
        p.error('invalid target, tests, modes or numeric limits')
    try:
        return Bench(a).execute()
    except Exception as exc:
        print(f'infrastructure: {exc}', file=sys.stderr)
        traceback.print_exc()
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
