#!/usr/bin/env python3
"""Application latency bound under agent load (GD-0061 R3-404, design-3 section 7).

One real `giggles-server` process; application and agent load from separate
client processes, so client-side interpreter locks never couple the two.

  setup    `--dev --trust-local` server: schema, rows, index, executor role, tasks.
  warmup   config-file server (PostgreSQL listener + agent listener); app only.
  phase A  application workload, no agent sessions.
  phase B  the same, plus `agent.max_concurrent_statements` agent sessions over
           four tasks, every session looping over the three limit steps.
  phase C  the same with 32 tasks (one session each).

The bound in `agent_load_bound.json` is checked per application operation:
p99(B) <= max(ratio * p99(A), p99(A) + slack_ms), the same for C, and zero
application errors. Exit 0 = pass, 2 = application errors, 3 = bound failed,
1 = harness failure.
"""
import sys
sys.dont_write_bytecode = True
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import argparse
import array
import datetime
import fractions
import json
import math
import os
import random
import secrets
import shutil
import socket
import ssl
import struct
import subprocess
import threading
import time
import traceback
import uuid

SCHEMA_ID = 'gigglesdb.agent-load/1'
HERE = Path(__file__).resolve().parent
BOUND_FILE = HERE / 'agent_load_bound.json'
WORK_BASE = Path('J:/gigglesdb-work')
OPERATIONS = ('select', 'update', 'insert')
PHASES = ('A', 'B', 'C')
SUMMARY_KEYS = {50: 'p50_ms', 99: 'p99_ms', 99.9: 'p999_ms'}
EXIT_PASS, EXIT_HARNESS, EXIT_APP_ERRORS, EXIT_BOUND = 0, 1, 2, 3

TABLE = 'shipments'
EXECUTOR = 'agent_exec'
DATABASE = 'postgres'
OWNER = 'postgres'
BLOCK = 1000                 # rows per merchant; ids of one merchant are contiguous
LOAD_BATCH = 500             # rows per INSERT while loading
PAGE = 100                   # rows per agent page (step 3)
APP_CONNS_PER_PROCESS = 2
AGENT_SESSIONS_PER_PROCESS = 8
BACKOFF_START_MS = 10
BACKOFF_CAP_MS = 200
STATUSES = ('delivered', 'pending', 'queued', 'failed')
# Per-task limits. A full scan of the table always passes rows_scanned; one
# page of PAGE rows of the task's own block never does. Step 1 charges rows
# read but returns nothing, so rows read is set out of reach and step 3 meets
# the rows-returned cap, which is below one block, inside the block. Step 1
# keeps scanning to its limit for the whole phase.
TASK_LIMITS = dict(statement_ms=2000, rows_scanned=500, concurrent_statements=2, lock_wait_ms=250)
DISCLOSE_ROWS_READ = 1000000000
DISCLOSE_ROWS_RETURNED = 600
DISCLOSE_BYTES = 1048576
SERVICE_LIMITS = dict(sessions=128, burst=1000000, per_second=1000000)
# The shared request bucket (ServiceLimits, default 4096/s for every listener
# together) is raised so the benchmark measures statement isolation, not the
# connection-closing rate limiter.


# ---------------------------------------------------------------- statistics

def percentile(sorted_values, p):
    """Nearest-rank percentile of an ascending sequence; None when empty."""
    n = len(sorted_values)
    if n == 0:
        return None
    rank = math.ceil(fractions.Fraction(str(p)) * n / 100)
    return sorted_values[min(max(rank, 1), n) - 1]


def summarize(values, seconds=None):
    ordered = sorted(values)
    out = dict(count=len(ordered))
    for p, key in SUMMARY_KEYS.items():
        v = percentile(ordered, p)
        out[key] = None if v is None else round(v, 4)
    out['max_ms'] = round(ordered[-1], 4) if ordered else None
    if seconds:
        out['per_second'] = round(len(ordered) / seconds, 1)
    return out


def load_bound(path=BOUND_FILE):
    bound = json.loads(Path(path).read_text(encoding='utf-8'))
    for key in ('percentile', 'ratio', 'slack_ms', 'max_application_errors', 'checked_phases', 'operations'):
        if key not in bound:
            raise ValueError('bound file lacks ' + key)
    if bound['percentile'] not in SUMMARY_KEYS:
        raise ValueError('bound percentile must be one of 50, 99, 99.9')
    return bound


def check_bound(phases, bound):
    """The pass/fail verdict of design-3 section 7 for measured phase summaries."""
    key = SUMMARY_KEYS[bound['percentile']]
    ratio, slack = float(bound['ratio']), float(bound['slack_ms'])
    results, passed, ratios = [], True, []
    for phase in bound['checked_phases']:
        for op in bound['operations']:
            base = phases['A']['operations'].get(op, {}).get(key)
            got = phases[phase]['operations'].get(op, {}).get(key)
            if base is None or got is None:
                results.append(dict(phase=phase, operation=op, baseline_ms=base, measured_ms=got,
                                    limit_ms=None, ratio=None, passed=False, reason='no samples'))
                passed = False
                continue
            limit = max(ratio * base, base + slack)
            measured_ratio = round(got / base, 4) if base > 0 else None
            ok = got <= limit
            passed &= ok
            if measured_ratio is not None:
                ratios.append(measured_ratio)
            results.append(dict(phase=phase, operation=op, baseline_ms=base, measured_ms=got,
                                limit_ms=round(limit, 4), ratio=measured_ratio, passed=ok))
    errors = sum(sum(phases[p]['application_errors'].values()) for p in phases)
    errors_ok = errors <= bound['max_application_errors']
    return dict(passed=passed and errors_ok, latency_passed=passed, application_errors=errors,
                application_errors_passed=errors_ok, percentile=bound['percentile'], ratio=ratio,
                slack_ms=slack, max_ratio=max(ratios) if ratios else None, results=results)


def exit_code(check):
    if not check['application_errors_passed']:
        return EXIT_APP_ERRORS
    return EXIT_PASS if check['latency_passed'] else EXIT_BOUND


def assemble_report(mode, commit, started_at, config, bound, phases, **extra):
    """The agent-load.json document: summaries, the bound used and its verdict."""
    check = check_bound(phases, bound)
    report = dict(schema=SCHEMA_ID, mode=mode, commit=commit, started_at=started_at, finished_at=utc(),
                  config=config, bound=bound, phases=phases, check=check, exit=exit_code(check))
    report.update(extra)
    return report


def validate_report(report):
    """Problems with a report's shape; [] when it matches SCHEMA_ID."""
    problems = []
    def need(obj, key, kind, where):
        if not isinstance(obj, dict) or key not in obj:
            problems.append(f'{where}.{key} missing')
            return None
        if kind is not None and not isinstance(obj[key], kind):
            problems.append(f'{where}.{key} is not {kind}')
        return obj[key]
    if need(report, 'schema', str, 'report') != SCHEMA_ID:
        problems.append('report.schema is not ' + SCHEMA_ID)
    need(report, 'mode', str, 'report')
    need(report, 'commit', (str, type(None)), 'report')
    need(report, 'started_at', str, 'report')
    need(report, 'finished_at', str, 'report')
    need(report, 'config', dict, 'report')
    need(report, 'bound', dict, 'report')
    need(report, 'exit', int, 'report')
    phases = need(report, 'phases', dict, 'report') or {}
    for name in PHASES:
        phase = need(phases, name, dict, 'phases')
        if not isinstance(phase, dict):
            continue
        need(phase, 'seconds', (int, float), name)
        need(phase, 'application_errors', dict, name)
        agents = need(phase, 'agents', dict, name)
        if isinstance(agents, dict):
            for key in ('tasks', 'sessions', 'statements', 'outcomes'):
                need(agents, key, None, name + '.agents')
            for key in ('backoffs', 'backoff_ms'):
                need(agents, key, int, name + '.agents')
        ops = need(phase, 'operations', dict, name) or {}
        for op in OPERATIONS:
            summary = need(ops, op, dict, name + '.operations')
            if isinstance(summary, dict):
                for key in ('count', 'p50_ms', 'p99_ms', 'p999_ms', 'max_ms'):
                    need(summary, key, None, f'{name}.{op}')
    check = need(report, 'check', dict, 'report')
    if isinstance(check, dict):
        for key, kind in (('passed', bool), ('application_errors', int), ('results', list),
                          ('max_ratio', (int, float, type(None)))):
            need(check, key, kind, 'check')
    return problems


# ---------------------------------------------------------------- pgwire client

class PgError(Exception):
    def __init__(self, sqlstate, message):
        super().__init__(f'{sqlstate}: {message}')
        self.sqlstate, self.message = sqlstate, message


class Pg:
    """Minimal PostgreSQL wire client: trust or certificate startup, simple query."""

    def __init__(self, port, user, database, params=(), tls=None, timeout=60.0):
        sock = socket.create_connection(('127.0.0.1', port), timeout=timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if tls is not None:
            sock.sendall(struct.pack('!ii', 8, 80877103))
            if sock.recv(1) != b'S':
                sock.close()
                raise ConnectionError('server refused TLS')
            sock = tls.wrap_socket(sock, server_hostname='localhost')
        self.sock, self.buf = sock, bytearray()
        body = struct.pack('!i', 196608)
        for key, value in (('user', user), ('database', database), *params):
            body += key.encode() + b'\0' + value.encode() + b'\0'
        body += b'\0'
        self.sock.sendall(struct.pack('!i', len(body) + 4) + body)
        while True:
            tag, data = self._message()
            if tag == b'R':
                code = struct.unpack('!i', data[:4])[0]
                if code != 0:
                    self.close()
                    raise ConnectionError(f'authentication method {code} is not supported')
            elif tag == b'E':
                self.close()
                raise _error(data)
            elif tag == b'Z':
                return

    def _fill(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError('server closed the connection')
            self.buf += chunk

    def _message(self):
        self._fill(5)
        tag, length = bytes(self.buf[:1]), struct.unpack('!i', self.buf[1:5])[0]
        self._fill(1 + length)
        data = bytes(self.buf[5:1 + length])
        del self.buf[:1 + length]
        return tag, data

    def query(self, sql):
        """Rows (lists of text or None) and the command tag; PgError on an ErrorResponse."""
        body = sql.encode('utf-8') + b'\0'
        self.sock.sendall(b'Q' + struct.pack('!i', len(body) + 4) + body)
        rows, tag, error = [], None, None
        while True:
            kind, data = self._message()
            if kind == b'D':
                count, at, row = struct.unpack('!h', data[:2])[0], 2, []
                for _ in range(count):
                    size = struct.unpack('!i', data[at:at + 4])[0]
                    at += 4
                    if size < 0:
                        row.append(None)
                    else:
                        row.append(data[at:at + size].decode('utf-8'))
                        at += size
                rows.append(row)
            elif kind == b'C':
                tag = data.rstrip(b'\0').decode()
            elif kind == b'E':
                error = _error(data)
            elif kind == b'Z':
                if error:
                    raise error
                return rows, tag

    def close(self):
        try:
            self.sock.sendall(b'X\0\0\0\4')
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def _error(data):
    fields = {}
    for part in data.split(b'\0'):
        if part:
            fields[part[:1]] = part[1:].decode('utf-8', 'replace')
    return PgError(fields.get(b'C', 'XX000'), fields.get(b'M', ''))


def agent_tls(ca, cert, key):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(ca)
    context.load_cert_chain(cert, key)
    return context


# ---------------------------------------------------------------- workload

def block_of(merchant):
    return (merchant - 1) * BLOCK + 1, merchant * BLOCK


def row_values(i):
    merchant = (i - 1) // BLOCK + 1
    status = 'failed' if (i - 1) % BLOCK == 0 else STATUSES[i % len(STATUSES)]
    at = datetime.datetime(2026, 11, 5, 17, 0, tzinfo=datetime.timezone.utc) + datetime.timedelta(seconds=i % 86400)
    return (f"({i},{merchant},'{status}','{at:%Y-%m-%d %H:%M:%S}+00',{i % 5},"
            f"'customer{i}@example.com','tok_{i:08x}',NULL)")


def task_plan(rows, b_tasks, c_tasks):
    """Task names, merchants and held rows for phases B and C."""
    merchants = max(1, rows // BLOCK)
    plan = {}
    for phase, count, offset in (('B', b_tasks, 0), ('C', c_tasks, b_tasks)):
        tasks = []
        for k in range(count):
            merchant = 1 + (offset + k) % merchants
            lo, hi = block_of(merchant)
            tasks.append(dict(name=f'load-{phase.lower()}-{k}', merchant=merchant, lo=lo,
                              hi=min(hi, rows), held=lo))
        plan[phase] = tasks
    return plan


def create_task_sql(task, expires):
    return (f'CREATE TASK "{task["name"]}" EXECUTOR {EXECUTOR} ON {TABLE} '
            f'VISIBLE ROWS WHERE (merchant_id = {task["merchant"]}) '
            f'READABLE (id, status, failed_at, attempt_count) MASKED (customer_email) '
            f"ALLOW UPDATE (status) OLD ROW WHERE (status = 'failed') NEW ROW WHERE (status = 'queued') "
            f'AT MOST 1000 DISTINCT ROWS AND 1000 TRANSITIONS APPROVE ABOVE 1000 '
            f'DISCLOSE AT MOST {DISCLOSE_ROWS_READ} ROWS READ, {DISCLOSE_ROWS_RETURNED} ROWS RETURNED, '
            f'{DISCLOSE_BYTES} BYTES RETURNED '
            'WITH (' + ', '.join(f'{k} = {v}' for k, v in TASK_LIMITS.items()) + ') '
            f"EXPIRES '{expires}'")


def app_statement(op, rng, rows, held, insert_id):
    if op == 'insert':
        return (f"INSERT INTO {TABLE} VALUES ({insert_id},0,'pending','2026-11-05 17:00:00+00',0,"
                f"'new{insert_id}@example.com','tok_new_{insert_id}',NULL)")
    key = rng.randint(1, rows)
    while key in held:
        key = rng.randint(1, rows)
    if op == 'select':
        return (f'SELECT id, merchant_id, status, failed_at, attempt_count, customer_email, '
                f'card_token, note FROM {TABLE} WHERE id = {key}')
    return f"UPDATE {TABLE} SET attempt_count = attempt_count + 1, note = 'app' WHERE id = {key}"


def choose(rng, mix):
    point, total = rng.random() * sum(mix.values()), 0
    for op in OPERATIONS:
        total += mix[op]
        if point < total:
            return op
    return OPERATIONS[-1]


def app_thread(spec, index, start_at, end_at, out, lock):
    rng = random.Random(f"{spec['seed']}:{index}")
    held = set(spec['held'])
    lat = {op: array.array('d') for op in OPERATIONS}
    errors, samples, inserted = {}, [], 0
    conn = spec['_conns'][index]
    while time.time() < start_at:
        time.sleep(min(.01, max(0, start_at - time.time())))
    while time.time() < end_at:
        op = choose(rng, spec['mix'])
        insert_id = spec['insert_base'] + index * 10**7 + inserted
        sql = app_statement(op, rng, spec['rows'], held, insert_id)
        begin = time.perf_counter_ns()
        try:
            conn.query(sql)
        except PgError as e:
            key = f'{op}:{e.sqlstate}'
            errors[key] = errors.get(key, 0) + 1
            if len(samples) < 5:
                samples.append(f'{key}: {e.message[:200]}')
            continue
        except (OSError, ConnectionError) as e:
            key = f'{op}:connection'
            errors[key] = errors.get(key, 0) + 1
            samples.append(f'{key}: {e}')
            break
        lat[op].append((time.perf_counter_ns() - begin) / 1e6)
        if op == 'insert':
            inserted += 1
    with lock:
        for op in OPERATIONS:
            out['lat'][op].extend(lat[op])
        for key, n in errors.items():
            out['errors'][key] = out['errors'].get(key, 0) + n
        out['samples'].extend(samples)


BUDGETS = {
    'rows scanned by this statement': 'rows_scanned',
    'rows read by this task': 'rows_read',
    'rows returned to this task': 'rows_returned',
    'bytes returned to this task': 'bytes_returned',
    'load of all agent sessions': 'all_agents',
    'concurrent statements of this task': 'concurrent_statements',
    # The agent statement ceiling refuses before a task statement runs (R3-403).
    'task budget exceeded': 'agent_ceiling',
}


def outcome(error):
    """SQLSTATE, plus the budget for 54T01 (e.g. `54T01/rows_scanned`)."""
    if error.sqlstate != '54T01':
        return error.sqlstate
    text = error.message.split('budget exceeded: ', 1)[-1].strip()
    return '54T01/' + BUDGETS.get(text, 'other')


def backoff_delay(attempt, rng):
    """Jittered exponential refusal delay in seconds, capped before exponentiation."""
    base = min(BACKOFF_CAP_MS, BACKOFF_START_MS * 2 ** min(attempt, 5))
    return rng.uniform(base / 2, base) / 1000


def agent_session(spec, session, tls, start_at, end_at, out, lock):
    counts, messages = {}, {}
    rng = random.Random(session['name'])
    attempt, backoffs, backoff_seconds = 0, 0, 0.0
    def record(step, outcome, message=None):
        key = f'{step}:{outcome}'
        counts[key] = counts.get(key, 0) + 1
        if message is not None and key not in messages:
            messages[key] = message[:200]
    conn = session['_conn']
    while time.time() < start_at:
        time.sleep(min(.01, max(0, start_at - time.time())))
    def run(step, sql):
        nonlocal conn, attempt, backoffs, backoff_seconds
        try:
            if conn is None:
                conn = Pg(spec['port'], EXECUTOR, DATABASE, (('giggles.task', session['name']),), tls=tls)
            rows, _ = conn.query(sql)
            record(step, 'ok')
            attempt = 0
            return rows
        except PgError as e:
            refused = outcome(e)
            record(step, refused, e.message)
            if refused.startswith('54T01') or refused == '55P03':
                delay = min(backoff_delay(attempt, rng), max(0, end_at - time.time()))
                time.sleep(delay)
                backoffs += 1
                backoff_seconds += delay
                attempt += 1
            return None
        except (OSError, ConnectionError):
            record(step, 'disconnect')
            if conn is not None:
                conn.close()
            conn = None
            time.sleep(.05)
            return None
    while time.time() < end_at:
        # 1. Unfiltered SELECT: the rows-scanned limit. The sort reads the whole
        # table before any row is sent, so the step never charges rows returned.
        run('scan', f'SELECT id, status FROM {TABLE} ORDER BY attempt_count, id')
        if time.time() >= end_at:
            break
        # 2. UPDATE of a row the harness holds locked: the lock wait.
        run('lock', f"OPERATION 'ld-{uuid.uuid4().hex}' UPDATE {TABLE} SET status = 'queued' "
                    f"WHERE id = {session['held']} AND status = 'failed'")
        # 3. Paged SELECT over the task's block: the disclosure cap.
        x = session['lo'] - 1
        while time.time() < end_at:
            rows = run('page', f'SELECT id, status FROM {TABLE} WHERE id > {x} AND id <= {x + PAGE}')
            if rows is None:
                break
            x += PAGE
            if x >= session['hi']:
                record('page', 'block_end')
                break
    if conn is not None:
        conn.close()
    with lock:
        out['backoffs'] += backoffs
        out['backoff_seconds'] += backoff_seconds
        for key, n in counts.items():
            out['outcomes'][key] = out['outcomes'].get(key, 0) + n
        for key, text in messages.items():
            out['messages'].setdefault(key, text)


def worker_main(spec_path):
    """Client process: connect, report ready, wait for the start time, run, write results."""
    spec = json.loads(Path(spec_path).read_text(encoding='utf-8'))
    out_dir = Path(spec['out'])
    lock = threading.Lock()
    threads = []
    if spec['kind'] == 'app':
        spec['_conns'] = [Pg(spec['port'], OWNER, DATABASE) for _ in range(spec['connections'])]
        out = dict(lat={op: array.array('d') for op in OPERATIONS}, errors={}, samples=[])
    else:
        tls = agent_tls(spec['ca'], spec['cert'], spec['key'])
        for session in spec['sessions']:
            session['_conn'] = Pg(spec['port'], EXECUTOR, DATABASE, (('giggles.task', session['name']),), tls=tls)
        out = dict(outcomes={}, messages={}, backoffs=0, backoff_seconds=0.0)
    print('ready', flush=True)
    start_at = float(sys.stdin.readline())
    end_at = start_at + spec['seconds']
    if spec['kind'] == 'app':
        for i in range(spec['connections']):
            threads.append(threading.Thread(target=app_thread, args=(spec, i, start_at, end_at, out, lock)))
    else:
        for session in spec['sessions']:
            threads.append(threading.Thread(target=agent_session, args=(spec, session, tls, start_at, end_at, out, lock)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if spec['kind'] == 'app':
        for conn in spec['_conns']:
            conn.close()
        for op in OPERATIONS:
            with (out_dir / f'lat-{op}.bin').open('wb') as f:
                out['lat'][op].tofile(f)
        result = dict(errors=out['errors'], samples=out['samples'],
                      counts={op: len(out['lat'][op]) for op in OPERATIONS})
    else:
        result = dict(outcomes=out['outcomes'], messages=out['messages'],
                      backoffs=out['backoffs'], backoff_seconds=out['backoff_seconds'])
    (out_dir / 'result.json').write_text(json.dumps(result, indent=1), encoding='utf-8')
    print('done', flush=True)
    return 0


# ---------------------------------------------------------------- orchestration

def under_work_base(path):
    from compat_lib.util import output_root
    path = output_root(path)
    if os.name == 'nt' and not path.is_relative_to(WORK_BASE.resolve()):
        raise ValueError('work directory must be under J:/gigglesdb-work')
    return path


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')


def git_commit(repo):
    try:
        out = subprocess.run(['git', '-C', str(repo), 'rev-parse', 'HEAD'], capture_output=True,
                             text=True, timeout=30)
        sha = out.stdout.strip()
        return sha if out.returncode == 0 and len(sha) == 40 else None
    except OSError:
        return None


class ConfigServer:
    """`giggles-server CONFIG.json` on the frozen binary of a compat_lib Server."""

    def __init__(self, base, config_path):
        self.base, self.config_path, self.process = base, config_path, None

    def start(self):
        from compat_lib.util import redact
        base = self.base
        (base.run/'cwd').mkdir(exist_ok=True)
        self.stdout_log = (base.run/'server.stdout.log').open('w', encoding='utf-8')
        self.stderr_log = (base.run/'server.stderr.log').open('w', encoding='utf-8')
        self.process = subprocess.Popen([str(base.binary), str(self.config_path)], cwd=base.run/'cwd',
                                        env=base.env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.stderr_log, text=True, encoding='utf-8', errors='replace')
        base.process = self.process
        base.thread = base.err_thread = None  # this class drains the streams itself
        lines = []
        ready = threading.Event()
        def drain():
            for line in self.process.stdout:
                self.stdout_log.write(redact(line)); self.stdout_log.flush()
                if not ready.is_set():
                    lines.append(line.rstrip())
                    if 'preview=' in line:
                        ready.set()
            ready.set()
        threading.Thread(target=drain, daemon=True).start()
        if not ready.wait(120) or not lines or 'preview=' not in lines[-1]:
            raise RuntimeError('config server did not print its listener banner; see ' + str(base.run))
        tokens = dict(part.split('=', 1) for part in lines[-1].split() if '=' in part)
        base.meta['stdout_banner'] = lines[-1]
        return {k: int(v.rsplit(':', 1)[1]) for k, v in tokens.items() if ':' in v}

    def stop(self):
        self.base.stop()
        for log in (self.stdout_log, self.stderr_log):
            log.close()


class Run:
    def __init__(self, args, bound):
        self.args, self.bound = args, bound
        self.root = under_work_base(args.work) / ('run-' + time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:8])
        self.root.mkdir(parents=True)
        (self.root/'tmp').mkdir()
        self.env = dict(os.environ, TEMP=str(self.root/'tmp'), TMP=str(self.root/'tmp'), TMPDIR=str(self.root/'tmp'))
        self.data = self.root/'data'
        self.log = (self.root/'harness.log').open('w', encoding='utf-8')
        self.plan = task_plan(args.rows, args.phase_b_tasks, args.phase_c_tasks)
        self.held = sorted({t['held'] for p in self.plan.values() for t in p})

    def say(self, text):
        line = f'{utc()} {text}'
        print(line, flush=True)
        self.log.write(line + '\n'); self.log.flush()

    def setup(self):
        from compat_lib.gigglesdb import Server
        run = self.root/'server-setup'
        run.mkdir()
        server = Server(self.args.server_bin, run, self.env, secrets.token_hex(12),
                        ['--data', str(self.data), '--trust-local', '--no-api'])
        try:
            port = server.start()
            c = Pg(port, OWNER, DATABASE)
            c.query(f'CREATE TABLE {TABLE} (id bigint primary key, merchant_id bigint, status text, '
                    'failed_at timestamptz, attempt_count int, customer_email text, card_token text, note text)')
            began = time.monotonic()
            for lo in range(1, self.args.rows + 1, LOAD_BATCH):
                hi = min(self.args.rows, lo + LOAD_BATCH - 1)
                c.query(f'INSERT INTO {TABLE} VALUES ' + ','.join(row_values(i) for i in range(lo, hi + 1)))
                if hi % 100000 == 0 or hi == self.args.rows:
                    self.say(f'loaded {hi}/{self.args.rows} rows')
            self.load_seconds = round(time.monotonic() - began, 1)
            c.query(f'CREATE INDEX shipments_merchant_status ON {TABLE} (merchant_id, status)')
            c.query(f'CREATE ROLE {EXECUTOR} LOGIN TASKONLY')
            c.query(f'GRANT SELECT, UPDATE ON {TABLE} TO {EXECUTOR}')
            expires = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S+00')
            statements = [create_task_sql(t, expires) for tasks in self.plan.values() for t in tasks]
            for sql in statements:
                c.query(sql)
            count = c.query(f'SELECT count(*) FROM {TABLE}')[0][0][0]
            if int(count) != self.args.rows:
                raise RuntimeError(f'loaded {count} rows, expected {self.args.rows}')
            c.close()
        finally:
            server.stop()
            (run/'bin'/self.args.server_bin.name).unlink(missing_ok=True)
        self.task_ddl = 'sql'
        self.say(f'setup done: {self.args.rows} rows in {self.load_seconds}s; tasks by {self.task_ddl}')

    def pki(self):
        from compat_lib import cluster, pins
        pki = self.root/'pki'
        cluster.generate_pki(pki, self.env, pins.reference(pins.load()['postgres']), nodes=2)
        self.ca, self.server_cert, self.server_key = pki/'ca.pem', pki/'node1.pem', pki/'node1.key'
        self.agent_cert, self.agent_key = pki/'node2.pem', pki/'node2.key'
        return cluster.der_fingerprint(self.agent_cert.read_text(encoding='utf-8'))

    def config(self, fingerprint):
        tasks = [t['name'] for p in self.plan.values() for t in p]
        config = {
            'database': str(self.data/'db'), 'native': '127.0.0.1:0', 'postgres': '127.0.0.1:0',
            'postgresql': {'listen': '127.0.0.1:0', 'tls': 'disabled', 'loopback_trust': True,
                           'databases': [DATABASE], 'connections': 64},
            'certificate': str(self.server_cert), 'private_key': str(self.server_key),
            'client_ca': str(self.ca), 'bindings': [], 'connections': 32,
            'service_limits': SERVICE_LIMITS, 'skew_seconds': 0, 'max_pause_seconds': 3600,
            'agent': {'postgres': '127.0.0.1:0', 'databases': [DATABASE], 'connections': 64,
                      'max_concurrent_statements': self.bound['agent_max_concurrent_statements'],
                      'bindings': [{'fingerprint': fingerprint, 'executor': EXECUTOR, 'tasks': tasks}]},
        }
        path = self.root/'server-config.json'
        path.write_text(json.dumps(config, indent=1), encoding='utf-8')
        return path

    def spawn(self, name, spec):
        folder = self.root/'clients'/name
        folder.mkdir(parents=True)
        spec = dict(spec, out=str(folder))
        (folder/'spec.json').write_text(json.dumps(spec, indent=1), encoding='utf-8')
        stderr = (folder/'stderr.log').open('w', encoding='utf-8')
        process = subprocess.Popen([sys.executable, '-I', str(Path(__file__).resolve()), '--worker', str(folder/'spec.json')],
                                   env=self.env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr,
                                   text=True, encoding='utf-8')
        return dict(name=name, folder=folder, process=process, stderr=stderr, kind=spec['kind'])

    def phase(self, label, seconds, agent_tasks, sessions_per_task, insert_slot):
        app_conns = self.args.connections
        workers = []
        holder = None
        try:
            if agent_tasks:
                holder = Pg(self.ports['postgresql'], OWNER, DATABASE)
                holder.query('BEGIN')
                _, tag = holder.query(f"UPDATE {TABLE} SET note = 'held by agent_load' WHERE id IN "
                                      f"({','.join(str(t['held']) for t in agent_tasks)})")
                expected = len({t['held'] for t in agent_tasks})
                if tag != f'UPDATE {expected}':
                    raise RuntimeError(f'lock holder updated {tag}, expected {expected}')
            for n in range(0, app_conns, APP_CONNS_PER_PROCESS):
                workers.append(self.spawn(f'{label}-app-{n // APP_CONNS_PER_PROCESS}', dict(
                    kind='app', port=self.ports['postgresql'], connections=min(APP_CONNS_PER_PROCESS, app_conns - n),
                    seconds=seconds, seed=f'{self.args.seed}:{label}:{n}', rows=self.args.rows, held=self.held,
                    mix=self.mix, insert_base=10**12 + insert_slot * 10**10 + n * 10**8)))
            sessions = [dict(name=t['name'], lo=t['lo'], hi=t['hi'], held=t['held'])
                        for t in agent_tasks for _ in range(sessions_per_task)]
            for n in range(0, len(sessions), AGENT_SESSIONS_PER_PROCESS):
                workers.append(self.spawn(f'{label}-agent-{n // AGENT_SESSIONS_PER_PROCESS}', dict(
                    kind='agent', port=self.ports['agent_postgres'], seconds=seconds,
                    sessions=sessions[n:n + AGENT_SESSIONS_PER_PROCESS], ca=str(self.ca),
                    cert=str(self.agent_cert), key=str(self.agent_key))))
            for w in workers:
                line = w['process'].stdout.readline().strip()
                if line != 'ready':
                    raise RuntimeError(f"client {w['name']} failed to start; see {w['folder']}")
            start_at = time.time() + 1.0
            for w in workers:
                w['process'].stdin.write(f'{start_at}\n'); w['process'].stdin.flush()
            for w in workers:
                try:
                    code = w['process'].wait(timeout=seconds + 180)
                except subprocess.TimeoutExpired:
                    raise RuntimeError(f"client {w['name']} did not finish; see {w['folder']}")
                if code:
                    raise RuntimeError(f"client {w['name']} exited {code}; see {w['folder']}")
        finally:
            for w in workers:
                if w['process'].poll() is None:
                    w['process'].kill(); w['process'].wait(timeout=30)
                for stream in (w['process'].stdin, w['process'].stdout):
                    try:
                        stream.close()
                    except OSError:
                        pass
                w['stderr'].close()
            if holder is not None:
                try:
                    holder.query('ROLLBACK')
                finally:
                    holder.close()
        lat = {op: array.array('d') for op in OPERATIONS}
        errors, samples, outcomes, messages = {}, [], {}, {}
        backoffs, backoff_seconds = 0, 0.0
        for w in workers:
            result = json.loads((w['folder']/'result.json').read_text(encoding='utf-8'))
            if w['kind'] == 'app':
                for op in OPERATIONS:
                    with (w['folder']/f'lat-{op}.bin').open('rb') as f:
                        lat[op].frombytes(f.read())
                for key, n in result['errors'].items():
                    errors[key] = errors.get(key, 0) + n
                samples.extend(result['samples'])
            else:
                backoffs += result['backoffs']
                backoff_seconds += result['backoff_seconds']
                for key, n in result['outcomes'].items():
                    outcomes[key] = outcomes.get(key, 0) + n
                for key, text in result['messages'].items():
                    messages.setdefault(key, text)
        summary = dict(
            seconds=seconds,
            operations={op: summarize(lat[op], seconds) for op in OPERATIONS},
            application_errors=errors, application_error_samples=samples[:20],
            agents=dict(tasks=len(agent_tasks), sessions=len(sessions), statements=sum(outcomes.values()),
                        backoffs=backoffs, backoff_ms=round(backoff_seconds * 1000),
                        outcomes=dict(sorted(outcomes.items())), messages=dict(sorted(messages.items()))))
        ops = ', '.join(f"{op} p99={summary['operations'][op]['p99_ms']}ms n={summary['operations'][op]['count']}" for op in OPERATIONS)
        self.say(f'phase {label}: {ops}; app errors={sum(errors.values())}; agent statements={summary["agents"]["statements"]}')
        return summary

    def execute(self):
        from compat_lib.gigglesdb import Server
        started = utc()
        self.mix = dict(self.bound['full']['mix'])
        self.setup()
        fingerprint = self.pki()
        config = self.config(fingerprint)
        run = self.root/'server-load'
        run.mkdir()
        base = Server(self.args.server_bin, run, self.env, secrets.token_hex(12))
        server = ConfigServer(base, config)
        phases = {}
        try:
            self.ports = server.start()
            for key in ('postgresql', 'agent_postgres'):
                if key not in self.ports:
                    raise RuntimeError(f'server banner has no {key} listener: {base.meta.get("stdout_banner")}')
            self.say(f'server listening: {base.meta["stdout_banner"]}')
            if self.args.warmup_seconds:
                warm = self.phase('warmup', self.args.warmup_seconds, [], 0, 0)
            ceiling = self.bound['agent_max_concurrent_statements']
            b_tasks = self.plan['B']
            phases['A'] = self.phase('A', self.args.phase_seconds, [], 0, 1)
            phases['B'] = self.phase('B', self.args.phase_seconds, b_tasks, max(1, ceiling // len(b_tasks)), 2)
            phases['C'] = self.phase('C', self.args.phase_seconds, self.plan['C'], 1, 3)
        finally:
            server.stop()
            (run/'bin'/self.args.server_bin.name).unlink(missing_ok=True)
        server_meta = dict(binary=str(self.args.server_bin), binary_sha256=base.meta.get('binary_sha256'),
                           banner=base.meta.get('stdout_banner'), exit=base.meta.get('exit'))
        config = dict(rows=self.args.rows, block_rows=BLOCK, load_seconds=self.load_seconds,
                      application_connections=self.args.connections, phase_seconds=self.args.phase_seconds,
                      warmup_seconds=self.args.warmup_seconds, mix=self.mix, page_rows=PAGE,
                      agent_max_concurrent_statements=self.bound['agent_max_concurrent_statements'],
                      tasks={p: [t['name'] for t in ts] for p, ts in self.plan.items()},
                      task_limits=TASK_LIMITS, disclose_rows_read=DISCLOSE_ROWS_READ,
                      disclose_rows_returned=DISCLOSE_ROWS_RETURNED, disclose_bytes=DISCLOSE_BYTES, service_limits=SERVICE_LIMITS,
                      seed=self.args.seed, task_ddl=self.task_ddl)
        report = assemble_report('smoke' if self.args.smoke else 'full', git_commit(HERE.parent), started,
                                 config, self.bound, phases, work_dir=str(self.root), server=server_meta)
        if self.args.warmup_seconds:
            report['warmup'] = warm
        return report

    def cleanup(self):
        if not self.args.keep_data and self.data.exists():
            shutil.rmtree(self.data, ignore_errors=True)
        shutil.rmtree(self.root/'tmp', ignore_errors=True)
        self.log.close()


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument('--server-bin', type=Path, default=os.environ.get('GIGGLES_SERVER_BIN'))
    p.add_argument('--smoke', action='store_true', help='10,000 rows and 5 s phases')
    p.add_argument('--work', type=Path, default=os.environ.get('AGENT_LOAD_WORK') or (WORK_BASE/'agent-load' if os.name == 'nt' else None))
    p.add_argument('--report', type=Path, help='default compat/reports/<sha8>/agent-load.json (smoke: in the run directory)')
    p.add_argument('--bound', type=Path, default=BOUND_FILE)
    p.add_argument('--rows', type=int)
    p.add_argument('--phase-seconds', type=float)
    p.add_argument('--warmup-seconds', type=float)
    p.add_argument('--connections', type=int)
    p.add_argument('--phase-b-tasks', type=int)
    p.add_argument('--phase-c-tasks', type=int)
    p.add_argument('--seed', type=int, default=404)
    p.add_argument('--keep-data', action='store_true')
    args = p.parse_args(argv)
    if args.server_bin is None or args.work is None:
        missing = ('--server-bin or GIGGLES_SERVER_BIN' if args.server_bin is None
                   else '--work or AGENT_LOAD_WORK on non-Windows systems')
        p.exit(EXIT_HARNESS, f'{p.prog}: required: {missing}\n')
    return args



def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ['--worker']:
        return worker_main(argv[1])
    args = parse_args(argv)
    bound = load_bound(args.bound)
    full, smoke = bound['full'], bound['smoke']
    defaults = dict(rows=smoke['rows'] if args.smoke else full['rows'],
                    phase_seconds=smoke['phase_seconds'] if args.smoke else full['phase_seconds'],
                    warmup_seconds=2 if args.smoke else 30,
                    connections=full['application_connections'],
                    phase_b_tasks=full['phase_b_tasks'], phase_c_tasks=full['phase_c_tasks'])
    for key, value in defaults.items():
        if getattr(args, key) is None:
            setattr(args, key, value)
    args.server_bin = args.server_bin.resolve()
    if not args.server_bin.is_file():
        print(f'no server binary at {args.server_bin}', file=sys.stderr)
        return EXIT_HARNESS
    run = Run(args, bound)
    try:
        report = run.execute()
    except Exception:
        run.say('harness failure:\n' + traceback.format_exc())
        run.cleanup()
        return EXIT_HARNESS
    if args.report:
        target = args.report
    elif args.smoke:
        target = run.root/'agent-load.json'
    else:
        target = HERE/'reports'/((report['commit'] or 'unknown')[:8])/'agent-load.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8', newline='\n')
    check = report['check']
    run.say(f"report {target}; passed={check['passed']} max_ratio={check['max_ratio']} "
            f"application_errors={check['application_errors']} exit={report['exit']}")
    for r in check['results']:
        run.say(f"  {r['phase']} {r['operation']}: p{check['percentile']} {r['measured_ms']} ms vs A "
                f"{r['baseline_ms']} ms, limit {r['limit_ms']} ms, ratio {r['ratio']}, passed={r['passed']}")
    run.cleanup()
    return report['exit']


if __name__ == '__main__':
    sys.exit(main())
