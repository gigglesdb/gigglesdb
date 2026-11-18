"""Real-process R3 safeguards runner.

Driver protocol v1: argv is --spec PATH. JSON spec fields: pg_port, agent_port,
database, ca, secret_file (JSON task-name -> secret), passwords (role -> password),
expires_ms, results_path, task_sql, suite (story|exit). Secrets are never passed on argv.
Results are atomically replaced JSON: {"steps": [{step, status, attempts, detail}],
"deviations": [str]}. Story step keys: 01..22 plus 13b; exit keys: race, stale, outcomes, lost_reply, undo.
A driver prints RESTART on stdout, flushes, and waits for RESTARTED on stdin.
The runner kills the server tree and reopens the same config before acknowledging.
Drivers run sequentially with separate databases; pgx is built once per run.
"""
import sys
sys.dont_write_bytecode = True
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import argparse
import datetime as dt
import json
import os
import queue
import re
import secrets
import subprocess
import threading
import time
import uuid
from agent_load import ConfigServer, Pg, under_work_base
from compat_lib import cluster, pins
from compat_lib.gigglesdb import Server

STEPS = [f'{n:02}' for n in range(1, 23)]
STEPS.insert(13, '13b')
EXIT_STEPS = ['race', 'stale', 'outcomes', 'lost_reply', 'undo']
SUITE_STEPS = {'story': STEPS, 'exit': EXIT_STEPS}
EXIT_TASKS = ['exit-race', 'exit-stale', 'exit-outcomes', 'exit-lost', 'exit-undo']
PASSWORDS = {'incident_controller': 'controller-pw', 'oncall_lead': 'lead-pw',
             'delivery_app': 'app-pw', 'repair_agent': 'agent-pw'}


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, default=str), encoding='utf-8', newline='\n')


DEFAULT_DRIVERS = 'psycopg,pgx'
PINNED_GO = Path('C:/Users/beach/go/go1.24.5/bin/go.exe')


def go_environment(work, root, inherited):
    env = dict(inherited, GOFLAGS='-mod=readonly')
    for key, folder in [('GOPATH', 'path'), ('GOMODCACHE', 'mod'), ('GOCACHE', 'cache')]:
        path = work/'go'/folder
        path.mkdir(parents=True, exist_ok=True)
        env[key] = str(path)
    temporary = root/'tmp'
    temporary.mkdir(parents=True, exist_ok=True)
    env.update(TEMP=str(temporary), TMP=str(temporary), PYTHONDONTWRITEBYTECODE='1')
    return env


def build_pgx(go, root, env):
    binary = root/'pgx-scenario.exe'
    with (root/'pgx-build.log').open('w', encoding='utf-8', newline='\n') as log:
        subprocess.run([go, 'build', '-o', str(binary), './'],
            cwd=Path(__file__).parent/'r3'/'pgx', env=env, stdout=log,
            stderr=subprocess.STDOUT, check=True)
    return binary


def timestamp(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f+00')


def task_sql(expires_ms):
    return f"""CREATE TASK "inc-436" EXECUTOR repair_agent APPROVER oncall_lead
ON shipments VISIBLE ROWS WHERE (merchant_id = 42)
READABLE (id, status, failed_at, attempt_count) MASKED (customer_email)
ALLOW UPDATE (status)
OLD ROW WHERE (status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00'
AND failed_at < '2026-11-05 17:07:00+00')
NEW ROW WHERE (status = 'queued') AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS
APPROVE ABOVE 5 DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED
WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2)
EXPIRES '{timestamp(expires_ms)}'"""


def make_config(data, pki, pg_port, agent_port):
    return {'database': str(data / 'db'), 'native': '127.0.0.1:0',
            'postgres': '127.0.0.1:0', 'certificate': str(pki / 'node1.pem'),
            'private_key': str(pki / 'node1.key'), 'client_ca': str(pki / 'ca.pem'),
            'bindings': [], 'connections': 32, 'skew_seconds': 0,
            'max_pause_seconds': 3600,
            'service_limits': {'sessions': 128, 'burst': 1000000, 'per_second': 1000000},
            'postgresql': {'listen': f'127.0.0.1:{pg_port}', 'tls': 'disabled',
                           'loopback_trust': False, 'databases': ['postgres'], 'connections': 64},
            'agent': {'postgres': f'127.0.0.1:{agent_port}', 'databases': ['postgres'],
                      'connections': 64, 'max_concurrent_statements': 16, 'bindings': []}}


def validate_steps(rows, suite='story'):
    if not isinstance(rows, list):
        return ['steps must be a list']
    errors, seen = [], []
    for r in rows:
        if not isinstance(r, dict):
            errors.append('step must be an object')
            continue
        seen.append(r.get('step'))
        if r.get('status') not in ('PASS', 'FAIL'):
            errors.append('invalid status')
        if type(r.get('attempts')) is not int or r['attempts'] < 1:
            errors.append('invalid attempts')
        if not isinstance(r.get('detail'), str):
            errors.append('invalid detail')
    if seen != SUITE_STEPS[suite]:
        errors.append('missing, duplicate or unordered steps')
    return errors


def parse_handshake(line):
    line = line.strip()
    if line == 'RESTART':
        return 'restart'
    if line == 'RESTARTED':
        return 'restarted'
    raise ValueError('unexpected handshake: ' + line)


def parse_spec(path):
    spec = json.loads(Path(path).read_text(encoding='utf-8'))
    for key in ('pg_port', 'agent_port', 'database', 'ca', 'secret_file',
                'passwords', 'expires_ms', 'results_path', 'task_sql'):
        if key not in spec:
            raise ValueError('spec lacks ' + key)
    for key in ('pg_port', 'agent_port'):
        if type(spec[key]) is not int or not 0 < spec[key] < 65536:
            raise ValueError('invalid port: ' + key)
    spec.setdefault('suite', 'story')
    if spec['suite'] not in SUITE_STEPS:
        raise ValueError('invalid suite')
    return spec


def report_exit(report):
    for driver in report['drivers'].values():
        if validate_steps(driver['steps'], report.get('suite', 'story')) or any(r['status'] != 'PASS' for r in driver['steps']):
            return 2
        if driver['workload'].get('unexpected_errors'):
            return 2
    return 0


def render_report(report):
    lines = ['# R3 safeguards: ' + report.get('suite', 'story') + ' suite', '', '| Driver | Step | Status | Attempts (retries) | Detail |',
             '|---|---|---|---|---|']
    for name, run in report['drivers'].items():
        for r in run['steps']:
            detail = r['detail'].replace('|', '/').replace('\n', ' ')
            lines.append(f"| {name} | {r['step']} | {r['status']} | {r['attempts']} ({r['attempts']-1}) | {detail} |")
        lines += ['', f"Workload {name}: " + json.dumps(run['workload']), '', 'Deviations:']
        lines += ['- ' + d for d in run.get('deviations', [])]
    lines += ['', f"Exit: {report_exit(report)}", '']
    return '\n'.join(lines)


def seed_sql(suite='story'):
    rows = []
    start = dt.datetime(2026, 11, 5, 17, tzinfo=dt.timezone.utc)
    def at(i):
        return (start + dt.timedelta(seconds=i*10)).strftime('%Y-%m-%d %H:%M:%S+00')
    for merchant, count in [(42, 31), (43, 40)] + ([(m, 31) for m in range(51, 56)] if suite == 'exit' else []):
        for i in range(count):
            ident = i+1
            email = f'customer{ident}' if merchant != 43 else f'buyer{ident}'
            rows.append(f"({merchant},{ident},'failed','{at(i%31)}',{1+i%3 if merchant!=43 else 1},'{email}@example.com','tok_{merchant}_{ident}')")
    for merchant in range(100, 200):
        for ident in range(1, 21):
            status = ('delivered', 'pending', 'failed', 'queued')[(merchant+ident)%4]
            failed = "'" + at(ident) + "'" if status == 'failed' else 'NULL'
            rows.append(f"({merchant},{ident},'{status}',{failed},0,'m{merchant}c{ident}@example.com','tok_{merchant}_{ident}')")
    return ['INSERT INTO shipments VALUES ' + ','.join(rows[i:i+500]) for i in range(0, len(rows), 500)]


class Workload:
    """Four concurrent psql loops in one persistent Docker container."""
    def __init__(self, port, env, root):
        self.env, self.root = env, root
        self.name = 'r3-safeguards-' + uuid.uuid4().hex[:12]
        self.stop_event = threading.Event()
        self.ops, self.errors, self.unexpected = 0, {}, {}
        self.operations = {'insert': 0, 'update': 0, 'select': 0}
        self.lock = threading.Lock()
        self.restart_window = False
        self.port = port

    def start(self):
        image = pins.reference(pins.load()['postgres'])
        subprocess.run(['docker', 'run', '-d', '--name', self.name, '--network', 'bridge',
                        '-e', 'PGPASSWORD=app-pw', '--entrypoint', 'sleep', image, 'infinity'],
                       env=self.env, check=True, capture_output=True, timeout=60)
        def loop(worker):
            n = 0
            with (self.root/f'workload-{worker}.log').open('w', encoding='utf-8', newline='\n') as log:
                while not self.stop_event.is_set():
                    n += 1
                    merchant = 100 + worker*25 + n % 25
                    statements = [
                        f"INSERT INTO shipments VALUES ({merchant},{1000000+worker*100000000+n},'pending',NULL,0,'load@example.com','tok_load')",
                        f"UPDATE shipments SET status='delivered',attempt_count=attempt_count+1 WHERE merchant_id={merchant} AND id=1",
                        f"SELECT id,status,attempt_count FROM shipments WHERE merchant_id={merchant} ORDER BY id"]
                    for sql in statements:
                        if self.stop_event.is_set():
                            break
                        window = self.restart_window
                        p = subprocess.run(['docker', 'exec', self.name, 'psql', '-X', '-q',
                            '-h', 'host.docker.internal', '-p', str(self.port), '-U', 'delivery_app',
                            '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=verbose',
                            '-c', sql], env=self.env, capture_output=True, text=True, timeout=30)
                        if p.returncode == 0:
                            with self.lock:
                                self.ops += 1
                                self.operations[sql.split()[0].lower()] += 1
                        else:
                            codes = re.findall(r'(?:ERROR|FATAL):\s+([0-9A-Z]{5}):', p.stderr)
                            code = codes[0] if codes else 'connection'
                            with self.lock:
                                self.errors[code] = self.errors.get(code, 0)+1
                            connection_error = code == 'connection' or code.startswith('08') or code in ('57P01', '57P02', '57P03')
                            if code not in ('40001', '40P01') and not (connection_error and (window or self.restart_window)):
                                with self.lock:
                                    self.unexpected[code] = self.unexpected.get(code, 0)+1
                            log.write(p.stderr); log.flush()
        def guarded_loop(worker):
            try:
                loop(worker)
            except Exception as exc:
                with self.lock:
                    self.unexpected['harness'] = self.unexpected.get('harness', 0)+1
                (self.root/f'workload-error-{worker}.txt').write_text(
                    f'{type(exc).__name__}: {exc}', encoding='utf-8', newline='\n')
        self.threads = [threading.Thread(target=guarded_loop, args=(worker,), daemon=True)
                        for worker in range(4)]
        for thread in self.threads:
            thread.start()

    def stop(self):
        self.stop_event.set()
        for thread in getattr(self, 'threads', []):
            thread.join(timeout=35)
            if thread.is_alive():
                self.unexpected['thread_timeout'] = 1
        if not self.ops:
            self.unexpected['no_operations'] = 1
        subprocess.run(['docker', 'rm', '-f', self.name], env=self.env, capture_output=True, timeout=30)
        return {'ops': self.ops, 'operations': self.operations, 'errors': self.errors, 'unexpected_errors': self.unexpected}


def run_driver(args, driver, root, env):
    folder = root/driver
    folder.mkdir()
    setup = folder/'setup'
    setup.mkdir()
    data = folder/'data'
    base = Server(Path(args.server_bin), setup, env, secrets.token_hex(16),
                  ['--data', str(data), '--trust-local', '--no-api'])
    expires_ms = None
    try:
        port = base.start()
        c = Pg(port, 'postgres', 'postgres')
        c.query('CREATE TABLE shipments (merchant_id bigint, id bigint, status text, failed_at timestamptz, attempt_count int, customer_email text, card_token text, PRIMARY KEY (merchant_id,id))')
        for sql in seed_sql(args.suite):
            c.query(sql)
        for role, password in PASSWORDS.items():
            c.query(f"CREATE ROLE {role} LOGIN " + ('TASKONLY ' if role == 'repair_agent' else '') + f"PASSWORD '{password}'")
            grants = 'SELECT, INSERT, UPDATE' if role == 'delivery_app' else 'SELECT, UPDATE'
            c.query(f'GRANT {grants} ON shipments TO {role}')
        c.close()
        c = Pg(port, 'incident_controller', 'postgres')
        expires_ms = int(time.time()*1000) + (3600 if args.suite == 'exit' else args.expiry_seconds)*1000
        sql = task_sql(expires_ms)
        issued = {'owner_password': base.password}
        for i, name in enumerate(EXIT_TASKS if args.suite == 'exit' else ['inc-436']):
            definition = sql.replace('inc-436', name)
            if args.suite == 'exit':
                definition = definition.replace('merchant_id = 42', f'merchant_id = {51+i}')
                if name == 'exit-undo':
                    definition = definition.replace('ALLOW UPDATE (status)', 'ALLOW UPDATE (status, attempt_count)').replace("status = 'failed' AND failed_at", "status IN ('failed','queued') AND failed_at")
            c.query(definition)
            rows, _ = c.query(f'ISSUE CREDENTIAL FOR TASK "{name}"')
            issued[name] = rows[0][2]
        secret_file = folder/'secrets.json'
        write_json(secret_file, issued)
        c.close()
    finally:
        base.stop()
    pki = folder/'pki'
    cluster.generate_pki(pki, env, pins.reference(pins.load()['postgres']), nodes=1)
    pg, agent = cluster.reserve_ports(2)
    config_path = folder/'config.json'
    write_json(config_path, make_config(data, pki, pg, agent))
    spec_path = folder/'spec.json'
    results_path = folder/'results.json'
    write_json(spec_path, dict(suite=args.suite, pg_port=pg, agent_port=agent, database='postgres',
        ca=str(pki/'ca.pem'), secret_file=str(secret_file), passwords=PASSWORDS,
        expires_ms=expires_ms, results_path=str(results_path), task_sql=sql))
    server = ConfigServer(base, config_path)
    workload = Workload(pg, env, folder)
    result = None
    try:
        server.start()
        if not args.no_workload:
            workload.start()
        if driver == 'psycopg':
            command = [args.psycopg_python, '-I', '-B', str(Path(__file__).parent/'r3'/'psycopg_scenario.py')]
        elif driver == 'pgx':
            command = [str(args.pgx_binary)]
        else:
            raise ValueError('unknown driver: ' + driver)
        with (folder/'driver.stderr.log').open('w', encoding='utf-8') as err:
            process = subprocess.Popen([*command, '--spec', str(spec_path)], env=env,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err, text=True)
            messages = queue.Queue()
            def drain():
                for line in process.stdout:
                    messages.put(line)
                messages.put(None)
            threading.Thread(target=drain, daemon=True).start()
            deadline = time.monotonic() + (3600 if args.suite == 'exit' else args.expiry_seconds) + 300
            try:
                while True:
                    line = messages.get(timeout=max(.1, deadline-time.monotonic()))
                    if line is None:
                        break
                    if parse_handshake(line) == 'restart':
                        workload.restart_window = True
                        subprocess.run(['taskkill', '/T', '/F', '/PID', str(server.process.pid)],
                                       env=env, check=True, capture_output=True, timeout=30)
                        server.process.wait(timeout=30)
                        server.stop()
                        server.start()
                        process.stdin.write('RESTARTED\n'); process.stdin.flush()
                        workload.restart_window = False
                code = process.wait(timeout=30)
            finally:
                if process.poll() is None:
                    subprocess.run(['taskkill', '/T', '/F', '/PID', str(process.pid)],
                                   env=env, capture_output=True, timeout=30)
                    process.wait(timeout=30)
        result = json.loads(results_path.read_text(encoding='utf-8'))
        if validate_steps(result['steps'], args.suite):
            raise RuntimeError('invalid driver results: ' + str(validate_steps(result['steps'], args.suite)))
        if code and all(r['status'] == 'PASS' for r in result['steps']):
            raise RuntimeError('driver exited unsuccessfully with passing results')
    finally:
        summary = workload.stop() if not args.no_workload else {'ops': 0, 'errors': {}, 'unexpected_errors': {}, 'disabled': True}
        server.stop()
        (setup/'bin'/Path(args.server_bin).name).unlink(missing_ok=True)
    result['workload'] = summary
    return result


def argument_parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--suite', choices=SUITE_STEPS, default='story')
    p.add_argument('--server-bin', required=True)
    p.add_argument('--work', required=True)
    p.add_argument('--drivers', default=DEFAULT_DRIVERS)
    p.add_argument('--psycopg-python', default=sys.executable)
    p.add_argument('--go', default=str(PINNED_GO) if PINNED_GO.exists() else 'go')
    p.add_argument('--expiry-seconds', type=int, default=300)
    p.add_argument('--no-workload', action='store_true')
    return p


def main():
    args = argument_parser().parse_args()
    try:
        work = under_work_base(args.work)
        root = work/('run-'+time.strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8])
        if args.expiry_seconds < 1:
            raise ValueError('expiry-seconds must be positive')
        root.mkdir(parents=True)
        env = go_environment(work, root, os.environ)
        report = {'suite': args.suite, 'drivers': {}}
        began = time.monotonic()
        print(str(root), flush=True)
        if 'pgx' in args.drivers.split(','):
            args.pgx_binary = build_pgx(args.go, root, env)
        for driver in args.drivers.split(','):
            report['drivers'][driver] = run_driver(args, driver, root, env)
            report['wall_seconds'] = round(time.monotonic()-began, 2)
            write_json(root/'report.json', report)
            (root/'report.md').write_text(render_report(report), encoding='utf-8', newline='\n')
        return report_exit(report)
    except Exception as exc:
        print(f'harness error: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
