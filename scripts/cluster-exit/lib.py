"""Shared client helpers for cluster-exit drivers (stdlib plus psycopg)."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import socket
import statistics
import time
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener


def wait_for(call, timeout):
    deadline, last = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        try:
            value = call()
            if value:
                return value
        except Exception as exc:
            last = exc
        time.sleep(.1)
    raise TimeoutError(f'condition not met within {timeout}s; last error: {last}')


class AgentClient:
    def __init__(self, node):
        host = node['host']
        self.url = f"http://{'[' + host + ']' if ':' in host else host}:{node['agent_port']}"

    def request(self, path, body=None):
        request = Request(self.url + path, data=None if body is None else json.dumps(body).encode(),
                          headers={'Content-Type': 'application/json'})
        try:
            with build_opener(ProxyHandler({})).open(request, timeout=130) as response:
                return json.load(response)
        except HTTPError as exc:
            raise RuntimeError(f'{path}: HTTP {exc.code}: {exc.read().decode()}') from exc

    def start(self, mode): return self.request('/start', dict(mode=mode))
    def stop(self): return self.request('/stop', {})
    def kill(self): return self.request('/kill', {})
    def wipe(self): return self.request('/wipe', {})
    def isolate(self, peers='all'): return self.request('/isolate', dict(peers=peers))
    def heal(self): return self.request('/heal', {})
    def admin(self, args): return self.request('/admin', dict(args=args))
    def status(self): return self.request('/status')
    def log(self, tail=40, stream='stdout'): return self.request(f'/log?tail={int(tail)}&stream={stream}')


class Cluster:
    def __init__(self, manifest):
        self.manifest, self.nodes = manifest, manifest['nodes']
        self.mode = manifest['mode_default']

    @classmethod
    def load(cls, path):
        return cls(json.loads(Path(path).read_text(encoding='utf-8')))

    def node(self, k):
        return next(n for n in self.nodes if n['id'] == k)

    def dsn(self, k):
        from psycopg.conninfo import make_conninfo
        n = self.node(k)
        return make_conninfo(host=n['host'], port=n['pg_port'], user=self.manifest['owner'],
                             password=self.manifest['password'], dbname=self.manifest['database'], sslmode='disable')

    def connect(self, k, autocommit=True, connect_timeout=10, **kw):
        import psycopg
        return psycopg.connect(self.dsn(k), autocommit=autocommit, connect_timeout=connect_timeout, **kw)

    def agent(self, k):
        return AgentClient(self.node(k))

    def reset(self, mode):
        if mode not in self.manifest['modes']:
            raise ValueError('unknown mode')
        agents = [self.agent(n['id']) for n in self.nodes]
        with ThreadPoolExecutor(len(agents)) as pool:
            for op in ('stop', 'wipe', 'start'):
                list(pool.map(lambda a: getattr(a, op)(*([mode] if op == 'start' else [])), agents))
        self.mode = mode
        wait_for(lambda: all(a.status()['ready'] for a in agents), 90)
        def sql_ready():
            # R4 followers expose refusal listeners; only their elected leader serves SQL.
            ids = [self.leader()] if mode == 'replication' else [n['id'] for n in self.nodes]
            for k in ids:
                with self.connect(k, connect_timeout=2) as conn:
                    if conn.execute('SELECT 1').fetchone() != (1,):
                        return False
            return True
        wait_for(sql_ready, 60)

    def leader(self):
        leaders = [n['id'] for n in self.nodes if self._is_leader(n['id'])]
        if len(leaders) != 1:
            raise RuntimeError(f'expected one live replication leader, got {leaders}')
        return leaders[0]

    def _is_leader(self, k):
        s = self.agent(k).status()
        return s['ready'] and 'role=leader' in s['ready_line'].split()

    def rtt(self):
        result = {}
        for n in self.nodes:
            samples = []
            for _ in range(5):
                start = time.perf_counter()
                with socket.create_connection((n['host'], n['pg_port']), timeout=10):
                    samples.append((time.perf_counter() - start) * 1000)
            result[n['id']] = statistics.median(samples)
        return result


def write_result(out_dir, name, result):
    out = Path(out_dir)
    if Path(name).name != name:
        raise ValueError('result name must be a filename')
    out.mkdir(parents=True, exist_ok=True)
    (out / f'{name}.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8', newline='\n')
    with (out / 'summary.md').open('a', encoding='utf-8', newline='\n') as f:
        f.write(f'- {name}: {json.dumps(result, sort_keys=True)}\n')
