"""Per-node process and fault-control HTTP agent for disposable exit clusters."""
import argparse
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import threading
import time
from urllib.parse import parse_qs, urlsplit


class Agent:
    def __init__(self, manifest, node):
        self.cluster, self.node = manifest, next(n for n in manifest['nodes'] if n['id'] == node)
        self.directory = Path(self.node['dir'])
        self.process, self.mode, self.ready_line, self.started = None, None, None, None
        self.lock = threading.RLock()
        self.reader = None

    def status(self):
        p = self.process
        code = p.poll() if p else None
        return dict(pid=p.pid if p else None, mode=self.mode, ready=bool(self.ready_line) and p is not None
                    and code is None, ready_line=self.ready_line, exit_code=code,
                    uptime=time.monotonic() - self.started if self.started else 0)

    def drain(self, p):
        with (self.directory / 'stdout.log').open('a', encoding='utf-8', newline='\n') as log:
            for line in p.stdout:
                log.write(line); log.flush()
                tokens = dict(s.split('=', 1) for s in line.split() if '=' in s)
                if tokens.get('preview') == 'cluster' or (tokens.get('preview') == 'replicated'
                        and tokens.get('role') in ('leader', 'follower')):
                    self.ready_line = line.rstrip()
        p.stdout.close()

    def stop(self, hard=False):
        p, ended = self.process, 'already_stopped'
        if p and p.poll() is None:
            ended = 'killed' if hard else 'shutdown'
            if not hard:
                try:
                    p.stdin.write('shutdown\n'); p.stdin.flush(); p.stdin.close()
                    p.wait(timeout=15)
                except (OSError, subprocess.TimeoutExpired):
                    ended = 'killed_after_timeout'
            if p.poll() is None:
                p.kill(); p.wait(timeout=10)
        if self.reader:
            self.reader.join(timeout=5)
        if p and p.stdin and not p.stdin.closed:
            p.stdin.close()
        self.ready_line = None
        return dict(ended=ended, **self.status())

    def firewall(self, peers):
        if os.name == 'nt':
            return 501, dict(error='isolation requires Linux ip6tables')
        ids = [n['id'] for n in self.cluster['nodes'] if n != self.node] if peers == 'all' else peers
        if not isinstance(ids, list) or any(i == self.node['id'] or i not in
                [n['id'] for n in self.cluster['nodes']] for i in ids):
            raise ValueError('peers must be other node ids or all')
        def run(*args, check=True):
            return subprocess.run(['sudo', '-n', 'ip6tables', *args], capture_output=True,
                                  text=True, timeout=15, check=check)
        exists = run('-L', 'GXISO', check=False)
        if exists.returncode:
            run('-N', 'GXISO')
        for direction in ('INPUT', 'OUTPUT'):
            if run('-C', direction, '-j', 'GXISO', check=False).returncode:
                run('-I', direction, '1', '-j', 'GXISO')
        run('-F', 'GXISO')
        for peer in (n for n in self.cluster['nodes'] if n['id'] in ids):
            for flag in ('-s', '-d'):
                for port in {self.node['peer_port'], peer['peer_port']}:
                    for port_flag in ('--sport', '--dport'):
                        run('-A', 'GXISO', '-p', 'tcp', flag, peer['host'], port_flag, str(port), '-j', 'DROP')
        return 200, dict(isolated=ids)

    def request(self, method, url, body):
        path = urlsplit(url)
        with self.lock:
            if method == 'GET' and path.path == '/status':
                return 200, self.status()
            if method == 'GET' and path.path == '/log':
                query = parse_qs(path.query)
                stream, tail = query.get('stream', ['stdout'])[0], int(query.get('tail', ['40'])[0])
                if stream not in ('stdout', 'stderr') or not 0 <= tail <= 10000:
                    raise ValueError('invalid stream or tail (0..10000)')
                log = self.directory / f'{stream}.log'
                with log.open(encoding='utf-8', errors='replace') if log.exists() else open(os.devnull) as f:
                    return 200, dict(stream=stream, lines=list(deque(f, maxlen=tail)))
            if method != 'POST':
                return 404, dict(error='unknown endpoint')
            if path.path == '/start':
                mode = body.get('mode')
                if mode not in self.cluster['modes']:
                    raise ValueError('unknown mode')
                if self.process and self.process.poll() is None:
                    return 409, dict(error='node is running')
                self.stop()
                self.mode, self.ready_line, self.started = mode, None, time.monotonic()
                temporary = self.directory / 'tmp'
                temporary.mkdir(exist_ok=True)
                with (self.directory / 'stderr.log').open('a', encoding='utf-8') as err:
                    self.process = subprocess.Popen([self.cluster['binary'], str(self.directory / f'{mode}.json')],
                        env=dict(os.environ, GIGGLES_OWNER_PASSWORD=self.cluster['password'],
                                 TEMP=str(temporary), TMP=str(temporary)),
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err,
                        text=True, encoding='utf-8', errors='replace')
                self.reader = threading.Thread(target=self.drain, args=(self.process,), daemon=True)
                self.reader.start()
                return 200, self.status()
            if path.path in ('/stop', '/kill'):
                return 200, self.stop(path.path == '/kill')
            if path.path == '/wipe':
                if self.process and self.process.poll() is None:
                    return 409, dict(error='stop node before wiping')
                for name in ('db', 'raftlog', 'raftlog.snapshots'):
                    target = (self.directory / name).resolve()
                    if not target.is_relative_to(self.directory.resolve()):
                        raise ValueError('wipe path escapes node directory')
                    if target.is_dir():
                        shutil.rmtree(target)
                return 200, dict(wiped=True)
            if path.path in ('/heal', '/isolate'):
                return self.firewall([] if path.path == '/heal' else body.get('peers'))
            if path.path == '/admin':
                args = body.get('args')
                if not isinstance(args, list) or len(args) < 2 or args[0] not in ('range', 'cluster') or not all(
                        isinstance(a, str) for a in args) or self.mode is None:
                    raise ValueError('args require range|cluster, operation and a started mode')
                result = subprocess.run([self.cluster['binary'], *args[:2],
                    str(self.directory / f'{self.mode}.json'), *args[2:]], capture_output=True,
                    text=True, timeout=120)
                return 200, dict(rc=result.returncode, stdout=result.stdout, stderr=result.stderr)
            return 404, dict(error='unknown endpoint')


def serve(agent):
    class Handler(BaseHTTPRequestHandler):
        def handle_request(self):
            try:
                length = int(self.headers.get('Content-Length', 0))
                if not 0 <= length <= 65536:
                    raise ValueError('body too large')
                body = json.loads(self.rfile.read(length)) if length else {}
                if not isinstance(body, dict):
                    raise ValueError('body must be a JSON object')
                code, result = agent.request(self.command, self.path, body)
            except (ValueError, TypeError) as exc:
                code, result = 400, dict(error=str(exc))
            except subprocess.TimeoutExpired as exc:
                code, result = 504, dict(error=str(exc))
            except Exception as exc:
                code, result = 500, dict(error=str(exc))
            payload = json.dumps(result).encode()
            self.send_response(code); self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload))); self.end_headers(); self.wfile.write(payload)
        do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = do_OPTIONS = do_HEAD = handle_request

    class Server(ThreadingHTTPServer):
        address_family = socket.AF_INET if agent.node['host'] == '127.0.0.1' else socket.AF_INET6
    with Server(('127.0.0.1' if Server.address_family == socket.AF_INET else '::',
                 agent.node['agent_port']), Handler) as server:
        try:
            server.serve_forever()
        finally:
            agent.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cluster', required=True)
    parser.add_argument('--node', type=int, required=True)
    args = parser.parse_args()
    serve(Agent(json.loads(Path(args.cluster).read_text()), args.node))
