"""Three-process replicated GigglesDB cluster for the compatibility harness.

`ClusterServer` mirrors the parts of `gigglesdb.Server` the harness uses:
`start()` returns the elected leader's PostgreSQL port, `meta` carries report
identity, and `stop()` shuts every node down. Clients only ever see the leader.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import secrets
import shlex
import shutil
import socket
import subprocess
import threading
import time
from .util import redact, utc

NODES = 3
OWNER = 'postgres'
DATABASE = 'postgres'
TICK_MS = 100
READY_SECONDS = 60

# Minimal OpenSSL configuration: one CA plus per-node leaves usable for both
# directions of the mTLS peer transport, which verifies SNI "localhost".
OPENSSL_CNF = """[req]
distinguished_name = dn
prompt = no
[dn]
CN = gigglesdb-compat-test-ca
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
[v3_leaf]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = DNS:localhost
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid
"""

GIT_OPENSSL = (Path('C:/Program Files/Git/mingw64/bin/openssl.exe'),
               Path('C:/Program Files/Git/usr/bin/openssl.exe'))


def tokens(line):
    """key=value tokens of a server stdout line."""
    return dict(part.split('=', 1) for part in line.split() if '=' in part)


def role_line(line):
    """Parsed replicated role announcement, or None for other stdout lines."""
    values = tokens(line)
    if values.get('preview') != 'replicated' or values.get('role') not in ('leader', 'follower'):
        return None
    return values


def der_fingerprint(pem_text):
    """Lowercase hex SHA-256 of the first certificate's DER bytes."""
    match = re.search(r'-----BEGIN CERTIFICATE-----(.+?)-----END CERTIFICATE-----', pem_text, re.S)
    if not match:
        raise ValueError('no PEM certificate')
    return hashlib.sha256(base64.b64decode(''.join(match[1].split()))).hexdigest()


def openssl_commands(nodes=NODES):
    """Ordered OpenSSL argv lists (relative file names) for the CA and node leaves."""
    commands = [
        ['genpkey', '-algorithm', 'EC', '-pkeyopt', 'ec_paramgen_curve:P-256', '-out', 'ca.key'],
        ['req', '-x509', '-new', '-config', 'openssl.cnf', '-extensions', 'v3_ca', '-key', 'ca.key',
         '-sha256', '-days', '30', '-out', 'ca.pem'],
    ]
    for node in range(1, nodes + 1):
        commands += [
            ['genpkey', '-algorithm', 'EC', '-pkeyopt', 'ec_paramgen_curve:P-256', '-out', f'node{node}.key'],
            ['req', '-new', '-config', 'openssl.cnf', '-key', f'node{node}.key',
             '-subj', f'/CN=gigglesdb-compat-node{node}', '-out', f'node{node}.csr'],
            ['x509', '-req', '-in', f'node{node}.csr', '-CA', 'ca.pem', '-CAkey', 'ca.key',
             '-set_serial', str(int.from_bytes(secrets.token_bytes(8), 'big')), '-days', '30', '-sha256',
             '-extfile', 'openssl.cnf', '-extensions', 'v3_leaf', '-out', f'node{node}.pem'],
        ]
    return commands


def output_files(nodes=NODES):
    return ['ca.pem'] + [f'node{n}.{ext}' for n in range(1, nodes + 1) for ext in ('pem', 'key')]


def local_openssl(env):
    found = shutil.which('openssl', path=env.get('PATH'))
    if found:
        return found
    return next((str(path) for path in GIT_OPENSSL if path.is_file()), None)


def generate_pki(directory, env, docker_image, nodes=NODES):
    """Write ca.pem and nodeN.{pem,key}; return how they were produced.

    Uses a host `openssl` (PATH, then Git for Windows) and falls back to the
    pinned PostgreSQL image's openssl through `docker run -i` (no mounts).
    """
    directory.mkdir(parents=True, exist_ok=True)
    commands = openssl_commands(nodes)
    binary = local_openssl(env)
    if binary:
        (directory/'openssl.cnf').write_text(OPENSSL_CNF, encoding='utf-8', newline='\n')
        version = subprocess.run([binary, 'version'], env=env, capture_output=True, text=True, timeout=30)
        for args in commands:
            result = subprocess.run([binary, *args], cwd=directory, env=env, capture_output=True,
                                    text=True, timeout=60, stdin=subprocess.DEVNULL)
            if result.returncode:
                raise RuntimeError('openssl failed: '+' '.join(args[:2])+': '+result.stderr.strip()[-400:])
        for name in [*(f'node{n}.csr' for n in range(1, nodes + 1))]:
            (directory/name).unlink()
        method = dict(method='host-openssl', openssl=binary, openssl_version=version.stdout.strip())
    else:
        script = ['set -e', 'mkdir -p /tmp/pki', 'cd /tmp/pki', "cat > openssl.cnf <<'EOF'", OPENSSL_CNF.rstrip('\n'), 'EOF']
        script += ['openssl ' + shlex.join(args) + ' 2>/dev/null' for args in commands]
        script += ['openssl version >&2']
        script += [f"printf '=== %s\\n' {name}; cat {name}" for name in output_files(nodes)]
        result = subprocess.run(['docker', 'run', '--rm', '-i', '--network', 'none', '--entrypoint', 'sh',
                                 docker_image, '-s'], input='\n'.join(script)+'\n', env=env,
                                capture_output=True, text=True, timeout=180)
        if result.returncode:
            raise RuntimeError('docker openssl failed: '+result.stderr.strip()[-400:])
        parts = re.split(r'^=== (\S+)\n', result.stdout, flags=re.M)
        files = dict(zip(parts[1::2], parts[2::2]))
        if sorted(files) != sorted(output_files(nodes)):
            raise RuntimeError('docker openssl produced unexpected files')
        for name, text in files.items():
            (directory/name).write_text(text, encoding='utf-8', newline='\n')
        method = dict(method='docker-openssl', image=docker_image, openssl_version=result.stderr.strip())
    return method


def reserve_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('127.0.0.1', 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def node_configs(root, pki, ports, fingerprints, cluster_id, tick_ms=TICK_MS):
    """Per-node config dicts. ports[i] = (postgresql_port, replication_port)."""
    configs = []
    for i in range(len(ports)):
        node_dir = root/f'node-{i + 1}'
        peers = [dict(id=j + 1, address=f'127.0.0.1:{ports[j][1]}', fingerprint=fingerprints[j])
                 for j in range(len(ports)) if j != i]
        configs.append({
            'database': str(node_dir/'db'), 'native': '127.0.0.1:0', 'postgres': '127.0.0.1:0',
            'certificate': str(pki/f'node{i + 1}.pem'), 'private_key': str(pki/f'node{i + 1}.key'),
            'client_ca': str(pki/'ca.pem'), 'bindings': [], 'connections': 32,
            'skew_seconds': 0, 'max_pause_seconds': 3600,
            # Same authentication as `--dev` without --trust-local: SCRAM password for the owner.
            'postgresql': {'listen': f'127.0.0.1:{ports[i][0]}', 'tls': 'disabled', 'loopback_trust': False,
                           'databases': [DATABASE]},
            'replication': {'node_id': i + 1, 'cluster': cluster_id, 'listen': f'127.0.0.1:{ports[i][1]}',
                            'log_dir': str(node_dir/'raftlog'), 'tick_ms': tick_ms, 'owner': OWNER,
                            'peers': peers},
        })
    return configs


def unique_leader(latest, alive):
    """Index of the single announced leader once every live node has announced."""
    if not all(latest[i] for i in range(len(latest)) if alive[i]):
        return None
    leaders = [i for i, values in enumerate(latest) if alive[i] and values and values['role'] == 'leader']
    return leaders[0] if len(leaders) == 1 else None


class _Group:
    """`process`-like view over all node processes for the harness shutdown check."""
    def __init__(self, processes):
        self.processes = processes

    def poll(self):
        codes = [p.poll() for p in self.processes]
        if any(code is None for code in codes):
            return None
        return next((code for code in codes if code), 0)

    @property
    def returncode(self):
        return self.poll()


class ClusterServer:
    def __init__(self, source, run, env, password, server_args=()):
        if server_args:
            raise ValueError('--server-arg is not supported with --cluster (config-file mode has no --dev flags)')
        self.run, self.env, self.password = run, env.copy(), password
        self.processes, self.threads, self.logs = [], [], []
        self.process = None
        self.events = queue.Queue()
        self.history = []
        self.lock = threading.Lock()
        (run/'bin').mkdir()
        frozen = run/'bin'/source.name
        shutil.copy2(source, frozen)
        self.binary = frozen
        self.meta = dict(binary_sha256=hashlib.sha256(frozen.read_bytes()).hexdigest(),
                         binary_size=frozen.stat().st_size, server_mode='replicated-3-leader')
        self.cluster = dict(nodes=NODES, owner=OWNER, database=DATABASE, tick_ms=TICK_MS)
        self.meta['cluster'] = self.cluster

    def _drain(self, index, process):
        def out():
            with (self.run/f'server-node{index + 1}.stdout.log').open('w', encoding='utf-8') as log:
                for line in process.stdout:
                    log.write(redact(line, [self.password])); log.flush()
                    values = role_line(line)
                    if values is not None:
                        record = dict(at=utc(), node=index + 1, line=redact(line.rstrip(), [self.password]))
                        with self.lock:
                            self.history.append(record)
                        self.events.put((index, values))
            self.events.put((index, None))

        def err():
            with (self.run/f'server-node{index + 1}.stderr.log').open('w', encoding='utf-8') as log:
                for line in process.stderr:
                    log.write(redact(line, [self.password])); log.flush()
        for target in (out, err):
            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            self.threads.append(thread)

    def start(self):
        (self.run/'cwd').mkdir(exist_ok=True)
        root = self.run/'data'/'cluster'
        pki = root/'pki'
        root.mkdir(parents=True)
        from . import pins
        self.cluster['pki'] = generate_pki(pki, self.env, pins.reference(pins.load()['postgres']))
        fingerprints = [der_fingerprint((pki/f'node{n}.pem').read_text(encoding='utf-8')) for n in range(1, NODES + 1)]
        flat = reserve_ports(NODES * 2)
        ports = [(flat[2*i], flat[2*i + 1]) for i in range(NODES)]
        cluster_id = secrets.token_hex(16)
        configs = node_configs(root, pki, ports, fingerprints, cluster_id)
        self.cluster.update(cluster_id=cluster_id, fingerprints=fingerprints,
                            postgresql_ports=[p[0] for p in ports], replication_ports=[p[1] for p in ports])
        self.env['GIGGLES_OWNER_PASSWORD'] = self.password
        for i, config in enumerate(configs):
            node_dir = root/f'node-{i + 1}'
            node_dir.mkdir()
            path = node_dir/'config.json'
            path.write_text(json.dumps(config, indent=2)+'\n', encoding='utf-8', newline='\n')
            # Config-file mode takes the config path as its single argument.
            process = subprocess.Popen([str(self.binary), str(path)], cwd=self.run/'cwd', env=self.env,
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True, encoding='utf-8', errors='replace')
            self.processes.append(process)
            self._drain(i, process)
        self.process = _Group(self.processes)
        latest, alive = [None]*NODES, [True]*NODES
        started = time.monotonic()
        deadline = started + READY_SECONDS
        leader = None
        while leader is None:
            try:
                index, values = self.events.get(timeout=max(.01, deadline - time.monotonic()))
            except queue.Empty as exc:
                raise RuntimeError('cluster readiness exceeded %d seconds: %s' % (READY_SECONDS, latest)) from exc
            if values is None:
                raise RuntimeError(f'node {index + 1} exited before cluster readiness')
            if values.get('node') != str(index + 1):
                raise RuntimeError(f'node {index + 1} announced node={values.get("node")}')
            latest[index] = values
            leader = unique_leader(latest, alive)
        values = latest[leader]
        port = ports[leader][0]
        if values.get('postgresql') != f'127.0.0.1:{port}':
            raise RuntimeError('leader announced an unexpected PostgreSQL address: '+str(values.get('postgresql')))
        self.leader = leader
        self.cluster.update(leader_node=leader + 1, leader_term=int(values.get('term', 0) or 0),
                            ready_s=round(time.monotonic() - started, 3), ready_at=utc())
        with self.lock:
            self.ready_events = len(self.history)
            self.meta['stdout_banner'] = [record['line'] for record in self.history]
        return port

    def stop(self):
        with self.lock:
            stopping = len(self.history)
        for process in self.processes:
            try:
                if process.poll() is None:
                    process.stdin.write('shutdown\n'); process.stdin.flush()
                # Replicated mode acts on `shutdown`, but its stdin reader task keeps
                # a blocking read pending, so the process exits only once stdin also
                # delivers EOF (or another line). Close stdin after the command.
                process.stdin.close()
            except (BrokenPipeError, OSError):
                pass
        deadline = time.monotonic() + 30
        killed = []
        for index, process in enumerate(self.processes):
            try:
                process.wait(timeout=max(.01, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=10)
                killed.append(index + 1)
        self.cluster['killed_after_shutdown_timeout'] = killed
        for thread in self.threads:
            thread.join(timeout=5)
        for process in self.processes:
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream:
                    try:
                        stream.close()
                    except OSError:
                        pass
        exits = {str(i + 1): p.returncode for i, p in enumerate(self.processes)}
        self.cluster['exits'] = exits
        if self.processes:
            self.meta['exit'] = next((code for code in exits.values() if code), 0)
        with self.lock:
            ready = getattr(self, 'ready_events', stopping)
            during = self.history[ready:stopping]
            self.cluster['role_events_during_shutdown'] = self.history[stopping:]
        # Any role announcement between readiness and shutdown means leadership
        # moved mid-run, so later clients would reach a refusing follower port.
        self.cluster['role_events_after_ready'] = during
        self.cluster['leader_changes_after_ready'] = len(during)
