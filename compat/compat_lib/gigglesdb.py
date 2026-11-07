import hashlib
import contextlib
import json
import os
from pathlib import Path
import queue
import re
import shutil
import subprocess
import threading
import tarfile
import time
import uuid
from urllib.parse import urlsplit
from .util import redact, command, output_root, utc


class BuildError(RuntimeError):
    def __init__(self, message, metadata):
        super().__init__(message)
        self.metadata = metadata


def resolve_commit(repo, ref, env):
    result = command(['git', '-C', str(repo), 'rev-parse', '--verify', '--end-of-options', str(ref) + '^{commit}'], env)
    sha = result['stdout'].strip()
    if result['exit'] != 0 or not re.fullmatch(r'[0-9a-fA-F]{40}', sha):
        raise BuildError('cannot resolve build ref to a commit', {'server_source': 'built-from-commit', 'build_identity': 'built-from-commit', 'build_ref': str(ref)})
    return sha.lower()


def _digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _junction(path):
    return getattr(os.path, 'isjunction', lambda value: False)(path)


def _lock_identity(stat):
    # Reading the ownership token may update atime, which is not identity.
    return stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_size


def _stop_build(process, env):
    if process.poll() is None:
        if os.name == 'nt':
            subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'],
                           env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=30)
        else:
            import signal
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=30)


def _extract_archive(repo, sha, source, env, log):
    archive = subprocess.Popen(['git', '-C', str(repo), 'archive', '--format=tar', sha],
        env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=log)
    try:
        with tarfile.open(fileobj=archive.stdout, mode='r|') as tar:
            tar.extractall(source, filter='data')
        # tarfile stops at end markers; git may still be writing record
        # padding exceeding the Windows pipe capacity.
        while archive.stdout.read(65536):
            pass
        if archive.wait(timeout=60) != 0:
            raise RuntimeError('git archive failed')
    finally:
        archive.stdout.close()
        if archive.poll() is None:
            archive.kill(); archive.wait()


def _cached(build_dir, sha):
    binary = build_dir/'giggles-server.exe'
    try:
        meta = json.loads((build_dir/'build.json').read_text(encoding='utf-8'))
        if (meta['commit'] == sha and binary.is_file() and
                meta['size'] == binary.stat().st_size and meta['sha256'] == _digest(binary)):
            return dict(meta, commit=sha, resolved_commit=sha,
                        server_source='built-from-commit', build_identity='built-from-commit',
                        build_root_dir=str(build_dir), binary_size=meta['size'],
                        binary_sha256=meta['sha256'])
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return None


@contextlib.contextmanager
def _build_lock(target, metadata, timeout=3600):
    lock = target/'.gd-r0-build.lock'
    token = uuid.uuid4().hex
    deadline = time.monotonic() + timeout
    while True:
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, 'w') as stream:
                stream.write(token)
            owned_stat = _lock_identity(lock.stat())
            break
        except FileExistsError:
            try:
                stat = lock.stat()
                if time.time() - stat.st_mtime > 7200:
                    # Check identity again before removing a stale lock.
                    if _lock_identity(lock.stat()) == _lock_identity(stat):
                        lock.unlink()
                        metadata.setdefault('stale_build_locks', []).append(str(lock))
                    continue
            except FileNotFoundError:
                continue
            if time.monotonic() >= deadline:
                raise BuildError('build lock exceeded 60 minutes', metadata)
            time.sleep(.2)
    try:
        yield
    finally:
        try:
            if _lock_identity(lock.stat()) == owned_stat and lock.read_text() == token and _lock_identity(lock.stat()) == owned_stat:
                lock.unlink()
        except FileNotFoundError:
            pass


def _reset_source(build_dir):
    source = build_dir/'src'
    marker = build_dir/'.gd-source-owner'
    if source.exists() or source.is_symlink():
        if (not marker.is_file() or marker.read_text() != str(build_dir.resolve()) or
                source.is_symlink() or source.resolve() != build_dir.resolve()/'src' or
                _junction(source)):
            raise ValueError('refusing to remove unowned or redirected build source')
        # Refuse nested reparse points before recursive deletion as well.
        for root, dirs, files in os.walk(source, followlinks=False):
            for name in dirs + files:
                child = Path(root)/name
                if child.is_symlink() or _junction(child):
                    raise ValueError('refusing to remove redirected build source')
        shutil.rmtree(source)
    marker.write_text(str(build_dir.resolve()), encoding='utf-8')
    source.mkdir()
    return source


def prepare_build(repo, ref, build_root, cargo_target, env):
    """Build an immutable commit, or return its verified cached executable."""
    build_root, target = output_root(build_root), output_root(cargo_target)
    sha = resolve_commit(repo, ref, env)
    build_dir = output_root(build_root/sha)
    if build_dir != build_root/sha:
        raise ValueError('build directory must not be redirected')
    metadata = dict(server_source='built-from-commit', build_identity='built-from-commit', commit=sha, resolved_commit=sha,
                    build_root_dir=str(build_dir), build_cache='miss')
    cached = _cached(build_dir, sha)
    if cached:
        return build_dir/'giggles-server.exe', dict(cached, build_cache='hit', build_duration_s=0)
    target.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    with _build_lock(target, metadata):
        cached = _cached(build_dir, sha)
        if cached:
            return build_dir/'giggles-server.exe', dict(cached, build_cache='hit', build_duration_s=0, **{k:v for k,v in metadata.items() if k == 'stale_build_locks'})
        build_dir.mkdir(parents=True, exist_ok=True)
        tmp = build_dir/'tmp'
        tmp.mkdir(exist_ok=True)
        build_env = dict(env, CARGO_TARGET_DIR=str(target), TEMP=str(tmp), TMP=str(tmp))
        log_path = build_dir/'build.log'
        metadata['build_log'] = str(log_path)
        try:
            source = _reset_source(build_dir)
            with log_path.open('wb') as log:
                _extract_archive(repo, sha, source, build_env, log)
                process = subprocess.Popen(['cargo', 'build', '--locked', '--manifest-path',
                    str(source/'crates'/'giggles-server'/'Cargo.toml'), '--bin', 'giggles-server'],
                    cwd=source, env=build_env, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                    start_new_session=os.name != 'nt', creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0)
                try:
                    code = process.wait(timeout=3600)
                except BaseException:
                    _stop_build(process, build_env)
                    raise
                if code:
                    raise RuntimeError('cargo build failed')
            binary = build_dir/'giggles-server.exe'
            shutil.copy2(target/'debug'/('giggles-server.exe' if os.name == 'nt' else 'giggles-server'), binary)
            versions = {}
            for tool in ('cargo', 'rustc'):
                result = command([tool, '--version'], build_env)
                if result['exit'] != 0:
                    raise RuntimeError(tool + ' --version failed')
                versions[tool + '_version'] = result['stdout'].strip()
            metadata.update(versions, binary_sha256=_digest(binary), binary_size=binary.stat().st_size,
                            built_at=utc(), profile='debug', build_duration_s=round(time.monotonic()-start, 3))
            metadata.update(sha256=metadata['binary_sha256'], size=metadata['binary_size'], duration_s=metadata['build_duration_s'])
            temporary = build_dir/('build.json.' + uuid.uuid4().hex)
            temporary.write_text(json.dumps(metadata, indent=2) + '\n', encoding='utf-8')
            temporary.replace(build_dir/'build.json')
            return binary, metadata
        except Exception as exc:
            metadata['build_duration_s'] = round(time.monotonic()-start, 3)
            if log_path.exists():
                metadata['build_log_tail'] = log_path.read_text(encoding='utf-8', errors='replace').splitlines()[-30:]
            raise BuildError(str(exc), metadata) from exc

def parse_banner(line, url):
    match = re.fullmatch(r'postgresql=127\.0\.0\.1:(\d+) preview=single-host', line.rstrip('\r\n'))
    if not match or not 0 < int(match[1]) <= 65535:
        raise ValueError('invalid server readiness banner')
    parsed = urlsplit(url.strip())
    if parsed.scheme not in ('postgres','postgresql') or parsed.hostname != '127.0.0.1' or parsed.port != int(match[1]) or parsed.username != 'postgres' or parsed.path != '/postgres':
        raise ValueError('invalid server readiness URL')
    return int(match[1])

class Server:
    def __init__(self, source, run, env, password):
        self.run, self.env, self.password = run, env.copy(), password
        self.process = None
        self.thread = None
        (run/'bin').mkdir()
        frozen = run/'bin'/source.name
        shutil.copy2(source, frozen)
        self.binary = frozen
        self.meta = dict(binary_sha256=hashlib.sha256(frozen.read_bytes()).hexdigest(), binary_size=frozen.stat().st_size)

    def start(self):
        (self.run/'cwd').mkdir()
        self.env['GIGGLES_OWNER_PASSWORD'] = self.password
        self.stderr = (self.run/'server.stderr.log').open('w', encoding='utf-8')
        self.process = subprocess.Popen([str(self.binary),'--dev','--data',str(self.run/'data'),'--port','0','--owner','postgres','--database','postgres'], cwd=self.run/'cwd', env=self.env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='replace')
        ready = queue.Queue()
        def drain():
            with (self.run/'server.stdout.log').open('w',encoding='utf-8') as log:
                for index, line in enumerate(self.process.stdout):
                    if index < 2:
                        ready.put(line)
                    log.write(redact(line, [self.password])); log.flush()
                ready.put(None)
        def err():
            for line in self.process.stderr:
                self.stderr.write(redact(line, [self.password])); self.stderr.flush()
        self.thread = threading.Thread(target=drain, daemon=True)
        self.err_thread = threading.Thread(target=err, daemon=True)
        self.thread.start(); self.err_thread.start()
        import time
        deadline = time.monotonic()+30
        try:
            first = ready.get(timeout=max(.01, deadline-time.monotonic()))
            second = ready.get(timeout=max(.01, deadline-time.monotonic()))
        except queue.Empty as exc:
            raise RuntimeError('server readiness exceeded 30 seconds') from exc
        if first is None or second is None:
            raise RuntimeError('server exited before readiness')
        port = parse_banner(first, second)
        self.meta['stdout_banner'] = [first.rstrip(), redact(second.rstrip(), [self.password])]
        return port

    def stop(self):
        if self.process:
            try:
                if self.process.poll() is None:
                    self.process.stdin.write('shutdown\n'); self.process.stdin.flush()
                    self.process.wait(timeout=30)
            except (BrokenPipeError, OSError, subprocess.TimeoutExpired):
                self.process.kill(); self.process.wait(timeout=10)
            self.meta['exit'] = self.process.returncode
            for thread in (self.thread, self.err_thread):
                if thread:
                    thread.join(timeout=5)
            for stream in (self.process.stdin,self.process.stdout,self.process.stderr):
                if stream:
                    stream.close()
        if hasattr(self,'stderr'):
            self.stderr.close()
