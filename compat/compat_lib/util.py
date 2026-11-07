import datetime
import os
from pathlib import Path
import re
import subprocess

def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')

def output_root(value):
    path = Path(value).resolve()
    def on_k(candidate):
        # Win32 device and extended-length prefixes preserve the underlying drive.
        spelling = str(candidate).replace('/', '\\')
        while spelling.startswith(('\\\\?\\', '\\\\.\\')):
            spelling = spelling[4:]
        return bool(re.match(r'^K:', spelling, re.I))
    if on_k(path) or on_k(value):
        raise ValueError('output root must not be on K:')
    return path

def redact(value, secrets=()):
    if isinstance(value, dict):
        return {k: redact(v, secrets) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(v, secrets) for v in value]
    if isinstance(value, str):
        value = re.sub(r'postgres(?:ql)?://[^\s\"\']+', '[REDACTED_URL]', value)
        for secret in secrets:
            if secret:
                value = value.replace(secret, '[REDACTED]')
    return value

def command(args, env, timeout=60, input=None, preserve_newlines=False):
    import time
    start = time.monotonic()
    try:
        if preserve_newlines:
            p = subprocess.run(args, input=input.encode('utf-8') if isinstance(input, str) else input,
                               capture_output=True, env=env, timeout=timeout)
            stdout, stderr = p.stdout.decode('utf-8', 'replace'), p.stderr.decode('utf-8', 'replace')
        else:
            p = subprocess.run(args, input=input, text=True, encoding='utf-8', errors='replace', capture_output=True, env=env, timeout=timeout)
            stdout, stderr = p.stdout, p.stderr
        return dict(exit=p.returncode, stdout=stdout, stderr=stderr, timed_out=False, duration_ms=round((time.monotonic()-start)*1000))
    except subprocess.TimeoutExpired as e:
        def decode(v):
            return v.decode('utf-8', 'replace') if isinstance(v, bytes) else (v or '')
        return dict(exit=None, stdout=decode(e.stdout), stderr=decode(e.stderr), timed_out=True, duration_ms=round((time.monotonic()-start)*1000))

def child_env(run):
    env = os.environ.copy()
    for key, sub in [('TEMP','tmp'), ('TMP','tmp'), ('CARGO_TARGET_DIR','target')]:
        (run/sub).mkdir(exist_ok=True)
        env[key] = str(run/sub)
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    return env
