import time
from .pins import reference

def start(docker, pins):
    ref = reference(pins['postgres'])
    network = docker.prefix + 'net'
    docker.networks.append(network)
    docker.require('network','create',network)
    name = docker.prefix + 'pg'
    docker.containers.append(name)
    docker.require('run','-d','--name',name,'--network',network,'--tmpfs','/var/lib/postgresql',
                   '-e','POSTGRES_PASSWORD','-e','POSTGRES_INITDB_ARGS','-e','TZ','-e','PGTZ',ref,
                   'postgres','-c','log_statement=all','-c','log_min_error_statement=error',
                   '-c','log_line_prefix=%m [%p] %a ')
    deadline = time.monotonic()+90
    while time.monotonic() < deadline:
        result = docker.call('exec',name,'pg_isready','-h','127.0.0.1','-p','5432','-U','postgres',timeout=5)
        if result['exit'] == 0:
            probe = docker.call('exec','-e','PGPASSWORD',name,'psql','-X','-h','127.0.0.1','-U','postgres','-Atc','SELECT 1',timeout=5)
            if probe['exit'] == 0 and probe['stdout'].strip() == '1':
                break
        time.sleep(.5)
    else:
        raise RuntimeError('PostgreSQL oracle readiness exceeded 90 seconds')
    meta = dict(pins['postgres'])
    for key, sql in [('server_version','SHOW server_version'),('collation',"SELECT datcollate FROM pg_database WHERE datname='postgres'"),('timezone','SHOW TimeZone'),('encoding','SHOW server_encoding')]:
        meta[key] = docker.require('exec','-e','PGPASSWORD',name,'psql','-X','-h','127.0.0.1','-U','postgres','-Atc',sql)
    return network, name, meta


LOG_LIMIT = 20 * 1024 * 1024
TRUNCATION_MARKER = b'\n[oracle log truncated at byte limit]\n'

def capture_log(docker, path, secrets=(), limit=LOG_LIMIT):
    """Merge Docker's streams, read only a bounded prefix, and redact before saving."""
    import subprocess
    from .util import redact
    if limit < len(TRUNCATION_MARKER):
        raise ValueError('oracle log limit too small')
    process = subprocess.Popen(['docker', 'logs', docker.prefix+'pg'], env=docker.env,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        content = bytearray()
        read_limit = limit + max([1, *(len(secret.encode('utf-8')) for secret in secrets if secret)])
        while len(content) < read_limit:
            chunk = process.stdout.read(min(65536, read_limit-len(content)))
            if not chunk:
                break
            content.extend(chunk)
        truncated = len(content) > limit
        if truncated:
            process.terminate()
        exit_code = process.wait(timeout=30)
        if exit_code != 0 and not truncated:
            raise RuntimeError('Docker oracle log capture failed (exit '+str(exit_code)+')')
        # Redaction can expand the prefix, so enforce the cap on final UTF-8 bytes too.
        output = redact(bytes(content).decode('utf-8', 'replace'), secrets).encode('utf-8')
        truncated = truncated or len(output) > limit
        if truncated:
            output = output[:limit-len(TRUNCATION_MARKER)].decode('utf-8', 'ignore').encode('utf-8') + TRUNCATION_MARKER
        path.write_bytes(output)
    finally:
        process.stdout.close()
        if process.poll() is None:
            process.kill()
            process.wait()
