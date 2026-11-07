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
                   '-e','POSTGRES_PASSWORD','-e','POSTGRES_INITDB_ARGS','-e','TZ','-e','PGTZ',ref)
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
