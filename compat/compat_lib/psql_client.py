from .pins import reference
from .checks import validate_env

def run(docker, pin, network, name, host, port, sql, timeout, client_env=None):
    environment = [arg for key,value in validate_env(client_env or {}).items() for arg in ('-e',key+'='+value)]
    docker.containers.append(name)
    result = docker.call('run','--rm','-i','--name',name,'--network',network,'--tmpfs','/var/lib/postgresql',
        '--add-host','host.docker.internal:host-gateway','-e','PGPASSWORD',
        '-e','PGSSLMODE=disable','-e','PGCONNECT_TIMEOUT=10', *environment,
        reference(pin),'psql','-X','-v','ON_ERROR_STOP=1','--echo-all',
        '-h',host,'-p',str(port),'-U','postgres','-d','postgres','-f','-', input=sql, timeout=timeout)
    if result['timed_out']:
        docker.remove_client(name)
    return result

def version(docker, pin, name, timeout):
    docker.containers.append(name)
    result = docker.call('run','--rm','--name',name,'--tmpfs','/var/lib/postgresql',reference(pin),'psql','--version',timeout=timeout)
    if result['timed_out']:
        docker.remove_client(name)
    return result

REGISTRY = {'psql': {'run': run, 'version': version}}
