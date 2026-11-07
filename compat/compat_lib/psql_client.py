from .pins import reference
from .checks import validate_env

def _execute(docker, pin, network, name, host, port, sql, timeout, options, client_env=None):
    environment = [arg for key,value in validate_env(client_env or {}).items() for arg in ('-e',key+'='+value)]
    docker.containers.append(name)
    result = docker.call('run','--rm','-i','--name',name,'--network',network,'--tmpfs','/var/lib/postgresql',
        '--add-host','host.docker.internal:host-gateway','-e','PGPASSWORD',
        '-e','PGSSLMODE=disable','-e','PGCONNECT_TIMEOUT=10', *environment,
        reference(pin),'psql','-X',*options,
        '-h',host,'-p',str(port),'-U','postgres','-d','postgres','-f','-', input=sql, timeout=timeout)
    if result['timed_out']:
        docker.remove_client(name)
    return result

def run(docker, pin, network, name, host, port, sql, timeout, client_env=None):
    return _execute(docker, pin, network, name, host, port, sql, timeout,
                    ('-v', 'ON_ERROR_STOP=1', '--echo-all'), client_env)


def run_session(docker, pin, network, name, host, port, sql, timeout=120, client_env=None):
    """Keep one psql session for a marked corpus script, including command tags."""
    return _execute(docker, pin, network, name, host, port, sql, timeout,
                    ('-v', 'ON_ERROR_STOP=0'), client_env)


def version(docker, pin, name, timeout):
    docker.containers.append(name)
    result = docker.call('run','--rm','--name',name,'--tmpfs','/var/lib/postgresql',reference(pin),'psql','--version',timeout=timeout)
    if result['timed_out']:
        docker.remove_client(name)
    return result

REGISTRY = {
    'psql': {'run': run, 'version': version, 'run_session': run_session},
    'psycopg': {'kind': 'probe', 'driver': 'psycopg', 'version_expected': '3.2.9',
                'probe_dir': 'probes/psycopg', 'version_fields': ('libpq_version',)},
    'node-pg': {'kind': 'probe', 'driver': 'node-pg', 'version_expected': '8.16.3',
                'probe_dir': 'probes/node-pg', 'r0_prefix': 'r0d_nodepg'},
    'jdbc': {'kind': 'probe', 'driver': 'jdbc', 'version_expected': '42.7.3',
             'probe_dir': 'probes/jdbc'},
}
