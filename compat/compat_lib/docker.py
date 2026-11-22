import re
from threading import Lock
from .util import command

class Docker:
    def __init__(self, run_id, env):
        if not re.fullmatch(r'r0-\d{8}T\d{6}Z-[0-9a-f]{6}', run_id):
            raise ValueError('invalid run id')
        self.prefix = 'gd-r0-' + run_id + '-'
        self.env = env
        self.containers = []
        self.networks = []
        self._corpus_lock = Lock()
        self._corpus_client = None
        self._corpus_generation = 0
        self._corpus_closed = False

    def call(self, *args, **kwargs):
        return command(['docker', *args], self.env, **kwargs)

    def require(self, *args, **kwargs):
        result = self.call(*args, **kwargs)
        if result['exit'] != 0 or result['timed_out']:
            raise RuntimeError('Docker operation failed: ' + result['stderr'])
        return result['stdout'].strip()

    def image(self, ref, pull):
        result = self.call('image','inspect',ref)
        if result['exit'] != 0:
            if not pull:
                return False
            self.require('pull',ref,timeout=300)
            self.require('image','inspect',ref)
        return True

    def remove_client(self, name):
        if not name.startswith(self.prefix) or name == self.prefix:
            raise ValueError('refusing unrelated container')
        result = self.call('rm','-f',name)
        if result and (result['timed_out'] or (result['exit'] != 0 and 'No such container' not in result['stderr'])):
            raise RuntimeError('container cleanup failed: '+result['stderr'])

    def corpus_client(self, image, network, replace=None):
        """Return the current client, replacing it only if it still matches replace."""
        with self._corpus_lock:
            if self._corpus_closed:
                if replace is not None:
                    return None
                raise RuntimeError('corpus client is closed')
            if replace is not None and self._corpus_client == replace:
                self.remove_client(replace)
                self.containers.remove(replace)
                self._corpus_client = None
            if self._corpus_client is None:
                self._corpus_generation += 1
                name = self.prefix + 'corpus-client-' + str(self._corpus_generation)
                # Register before startup so a failed or interrupted run can clean up.
                self.containers.append(name)
                self.require('run','-d','--name',name,'--network',network,'--tmpfs','/var/lib/postgresql',
                             '--add-host','host.docker.internal:host-gateway','-e','PGPASSWORD',
                             '-e','PGSSLMODE=disable','-e','PGCONNECT_TIMEOUT=10',
                             '--entrypoint','sleep',image,'infinity')
                self._corpus_client = name
            return self._corpus_client

    def close_corpus_client(self):
        """Stop active execs and prevent an in-flight worker from starting a client."""
        with self._corpus_lock:
            self._corpus_closed = True
            for name in self.containers.copy():
                if name.startswith(self.prefix + 'corpus-client-'):
                    self.remove_client(name)
                    self.containers.remove(name)
            self._corpus_client = None

    def cleanup(self):
        errors = []
        with self._corpus_lock:
            containers = self.containers.copy()
        for name in containers:
            try:
                self.remove_client(name)
            except Exception as exc:
                errors.append(str(exc))
        for name in self.networks:
            if not name.startswith(self.prefix) or name == self.prefix:
                raise ValueError('refusing unrelated network')
            result = self.call('network','rm',name)
            if result and (result['timed_out'] or (result['exit'] != 0 and 'not found' not in result['stderr'])):
                errors.append('network cleanup failed: '+result['stderr'])
        if errors:
            raise RuntimeError('; '.join(errors))
