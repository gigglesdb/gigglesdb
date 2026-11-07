import re
from .util import command

class Docker:
    def __init__(self, run_id, env):
        if not re.fullmatch(r'r0-\d{8}T\d{6}Z-[0-9a-f]{6}', run_id):
            raise ValueError('invalid run id')
        self.prefix = 'gd-r0-' + run_id + '-'
        self.env = env
        self.containers = []
        self.networks = []

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

    def cleanup(self):
        errors = []
        for name in self.containers:
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
