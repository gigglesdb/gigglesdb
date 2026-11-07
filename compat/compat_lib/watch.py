"""One-iteration, stateful R0 observer; never polls or starts overlapping runs."""
import datetime
import json
import math
import os
from pathlib import Path
import re
import secrets
import sys
import tempfile
from . import delta, util


class LockBusy(RuntimeError):
    pass


class RunLock:
    def __init__(self, target):
        self.path = Path(target)/'.gd-r0-run.lock'
        self.token = secrets.token_hex(24)

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            raise LockBusy('another R0 harness run holds '+str(self.path)) from None
        try:
            with os.fdopen(fd, 'w') as stream:
                stream.write(self.token)
            stat = self.path.stat()
            self.identity = (stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_size)
        except BaseException:
            self.path.unlink(missing_ok=True)
            raise
        return self

    def __exit__(self, *exc):
        try:
            stat = self.path.stat()
            identity = (stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_size)
            if identity == self.identity and self.path.read_text() == self.token:
                self.path.unlink()
        except FileNotFoundError:
            pass


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError('state/report timestamp must be an ISO timestamp')
    result = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('timestamp must include timezone')
    return result


def sha(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-fA-F]{40}', value):
        raise ValueError('commit must be a full SHA')
    return value.lower()


def save(path, state):
    temporary = path.with_name(path.name+'.'+secrets.token_hex(8)+'.tmp')
    try:
        temporary.write_text(json.dumps(state, indent=2)+'\n', encoding='utf-8')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def regressions(old, new):
    before, after = delta.records(old), delta.records(new)
    return {key: row for key, row in after.items() if key in before and
            ((before[key]['outcome'] == 'PASS' and row['outcome'] in ('FAIL','HARNESS_ERROR')) or
             (before[key]['outcome'] == 'COMPATIBLE' and row['outcome'] in ('UNSUPPORTED','DIVERGENT','HARNESS_ERROR')))}


def relevant(paths):
    return any(path.startswith('crates/') and not path.startswith('crates/giggles-pg-conformance/') for path in paths)


def _run(args, runner, repo, env):
    try:
        if not math.isfinite(args.min_interval_minutes) or args.min_interval_minutes <= 0:
            raise ValueError('--min-interval-minutes must be positive and finite')
        state_path = util.output_root(args.state)
        target = util.output_root(args.cargo_target)
        for value in (args.build_root, args.out_root):
            util.output_root(value)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        def git(*command):
            result = util.command(['git', '-C', str(repo), *command], env)
            if result['exit'] != 0:
                raise ValueError('git failed: '+result['stderr'])
            return result['stdout'].strip()
        current = sha(git('rev-parse', '--verify', 'main^{commit}'))
        if state_path.exists():
            state = json.loads(state_path.read_text(encoding='utf-8'))
        else:
            if not args.seed_report or not args.seed_commit:
                raise ValueError('missing watch state requires --seed-report and --seed-commit')
            seed = delta.load(args.seed_report)
            seed_sha = sha(args.seed_commit)
            if sha(seed.get('build', {}).get('commit')) != seed_sha:
                raise ValueError('seed commit does not match report build.commit')
            state = dict(last_run_commit=seed_sha, last_run_at=seed.get('finished_at') or seed.get('started_at'), last_report=str(Path(args.seed_report).resolve()))
            timestamp(state['last_run_at'])
            save(state_path, state)
        previous = sha(state['last_run_commit'])
        elapsed = (datetime.datetime.now(datetime.timezone.utc)-timestamp(state['last_run_at'])).total_seconds()
        old = delta.load(state['last_report'])
        if sha(old.get('build', {}).get('commit')) != previous:
            raise ValueError('state commit does not match baseline report')
        reason = None
        if current == previous:
            reason = 'main unchanged'
        elif elapsed < args.min_interval_minutes*60:
            reason = 'minimum interval has not elapsed'
        elif not relevant(git('diff', '--name-only', previous, current).splitlines()):
            reason = 'no qualifying crates changes'
        elif any((target/name).exists() for name in ('.gd-r0-run.lock', '.gd-r0-build.lock')):
            reason = 'another harness run or build is active'
        if reason:
            print('watch: no-op: '+reason)
            return 0
        base = ['run', '--commit', current, '--out-root', str(args.out_root), '--build-root', str(args.build_root), '--cargo-target', str(target)]
        try:
            code, path = runner(base)
        except LockBusy:
            print('watch: no-op: another harness run acquired the lock')
            return 0
        if code == 2 or path is None:
            raise ValueError('full harness run failed; baseline unchanged')
        new = delta.load(path)
        if sha(new.get('build', {}).get('commit')) != current:
            raise ValueError('full report commit does not match main')
        finished = new.get('finished_at') or new.get('started_at')
        timestamp(finished)
        save(state_path, dict(last_run_commit=current, last_run_at=finished, last_report=str(Path(path).resolve())))
        markdown, _ = delta.compare(old, new)
        entries = regressions(old, new)
        isolated = {}
        for key, row in entries.items():
            group = ('client', row['client']) if key.startswith('check:') else ('corpus', row['area']+'/'+row['id'])
            if group not in isolated:
                options = ['--clients', group[1], '--no-corpus'] if group[0] == 'client' else ['--clients', 'psql', '--checks', '__none__', '--corpus', group[1]]
                try:
                    isolated[group] = runner(base+options)
                except (LockBusy, OSError, ValueError):
                    isolated[group] = (2, None)
            isolated_code, isolated_path = isolated[group]
            label = 'isolation-inconclusive'
            if isolated_code != 2 and isolated_path:
                try:
                    result = delta.records(delta.load(isolated_path)).get(key, {}).get('outcome')
                    if result in ('FAIL','HARNESS_ERROR','UNSUPPORTED','DIVERGENT'):
                        label = 'reproduces-in-isolation'
                    elif result in ('PASS', 'COMPATIBLE'):
                        label = 'state-dependent (passes alone)'
                except (OSError, ValueError):
                    pass
            markdown = markdown.replace('- `'+key+'`:', '- `'+key+'` ['+label+']:', 1)
            markdown += '\nIsolation evidence for `'+key+'`: '+str(isolated_path or 'no report')+' ('+label+')\n'
            print('REGRESSION '+previous[:7]+'..'+current[:7]+' '+key+' '+label)
        output = state_path.parent/('delta-'+previous[:7]+'-'+current[:7]+'.md')
        output.write_text(markdown, encoding='utf-8')
        print(output)
        if entries:
            print(git('log', '--oneline', previous+'..'+current, '--', 'crates'))
        return 3 if entries else 0
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        print('watch: '+str(exc), file=sys.stderr)
        return 2


def run(args, runner, repo):
    try:
        directory = util.output_root(args.state).parent
        directory.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='.watch-tmp-', dir=directory) as temporary:
            env = os.environ.copy()
            env.update(TEMP=temporary, TMP=temporary, CARGO_TARGET_DIR=str(util.output_root(args.cargo_target)), PYTHONDONTWRITEBYTECODE='1')
            return _run(args, runner, repo, env)
    except (OSError, ValueError) as exc:
        print('watch: '+str(exc), file=sys.stderr)
        return 2
