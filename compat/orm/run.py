#!/usr/bin/env python3
"""Qualify unchanged Django and SQLAlchemy samples against PG18 and GigglesDB v2."""
import sys
sys.dont_write_bytecode = True
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import argparse
import datetime
import hashlib
import json
import secrets
import shutil
from compat_lib import docker, gigglesdb, oracle, pins, util


def write(path, value):
    path.write_text(value, encoding='utf-8', newline='\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server-bin', type=Path, required=True)
    parser.add_argument('--orm', choices=['django', 'sqlalchemy', 'all'], default='all')
    parser.add_argument('--out-root', default='J:/gigglesdb-work/p1-orm/runs')
    args = parser.parse_args()
    run_id = 'r0-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + secrets.token_hex(3)
    root = util.output_root(args.out_root)
    if not root.is_relative_to(Path('J:/gigglesdb-work').resolve()):
        parser.error('--out-root must be under J:/gigglesdb-work')
    run = root / run_id
    run.mkdir(parents=True)
    password = secrets.token_urlsafe(30)
    env = util.child_env(run)
    env.update(POSTGRES_PASSWORD=password, PGPASSWORD=password, POSTGRES_INITDB_ARGS='--locale=C.UTF-8 --encoding=UTF8', TZ='UTC', PGTZ='UTC')
    engine = docker.Docker(run_id, env)
    data = dict(run_id=run_id, results=[], oracle={}, errors=[], known_divergences=[
        'No numeric/timestamptz secondary indexes: unsupported by GigglesDB; neither ORM requires one by default.'])
    app = Path(__file__).resolve().parent
    try:
        pinned = pins.load()
        engine.image(pins.reference(pinned['postgres']), True)
        base = pins.reference(pinned['orm_python'])
        engine.image(base, True)
        digest = hashlib.sha256(b''.join(p.relative_to(app).as_posix().encode() + p.read_bytes() for p in sorted(app.rglob('*')) if p.is_file())).hexdigest()[:12]
        image = 'gd-r0-orm:' + digest
        result = engine.call('build', '--build-arg', 'BASE=' + base, '-t', image, str(app), timeout=600)
        write(run / 'image-build.log', util.redact(result['stdout'] + result['stderr'], [password]))
        if result['exit'] != 0 or result['timed_out']:
            raise RuntimeError('ORM image build failed; see image-build.log')
        data['image'] = json.loads(engine.require('image', 'inspect', image))[0]['Id']
        data['packages'] = engine.require('run', '--rm', '--entrypoint', 'pip', image, 'freeze').splitlines()
        network, pg_host, data['oracle'] = oracle.start(engine, pinned)
        for orm in (['django', 'sqlalchemy'] if args.orm == 'all' else [args.orm]):
            work = run / orm
            work.mkdir()
            server = gigglesdb.Server(args.server_bin.resolve(strict=True), work, env, password, server_args=['--engine', 'v2'])
            try:
                port = server.start()
                for target, host, dest_port in [('pg', pg_host, 5432), ('giggles', 'host.docker.internal', port)]:
                    name = engine.prefix + orm + '-' + target
                    engine.containers.append(name)
                    client_env = dict(env, PGHOST=host, PGPORT=str(dest_port))
                    output = util.command(['docker', 'run', '--rm', '--name', name, '--network', network,
                        '--add-host', 'host.docker.internal:host-gateway', '-e', 'PGPASSWORD', '-e', 'PGHOST', '-e', 'PGPORT', image, orm], client_env, timeout=180)
                    for stream in ['stdout', 'stderr']:
                        write(work / (target + '.' + stream + '.txt'), util.redact(output[stream], [password]))
                    records = [line.removeprefix('ORM_RESULT=') for line in output['stdout'].splitlines() if line.startswith('ORM_RESULT=')]
                    if output['exit'] != 0 or output['timed_out'] or len(records) != 1:
                        raise RuntimeError(orm + '/' + target + ': invalid probe output')
                    rows = json.loads(records[0])
                    if len(rows) != 10:
                        data['errors'].append(orm + '/' + target + ': incomplete probe (' + str(len(rows)) + '/10 steps)')
                    data['results'].extend(dict(row, orm=orm, target=target) for row in rows)
            finally:
                server.stop()
                data.setdefault('servers', {})[orm] = server.meta
                for sub in ['data', 'bin', 'cwd']:
                    if (work / sub).exists():
                        shutil.rmtree(work / sub)
    except Exception as exc:
        data['errors'].append(str(exc))
    finally:
        for action in [lambda: oracle.capture_log(engine, run / 'postgres.log', [password]), engine.cleanup]:
            try:
                action()
            except Exception as exc:
                data['errors'].append(str(exc))
        data = util.redact(data, [password])
        write(run / 'report.json', json.dumps(data, indent=2) + '\n')
        lines = ['# Python ORM qualification', '', '| ORM | Target | Step | Result |', '|---|---|---|---|']
        lines += ['| {orm} | {target} | {step} | {status} |'.format(**row) for row in data['results']]
        for row in data['results']:
            if row['status'] == 'FAIL':
                lines += ['', '## ' + row['orm'] + '/' + row['target'] + '/' + row['step'], '```json', json.dumps(row, indent=2), '```']
        lines += ['', 'Known divergence: ' + data['known_divergences'][0], '', 'Infrastructure errors: ' + json.dumps(data['errors'])]
        write(run / 'report.md', '\n'.join(lines) + '\n')
        print(run / 'report.md')
    return int(bool(data['errors']) or any(row['status'] != 'PASS' for row in data['results']))


if __name__ == '__main__':
    sys.exit(main())
