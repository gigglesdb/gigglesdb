#!/usr/bin/env python3
"""Qualify unchanged ORM samples against PG18 and GigglesDB v2."""
import sys
sys.dont_write_bytecode = True
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import argparse
import datetime
import hashlib
import json
import re
import secrets
import shutil
from compat_lib import docker, gigglesdb, oracle, pins, util


def write(path, value):
    path.write_text(value, encoding='utf-8', newline='\n')


def server_evidence(rows, log):
    """Recover errors a client suppresses, joining exact SQL by server SHA-256."""
    errors = {query_id: dict(sqlstate=state, phase=phase, query_id=query_id)
              for state, phase, query_id in re.findall(r'protocol=postgres sqlstate=(\w+) phase=(.*?); query_id=([0-9a-f]+)', log)}
    for row in rows:
        if row['status'] != 'FAIL' or row.get('classification') == 'blocked':
            continue
        evidence = []
        for failure in row.get('failures', []):
            for sql in [failure.get('sql', ''), *re.findall(r'db.query.text: (.*?), otel.kind:', failure.get('output', ''), re.S)]:
                query_id = hashlib.sha256(sql.encode()).hexdigest()
                if query_id in errors:
                    evidence.append(dict(sql=sql, source='ORM SQL trace + server query hash', **errors[query_id]))
        for state, reason in re.findall(r'protocol=postgres sqlstate=(\w+) reason=([^\n]+)', log):
            if reason in row.get('error', ''):
                evidence.append(dict(sql=None, phase='startup (no SQL sent)', sqlstate=state, error=reason,
                                     source='server startup diagnostic'))
        if evidence:
            row.update(sqlstate=evidence[0]['sqlstate'], classification='product-gap', server_evidence=evidence)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server-bin', type=Path, required=True)
    parser.add_argument('--orm', choices=['django', 'sqlalchemy', 'rails', 'prisma', 'all'], default='all')
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
        'No numeric/timestamptz secondary indexes: unsupported by GigglesDB; none of these ORMs requires one by default.'])
    app = Path(__file__).resolve().parent
    try:
        pinned = pins.load()
        engine.image(pins.reference(pinned['postgres']), True)
        network, pg_host, data['oracle'] = oracle.start(engine, pinned)
        images = {}
        data['images'] = {}
        for orm in (['django', 'sqlalchemy', 'rails', 'prisma'] if args.orm == 'all' else [args.orm]):
            kind = orm if orm in ['rails', 'prisma'] else 'python'
            if kind not in images:
                context = app if kind == 'python' else app / kind
                base = pins.reference(pinned['orm_' + dict(python='python', rails='ruby', prisma='node')[kind]])
                engine.image(base, True)
                digest = hashlib.sha256(base.encode() + b''.join(p.relative_to(context).as_posix().encode() + p.read_bytes()
                    for p in sorted(context.rglob('*')) if p.is_file())).hexdigest()[:12]
                image = 'gd-r0-orm-' + kind + ':' + digest
                result = engine.call('build', '--build-arg', 'BASE=' + base, '-t', image, str(context), timeout=900)
                write(run / (kind + '-image-build.log'), util.redact(result['stdout'] + result['stderr'], [password]))
                if result['exit'] != 0 or result['timed_out']:
                    raise RuntimeError(kind + ' image build failed; see image-build.log')
                images[kind] = image
                version_cmd = dict(python=['pip', 'freeze'], rails=['gem', 'list', '--local'], prisma=['npm', 'ls', '--depth=0'])[kind]
                runtime_cmd = dict(python=['python', '--version'], rails=['ruby', '--version'], prisma=['node', '--version'])[kind]
                data['images'][kind] = dict(base=base, image=json.loads(engine.require('image', 'inspect', image))[0]['Id'],
                    packages=engine.require('run', '--rm', '--entrypoint', version_cmd[0], image, *version_cmd[1:]).splitlines(),
                    runtime=engine.require('run', '--rm', '--entrypoint', runtime_cmd[0], image, *runtime_cmd[1:]))
                if kind == 'python':
                    data.update(image=data['images'][kind]['image'], packages=data['images'][kind]['packages'])
            image = images[kind]
            # Each lane needs an empty oracle, just like its fresh GigglesDB server.
            engine.require('exec', '-e', 'PGPASSWORD', pg_host, 'psql', '-X', '-U', 'postgres', '-d', 'postgres',
                           '-v', 'ON_ERROR_STOP=1', '-c', 'DROP SCHEMA public CASCADE; CREATE SCHEMA public')
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
                        '--add-host', 'host.docker.internal:host-gateway', '-e', 'PGPASSWORD', '-e', 'PGHOST', '-e', 'PGPORT', image, orm], client_env, timeout=240)
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
                server_evidence([row for row in data['results'] if row['orm'] == orm and row['target'] == 'giggles'],
                                (work / 'server.stderr.log').read_text(encoding='utf-8') if (work / 'server.stderr.log').exists() else '')
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
        lines = ['# ORM qualification', '', '| ORM | Target | Step | Result |', '|---|---|---|---|']
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
