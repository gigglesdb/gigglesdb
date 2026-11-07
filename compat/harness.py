#!/usr/bin/env python3
"""Run pinned PostgreSQL client checks against PostgreSQL and GigglesDB."""
import sys
sys.dont_write_bytecode = True
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import argparse
import datetime
import fnmatch
import json
import math
import secrets
from compat_lib import checks, docker, gigglesdb, oracle, pins, psql_client, report, util

def parser():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='action', required=True)
    run = sub.add_parser('run')
    run.add_argument('--server-bin', type=Path)
    run.add_argument('--build-root', default='J:/gigglesdb-work/r0-harness/builds')
    run.add_argument('--cargo-target', default='J:/gigglesdb-work/r0-harness/cargo-target')
    run.add_argument('--commit')
    run.add_argument('--out-root', default='J:/gigglesdb-work/r0-harness/runs')
    run.add_argument('--clients', default=','.join(psql_client.REGISTRY))
    run.add_argument('--checks', default='*')
    run.add_argument('--client-timeout', type=float, default=60)
    for flag in ('pull','keep','strict'):
        run.add_argument('--'+flag, action='store_true')
    return p

def setup_client(engine, client, client_pin, args, data, password):
    reason = 'client unavailable'
    try:
        available = client_pin is not None and engine.image(pins.reference(client_pin), args.pull)
    except (OSError, RuntimeError) as exc:
        available = False
        reason = util.redact(str(exc), [password])
    meta = dict(client_pin or {}, available=available, version=None)
    data['clients'][client] = meta
    if available:
        version = psql_client.REGISTRY[client]['version'](engine, client_pin, engine.prefix+'version-'+client, args.client_timeout)
        available = version['exit'] == 0 and not version['timed_out']
        meta.update(available=available, version=version['stdout'].strip() if available else None)
    return available, reason


def run_check(engine, client, client_pin, check, available, unavailable_reason, args, data, run, network, pg_host, port, password):
    directory = run/'checks'/client/check['id']
    directory.mkdir(parents=True, exist_ok=False)
    item = {k:check[k] for k in ('id','title','owner')}
    item.update(env=check.get('env', {}), compare=check.get('compare', 'stdout'))
    item.update(client=client, outcome='UNTESTED', pg=None, giggles=None, diff_excerpt=[], first_error=None, artifact_dir=str(directory))
    if not available or check.get('status'):
        item['reason'] = check.get('status') or unavailable_reason
    else:
        outputs = {}
        for side, host, dest_port in [('pg',pg_host,5432), ('giggles','host.docker.internal',port)]:
            name = engine.prefix+'c-'+str(len(engine.containers))
            result = psql_client.REGISTRY[client]['run'](engine,client_pin,network,name,host,dest_port,check['sql'],args.client_timeout, client_env=check['env'])
            outputs[side] = result
            item[side] = {k:result[k] for k in ('exit','duration_ms','timed_out')}
            for stream in ('stdout','stderr'):
                (directory/f'{side}.{stream}.txt').write_text(util.redact(result[stream],[password]),encoding='utf-8')
        item['outcome'] = checks.classify(outputs['pg'], outputs['giggles'], compare=check.get('compare','stdout'))
        item['first_error'] = next((line for line in outputs['giggles']['stderr'].splitlines() if 'ERROR' in line), None)
        if outputs['giggles']['exit'] == 0 or item['first_error']:
            data['networking']['status'] = 'reached-server'
        if item['outcome'] == 'FAIL':
            difference = checks.output_diff(outputs['pg'],outputs['giggles'],check.get('compare','stdout'))
            (directory/'diff.txt').write_text(util.redact(difference,[password]),encoding='utf-8')
            item['diff_excerpt'] = difference.splitlines()[:40]
    item = util.redact(item,[password])
    (directory/'result.json').write_text(json.dumps(item,indent=2)+'\n',encoding='utf-8')
    data['checks'].append(item)

def main(argv=None):
    args = parser().parse_args(argv)
    if not math.isfinite(args.client_timeout) or args.client_timeout <= 0:
        parser().error('--client-timeout must be positive')
    try:
        root = util.output_root(args.out_root)
        build_root = util.output_root(args.build_root)
        cargo_target = util.output_root(args.cargo_target)
    except ValueError as exc:
        print(str(exc), file=sys.stderr); return 2
    run_id = 'r0-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+secrets.token_hex(3)
    run = root/run_id
    run.mkdir(parents=True)
    env = util.child_env(run)
    password = secrets.token_urlsafe(30)
    env.update(POSTGRES_PASSWORD=password, PGPASSWORD=password, POSTGRES_INITDB_ARGS='--locale=C.UTF-8 --encoding=UTF8', TZ='UTC', PGTZ='UTC')
    selected = list(dict.fromkeys(x.strip() for x in args.clients.split(',') if x.strip()))
    data = dict(schema=1, run_id=run_id, started_at=util.utc(), finished_at=None, harness={}, build=dict(commit=args.commit, build_identity='labelled-by-operator' if args.commit else 'unverified-binary'), oracle={}, clients={}, networking=dict(hostname='host.docker.internal', add_host='host.docker.internal:host-gateway', listener='127.0.0.1', status='unverified'), normalization_rules=checks.NORMALIZATION_RULES, checks=[])
    engine = docker.Docker(run_id, env)
    server = None
    infra = False
    try:
        if not selected or any(name not in psql_client.REGISTRY for name in selected):
            raise ValueError('--clients must contain registered client names: '+', '.join(psql_client.REGISTRY))
        for key, command in [('commit',['git','rev-parse','HEAD']), ('dirty',['git','status','--porcelain','--','compat','scripts/compat.ps1'])]:
            result = util.command(['git','-C',str(Path(__file__).resolve().parent.parent),*command[1:]], env)
            data['harness'][key] = result['stdout'].strip() if key == 'commit' else bool(result['stdout'].strip())
        pinned = pins.load()
        env.update(POSTGRES_INITDB_ARGS=pinned['postgres']['initdb_args'], TZ=pinned['postgres']['tz'], PGTZ=pinned['postgres']['tz'])
        binary = args.server_bin
        if binary is None:
            binary, build_meta = gigglesdb.prepare_build(Path(__file__).resolve().parent.parent, args.commit or 'HEAD', build_root, cargo_target, env)
            data['build'].update(build_meta)
        engine.require('info',timeout=30)
        if not engine.image(pins.reference(pinned['postgres']), args.pull):
            raise RuntimeError('pinned oracle image unavailable: '+pins.reference(pinned['postgres'])+'; use --pull to permit fetching')
        network, pg_host, data['oracle'] = oracle.start(engine, pinned)
        server = gigglesdb.Server(binary.resolve(strict=True), run, env, password)
        data['build'].update(server.meta)
        port = server.start()
        data['build'].update(server.meta)
        for client in selected:
            client_pin = pinned['clients'].get(client)
            available, unavailable_reason = setup_client(engine, client, client_pin, args, data, password)
            paths = sorted((Path(__file__).parent/'checks'/client).glob('*.sql'))
            seen = set()
            for path in paths:
                check = checks.parse_check(path)
                if check['id'] in seen:
                    raise ValueError('duplicate check id: '+check['id'])
                seen.add(check['id'])
                if not (fnmatch.fnmatch(check['id'], args.checks) or fnmatch.fnmatch(path.name,args.checks)):
                    continue
                run_check(engine, client, client_pin, check, available, unavailable_reason, args, data, run, network, pg_host, port, password)
    except (Exception, KeyboardInterrupt) as exc:
        infra = True
        if isinstance(exc, gigglesdb.BuildError):
            data['build'].update(exc.metadata)
        data['error'] = util.redact(str(exc) or type(exc).__name__, [password])
    finally:
        if server:
            try:
                server.stop()
                data['build'].update(server.meta)
            except Exception as exc:
                infra = True; data['shutdown_error'] = util.redact(str(exc),[password])
        if args.keep:
            retained = []
            for name in engine.containers:
                if engine.call('container','inspect',name)['exit'] == 0:
                    retained.append(name)
            for name in engine.networks:
                if engine.call('network','inspect',name)['exit'] == 0:
                    retained.append(name)
            data['retained_objects'] = retained
            print('Retained objects: '+', '.join(retained))
        else:
            try:
                engine.cleanup()
            except Exception as exc:
                infra = True; data['cleanup_error'] = util.redact(str(exc),[password])
        data['finished_at'] = util.utc()
        data['summary'], data['failures_by_owner'] = report.summarize(data['checks'], selected)
        report.write(run,data,[password])
        print(run/'report.md')
        print(report.client_table(data['summary']['clients']))
    if infra:
        return 2
    return 1 if args.strict and any(c['outcome'] in ('FAIL','HARNESS_ERROR') for c in data['checks']) else 0

if __name__ == '__main__':
    raise SystemExit(main())
