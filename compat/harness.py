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
from compat_lib import checks, corpus, delta, docker, gigglesdb, oracle, pins, probes, psql_client, report, scratch, util, watch

def parser():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='action', required=True)
    comparison = sub.add_parser('delta')
    comparison.add_argument('old', type=Path)
    comparison.add_argument('new', type=Path)
    comparison.add_argument('--out', type=Path)
    run = sub.add_parser('run')
    run.add_argument('--server-bin', type=Path)
    run.add_argument('--server-arg', action='append', default=[], help='extra server argument (use --server-arg=--flag)')
    run.add_argument('--build-root', default='J:/gigglesdb-work/r0-harness/builds')
    run.add_argument('--build-worktree', default='J:/gigglesdb-work/wt/r0-build')
    run.add_argument('--cargo-target', default='J:/gigglesdb-work/r0-harness/cargo-target')
    run.add_argument('--commit')
    run.add_argument('--out-root', default='J:/gigglesdb-work/r0-harness/runs')
    run.add_argument('--clients', default=','.join(psql_client.REGISTRY))
    run.add_argument('--checks', default='*')
    run.add_argument('--owners', default=','.join(sorted(checks.OWNERS)))
    run.add_argument('--corpus', default='*')
    run.add_argument('--no-corpus', action='store_true')
    run.add_argument('--no-checks', action='store_true')
    run.add_argument('--client-timeout', type=float, default=60)
    run.add_argument('--keep-scratch', action='store_true')
    for flag in ('pull','keep','strict'):
        run.add_argument('--'+flag, action='store_true')
    observer = sub.add_parser('watch')
    observer.add_argument('--state', type=Path, default=Path('J:/gigglesdb-work/r0-harness/watch/state.json'))
    observer.add_argument('--seed-report', type=Path)
    observer.add_argument('--seed-commit')
    observer.add_argument('--min-interval-minutes', type=float, default=60)
    for flag in ('build-root', 'build-worktree', 'cargo-target', 'out-root'):
        observer.add_argument('--'+flag, default=run.get_default(flag.replace('-', '_')))
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
            result = psql_client.REGISTRY[client]['run'](engine,client_pin,network,name,host,dest_port,check['sql'],args.client_timeout, client_env=dict(check.get('env', {}), PGAPPNAME='r0:psql:'+check['id']))
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

def run_corpus_case(engine, client, client_pin, case, available, reason, data, run,
                    network, pg_host, port, password):
    directory = run/'corpus'/case['area']/case['id']
    directory.mkdir(parents=True, exist_ok=False)
    script = corpus.generate_script(case)
    (directory/'script.sql').write_text(script, encoding='utf-8')
    runner = psql_client.REGISTRY[client].get('run_session')
    if case['expect'] == 'untested' or not available or runner is None:
        item = {key:case.get(key) for key in ('area','id','title','owner','expect','reason','source')}
        item['compare'] = case.get('compare', 'full')
        item.update(outcome='UNTESTED', pg=None, giggles=None, first_divergence=None,
                    warnings=case.get('warnings', []), stale_expectation=False, row_counts=[])
        if case['expect'] != 'untested':
            item['reason'] = reason if not available else 'client has no session runner'
    else:
        outputs = {}
        for side, host, dest_port in [('pg',pg_host,5432), ('giggles','host.docker.internal',port)]:
            name = engine.prefix+'c-'+str(len(engine.containers))
            result = runner(engine,client_pin,network,name,host,dest_port,script,120,client_env={'PGAPPNAME':'r0:corpus:'+case['area']+'/'+case['id']})
            outputs[side] = result
            for stream in ('stdout','stderr'):
                (directory/f'{side}.{stream}.txt').write_text(util.redact(result[stream],[password]),encoding='utf-8')
        cleanup = {}
        try:
            cleanup_script = corpus.generate_cleanup_script(case)
            if cleanup_script is not None:
                (directory/'cleanup.sql').write_text(cleanup_script, encoding='utf-8')
        except Exception as exc:
            cleanup_script = None
            cleanup = {side: {'error': str(exc)} for side in ('pg', 'giggles')}
        if cleanup_script is not None:
            for side, host, dest_port in [('pg',pg_host,5432), ('giggles','host.docker.internal',port)]:
                try:
                    name = engine.prefix+'c-'+str(len(engine.containers))
                    result = runner(engine,client_pin,network,name,host,dest_port,cleanup_script,120,
                                    client_env={'PGAPPNAME':'r0:cleanup:'+case['area']+'/'+case['id']})
                    cleanup[side] = dict(exit=result['exit'] if result['exit'] is not None else -1,
                                         timed_out=bool(result['timed_out']))
                    for stream in ('stdout','stderr'):
                        (directory/f'{side}.cleanup.{stream}.txt').write_text(util.redact(result[stream],[password]),encoding='utf-8')
                except Exception as exc:
                    cleanup[side] = {'error': str(exc)}
        transcripts = {}
        for side, output in outputs.items():
            transcript = corpus.parse_transcript(output['stdout'], case['statements'], side, output['stderr'])
            transcript.update({key:output[key] for key in ('exit','duration_ms','timed_out')})
            if side == 'pg' and output['exit'] != 0:
                transcript.update(valid=False, reason='PostgreSQL session failed')
            transcripts[side] = transcript
        item = corpus.compare_case(case, transcripts['pg'], transcripts['giggles'])
        item['cleanup'] = cleanup
        reached = outputs['giggles']['exit'] == 0 or any(
            len(record.get('sqlstate', '')) == 5 and record['sqlstate'].isalnum()
            for record in transcripts['giggles']['records'])
        if reached:
            data['networking']['status'] = 'reached-server'
    item['artifact_dir'] = str(directory)
    item = util.redact(item,[password])
    (directory/'result.json').write_text(json.dumps(item,indent=2)+'\n',encoding='utf-8')
    summary_keys = ('area','id','title','owner','expect','reason','outcome','compare','source',
                    'first_divergence','warnings','stale_expectation','row_counts','artifact_dir','harness_errors','cleanup','sqlstate_pin_failures')
    data['differential']['cases'].append({key:item.get(key) for key in summary_keys})


def run_probe_client(engine, client, registration, args, data, run, network, pg_host, port, password):
    """Execute a registered driver once per server; filter only its reported cases."""
    probe_dir = Path(__file__).parent/registration['probe_dir']
    manifest = probes.load_manifest(probe_dir/'cases.json')
    directory = run/'checks'/client
    # A shared driver's earlier alias may already have written image build logs.
    directory.mkdir(parents=True, exist_ok=True)
    image = probes.prepare_image(engine, registration['driver'], probe_dir, run,
                                 expected_version=registration['version_expected'])
    data['clients'][client] = dict(image, image=image.get('tag'), version=None)
    outputs = {}
    if image['available']:
        base = len(engine.containers)
        for index, (side, host, dest_port) in enumerate([('pg',pg_host,5432), ('giggles','host.docker.internal',port)]):
            outputs[side] = probes.run_probe_side(
                engine, registration['driver'], image, network, engine.prefix+'c-'+str(base+index),
                host, dest_port, manifest, expected_version=registration['version_expected'], timeout=180,
                r0_prefix=registration.get('r0_prefix'), version_fields=registration.get('version_fields', ()),
                client_env=dict(registration.get('client_env', {}), PGAPPNAME='r0:'+client))
            for stream in ('stdout','stderr'):
                (directory/f'{side}.{stream}.txt').write_text(util.redact(outputs[side][stream],[password]),encoding='utf-8')
        data['clients'][client]['version'] = outputs['pg'].get('version')
        for field in registration.get('version_fields', ()):
            data['clients'][client][field] = outputs['pg'].get(field)
        rows = probes.classify_cases(manifest, outputs['pg'], outputs['giggles'])
    else:
        rows = [dict(case, outcome='UNTESTED', reason=image.get('reason','probe image unavailable')) for case in manifest]
    for row in rows:
        if not owner_matches(row, args.owners):
            continue
        if not (fnmatch.fnmatch(row['id'], args.checks) or fnmatch.fnmatch(row['id']+'.json',args.checks)):
            continue
        item = dict(row, client=client, artifact_dir=str(directory/row['id']))
        item['outcome'] = item.pop('status', item.get('outcome', 'UNTESTED'))
        item.setdefault('pg', None)
        item.setdefault('giggles', None)
        difference = item.pop('diff', '')
        item['diff_excerpt'] = difference.splitlines()[:40]
        record = item['giggles'] or {}
        item['first_error'] = record.get('observation',{}).get('sqlstate') if record.get('status') == 'ERR' else None
        if record.get('status') in ('OK','ERR'):
            data['networking']['status'] = 'reached-server'
        item = util.redact(item,[password])
        artifact = directory/row['id']
        artifact.mkdir()
        if difference:
            (artifact/'diff.txt').write_text(util.redact(difference,[password]),encoding='utf-8')
        (artifact/'result.json').write_text(json.dumps(item,indent=2,sort_keys=True)+'\n',encoding='utf-8')
        data['checks'].append(item)


def parse_owners(value):
    owners = {part.strip() for part in value.split(',') if part.strip()}
    if not owners or not owners <= checks.OWNERS:
        raise ValueError('--owners must contain owners from: '+', '.join(sorted(checks.OWNERS)))
    return owners


def owner_matches(case, owners):
    return case['owner'] in parse_owners(owners)


def corpus_matches(case, pattern):
    path = Path(case['path'])
    values = (case['id'], case['area'], case['area']+'/'+case['id'],
              case['area']+'/'+path.name, str(path), path.as_posix())
    return any(fnmatch.fnmatch(value, pattern) for value in values)


def _run(args, capture):
    try:
        parse_owners(args.owners)
    except ValueError as exc:
        parser().error(str(exc))
    if not math.isfinite(args.client_timeout) or args.client_timeout <= 0:
        parser().error('--client-timeout must be positive')
    try:
        root = util.output_root(args.out_root)
        build_root = util.output_root(args.build_root)
        cargo_target = util.output_root(args.cargo_target)
        build_worktree = gigglesdb.validate_build_worktree(getattr(args, 'build_worktree', 'J:/gigglesdb-work/wt/r0-build'), Path(__file__).resolve().parent.parent)
    except ValueError as exc:
        print(str(exc), file=sys.stderr); return 2
    run_id = 'r0-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+secrets.token_hex(3)
    run = root/run_id
    run.mkdir(parents=True)
    capture.append(run/'report.json')
    env = util.child_env(run)
    password = secrets.token_urlsafe(30)
    env.update(POSTGRES_PASSWORD=password, PGPASSWORD=password, POSTGRES_INITDB_ARGS='--locale=C.UTF-8 --encoding=UTF8', TZ='UTC', PGTZ='UTC')
    selected = list(dict.fromkeys(x.strip() for x in args.clients.split(',') if x.strip()))
    data = dict(schema=1, run_id=run_id, started_at=util.utc(), finished_at=None, harness={}, build=dict(commit=args.commit, build_identity='labelled-by-operator' if args.commit else 'unverified-binary'), oracle={}, clients={}, networking=dict(hostname='host.docker.internal', add_host='host.docker.internal:host-gateway', listener='127.0.0.1', status='unverified'), normalization_rules=checks.NORMALIZATION_RULES, checks=[], differential=dict(unsupported_sqlstates=sorted(corpus.UNSUPPORTED_STATES), cases=[]))
    engine = docker.Docker(run_id, env)
    server = None
    infra = False
    stopped = True
    try:
        if not selected or any(name not in psql_client.REGISTRY for name in selected):
            raise ValueError('--clients must contain registered client names: '+', '.join(psql_client.REGISTRY))
        for key, command in [('commit',['git','rev-parse','HEAD']), ('dirty',['git','status','--porcelain','--','compat','scripts/compat.ps1'])]:
            result = util.command(['git','-C',str(Path(__file__).resolve().parent.parent),*command[1:]], env)
            data['harness'][key] = result['stdout'].strip() if key == 'commit' else bool(result['stdout'].strip())
        cases = corpus.discover_cases(Path(__file__).parent/'corpus') if not args.no_corpus else []
        cases = [case for case in cases if corpus_matches(case, args.corpus) and owner_matches(case, args.owners)]
        pinned = pins.load()
        env.update(POSTGRES_INITDB_ARGS=pinned['postgres']['initdb_args'], TZ=pinned['postgres']['tz'], PGTZ=pinned['postgres']['tz'])
        binary = args.server_bin
        if binary is None:
            binary, build_meta = gigglesdb.prepare_build(Path(__file__).resolve().parent.parent, args.commit or 'HEAD', build_root, cargo_target, env, build_worktree=build_worktree)
            data['build'].update(build_meta)
        engine.require('info',timeout=30)
        if not engine.image(pins.reference(pinned['postgres']), args.pull):
            raise RuntimeError('pinned oracle image unavailable: '+pins.reference(pinned['postgres'])+'; use --pull to permit fetching')
        network, pg_host, data['oracle'] = oracle.start(engine, pinned)
        server = gigglesdb.Server(binary.resolve(strict=True), run, env, password, server_args=args.server_arg)
        data['build'].update(server.meta)
        port = server.start()
        data['build'].update(server.meta)
        for client in selected:
            registration = psql_client.REGISTRY[client]
            if registration.get('run_client'):
                registration['run_client'](engine,client,registration,args,data,run,network,pg_host,port,password)
                continue
            if registration.get('kind') == 'probe':
                if args.no_checks:
                    data['clients'][client] = dict(available=False, version=None, reason='checks disabled')
                else:
                    run_probe_client(engine,client,registration,args,data,run,network,pg_host,port,password)
                continue
            client_pin = pinned['clients'].get(client)
            available, unavailable_reason = setup_client(engine, client, client_pin, args, data, password)
            paths = [] if args.no_checks else sorted((Path(__file__).parent/'checks'/client).glob('*.sql'))
            seen = set()
            for path in paths:
                check = checks.parse_check(path)
                if check['id'] in seen:
                    raise ValueError('duplicate check id: '+check['id'])
                seen.add(check['id'])
                if not owner_matches(check, args.owners):
                    continue
                if not (fnmatch.fnmatch(check['id'], args.checks) or fnmatch.fnmatch(path.name,args.checks)):
                    continue
                run_check(engine, client, client_pin, check, available, unavailable_reason, args, data, run, network, pg_host, port, password)
        if cases:
            client = next((name for name in selected if psql_client.REGISTRY[name].get('run_session')), selected[0])
            for case in cases:
                run_corpus_case(engine, client, pinned['clients'].get(client), case, data['clients'][client]['available'], 'client unavailable', data, run, network, pg_host, port, password)
    except (Exception, KeyboardInterrupt) as exc:
        infra = True
        if isinstance(exc, gigglesdb.BuildError):
            data['build'].update(exc.metadata)
        data['error'] = util.redact(str(exc) or type(exc).__name__, [password])
    finally:
        if server:
            try:
                server.stop()
                if server.process is not None and server.process.poll() is None:
                    raise RuntimeError('server remains running after shutdown')
                data['build'].update(server.meta)
            except Exception as exc:
                stopped = False
                infra = True; data['shutdown_error'] = util.redact(str(exc),[password])
        if engine.prefix+'pg' in engine.containers:
            try:
                oracle.capture_log(engine, run/'oracle.log', [password])
                data['oracle_log'] = 'oracle.log'
                data['oracle']['oracle_log'] = 'oracle.log'
            except Exception as exc:
                infra = True
                data['oracle_log_error'] = util.redact(str(exc), [password])
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
        data['differential']['summary'] = corpus.summarize(data['differential']['cases'])
        report.write(run,data,[password])
        data.update(scratch_removed=[], scratch_bytes_freed=0)
        data['scratch_kept'] = args.keep_scratch
        if stopped:
            try:
                data.update(scratch.remove_children(run, keep=args.keep_scratch))
            except Exception as exc:
                infra = True
                data['scratch_cleanup_error'] = util.redact(str(exc), [password])
        else:
            data['scratch_cleanup_error'] = 'server shutdown failed; scratch retained'
        report.write(run,data,[password])
        print(run/'report.md')
        print(report.client_table(data['summary']['clients']))
        print(report.differential_table(data['differential']['summary']))
    if infra:
        return 2
    failed_checks = any(c['outcome'] in ('FAIL','HARNESS_ERROR') for c in data['checks'])
    failed_corpus = any(c['outcome'] in ('UNSUPPORTED','DIVERGENT','HARNESS_ERROR') for c in data['differential']['cases'])
    return 1 if args.strict and (failed_checks or failed_corpus) else 0

def run_once(argv=None, *, fail_if_build_busy=False):
    args = parser().parse_args(argv)
    capture = []
    try:
        try:
            parse_owners(args.owners)
        except ValueError as exc:
            parser().error(str(exc))
        if not math.isfinite(args.client_timeout) or args.client_timeout <= 0:
            parser().error('--client-timeout must be positive')
        selected = [name.strip() for name in args.clients.split(',') if name.strip()]
        if not selected or any(name not in psql_client.REGISTRY for name in selected):
            parser().error('--clients must contain registered client names')
        util.output_root(args.out_root)
        util.output_root(args.build_root)
        target = util.output_root(args.cargo_target)
        gigglesdb.validate_build_worktree(args.build_worktree, Path(__file__).resolve().parent.parent)
        with watch.RunLock(target):
            if fail_if_build_busy and (target/'.gd-r0-build.lock').exists():
                raise watch.LockBusy('another R0 build is active')
            code = _run(args, capture)
        return code, capture[0] if capture else None
    except watch.LockBusy:
        raise
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2, capture[0] if capture else None


def main(argv=None):
    args = parser().parse_args(argv)
    if args.action == 'delta':
        return delta.run(args.old, args.new, args.out)
    if args.action == 'watch':
        return watch.run(args, lambda argv: run_once(argv, fail_if_build_busy=True), Path(__file__).resolve().parent.parent)
    try:
        return run_once(argv)[0]
    except watch.LockBusy as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
