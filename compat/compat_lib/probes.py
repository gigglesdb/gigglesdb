"""Docker driver probes with strict, observation-based differential records.

Reuses GD0048's JdbcProbe.java CASE/base64 framing and src/lib.rs evaluate
integrity checks conceptually; this protocol adds JSON observations and DONE.
"""
import base64
import hashlib
import json
import math
from pathlib import Path
import re

from . import checks
from .util import command, redact


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def _json(text):
    def finite(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError('nonfinite JSON number')
        return parsed
    return json.loads(text, object_pairs_hook=_pairs,
                      parse_float=finite,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def validate_manifest(manifest):
    if not isinstance(manifest, list) or not manifest:
        raise ValueError('manifest must be a nonempty list')
    seen = set()
    for case in manifest:
        if not isinstance(case, dict) or set(case) - {'id', 'title', 'owner', 'expect_error', 'untested_reason'}:
            raise ValueError('invalid manifest fields')
        if not isinstance(case.get('id'), str) or not re.fullmatch(r'[A-Za-z0-9_-]+', case['id']) or case['id'] in seen:
            raise ValueError('invalid or duplicate case id')
        seen.add(case['id'])
        if not isinstance(case.get('title'), str) or not case['title'].strip() or not isinstance(case.get('owner'), str) or case['owner'] not in checks.OWNERS:
            raise ValueError('invalid case title or owner')
        if type(case.get('expect_error')) is not bool:
            raise ValueError('expect_error must be a boolean')
        if 'untested_reason' in case and (not isinstance(case['untested_reason'], str) or not case['untested_reason'].strip()):
            raise ValueError('untested_reason must be nonempty')
    return manifest


def load_manifest(path):
    return validate_manifest(_json(Path(path).read_text(encoding='utf-8')))


def _validate_reasons(value):
    if isinstance(value, dict):
        if ('exception_class' in value or value.get('status') == 'ERR') and 'reason' in value and (not isinstance(value['reason'], str) or
                                  not re.fullmatch(r'[a-z][a-z0-9_]{0,63}', value['reason'])):
            raise ValueError('invalid probe assertion reason')
        exception = value.get('exception_class', '')
        if isinstance(exception, str) and exception.endswith(('ProbeAssertion', 'AssertionError')) and 'reason' not in value:
            raise ValueError('probe assertion requires reason')
        for child in value.values():
            _validate_reasons(child)
    elif isinstance(value, list):
        for child in value:
            _validate_reasons(child)


def parse_records(stdout, manifest, driver, expected_version, version_fields=()):
    validate_manifest(manifest)
    result = dict(valid=False, error='', records={}, missing=[], version=None)
    expected = {case['id'] for case in manifest}
    try:
        if isinstance(stdout, bytes):
            stdout = stdout.decode('utf-8', 'strict')
        lines = stdout.split('\n')
        if lines[-1] == '':
            lines.pop()
        lines = [line[:-1] if line.endswith('\r') else line for line in lines]
        if version_fields not in ((), ('libpq_version',)) or (version_fields and driver != 'psycopg'):
            raise ValueError('unsupported VERSION metadata fields')
        version = lines[0].split('\t') if lines else []
        if len(version) != 3 + len(version_fields) or version[:3] != ['VERSION', driver, expected_version]:
            raise ValueError('missing or incorrect first VERSION record')
        for field, value in zip(version_fields, version[3:]):
            if not re.fullmatch(r'[1-9][0-9]*', value):
                raise ValueError('invalid VERSION integer metadata')
            result[field] = int(value)
        result['version'] = expected_version
        done = False
        for line in lines[1:]:
            fields = line.split('\t')
            if done:
                raise ValueError('stdout after DONE')
            if len(fields) == 2 and fields[0] == 'DONE':
                if not re.fullmatch(r'0|[1-9][0-9]*', fields[1]) or int(fields[1]) != len(result['records']):
                    raise ValueError('incorrect DONE count')
                done = True
                continue
            if len(fields) != 4 or fields[0] != 'CASE' or fields[2] not in ('OK', 'ERR'):
                raise ValueError('malformed or unsolicited stdout record')
            _, case_id, status, encoded = fields
            if case_id not in expected or case_id in result['records']:
                raise ValueError('unknown or duplicate case id')
            data = base64.b64decode(encoded, validate=True)
            if base64.b64encode(data).decode('ascii') != encoded:
                raise ValueError('noncanonical base64')
            observation = _json(data.decode('utf-8', 'strict'))
            if not isinstance(observation, dict):
                raise ValueError('observation must be a JSON object')
            _validate_reasons(observation)
            if status == 'ERR':
                exception = observation.get('exception_class')
                state = observation.get('sqlstate')
                if not isinstance(exception, str) or not exception.strip():
                    raise ValueError('ERR requires exception_class')
                if 'sqlstate' not in observation or (state is not None and not (isinstance(state, str) and re.fullmatch(r'[0-9A-Z]{5}', state))):
                    raise ValueError('ERR requires SQLSTATE')
            result['records'][case_id] = dict(status=status, observation=observation)
        if not done:
            raise ValueError('missing DONE')
        result['valid'] = True
    except (ValueError, TypeError, UnicodeError) as exc:
        result['error'] = str(exc)
    result['missing'] = sorted(expected - result['records'].keys())
    return result


def classify_cases(manifest, pg, giggles):
    rows = []
    for case in validate_manifest(manifest):
        row = dict(case, status='PASS', reason='', diff='')
        left = pg.get('records', {}).get(case['id'])
        right = giggles.get('records', {}).get(case['id'])
        if case.get('untested_reason') or not pg.get('available', True) or not giggles.get('available', True):
            row.update(status='UNTESTED', reason=case.get('untested_reason') or pg.get('reason') or giggles.get('reason') or 'probe image unavailable')
        elif pg.get('timed_out') or pg.get('exit', 0) != 0 or not pg.get('valid') or left is None:
            row.update(status='HARNESS_ERROR', reason=pg.get('error') or 'PostgreSQL probe failed or case missing')
        elif (left['status'] == 'ERR') != case['expect_error']:
            row.update(status='HARNESS_ERROR', reason='PostgreSQL result does not match expect_error')
        elif giggles.get('timed_out') or giggles.get('exit', 0) != 0 or not giggles.get('valid') or right is None:
            row.update(status='FAIL', reason=giggles.get('error') or 'GigglesDB probe failed or case missing')
        elif _canonical(left) != _canonical(right):
            row.update(status='FAIL', reason='driver observations differ', diff=checks.diff(_canonical(left), _canonical(right))[:8000])
        if left is not None:
            row['pg'] = left
        if right is not None:
            row['giggles'] = right
        rows.append(row)
    return rows


def _canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2, allow_nan=False)


def probe_image_tag(driver, probe_dir):
    if not re.fullmatch(r'[a-z0-9_-]+', driver):
        raise ValueError('invalid driver')
    root = Path(probe_dir)
    if not root.is_dir() or root.is_symlink() or bool(getattr(root.lstat(), 'st_file_attributes', 0) & 0x400):
        raise ValueError('probe build context must be a regular directory')
    digest = hashlib.sha256()
    for path in sorted(root.rglob('*'), key=lambda p: p.relative_to(root).as_posix()):
        if path.is_symlink() or bool(getattr(path.lstat(), 'st_file_attributes', 0) & 0x400):
            raise ValueError('probe build context must not contain symlinks')
        if path.is_file():
            name = path.relative_to(root).as_posix().encode('utf-8')
            content = path.read_bytes()
            digest.update(name + b'\0' + str(len(content)).encode('ascii') + b'\0' + content)
    return 'gd-r0-probe-' + driver + ':' + digest.hexdigest()[:12]


def prepare_image(engine, driver, probe_dir, run, expected_version='42.7.3'):
    tag = probe_image_tag(driver, probe_dir)
    metadata = dict(available=False, tag=tag, reason='', image_id=None, digest=None, repo_digests=[], expected_version=expected_version)
    try:
        return _prepare_image(engine, driver, probe_dir, run, metadata)
    except OSError:
        metadata['reason'] = 'probe image build or inspection could not execute'
        return metadata


def _prepare_image(engine, driver, probe_dir, run, metadata):
    tag = metadata['tag']
    inspect = engine.call('image', 'inspect', tag)
    if inspect['exit'] != 0 or inspect['timed_out']:
        build = engine.call('build', '--tag', tag, str(Path(probe_dir).resolve()), timeout=600)
        logs = Path(run) / 'checks' / driver
        logs.mkdir(parents=True, exist_ok=True)
        for stream in ('stdout', 'stderr'):
            (logs / ('build.' + stream + '.txt')).write_text(redact(build[stream], [engine.env.get('PGPASSWORD', '')]), encoding='utf-8')
        if build['exit'] != 0 or build['timed_out']:
            metadata['reason'] = 'probe image build failed' + (' (timeout)' if build['timed_out'] else '')
            return metadata
        inspect = engine.call('image', 'inspect', tag)
    try:
        item = json.loads(inspect['stdout'])[0]
        if inspect['exit'] != 0 or inspect['timed_out'] or not item.get('Id'):
            raise ValueError('image inspection failed')
        metadata.update(available=True, image_id=item['Id'], digest=item['Id'], repo_digests=item.get('RepoDigests') or [])
    except (ValueError, KeyError, TypeError, IndexError):
        metadata['reason'] = 'probe image inspection failed'
    return metadata


def run_probe_side(engine, driver, image, network, name, host, port, manifest, expected_version='42.7.3', timeout=180, r0_prefix=None, version_fields=()):
    if not name.startswith(engine.prefix) or name == engine.prefix:
        raise ValueError('refusing unrelated client name')
    env = dict(engine.env, PGHOST=host, PGPORT=str(port), PGUSER='postgres', PGDATABASE='postgres', R0_PREFIX=r0_prefix or 'r0d_' + driver)
    args = ['docker', 'run', '--rm', '--name', name, '--network', network,
            '--add-host', 'host.docker.internal:host-gateway']
    for key in ('PGHOST', 'PGPORT', 'PGUSER', 'PGDATABASE', 'PGPASSWORD', 'R0_PREFIX'):
        args.extend(['-e', key])
    args.append(image['tag'] if isinstance(image, dict) else image)
    engine.containers.append(name)
    result = command(args, env, timeout=timeout)
    if result['timed_out']:
        engine.remove_client(name)
    result.update(parse_records(result['stdout'], manifest, driver, expected_version, version_fields))
    return redact(result, [env.get('PGPASSWORD', '')])
