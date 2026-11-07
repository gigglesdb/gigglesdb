import difflib
import re

def _trailing_ws(text):
    lines = [line.rstrip() for line in text.split('\n')]
    while lines and not lines[-1]:
        lines.pop()
    return '\n'.join(lines)

NORMALIZERS = {'crlf': lambda text: text.replace('\r\n','\n'), 'trailing-ws': _trailing_ws}
NORMALIZATION_RULES = list(NORMALIZERS)
OWNERS = {'A','B','C','D','E1','E2','E3','F','G','H','DD-0068','unowned'}
CLIENT_ENV = {'PGTZ', 'PGOPTIONS', 'PGAPPNAME', 'PGCLIENTENCODING', 'PGDATESTYLE'}

def validate_env(env):
    for key, value in env.items():
        if key not in CLIENT_ENV or not isinstance(value, str) or any(c in value for c in '\r\n\0'):
            raise ValueError('invalid client environment header: '+str(key))
    return env

def normalize(text):
    for rule in NORMALIZERS.values():
        text = rule(text)
    return text

def parse_check(path):
    sql = path.read_text(encoding='utf-8')
    headers = {}
    client_env = {}
    for line in sql.splitlines():
        if not line.strip():
            continue
        env_match = re.match(r'^\s*--\s*env:\s?(.*)$', line)
        if env_match:
            key, separator, value = env_match[1].partition('=')
            if not separator or key in client_env:
                raise ValueError(f'{path}: malformed or duplicate env header')
            validate_env({key:value})
            client_env[key] = value
            continue
        match = re.match(r'^\s*--\s*(id|title|owner|status|compare):\s*(.*?)\s*$', line)
        if match:
            if match[1] in headers:
                raise ValueError(f'{path}: duplicate {match[1]} header')
            headers[match[1]] = match[2]
        elif not line.lstrip().startswith('--'):
            break
    if not all(headers.get(k) for k in ('id','title','owner')):
        raise ValueError(f'{path}: id/title/owner headers required')
    if headers['owner'] not in OWNERS or not re.fullmatch(r'[A-Za-z0-9_-]+', headers['id']):
        raise ValueError(f'{path}: invalid owner or id')
    status = headers.get('status', '')
    if status and not re.fullmatch(r'untested\s+\S.*', status, re.I):
        raise ValueError(f'{path}: status must be untested followed by reason')
    if headers.get('compare', 'stdout') not in ('stdout', 'stdout+stderr'):
        raise ValueError(f'{path}: unsupported compare header')
    return dict(headers, sql=sql, env=client_env)

def classify(pg, giggles, available=True, marked=False, compare='stdout'):
    if not available or marked:
        return 'UNTESTED'
    if pg['timed_out'] or pg['exit'] != 0:
        return 'HARNESS_ERROR'
    if giggles['timed_out'] or giggles['exit'] != 0:
        return 'FAIL'
    return 'PASS' if all(normalize(pg.get(s,'')) == normalize(giggles.get(s,'')) for s in streams(compare)) else 'FAIL'

def streams(compare):
    if compare not in ('stdout', 'stdout+stderr'):
        raise ValueError('unsupported comparison: '+compare)
    return compare.split('+')

def output_diff(pg, giggles, compare='stdout'):
    return '\n'.join(f'=== {stream} ===\n'+diff(pg.get(stream,''),giggles.get(stream,'')) for stream in streams(compare))

def diff(pg, giggles):
    return '\n'.join(difflib.unified_diff(normalize(pg).splitlines(), normalize(giggles).splitlines(), fromfile='pg', tofile='giggles', lineterm=''))
