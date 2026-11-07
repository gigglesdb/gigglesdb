"""Deterministic, offline comparisons of compatibility report JSON files."""
import json
import re
import sys
from pathlib import Path
from . import util


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key: '+key)
        result[key] = value
    return result


def load(path):
    value = json.loads(Path(path).read_text(encoding='utf-8-sig'), object_pairs_hook=_unique)
    records(value)
    return value


def records(report):
    if not isinstance(report, dict):
        raise ValueError('report must be an object')
    differential = report.get('differential', {})
    if not isinstance(differential, dict):
        raise ValueError('differential must be an object')
    result = {}
    for kind, rows, scope, outcomes in (
            ('check', report.get('checks', []), 'client', {'PASS','FAIL','UNTESTED','HARNESS_ERROR'}),
            ('corpus', differential.get('cases', []), 'area', {'COMPATIBLE','UNSUPPORTED','DIVERGENT','INTENTIONAL','UNTESTED','HARNESS_ERROR'})):
        if not isinstance(rows, list):
            raise ValueError(kind+' records must be an array')
        for row in rows:
            if not isinstance(row, dict) or any(not isinstance(row.get(k), str) or not row[k] for k in (scope, 'id', 'outcome')):
                raise ValueError('invalid '+kind+' record')
            if row['outcome'] not in outcomes:
                raise ValueError('invalid '+kind+' outcome: '+row['outcome'])
            key = kind+':'+row[scope]+'/'+row['id']
            if key in result:
                raise ValueError('duplicate stable key: '+key)
            result[key] = row
    return result


def _state(value):
    if isinstance(value, dict):
        state = value.get('sqlstate')
        if isinstance(state, str) and state not in ('00000', ''):
            return state
        for nested in value.values():
            found = _state(nested)
            if found:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _state(nested)
            if found:
                return found
    elif isinstance(value, str):
        match = re.search(r'\b(?:SQLSTATE[\s:=\[]+|(?:ERROR|FATAL):\s+)([0-9A-Z]{5})\b', value)
        if match:
            return match[1]
    return None


def first_error(row):
    first = row.get('first_error')
    if isinstance(first, str) and re.fullmatch(r'[0-9A-Z]{5}', first) and first != '00000':
        return first
    divergence = row.get('first_divergence') or {}
    for candidate in (first, divergence.get('giggles')):
        found = _state(candidate)
        if found:
            return found
    found = _actual_error(row.get('giggles'))
    if found:
        return found
    if row.get('outcome') in ('PASS', 'COMPATIBLE'):
        return 'N/A'
    for candidate in (first, (divergence.get('giggles') or {}).get('stderr_first_line'), divergence.get('reason'), row.get('reason')):
        if candidate:
            return str(candidate)
    return 'N/A'


def _actual_error(value):
    """Visit recorded steps in execution order, ignoring expected OK data."""
    if isinstance(value, list):
        return next((error for item in value if (error := _actual_error(item))), None)
    if not isinstance(value, dict):
        return None
    if value.get('status') == 'ERR':
        # Aggregate ERR nodes may wrap the actual failed step.
        for key, nested in value.items():
            if key not in ('sqlstate', 'reason'):
                found = _actual_error(nested)
                if found:
                    return found
        observation = value.get('observation') or {}
        state = value.get('sqlstate') or (observation.get('sqlstate') if isinstance(observation, dict) else None)
        state = state or _state(value.get('errors')) or (_state(observation.get('errors')) if isinstance(observation, dict) else None)
        if state and state != '00000':
            return str(state)
        reason = value.get('reason') or (observation.get('reason') if isinstance(observation, dict) else None)
        if reason:
            return str(reason)
    for nested in value.values():
        found = _actual_error(nested)
        if found:
            return found
    return None


def _text(value):
    return str(value if value is not None else 'N/A').replace('|', '\\|').replace('\r', ' ').replace('\n', ' ').replace('`', "'")


def _rate(rows, positive, negative):
    good = sum(row['outcome'] == positive for row in rows)
    denominator = sum(row['outcome'] in {positive, *negative} for row in rows)
    return good / denominator if denominator else None


def _rate_delta(old, new):
    before = 'N/A' if old is None else f'{old:.1%}'
    after = 'N/A' if new is None else f'{new:.1%}'
    change = 'N/A' if old is None or new is None else f'{(new-old)*100:+.1f} pp'
    return f'{before} -> {after} ({change})'


def compare(old, new):
    before, after = records(old), records(new)
    groups = {name: [] for name in ('Regressions','Fixed','Other changes','Added','Removed')}
    for key in sorted(before.keys() | after.keys()):
        a, b = before.get(key), after.get(key)
        if a is None or b is None:
            row = b if a is None else a
            groups['Added' if a is None else 'Removed'].append(f'- `{_text(key)}`: {row["outcome"]}; owner {_text(row.get("owner"))}; first GigglesDB error: {_text(first_error(row))}')
            continue
        x, y = a['outcome'], b['outcome']
        regression = (x == 'PASS' and y == 'FAIL') or (x == 'COMPATIBLE' and y in ('UNSUPPORTED','DIVERGENT')) or (x in ('PASS','COMPATIBLE') and y == 'HARNESS_ERROR')
        fixed = (x == 'FAIL' and y == 'PASS') or (x in ('UNSUPPORTED','DIVERGENT') and y == 'COMPATIBLE')
        owner_changed = a.get('owner') != b.get('owner')
        error_changed = first_error(a) != first_error(b)
        if x == y and not owner_changed and not error_changed:
            continue
        details = f'- `{_text(key)}`: {x} -> {y}; owner {_text(b.get("owner"))}; first GigglesDB error: {_text(first_error(b))}'
        if owner_changed:
            details += f'; owner changed {_text(a.get("owner"))} -> {_text(b.get("owner"))}'
        if error_changed:
            details += f'; first error changed {_text(first_error(a))} -> {_text(first_error(b))}'
        groups['Regressions' if regression else 'Fixed' if fixed else 'Other changes'].append(details)
    lines = ['# GigglesDB compatibility delta', '']
    for label, report in (('Old', old), ('New', new)):
        build = report.get('build') or {}
        lines.append(f'{label}: commit {_text(build.get("commit"))}; run {_text(report.get("run_id"))}; started {_text(report.get("started_at"))}; finished {_text(report.get("finished_at"))}')
    for label, entries in groups.items():
        lines += ['', '## '+label, '', *(entries or ['None.'])]
    lines += ['', '## Summary', '']
    clients = set((old.get('clients') or {}).keys()) | set((new.get('clients') or {}).keys())
    clients |= {row['client'] for row in [*before.values(), *after.values()] if 'client' in row}
    summary = []
    for client in sorted(clients):
        a = [row for key, row in before.items() if key.startswith('check:') and row['client'] == client]
        b = [row for key, row in after.items() if key.startswith('check:') and row['client'] == client]
        x, y = sum(r['outcome'] == 'PASS' for r in a), sum(r['outcome'] == 'PASS' for r in b)
        lines.append(f'- {_text(client)}: PASS {x} -> {y} ({y-x:+d}); pass rate {_rate_delta(_rate(a,"PASS",("FAIL",)), _rate(b,"PASS",("FAIL",)))}')
        summary.append(f'{_text(client)} PASS {x} -> {y} ({y-x:+d})')
    a = [r for k,r in before.items() if k.startswith('corpus:')]
    b = [r for k,r in after.items() if k.startswith('corpus:')]
    lines.append('- Differential pass rate: '+_rate_delta(_rate(a,'COMPATIBLE',('UNSUPPORTED','DIVERGENT')), _rate(b,'COMPATIBLE',('UNSUPPORTED','DIVERGENT'))))
    summary.append('Differential pass rate '+_rate_delta(_rate(a,'COMPATIBLE',('UNSUPPORTED','DIVERGENT')), _rate(b,'COMPATIBLE',('UNSUPPORTED','DIVERGENT'))))
    lines += ['', 'Summary: '+'; '.join(summary)]
    return '\n'.join(lines)+'\n', bool(groups['Regressions'])


def run(old, new, out=None):
    try:
        output = util.output_root(out) if out is not None else None
        markdown, regression = compare(load(old), load(new))
        if output is not None:
            # Resolve and validate the parent before creating any directories.
            util.output_root(output.parent)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(markdown, encoding='utf-8')
        print(markdown, end='')
        return 3 if regression else 0
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        print('delta: '+str(exc), file=sys.stderr)
        return 2
