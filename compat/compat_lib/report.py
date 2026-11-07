import json
from .util import redact

OUTCOMES = ('PASS','FAIL','UNTESTED','HARNESS_ERROR')

def summarize(checks, clients):
    totals = {name: dict.fromkeys(OUTCOMES, 0) for name in clients}
    owners = {}
    for check in checks:
        totals[check['client']][check['outcome']] += 1
        if check['outcome'] in ('FAIL','HARNESS_ERROR'):
            owners.setdefault(check['owner'], []).append(check['client']+'/'+check['id'])
    for counts in totals.values():
        denominator = counts['PASS']+counts['FAIL']
        counts['giggles_pass_rate'] = counts['PASS']/denominator if denominator else None
    aggregate = {outcome:sum(counts[outcome] for counts in totals.values()) for outcome in OUTCOMES}
    denominator = aggregate['PASS']+aggregate['FAIL']
    return dict(clients=totals, totals=aggregate, giggles_pass_rate=aggregate['PASS']/denominator if denominator else None), owners

def client_table(summary):
    lines = ['| Client | PASS | FAIL | UNTESTED | HARNESS_ERROR | Pass rate |','| --- | ---: | ---: | ---: | ---: | ---: |']
    for name, counts in summary.items():
        rate = counts['giggles_pass_rate']
        lines.append('| '+ ' | '.join([name, *(str(counts[k]) for k in OUTCOMES), f'{rate:.1%}' if rate is not None else 'N/A'])+' |')
    return '\n'.join(lines)

DIFFERENTIAL_OUTCOMES = ('COMPATIBLE','UNSUPPORTED','DIVERGENT','INTENTIONAL','UNTESTED','HARNESS_ERROR')


def differential_table(summary):
    lines = ['| Area | Cases | COMPATIBLE | UNSUPPORTED | DIVERGENT | INTENTIONAL | UNTESTED | HARNESS_ERROR | Pass rate |',
             '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    groups = [('Overall', summary.get('overall', {})), *sorted(summary.get('by_area', {}).items())]
    for area, counts in groups:
        rate = counts.get('giggles_pass_rate', counts.get('pass_rate'))
        values = [str(counts.get(key, 0)) for key in DIFFERENTIAL_OUTCOMES]
        lines.append('| '+' | '.join([area, str(sum(counts.get(key,0) for key in DIFFERENTIAL_OUTCOMES)), *values,
                                      f'{rate:.1%}' if rate is not None else 'N/A'])+' |')
    return '\n'.join(lines)


def differential_markdown(differential):
    lines = ['', '## Differential SQL', '', differential_table(differential.get('summary', {})),
             '', '## Differential failures by owning track']
    cases = differential.get('cases', [])
    lines += ['', '| Case | Owner | Comparison | Outcome |', '| --- | --- | --- | --- |']
    for case in cases:
        lines.append('| ' + ' | '.join(str(value).replace('|', '\\|') for value in (case['id'], case['owner'], 'compare: '+case.get('compare', 'full'), case['outcome'])) + ' |')
    owners = sorted({case['owner'] for case in cases if case['outcome'] in ('UNSUPPORTED','DIVERGENT','INTENTIONAL','HARNESS_ERROR')})
    for owner in owners:
        lines += ['', '### '+('Unowned (needs an owner)' if owner == 'unowned' else owner)]
        for case in cases:
            if case['owner'] != owner or case['outcome'] not in ('UNSUPPORTED','DIVERGENT','INTENTIONAL','HARNESS_ERROR'):
                continue
            diff = case.get('first_divergence') or next(iter(case.get('harness_errors') or []), {})
            sql = ' '.join(diff.get('sql','').split())[:160]
            pg = diff.get('pg') or {}
            giggles = diff.get('giggles') or {}
            lines += ['', f"- {case['id']}: {case['outcome']}; SQL: `{sql.replace('`', '')}`; PostgreSQL {pg.get('sqlstate','N/A')} vs GigglesDB {giggles.get('sqlstate','N/A')}."]
            if diff.get('reason'):
                lines += ['  Harness reason: '+diff['reason']]
            error = giggles.get('stderr_first_line')
            if error:
                lines += ['  First GigglesDB error: '+error]
    warnings = [(case, warning) for case in cases for warning in case.get('warnings', [])]
    warnings += [(case, 'stale_expectation: intentional case now matches PostgreSQL') for case in cases if case.get('stale_expectation')]
    if warnings:
        lines += ['', '### Differential warnings']
        for case, warning in warnings:
            lines.append(f"- {case['id']}: {warning}")
    return lines


def markdown(report):
    lines = ['# GigglesDB compatibility report', '', f"Run: {report['run_id']}", f"Started: {report['started_at']}", f"Finished: {report['finished_at']}", '', 'Identity:', '```json', json.dumps({k:report.get(k) for k in ('harness','build','oracle','clients','networking','normalization_rules')}, indent=2), '```', '', client_table(report['summary']['clients']), '', '| Client | Check | Owner | Outcome |','| --- | --- | --- | --- |']
    build = report.get('build', {})
    oracle = report.get('oracle', {})
    identity = [
        f"Commit: {build.get('commit') or 'unknown'} ({build.get('build_identity', 'unknown')})",
        f"Server binary SHA-256: {build.get('binary_sha256', 'unavailable')}",
        f"Oracle: PostgreSQL {oracle.get('server_version', 'unavailable')}; digest: {oracle.get('digest', 'unavailable')}; collation: {oracle.get('collation', 'unavailable')}; TZ: {oracle.get('timezone', 'unavailable')}",
        'Clients: '+ '; '.join(f"{name} -> {meta.get('version') or 'unavailable'}; digest: {meta.get('digest', 'unavailable')}" for name,meta in report.get('clients',{}).items()),
        '',
    ]
    lines[lines.index('Identity:'):lines.index('Identity:')] = identity
    for item in report['checks']:
        lines.append('| '+' | '.join(str(item[k]).replace('|','\\|') for k in ('client','id','owner','outcome'))+' |')
    lines += ['', '## Failures by owning track']
    for owner, failures in report['failures_by_owner'].items():
        lines += ['', '### '+('Unowned (needs an owner)' if owner == 'unowned' else owner), ', '.join(failures)]
        for check in report['checks']:
            if check['client']+'/'+check['id'] in failures:
                lines += ['', f"{check['client']}/{check['id']}", '```diff', '\n'.join(check.get('diff_excerpt', [])), '```']
                if check.get('first_error'):
                    lines += ['', 'First GigglesDB error:', '```text', check['first_error'], '```']
    if report.get('differential'):
        lines += differential_markdown(report['differential'])
    if report.get('error'):
        lines += ['', 'Harness error: '+report['error']]
    if 'scratch_removed' in report:
        lines += ['', 'Scratch removed: '+(', '.join(report['scratch_removed']) or 'none'),
                  'Scratch bytes freed: '+str(report['scratch_bytes_freed'])]
        if report.get('scratch_cleanup_error'):
            lines += ['Scratch cleanup error: '+report['scratch_cleanup_error']]
    if build.get('build_log_tail'):
        tail = build['build_log_tail']
        lines += ['', 'Build log: '+str(build.get('build_log', 'unavailable')), 'Build log (last 30 lines):', '```text', '\n'.join(tail) if isinstance(tail,list) else str(tail), '```']
    return '\n'.join(lines)+'\n'

def write(run, report, secrets=()):
    clean = redact(report, secrets)
    (run/'report.json').write_text(json.dumps(clean, indent=2)+'\n',encoding='utf-8')
    (run/'report.md').write_text(markdown(clean),encoding='utf-8')
    return clean
