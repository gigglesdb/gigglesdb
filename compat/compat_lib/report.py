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
        lines += ['', f'### {owner}', ', '.join(failures)]
        for check in report['checks']:
            if check['client']+'/'+check['id'] in failures:
                lines += ['', f"{check['client']}/{check['id']}", '```diff', '\n'.join(check.get('diff_excerpt', [])), '```']
                if check.get('first_error'):
                    lines += ['', 'First GigglesDB error:', '```text', check['first_error'], '```']
    if report.get('error'):
        lines += ['', 'Harness error: '+report['error']]
    if build.get('build_log_tail'):
        tail = build['build_log_tail']
        lines += ['', 'Build log: '+str(build.get('build_log', 'unavailable')), 'Build log (last 30 lines):', '```text', '\n'.join(tail) if isinstance(tail,list) else str(tail), '```']
    return '\n'.join(lines)+'\n'

def write(run, report, secrets=()):
    clean = redact(report, secrets)
    (run/'report.json').write_text(json.dumps(clean, indent=2)+'\n',encoding='utf-8')
    (run/'report.md').write_text(markdown(clean),encoding='utf-8')
    return clean
