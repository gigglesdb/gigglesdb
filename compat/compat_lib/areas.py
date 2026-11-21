"""Per-area corpus outcome counts, and merging reports of one commit run in parts."""
import copy
import json
import sys
from . import corpus, delta, util

COLUMNS = ('COMPATIBLE', 'UNSUPPORTED', 'DIVERGENT', 'INTENTIONAL', 'UNTESTED', 'HARNESS_ERROR')


def merge(reports):
    """One report from several runs of the same commit over disjoint case sets."""
    if not reports:
        raise ValueError('no reports to merge')
    if len(reports) == 1:
        return reports[0]
    commits = {(report.get('build') or {}).get('commit') for report in reports}
    if len(commits) != 1 or None in commits:
        raise ValueError('merged reports must share one build.commit')
    merged = copy.deepcopy(reports[0])
    merged['run_id'] = '+'.join(str(report.get('run_id')) for report in reports)
    merged['merged_from'] = [report.get('run_id') for report in reports]
    starts = [report['started_at'] for report in reports if report.get('started_at')]
    finishes = [report['finished_at'] for report in reports if report.get('finished_at')]
    merged['started_at'] = min(starts) if starts else None
    merged['finished_at'] = max(finishes) if len(finishes) == len(reports) else None
    merged['checks'] = [copy.deepcopy(row) for report in reports for row in report.get('checks', [])]
    cases = [copy.deepcopy(row) for report in reports for row in (report.get('differential') or {}).get('cases', [])]
    merged.setdefault('differential', {})['cases'] = cases
    merged['differential']['summary'] = corpus.summarize(cases)
    delta.records(merged)  # rejects a case present in two parts
    return merged


def counts(report):
    """Return {area: {outcome: n}} counted from the report's corpus cases."""
    result = {}
    for key, row in delta.records(report).items():
        if not key.startswith('corpus:'):
            continue
        area = result.setdefault(row['area'], dict.fromkeys(COLUMNS, 0))
        area[row['outcome']] += 1
    return result


def table(new, old=None):
    """Markdown table: one row per area plus a total, with COMPATIBLE change vs old."""
    after, before = counts(new), counts(old) if old is not None else {}
    header = ['area', *COLUMNS, 'total']
    if old is not None:
        header += ['COMPATIBLE before', 'change']
    lines = ['| ' + ' | '.join(header) + ' |', '|' + '|'.join(['---'] + ['---:'] * (len(header) - 1)) + '|']
    total_new, total_old = dict.fromkeys(COLUMNS, 0), dict.fromkeys(COLUMNS, 0)
    for area in sorted(after.keys() | before.keys()):
        row = after.get(area, dict.fromkeys(COLUMNS, 0))
        cells = [area, *(str(row[c]) for c in COLUMNS), str(sum(row.values()))]
        for c in COLUMNS:
            total_new[c] += row[c]
        if old is not None:
            was = before.get(area, dict.fromkeys(COLUMNS, 0))
            for c in COLUMNS:
                total_old[c] += was[c]
            cells += [str(was['COMPATIBLE']), f"{row['COMPATIBLE'] - was['COMPATIBLE']:+d}"]
        lines.append('| ' + ' | '.join(cells) + ' |')
    cells = ['**total**', *(str(total_new[c]) for c in COLUMNS), str(sum(total_new.values()))]
    if old is not None:
        cells += [str(total_old['COMPATIBLE']), f"{total_new['COMPATIBLE'] - total_old['COMPATIBLE']:+d}"]
    lines.append('| ' + ' | '.join(cells) + ' |')
    return '\n'.join(lines) + '\n'


def _write(out, text):
    output = util.output_root(out)
    util.output_root(output.parent)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text, encoding='utf-8')


def run(new, previous=None, out=None):
    """Print (and optionally write) the area table of the merged new report(s)."""
    try:
        merged_new = merge([delta.load(path) for path in new])
        merged_old = merge([delta.load(path) for path in previous]) if previous else None
        markdown = table(merged_new, merged_old)
        if out is not None:
            _write(out, markdown)
        print(markdown, end='')
        return 0
    except (OSError, ValueError, TypeError, AttributeError, KeyError) as exc:
        print('areas: ' + str(exc), file=sys.stderr)
        return 2


def run_merge(reports, out):
    """Write one report merged from parts of the same commit's run."""
    try:
        merged = merge([delta.load(path) for path in reports])
        _write(out, json.dumps(merged, indent=2) + '\n')
        print(out)
        return 0
    except (OSError, ValueError, TypeError, AttributeError, KeyError) as exc:
        print('merge: ' + str(exc), file=sys.stderr)
        return 2
