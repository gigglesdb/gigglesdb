#!/usr/bin/env python3
"""Compare two harness report.json files record by record.

Client checks are keyed by (client, check id) and differential corpus cases by
(area, case id). Prints a Markdown summary and a table of every record whose
outcome differs or that exists on only one side. Exit 0 when identical, 1 when
any difference exists, 2 for invalid input.

    python -I -B compat/tools/compare_reports.py A/report.json B/report.json \
        --label-a single --label-b cluster-leader [--out comparison.md]
"""
import sys
sys.dont_write_bytecode = True
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import argparse
from collections import Counter
from compat_lib import delta


def identity(report):
    build = report.get('build', {})
    return dict(run_id=report.get('run_id'), commit=build.get('commit'),
                binary_sha256=build.get('binary_sha256'), server_mode=build.get('server_mode', 'single-node-dev (unlabelled)'),
                harness_commit=report.get('harness', {}).get('commit'), harness_dirty=report.get('harness', {}).get('dirty'),
                started_at=report.get('started_at'), finished_at=report.get('finished_at'))


def totals(rows):
    checks = Counter(row['outcome'] for key, row in rows.items() if key.startswith('check:'))
    corpus = Counter(row['outcome'] for key, row in rows.items() if key.startswith('corpus:'))
    return checks, corpus


def detail(row):
    if row is None:
        return 'absent'
    error = delta.first_error(row)
    return row['outcome'] + (f' ({error})' if error else '')


def compare(a, b):
    """Sorted list of (key, row_a, row_b) whose outcome or presence differs."""
    rows_a, rows_b = delta.records(a), delta.records(b)
    keys = sorted(set(rows_a) | set(rows_b))
    return rows_a, rows_b, [(key, rows_a.get(key), rows_b.get(key)) for key in keys
                            if (rows_a.get(key) or {}).get('outcome') != (rows_b.get(key) or {}).get('outcome')]


def markdown(a, b, label_a, label_b):
    rows_a, rows_b, differences = compare(a, b)
    lines = [f'| Side | Run | Commit label | Server mode | Binary SHA-256 | Harness commit (dirty) |',
             '| --- | --- | --- | --- | --- | --- |']
    for label, report in ((label_a, a), (label_b, b)):
        i = identity(report)
        lines.append(f"| {label} | {i['run_id']} | {i['commit'] or 'unlabelled'} | {i['server_mode']} | "
                     f"{(i['binary_sha256'] or '')[:16]} | {(i['harness_commit'] or '')[:8]} ({i['harness_dirty']}) |")
    lines += ['', '| Side | Client checks | Corpus cases |', '| --- | --- | --- |']
    for label, rows in ((label_a, rows_a), (label_b, rows_b)):
        checks, corpus = totals(rows)
        fmt = lambda counter: ', '.join(f'{k} {v}' for k, v in sorted(counter.items())) or 'none'
        lines.append(f'| {label} | {fmt(checks)} | {fmt(corpus)} |')
    lines.append('')
    if not differences:
        lines.append(f'Identical: all {len(rows_a)} records have the same outcome on both sides.')
    else:
        lines += [f'{len(differences)} of {len(set(rows_a) | set(rows_b))} records differ.', '',
                  f'| Kind | Scope | ID | {label_a} | {label_b} |', '| --- | --- | --- | --- | --- |']
        for key, row_a, row_b in differences:
            kind, rest = key.split(':', 1)
            scope, ident = rest.split('/', 1)
            lines.append(f'| {kind} | {scope} | {ident} | {detail(row_a)} | {detail(row_b)} |')
    return '\n'.join(lines) + '\n', differences


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('a', type=Path)
    parser.add_argument('b', type=Path)
    parser.add_argument('--label-a', default='A')
    parser.add_argument('--label-b', default='B')
    parser.add_argument('--out', type=Path)
    args = parser.parse_args(argv)
    try:
        a, b = delta.load(args.a), delta.load(args.b)
    except (OSError, ValueError) as exc:
        print(f'invalid report: {exc}', file=sys.stderr)
        return 2
    text, differences = markdown(a, b, args.label_a, args.label_b)
    print(text, end='')
    if args.out:
        args.out.write_text(text, encoding='utf-8', newline='\n')
    return 1 if differences else 0


if __name__ == '__main__':
    raise SystemExit(main())
