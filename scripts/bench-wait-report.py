#!/usr/bin/env python3
"""Summarize best-effort benchmark wait captures; Python 3.10+, stdlib only."""
import argparse
from collections import Counter, defaultdict
from pathlib import Path
import re

MISSING = 'not captured (see manifest status)'
GENERIC = re.compile(r'std::|core::|alloc::|tokio::runtime::|parking_lot_core::|__GI_|syscall|start_thread|clone3|\[unknown\]')
WAIT = [
    ('futex', r'futex_'), ('epoll', r'do_epoll_wait|ep_poll'),
    ('io', r'io_schedule|blk|folio_wait|wait_on_page'),
    ('fsync', r'jbd2|ext4_sync|xfs_.*sync|file_fsync|vfs_fsync'),
    ('sleep', r'do_nanosleep|hrtimer_nanosleep'),
    ('net', r'pipe_read|unix_stream|tcp_recvmsg|sk_wait_data|inet_'),
    ('kernel-lock', r'schedule_timeout|rwsem|mutex_lock'),
]


def read(path):
    try:
        return path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return ''


def normalize(comm):
    return re.sub(r'-?\d+$', '', comm).rstrip('-') or comm


def manifest(text):
    durations, statuses, threads = {}, {}, Counter()
    in_ps = False
    for line in text.splitlines():
        fields = dict(re.findall(r'(\w+)=([^\s]+)', line))
        if 'capture' in fields:
            statuses[fields['capture']] = fields.get('status', 'unknown')
            try:
                durations[fields['capture']] = float(fields['duration_seconds'])
            except (KeyError, ValueError):
                pass
        if re.fullmatch(r'\s*TID\s+COMMAND\s*', line):
            in_ps = True
        elif in_ps:
            match = re.fullmatch(r'\s*\d+\s+(.+?)\s*', line)
            if match:
                threads[normalize(match[1])] += 1
            else:
                in_ps = False
    return durations, statuses, threads


def wait_reason(frames):
    # Folded stacks are root first: ignore scheduler plumbing at the leaf.
    for frame in reversed(frames):
        for kind, pattern in WAIT:
            if re.search(pattern, frame):
                return kind
    return 'other'


def split_stack(frames):
    if '-' not in frames:
        return frames, []  # Missing delimiter: retain symbols, avoid inventing a boundary.
    cut = frames.index('-')
    left, right = frames[:cut], frames[cut + 1:]
    # bcc tools/offcputime.py folded output emits reversed(user_stack), then
    # delimiter, then reversed(kernel_stack): both root first. Accept swapped parts.
    kernel_hint = r'finish_task_switch|^schedule|entry_SYSCALL|__.*sys_|^do_syscall'
    def score(part):
        return sum(bool(re.search(kernel_hint, f)) or any(re.search(p, f) for _, p in WAIT)
                   for f in part if '::' not in f)
    return (right, left) if score(left) > score(right) else (left, right)


def short_user(frames):
    return list(reversed([f for f in frames if f != '-' and not GENERIC.search(f)]))[:3]


def idle_stack(user, comm, kind):
    joined = ';'.join(user)
    # [^;]* keeps each match inside one frame: every worker stack has
    # tokio::runtime::scheduler at its root, and an application lock wait
    # (parking_lot_core::parking_lot::park) must stay a working wait.
    if re.search(r'tokio::runtime::(?:park::|scheduler::[^;]*::park)', joined):
        return True
    # Idle blocking-pool thread waiting for a task, with no application frame.
    if re.search(r'tokio::runtime::blocking::pool::Inner::run', joined) and not short_user(user):
        return True
    if kind == 'epoll' and re.search(r'tokio::(?:runtime::io::|io::).*', joined):
        return True
    park = re.search(r'(?:crossbeam[^;]*|std::[^;]*)(?:::park|::Parker::park)', joined)
    # Require an identified pool and no application frame for generic park.
    pool = re.search(r'tokio|pool|worker|rayon', comm, re.I)
    meaningful = [f for f in short_user(user) if not re.search(r'crossbeam|rayon', f)]
    return bool(park and pool and not meaningful)


def folded(text):
    for line in text.splitlines():
        match = re.fullmatch(r'(.+)\s+(\d+)', line.strip())
        if match and ';' in match[1]:
            yield match[1].split(';'), int(match[2])


def offcpu_rows(text, full=False):
    rows = Counter()
    for frames, us in folded(text):
        comm = normalize(frames[0])
        user, kernel = split_stack(frames[1:])
        kind = wait_reason(kernel)
        stack = ';'.join(frames[1:]) if full else ' <- '.join([kind] + short_user(user))
        rows[(comm, kind, stack, idle_stack(user, comm, kind))] += us
    return rows


def futex_rows(text, full=False):
    maps = defaultdict(lambda: [0, 0])
    for match in re.finditer(r'@(us|count)\[([^,\n]+),\s*\n(.*?)\]\s*:\s*(\d+)', text, re.S):
        metric, comm, body, value = match.groups()
        frames = []
        for line in body.splitlines():
            frame = re.match(r'\s*(?:0x)?[0-9a-fA-F]+\s+(.+?)(?:\s+\(.*\))?\s*$', line)
            if frame:
                frames.append(frame[1])
        # perf ustack input is leaf first; canonical user stacks are root first.
        maps[(normalize(comm.strip()), tuple(reversed(frames)))][metric == 'count'] += int(value)
    rows = defaultdict(lambda: [0, 0])
    for (comm, frames), (us, count) in maps.items():
        stack = ';'.join(frames) if full else ' <- '.join(short_user(frames)) or '[unknown]'
        row = rows[(comm, stack)]
        row[0] += us
        row[1] += count
    return rows


def sample_means(text, device=False):
    samples = defaultdict(lambda: defaultdict(list))
    header = None
    for line in text.splitlines():
        if line.startswith('Average:'):
            continue  # Do not count the final summary as another interval.
        fields = line.replace('|__', '').split()
        fields = [f.rstrip(':') for f in fields]
        if (device and 'Device' in fields) or (not device and 'Command' in fields):
            start = fields.index('Device') if device else next(
                (i for i, f in enumerate(fields) if f in ('UID', 'TGID', 'TID')), 0)
            header = fields[start:]
            continue
        if not header or len(fields) < len(header):
            continue
        # Timestamp (and optional AM/PM) precedes the header's data columns.
        values = fields[-len(header):]
        row = dict(zip(header, values))
        name = row.get('Device') if device else row.get('TID')
        if not device and (not name or not name.isdigit()):
            continue
        if not name:
            continue
        for metric in ('%util', 'w_await') if device else ('%CPU', 'cswch/s', 'nvcswch/s'):
            try:
                samples[(name, row.get('Command', ''))][metric].append(float(row[metric]))
            except (KeyError, ValueError):
                pass
    return {key: {m: sum(v) / len(v) for m, v in metrics.items()} for key, metrics in samples.items() if metrics}


def cell(value):
    return str(value).replace('|', '\\|').replace('\n', ' ')


def table(headers, rows):
    if not rows:
        return MISSING + '\n'
    return '\n'.join(['| ' + ' | '.join(headers) + ' |',
                      '| ' + ' | '.join('---' for _ in headers) + ' |'] +
                     ['| ' + ' | '.join(cell(x) for x in row) + ' |' for row in rows]) + '\n'


def report(directory, key, top=15, full=False):
    def content(prefix, suffix='txt'):
        return read(directory / f'{prefix}-{key}.{suffix}')
    durations, statuses, threads = manifest(content('manifest'))
    def wall(us, comm, capture):
        duration = durations.get(capture, 30)
        if duration <= 0 or not threads[comm]:
            return 'n/a'
        return f'{100 * us / (duration * 1e6 * threads[comm]):.2f}%'
    lines = [f'## {key}', '', 'Captures: ' + (', '.join(
        f'{name} {seconds:g}s ({statuses.get(name, "unknown")})' for name, seconds in durations.items()) or MISSING),
        f'Server threads: {sum(threads.values())}; ' + (', '.join(f'{n} ×{c}' for n, c in sorted(threads.items())) or MISSING), '',
        '### Off-CPU working waits', '']
    off = offcpu_rows(content('offcpu', 'folded'), full)
    total = sum(off.values())
    idle = sum(us for row, us in off.items() if row[3])
    lines += [f'All off-CPU: {total / 1000:.3f} ms; working waits: {(total-idle) / 1000:.3f} ms; idle: {idle / 1000:.3f} ms.', '']
    working = [(row, us) for row, us in off.most_common() if not row[3]][:top]
    lines += [table(['Rank', 'Thread', 'Wait class', 'Stack', 'Total wait ms', '% all off-CPU', '% of wall'],
                    [[i, row[0], row[1], row[2], f'{us/1000:.3f}', f'{100*us/total:.2f}%', wall(us, row[0], 'offcpu')]
                     for i, (row, us) in enumerate(working, 1)])]
    if off and not working:
        lines += ['No working waits; all captured waits classified as idle.', '']
    lines += ['Idle summary: ' + (', '.join(f'{c}: {v/1000:.3f} ms' for c, v in Counter(
        {c: sum(us for r, us in off.items() if r[3] and r[0] == c) for c in {r[0] for r in off if r[3]}}).most_common()) or ('none' if off else MISSING)), '',
        '### Futex / lock waits', '']
    locks = sorted(futex_rows(content('futex'), full).items(), key=lambda item: item[1][0], reverse=True)[:top]
    lines += [table(['Rank', 'Thread', 'Stack', 'Total wait ms', 'Count', 'Avg µs', '% of wall'],
                    [[i, comm, stack, f'{us/1000:.3f}', count, f'{us/count:.2f}' if count else 'n/a', wall(us, comm, 'futex')]
                     for i, ((comm, stack), (us, count)) in enumerate(locks, 1)]),
        '*% of wall = wait_us / (capture_seconds × 1e6 × threads_with_that_comm) × 100; thread names have trailing digits/dashes removed. Durations come from the manifest (fallback 30s); absent thread counts or zero durations give n/a. Concurrent thread waits are summed, not elapsed latency.*', '',
        '### Wake-ups', '']
    wake = Counter()
    for frames, us in folded(content('wakeup', 'folded')):
        stack = ';'.join(frames) if full else ' <- '.join(reversed(frames[-4:]))
        wake[stack] += us
    lines += [table(['Waker stack (leaf first)', 'Total us'], wake.most_common(top)), '### Storage and threads', '']
    for title, prefix in [('Fsync histogram', 'fsync'), ('Syscount (first 10 rows)', 'syscount')]:
        text = content(prefix).strip()
        if prefix == 'syscount' and text:
            raw = text.splitlines()
            # syscount -L prints SYSCALL COUNT LATENCY(ms), preceded by a banner.
            start = next((i for i, line in enumerate(raw) if 'SYSCALL' in line and 'COUNT' in line), None)
            text = '\n'.join(raw[start:start+11]) if start is not None else '\n'.join(raw[:10])
        lines += [title, '', '```text\n' + text + '\n```' if text else MISSING, '']
    devices = sample_means(content('iostat'), True)
    lines += ['Iostat (mean over reported samples, including the initial since-boot sample):', '',
              table(['Device', 'Mean %util', 'Mean w_await ms'],
                    [[name, f'{v["%util"]:.2f}' if '%util' in v else 'n/a', f'{v["w_await"]:.2f}' if 'w_await' in v else 'n/a']
                     for (name, _), v in sorted(devices.items())])]
    cpu = sample_means(content('pidstat'))
    for title, metrics in [('CPU', ('%CPU',)), ('context switches', ('cswch/s', 'nvcswch/s'))]:
        ranked = sorted(((k, v) for k, v in cpu.items() if any(m in v for m in metrics)),
                        key=lambda item: sum(item[1].get(m, 0) for m in metrics), reverse=True)[:10]
        lines += [f'Pidstat top threads by mean {title}:', '', table(['TID', 'Thread', 'Mean ' + ' + '.join(metrics)],
                 [[tid, name, f'{sum(v.get(m, 0) for m in metrics):.2f}'] for (tid, name), v in ranked])]
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('profiles', type=Path)
    parser.add_argument('--top', type=int, default=15)
    parser.add_argument('--key')
    parser.add_argument('--out', type=Path)
    parser.add_argument('--full', action='store_true', help='retain full stacks in wait tables')
    args = parser.parse_args()
    if args.top < 1:
        parser.error('--top must be positive')
    keys = set()
    if args.key:
        keys.add(args.key)
    elif args.profiles.is_dir():
        for path in args.profiles.iterdir():
            match = re.match(r'(?:manifest|offcpu|futex|wakeup|pidstat|iostat|biolatency|fsync|syscount|folded|top|lock|cpu)-(.+)\.(?:txt|folded)$', path.name)
            if match:
                keys.add(match[1])
    output = '# Benchmark wait report\n\n' + ('\n'.join(report(args.profiles, key, args.top, args.full) for key in sorted(keys)) if keys else MISSING + '\n')
    if args.out:
        args.out.write_text(output, encoding='utf-8', newline='\n')
    else:
        import sys
        sys.stdout.reconfigure(encoding='utf-8')
        print(output)


if __name__ == '__main__':
    main()
