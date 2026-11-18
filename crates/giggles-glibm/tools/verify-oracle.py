"""Verify direct libm against PostgreSQL's shortest-round-trip output rows.

PostgreSQL ln maps to libm log; PostgreSQL log maps to libm log10.
PostgreSQL converts negative zero and NaN representations, so compare text
results numerically (zero signs and NaN payloads cannot be recovered from PG).
"""
import collections
import math
import pathlib
import struct
import subprocess
import sys

mapping = dict((x, x) for x in ('cbrt exp sin cos tan asin acos atan sinh cosh tanh asinh acosh atanh erf erfc lgamma atan2').split())
mapping.update(ln='log', log='log10', gamma='tgamma', power='pow')
bits = lambda x: struct.unpack('<Q', struct.pack('<d', x))[0]
rows = []
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    fn, args, expected = line.split('|')
    if fn not in mapping or expected.startswith('ERROR'):
        continue
    values = [float(x) for x in args.split()]
    rows.append((mapping[fn], values, float(expected), line))
query = ''.join(f'{fn} {bits(a[0]):016x} {bits(a[1]) if len(a)>1 else 0:016x}\n' for fn, a, _, _ in rows)
proc = subprocess.run(['docker', 'exec', '-i', sys.argv[2], '/task/tmp-1b-1/capture', '--query'], input=query, text=True, capture_output=True, check=True)
outputs = proc.stdout.splitlines()
assert len(outputs) == len(rows)
totals = collections.Counter()
failures = []
for row, output in zip(rows, outputs):
    fn, _, expected, original = row
    actual = struct.unpack('<d', struct.pack('<Q', int(output, 16)))[0]
    totals[fn] += 1
    if not (actual == expected or math.isnan(actual) and math.isnan(expected)):
        failures.append((original, output, f'{bits(expected):016x}'))
for fn, count in sorted(totals.items()):
    print(fn, count)
print('total', len(rows), 'mismatches', len(failures))
for failure in failures[:20]:
    print(*failure)
sys.exit(bool(failures))
