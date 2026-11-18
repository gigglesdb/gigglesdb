"""Deterministic edge-first sample, plus random/domain-stratified sweep records."""
import pathlib
import struct
import sys

source, target = map(pathlib.Path, sys.argv[1:])
target.mkdir(parents=True, exist_ok=True)
for path in sorted(source.glob('*.bin')):
    arity = 3 if path.stem in ('pow', 'atan2') else 2
    record = struct.Struct('<' + 'Q' * arity)
    data = path.read_bytes()
    assert len(data) % record.size == 0
    count = len(data) // record.size
    indices = sorted(set(range(min(1000, count))) | {
        1000 + i * (count - 1000) // 1000 for i in range(1000)
        if count > 1000
    })
    with (target / (path.stem + '.txt')).open('w', newline='\n') as out:
        for i in indices:
            out.write(' '.join(f'{u:016x}' for u in record.unpack_from(data, i * record.size)) + '\n')
    print(path.stem, count, len(indices))
