"""Generate DataGrip cases from read-only captures (stdlib only).

Identity is SHA256 of source SQL after stripping a leading UTF-8 BOM and
normalizing CRLF to LF, before parameter expansion. JSON digests are checked
against the normalized SQL UTF-8 bytes, including embedded JSON SQL strings.
Only unexpanded :schema_id and $N parameters become literal 2200 (public's
standard namespace OID); existing captured OIDs/names are never rewritten.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'compat/corpus/datagrip'
MARKER = '-- generated-by: compat/tools/gen_datagrip_corpus.py'
# Populated only from PostgreSQL 18 qualification evidence, keyed by source hash.
PG18_UNTESTED = {}


def digest(sql):
    return hashlib.sha256(sql.encode('utf-8')).hexdigest()


def normalize_source(text):
    return text.removeprefix('\ufeff').replace('\r\n', '\n')


def read_source(path):
    return normalize_source(path.read_bytes().decode('utf-8'))


def statements(sql):
    """Split on real SQL terminators, retaining comments, literals and spacing."""
    start = i = 0
    quote = None
    block = 0
    line = False
    while i < len(sql):
        if line:
            if sql[i] == '\n':
                line = False
        elif block:
            if sql.startswith('/*', i):
                block += 1
                i += 1
            elif sql.startswith('*/', i):
                block -= 1
                i += 1
        elif quote:
            if sql.startswith(quote, i):
                if quote in ("'", '"') and sql.startswith(quote * 2, i):
                    i += 1
                else:
                    i += len(quote) - 1
                    quote = None
        elif sql.startswith('--', i):
            line = True
            i += 1
        elif sql.startswith('/*', i):
            block = 1
            i += 1
        elif sql[i] in ("'", '"'):
            quote = sql[i]
        elif sql[i] == '$' and (m := re.match(r'\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$', sql[i:])):
            quote = m[0]
            i += len(quote) - 1
        elif sql[i] == ';':
            yield sql[start:i + 1]
            start = i + 1
        i += 1
    tail = sql[start:]
    # A trailing comment belongs to the capture, even after its last command.
    if tail.strip():
        if re.sub(r'--[^\n]*|/\*.*?\*/', '', tail, flags=re.S).strip():
            yield tail + '\n;'
        else:
            yield tail


def sources(root=ROOT):
    probe = root / 'crates/giggles-pg-conformance/probes/datagrip-2026.2.5'
    for path in sorted(probe / name for name in ('latest-expanded-queries.json', 'app-expanded-queries.json')):
        for row in json.loads(read_source(path)):
            sql = normalize_source(row['sql'])
            sha = digest(sql)
            expected = row.get('sha256', row.get('expanded_sha256'))
            if expected != sha:
                raise ValueError(f'{path}#{row["case"]}: normalized SQL SHA256 mismatch')
            yield dict(sql=sql, sha=sha, family=row.get('family', row.get('name')), source=path.relative_to(root).as_posix(), case=row['case'])
    for path in sorted((root / 'crates/giggles-pg-catalog/tests/fixtures').glob('datagrip_*.sql')):
        sql = read_source(path)
        yield dict(sql=sql, sha=digest(sql), family=path.stem.removeprefix('datagrip_'), source=path.relative_to(root).as_posix(), case=path.stem)


def cases(root=ROOT):
    seen = set()
    used = set()
    for item in sources(root):
        if item['sha'] in seen:
            continue
        seen.add(item['sha'])
        family = re.sub('[^a-z0-9_]+', '_', item['family'].lower()).strip('_')
        name = 'datagrip_' + family
        if len(name) > 47 or name in used:
            name = name[:34] + '_' + item['sha'][:12]
        if name in used:
            raise ValueError('case id collision: ' + name)
        used.add(name)
        yield dict(item, id=name)


def render(item):
    name = item['id']
    table = 'r0c_' + name + '_app'
    # Parameter replacements are SQL literals, never changes to captured names.
    parameter_pattern = r'(?<!:):schema_id\b|\$[0-9]+\b'
    parameters = list(dict.fromkeys(re.findall(parameter_pattern, item['sql'])))
    sql = re.sub(parameter_pattern, '2200', item['sql'])
    expectation = PG18_UNTESTED.get(item['sha'])
    lines = [MARKER, f'-- case: {name}', f'-- title: {Path(item["source"]).name}#{item["case"]} sha256:{item["sha"][:12]}', '-- owner: G', '-- expect: ' + ('untested ' + expectation if expectation else 'compatible'), '-- compare: header', f'-- source: {item["source"]}#{item["case"]}', f'-- source-sha256: {item["sha"]}']
    if parameters:
        lines.append('-- parameters: ' + ', '.join(token + '=2200' for token in parameters) + ' (literal public namespace OID).')
    lines += [f'CREATE TABLE {table} (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);', f'CREATE INDEX {name}_body ON {table} (body);']
    return '\n'.join(lines) + '\n' + '\n'.join(statements(sql)) + '\n'


def generate(out=OUT, root=ROOT):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    desired = {item['id'] + '.sql': render(item) for item in cases(root)}
    for path in out.glob('*.sql'):
        if path.name not in desired and read_source(path).startswith(MARKER + '\n'):
            path.unlink()
    for name, text in desired.items():
        path = out / name
        if path.exists() and not read_source(path).startswith(MARKER + '\n'):
            raise ValueError(f'refusing to overwrite unowned file: {path}')
        path.write_bytes(text.encode('utf-8'))
    return len(desired)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, default=OUT)
    args = parser.parse_args()
    print(f'Generated {generate(args.out)} DataGrip cases in {args.out}')
