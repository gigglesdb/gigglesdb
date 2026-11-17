"""Rebuild compact, self-contained PG18.6 oracle data from capture directory."""
import hashlib
import json
from pathlib import Path
import re
import sys

source = Path(sys.argv[1])
sql = (source / 'gen.sql').read_text(encoding='utf-8')
def decode(s):
    return re.sub(r'\\(?:\+([0-9A-Fa-f]{6})|([0-9A-Fa-f]{4}))', lambda m: chr(int(m[1] or m[2], 16)), s.replace("''", "'"))
strings = {int(i): decode(s) for i, s in re.findall(r"\((\d+),U&'((?:[^']|'')*)'\)", sql)}
assert len(strings) == 700
rows = {r[1]: r for line in (source / 'pg_collation.txt').read_text().splitlines() if len(r := line.split('|')) == 12}
base = []
for line in (source / 'orders.txt').read_text(encoding='utf-8').splitlines():
    if not line.startswith('O|'):
        continue
    _, name, order = line.split('|')
    r = rows[name]
    if r[4] == 'i':
        base.append(dict(name=name, locale=r[9], rules=r[10], version=r[11], det=r[5] == 't', order=order))
options = {}
notices = []
for line in (source / 'options.txt').read_text(encoding='utf-8').splitlines():
    if line.startswith('P|'):
        r = line.split('|')
        if r[2] == 'i':
            options[r[1]] = dict(name=r[1], locale=r[6], rules=r[7], version=r[8], det=r[3] == 't')
    elif line.startswith(('O|', 'G|')):
        kind, name, value = line.split('|')
        if name in options:
            options[name]['order' if kind == 'O' else 'groups'] = value
    elif 'using standard form' in line:
        message = line.split('00000: ', 1)[1]
        match = re.search(r'using standard form "([^"]*)" for ICU locale "([^"]*)"', message)
        notices.append(dict(input=match[2], tag=match[1], message=message))
original = (source / 'gen2.sql').read_text()
for name, case in options.items():
    match = re.search(r"CREATE COLLATION r0c_" + re.escape(name) + r" \(provider=icu, locale='([^']*)'", original)
    case['input_locale'] = match[1] if match else case['locale']
versions = [dict(name=r[1], locale=r[9], version=r[11]) for r in rows.values() if r[4] == 'i']
data = dict(strings=[strings[i] for i in range(700)], base=base, options=list(options.values()), notices=notices, versions=versions,
            sha256={f: hashlib.sha256((source / f).read_bytes()).hexdigest() for f in ['gen.sql', 'gen2.sql', 'orders.txt', 'options.txt', 'pg_collation.txt']})
Path(__file__).with_name('fixtures').joinpath('oracle.json').write_text(json.dumps(data, ensure_ascii=True, separators=(',', ':')) + '\n', encoding='utf-8', newline='\n')
