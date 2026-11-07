"""Pure parsing and differential comparison for the statement corpus."""
import re
import copy
import csv
import io
from pathlib import Path

from .checks import OWNERS, normalize

UNSUPPORTED_STATES = frozenset({'0A000', '42601', '42883', '42704'})
OUTCOMES = ('COMPATIBLE', 'UNSUPPORTED', 'DIVERGENT', 'INTENTIONAL', 'UNTESTED', 'HARNESS_ERROR')
COMPARISONS = ('full', 'shape', 'header')


def _scan(line, quote=None, terminators=None):
    """Return SQL with comments masked and the continuing lexical state."""
    i = 0
    code = list(line)
    while i < len(line):
        if isinstance(quote, int):
            if line.startswith('/*', i):
                quote += 1
                code[i:i+2] = '  '
                i += 2
            elif line.startswith('*/', i):
                quote -= 1
                quote = quote or None
                code[i:i+2] = '  '
                i += 2
            else:
                code[i] = ' '
                i += 1
            continue
        if quote:
            if line.startswith(quote, i):
                if quote in ("'", '"') and line.startswith(quote * 2, i):
                    i += 2
                    continue
                i += len(quote)
                quote = None
                continue
            # Backslashes inside SQL literals cannot become psql commands.
            i += 1
            continue
        if line.startswith('--', i):
            return ''.join(code[:i]), quote
        if line.startswith('/*', i):
            quote = 1
            code[i:i+2] = '  '
            i += 2
            continue
        if line[i] == '\\':
            raise ValueError('psql backslash commands are forbidden')
        if line[i] == ';' and terminators is not None:
            terminators.append(i)
        if line[i] in ("'", '"'):
            quote = line[i]
        elif line[i] == '$':
            m = re.match(r'\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$', line[i:])
            if m:
                quote = m[0]
                i += len(quote)
                continue
        i += 1
    return ''.join(code), quote


def parse_case(path, area=None):
    path = Path(path)
    headers, statements, pending, parts = {}, [], False, []
    quote = None
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        if not quote and line.lstrip().startswith('--'):
            if re.fullmatch(r'\s*--\s*@error\s*', line):
                if parts or pending:
                    raise ValueError(f'{path}: misplaced @error')
                pending = True
            else:
                m = re.fullmatch(r'\s*--\s*(case|title|owner|expect|compare|source):\s*(.*?)\s*', line)
                if m:
                    if statements or parts or m[1] in headers:
                        raise ValueError(f'{path}: misplaced or duplicate header')
                    headers[m[1]] = m[2]
                elif parts:
                    parts.append(line.rstrip())
            continue
        terminators = []
        code, quote = _scan(line, quote, terminators)
        if terminators and (len(terminators) != 1 or code[terminators[0] + 1:].strip()):
            raise ValueError(f'{path}: one SQL command per line-terminated chunk is required')
        if code.strip() or parts or quote or line.lstrip().startswith('/*'):
            parts.append(line.rstrip())
        if not quote and terminators:
            statements.append(dict(index=len(statements) + 1, sql='\n'.join(parts), expected_error=pending))
            parts, pending = [], False
    if parts or quote or pending:
        raise ValueError(f'{path}: unterminated SQL or dangling @error')
    if not statements:
        raise ValueError(f'{path}: empty case')
    if not all(headers.get(k) for k in ('case', 'title', 'owner')):
        raise ValueError(f'{path}: case/title/owner headers required')
    if not re.fullmatch(r'[A-Za-z0-9_-]+', headers['case']) or headers['owner'] not in OWNERS:
        raise ValueError(f'{path}: invalid case id or owner')
    expectation = headers.get('expect', 'compatible').split(None, 1)
    if not expectation or expectation[0] not in ('compatible', 'intentional', 'untested') or (expectation[0] == 'compatible' and len(expectation) != 1) or (expectation[0] != 'compatible' and len(expectation) != 2):
        raise ValueError(f'{path}: invalid expectation (reason required)')
    warnings = []
    comparison = headers.get('compare', 'full')
    if comparison not in COMPARISONS:
        raise ValueError(f'{path}: unsupported compare header')
    for statement in statements:
        for m in re.finditer(r'\bCREATE\s+(?:TEMP(?:ORARY)?\s+|UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)', statement['sql'], re.I):
            if headers['case'].lower() not in m[1].lower():
                warnings.append(f"statement {statement['index']}: CREATE TABLE {m[1]} does not contain case id")
    return dict(id=headers['case'], title=headers['title'], owner=headers['owner'], compare=comparison, source=headers.get('source', ''), expect=expectation[0], reason=expectation[1] if len(expectation) > 1 else '', area=area or path.parent.name, path=str(path), statements=statements, warnings=warnings)


def discover_cases(root, glob='**/*.sql'):
    root = Path(root)
    cases = [parse_case(p, p.relative_to(root).parts[0] if len(p.relative_to(root).parts) > 1 else root.name) for p in sorted(root.glob(glob)) if p.is_file()]
    seen = set()
    for case in cases:
        if case['id'] in seen:
            raise ValueError('duplicate corpus case id: ' + case['id'])
        seen.add(case['id'])
    return cases


_SCRIPT_PREAMBLE = ['\\set ON_ERROR_STOP 0', '\\set VERBOSITY sqlstate', '\\pset format csv', '\\pset pager off']


def _cleanup_tokens(sql):
    """Tokenize identifiers while hiding comments and SQL literal contents."""
    tokens, i = [], 0
    while i < len(sql):
        if sql[i].isspace():
            i += 1
        elif sql.startswith('--', i):
            end = sql.find('\n', i)
            i = len(sql) if end < 0 else end + 1
        elif sql.startswith('/*', i):
            depth, i = 1, i + 2
            while i < len(sql) and depth:
                if sql.startswith('/*', i):
                    depth, i = depth + 1, i + 2
                elif sql.startswith('*/', i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
        elif sql[i] in ('"', "'"):
            quote, start = sql[i], i
            i += 1
            while i < len(sql):
                if sql[i] == quote:
                    i += 1
                    if i < len(sql) and sql[i] == quote:
                        i += 1
                        continue
                    break
                if quote == "'" and sql[i] == '\\':
                    i += 1
                i += 1
            tokens.append(sql[start:i] if quote == '"' else '<literal>')
        else:
            dollar = re.match(r'\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$', sql[i:])
            word = re.match(r'[A-Za-z_\u0080-\uffff][A-Za-z0-9_$\u0080-\uffff]*', sql[i:])
            if dollar:
                end = sql.find(dollar[0], i + len(dollar[0]))
                i = len(sql) if end < 0 else end + len(dollar[0])
                tokens.append('<literal>')
            elif word:
                tokens.append(word[0])
                i += len(word[0])
            else:
                tokens.append(sql[i])
                i += 1
    return tokens


def cleanup_tables(case):
    """Return only corpus-owned table names, newest creation/rename first."""
    def identifier(token):
        return token.startswith('"') or bool(re.fullmatch(r'[A-Za-z_\u0080-\uffff][A-Za-z0-9_$\u0080-\uffff]*', token))

    def name_at(tokens, index):
        parts = []
        if index < len(tokens) and identifier(tokens[index]):
            parts.append(tokens[index])
            index += 1
            while index + 1 < len(tokens) and tokens[index] == '.' and identifier(tokens[index + 1]):
                parts.append(tokens[index + 1])
                index += 2
        return parts, index

    events = []
    for statement in case['statements']:
        tokens = _cleanup_tokens(statement['sql'])
        upper = [token.upper() for token in tokens]
        if not tokens:
            continue
        index = 1
        if upper[0] == 'CREATE':
            if index < len(tokens) and upper[index] in ('TEMP', 'TEMPORARY', 'UNLOGGED'):
                index += 1
            if upper[index:index + 1] != ['TABLE']:
                continue
            index += 1
            if upper[index:index + 3] == ['IF', 'NOT', 'EXISTS']:
                index += 3
            parts, _ = name_at(tokens, index)
        elif upper[:2] == ['ALTER', 'TABLE']:
            index = 2
            if upper[index:index + 2] == ['IF', 'EXISTS']:
                index += 2
            if upper[index:index + 1] == ['ONLY']:
                index += 1
            old, index = name_at(tokens, index)
            if not old or upper[index:index + 2] != ['RENAME', 'TO']:
                continue
            old_basename = old[-1]
            old_content = old_basename[1:-1].replace('""', '"') if old_basename.startswith('"') else old_basename.lower()
            if old_content.startswith('r0c_'):
                events.append(old)
            parts, _ = name_at(tokens, index + 2)
            if len(parts) == 1:
                parts = old[:-1] + parts
        else:
            continue
        if parts:
            basename = parts[-1]
            content = basename[1:-1].replace('""', '"') if basename.startswith('"') else basename.lower()
            if content.startswith('r0c_'):
                events.append(parts)
    result, seen = [], set()
    for parts in reversed(events):
        key = tuple(part[1:-1].replace('""', '"') if part.startswith('"') else part.lower() for part in parts)
        if key not in seen:
            seen.add(key)
            result.append('.'.join(parts))
    return result


def generate_cleanup_script(case):
    names = cleanup_tables(case)
    if not names:
        return None
    return '\n'.join(_SCRIPT_PREAMBLE + ['DROP TABLE ' + name + ';' for name in names]) + '\n'


def generate_script(case):
    lines = list(_SCRIPT_PREAMBLE)
    for stmt in case['statements']:
        lines += [f"\\echo @@R0 BEGIN {stmt['index']}", stmt['sql'], f"\\echo @@R0 END {stmt['index']} :SQLSTATE :ROW_COUNT"]
    return '\n'.join(lines) + '\n'


def parse_transcript(stdout, statements, server='giggles', stderr=''):
    records, active, output, invalid = [], None, [], None
    csv_quote = False
    for line in stdout.splitlines():
        begin = None if csv_quote else re.fullmatch(r'@@R0 BEGIN (\d+)', line)
        end = None if csv_quote else re.fullmatch(r'@@R0 END (\d+) ([A-Z0-9]{5}) (\d+)', line)
        if begin:
            index = int(begin[1])
            if active is not None or index != len(records) + 1 or index > len(statements):
                invalid = 'missing or misordered BEGIN/END marker'
                break
            active, output = index, []
        elif end:
            if active is None or int(end[1]) != active:
                invalid = 'missing or misordered END marker'
                break
            # CSV mode omits SELECT command tags. Preserve the actual server's
            # ROW_COUNT alongside its unmodified CSV and DDL/DML command tags.
            output.append('@@R0 ROW_COUNT ' + end[3])
            records.append(dict(index=active, sql=statements[active - 1]['sql'], sqlstate=end[2], row_count=int(end[3]), output_lines=output, stderr_first_line=''))
            active = None
        elif active is not None:
            output.append(line)
            # RFC 4180 double quotes escape each other; paired quotes do not
            # change whether the next physical line is inside a CSV field.
            csv_quote ^= bool(line.count('"') % 2)
    if active is not None or len(records) != len(statements):
        invalid = invalid or 'missing END marker'
    # psql -f - reports the final SQL line number. Reconstruct generated lines.
    sql_lines, cursor = {}, 4
    for stmt in statements:
        cursor += 1 + len(stmt['sql'].splitlines())
        sql_lines[cursor] = stmt['index']
        cursor += 1
    for stmt in statements[len(records):]:
        records.append(dict(index=stmt['index'], sql=stmt['sql'], sqlstate='CONNECTION_LOST', row_count=None, output_lines=[], stderr_first_line=''))
    if invalid and records and all(r['sqlstate'] != 'CONNECTION_LOST' for r in records):
        # A malformed suffix must not make a broken protocol look compatible.
        records[-1]['sqlstate'] = 'CONNECTION_LOST'
    current = None
    for line in stderr.splitlines():
        m = re.search(r'psql:(?:<stdin>|-):([0-9]+):\s*(?:ERROR|FATAL):', line)
        if m:
            current = sql_lines.get(int(m[1]))
        elif re.search(r'(?:psql:.*(?:error|fatal)|connection.*(?:closed|lost)|server closed)', line, re.I):
            current = next((r['index'] for r in records if r['sqlstate'] == 'CONNECTION_LOST'), None)
        if current is not None and 0 < current <= len(records):
            rec = records[current - 1]
            rec['stderr_first_line'] = rec['stderr_first_line'] or line
            rec['stderr'] = rec.get('stderr', '') + line + '\n'
    return dict(records=records, valid=invalid is None, reason=invalid, server=server)


def _shape(record):
    # csv.reader consumes a logical record, including quoted multiline names.
    # For command-only output its first record is the command tag; empty output
    # has no header. ROW_COUNT always comes from the framed protocol metadata.
    lines = record['output_lines']
    if lines and re.fullmatch(r'@@R0 ROW_COUNT \d+', lines[-1]):
        lines = lines[:-1]
    header = next(csv.reader(io.StringIO('\n'.join(lines))), None)
    return header, record.get('row_count')


def statement_difference(pg, giggles, compare='full'):
    if compare not in COMPARISONS:
        raise ValueError('unsupported comparison: ' + compare)
    p, g = pg['sqlstate'], giggles['sqlstate']
    if 'CONNECTION_LOST' in (p, g):
        if p == '00000':
            return 'giggles_error'
        if g == '00000':
            return 'pg_error_only'
        return 'sqlstate_differs'
    if p == g:
        if compare == 'header':
            equal = _shape(pg)[0] == _shape(giggles)[0]
        elif compare == 'shape':
            equal = _shape(pg) == _shape(giggles)
        else:
            equal = normalize('\n'.join(pg['output_lines'])) == normalize('\n'.join(giggles['output_lines']))
        return None if p != '00000' or equal else 'result_differs'
    if p == '00000':
        return 'giggles_error'
    if g == '00000':
        return 'pg_error_only'
    return 'sqlstate_differs'


def compare_case(case, pg, giggles):
    if giggles.get('timed_out') and giggles.get('records'):
        giggles = copy.deepcopy(giggles)
        giggles['records'][-1]['sqlstate'] = 'CONNECTION_LOST'
        giggles['valid'] = False
        giggles['reason'] = 'client timed out'
    result = {k: case.get(k) for k in ('id', 'area', 'title', 'owner', 'expect', 'reason', 'warnings', 'source')}
    result['compare'] = case.get('compare', 'full')
    result['row_counts'] = [dict(index=stmt['index'], pg=p.get('row_count'), giggles=g.get('row_count'))
                            for stmt, p, g in zip(case['statements'], pg.get('records', []), giggles.get('records', []))]
    result.update(outcome='COMPATIBLE', first_divergence=None, stale_expectation=False, pg=pg, giggles=giggles, differences=[], harness_errors=[])
    if case['expect'] == 'untested':
        result['outcome'] = 'UNTESTED'
        return result
    harness = not pg.get('valid', True) or pg.get('timed_out', False) or pg.get('exit', 0) != 0
    if harness:
        result['harness_errors'].append(dict(reason=pg.get('reason') or 'PostgreSQL invocation failed or timed out', index=None, sql=None))
    for stmt, p, g in zip(case['statements'], pg['records'], giggles['records']):
        if (p['sqlstate'] != '00000') != stmt['expected_error'] or p['sqlstate'] == 'CONNECTION_LOST':
            harness = True
            result['harness_errors'].append(dict(reason='PostgreSQL connection lost' if p['sqlstate'] == 'CONNECTION_LOST' else ('expected PostgreSQL error did not occur' if stmt['expected_error'] else 'unexpected PostgreSQL error: ' + p['sqlstate']), index=stmt['index'], sql=stmt['sql']))
        kind = statement_difference(p, g, result['compare'])
        if kind:
            detail = dict(index=stmt['index'], kind=kind, sql=stmt['sql'], pg=dict(sqlstate=p['sqlstate'], output_excerpt='\n'.join(p['output_lines'])[:2000]), giggles=dict(sqlstate=g['sqlstate'], output_excerpt='\n'.join(g['output_lines'])[:2000], stderr_first_line=g.get('stderr_first_line', '')))
            result['differences'].append(detail)
    if len(pg['records']) != len(case['statements']) or len(giggles['records']) != len(case['statements']):
        harness = True
        result['harness_errors'].append(dict(reason='transcript statement count mismatch', index=None, sql=None))
    result['first_divergence'] = next(iter(result['differences']), None)
    if harness:
        result['outcome'] = 'HARNESS_ERROR'
    elif result['first_divergence']:
        first = result['first_divergence']
        g = next(record for record in giggles['records'] if record['index'] == first['index'])
        unsupported = first['kind'] == 'giggles_error' and (g['sqlstate'] in UNSUPPORTED_STATES or (g['sqlstate'] == 'XX000' and re.search(r'unsupported|not supported', g.get('stderr', g.get('stderr_first_line', '')), re.I)))
        result['outcome'] = 'UNSUPPORTED' if unsupported else 'DIVERGENT'
        if case['expect'] == 'intentional':
            result['outcome'] = 'INTENTIONAL'
    elif case['expect'] == 'intentional':
        result['stale_expectation'] = True
    return result


def summarize(results):
    def counts(items):
        total = {key: 0 for key in OUTCOMES}
        for item in items:
            total[item['outcome']] += 1
        denominator = sum(total[k] for k in ('COMPATIBLE', 'UNSUPPORTED', 'DIVERGENT'))
        return dict(total, pass_rate=total['COMPATIBLE'] / denominator if denominator else None)
    return dict(overall=counts(results), by_area={v: counts([r for r in results if r['area'] == v]) for v in sorted({r['area'] for r in results})}, by_owner={v: counts([r for r in results if r['owner'] == v]) for v in sorted({r['owner'] for r in results})})
