#!/usr/bin/env python3
"""Regenerate the PostgreSQL 18.6 API fixture with python -I tools/gen_api_cases.py."""

import argparse
import collections
import json
import pathlib
import re
import subprocess
from dataclasses import dataclass

CRATE = pathlib.Path(__file__).resolve().parents[1]
FUNCTIONS = {
    "regexp_count", "regexp_instr", "regexp_like", "regexp_match", "regexp_matches",
    "regexp_replace", "regexp_split_to_array", "regexp_split_to_table",
    "regexp_substr", "similar_escape", "similar_to_escape", "substring",
}
OPERATORS = {
    "~": "textregexeq", "!~": "textregexne", "~*": "texticregexeq", "!~*": "texticregexne",
}
SET_FUNCTIONS = {"regexp_matches", "regexp_split_to_table"}
TOKEN = re.compile(
    r"(?P<skip>\s+|--[^\n]*|/\*.*?\*/|^[ \t]*\\[^\n]*)"
    r"|(?P<literal>[eE]'(?:\\.|''|[^'\\])*'|'(?:''|[^'])*'"
    r"|(?P<tag>\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$).*?(?P=tag))"
    r'|"(?:[^"]|"")*"|[a-z_][a-z_0-9]*|[0-9]+|!?~\*?|\|\||::|.',
    re.DOTALL | re.IGNORECASE | re.MULTILINE,
)


@dataclass(frozen=True)
class Case:
    origin: str
    op: str
    args: tuple[str, ...]
    expression: str
    locale: str = "CUtf8"

    def oracle_input(self):
        if self.op in SET_FUNCTIONS:
            value = (
                "SELECT coalesce(json_agg(value ORDER BY ord), '[]'::json) "
                f"FROM {self.expression} WITH ORDINALITY AS matches(value, ord)"
            )
        else:
            value = f"SELECT to_json(result.value) FROM (SELECT {self.expression} AS value) result"
        return {
            "args": f"SELECT json_build_array({', '.join(self.args)})",
            "value": value,
        }


def tokenize(sql):
    return [
        (match.group(), match.start())
        for match in TOKEN.finditer(sql) if match.lastgroup != "skip"
    ]


def statements(tokens):
    start = 0
    for index, (token, _) in enumerate(tokens):
        if token == ";":
            if start != index:
                yield tokens[start:index]
            start = index + 1
    if start < len(tokens):
        yield tokens[start:]


def argument_lists(tokens, start, separators):
    """Read top-level arguments after an opening delimiter."""
    args = []
    begin = start
    depth = 0
    for index in range(start, len(tokens)):
        token = tokens[index].lower()
        if token == ")" and depth == 0:
            args.append(tokens[begin:index])
            return args, index
        if token in separators and depth == 0:
            args.append(tokens[begin:index])
            begin = index + 1
        if token == "(":
            depth += 1
        if token == ")":
            depth -= 1
    raise ValueError(f"unclosed argument list in {tokens}")


def operand(tokens, index):
    """Read one argument in the regression files' infix expressions."""
    end = index + 1
    if tokens[end:end + 1] == ["("]:
        _, close = argument_lists(tokens, end + 1, {","})
        end = close + 1
    return tokens[index:end], end


def call(origin, op, args, expression=None, locale="CUtf8"):
    if expression is None:
        expression = f"{op}({', '.join(args)})"
    return Case(origin, op, tuple(args), expression, locale)


def read_source(path, corpus=False):
    sql = path.read_text(encoding="utf-8")
    source = list(statements(tokenize(sql)))
    rows = []
    if corpus:
        for statement in source:
            tokens = [token for token, _ in statement]
            if tokens[0].lower() == "insert":
                i = [token.lower() for token in tokens].index("values") + 1
                while i < len(tokens):
                    assert tokens[i] == "("
                    (row_id, text), end = argument_lists(tokens, i + 1, {","})
                    rows.append(("".join(row_id), " ".join(text)))
                    i = end + 2
        assert rows

    def inputs(origin, arg_tokens):
        for args in arg_tokens:
            for token in args:
                if re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", token):
                    assert token.lower() in {"null", "s", "chr", "repeat"}, (origin, token)
        if any("s" in arg for arg in arg_tokens):
            assert corpus
            for row_id, text in rows:
                yield f"{origin} row {row_id}", [
                    " ".join(text if token == "s" else token for token in arg) for arg in arg_tokens
                ]
        else:
            yield origin, [" ".join(arg) for arg in arg_tokens]

    for statement in source:
        tokens = [token for token, _ in statement]
        lower = [token.lower() for token in tokens]
        if lower[0] not in {"select", "explain"}:
            continue
        for i, name in enumerate(lower):
            origin = f"{path.name}:{sql.count(chr(10), 0, statement[i][1]) + 1}"
            if name in FUNCTIONS and lower[i + 1:i + 2] == ["("]:
                args, _ = argument_lists(tokens, i + 2, {","})
                if name != "substring":
                    for location, values in inputs(origin, args):
                        yield call(location, name, values)
                    continue
                args, end = argument_lists(tokens, i + 2, {",", "from", "for", "similar", "escape"})
                if len(args) < 2 or re.fullmatch(r"-?[0-9]+", "".join(args[1])):
                    continue  # Position-based substring.
                for location, values in inputs(origin, args):
                    if "similar" in lower[i + 2:end]:
                        a, p, e = values
                        yield call(location, "substring_similar", values, f"substring({a} SIMILAR {p} ESCAPE {e})")
                    elif "for" in lower[i + 2:end]:
                        a, p, e = values
                        yield call(location, "substring_similar", values, f"substring({a} FROM {p} FOR {e})")
                    elif len(values) == 3:
                        yield call(location, "substring_similar", values, f"substring({', '.join(values)})")
                    else:
                        a, p = values
                        yield call(location, "textregexsubstr", values, f"substring({a} FROM {p})")
            elif name in OPERATORS and lower[0] == "select":
                right, _ = operand(tokens, i + 1)
                for location, values in inputs(origin, [tokens[i - 1:i], right]):
                    a, p = values
                    yield call(location, OPERATORS[name], values, f"({a} {name} {p})")
            elif name == "similar" and lower[i + 1:i + 2] == ["to"]:
                pattern, end = operand(tokens, i + 2)
                suffix = []
                if lower[end:end + 1] == ["escape"]:
                    escape, _ = operand(tokens, end + 1)
                    suffix = [escape]
                if lower[0] == "explain":
                    # These planner tests have a column on the left. Replay the
                    # constant pattern translation that EXPLAIN folds.
                    for location, values in inputs(origin, [pattern, *suffix]):
                        yield call(location, "similar_to_escape", values)
                    continue
                negated = lower[i - 1] == "not"
                first = i - 2 if negated else i - 1
                for location, values in inputs(origin, [[tokens[first]], pattern, *suffix]):
                    a, p, *esc = values
                    operator = "NOT SIMILAR TO" if negated else "SIMILAR TO"
                    expression = f"({a} {operator} {p}" + (f" ESCAPE {esc[0]})" if esc else ")")
                    op = "not_similar_to" if negated else "similar_to"
                    yield call(location, op, values, expression)


def literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return str(value)
    return "'" + value.replace("'", "''") + "'"


def edge_cases():
    def edge(label, op, *args, **kwargs):
        return call(f"edge: {label}", op, [literal(arg) for arg in args], **kwargs)

    yield edge("mentions in body order", "regexp_matches",
               "hi @alice, @bob.smith and @dev_team-2 / @alice", r"@([A-Za-z0-9_.-]+)", "g")
    yield edge("null capture and empty capture", "regexp_match", "b", "(a)?(b)()")
    yield edge("empty input with global zero-width match", "regexp_matches", "", "", "g")
    yield edge("zero-width matches over Unicode", "regexp_matches", "é😀", "", "g")
    yield edge("Unicode scalar positions", "regexp_instr", "😀éaé", "(é)", 2, 2, 1, "", 1)
    yield edge("Unicode scalar substring", "regexp_substr", "😀éaé", "(é)", 2, 2, "", 1)
    yield edge("end-of-input start position", "regexp_count", "é", "$", 2, "")
    yield edge("beyond end still compiles", "regexp_count", "é", "(", 3, "")
    yield edge("replace beyond end still compiles", "regexp_replace", "é", "(", "_", 3)
    yield edge("minimum signed start", "regexp_count", "a", "(", -2147483648, "?")
    yield edge("maximum signed start", "regexp_count", "a", "a", 2147483647)
    yield edge("subexpr one without captures", "regexp_substr", "abc", "bc", 1, 1, "", 1)
    yield edge("instr subexpr one without captures", "regexp_instr", "abc", "bc", 1, 1, 1, "", 1)
    yield edge("instr subexpr two without captures", "regexp_instr", "abc", "bc", 1, 1, 1, "", 2)
    yield edge("replace omitted flags", "regexp_replace", "aaaa", "a", "X")
    yield edge("replace start with Unicode", "regexp_replace", "éa😀a", "a", "X", 3)
    yield edge("replace explicit all without g", "regexp_replace", "a a", "a", "X", 1, 0, "")
    yield edge("replace explicit nth with g", "regexp_replace", "a a a", "a", "X", 1, 2, "g")
    yield edge("replace Unicode zero-width all", "regexp_replace", "é😀", "", r"<\&>", "g")
    yield edge("replace missing groups and literal escapes", "regexp_replace",
               "xéa", "(é)(a)(z)?", r"\&:\2\1[\3][\9]:\\1:\0:\β:\x:" + "\\")
    yield edge("replace all nine capture indexes", "regexp_replace",
               "abcdefghi", "(a)(b)(c)(d)(e)(f)(g)(h)(i)", r"\9\8\7\6\5\4\3\2\1")
    yield edge("invalid four-argument digit", "regexp_replace", "a", "(", "", "2x")
    yield edge("invalid four-argument nondigit prefix", "regexp_replace", "a", "(", "", "i1")
    yield edge("no digit hint in extended form", "regexp_replace", "a", "(", "", 1, 1, "2x")
    yield edge("negative-looking flags", "regexp_replace", "a", "(", "", "-1")
    yield edge("split ignores empty boundaries", "regexp_split_to_array", "é😀é", "")
    yield edge("split empty input", "regexp_split_to_table", "", "")
    yield edge("split empty pieces around nonempty matches", "regexp_split_to_table", ",é,,", ",")
    yield edge("split suppresses adjacent empty matches", "regexp_split_to_array", "abXXc", "X*")
    yield edge("substring captured empty", "textregexsubstr", "abc", "()a",
               expression="substring('abc' FROM '()a')")
    yield edge("negation still propagates compile error", "textregexne", "a", "(",
               expression="'a' !~ '('")
    yield edge("strict scalar NULL", "regexp_replace", None, "(", "a")
    yield edge("strict set NULL input", "regexp_matches", None, "(", "g")
    yield edge("strict NULL flags", "regexp_split_to_table", "a", "(", None)

    for flag, text, pattern in [
        ("b", "a+", r"a\+"), ("e", "aaa", "^a+$"), ("i", "ABC", "abc"),
        ("ic", "ABC", "abc"), ("q", "a.b", "a.b"), ("q", "acb", "a.b"),
        ("m", "a\nb", "^b$"), ("n", "a\nb", "a.b"), ("p", "a\nb", "^b$"),
        ("np", "a\nb", "a.b"), ("ns", "a\nb", "a.b"), ("w", "a\nb", "a.b"),
        ("w", "a\nb", "^b$"), ("x", "abc", "a . c # comment"),
        ("xt", "abc", "a . c"), ("", "a", "a"), ("ii", "A", "a"),
        ("gé", "a", "("), ("🦉i", "", "("), ("G", "a", "("), (" ", "a", "("),
    ]:
        yield edge(f"flags {flag!r} on {pattern!r}", "regexp_like", text, pattern, flag)

    for op, args in [
        ("regexp_count", ("a", "(", 0, "g?")),
        ("regexp_count", ("a", "(", 1, "g")),
        ("regexp_instr", ("a", "(", 0, 0, 2, "g?", -1)),
        ("regexp_instr", ("a", "(", 1, 0, 2, "g?", -1)),
        ("regexp_instr", ("a", "(", 1, 1, 2, "g?", -1)),
        ("regexp_instr", ("a", "(", 1, 1, 0, "g?", -1)),
        ("regexp_instr", ("a", "(", 1, 1, 0, "g")),
        ("regexp_substr", ("a", "(", 0, 0, "g?", -1)),
        ("regexp_substr", ("a", "(", 1, 0, "g?", -1)),
        ("regexp_substr", ("a", "(", 1, 1, "g?", -1)),
        ("regexp_substr", ("a", "(", 1, 1, "g")),
        ("regexp_match", ("a", "(", "g")),
        ("regexp_match", ("a", "(", "g?")),
        ("regexp_matches", ("a", "(", "g?")),
        ("regexp_split_to_array", ("a", "(", "g")),
        ("regexp_split_to_table", ("a", "(", "g")),
        ("regexp_replace", ("a", "(", "x", 0, -1, "?")),
        ("regexp_replace", ("a", "(", "x", 1, -1, "?")),
    ]:
        yield edge("validation before compilation", op, *args)

    for pattern in [
        r"[[.xx.]]", "[[:absent:]]", "\\", r"\1", "[", "(", "a{1", "a{3,2}",
        "[z-a]", "*", "(?z)x", r"\x7fffffff",
    ]:
        yield edge(f"invalid engine pattern {pattern!r}", "regexp_like", "a", pattern)

    for locale in ("C", "CUtf8", "C"):
        collation = ' COLLATE "C"' if locale == "C" else ""
        yield edge(f"{locale} character class", "regexp_like", "é", "[[:alpha:]]",
                   locale=locale, expression=f"regexp_like('é'{collation}, '[[:alpha:]]')")
        yield edge(f"{locale} case mapping", "texticregexeq", "é", "É",
                   locale=locale, expression=f"('é'{collation} ~* 'É')")

    for pattern, escape in [
        (r"""a\ "%"^.$_\d""", "\\"), ("", ""), (r"[\]()%._$^", ""),
        ("abc#", "#"), ("abc", "##"), ("abc", "😀é"), ("a💡%_", "💡"),
        ('[é]💡"x', "💡"), ('[💡é]💡"x', "💡"), ('[é]#"x', "#"),
        ('[^]#"]#"', "#"), ('[[:alnum:]#"]#"', "#"), ('#[#]#"x#"z', "#"),
        ('#"#"#"', "#"), ('💡"💡"💡"', "💡"),
    ]:
        yield edge(f"SIMILAR translation {pattern!r}", "similar_to_escape", pattern, escape)
    yield edge("one argument default escape", "similar_to_escape", r'%\"a\%\"_')
    yield edge("strict translation NULL escape", "similar_to_escape", "%", None)
    yield edge("legacy NULL escape defaults", "similar_escape", r"\%_", None)
    yield edge("legacy NULL pattern short circuit", "similar_escape", None, "##")
    yield edge("legacy multibyte escape", "similar_escape", "😀%%", "😀")
    yield edge("legacy invalid escape", "similar_escape", "%", "##")
    yield edge("invalid translated regex", "similar_to", "a", "[",
               expression="'a' SIMILAR TO '['")
    yield edge("negated SIMILAR", "not_similar_to", "abc", "b%",
               expression="'abc' NOT SIMILAR TO 'b%'")
    yield edge("function-call SIMILAR substring", "substring_similar", "abc", 'a#"b#"c', "#",
               expression="""substring('abc', 'a#"b#"c', '#')""")


ORACLE = """
BEGIN READ ONLY;
SET LOCAL standard_conforming_strings = on;
SET LOCAL client_min_messages = notice;
DO $oracle$
DECLARE
    item jsonb;
    args json;
    result json;
    state text;
    message text;
    detail text;
    hint text;
BEGIN
    IF current_setting('server_version_num') <> '180006'
        OR current_setting('server_encoding') <> 'UTF8'
        OR (SELECT datctype FROM pg_database WHERE datname = current_database()) <> 'C.UTF-8'
    THEN
        RAISE EXCEPTION 'Expected a PostgreSQL 18.6 UTF8 / C.UTF-8 oracle';
    END IF;
    FOR item IN SELECT value FROM jsonb_array_elements($cases$PAYLOAD$cases$::jsonb)
    LOOP
        EXECUTE item->>'args' INTO args;
        BEGIN
            EXECUTE item->>'value' INTO result;
            result := json_build_object('value', result);
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS state = RETURNED_SQLSTATE, message = MESSAGE_TEXT,
                detail = PG_EXCEPTION_DETAIL, hint = PG_EXCEPTION_HINT;
            result := json_build_object(
                'error', json_build_object(
                    'sqlstate', state, 'message', message,
                    'detail', nullif(detail, ''), 'hint', nullif(hint, '')
                )
            );
        END;
        RAISE NOTICE 'API_CASE %', jsonb_build_object('args', args, 'expected', result);
    END LOOP;
END;
$oracle$;
ROLLBACK;
"""


def needs_engine(case, args, expected):
    if case.op in {"similar_escape", "similar_to_escape"} or None in args:
        return False
    state = expected.get("error", {}).get("sqlstate")
    return state not in {"22023", "22025", "2200C"}


def run_oracle(cases):
    payload = json.dumps([case.oracle_input() for case in cases], ensure_ascii=False)
    assert "$cases$" not in payload and "$oracle$" not in payload
    process = subprocess.run(
        ["docker", "exec", "-e", "PGCLIENTENCODING=UTF8", "-i", "pgp-oracle-regex",
         "psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres"],
        input=ORACLE.replace("PAYLOAD", payload).encode("utf-8"),
        capture_output=True, check=False,
    )
    stderr = process.stderr.decode("utf-8")
    if process.returncode:
        raise RuntimeError(stderr[-3000:])
    prefix = "NOTICE:  API_CASE "
    results = [json.loads(line[len(prefix):]) for line in stderr.split("\n") if line.startswith(prefix)]
    if len(results) != len(cases):
        raise RuntimeError(f"Oracle returned {len(results)} results for {len(cases)} cases")

    supported_errors = {"2201B", "22023", "22025", "2200C", "54000", "57014"}
    for case, observed in zip(cases, results, strict=True):
        expected = observed["expected"]
        if "error" in expected and expected["error"]["sqlstate"] not in supported_errors:
            raise RuntimeError(f"Unexpected SQL failure for {case}: {expected}")
        yield {
            "source": case.origin,
            "sql": case.expression,
            "op": case.op,
            "locale": case.locale,
            "engine": needs_engine(case, observed["args"], expected),
            **observed,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=pathlib.Path,
                        default=pathlib.Path("K:/Projects/gigglesdb/compat/corpus/pg_functions"))
    args = parser.parse_args()
    cases = []
    for source in ("regex", "strings"):
        cases.extend(read_source(CRATE / "upstream" / "regress" / f"{source}.sql"))
    for source in ("patterns", "regex_scalar", "regex_sets", "substrings"):
        cases.extend(read_source(args.corpus / f"pgf_string_{source}.sql", corpus=True))
    cases.extend(edge_cases())
    records = list(run_oracle(cases))
    target = CRATE / "tests" / "data" / "api_cases.json"
    target.write_text("[\n" + ",\n".join(
        "  " + json.dumps(record, ensure_ascii=False) for record in records
    ) + "\n]\n", encoding="utf-8", newline="\n")
    by_source = collections.Counter(record["source"].split(":")[0] for record in records)
    engine_free = sum(not record["engine"] for record in records)
    print(f"{len(records)} cases, {engine_free} engine-free, {len(records) - engine_free} engine-dependent")
    print(dict(by_source))


if __name__ == "__main__":
    main()
