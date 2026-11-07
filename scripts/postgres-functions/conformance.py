"""DD-0068 reference inventory and initial embedded-SQL differential runner.

Uses only Python's standard library and Docker. The reference is an owned,
network-disabled container, never an existing database. No function is certified
by merely having its name in the inventory or by passing one happy-path case.
"""
from __future__ import annotations

import argparse
import hashlib
import gzip
import json
import re
import struct
import math
from decimal import Decimal
from pathlib import Path
import subprocess
import time

from workdir import check_outside_repo

ROOT = Path(__file__).resolve().parent
IMAGE = "postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c"
REQUIRED_KINDS = {"normal", "null", "boundary", "error"}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def run(args, input=None, timeout=60):
    return subprocess.run(args, input=input, text=True, encoding="utf-8",
                          capture_output=True, timeout=timeout, check=False)


def require(result):
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or f"exit {result.returncode}")
    return result.stdout.strip()


def validate_inventory(inventory):
    if inventory.get("server_version_num") != 180000:
        raise ValueError("reference must be exactly PostgreSQL 18.0")
    routines = inventory.get("routines")
    if not isinstance(routines, list) or not routines:
        raise ValueError("empty or malformed routine inventory")
    identities = [r["identity"] for r in routines]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate routine identities")
    for field in ("types", "casts", "operators", "aggregates", "namespaces", "extensions"):
        if not inventory.get(field):
            raise ValueError(f"missing catalog {field}")
    return set(identities)


def validate_pin(inventory, metadata):
    identities = validate_inventory(inventory)
    if (metadata.get("image") != IMAGE or metadata.get("server_version_num") != 180000
            or metadata.get("routine_count") != len(identities)
            or metadata.get("inventory_sha256") != digest(inventory)):
        raise ValueError("reference identity/hash/count mismatch")


def validate_cases(cases, identities):
    if not cases:
        raise ValueError("empty case corpus")
    seen = set()
    for case in cases:
        if case["id"] in seen:
            raise ValueError("duplicate case ID")
        seen.add(case["id"])
        if case["kind"] not in REQUIRED_KINDS:
            raise ValueError("unknown case kind")
        if not isinstance(case.get("setup"), list) or not isinstance(case.get("query"), str):
            raise ValueError("invalid SQL fixture")
        expected = case.get("expected", {})
        if expected.get("status") not in {"ok", "error"}:
            raise ValueError("explicit oracle result required")
        if expected["status"] == "error":
            if not re.fullmatch(r"[0-9A-Z]{5}", expected.get("sqlstate", "")) or not re.fullmatch(r"Sql\([A-Za-z]+\)", case.get("expected_core_error", "")):
                raise ValueError("negative case requires SQLSTATE and exact core error expectation")
        if not case.get("routines") and case.get("special_form") != "coalesce":
            raise ValueError("case must cover a routine or explicit special form")
        oracle_query(case)
        if expected["status"] == "ok" and (not isinstance(expected.get("columns"), list) or
                not isinstance(expected.get("rows"), list)):
            raise ValueError("successful expectation requires explicit columns and rows")
        if len(case["expected"].get("rows", [])) > 1:
            raise ValueError("initial scalar adapter supports at most one result row")
        for identity in case["routines"]:
            if identity not in identities:
                raise ValueError(f"unknown routine identity: {identity}")


def compare(expected, reference, candidate, expected_core_error=None):
    # Never treat matching infrastructure failures or matching unsupported errors as PASS.
    if canonical(reference) != canonical(expected):
        return "ORACLE_ERROR"
    if candidate.get("status") == "setup_error":
        return "SETUP_ERROR"
    if expected.get("status") == "error":
        return "PASS" if (expected_core_error and candidate == {
            "status": "error", "core_error": expected_core_error}) else "FAIL"
    return "PASS" if canonical(candidate) == canonical(reference) else "FAIL"


def coverage(inventory, cases, results):
    identities = validate_inventory(inventory)
    validate_cases(cases, identities)
    case_ids = {c["id"] for c in cases}
    if set(results) != case_ids:
        raise ValueError("missing or extra case results")
    if any(r not in {"PASS", "FAIL", "ORACLE_ERROR", "SETUP_ERROR", "UNTESTED"}
           for r in results.values()):
        raise ValueError("invalid verdict")
    ledger = []
    for identity in sorted(identities):
        linked = [c for c in cases if identity in c["routines"]]
        passing = {c["kind"] for c in linked if results[c["id"]] == "PASS"}
        status = "untested"
        if linked:
            status = "implemented-unverified" if all(results[c["id"]] == "PASS" for c in linked) else "failing"
        # These seed cases do not qualify legal contexts, effects, wire or persistence.
        # A future reviewed family qualification gate is required for 'verified'.
        ledger.append({"identity": identity, "status": status,
                       "case_ids": [c["id"] for c in linked],
                       "missing_case_kinds": sorted(REQUIRED_KINDS - passing)})
    return {"complete": False, "qualification": "initial embedded scalar probe only",
            "inventory_sha256": digest(inventory), "routines": ledger,
            "counts": {s: sum(r["status"] == s for r in ledger)
                       for s in ("untested", "implemented-unverified", "failing", "verified")}}



def parameter_literal(param):
    if not isinstance(param, dict) or set(param) != {"type", "value"}:
        raise ValueError("parameter requires exactly type and value")
    kind, value = param["type"], param["value"]
    if not isinstance(kind, str):
        raise ValueError("unsupported parameter type/value")
    if kind in {"numeric", "real", "double precision"}:
        if value is None:
            return f"CAST(NULL AS {kind})"
        if not isinstance(value, str):
            raise ValueError("lossless parameter must be a string")
        if kind == "numeric":
            if not re.fullmatch(r"[+-]?(?:(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|Infinity|NaN)", value):
                raise ValueError("invalid numeric parameter")
            text = value
        else:
            width, fmt = (8, ">f") if kind == "real" else (16, ">d")
            if not re.fullmatch(r"[0-9a-f]{"+str(width)+r"}", value):
                raise ValueError("invalid float bits")
            number = struct.unpack(fmt, bytes.fromhex(value))[0]
            text = "NaN" if math.isnan(number) else repr(number)
        return f"CAST('{text}' AS {kind})"
    widths = {"smallint": 16, "integer": 32, "bigint": 64}
    valid = (value is None or
             kind in {"smallint", "integer", "bigint"} and type(value) is int and
             -(2**(widths[kind]-1)) <= value < 2**(widths[kind]-1) or
             kind == "text" and isinstance(value, str) and "\x00" not in value or
             kind == "boolean" and type(value) is bool)
    if kind not in {"smallint", "integer", "bigint", "text", "boolean"} or not valid:
        raise ValueError("unsupported parameter type/value")
    literal = ("NULL" if value is None else
               "'" + value.replace("'", "''") + "'" if isinstance(value, str) else
               str(value).lower())
    return f"CAST({literal} AS {kind})"


def oracle_query(case):
    """Expand lexical bind tokens, with standard_conforming_strings=on.

    This trusted-fixture adapter is NOT PostgreSQL extended-protocol evidence.
    It validates lexical boundaries, not the SQL grammar or statement semantics.
    """
    query = case["query"]
    params = case.get("params", [])
    if not isinstance(query, str) or "\x00" in query or not isinstance(params, list):
        raise ValueError("invalid SQL fixture or parameter list")
    literals = [parameter_literal(p) for p in params]
    used, parts = set(), []
    i, size = 0, len(query)

    def ident_start(c):
        return c == "_" or c.isalpha() or ord(c) >= 128

    def ident_cont(c):
        return ident_start(c) or c in "0123456789$"

    def quoted(start, quote, escapes=False):
        j = start + 1
        while j < size:
            if escapes and query[j] == "\\":
                j += 2
            elif query[j] == quote:
                if j + 1 < size and query[j + 1] == quote:
                    j += 2
                else:
                    # PostgreSQL continues E strings across newline-separated
                    # quoted fragments, retaining backslash escape semantics.
                    if escapes:
                        gap = re.match(r"(?:\s|--[^\r\n]*)+", query[j + 1:])
                        if gap and any(c in gap[0] for c in "\r\n"):
                            next_quote = j + 1 + len(gap[0])
                            if next_quote < size and query[next_quote] == quote:
                                j = next_quote + 1
                                continue
                    return j + 1
            else:
                j += 1
        raise ValueError("unterminated SQL quote")

    while i < size:
        start = i
        if query.startswith("--", i):
            i += 2
            while i < size and query[i] not in "\r\n":
                i += 1
        elif query.startswith("/*", i):
            depth, i = 1, i + 2
            while i < size and depth:
                if query.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif query.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
            if depth:
                raise ValueError("unterminated SQL block comment")
        elif query.startswith("*/", i):
            raise ValueError("unmatched SQL block comment terminator")
        elif query[i] in "eE" and i + 1 < size and query[i + 1] == "'":
            i = quoted(i + 1, "'", escapes=True)
        elif query[i] in "'\"":
            i = quoted(i, query[i])
        elif ident_start(query[i]):
            i += 1
            while i < size and ident_cont(query[i]):
                i += 1
        elif query[i] == "$":
            delimiter = re.match(r"\$(?:[^\W\d][\w]*|)\$", query[i:], re.UNICODE)
            if delimiter:
                tag = delimiter[0]
                end = query.find(tag, i + len(tag))
                if end < 0:
                    raise ValueError("unterminated SQL dollar quote")
                i = end + len(tag)
            elif i + 1 < size and query[i + 1] in "0123456789":
                i += 1
                while i < size and query[i] in "0123456789":
                    i += 1
                if i < size and ident_cont(query[i]):
                    raise ValueError("malformed parameter token")
                index = int(query[start + 1:i])
                if index < 1 or index > len(literals):
                    raise ValueError("missing parameter binding")
                used.add(index)
                parts.append(literals[index - 1])
                continue
            else:
                raise ValueError("malformed SQL dollar token")
        else:
            i += 1
        parts.append(query[start:i])
    if used != set(range(1, len(params) + 1)):
        raise ValueError("unused parameter binding")
    return "".join(parts)


def decode_oracle(result):
    if result.returncode:
        if "__DD68_SETUP_OK__" not in result.stdout:
            return {"status": "setup_error", "diagnostic": result.stderr.strip()}
        match = re.search(r"ERROR:\s+([0-9A-Z]{5}):", result.stderr)
        if match:
            return {"status": "error", "sqlstate": match[1]}
        return {"status": "oracle_error", "diagnostic": result.stderr.strip()}
    payload = result.stdout.split("__DD68_SETUP_OK__", 1)[-1].strip()
    try:
        return json.loads(payload, parse_float=lambda v: {"numeric_decimal": str(Decimal(v))})
    except ValueError:
        return {"status": "oracle_error", "diagnostic": payload}


class Reference:
    def __enter__(self):
        self.container = require(run([
            "docker", "run", "--detach", "--rm", "--network", "none",
            "--tmpfs", "/var/lib/postgresql", "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
            "-e", "POSTGRES_INITDB_ARGS=--encoding=UTF8 --locale=C", IMAGE,
            "-c", "timezone=UTC", "-c", "listen_addresses=", "-c", "statement_timeout=10000",
        ], timeout=120))
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if run(["docker", "exec", self.container, "pg_isready", "-U", "postgres"], timeout=10).returncode == 0:
                    # initdb's temporary server can accept connections before final startup.
                    probe = run(["docker", "exec", self.container, "cat", "/proc/1/comm"])
                    if probe.returncode == 0 and probe.stdout.strip() == "postgres":
                        return self
                time.sleep(0.2)
            raise RuntimeError("owned PostgreSQL fixture did not become ready")
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_):
        require(run(["docker", "stop", "--time", "10", self.container], timeout=30))

    def sql(self, sql):
        return run(["docker", "exec", "-i", self.container, "psql", "-X", "-qAt",
                    "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1"], input=sql)

    def inventory(self):
        result = json.loads(require(self.sql((ROOT / "inventory.sql").read_text(encoding="utf-8"))))
        validate_inventory(result)
        return result

    def query(self, case):
        # Every psql invocation is a new session. BEGIN/ROLLBACK isolates seed writes.
        # Fixtures are checked-in trusted SQL, not user strings or network input.
        setup = "\n".join(s.rstrip(";") + ";" for s in case.get("reference_setup", case["setup"]))
        sql = "\\set VERBOSITY verbose\nSET standard_conforming_strings = on;\nBEGIN;\n" + setup + "\n\\echo __DD68_SETUP_OK__\nCREATE TEMP VIEW probe_result AS " + oracle_query(case).rstrip(";") + ";\n" + """
SELECT jsonb_build_object('status','ok',
  'columns',(SELECT jsonb_agg(jsonb_build_object('name',attname,
    'type',format_type(atttypid,atttypmod)) ORDER BY attnum)
    FROM pg_attribute WHERE attrelid='probe_result'::regclass AND attnum>0 AND NOT attisdropped),
  'rows',COALESCE((SELECT jsonb_agg((SELECT jsonb_agg(to_jsonb(t)->a.attname ORDER BY a.attnum)
    FROM pg_attribute a WHERE a.attrelid='probe_result'::regclass AND a.attnum>0 AND NOT a.attisdropped))
    FROM probe_result t),'[]'::jsonb));
ROLLBACK;
"""
        if case.get("exact_numeric"):
            # Generate typed expressions from actual PG result attributes. No float JSON hop.
            start = sql.index("SELECT jsonb_build_object('status','ok',")
            sql = sql[:start] + r"""
SELECT 'SELECT jsonb_build_object(''status'',''ok'',''columns'',(SELECT jsonb_agg(jsonb_build_object(''name'',attname,''type'',format_type(atttypid,atttypmod)) ORDER BY attnum) FROM pg_attribute WHERE attrelid=''probe_result''::regclass AND attnum>0 AND NOT attisdropped),''rows'',COALESCE((SELECT jsonb_agg(jsonb_build_array(' || string_agg(CASE atttypid
WHEN 'numeric'::regtype THEN format('jsonb_build_object(''numeric_text'',%I::text)',attname)
WHEN 'float4'::regtype THEN format('jsonb_build_object(''float4_bits'',encode(float4send(%I),''hex''))',attname)
WHEN 'float8'::regtype THEN format('jsonb_build_object(''float8_bits'',encode(float8send(%I),''hex''))',attname)
ELSE format('to_jsonb(%I)',attname) END,',' ORDER BY attnum) || ')) FROM probe_result),''[]''::jsonb));'
FROM pg_attribute WHERE attrelid='probe_result'::regclass AND attnum>0 AND NOT attisdropped
\gexec
ROLLBACK;
"""
        result = self.sql(sql)
        result = decode_oracle(result)
        if case.get("exact_numeric") and result.get("status") == "ok":
            for row in result["rows"]:
                for i, cell in enumerate(row):
                    if isinstance(cell, dict) and list(cell.values()) == [None]: row[i] = None
        return result


def write_new(path, value):
    check_outside_repo(path)
    with path.open("x", encoding="utf-8", newline="\n") as output:
        json.dump(value, output, ensure_ascii=False, sort_keys=True, indent=2)
        output.write("\n")


def load_cases(paths=None):
    """Explicit files replace the default seed corpus; repeated files combine."""
    cases = []
    for path in paths or [ROOT / "cases.json"]:
        cases.extend(json.loads(Path(path).read_text(encoding="utf-8")))
    inventory = json.loads(gzip.decompress((ROOT / "postgres-18.0.json.gz").read_bytes()))
    validate_cases(cases, validate_inventory(inventory))
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["capture", "check", "run"])
    parser.add_argument("--output", type=Path, required=True, help="new output directory")
    parser.add_argument("--probe", type=Path, help="built core function_probe executable")
    parser.add_argument("--cases", type=Path, action="append", help="corpus file; repeat to combine; default cases.json")
    args = parser.parse_args()
    if args.mode == "run" and (args.probe is None or not args.probe.is_file()):
        parser.error("run requires --probe pointing to a built function_probe executable")
    check_outside_repo(args.output)
    args.output.mkdir(parents=True, exist_ok=False)
    with Reference() as reference:
        inventory = reference.inventory()
        write_new(args.output / "inventory.json", inventory)
        if args.mode == "capture":
            write_new(args.output / "reference.json", {"image": IMAGE,
                      "inventory_sha256": digest(inventory), "server_version_num": 180000})
            return 0
        expected_inventory = json.loads(gzip.decompress((ROOT / "postgres-18.0.json.gz").read_bytes()))
        validate_pin(expected_inventory, json.loads((ROOT / "reference.json").read_text(encoding="utf-8")))
        if inventory != expected_inventory:
            raise RuntimeError("reference catalog drift; inspect captured inventory before changing baseline")
        if args.mode == "check":
            return 0
        cases = load_cases(args.cases)
        validate_cases(cases, validate_inventory(inventory))
        results, details = {}, []
        for index, case in enumerate(cases):
            oracle = reference.query(case)
            probe = run([str(args.probe.resolve()), str((args.output / f"case-{index}.db").resolve())],
                        input=json.dumps({"setup": case["setup"], "query": case["query"], "exact_numeric": case.get("exact_numeric", False), "params": [p if p["type"] in {"numeric", "real", "double precision"} and p["value"] is not None else p["value"] for p in case.get("params", [])]}))
            try:
                candidate = json.loads(require(probe))
            except (ValueError, RuntimeError) as error:
                candidate = {"status": "probe_error", "diagnostic": str(error)}
            verdict = compare(case["expected"], oracle, candidate, case.get("expected_core_error"))
            if case.get("expectedUnsupported") and candidate.get("status") != "ok":
                verdict = "FAIL"  # Missing SQL foundation never qualifies a negative oracle case.
            results[case["id"]] = verdict
            details.append({"id": case["id"], "verdict": verdict,
                            "reference": oracle, "candidate": candidate, "native_exit": probe.returncode})
        report = coverage(inventory, cases, results)
        report.update({"cases": details, "reference_image": IMAGE,
                       "corpus_sha256": digest(cases),
                       "corpus_files": [str(p.resolve()) for p in args.cases or [ROOT / "cases.json"]],
                       "build_source_correspondence": "unverified: source hashes at run do not prove executable build inputs",
                       "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                       "source_hashes_at_run": {str(p.relative_to(ROOT.parent.parent)): hashlib.sha256(p.read_bytes()).hexdigest()
                           for p in sorted((ROOT.parent.parent / "crates/giggles-core/src").rglob("*.rs"))},
                       "probe_source_sha256_at_run": hashlib.sha256((ROOT.parent.parent / "crates/giggles-core/examples/function_probe.rs").read_bytes()).hexdigest(),
                       "special_forms": {name: [{"id": c["id"], "verdict": results[c["id"]]} for c in cases if c.get("special_form") == name] for name in sorted({"coalesce"} | {c["special_form"] for c in cases if c.get("special_form")})},
                       "probe_sha256": hashlib.sha256(args.probe.read_bytes()).hexdigest()})
        write_new(args.output / "report.json", report)
        print(f"Report: {args.output / 'report.json'}; full qualification: incomplete")
        return 1  # This initial harness never certifies full compatibility.


if __name__ == "__main__":
    raise SystemExit(main())
