"""Reuse JDBC's de-cascaded observations and GD0048's strict record protocol.

See ../jdbc/JdbcCompatProbe.java and crates/giggles-pg-conformance/src/lib.rs.
Only stable values, mapped PostgreSQL types and SQLSTATEs enter the records.
"""
import base64
import datetime as dt
import decimal
import json
import os
import re
import uuid

import psycopg


class ProbeAssertion(Exception):
    def __init__(self, reason):
        if not re.fullmatch(r"[a-z0-9_]+", reason):
            reason = "invalid_assertion_reason"
        self.reason = reason
        super().__init__(reason)


def require(condition, reason):
    if not condition:
        raise ProbeAssertion(reason)


def canonical(value):
    if isinstance(value, (bytes, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (decimal.Decimal, uuid.UUID)):
        return str(value)
    if isinstance(value, dt.datetime):
        return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, (list, tuple)):
        return [canonical(v) for v in value]
    if isinstance(value, dict):
        return {k: canonical(v) for k, v in value.items()}
    return value


def error(exc):
    result = {"sqlstate": getattr(exc, "sqlstate", None),
              "exception_class": type(exc).__module__ + "." + type(exc).__name__}
    if isinstance(exc, ProbeAssertion):
        result["reason"] = exc.reason
    return result


def observe(step):
    try:
        data = canonical(step())
        json.dumps(data, allow_nan=False)
        return {"status": "OK", "data": data}
    except Exception as exc:
        return {"status": "ERR", **error(exc)}


def connect():
    return psycopg.connect(host=os.getenv("PGHOST", "localhost"),
                           port=os.getenv("PGPORT", "5432"),
                           user=os.getenv("PGUSER", "postgres"),
                           dbname=os.getenv("PGDATABASE", "postgres"),
                           password=os.getenv("PGPASSWORD", ""),
                           application_name=os.getenv("PGAPPNAME", ""),
                           sslmode="disable", connect_timeout=10, autocommit=True)


TYPES = {16: "bool", 17: "bytea", 20: "int8", 21: "int2", 23: "int4",
         25: "text", 700: "float4", 701: "float8", 1043: "varchar",
         1700: "numeric", 1184: "timestamptz", 2950: "uuid"}


def fields(cur):
    return [{"name": f.name, "type": TYPES.get(f.type_code, "unknown")}
            for f in cur.description]


def rows(c, sql, params=None, **kwargs):
    with c.cursor() as cur:
        cur.execute(sql, params, **kwargs)
        return {"rows": cur.fetchall(), "fields": fields(cur)}


def checked_rows(c, sql, expected, params=None, **kwargs):
    result = rows(c, sql, params, **kwargs)
    require(canonical(result["rows"]) == canonical(expected), "rows_mismatch")
    return result


def execute(c, sql, params=None):
    with c.cursor() as cur:
        cur.execute(sql, params)
        count = cur.rowcount
        require(count == 1, "rowcount_mismatch")
        return {"rowcount": count}


def point(c, id, expected=None, **kwargs):
    result = rows(c, f"SELECT id,name,score FROM {TABLE} WHERE id=%s", (id,), **kwargs)
    require(result["fields"] == [{"name": "id", "type": "int4"},
                                {"name": "name", "type": "text"},
                                {"name": "score", "type": "int8"}], "point_fields_mismatch")
    if expected is not None:
        require(result["rows"] == [expected], "point_values_mismatch")
    return result


def insert(c, id):
    return execute(c, f"INSERT INTO {TABLE} (id,name) VALUES (%s,%s)", (id, f"tx{id}"))


def identity(c):
    result = rows(c, "SELECT current_user,current_database()")
    require(result["rows"] == [(os.getenv("PGUSER", "postgres"),
                                os.getenv("PGDATABASE", "postgres"))], "identity_mismatch")
    return result


def session(c):
    def version():
        positive = c.info.server_version > 0
        require(positive, "server_version_nonpositive")
        return positive
    def statuses():
        result = {k: c.info.parameter_status(k) for k in
                  ("server_encoding", "client_encoding", "integer_datetimes")}
        require(all(isinstance(v, str) and v for v in result.values()), "parameter_status_missing")
        return result
    return {"identity": observe(lambda: identity(c)),
            "server_version_positive": observe(version), "parameter_status": observe(statuses)}


def create(c):
    c.execute(f"CREATE TABLE {TABLE} (id integer PRIMARY KEY,name text,score bigint)")
    return {"created": True}


def repeat(c):
    results = [point(c, 1, (1, "a", 20), prepare=True) for _ in range(7)]
    require(all(r == results[0] for r in results), "repeat_mismatch")
    return {"executions": len(results), "identical": True, "row": results[-1]}


def update_delete(c):
    update = observe(lambda: execute(c, f"UPDATE {TABLE} SET score=score+%s WHERE id=%s", (5, 1)))
    delete = observe(lambda: execute(c, f"DELETE FROM {TABLE} WHERE id=%s", (3,)))
    def read():
        result = point(c, 1)
        require(len(result["rows"]) == 1, "updated_row_missing")
        actual = result["rows"][0]
        require(actual[:2] == (1, "a"), "updated_identity_mismatch")
        if update["status"] == "OK":
            require(actual[2] == 25, "updated_score_mismatch")
        return result
    return {"update": update, "delete": delete, "point": observe(read)}


def transaction(c, commit):
    c.autocommit = False
    id = 10 if commit else 11
    result = {"insert": observe(lambda: insert(c, id))}
    result["commit" if commit else "rollback"] = observe(lambda: (c.commit() if commit else c.rollback()) or True)
    def read():
        with connect() as other:
            return checked_rows(other, f"SELECT id,name FROM {TABLE} WHERE id=%s",
                                [(10, "tx10")] if commit else [], (id,))
    result["other_connection"] = observe(read)
    return result


def expected_state(c, sql, state):
    try:
        c.execute(sql)
    except psycopg.Error as exc:
        if exc.sqlstate != state:
            raise
        return {"sqlstate": exc.sqlstate}
    raise ProbeAssertion("expected_error_missing")


def recovery(c):
    c.autocommit = False
    return {"duplicate": observe(lambda: expected_state(c, f"INSERT INTO {TABLE} (id) VALUES (1)", "23505")),
            "aborted": observe(lambda: expected_state(c, "SELECT 1", "25P02")),
            "rollback": observe(lambda: c.rollback() or True),
            "recovered": observe(lambda: checked_rows(c, "SELECT 1", [(1,)]))}


class RollbackInner(Exception):
    pass


def savepoint(c):
    result = {}
    def run():
        with c.transaction():
            result["insert_a"] = observe(lambda: insert(c, 20))
            def inner():
                try:
                    with c.transaction():
                        result["insert_b"] = observe(lambda: insert(c, 21))
                        raise RollbackInner()
                except RollbackInner:
                    return {"rolled_back": True}
            result["inner"] = observe(inner)
        return {"committed": True}
    result["transaction"] = observe(run)
    result["recovery"] = observe(lambda: c.rollback() or True)
    result["point_a"] = observe(lambda: checked_rows(c, f"SELECT id FROM {TABLE} WHERE id=%s", [(20,)], (20,)))
    result["point_b"] = observe(lambda: checked_rows(c, f"SELECT id FROM {TABLE} WHERE id=%s", [], (21,)))
    return result


def types_roundtrip(c):
    specs = [("boolean", "boolean", True), ("bytea", "bytea", b"\x00\x01\xff"),
             ("numeric", "numeric(10,2)", decimal.Decimal("12.30")),
             ("smallint", "smallint", -12), ("double", "double precision", 1.25),
             ("varchar", "varchar(20)", "text"),
             ("timestamptz", "timestamptz", dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)),
             ("uuid", "uuid", uuid.UUID("123e4567-e89b-12d3-a456-426614174000"))]
    result = {}
    for name, declaration, value in specs:
        table = PREFIX + "_t_" + name
        steps = {}
        def create_type():
            c.execute(f"CREATE TABLE {table} (id integer PRIMARY KEY,v {declaration})")
            return {"created": True}
        steps["create"] = observe(create_type)
        if steps["create"]["status"] == "OK":
            for id, val in ((1, value), (2, None)):
                steps[f"insert_{id}"] = observe(lambda: execute(c, f"INSERT INTO {table} (id,v) VALUES (%s,%s)", (id, val)))
                steps[f"point_{id}"] = observe(lambda: checked_rows(c, f"SELECT id,v FROM {table} WHERE id=%s", [(id, val)], (id,)))
        result[name] = {"status": "OK" if all(v["status"] == "OK" for v in steps.values()) else "ERR", "steps": steps}
    return result


def scalar(c, sql, expected, params=None):
    return checked_rows(c, sql, [(expected,)], params)


def copy_rows(c):
    result = {}
    def write():
        with c.cursor() as cur:
            with cur.copy(f"COPY {TABLE} (id,name,score) FROM STDIN") as copy:
                copy.write_row((30, "copy30", 30))
                copy.write_row((31, "copy31", None))
        return {"rows_written": 2}
    result["copy"] = observe(write)
    result["point_30"] = observe(lambda: point(c, 30, (30, "copy30", 30)))
    result["point_31"] = observe(lambda: point(c, 31, (31, "copy31", None)))
    return result


def client_cursor(c):
    with psycopg.ClientCursor(c) as cur:
        cur.execute(f"SELECT id,name,score FROM {TABLE} WHERE id=%s", (2,))
        result = {"rows": cur.fetchall(), "fields": fields(cur)}
        require(result["rows"] == [(2, "b", None)], "client_cursor_values_mismatch")
        return result


def server_cursor(c):
    with c.transaction():
        with c.cursor(name="probe_point") as cur:
            cur.execute(f"SELECT id,name,score FROM {TABLE} WHERE id=%s", (2,))
            row = cur.fetchone()
            result = {"row": row, "fields": fields(cur)}
            require(row == (2, "b", None), "server_cursor_values_mismatch")
            require(cur.fetchone() is None, "server_cursor_extra_row")
            return result


def introspection(c):
    return checked_rows(c, "SELECT column_name,data_type,is_nullable FROM information_schema.columns "
                        f"WHERE table_name='{TABLE}' ORDER BY ordinal_position",
                        [("id", "integer", "NO"), ("name", "text", "YES"), ("score", "bigint", "YES")])


def test(id, check):
    c = None
    try:
        c = connect()
        try:
            data = canonical(check(c))
        finally:
            c.close()
        payload = json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False)
        status = "OK"
    except Exception as exc:
        payload = json.dumps(error(exc), sort_keys=True, separators=(",", ":"))
        status = "ERR"
    encoded = base64.b64encode(payload.encode("utf-8")).decode("ascii")
    print(f"CASE\t{id}\t{status}\t{encoded}", flush=True)


require(psycopg.__version__ == "3.2.9", "driver_version_mismatch")
PREFIX = os.getenv("R0_PREFIX", "r0d_psycopg")
require(re.fullmatch(r"[a-z][a-z0-9_]{0,47}", PREFIX) is not None, "unsafe_prefix")
TABLE = PREFIX + "_items"
print(f"VERSION\tpsycopg\t3.2.9\t{psycopg.pq.version()}", flush=True)
CASES = [
    ("connect", session),
    ("keepalive", lambda c: {"statement": observe(lambda: scalar(c, "SELECT 1", 1)),
                             "prepared": observe(lambda: checked_rows(c, "SELECT 1", [(1,)], prepare=True))}),
    ("ddl_create", create),
    ("prepared_insert", lambda c: {f"row_{id}": observe(lambda id=id, name=name, score=score: execute(c, f"INSERT INTO {TABLE} (id,name,score) VALUES (%s,%s,%s)", (id, name, score))) for id, name, score in [(1, "a", 20), (2, "b", None), (3, "c", 20)]}),
    ("prepared_point_select", lambda c: {"row": observe(lambda: point(c, 1, (1, "a", 20))), "null_row": observe(lambda: point(c, 2, (2, "b", None)))}),
    ("prepared_repeat", repeat),
    ("select_ordered", lambda c: checked_rows(c, f"SELECT id,name FROM {TABLE} ORDER BY id", [(1, "a"), (2, "b"), (3, "c")])),
    ("update_delete_params", update_delete),
    ("tx_commit", lambda c: transaction(c, True)),
    ("tx_rollback", lambda c: transaction(c, False)),
    ("tx_error_recovery", recovery),
    ("savepoint", savepoint),
    ("types_roundtrip", types_roundtrip),
    ("param_bytea_position", lambda c: {"cast": observe(lambda: scalar(c, "SELECT position(%s::bytea IN %s::bytea)", 2, (b"\x02", b"\x01\x02\x03"))), "uncast": observe(lambda: scalar(c, "SELECT position(%s IN %s)", 2, (b"\x02", b"\x01\x02\x03")))}),
    ("param_no_from_aggregate", lambda c: {"count": observe(lambda: scalar(c, "SELECT count(*)", 1)), "max": observe(lambda: scalar(c, "SELECT max(%s)", 7, (7,)))}),
    ("copy_from_stdin", copy_rows),
    ("client_cursor_simple_protocol", client_cursor),
    ("introspection_information_schema", introspection),
    ("server_side_cursor", server_cursor),
]
for case_id, check in CASES:
    test(case_id, check)
print(f"DONE\t{len(CASES)}", flush=True)
