#!/usr/bin/env python3
"""Check refreshed Tokenize defaults using an access session supplied via PG*."""
import logging
import os
import re
import sys


GENERATED_TOKEN = re.compile(r"tok_(f[a-z2-7]{10})_([a-z2-7]+)\Z")
TOKEN = re.compile(r"tok_(.+)_([a-z2-7]+)\Z")


def main():
    """Print only step outcomes and totals, never database values or diagnostics."""
    logging.disable(logging.CRITICAL)
    try:
        import psycopg
    except Exception:
        print("FAIL step 1: driver setup", flush=True)
        print("0 passed; 1 failed", flush=True)
        return 1
    passed = failed = 0
    connection = None
    token = None
    declared = set(os.environ.get("NUNYA_TEST_DEFAULTS_DECLARED_DOMAINS", "email").split(","))

    def step(name, action):
        nonlocal passed, failed
        try:
            ok = bool(action())
        except Exception:
            ok = False
        print(f'{"PASS" if ok else "FAIL"} step {passed + failed + 1}: {name}', flush=True)
        passed += int(ok)
        failed += int(not ok)

    def query(sql):
        with connection.cursor() as cursor:
            cursor.execute(sql, prepare=True)
            return cursor.fetchall()

    def direct_token():
        nonlocal connection, token
        connection = psycopg.connect(autocommit=True, connect_timeout=10)
        rows = query("SELECT recovery_email, email FROM customers WHERE id = 1")
        if len(rows) != 1 or len(rows[0]) != 2:
            return False
        recovery, email = rows[0]
        generated = GENERATED_TOKEN.fullmatch(recovery) if isinstance(recovery, str) else None
        email_token = TOKEN.fullmatch(email) if isinstance(email, str) else None
        if not generated or not email_token:
            return False
        if generated.group(1) in declared or generated.group(1) == email_token.group(1):
            return False
        token = recovery
        return True

    def star_token():
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM customers WHERE id = 1", prepare=True)
            rows = cursor.fetchall()
            names = [column.name for column in cursor.description]
            return (token is not None and len(rows) == 1 and
                    names.count("recovery_email") == 1 and
                    rows[0][names.index("recovery_email")] == token)

    def refused(sql):
        try:
            query(sql)
        except psycopg.Error as error:
            return error.sqlstate == "NY005"
        return False

    def reconnect():
        nonlocal connection
        if connection is not None:
            connection.close()
            connection = None
        connection = psycopg.connect(autocommit=True, connect_timeout=10)
        return token is not None and query(
            "SELECT recovery_email FROM customers WHERE id = 1") == [(token,)]

    try:
        step("direct generated token has distinct domain", direct_token)
        step("star read returns identical token", star_token)
        step("tier is SQL NULL", lambda: query(
            "SELECT tier FROM customers WHERE id = 1") == [(None,)])
        step("active is SQL NULL", lambda: query(
            "SELECT active FROM customers WHERE id = 1") == [(None,)])
        step("kind is SQL NULL", lambda: query(
            "SELECT kind FROM customers WHERE id = 1") == [(None,)])
        step("tier WHERE refused NY005", lambda: refused(
            "SELECT id FROM customers WHERE tier = 1"))
        step("active WHERE refused NY005", lambda: refused(
            "SELECT id FROM customers WHERE active"))
        step("kind WHERE refused NY005", lambda: refused(
            "SELECT id FROM customers WHERE kind = 'x'"))
        step("reconnect in same access session preserves token", reconnect)
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
    print(f"{passed} passed; {failed} failed", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
