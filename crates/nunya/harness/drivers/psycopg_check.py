#!/usr/bin/env python3
"""Psycopg session checks. Connection settings come only from PG* variables."""
import argparse
import logging
import re
import sys

import psycopg
from psycopg_pool import ConnectionPool


def query(conn, sql, params=None):
    """Exercise prepared queries using text parameters and results."""
    with conn.cursor() as cur:
        cur.execute(sql, params, prepare=True)
        return cur.fetchall() if cur.description else cur.rowcount


def main():
    """Run the selected scenario without printing database data."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('main', 'expiry'), required=True)
    args = parser.parse_args()
    # Pool diagnostics can contain credentials and server messages.
    logging.disable(logging.CRITICAL)
    passed = failures = 0
    token = None
    conn_c = None

    def step(name, action):
        nonlocal passed, failures
        detail = ''
        try:
            result = action()
            if isinstance(result, tuple):
                ok, detail = result
            else:
                ok = result
        except Exception:
            ok = False
        print(f'{"PASS" if ok else "FAIL"} step {passed + failures + 1}: {name}{detail}', flush=True)
        passed += bool(ok)
        failures += not ok

    def wait(event):
        print(f'WAIT {event}', flush=True)
        return sys.stdin.readline().strip() == 'GO'

    def refused(action, expected):
        try:
            action()
        except psycopg.Error as error:
            state = error.sqlstate
            if state and re.fullmatch('[A-Z0-9]{5}', state):
                return state == expected, f' (observed SQLSTATE {state})'
            # libpq omits SQLSTATE on connection refusal but preserves NY003's fixed message.
            if (state is None and expected == 'NY003' and
                    isinstance(error, psycopg.OperationalError) and
                    'ERROR:  token not valid in this session' in str(error)):
                return True, ' (observed NY003 fixed message)'
            return False, ' (other refusal)'
        return False, ' (unexpected success)'

    def open_c():
        nonlocal conn_c
        conn_c = psycopg.connect(autocommit=True, connect_timeout=10)
        return query(conn_c, 'SELECT country FROM customers WHERE id = 3') == [('US',)]

    def end_session(event):
        ready = wait(event)
        step(f'{event} ends open connection (NY011)', lambda: ready and refused(
            lambda: query(conn_c, 'SELECT country FROM customers WHERE id = 3'), 'NY011'))

        def new_connection():
            with psycopg.connect(autocommit=True, connect_timeout=10):
                pass

        step(f'{event} refuses new connection (NY003 expected)', lambda: ready and refused(
            new_connection, 'NY003'))

    if args.mode == 'expiry':
        step('open expiry connection', open_c)
        end_session('expiry')
    else:
        with ConnectionPool(min_size=2, max_size=2, timeout=30,
                            kwargs={'autocommit': True, 'connect_timeout': 10}) as pool:
            def pooled_query(sql, params=None):
                with pool.connection() as conn:
                    return query(conn, sql, params)

            def select_tokens():
                nonlocal token
                rows = pooled_query(
                    'SELECT id, email FROM customers WHERE failed_payments >= 2 ORDER BY id')
                if [r[0] for r in rows] != [3, 5] or not all(
                        isinstance(r[1], str) and r[1].startswith('tok_email_') for r in rows):
                    return False
                token = rows[0][1]
                return True

            step('read blind emails as tokens', select_tokens)
            step('write token to same domain', lambda: token is not None and pooled_query(
                "INSERT INTO outbox (to_address, template) VALUES (%s, 'reminder')", (token,)) == 1)
            step('read same-domain token unchanged', lambda: token is not None and
                 (token,) in pooled_query('SELECT to_address FROM outbox'))
            step('refuse token into plain notes (NY001)', lambda: token is not None and refused(
                lambda: pooled_query('UPDATE notes SET body = %s WHERE id = 3', (token,)), 'NY001'))
            step('refuse blind LIKE (NY002)', lambda: refused(
                lambda: pooled_query("SELECT id FROM customers WHERE email LIKE 'a%'"), 'NY002'))
            step('plain country query for absent id', lambda: pooled_query(
                'SELECT country FROM customers WHERE id = 17') == [])
            step('mask phone output', lambda: pooled_query(
                'SELECT phone FROM customers WHERE id = 3') == [('[hidden]',)])

            def round_trip():
                nonlocal token
                with pool.connection() as conn_a:
                    rows = query(conn_a, 'SELECT email FROM customers WHERE id = 3')
                    if len(rows) != 1 or not isinstance(rows[0][0], str) or not rows[0][0].startswith('tok_email_'):
                        return False
                    token = rows[0][0]
                    with pool.connection() as conn_b:
                        inserted = query(conn_b,
                            "INSERT INTO outbox (to_address, template) VALUES (%s, 'pooled')", (token,))
                        rows = query(conn_b,
                            'SELECT to_address FROM outbox WHERE to_address = %s', (token,))
                        return conn_a is not conn_b and inserted == 1 and bool(rows) and all(
                            row == (token,) for row in rows)

            step('pooled token round trip on distinct connections', round_trip)

            def restart():
                if not wait('restart'):
                    return False
                pool.check()
                return token is not None and pooled_query(
                    'SELECT email FROM customers WHERE id = 3') == [(token,)]

            step('same token after gateway restart and pool recovery', restart)
            step('open revocation connection', open_c)
            end_session('revoke')
    if conn_c is not None:
        conn_c.close()
    print(f'{passed} passed; {failures} failed', flush=True)
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
