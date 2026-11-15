#!/usr/bin/env python3
"""psycopg 3 worked-example smoke check; use a freshly seeded harness.

Usage: python psycopg_check.py --dsn postgresql://127.0.0.1:6432/app
       --task-id ID --secret SECRET
No returned values, tokens, credentials, or server error messages are printed.
"""
import argparse
import sys

import psycopg


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--task-id', required=True)
    parser.add_argument('--secret', required=True)
    args = parser.parse_args()
    failures = 0
    token = None

    def step(number, name, action):
        nonlocal failures
        try:
            ok = action()
        except Exception:
            ok = False
        print(f'{"PASS" if ok else "FAIL"} step {number}: {name}')
        failures += not ok

    try:
        with psycopg.connect(args.dsn, user=f'task:{args.task_id}',
                             password=args.secret, autocommit=True) as conn:
            def query(sql, params=None):
                # Text parameters/results exercise extended protocol, without
                # relying on binary codecs for the token's apparent SQL type.
                with conn.cursor() as cur:
                    cur.execute(sql, params, prepare=True)
                    return cur.fetchall() if cur.description else cur.rowcount

            def select_tokens():
                nonlocal token
                rows = query('SELECT id, email FROM customers WHERE failed_payments >= 2 ORDER BY id')
                if [r[0] for r in rows] != [3, 5] or not all(
                        isinstance(r[1], str) and r[1].startswith('tok_email_') for r in rows):
                    return False
                token = rows[0][1]
                return True

            def refused(sql, params, state):
                try:
                    query(sql, params)
                except psycopg.Error as error:
                    return error.sqlstate == state
                return False

            step(1, 'read blind emails as tokens', select_tokens)
            step(2, 'write token to same domain', lambda: token is not None and query(
                "INSERT INTO outbox (to_address, template) VALUES (%s, 'reminder')", (token,)) == 1)
            step(3, 'read same-domain token unchanged', lambda: token is not None and
                 (token,) in query('SELECT to_address FROM outbox'))
            step(4, 'refuse token into plain notes (NY001)', lambda: token is not None and refused(
                'UPDATE notes SET body = %s WHERE id = 3', (token,), 'NY001'))
            step(5, 'refuse blind LIKE (NY002)', lambda: refused(
                "SELECT id FROM customers WHERE email LIKE 'a%'", None, 'NY002'))
            # country is plain in harness/policy.toml; id 17 is absent in seed.
            step(6, 'plain country query for absent id', lambda: query(
                'SELECT country FROM customers WHERE id = 17') == [])
            step(7, 'mask phone output', lambda: query(
                'SELECT phone FROM customers WHERE id = 3') == [('[hidden]',)])
    except Exception:
        print('FAIL connection/setup (details suppressed)')
        return 1
    print(f'{7 - failures} passed; {failures} failed')
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
