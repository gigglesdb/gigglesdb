#!/usr/bin/env python3
"""Revision stream handshake; stdout contains only fixed outcomes."""
import logging
import os
import sys
import psycopg

logging.disable(logging.CRITICAL)
def main():
    count = 0
    activation = None
    rows = None
    scenario = os.environ['FENCING_SCENARIO']
    try:
        with psycopg.connect(autocommit=True) as conn:
            with conn.cursor() as cur:
                rows = cur.stream('SELECT id, secret FROM ' + os.environ['FENCING_TABLE'] + ' ORDER BY id', size=1)
                for row in rows:
                    count += 1
                    if scenario == 'loosen' and row[1] != 'MASKED':
                        raise RuntimeError('old plan')
                    if count == 100:
                        print('READY', flush=True)
                        handshake = sys.stdin.readline().strip().split()
                        activation = handshake[1] if len(handshake) == 2 else None
                        if not handshake or handshake[0] != 'CONTINUE':
                            return 1
        ok = scenario != 'tighten' and count == 50000
    except Exception as error:
        print('CODE psycopg ' + type(error).__name__ + ' ' + str(getattr(error, 'sqlstate', None)), flush=True)
        after = count - 100
        print('AFTER_APPLY psycopg rows=' + str(after) + ' remaining=49900', flush=True)
        ok = (scenario == 'tighten' and getattr(error, 'sqlstate', None) == 'NY008'
              and activation in ('active', 'pending') and 0 <= after < 4990
              and (activation != 'active' or after == 0))
        if ok:
            try:
                next(rows)
                ok = False
            except StopIteration:
                pass
            except Exception:
                ok = False
    print(('PASS' if ok else 'FAIL') + ' psycopg-stream-' + scenario, flush=True)
    return 0 if ok else 1

if __name__ == '__main__':
    sys.exit(main())
