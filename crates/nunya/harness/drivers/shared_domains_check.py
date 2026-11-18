"""Shared-domain live checks; output contains fixed labels and counts only."""
import os, sys
import psycopg
passed = 0
def check(label, result):
    global passed
    print(('PASS ' if result else 'FAIL ') + label, flush=True)
    if not result:
        print(f'SUMMARY psycopg passed={passed} failed=1', flush=True)
        sys.exit(1)
    passed += 1
def connect(user, password):
    return psycopg.connect(host=os.environ['PGHOST'], port=os.environ['PGPORT'], dbname='app', user=user, password=password, sslmode='disable', autocommit=True)
def refused(c, sql, params=None):
    try:
        c.execute(sql, params).fetchall()
        return False
    except psycopg.Error as e:
        return bool(e.sqlstate and e.sqlstate.startswith('NY') and 'example.test' not in str(e))
try:
    c = connect(os.environ['PGUSER'], os.environ['PGPASSWORD'])
    old = c.execute('SELECT email FROM customers WHERE id=1').fetchone()[0]
    out = c.execute('SELECT to_address FROM outbox WHERE id=2').fetchone()[0]
    check('before-distinct', old != out)
    join = 'SELECT customers.id FROM customers JOIN outbox ON customers.email=outbox.to_address'
    check('before-join', refused(c, join))
    check('before-write', refused(c, 'INSERT INTO outbox(id,to_address) SELECT id,email FROM customers'))
    print('READY', flush=True)
    user = sys.stdin.readline().strip()
    password = sys.stdin.readline().strip()
    try:
        c.execute('SELECT id FROM customers').fetchall()
        reset = False
        e_code = None
    except psycopg.Error as e:
        e_code = e.sqlstate
        reset = e.sqlstate == 'NY011' or (e.sqlstate is None and c.broken)
    check('reset-NY011-or-closed', reset)
    check('reset-form-NY011' if e_code == 'NY011' else 'reset-form-closed', reset)
    n = connect(user, password)
    check('linked-equal', n.execute('SELECT email FROM customers WHERE id=1').fetchone()[0] == n.execute('SELECT to_address FROM outbox WHERE id=2').fetchone()[0])
    check('linked-join', len(n.execute(join).fetchall()) >= 1)
    check('old-literal', refused(n, "SELECT id FROM customers WHERE email='" + old + "'"))
    check('old-parameter', refused(n, 'SELECT id FROM customers WHERE email=%s', (old,)))
    n.close()
    c.close()
    print(f'SUMMARY psycopg passed={passed} failed=0', flush=True)
except Exception:
    print(f'FAIL driver-operation; SUMMARY psycopg passed={passed} failed=1', flush=True)
    sys.exit(1)
