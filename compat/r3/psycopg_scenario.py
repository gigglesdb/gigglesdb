"""Psycopg 3.2.9 implementation of the r3_safeguards driver protocol."""
import sys
sys.dont_write_bytecode = True
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import argparse
import concurrent.futures
import json
import threading
import time
import psycopg
from psycopg import ClientCursor
from r3_safeguards import STEPS, SUITE_STEPS, parse_spec

ERRORS = {
    'required': ('42T01', 'approval required: preview the change and have it approved'),
    'stale': ('55T01', 'approval is stale: preview the change again, do not retry blindly'),
    'conflict': ('42T03', 'operation id already used for a different request'),
    'expired': ('28T02', 'task authorization expired'),
    'transactions': ('0A000', 'explicit transactions are not allowed in a task session'),
    'transitions': ('54T01', 'budget exceeded: row transitions made by this task'),
    'scanned': ('54T01', 'budget exceeded: rows scanned by this statement'),
    'undo': ('42501', 'denied: complete undo is not possible because later changes conflict'),
}
TRANSIENT = {'task budget exceeded', 'budget exceeded: concurrent statements of this task',
             'budget exceeded: load of all agent sessions'}
REQUEUE = "UPDATE shipments SET status = 'queued' WHERE status = 'failed'"


def require(value, detail):
    if not value:
        raise AssertionError(detail)


def requeue(op, rows):
    return f"OPERATION '{op}' {REQUEUE} AND {rows}"


class Scenario:
    def __init__(self, spec):
        self.spec = spec
        self.secrets = json.loads(Path(spec['secret_file']).read_text(encoding='utf-8'))
        self.output = {'steps': [], 'deviations': [
            'Step 13: app INSERT occurs after approval and before APPLY; no trusted in-process pause seam.',
            'Steps 19-20 use real expiry and no next-day clock advance.',
            'Admin receipt internals and change-summary bytes are checked only through SQL-visible fields; no Admin API is used.']}
        self.connections = []
        self.attempts = 1
        self.local = threading.local()

    def save(self):
        path = Path(self.spec['results_path'])
        temporary = path.with_suffix('.tmp')
        temporary.write_text(json.dumps(self.output, indent=2, default=str), encoding='utf-8', newline='\n')
        temporary.replace(path)

    def connect(self, role='repair_agent', task='inc-436', pg=False):
        agent = role == 'repair_agent' and not pg
        kwargs = dict(host='localhost', port=self.spec['agent_port' if agent else 'pg_port'],
                      dbname=self.spec['database'], user=role, autocommit=True,
                      connect_timeout=10, cursor_factory=ClientCursor)
        if agent:
            kwargs.update(password=self.secrets[task], sslmode='verify-full',
                          sslrootcert=self.spec['ca'], options=f'-c giggles.task={task}')
        else:
            kwargs.update(password=self.secrets['owner_password'] if role == 'postgres'
                          else self.spec['passwords'][role], sslmode='disable')
        if self.spec.get('suite') == 'exit':
            # Listeners bind IPv4; keep localhost for TLS name verification.
            kwargs['hostaddr'] = '127.0.0.1'
        c = psycopg.connect(**kwargs)
        self.connections.append(c)
        return c

    def q(self, c, sql, race=False):
        for attempt in range(1, 51):
            self.local.attempts = max(getattr(self.local, 'attempts', 1), attempt)
            try:
                with c.cursor() as cur:
                    cur.execute(sql, prepare=False)
                    cols = [d.name for d in cur.description] if cur.description else []
                    rows = cur.fetchall() if cols else []
                    return {'columns': cols, 'rows': rows, 'count': cur.rowcount}
            except psycopg.Error as e:
                if attempt < 50 and (e.sqlstate == '40001' or
                        (race and e.sqlstate == '54T01' and e.diag.message_primary in TRANSIENT)):
                    time.sleep(.005*min(attempt, 20))
                    continue
                raise

    def fail(self, c, sql, expected):
        try:
            self.q(c, sql)
        except psycopg.Error as e:
            got = (e.sqlstate, e.diag.message_primary)
            require(got == ERRORS[expected], f'{expected}: expected {ERRORS[expected]}, got {got}')
            return
        raise AssertionError('expected ' + expected + ', statement succeeded')

    def lookup(self, op, c=None):
        return self.q(c or self.agent, f"SHOW OPERATION '{op}'")

    def fields(self, reply):
        require(len(reply['rows']) == 1, 'expected one result row: '+str(reply))
        return dict(zip(reply['columns'], reply['rows'][0]))

    def preview(self, filter_, n):
        r = self.q(self.agent, 'PREVIEW '+REQUEUE+' AND '+filter_)
        require(r['columns'] == ['change_id', 'digest', 'row_count', 'expires_at'], str(r))
        row = r['rows'][0]
        require(int(row[2]) == n and len(row[1]) == 64 and all(c in '0123456789abcdef' for c in row[1]), str(row))
        return row[:2]

    def approve(self, pair):
        self.q(self.approver, f'APPROVE CHANGE {pair[0]} OF TASK "inc-436" DIGEST \'{pair[1]}\'')

    def apply(self, op, pair):
        return f"OPERATION '{op}' APPLY CHANGE {pair[0]} DIGEST '{pair[1]}'"

    def statuses(self):
        return dict(self.q(self.controller, 'SELECT id,status FROM shipments WHERE merchant_id=42 ORDER BY id')['rows'])

    def queued(self):
        return sorted(int(k) for k, v in self.statuses().items() if v == 'queued')

    def reads(self, task='inc-436', suffix=''):
        r = self.q(self.controller, f'SHOW TASK READS "{task}"'+suffix)
        return [dict(zip(r['columns'], row)) for row in r['rows']]

    def exposure(self, task='inc-436'):
        r = self.q(self.controller, f'SHOW TASK EXPOSURE "{task}"')
        return {(row[0], row[1]): row[3] for row in r['rows'] if row[3] is not None}

    def counters(self, distinct, transitions):
        e = self.exposure()
        require(int(e['changed', 'distinct_rows']) == distinct and
                int(e['changed', 'transitions']) == transitions, str(e))

    def run_step(self, step, fn):
        self.local.attempts = 1
        try:
            detail = fn() or 'checked'
            status = 'PASS'
        except Exception as e:
            status, detail = 'FAIL', f'{type(e).__name__}: {e}'
        self.output['steps'].append(dict(step=step, status=status,
            attempts=getattr(self.local, 'attempts', 1), detail=detail))
        self.save()

    def start(self):
        require(psycopg.__version__ == '3.2.9', 'requires psycopg 3.2.9')
        self.agent = self.connect()
        self.app = self.connect('delivery_app')
        self.approver = self.connect('oncall_lead')
        self.controller = self.connect('incident_controller')

    def step01(self):
        sql = "SELECT * FROM shipments WHERE status = 'failed'"
        r = self.q(self.agent, sql)
        require(r['columns'] == ['id','status','failed_at','attempt_count','customer_email'], str(r['columns']))
        require(len(r['rows']) == 31, '31 rows')
        require(all(row[1] == 'failed' and row[4] is None and
                    all('@' not in str(v) and 'tok_' not in str(v) for v in row)
                    for row in r['rows']), 'masking')
        records = self.reads()
        matches = [r for r in records if sql in r.values()]
        require(matches, 'statement in SQL read log')
        record = matches[-1]
        require(str(record['sqlstate']) == '00000' and int(record['released_rows']) == 31
                and int(record['charged_rows_returned']) >= 31, str(record))
        self.q(self.controller, 'SHOW TASK "inc-436"')
        e = self.exposure()
        require(int(e['charged', 'rows_returned']) >= 31, 'disclosure charge')

    def step02(self):
        sql = 'SELECT card_token FROM shipments'
        try:
            self.q(self.agent, sql)
        except psycopg.Error as e:
            require(e.sqlstate == '42501' and 'card_token' not in e.diag.message_primary, str(e))
        else:
            raise AssertionError('hidden column accepted')
        for r in self.reads():
            if sql in r.values():
                require(all(int(r[k]) == 0 for k in ('rows_scanned','charged_rows_read','released_rows')), str(r))

    def step03(self):
        try:
            self.q(self.agent, "SELECT id FROM shipments WHERE customer_email LIKE 'a%'")
        except psycopg.Error as e:
            require(e.sqlstate == '42501' and 'customer_email' not in e.diag.message_primary, str(e))
            return
        raise AssertionError('masked filter accepted')

    def step04(self):
        self.fail(self.agent, 'BEGIN', 'transactions')

    def step05(self):
        self.a1 = requeue('a1', 'id <= 4')
        require(self.q(self.agent, self.a1)['count'] == 4, '4 changed')
        self.r1 = self.lookup('a1')
        self.counters(4, 4)
        require(self.queued() == [1,2,3,4], 'queued 1..4')
        self.q(self.controller, 'SHOW CHANGES OF TASK "inc-436"')

    def step06(self):
        again = self.connect()
        require(self.q(again, self.a1)['count'] == 4, 'replay count')
        require(self.lookup('a1', again) == self.r1, 'same receipt')
        require(self.queued() == [1,2,3,4], 'no duplicate changes')

    def step07(self):
        self.direct = requeue('a2', 'id BETWEEN 5 AND 16')
        self.fail(self.agent, self.direct, 'required')
        require(not self.lookup('a2')['rows'] and len(self.queued()) == 4, 'a2 free')

    def step08(self):
        self.p1 = self.preview('id BETWEEN 5 AND 16', 12)

    def step09(self):
        require(self.q(self.app, "UPDATE shipments SET status='delivered',attempt_count=attempt_count+1 WHERE merchant_id=42 AND id=7")['count'] == 1, 'app update')
        require(self.statuses()[7] == 'delivered', 'row 7 delivered')

    def step10(self):
        self.approve(self.p1)
        self.fail(self.agent, self.apply('a2', self.p1), 'stale')
        require(not self.lookup('a2')['rows'] and len(self.queued()) == 4, 'stale applied nothing')

    def step11(self):
        self.p2 = self.preview('id BETWEEN 5 AND 16', 11)
        require(self.p2 != self.p1, 'new preview')
        self.approve(self.p2)
        require(self.q(self.agent, self.apply('a2', self.p2))['count'] == 11, '11 changed')
        self.r2 = self.lookup('a2')
        self.counters(15, 15)
        require(self.queued() == [1,2,3,4,5,6,*range(8,17)], '15 queued')

    def step12(self):
        self.fail(self.agent, self.direct, 'conflict')
        require(self.lookup('a2') == self.r2 and len(self.queued()) == 15, 'receipt unchanged')

    def step13(self):
        p3 = self.preview('id >= 27', 5)
        self.approve(p3)
        self.q(self.app, "INSERT INTO shipments VALUES (42,32,'failed','2026-11-05 17:06:00+00',1,'customer32@example.com','tok_42_32')")
        self.fail(self.agent, self.apply('a3', p3), 'stale')
        require(not self.lookup('a3')['rows'] and len(self.queued()) == 15, 'stale applies nothing')

    def step13b(self):
        require(self.q(self.app, "UPDATE shipments SET status='failed' WHERE merchant_id=42 AND id BETWEEN 8 AND 16")['count'] == 9, 'refail 9')
        for op, clause, n in [('a4','id BETWEEN 8 AND 12',5), ('a5','id BETWEEN 13 AND 16',4)]:
            require(self.q(self.agent, requeue(op, clause))['count'] == n, op)
        require(len(self.queued()) == 15, '15 distinct queued')
        self.counters(15, 24)

    def step14(self):
        clients = [self.connect() for _ in range(10)]
        barrier = threading.Barrier(10)
        def racer(k):
            self.local.attempts = 1
            barrier.wait(timeout=30)
            try:
                r = self.q(clients[k], requeue(f'r{k}', f'id = {17+k}'), race=True)
                require(r['count'] == 1, 'winner count')
                return k, True, self.local.attempts
            except psycopg.Error as e:
                require((e.sqlstate,e.diag.message_primary) == ERRORS['transitions'], str(e))
                return k, False, self.local.attempts
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(racer, range(10)))
        self.local.attempts = max(r[2] for r in results)
        winners = [k for k, ok, _ in results if ok]
        require(len(winners) == 1, str(results))
        self.winner = 'r'+str(winners[0])
        for k, ok, _ in results:
            r = self.lookup(f'r{k}')
            require(len(r['rows']) == 1, f'r{k}: missing outcome')
            require(str(r['rows'][0][1]) == ('committed' if ok else 'budget_exceeded'), str(r))
        require(len(self.queued()) == 16, '16 distinct queued')
        self.counters(16, 25)
        return 'one winner; nine recorded transition-budget refusals; attempts '+str([r[2] for r in results])

    def step15(self):
        a = self.q(self.agent, requeue('a6','id = 40'))
        b = self.q(self.agent, requeue('a7','id = 999'))
        require(a == b and a['count'] == 0, 'invisible == missing')
        require(int(self.q(self.controller, "SELECT count(*) FROM shipments WHERE merchant_id=43 AND status='failed'")['rows'][0][0]) == 40, 'merchant 43 unchanged')

    def step16(self):
        sql = self.spec['task_sql'].replace('inc-436','scan-limit').replace(
            'VISIBLE ROWS WHERE (merchant_id = 42)', "VISIBLE ROWS WHERE (status = 'failed')").replace(', rows_scanned = 100000','')
        self.q(self.controller, sql)
        row = self.q(self.controller, 'ISSUE CREDENTIAL FOR TASK "scan-limit"')['rows'][0]
        self.secrets['scan-limit'] = row[2]
        scan = self.connect(task='scan-limit')
        owner = self.connect('postgres')
        for chunk in range(101):
            values = [f"(7,{chunk*1000+i},'delivered',NULL,0,'bulk{chunk*1000+i}@example.com','tok_7_{chunk*1000+i}')" for i in range(1,1001)]
            self.q(owner, 'INSERT INTO shipments VALUES '+','.join(values))
        self.fail(scan, 'SELECT id, status FROM shipments WHERE attempt_count >= 0', 'scanned')
        r = self.reads('scan-limit')[-1]
        require(100000 < int(r['rows_scanned']) <= 100200 and int(r['released_rows']) == 0
                and r['sqlstate'] == '54T01', str(r))

    def step17(self):
        try:
            c = self.connect('repair_agent', pg=True)
        except psycopg.Error as e:
            # The refusal must be the password check (28P01), not an
            # unreachable listener.
            require('password authentication failed' in str(e), str(e))
            return
        c.close()
        raise AssertionError('TASKONLY password admitted on PostgreSQL')

    def step18(self):
        self.before = self.lookup('a2')
        for c in self.connections:
            c.close()
        self.connections = []
        print('RESTART', flush=True)
        require(sys.stdin.readline().strip() == 'RESTARTED', 'restart acknowledgement')
        self.start()
        require(self.lookup('a2') == self.before, 'persisted receipt')
        require(int(self.before['rows'][0][2]) == 11 and len(self.queued()) == 16, 'receipt and rows')

    def step19(self):
        delay = self.spec['expires_ms']/1000+3-time.time()
        if delay > 0:
            time.sleep(delay)
        require(self.lookup('a2') == self.before, 'outcome after expiry')
        self.fail(self.agent, requeue('a8','id = 27'), 'expired')
        require(self.statuses()[27] == 'failed', 'expired write refused')

    def step20(self):
        require(self.q(self.app, "UPDATE shipments SET status='delivered',attempt_count=attempt_count+1 WHERE merchant_id=42 AND id BETWEEN 1 AND 3")['count'] == 3, 'app changed 3')
        r = self.q(self.approver, 'PREVIEW UNDO OF TASK "inc-436"')
        self.undo = r['rows'][0][:2]
        require([int(v) for v in r['rows'][0][2:6]] == [13,3,9,0], str(r))
        lines = self.q(self.approver, f'SHOW UNDO {self.undo[0]} OF TASK "inc-436"')['rows']
        restores = [r for r in lines if r[0] == 'restore']
        conflicts = [r for r in lines if r[0] == 'conflict']
        require(len({r[1] for r in restores}) == 13 and len({r[1] for r in conflicts}) == 3, str(lines))
        require(all(r[5] == 'changed_since' for r in conflicts), 'conflict reason')
        require(all(tuple(r[2:5]) == ('status','queued','failed') for r in restores), 'restored values')
        self.before_undo = self.statuses()

    def undo_sql(self, mode):
        return f'APPROVE UNDO {self.undo[0]} OF TASK "inc-436" DIGEST \'{self.undo[1]}\' MODE {mode}'

    def step21(self):
        self.fail(self.approver, self.undo_sql('COMPLETE'), 'undo')
        require(self.statuses() == self.before_undo, 'no complete undo applied')

    def step22(self):
        fields = self.fields(self.q(self.approver, self.undo_sql('PARTIAL')))
        expected = dict(decision='undo_partial',mode='partial',restored='13',conflicts='3',
            superseded='9',row_count='13',distinct_charged='0',transitions_charged='0',
            distinct_after='16',transitions_after='25',executor='repair_agent',undone_by='oncall_lead')
        require(all(str(fields.get(k)) == v for k,v in expected.items()), str(fields))
        after = self.statuses()
        require(all(s == (self.before_undo[i] if i in (1,2,3,7) else 'failed') for i,s in after.items()), str(after))
        require(sum(s == 'failed' for s in after.values()) == 28, '28 failed')
        self.q(self.controller, 'SHOW CHANGES OF TASK "inc-436"')
        reads = self.reads(suffix=' LIMIT 1000')
        e = self.exposure()
        require(int(e['charged', 'reconciled']) == 1, 'SQL exposure reconciliation')
        for column, item in [('charged_rows_read','rows_read'),
                             ('charged_rows_returned','rows_returned'),
                             ('charged_bytes','bytes_returned')]:
            require(sum(int(r[column]) for r in reads) == int(e['charged', item]),
                    'read-log sum differs from exposure: '+item)
        self.counters(16, 25)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--spec', required=True)
    args = parser.parse_args()
    spec = parse_spec(args.spec)
    if spec['suite'] == 'exit':
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from psycopg_exit import ExitScenario
        scenario = ExitScenario(spec)
    else:
        scenario = Scenario(spec)
    steps = SUITE_STEPS[spec['suite']]
    try:
        scenario.start()
        for step in steps:
            scenario.run_step(step, getattr(scenario, 'step'+step))
    except Exception as exc:
        done = {r['step'] for r in scenario.output['steps']}
        for step in steps:
            if step not in done:
                scenario.output['steps'].append(dict(step=step,status='FAIL',attempts=1,
                    detail=f'scenario initialization failed: {type(exc).__name__}: {exc}'))
        scenario.save()
    finally:
        for c in scenario.connections:
            c.close()
    return 0 if all(r['status'] == 'PASS' for r in scenario.output['steps']) else 2


if __name__ == '__main__':
    sys.exit(main())
