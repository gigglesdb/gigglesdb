"""Real-driver exit checks; SQL-visible equivalent of the in-process exit tests."""
import concurrent.futures
import socket
import threading
import time
import psycopg
from psycopg_scenario import Scenario, ERRORS, REQUEUE, requeue, require

ERRORS.update({
    'distinct': ('54T01', 'budget exceeded: distinct rows changed by this task'),
    'denied': ('42501', 'denied: change is outside the allowed write of this task'),
    'id': ('42T04', 'operation id required for this statement'),
    'revoked': ('28T01', 'task authorization revoked'),
    'not_allowed': ('0A000', 'statement is not allowed in a task session'),
    'approver': ('42501', 'request rejected; consult documented code'),
})
DEVIATIONS = [
    'Race adds rows 32..34 via the application so ten distinct targets remain after 24 distinct writes.',
    'Undo task permits attempt_count updates and queued old rows, matching the Rust undo fixture.',
    'Lost reply drops the socket after flushing; no trusted before-commit pause seam is available.',
    'SQL-issued credentials are invalidated on revocation: live sessions close at check_binding before a 28T01 statement reply; fresh authentication is refused. Controller SHOW TASK/SHOW CHANGES replaces Admin-only outcome inspection.',
    'Admin receipt internals and approval storage are checked through SQL-visible outcomes and APPLY.',
]


class ExitScenario(Scenario):
    def __init__(self, spec):
        super().__init__(spec)
        self.output = {'steps': [], 'deviations': DEVIATIONS.copy()}

    def start(self):
        require(psycopg.__version__ == '3.2.9', 'requires psycopg 3.2.9')
        self.app = self.connect('delivery_app')
        self.approver = self.connect('oncall_lead')
        self.controller = self.connect('incident_controller')

    def select_task(self, task, merchant):
        self.task, self.merchant = task, merchant
        self.agent = self.connect(task=task)

    def approve_sql(self, pair):
        return f'APPROVE CHANGE {pair[0]} OF TASK "{self.task}" DIGEST \'{pair[1]}\''

    def approve(self, pair):
        self.q(self.approver, self.approve_sql(pair))

    def exposure(self):
        return super().exposure(self.task)

    def statuses(self):
        return dict(self.q(self.controller, f'SELECT id,status FROM shipments WHERE merchant_id={self.merchant} ORDER BY id')['rows'])

    def app_update(self, clause, assignment):
        return self.q(self.app, f'UPDATE shipments SET {assignment} WHERE merchant_id={self.merchant} AND {clause}')

    def decision(self, op, decision):
        r = self.lookup(op)
        require(len(r['rows']) == 1 and r['rows'][0][1] == decision, str(r))
        return r

    def steprace(self):
        self.select_task('exit-race', 51)
        for i in range(32, 35):
            self.q(self.app, f"INSERT INTO shipments VALUES (51,{i},'failed','2026-11-05 17:06:00+00',1,'customer{i}@example.com','tok_51_{i}')")
        for lo in range(1, 25, 5):
            hi = min(lo+4, 24)
            require(self.q(self.agent, requeue(f'burn{lo}', f'id BETWEEN {lo} AND {hi}'))['count'] == hi-lo+1, 'burn count')
        self.counters(24, 24)
        clients = [self.connect(task=self.task) for _ in range(10)]
        barrier = threading.Barrier(10)
        def racer(k):
            self.local.attempts = 1
            barrier.wait(timeout=30)
            try:
                require(self.q(clients[k], requeue(f'r{k}', f'id = {25+k}'), race=True)['count'] == 1, 'winner count')
                return True, 'committed', self.local.attempts
            except psycopg.Error as e:
                pair = (e.sqlstate, e.diag.message_primary)
                require(pair in (ERRORS['distinct'], ERRORS['transitions']), str(pair))
                return False, pair[1], self.local.attempts
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(racer, range(10)))
        self.local.attempts = max(r[2] for r in results)
        require(sum(r[0] for r in results) == 1, str(results))
        for k, (won, _, _) in enumerate(results):
            self.decision(f'r{k}', 'committed' if won else 'budget_exceeded')
        self.counters(25, 25)
        require(len(self.queued()) == 25, '25 queued rows')
        for c in clients:
            c.close()
        return 'one winner; nine budget refusals: '+str(results)

    def stepstale(self):
        self.select_task('exit-stale', 52)
        for extra in (False, True):
            pair = self.preview('id >= 26', 6)
            self.approve(pair)
            if extra:
                self.q(self.app, "INSERT INTO shipments VALUES (52,32,'failed','2026-11-05 17:06:00+00',1,'customer32@example.com','tok_52_32')")
            else:
                self.app_update('id=26', 'attempt_count=attempt_count+1')
            before = self.statuses()
            self.fail(self.agent, self.apply('extra' if extra else 'stale', pair), 'stale')
            require(not self.lookup('extra' if extra else 'stale')['rows'], 'stale id free')
            require(self.statuses() == before and not self.queued(), 'stale changed rows')
        self.app_update('id=32', "status='pending'")
        pair = self.preview('id >= 26', 6)
        wrong = (pair[0], '1'*64 if pair[1] == '0'*64 else '0'*64)
        self.fail(self.approver, self.approve_sql(wrong), 'stale')
        self.fail(self.agent, self.apply('fresh', pair), 'required')
        self.fail(self.agent, self.approve_sql(pair), 'not_allowed')
        for c in (self.controller, self.app):
            self.fail(c, self.approve_sql(pair), 'approver')
        self.fail(self.agent, self.apply('fresh', pair), 'required')
        require(not self.lookup('fresh')['rows'], 'unapproved id free')
        self.approve(pair)
        require(self.q(self.agent, self.apply('fresh', pair))['count'] == 6, 'fresh applies six')
        self.counters(6, 6)

    def revoked_refusal(self, c, sql):
        try:
            self.q(c, sql)
        except psycopg.Error as e:
            if e.sqlstate is not None:
                require((e.sqlstate, e.diag.message_primary) == ERRORS['revoked'], str(e))
            else:
                require(isinstance(e, psycopg.OperationalError) and c.broken and
                        any(message in str(e) for message in ('SSL connection has been closed unexpectedly',
                            'server closed the connection unexpectedly', 'consuming input failed',
                            'SSL SYSCALL error: EOF detected')), repr(str(e)))
            return
        raise AssertionError('revoked credential admitted a statement')

    def stepoutcomes(self):
        self.select_task('exit-outcomes', 53)
        sql = requeue('commit', 'id=1')
        require(self.q(self.agent, sql)['count'] == 1, 'commit count')
        receipt = self.decision('commit', 'committed')
        before = self.exposure()
        require(self.q(self.agent, sql)['count'] == 1 and self.lookup('commit') == receipt, 'replay receipt')
        require(self.exposure() == before, 'replay exposure')
        self.fail(self.agent, requeue('commit', 'id=2'), 'conflict')
        denied = "OPERATION 'denied' UPDATE shipments SET status='delivered' WHERE id=2"
        self.fail(self.agent, denied, 'denied')
        receipt = self.decision('denied', 'denied')
        self.fail(self.agent, denied, 'denied')
        require(self.lookup('denied') == receipt, 'denied replay receipt')
        self.fail(self.agent, requeue('denied', 'id=2'), 'conflict')
        self.fail(self.agent, requeue('approval', 'id BETWEEN 2 AND 7'), 'required')
        require(not self.lookup('approval')['rows'], 'approval id free')
        pair = self.preview('id BETWEEN 2 AND 7', 6)
        self.approve(pair)
        require(self.q(self.agent, self.apply('approval', pair))['count'] == 6, 'approval same id')
        pair = self.preview('id BETWEEN 8 AND 13', 6)
        self.approve(pair)
        self.app_update('id=8', 'attempt_count=attempt_count+1')
        self.fail(self.agent, self.apply('stale', pair), 'stale')
        require(not self.lookup('stale')['rows'], 'stale id free')
        pair = self.preview('id BETWEEN 8 AND 13', 6)
        self.approve(pair)
        require(self.q(self.agent, self.apply('stale', pair))['count'] == 6, 'stale same id')
        for lo, hi in ((14,18), (19,23), (24,25)):
            require(self.q(self.agent, requeue(f'burn{lo}', f'id BETWEEN {lo} AND {hi}'))['count'] == hi-lo+1, 'budget burn')
        over = requeue('over', 'id=26')
        self.fail(self.agent, over, 'distinct')
        receipt = self.decision('over', 'budget_exceeded')
        before = self.statuses()
        self.fail(self.agent, over, 'distinct')
        require(self.lookup('over') == receipt, 'budget replay receipt')
        self.fail(self.agent, requeue('over', 'id=27'), 'conflict')
        self.fail(self.agent, REQUEUE+' AND id=26', 'id')
        self.counters(25, 25)
        lookup = self.connect(task=self.task)
        changes_before = self.q(self.controller, f'SHOW CHANGES OF TASK "{self.task}"')
        self.q(self.controller, f'REVOKE TASK "{self.task}"')
        self.revoked_refusal(self.agent, requeue('revoked', 'id=27'))
        self.revoked_refusal(lookup, "SHOW OPERATION 'commit'")
        try:
            self.connect(task=self.task)
        except psycopg.Error as e:
            require('password authentication failed for user "repair_agent"' in str(e), str(e))
        else:
            raise AssertionError('revoked credential connected')
        self.q(self.controller, f'SHOW TASK "{self.task}"')
        changes = self.q(self.controller, f'SHOW CHANGES OF TASK "{self.task}"')
        require(changes == changes_before, 'revoked recorded a change')
        require(self.statuses() == before, 'refusals changed rows')

    def steplost_reply(self):
        self.select_task('exit-lost', 54)
        sql = requeue('lost', 'id=1')
        lost = self.agent
        lost.pgconn.send_query(sql.encode())
        while lost.pgconn.flush():
            time.sleep(.001)
        # Close the transport without sending Terminate or consuming a reply.
        socket.socket(fileno=lost.pgconn.socket).close()
        lost.pgconn.finish()
        self.agent = self.connect(task=self.task)
        deadline = time.monotonic()+10
        receipt = self.lookup('lost')
        while not receipt['rows'] and time.monotonic() < deadline:
            time.sleep(.02)
            receipt = self.lookup('lost')
        path = 'lookup recovered committed operation'
        if not receipt['rows']:
            require(self.q(self.agent, sql)['count'] == 1, 'retry count')
            receipt = self.lookup('lost')
            path = 'empty lookup; retried identical request'
        self.decision('lost', 'committed')
        require(self.q(self.agent, sql)['count'] == 1 and self.lookup('lost') == receipt, 'lost replay receipt')
        self.fail(self.agent, requeue('lost', 'id=2'), 'conflict')
        require(self.queued() == [1], 'changed exactly once')
        self.counters(1, 1)
        return path

    def stepundo(self):
        self.select_task('exit-undo', 55)
        require(self.q(self.agent, "OPERATION 'first' UPDATE shipments SET status='queued' WHERE id<=2")['count'] == 2, 'first count')
        require(self.q(self.agent, "OPERATION 'second' UPDATE shipments SET attempt_count=9 WHERE id=2")['count'] == 1, 'second count')
        self.app_update('id=1', "status='delivered'")
        self.counters(2, 3)
        row = self.q(self.approver, f'PREVIEW UNDO OF TASK "{self.task}"')['rows'][0]
        require([int(v) for v in row[2:6]] == [1,1,0,0], str(row))
        lines = self.q(self.approver, f'SHOW UNDO {row[0]} OF TASK "{self.task}"')['rows']
        require(len({r[1] for r in lines if r[0]=='restore'}) == 1, str(lines))
        require(len({r[1] for r in lines if r[0]=='conflict'}) == 1, str(lines))
        require(all(r[5]=='changed_since' for r in lines if r[0]=='conflict'), str(lines))
        require(any(tuple(r[2:5]) == ('status','queued','failed') for r in lines if r[0]=='restore'), str(lines))
        require(any(tuple(r[2:5]) == ('attempt_count','9','2') for r in lines if r[0]=='restore'), str(lines))
        sql = f'APPROVE UNDO {row[0]} OF TASK "{self.task}" DIGEST \'{row[1]}\' MODE '
        before = self.statuses()
        self.fail(self.approver, sql+'COMPLETE', 'undo')
        require(self.statuses() == before, 'complete changed rows')
        fields = self.fields(self.q(self.approver, sql+'PARTIAL'))
        expected = dict(decision='undo_partial', mode='partial', restored='1', conflicts='1', superseded='0', row_count='1', distinct_charged='0', transitions_charged='0', distinct_after='2', transitions_after='3', executor='repair_agent', undone_by='oncall_lead')
        require(all(str(fields.get(k)) == v for k,v in expected.items()), str(fields))
        require(self.statuses()[1]=='delivered' and self.statuses()[2]=='failed', 'undo statuses')
        require(int(self.q(self.controller, 'SELECT attempt_count FROM shipments WHERE merchant_id=55 AND id=2')['rows'][0][0]) == 2, 'original attempt_count')
        self.counters(2, 3)
        self.q(self.controller, f'SHOW CHANGES OF TASK "{self.task}"')
