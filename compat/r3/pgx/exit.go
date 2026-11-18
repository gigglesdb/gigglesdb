// SQL-visible exit criteria. Keep these checks in sync with psycopg_exit.py.
package main

import (
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"io"
	"sync"
	"time"
)

var exitDeviations = []string{
	"Race adds rows 32..34 via the application so ten distinct targets remain after 24 distinct writes.",
	"Undo task permits attempt_count updates and queued old rows, matching the Rust undo fixture.",
	"Lost reply drops the socket after flushing; no trusted before-commit pause seam is available.",
	"SQL-issued credentials are invalidated on revocation: live sessions close at check_binding before a 28T01 statement reply; fresh authentication is refused. Controller SHOW TASK/SHOW CHANGES replaces Admin-only outcome inspection.",
	"Admin receipt internals and approval storage are checked through SQL-visible outcomes and APPLY.",
}

func init() {
	for k, v := range map[string][2]string{
		"distinct":    {"54T01", "budget exceeded: distinct rows changed by this task"},
		"denied":      {"42501", "denied: change is outside the allowed write of this task"},
		"id":          {"42T04", "operation id required for this statement"},
		"revoked":     {"28T01", "task authorization revoked"},
		"not_allowed": {"0A000", "statement is not allowed in a task session"},
		"approver":    {"42501", "request rejected; consult documented code"},
	} {
		expectedErrors[k] = v
	}
}
func (s *scenario) selectTask(task string, merchant int) {
	s.task, s.merchant = task, merchant
	s.agent = s.connect("repair_agent", task, false)
}
func (s *scenario) exitApproveSQL(pair []any) string {
	return fmt.Sprintf(`APPROVE CHANGE %v OF TASK "%s" DIGEST '%v'`, pair[0], s.task, pair[1])
}
func (s *scenario) exitApprove(pair []any) { s.q(s.approver, s.exitApproveSQL(pair)) }
func (s *scenario) exitExposure() map[[2]string]any {
	r := s.q(s.controller, fmt.Sprintf(`SHOW TASK EXPOSURE "%s"`, s.task))
	m := map[[2]string]any{}
	for _, row := range r.Rows {
		if row[3] != nil {
			m[[2]string{text(row[0]), text(row[1])}] = row[3]
		}
	}
	return m
}
func (s *scenario) exitCounters(d, t int) {
	e := s.exitExposure()
	require(integer(e[[2]string{"changed", "distinct_rows"}]) == d && integer(e[[2]string{"changed", "transitions"}]) == t, e)
}
func (s *scenario) exitStatuses() map[int]string {
	m := map[int]string{}
	for _, r := range s.q(s.controller, fmt.Sprintf("SELECT id,status FROM shipments WHERE merchant_id=%d ORDER BY id", s.merchant)).Rows {
		m[integer(r[0])] = text(r[1])
	}
	return m
}
func (s *scenario) queuedCount() int {
	n := 0
	for _, v := range s.exitStatuses() {
		if v == "queued" {
			n++
		}
	}
	return n
}
func (s *scenario) appUpdate(clause, assignment string) {
	s.q(s.app, fmt.Sprintf("UPDATE shipments SET %s WHERE merchant_id=%d AND %s", assignment, s.merchant, clause))
}
func (s *scenario) decision(op, decision string) result {
	r := s.lookup(op)
	require(len(r.Rows) == 1 && r.Rows[0][1] == decision, r)
	return r
}
func (s *scenario) revokedRefusal(c *pgx.Conn, sql string) {
	_, e := query(c, sql, false, &s.attempts)
	require(e != nil, "revoked credential admitted a statement")
	if errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF) {
		require(c.IsClosed(), e)
		return
	}
	require(errorPair(e) == expectedErrors["revoked"], e)
}
func (s *scenario) revokedConnectRefused() {
	refused := false
	func() {
		defer func() {
			if p := recover(); p != nil {
				e, ok := p.(error)
				require(ok, p)
				require(errorPair(e) == [2]string{"28P01", `password authentication failed for user "repair_agent"`}, e)
				refused = true
			}
		}()
		c := s.connect("repair_agent", s.task, false)
		_ = c.Close(ctx)
	}()
	require(refused, "revoked credential connected")
}
func (s *scenario) exitStep(step string) string {
	switch step {
	case "race":
		return s.exitRace()
	case "stale":
		s.selectTask("exit-stale", 52)
		for _, extra := range []bool{false, true} {
			p := s.preview("id >= 26", 6)
			s.exitApprove(p)
			op := "stale"
			if extra {
				op = "extra"
				s.q(s.app, "INSERT INTO shipments VALUES (52,32,'failed','2026-11-05 17:06:00+00',1,'customer32@example.com','tok_52_32')")
			} else {
				s.appUpdate("id=26", "attempt_count=attempt_count+1")
			}
			before := s.exitStatuses()
			s.fail(s.agent, apply(op, p), "stale")
			require(len(s.lookup(op).Rows) == 0, "stale id free")
			require(equal(s.exitStatuses(), before) && s.queuedCount() == 0, "stale changed rows")
		}
		s.appUpdate("id=32", "status='pending'")
		p := s.preview("id >= 26", 6)
		wrong := "0000000000000000000000000000000000000000000000000000000000000000"
		if p[1] == wrong {
			wrong = "1111111111111111111111111111111111111111111111111111111111111111"
		}
		s.fail(s.approver, s.exitApproveSQL([]any{p[0], wrong}), "stale")
		s.fail(s.agent, apply("fresh", p), "required")
		s.fail(s.agent, s.exitApproveSQL(p), "not_allowed")
		for _, c := range []*pgx.Conn{s.controller, s.app} {
			s.fail(c, s.exitApproveSQL(p), "approver")
		}
		s.fail(s.agent, apply("fresh", p), "required")
		require(len(s.lookup("fresh").Rows) == 0, "unapproved id free")
		s.exitApprove(p)
		require(s.q(s.agent, apply("fresh", p)).Count == 6, "fresh applies six")
		s.exitCounters(6, 6)
	case "outcomes":
		s.selectTask("exit-outcomes", 53)
		sql := requeue("commit", "id=1")
		require(s.q(s.agent, sql).Count == 1, "commit count")
		receipt := s.decision("commit", "committed")
		before := s.exitExposure()
		require(s.q(s.agent, sql).Count == 1 && equal(s.lookup("commit"), receipt), "replay receipt")
		require(equal(s.exitExposure(), before), "replay exposure")
		s.fail(s.agent, requeue("commit", "id=2"), "conflict")
		denied := "OPERATION 'denied' UPDATE shipments SET status='delivered' WHERE id=2"
		s.fail(s.agent, denied, "denied")
		receipt = s.decision("denied", "denied")
		s.fail(s.agent, denied, "denied")
		require(equal(s.lookup("denied"), receipt), "denied replay receipt")
		s.fail(s.agent, requeue("denied", "id=2"), "conflict")
		s.fail(s.agent, requeue("approval", "id BETWEEN 2 AND 7"), "required")
		require(len(s.lookup("approval").Rows) == 0, "approval id free")
		p := s.preview("id BETWEEN 2 AND 7", 6)
		s.exitApprove(p)
		require(s.q(s.agent, apply("approval", p)).Count == 6, "approval same id")
		p = s.preview("id BETWEEN 8 AND 13", 6)
		s.exitApprove(p)
		s.appUpdate("id=8", "attempt_count=attempt_count+1")
		s.fail(s.agent, apply("stale", p), "stale")
		require(len(s.lookup("stale").Rows) == 0, "stale id free")
		p = s.preview("id BETWEEN 8 AND 13", 6)
		s.exitApprove(p)
		require(s.q(s.agent, apply("stale", p)).Count == 6, "stale same id")
		for _, pair := range [][2]int{{14, 18}, {19, 23}, {24, 25}} {
			require(s.q(s.agent, requeue(fmt.Sprintf("burn%d", pair[0]), fmt.Sprintf("id BETWEEN %d AND %d", pair[0], pair[1]))).Count == int64(pair[1]-pair[0]+1), "budget burn")
		}
		over := requeue("over", "id=26")
		s.fail(s.agent, over, "distinct")
		receipt = s.decision("over", "budget_exceeded")
		beforeRows := s.exitStatuses()
		s.fail(s.agent, over, "distinct")
		require(equal(s.lookup("over"), receipt), "budget replay receipt")
		s.fail(s.agent, requeue("over", "id=27"), "conflict")
		s.fail(s.agent, requeueSQL+" AND id=26", "id")
		s.exitCounters(25, 25)
		lookup := s.connect("repair_agent", s.task, false)
		changesBefore := s.q(s.controller, fmt.Sprintf(`SHOW CHANGES OF TASK "%s"`, s.task))
		s.q(s.controller, fmt.Sprintf(`REVOKE TASK "%s"`, s.task))
		s.revokedRefusal(s.agent, requeue("revoked", "id=27"))
		s.revokedRefusal(lookup, "SHOW OPERATION 'commit'")
		s.revokedConnectRefused()
		s.q(s.controller, fmt.Sprintf(`SHOW TASK "%s"`, s.task))
		changes := s.q(s.controller, fmt.Sprintf(`SHOW CHANGES OF TASK "%s"`, s.task))
		require(equal(changes, changesBefore), "revoked recorded a change")
		require(equal(s.exitStatuses(), beforeRows), "refusals changed rows")
	case "lost_reply":
		s.selectTask("exit-lost", 54)
		sql := requeue("lost", "id=1")
		lost := s.agent
		frontend := lost.PgConn().Frontend()
		frontend.Send(&pgproto3.Query{String: sql})
		check(frontend.Flush())
		check(lost.PgConn().Conn().Close())
		s.agent = s.connect("repair_agent", s.task, false)
		deadline := time.Now().Add(10 * time.Second)
		receipt := s.lookup("lost")
		for len(receipt.Rows) == 0 && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
			receipt = s.lookup("lost")
		}
		path := "lookup recovered committed operation"
		if len(receipt.Rows) == 0 {
			require(s.q(s.agent, sql).Count == 1, "retry count")
			receipt = s.lookup("lost")
			path = "empty lookup; retried identical request"
		}
		s.decision("lost", "committed")
		require(s.q(s.agent, sql).Count == 1 && equal(s.lookup("lost"), receipt), "lost replay receipt")
		s.fail(s.agent, requeue("lost", "id=2"), "conflict")
		require(s.exitStatuses()[1] == "queued" && s.queuedCount() == 1, "changed exactly once")
		s.exitCounters(1, 1)
		return path
	case "undo":
		s.selectTask("exit-undo", 55)
		require(s.q(s.agent, "OPERATION 'first' UPDATE shipments SET status='queued' WHERE id<=2").Count == 2, "first count")
		require(s.q(s.agent, "OPERATION 'second' UPDATE shipments SET attempt_count=9 WHERE id=2").Count == 1, "second count")
		s.appUpdate("id=1", "status='delivered'")
		s.exitCounters(2, 3)
		row := s.q(s.approver, fmt.Sprintf(`PREVIEW UNDO OF TASK "%s"`, s.task)).Rows[0]
		for i, n := range []int{1, 1, 0, 0} {
			require(integer(row[2+i]) == n, row)
		}
		lines := s.q(s.approver, fmt.Sprintf(`SHOW UNDO %v OF TASK "%s"`, row[0], s.task)).Rows
		restores, conflicts := map[string]bool{}, map[string]bool{}
		status, attempt := false, false
		for _, r := range lines {
			if r[0] == "restore" {
				restores[text(r[1])] = true
				status = status || equal(r[2:5], []any{"status", "queued", "failed"})
				attempt = attempt || equal(r[2:5], []any{"attempt_count", "9", "2"})
			}
			if r[0] == "conflict" {
				conflicts[text(r[1])] = true
				require(r[5] == "changed_since", lines)
			}
		}
		require(len(restores) == 1 && len(conflicts) == 1 && status && attempt, lines)
		sql := fmt.Sprintf(`APPROVE UNDO %v OF TASK "%s" DIGEST '%v' MODE `, row[0], s.task, row[1])
		before := s.exitStatuses()
		s.fail(s.approver, sql+"COMPLETE", "undo")
		require(equal(s.exitStatuses(), before), "complete changed rows")
		f := fields(s.q(s.approver, sql+"PARTIAL"))
		expected := map[string]string{"decision": "undo_partial", "mode": "partial", "restored": "1", "conflicts": "1", "superseded": "0", "row_count": "1", "distinct_charged": "0", "transitions_charged": "0", "distinct_after": "2", "transitions_after": "3", "executor": "repair_agent", "undone_by": "oncall_lead"}
		for k, v := range expected {
			require(text(f[k]) == v, f)
		}
		require(s.exitStatuses()[1] == "delivered" && s.exitStatuses()[2] == "failed", "undo statuses")
		require(integer(s.q(s.controller, "SELECT attempt_count FROM shipments WHERE merchant_id=55 AND id=2").Rows[0][0]) == 2, "original attempt_count")
		s.exitCounters(2, 3)
		s.q(s.controller, fmt.Sprintf(`SHOW CHANGES OF TASK "%s"`, s.task))
	}
	return "checked"
}
func (s *scenario) exitRace() string {
	s.selectTask("exit-race", 51)
	for i := 32; i <= 34; i++ {
		s.q(s.app, fmt.Sprintf("INSERT INTO shipments VALUES (51,%d,'failed','2026-11-05 17:06:00+00',1,'customer%d@example.com','tok_51_%d')", i, i, i))
	}
	for lo := 1; lo < 25; lo += 5 {
		hi := min(lo+4, 24)
		require(s.q(s.agent, requeue(fmt.Sprintf("burn%d", lo), fmt.Sprintf("id BETWEEN %d AND %d", lo, hi))).Count == int64(hi-lo+1), "burn count")
	}
	s.exitCounters(24, 24)
	clients := make([]*pgx.Conn, 10)
	for i := range clients {
		clients[i] = s.connect("repair_agent", s.task, false)
	}
	type outcome struct {
		won      bool
		attempts int
		message  string
		err      any
	}
	results := make([]outcome, 10)
	barrier := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(10)
	done.Add(10)
	for k := range clients {
		go func(k int) {
			defer done.Done()
			defer func() {
				if p := recover(); p != nil {
					results[k].err = p
				}
			}()
			results[k].attempts = 1
			ready.Done()
			<-barrier
			r, e := query(clients[k], requeue(fmt.Sprintf("r%d", k), fmt.Sprintf("id = %d", 25+k)), true, &results[k].attempts)
			if e != nil {
				p := errorPair(e)
				require(p == expectedErrors["distinct"] || p == expectedErrors["transitions"], p)
				results[k].message = p[1]
			} else {
				require(r.Count == 1, "winner count")
				results[k].won = true
				results[k].message = "committed"
			}
		}(k)
	}
	ready.Wait()
	close(barrier)
	done.Wait()
	wins := 0
	details := []string{}
	for k, r := range results {
		require(r.err == nil, r.err)
		s.attempts = max(s.attempts, r.attempts)
		want := "budget_exceeded"
		if r.won {
			wins++
			want = "committed"
		}
		s.decision(fmt.Sprintf("r%d", k), want)
		details = append(details, fmt.Sprintf("r%d: %s (%d attempts)", k, r.message, r.attempts))
		_ = clients[k].Close(ctx)
	}
	require(wins == 1, details)
	s.exitCounters(25, 25)
	require(s.queuedCount() == 25, "25 queued rows")
	return fmt.Sprintf("one winner; nine budget refusals: %v", details)
}
