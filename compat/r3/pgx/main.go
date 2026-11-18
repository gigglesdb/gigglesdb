// Real-process R3 scenario. Keep checks and deviations in sync with psycopg_scenario.py.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ctx = context.Background()

const requeueSQL = "UPDATE shipments SET status = 'queued' WHERE status = 'failed'"

var expectedErrors = map[string][2]string{
	"required":     {"42T01", "approval required: preview the change and have it approved"},
	"stale":        {"55T01", "approval is stale: preview the change again, do not retry blindly"},
	"conflict":     {"42T03", "operation id already used for a different request"},
	"expired":      {"28T02", "task authorization expired"},
	"transactions": {"0A000", "explicit transactions are not allowed in a task session"},
	"transitions":  {"54T01", "budget exceeded: row transitions made by this task"},
	"scanned":      {"54T01", "budget exceeded: rows scanned by this statement"},
	"undo":         {"42501", "denied: complete undo is not possible because later changes conflict"},
}
var transient = map[string]bool{"task budget exceeded": true, "budget exceeded: concurrent statements of this task": true, "budget exceeded: load of all agent sessions": true}
var deviations = []string{
	"Step 13: app INSERT occurs after approval and before APPLY; no trusted in-process pause seam.",
	"Steps 19-20 use real expiry and no next-day clock advance.",
	"Admin receipt internals and change-summary bytes are checked only through SQL-visible fields; no Admin API is used.",
}

type spec struct {
	PgPort      int               `json:"pg_port"`
	AgentPort   int               `json:"agent_port"`
	Database    string            `json:"database"`
	CA          string            `json:"ca"`
	SecretFile  string            `json:"secret_file"`
	Passwords   map[string]string `json:"passwords"`
	ExpiresMS   int64             `json:"expires_ms"`
	ResultsPath string            `json:"results_path"`
	TaskSQL     string            `json:"task_sql"`
}
type result struct {
	Columns []string
	Rows    [][]any
	Count   int64
}
type stepResult struct {
	Step     string `json:"step"`
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
	Detail   string `json:"detail"`
}
type output struct {
	Steps      []stepResult `json:"steps"`
	Deviations []string     `json:"deviations"`
}
type scenario struct {
	spec                             spec
	secrets                          map[string]string
	output                           output
	connections                      []*pgx.Conn
	agent, app, approver, controller *pgx.Conn
	attempts                         int
	a1, direct                       string
	p1, p2, undo                     []any
	r1, r2, before                   result
	beforeUndo                       map[int]string
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func require(ok bool, detail any) {
	if !ok {
		panic(fmt.Errorf("%v", detail))
	}
}
func text(v any) string   { return fmt.Sprint(v) }
func integer(v any) int   { n, e := strconv.Atoi(text(v)); check(e); return n }
func equal(a, b any) bool { return reflect.DeepEqual(a, b) }
func requeue(op, rows string) string {
	return fmt.Sprintf("OPERATION '%s' %s AND %s", op, requeueSQL, rows)
}
func (s *scenario) connect(role, task string, pg bool) *pgx.Conn {
	agent := role == "repair_agent" && !pg
	port := s.spec.PgPort
	mode := "disable"
	password := s.spec.Passwords[role]
	if role == "postgres" {
		password = s.secrets["owner_password"]
	}
	if agent {
		port = s.spec.AgentPort
		mode = "verify-full"
		password = s.secrets[task]
	}
	// ParseConfig owns TLS verification, including the localhost certificate name.
	dsn := fmt.Sprintf("host=localhost port=%d dbname=%s sslmode=%s", port, s.spec.Database, mode)
	if agent {
		dsn += " sslrootcert='" + strings.ReplaceAll(strings.ReplaceAll(s.spec.CA, "\\", "\\\\"), "'", "\\'") + "'"
	}
	cfg, e := pgx.ParseConfig(dsn)
	check(e)
	cfg.User = role
	cfg.Password = password
	cfg.ConnectTimeout = 10 * time.Second
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	if agent {
		cfg.RuntimeParams["options"] = "-c giggles.task=" + task
	}
	c, e := pgx.ConnectConfig(ctx, cfg)
	check(e)
	s.connections = append(s.connections, c)
	return c
}
func query(c *pgx.Conn, sql string, race bool, attempts *int) (result, error) {
	for attempt := 1; attempt <= 50; attempt++ {
		if attempt > *attempts {
			*attempts = attempt
		}
		r, e := c.Query(ctx, sql)
		out := result{Columns: []string{}, Rows: [][]any{}}
		if e == nil {
			for _, f := range r.FieldDescriptions() {
				out.Columns = append(out.Columns, f.Name)
			}
			for r.Next() {
				values, err := r.Values()
				if err != nil {
					e = err
					break
				}
				for i, v := range values {
					if v != nil {
						if t, ok := v.(time.Time); ok {
							values[i] = t.UTC().Format(time.RFC3339Nano)
						} else {
							values[i] = fmt.Sprint(v)
						}
					}
				}
				out.Rows = append(out.Rows, values)
			}
			r.Close()
			if e == nil {
				e = r.Err()
			}
			out.Count = r.CommandTag().RowsAffected()
		}
		if e == nil {
			return out, nil
		}
		var p *pgconn.PgError
		retry := errors.As(e, &p) && (p.Code == "40001" || (race && p.Code == "54T01" && transient[p.Message]))
		if !retry || attempt == 50 {
			return out, e
		}
		time.Sleep(5 * time.Millisecond * time.Duration(min(attempt, 20)))
	}
	panic("unreachable")
}
func (s *scenario) q(c *pgx.Conn, sql string) result {
	r, e := query(c, sql, false, &s.attempts)
	check(e)
	return r
}
func errorPair(e error) [2]string {
	var p *pgconn.PgError
	require(errors.As(e, &p), fmt.Sprintf("expected PostgreSQL error: %v", e))
	return [2]string{p.Code, p.Message}
}
func (s *scenario) fail(c *pgx.Conn, sql, key string) {
	_, e := query(c, sql, false, &s.attempts)
	require(e != nil, "expected "+key+", statement succeeded")
	got := errorPair(e)
	require(got == expectedErrors[key], fmt.Sprintf("%s: expected %v, got %v", key, expectedErrors[key], got))
}
func (s *scenario) lookup(op string, c ...*pgx.Conn) result {
	conn := s.agent
	if len(c) > 0 {
		conn = c[0]
	}
	return s.q(conn, "SHOW OPERATION '"+op+"'")
}
func fields(r result) map[string]any {
	require(len(r.Rows) == 1, r)
	return rowFields(r.Columns, r.Rows[0])
}
func rowFields(cols []string, row []any) map[string]any {
	m := map[string]any{}
	for i, c := range cols {
		m[c] = row[i]
	}
	return m
}
func (s *scenario) preview(filter string, n int) []any {
	r := s.q(s.agent, "PREVIEW "+requeueSQL+" AND "+filter)
	require(equal(r.Columns, []string{"change_id", "digest", "row_count", "expires_at"}), r)
	row := r.Rows[0]
	require(integer(row[2]) == n && regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(text(row[1])), row)
	return row[:2]
}
func (s *scenario) approve(pair []any) {
	s.q(s.approver, fmt.Sprintf(`APPROVE CHANGE %v OF TASK "inc-436" DIGEST '%v'`, pair[0], pair[1]))
}
func apply(op string, pair []any) string {
	return fmt.Sprintf("OPERATION '%s' APPLY CHANGE %v DIGEST '%v'", op, pair[0], pair[1])
}
func (s *scenario) statuses() map[int]string {
	m := map[int]string{}
	for _, r := range s.q(s.controller, "SELECT id,status FROM shipments WHERE merchant_id=42 ORDER BY id").Rows {
		m[integer(r[0])] = text(r[1])
	}
	return m
}
func (s *scenario) queued() []int {
	a := []int{}
	for i, v := range s.statuses() {
		if v == "queued" {
			a = append(a, i)
		}
	}
	sort.Ints(a)
	return a
}
func (s *scenario) reads(task, suffix string) []map[string]any {
	r := s.q(s.controller, fmt.Sprintf("SHOW TASK READS \"%s\"%s", task, suffix))
	a := []map[string]any{}
	for _, row := range r.Rows {
		a = append(a, rowFields(r.Columns, row))
	}
	return a
}
func (s *scenario) exposure() map[[2]string]any {
	r := s.q(s.controller, `SHOW TASK EXPOSURE "inc-436"`)
	m := map[[2]string]any{}
	for _, row := range r.Rows {
		if row[3] != nil {
			m[[2]string{text(row[0]), text(row[1])}] = row[3]
		}
	}
	return m
}
func (s *scenario) counters(distinct, transitions int) {
	e := s.exposure()
	require(integer(e[[2]string{"changed", "distinct_rows"}]) == distinct && integer(e[[2]string{"changed", "transitions"}]) == transitions, e)
}
func (s *scenario) start() {
	s.agent = s.connect("repair_agent", "inc-436", false)
	s.app = s.connect("delivery_app", "", false)
	s.approver = s.connect("oncall_lead", "", false)
	s.controller = s.connect("incident_controller", "", false)
}
func (s *scenario) close() {
	for _, c := range s.connections {
		_ = c.Close(ctx)
	}
	s.connections = nil
}
func (s *scenario) step(step string) string {
	switch step {
	case "01":
		sql := "SELECT * FROM shipments WHERE status = 'failed'"
		r := s.q(s.agent, sql)
		require(equal(r.Columns, []string{"id", "status", "failed_at", "attempt_count", "customer_email"}), r.Columns)
		require(len(r.Rows) == 31, "31 rows")
		for _, row := range r.Rows {
			require(row[1] == "failed" && row[4] == nil, "masking")
			for _, v := range row {
				require(!strings.Contains(text(v), "@") && !strings.Contains(text(v), "tok_"), "masking")
			}
		}
		var record map[string]any
		for _, r := range s.reads("inc-436", "") {
			for _, v := range r {
				if v == sql {
					record = r
				}
			}
		}
		require(record != nil, "statement in SQL read log")
		require(record["sqlstate"] == "00000" && integer(record["released_rows"]) == 31 && integer(record["charged_rows_returned"]) >= 31, record)
		s.q(s.controller, `SHOW TASK "inc-436"`)
		require(integer(s.exposure()[[2]string{"charged", "rows_returned"}]) >= 31, "disclosure charge")
	case "02", "03":
		sql := "SELECT card_token FROM shipments"
		hidden := "card_token"
		if step == "03" {
			sql = "SELECT id FROM shipments WHERE customer_email LIKE 'a%'"
			hidden = "customer_email"
		}
		_, e := query(s.agent, sql, false, &s.attempts)
		require(e != nil, "hidden column or masked filter accepted")
		p := errorPair(e)
		require(p[0] == "42501" && !strings.Contains(p[1], hidden), p)
		if step == "02" {
			for _, r := range s.reads("inc-436", "") {
				for _, v := range r {
					if v == sql {
						for _, k := range []string{"rows_scanned", "charged_rows_read", "released_rows"} {
							require(integer(r[k]) == 0, r)
						}
					}
				}
			}
		}
	case "04":
		s.fail(s.agent, "BEGIN", "transactions")
	case "05":
		s.a1 = requeue("a1", "id <= 4")
		require(s.q(s.agent, s.a1).Count == 4, "4 changed")
		s.r1 = s.lookup("a1")
		s.counters(4, 4)
		require(equal(s.queued(), []int{1, 2, 3, 4}), "queued 1..4")
		s.q(s.controller, `SHOW CHANGES OF TASK "inc-436"`)
	case "06":
		again := s.connect("repair_agent", "inc-436", false)
		require(s.q(again, s.a1).Count == 4, "replay count")
		require(equal(s.lookup("a1", again), s.r1), "same receipt")
		require(equal(s.queued(), []int{1, 2, 3, 4}), "no duplicate changes")
	case "07":
		s.direct = requeue("a2", "id BETWEEN 5 AND 16")
		s.fail(s.agent, s.direct, "required")
		require(len(s.lookup("a2").Rows) == 0 && len(s.queued()) == 4, "a2 free")
	case "08":
		s.p1 = s.preview("id BETWEEN 5 AND 16", 12)
	case "09":
		require(s.q(s.app, "UPDATE shipments SET status='delivered',attempt_count=attempt_count+1 WHERE merchant_id=42 AND id=7").Count == 1, "app update")
		require(s.statuses()[7] == "delivered", "row 7 delivered")
	case "10":
		s.approve(s.p1)
		s.fail(s.agent, apply("a2", s.p1), "stale")
		require(len(s.lookup("a2").Rows) == 0 && len(s.queued()) == 4, "stale applied nothing")
	case "11":
		s.p2 = s.preview("id BETWEEN 5 AND 16", 11)
		require(!equal(s.p2, s.p1), "new preview")
		s.approve(s.p2)
		require(s.q(s.agent, apply("a2", s.p2)).Count == 11, "11 changed")
		s.r2 = s.lookup("a2")
		s.counters(15, 15)
		require(equal(s.queued(), []int{1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16}), "15 queued")
	case "12":
		s.fail(s.agent, s.direct, "conflict")
		require(equal(s.lookup("a2"), s.r2) && len(s.queued()) == 15, "receipt unchanged")
	case "13":
		p3 := s.preview("id >= 27", 5)
		s.approve(p3)
		s.q(s.app, "INSERT INTO shipments VALUES (42,32,'failed','2026-11-05 17:06:00+00',1,'customer32@example.com','tok_42_32')")
		s.fail(s.agent, apply("a3", p3), "stale")
		require(len(s.lookup("a3").Rows) == 0 && len(s.queued()) == 15, "stale applies nothing")
	case "13b":
		require(s.q(s.app, "UPDATE shipments SET status='failed' WHERE merchant_id=42 AND id BETWEEN 8 AND 16").Count == 9, "refail 9")
		require(s.q(s.agent, requeue("a4", "id BETWEEN 8 AND 12")).Count == 5, "a4")
		require(s.q(s.agent, requeue("a5", "id BETWEEN 13 AND 16")).Count == 4, "a5")
		require(len(s.queued()) == 15, "15 distinct queued")
		s.counters(15, 24)
	case "14":
		return s.race()
	case "15":
		a := s.q(s.agent, requeue("a6", "id = 40"))
		b := s.q(s.agent, requeue("a7", "id = 999"))
		require(equal(a, b) && a.Count == 0, "invisible == missing")
		require(integer(s.q(s.controller, "SELECT count(*) FROM shipments WHERE merchant_id=43 AND status='failed'").Rows[0][0]) == 40, "merchant 43 unchanged")
	case "16":
		sql := strings.ReplaceAll(s.spec.TaskSQL, "inc-436", "scan-limit")
		sql = strings.ReplaceAll(sql, "VISIBLE ROWS WHERE (merchant_id = 42)", "VISIBLE ROWS WHERE (status = 'failed')")
		sql = strings.ReplaceAll(sql, ", rows_scanned = 100000", "")
		s.q(s.controller, sql)
		s.secrets["scan-limit"] = text(s.q(s.controller, `ISSUE CREDENTIAL FOR TASK "scan-limit"`).Rows[0][2])
		scan := s.connect("repair_agent", "scan-limit", false)
		owner := s.connect("postgres", "", false)
		for chunk := 0; chunk < 101; chunk++ {
			values := make([]string, 1000)
			for i := 1; i <= 1000; i++ {
				n := chunk*1000 + i
				values[i-1] = fmt.Sprintf("(7,%d,'delivered',NULL,0,'bulk%d@example.com','tok_7_%d')", n, n, n)
			}
			s.q(owner, "INSERT INTO shipments VALUES "+strings.Join(values, ","))
		}
		s.fail(scan, "SELECT id, status FROM shipments WHERE attempt_count >= 0", "scanned")
		reads := s.reads("scan-limit", "")
		r := reads[len(reads)-1]
		require(integer(r["rows_scanned"]) > 100000 && integer(r["rows_scanned"]) <= 100200 && integer(r["released_rows"]) == 0 && r["sqlstate"] == "54T01", r)
	case "17":
		// Only a connection error is accepted; a successful PostgreSQL login is a failure.
		denied := false
		func() {
			defer func() {
				if p := recover(); p != nil {
					e, ok := p.(error)
					require(ok, p)
					// The refusal must be the password check (28P01), not an
					// unreachable listener.
					var pgErr *pgconn.PgError
					require(errors.As(e, &pgErr) && pgErr.Code == "28P01", e)
					denied = true
				}
			}()
			c := s.connect("repair_agent", "", true)
			_ = c.Close(ctx)
		}()
		require(denied, "TASKONLY password admitted on PostgreSQL")
	case "18":
		s.before = s.lookup("a2")
		s.close()
		fmt.Println("RESTART")
		line, e := bufio.NewReader(os.Stdin).ReadString('\n')
		check(e)
		require(strings.TrimSpace(line) == "RESTARTED", "restart acknowledgement")
		s.start()
		require(equal(s.lookup("a2"), s.before), "persisted receipt")
		require(integer(s.before.Rows[0][2]) == 11 && len(s.queued()) == 16, "receipt and rows")
	case "19":
		if delay := time.Until(time.UnixMilli(s.spec.ExpiresMS).Add(3 * time.Second)); delay > 0 {
			time.Sleep(delay)
		}
		require(equal(s.lookup("a2"), s.before), "outcome after expiry")
		s.fail(s.agent, requeue("a8", "id = 27"), "expired")
		require(s.statuses()[27] == "failed", "expired write refused")
	case "20":
		require(s.q(s.app, "UPDATE shipments SET status='delivered',attempt_count=attempt_count+1 WHERE merchant_id=42 AND id BETWEEN 1 AND 3").Count == 3, "app changed 3")
		r := s.q(s.approver, `PREVIEW UNDO OF TASK "inc-436"`)
		s.undo = r.Rows[0][:2]
		for i, n := range []int{13, 3, 9, 0} {
			require(integer(r.Rows[0][i+2]) == n, r)
		}
		lines := s.q(s.approver, fmt.Sprintf(`SHOW UNDO %v OF TASK "inc-436"`, s.undo[0])).Rows
		restores, conflicts := map[string]bool{}, map[string]bool{}
		for _, row := range lines {
			switch row[0] {
			case "restore":
				restores[text(row[1])] = true
				require(equal(row[2:5], []any{"status", "queued", "failed"}), "restored values")
			case "conflict":
				conflicts[text(row[1])] = true
				require(row[5] == "changed_since", "conflict reason")
			}
		}
		require(len(restores) == 13 && len(conflicts) == 3, lines)
		s.beforeUndo = s.statuses()
	case "21":
		s.fail(s.approver, s.undoSQL("COMPLETE"), "undo")
		require(equal(s.statuses(), s.beforeUndo), "no complete undo applied")
	case "22":
		f := fields(s.q(s.approver, s.undoSQL("PARTIAL")))
		expected := map[string]string{"decision": "undo_partial", "mode": "partial", "restored": "13", "conflicts": "3", "superseded": "9", "row_count": "13", "distinct_charged": "0", "transitions_charged": "0", "distinct_after": "16", "transitions_after": "25", "executor": "repair_agent", "undone_by": "oncall_lead"}
		for k, v := range expected {
			require(text(f[k]) == v, f)
		}
		after := s.statuses()
		failed := 0
		for i, status := range after {
			want := "failed"
			if i == 1 || i == 2 || i == 3 || i == 7 {
				want = s.beforeUndo[i]
			}
			require(status == want, after)
			if status == "failed" {
				failed++
			}
		}
		require(failed == 28, "28 failed")
		s.q(s.controller, `SHOW CHANGES OF TASK "inc-436"`)
		reads := s.reads("inc-436", " LIMIT 1000")
		e := s.exposure()
		require(integer(e[[2]string{"charged", "reconciled"}]) == 1, "SQL exposure reconciliation")
		for _, pair := range [][2]string{{"charged_rows_read", "rows_read"}, {"charged_rows_returned", "rows_returned"}, {"charged_bytes", "bytes_returned"}} {
			sum := 0
			for _, r := range reads {
				sum += integer(r[pair[0]])
			}
			require(sum == integer(e[[2]string{"charged", pair[1]}]), "read-log sum differs from exposure: "+pair[1])
		}
		s.counters(16, 25)
	}
	return "checked"
}
func (s *scenario) undoSQL(mode string) string {
	return fmt.Sprintf(`APPROVE UNDO %v OF TASK "inc-436" DIGEST '%v' MODE %s`, s.undo[0], s.undo[1], mode)
}
func (s *scenario) race() string {
	clients := make([]*pgx.Conn, 10)
	for i := range clients {
		clients[i] = s.connect("repair_agent", "inc-436", false)
	}
	type outcome struct {
		won      bool
		attempts int
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
			r, e := query(clients[k], requeue(fmt.Sprintf("r%d", k), fmt.Sprintf("id = %d", 17+k)), true, &results[k].attempts)
			if e != nil {
				require(errorPair(e) == expectedErrors["transitions"], e)
			} else {
				require(r.Count == 1, "winner count")
				results[k].won = true
			}
		}(k)
	}
	ready.Wait()
	close(barrier)
	done.Wait()
	winners := 0
	attempts := []int{}
	for _, r := range results {
		require(r.err == nil, r.err)
		if r.won {
			winners++
		}
		s.attempts = max(s.attempts, r.attempts)
		attempts = append(attempts, r.attempts)
	}
	require(winners == 1, results)
	for k, r := range results {
		receipt := s.lookup(fmt.Sprintf("r%d", k))
		require(len(receipt.Rows) == 1, fmt.Sprintf("r%d: missing outcome", k))
		want := "budget_exceeded"
		if r.won {
			want = "committed"
		}
		require(receipt.Rows[0][1] == want, receipt)
	}
	require(len(s.queued()) == 16, "16 distinct queued")
	s.counters(16, 25)
	return fmt.Sprintf("one winner; nine recorded transition-budget refusals; attempts %v", attempts)
}
func (s *scenario) save() {
	b, e := json.MarshalIndent(s.output, "", "  ")
	check(e)
	tmp := strings.TrimSuffix(s.spec.ResultsPath, filepath.Ext(s.spec.ResultsPath)) + ".tmp"
	check(os.WriteFile(tmp, append(b, '\n'), 0600))
	check(os.Rename(tmp, s.spec.ResultsPath))
}
func (s *scenario) runStep(step string) {
	s.attempts = 1
	r := stepResult{Step: step, Status: "PASS", Detail: "checked"}
	func() {
		defer func() {
			if p := recover(); p != nil {
				r.Status = "FAIL"
				r.Detail = fmt.Sprintf("%T: %v", p, p)
			}
		}()
		r.Detail = s.step(step)
	}()
	r.Attempts = s.attempts
	s.output.Steps = append(s.output.Steps, r)
	s.save()
}
func pinnedVersion() {
	b, ok := debug.ReadBuildInfo()
	require(ok, "missing build info")
	for _, d := range b.Deps {
		if d.Path == "github.com/jackc/pgx/v5" {
			require(d.Version == "v5.7.6" && d.Replace == nil, "requires linked pgx v5.7.6")
			return
		}
	}
	panic("missing pgx build dependency")
}
func main() {
	path := flag.String("spec", "", "driver protocol spec")
	flag.Parse()
	s := &scenario{attempts: 1, output: output{Steps: []stepResult{}, Deviations: deviations}}
	steps := []string{}
	for i := 1; i <= 22; i++ {
		steps = append(steps, fmt.Sprintf("%02d", i))
		if i == 13 {
			steps = append(steps, "13b")
		}
	}
	func() {
		defer s.close()
		defer func() {
			if p := recover(); p != nil {
				for _, step := range steps[len(s.output.Steps):] {
					s.output.Steps = append(s.output.Steps, stepResult{step, "FAIL", 1, fmt.Sprintf("scenario initialization failed: %T: %v", p, p)})
				}
				if s.spec.ResultsPath != "" {
					s.save()
				} else {
					fmt.Fprintln(os.Stderr, p)
				}
			}
		}()
		data, e := os.ReadFile(*path)
		check(e)
		check(json.Unmarshal(data, &s.spec))
		data, e = os.ReadFile(s.spec.SecretFile)
		check(e)
		check(json.Unmarshal(data, &s.secrets))
		pinnedVersion()
		s.start()
		for _, step := range steps {
			s.runStep(step)
		}
	}()
	for _, r := range s.output.Steps {
		if r.Status != "PASS" {
			os.Exit(2)
		}
	}
}
