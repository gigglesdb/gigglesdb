//! GD-0061 R3-305: a task session's SQL reads see only the task's visible rows
//! (design-3 section 3). R3-306: so do its searched UPDATEs (through the
//! protected commit) and its native requests: a row outside the scope is never
//! matched, read or rewritten (worked example step 15).
//!
//! Task-session results stay unreleased until the release protocol (R3-308), so
//! these tests observe the scope without reading rows: an expression that
//! divides by zero on exactly one row shows whether that row reached the
//! statement (22012) or not, and `task::rows::take_last_counts` shows how many
//! rows the scoped source pulled and kept. Every case runs on the streaming
//! cursor path and on the buffered path. Writes are observed through the
//! receipt's `rows_affected` and the rows an untasked session reads afterwards.
#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    operations::{
        Arithmetic, Bindings, Compare, Error as NativeError, Expr, Intent, Request, ValueExpr,
    },
    relational::Value,
    roles::{NewRole, TablePrivilege},
    task::{
        Actor, OperationId, ProtectedRequest, Receipt, TaskError,
        context::open_context,
        definition::{
            AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText,
            TaskName, WallClockMs,
        },
        protected_commit,
        rows::{RowCounts, take_last_counts},
    },
};
use std::path::PathBuf;
use std::sync::Arc;

/// 2026-11-05 17:00:00 UTC.
const NOW: WallClockMs = WallClockMs(1_793_898_000_000);

/// Visible rows are `merchant_id = 42`: ids 1, 3 and 5. `attempt_count` is
/// zero only outside the scope (2, 4, 6); row 4 has a NULL `merchant_id`.
const ROWS: &[&str] = &[
    "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
     failed_at timestamp, attempt_count int, customer_email text, card_token text, note text)",
    "INSERT INTO shipments (id, merchant_id, status, attempt_count) VALUES \
     (1, 42, 'failed', 1), (2, 7, 'failed', 0), (3, 42, 'queued', 2), \
     (4, NULL, 'failed', 0), (5, 42, 'failed', 3), (6, 9, 'done', 0)",
];

struct Fixture {
    path: PathBuf,
    admin: Option<Admin>,
}

impl Fixture {
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }

    fn sql(&self, sql: &str) {
        self.admin()
            .sql_session()
            .execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }

    /// A fresh session bound to the task.
    fn agent(&self) -> SqlSession {
        let mut agent = self.admin().sql_session();
        let name = TaskName::new("inc-436").unwrap();
        let ctx = open_context(&agent, &name, &"repair_agent".to_string(), [7; 32], NOW, 0)
            .unwrap();
        agent.bind_task(ctx).unwrap();
        agent
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin = None;
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn fixture(name: &str) -> Fixture {
    fixture_with(name, false)
}

/// The fixture with the worked example's write: `status` from 'failed' to
/// 'queued', and the executor holding SELECT and UPDATE. It expires far
/// ahead of the real wall clock, which the protected commit checks.
fn writable(name: &str) -> Fixture {
    fixture_with(name, true)
}

fn fixture_with(name: &str, write: bool) -> Fixture {
    let path = common::fresh(name);
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let f = Fixture {
        path,
        admin: Some(admin),
    };
    for sql in ROWS {
        f.sql(sql);
    }
    let roles = f.admin().roles();
    for (name, task_only) in [("incident_controller", false), ("repair_agent", true)] {
        roles
            .create(NewRole {
                name: name.into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    let granted: &[TablePrivilege] = if write {
        &[TablePrivilege::Select, TablePrivilege::Update]
    } else {
        &[TablePrivilege::Select]
    };
    roles
        .grant_table("shipments", "incident_controller", granted)
        .unwrap();
    if write {
        roles.grant_table("shipments", "repair_agent", granted).unwrap();
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    f.admin()
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: "inc-436".into(),
                executor: "repair_agent".into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: ["id", "status", "attempt_count"].map(String::from).to_vec(),
                masked: Vec::new(),
                untrusted_text: Vec::new(),
                allowed_write: write.then(|| AllowedWrite {
                    columns: ["status".to_string()].into_iter().collect(),
                    old_row: PredicateText("status = 'failed'".into()),
                    new_row: PredicateText("status = 'queued'".into()),
                }),
                mutation_caps: if write {
                    MutationCaps {
                        distinct_rows: 25,
                        transitions: 25,
                    }
                } else {
                    MutationCaps::default()
                },
                disclosure_caps: DisclosureCaps {
                    rows_read: 1000,
                    rows_returned: 1000,
                    bytes_returned: 2_097_152,
                },
                limits: LoadLimits::default(),
                approval_threshold: if write { 25 } else { 0 },
                expires: if write {
                    "2099-12-31 00:00:00+00".into()
                } else {
                    "2026-11-05 17:30:00+00".into()
                },
            },
            if write { WallClockMs(now_ms()) } else { NOW },
        )
        .unwrap();
    f
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[derive(Clone, Copy, Debug)]
enum Path {
    Cursor,
    Buffered,
}
const PATHS: [Path; 2] = [Path::Cursor, Path::Buffered];

/// Runs `sql` to completion on `path`; the result stays unreleased.
fn run(s: &mut SqlSession, path: Path, sql: &str) -> Result<(), CoreError> {
    match path {
        Path::Buffered => s.execute_unreleased(sql, &[], None).map(|page| {
            assert_eq!(page.is_task(), s.task().is_some(), "{sql}");
        }),
        Path::Cursor => {
            let p = s.prepare(sql)?;
            let mut cursor = s.open_cursor(&p, &[])?;
            while !cursor.is_exhausted() {
                let page = cursor.fetch_unreleased(64, 1 << 20)?;
                assert_eq!(page.is_task(), s.task().is_some(), "{sql}");
            }
            Ok(())
        }
    }
}

/// `sql` succeeds in a fresh task session on `path`; returns the scoped counts.
fn ok(f: &Fixture, path: Path, sql: &str) -> RowCounts {
    let _ = take_last_counts();
    run(&mut f.agent(), path, sql).unwrap_or_else(|e| panic!("{path:?} {sql}: {e:?}"));
    take_last_counts().unwrap_or_else(|| panic!("{path:?} {sql}: no scoped read"))
}

/// `sql` fails with division by zero in a fresh task session on `path`.
fn divides_by_zero(f: &Fixture, path: Path, sql: &str) {
    match run(&mut f.agent(), path, sql) {
        Err(e) => assert_eq!(giggles_core::sqlstate(&e), "22012", "{path:?} {sql}: {e:?}"),
        Ok(()) => panic!("{path:?} {sql}: expected 22012"),
    }
}

/// Whether the row with `id` reaches a statement expression.
fn sees(f: &Fixture, path: Path, id: i64) -> bool {
    let sql = format!("SELECT 1 / (id - {id}) FROM shipments");
    match run(&mut f.agent(), path, &sql) {
        Ok(()) => false,
        Err(e) if giggles_core::sqlstate(&e) == "22012" => true,
        Err(e) => panic!("{path:?} {sql}: {e:?}"),
    }
}

#[test]
fn select_sees_only_scope_rows() {
    let f = fixture("task-rows-select");
    for path in PATHS {
        assert_eq!(
            ok(&f, path, "SELECT id, status FROM shipments"),
            RowCounts { scanned: 6, read: 3 },
            "{path:?}"
        );
        let seen: Vec<i64> = (1..=6).filter(|id| sees(&f, path, *id)).collect();
        assert_eq!(seen, [1, 3, 5], "{path:?}");
        // Primary-key point reads go through the same scope.
        ok(&f, path, "SELECT 1 / (id - 2) FROM shipments WHERE id = 2");
        divides_by_zero(&f, path, "SELECT 1 / (id - 3) FROM shipments WHERE id = 3");
        ok(&f, path, "SELECT 1 / (id - 6) FROM shipments WHERE id IN (5, 6)");
    }
}

#[test]
fn aggregate_counts_only_scope_rows() {
    let f = fixture("task-rows-aggregate");
    for path in PATHS {
        divides_by_zero(&f, path, "SELECT 1 / (count(*) - 3) FROM shipments");
        divides_by_zero(&f, path, "SELECT 1 / (sum(attempt_count) - 6) FROM shipments");
        divides_by_zero(&f, path, "SELECT 1 / (max(id) - 5) FROM shipments");
        divides_by_zero(
            &f,
            path,
            "SELECT 1 / (count(*) - 2) FROM shipments WHERE status = 'failed'",
        );
        let counts = ok(&f, path, "SELECT 1 / (count(*) - 6) FROM shipments");
        assert_eq!(counts.read, 3, "{path:?}");
        ok(
            &f,
            path,
            "SELECT status, 1 / count(*) FROM shipments GROUP BY status",
        );
    }
}

#[test]
fn erroring_expression_never_sees_out_of_scope_row() {
    let f = fixture("task-rows-erroring");
    for sql in [
        "SELECT id, 1 / attempt_count FROM shipments",
        "SELECT id FROM shipments WHERE 1 / attempt_count > 0",
        "SELECT id FROM shipments ORDER BY 1 / attempt_count",
        "SELECT sum(1 / attempt_count) FROM shipments",
        "SELECT id, 1 / attempt_count FROM shipments WHERE id IN (2, 4, 6)",
    ] {
        for path in PATHS {
            // Outside a task session every statement reaches a zero.
            let mut admin = f.admin().sql_session();
            match run(&mut admin, path, sql) {
                Err(e) => assert_eq!(giggles_core::sqlstate(&e), "22012", "{path:?} {sql}"),
                Ok(()) => panic!("{path:?} {sql}: expected 22012 without a task"),
            }
            // In the task session no expression sees a row with a zero.
            ok(&f, path, sql);
        }
    }
}

#[test]
fn order_limit_within_scope() {
    let f = fixture("task-rows-order-limit");
    for path in PATHS {
        // The first two scope rows by id are 1 and 3 (not 1 and 2).
        divides_by_zero(
            &f,
            path,
            "SELECT 1 / (max(id) - 3) FROM (SELECT id FROM shipments ORDER BY id LIMIT 2) s",
        );
        divides_by_zero(
            &f,
            path,
            "SELECT 1 / (min(id) - 5) FROM (SELECT id FROM shipments ORDER BY id DESC LIMIT 1) s",
        );
        divides_by_zero(
            &f,
            path,
            "SELECT 1 / (min(id) - 3) FROM \
             (SELECT id FROM shipments ORDER BY id LIMIT 1 OFFSET 1) s",
        );
        divides_by_zero(&f, path, "SELECT 1 / (id - 3) FROM shipments ORDER BY id LIMIT 2");
        let counts = ok(&f, path, "SELECT 1 / (id - 5) FROM shipments ORDER BY id LIMIT 2");
        if matches!(path, Path::Cursor) {
            // The streaming cursor stops at the second scope row.
            assert_eq!(counts.read, 2, "{counts:?}");
        }
    }
}

#[test]
fn secondary_index_used_with_pushdown() {
    let f = fixture("task-rows-index");
    let values: Vec<String> = (100..300)
        .map(|id| format!("({id}, {}, 'other', 0)", 1000 + id))
        .collect();
    f.sql(&format!(
        "INSERT INTO shipments (id, merchant_id, status, attempt_count) VALUES {}",
        values.join(", ")
    ));
    let full = RowCounts { scanned: 206, read: 3 };
    for path in PATHS {
        assert_eq!(ok(&f, path, "SELECT id FROM shipments"), full, "{path:?}");
    }
    f.sql("CREATE INDEX shipments_merchant ON shipments (merchant_id)");
    let indexed = RowCounts { scanned: 3, read: 3 };
    for path in PATHS {
        // The scope's `merchant_id = 42` reaches the index: only scope rows are pulled.
        assert_eq!(ok(&f, path, "SELECT id FROM shipments"), indexed, "{path:?}");
        assert_eq!(
            ok(&f, path, "SELECT id FROM shipments WHERE status = 'failed'"),
            indexed,
            "{path:?}"
        );
        let seen: Vec<i64> = (1..=6).filter(|id| sees(&f, path, *id)).collect();
        assert_eq!(seen, [1, 3, 5], "{path:?}");
        ok(&f, path, "SELECT id, 1 / attempt_count FROM shipments");
    }
    // A statement predicate on the same column intersects with the scope.
    assert_eq!(
        ok(&f, Path::Cursor, "SELECT id FROM shipments WHERE merchant_id = 7"),
        RowCounts { scanned: 0, read: 0 }
    );
}

#[test]
fn null_predicate_is_invisible() {
    let f = fixture("task-rows-null");
    for path in PATHS {
        // Row 4's predicate is NULL, not true: it is invisible.
        assert!(!sees(&f, path, 4), "{path:?}");
        divides_by_zero(
            &f,
            path,
            "SELECT 1 / count(*) FROM shipments WHERE merchant_id IS NULL",
        );
        ok(&f, path, "SELECT 1 / (id - 4) FROM shipments WHERE id = 4");
        // Outside a task session the row is there.
        let mut admin = f.admin().sql_session();
        run(
            &mut admin,
            path,
            "SELECT 1 / count(*) FROM shipments WHERE merchant_id IS NULL",
        )
        .unwrap();
    }
}

#[test]
fn no_task_session_unchanged() {
    let f = fixture("task-rows-untasked");
    f.sql("CREATE INDEX shipments_merchant ON shipments (merchant_id)");
    let _ = take_last_counts();
    let mut s = f.admin().sql_session();
    let rows = s
        .execute("SELECT id FROM shipments ORDER BY id", &[], None)
        .unwrap()
        .rows;
    let ids: Vec<_> = rows.iter().map(|row| row.values[0].clone()).collect();
    assert_eq!(ids.len(), 6);
    let count = s.execute("SELECT count(*) FROM shipments", &[], None).unwrap().rows;
    assert_eq!(count[0].values[0], giggles_core::relational::Value::Int(6));
    let p = s.prepare("SELECT id FROM shipments ORDER BY id").unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    let mut fetched = Vec::new();
    while !cursor.is_exhausted() {
        fetched.extend(cursor.fetch(4, 1 << 20).unwrap());
    }
    assert_eq!(
        fetched.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
        ids
    );
    for path in PATHS {
        let mut s = f.admin().sql_session();
        divides_by_zero_untasked(&mut s, path, "SELECT 1 / (id - 4) FROM shipments");
    }
    // No scoped read ran.
    assert_eq!(take_last_counts(), None);
}

fn divides_by_zero_untasked(s: &mut SqlSession, path: Path, sql: &str) {
    match run(s, path, sql) {
        Err(e) => assert_eq!(giggles_core::sqlstate(&e), "22012", "{path:?} {sql}: {e:?}"),
        Ok(()) => panic!("{path:?} {sql}: expected 22012"),
    }
}

// R3-306: searched UPDATE and native requests.

/// `status` of rows 1..=6, read outside the task.
fn statuses(f: &Fixture) -> Vec<String> {
    f.admin()
        .sql_session()
        .execute("SELECT status FROM shipments ORDER BY id", &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match &row.values[0] {
            Value::Text(text) => text.clone(),
            other => panic!("{other:?}"),
        })
        .collect()
}

const BEFORE: [&str; 6] = ["failed", "failed", "queued", "failed", "failed", "done"];
const REQUEUED: [&str; 6] = ["queued", "failed", "queued", "failed", "queued", "done"];

/// `text` through the protected commit, as the bound executor.
fn commit(f: &Fixture, op: &str, text: &str) -> Result<Receipt, TaskError> {
    let mut agent = f.agent();
    let ctx = agent.task().unwrap().clone();
    let request = ProtectedRequest::Direct {
        statement: agent.prepare(text).unwrap(),
        params: Vec::new(),
        text: Arc::from(text),
    };
    protected_commit(
        &mut agent,
        Actor::Executor(&ctx),
        &OperationId::new(op.as_bytes()).unwrap(),
        request,
    )
}

/// `text` commits in the task with `affected` rows; returns the scoped counts.
fn commits(f: &Fixture, op: &str, text: &str, affected: u32) -> RowCounts {
    let _ = take_last_counts();
    let receipt = commit(f, op, text).unwrap_or_else(|e| panic!("{text}: {e:?}"));
    assert_eq!(receipt.rows_affected, affected, "{text}");
    take_last_counts().unwrap_or_else(|| panic!("{text}: no scoped read"))
}

/// Outside a task `sql` fails with division by zero and changes nothing.
fn untasked_divides_by_zero(f: &Fixture, sql: &str) {
    match f.admin().sql_session().execute(sql, &[], None) {
        Err(e) => assert_eq!(giggles_core::sqlstate(&e), "22012", "{sql}: {e:?}"),
        Ok(_) => panic!("{sql}: expected 22012 without a task"),
    }
    assert_eq!(statuses(f), BEFORE, "{sql}");
}

#[test]
fn update_out_of_scope_matches_zero_rows() {
    // Rows 2 and 4 are 'failed' but outside the scope.
    let f = writable("task-rows-update-scope");
    let counts = commits(
        &f,
        "outside",
        "UPDATE shipments SET status = 'queued' WHERE merchant_id = 7",
        0,
    );
    assert_eq!(counts, RowCounts { scanned: 6, read: 3 });
    commits(
        &f,
        "null",
        "UPDATE shipments SET status = 'queued' WHERE merchant_id IS NULL",
        0,
    );
    assert_eq!(statuses(&f), BEFORE);
    // Before R3-306 rows 2 and 4 matched too and the policy denied the whole
    // request (OutsideVisibleRows); now only the visible rows 1 and 5 match.
    commits(
        &f,
        "failed",
        "UPDATE shipments SET status = 'queued' WHERE status = 'failed'",
        2,
    );
    assert_eq!(statuses(&f), REQUEUED);
}

#[test]
fn update_by_pk_out_of_scope_matches_zero_rows() {
    let f = writable("task-rows-update-pk");
    for (op, id) in [("two", 2), ("four", 4), ("six", 6)] {
        let counts = commits(
            &f,
            op,
            &format!("UPDATE shipments SET status = 'queued' WHERE id = {id}"),
            0,
        );
        // The point read pulled the row and dropped it.
        assert_eq!(counts, RowCounts { scanned: 1, read: 0 }, "id {id}");
    }
    assert_eq!(statuses(&f), BEFORE);
    let counts = commits(
        &f,
        "mixed",
        "UPDATE shipments SET status = 'queued' WHERE id IN (1, 2, 4)",
        1,
    );
    assert_eq!(counts, RowCounts { scanned: 3, read: 1 });
    assert_eq!(
        statuses(&f),
        ["queued", "failed", "queued", "failed", "failed", "done"]
    );
}

#[test]
fn update_set_source_reads_only_scope_rows() {
    // `attempt_count` is zero only outside the scope: no expression of the
    // statement (SET, WHERE, the key-selecting subquery) may see such a row.
    for (name, sql) in [
        (
            "task-rows-update-set",
            "UPDATE shipments SET status = CASE WHEN 1 / attempt_count > 0 \
             THEN 'queued' ELSE 'queued' END WHERE status = 'failed'",
        ),
        (
            "task-rows-update-where",
            "UPDATE shipments SET status = 'queued' \
             WHERE 1 / attempt_count >= 0 AND status = 'failed'",
        ),
        (
            "task-rows-update-subquery",
            "UPDATE shipments SET status = 'queued' WHERE status = 'failed' \
             AND id IN (SELECT id FROM shipments WHERE 1 / attempt_count >= 0)",
        ),
    ] {
        let f = writable(name);
        untasked_divides_by_zero(&f, sql);
        commits(&f, "requeue", sql, 2);
        assert_eq!(statuses(&f), REQUEUED, "{sql}");
    }
}

fn int(value: i64) -> Expr {
    Expr::Value(ValueExpr::Literal(Value::Int(value)))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn compare(a: Expr, op: Compare, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), op, Box::new(b))
}
fn and(a: Expr, b: Expr) -> Expr {
    Expr::And(Box::new(a), Box::new(b))
}
fn divide(a: Expr, b: Expr) -> Expr {
    Expr::Arithmetic(Box::new(a), Arithmetic::Divide, Box::new(b))
}
/// `1 / attempt_count >= 0`: true on every visible row, fails on exactly the
/// out-of-scope rows.
fn inverse_attempts() -> Expr {
    compare(divide(int(1), field("attempt_count")), Compare::Ge, int(0))
}
fn failed() -> Expr {
    compare(
        field("status"),
        Compare::Eq,
        Expr::Value(ValueExpr::Literal(Value::Text("failed".into()))),
    )
}
fn id_is(id: i64) -> Expr {
    compare(field("id"), Compare::Eq, int(id))
}
fn native_read(filter: Option<Expr>) -> Request {
    Request::Read {
        table: "shipments".into(),
        filter,
        order: Vec::new(),
        limit: Some(ValueExpr::Literal(Value::Int(1024))),
        projection: Vec::new(),
    }
}
fn native_requeue(filter: Expr, intent: Intent) -> Request {
    Request::Update {
        table: "shipments".into(),
        filter,
        guard: None,
        values: vec![(
            "status".into(),
            ValueExpr::Literal(Value::Text("queued".into())),
        )],
        intent,
    }
}
fn many(cap: i64) -> Intent {
    Intent::Many {
        cap: ValueExpr::Literal(Value::Int(cap)),
    }
}

/// `request` in a fresh task session; returns the counts of its last scoped
/// read. A native update writes each matched row through a keyed statement,
/// which reads that row through the scope again, so its last read is that
/// one row.
fn native(f: &Fixture, request: &Request) -> Result<RowCounts, NativeError> {
    let _ = take_last_counts();
    let page = f.agent().execute_native_unreleased(request, &Bindings::new())?;
    assert!(page.is_task());
    Ok(take_last_counts().expect("no scoped native read"))
}

/// `request` outside a task fails with `error`.
fn untasked_native_fails(f: &Fixture, request: &Request, error: NativeError) {
    assert_eq!(
        f.admin()
            .sql_session()
            .execute_native(request, &Bindings::new())
            .unwrap_err(),
        error
    );
}

#[test]
fn native_read_and_update_scope() {
    let f = writable("task-rows-native");
    // A full read pulls every row and keeps the three visible ones.
    assert_eq!(
        native(&f, &native_read(None)),
        Ok(RowCounts { scanned: 6, read: 3 })
    );
    // An expression that fails on the out-of-scope rows never sees them.
    let probe = native_read(Some(inverse_attempts()));
    untasked_native_fails(&f, &probe, NativeError::DivisionByZero);
    assert_eq!(native(&f, &probe), Ok(RowCounts { scanned: 6, read: 3 }));
    // A primary-key read of an invisible row reads nothing ...
    let point = native_read(Some(and(id_is(2), inverse_attempts())));
    untasked_native_fails(&f, &point, NativeError::DivisionByZero);
    assert!(native(&f, &point).is_ok());
    // ... and of a visible one reaches its expressions (1 / (2 - 2)).
    let visible = native_read(Some(and(
        id_is(3),
        compare(
            divide(
                int(1),
                Expr::Arithmetic(
                    Box::new(field("attempt_count")),
                    Arithmetic::Subtract,
                    Box::new(int(2)),
                ),
            ),
            Compare::Gt,
            int(0),
        ),
    )));
    assert_eq!(native(&f, &visible), Err(NativeError::DivisionByZero));
    // R3-308: a native write in a task session runs only through the
    // protected commit; the native exit refuses it before it runs, visible
    // key or not. (Its scoped matching is shown in the lib test
    // `task::rows::tests::update_and_native_outcomes_count_only_scope_rows`.)
    for request in [
        native_requeue(id_is(2), Intent::One),
        native_requeue(id_is(1), Intent::One),
        native_requeue(and(failed(), inverse_attempts()), many(10)),
    ] {
        assert_eq!(native(&f, &request), Err(refused_write()), "{request:?}");
    }
    assert_eq!(statuses(&f), BEFORE);
    // Outside the task the same filter reaches the out-of-scope zero.
    let request = native_requeue(and(failed(), inverse_attempts()), many(10));
    untasked_native_fails(&f, &request, NativeError::DivisionByZero);
    assert_eq!(statuses(&f), BEFORE);
}

/// A native write refused in a task session (0A000, before it runs).
fn refused_write() -> NativeError {
    NativeError::Sql {
        sqlstate: "0A000".into(),
    }
}

#[test]
fn native_matched_count_excludes_out_of_scope() {
    // Four rows are 'failed' (1, 2, 4, 5); two of them (1, 5) are visible.
    let f = writable("task-rows-native-matched");
    let request = native_requeue(failed(), many(2));
    // Outside the task four rows match, more than the cap.
    untasked_native_fails(&f, &request, NativeError::TooManyMatches);
    // Row 1 is visible and row 2 is not: outside the task both match.
    let one = native_requeue(
        and(failed(), Expr::Or(Box::new(id_is(1)), Box::new(id_is(2)))),
        Intent::One,
    );
    untasked_native_fails(&f, &one, NativeError::AmbiguousMatch);
    assert_eq!(statuses(&f), BEFORE);
    // The matching read of the same filter pulls six rows and keeps three;
    // the native write itself is refused in the task session (R3-308).
    assert_eq!(
        native(&f, &native_read(Some(failed()))),
        Ok(RowCounts { scanned: 6, read: 3 })
    );
    assert_eq!(native(&f, &request), Err(refused_write()));
    assert_eq!(statuses(&f), BEFORE);
    // The SQL path on an identical table affects the same two rows (worked
    // example step 15, SQL and native alike), from the same scoped read.
    let g = writable("task-rows-native-matched-sql");
    let sql_counts = commits(
        &g,
        "requeue",
        "UPDATE shipments SET status = 'queued' WHERE status = 'failed'",
        2,
    );
    assert_eq!(statuses(&g), REQUEUED);
    assert_eq!(sql_counts, RowCounts { scanned: 6, read: 3 });
}
