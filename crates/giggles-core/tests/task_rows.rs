//! GD-0061 R3-305: a task session's SQL reads see only the task's visible rows
//! (design-3 section 3).
//!
//! Task-session results stay unreleased until the release protocol (R3-308), so
//! these tests observe the scope without reading rows: an expression that
//! divides by zero on exactly one row shows whether that row reached the
//! statement (22012) or not, and `task::rows::take_last_counts` shows how many
//! rows the scoped source pulled and kept. Every case runs on the streaming
//! cursor path and on the buffered path.
#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    roles::{NewRole, TablePrivilege},
    task::{
        context::open_context,
        definition::{
            DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskName,
            WallClockMs,
        },
        rows::{RowCounts, take_last_counts},
    },
};
use std::path::PathBuf;

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
    roles
        .grant_table("shipments", "incident_controller", &[TablePrivilege::Select])
        .unwrap();
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
                allowed_write: None,
                mutation_caps: MutationCaps::default(),
                disclosure_caps: DisclosureCaps {
                    rows_read: 1000,
                    rows_returned: 1000,
                    bytes_returned: 2_097_152,
                },
                limits: LoadLimits::default(),
                approval_threshold: 0,
                expires: "2026-11-05 17:30:00+00".into(),
            },
            NOW,
        )
        .unwrap();
    f
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
