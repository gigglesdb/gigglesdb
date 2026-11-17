//! GD-0061 R3: the task's column rules run inside `SqlSession::prepare`
//! (design-3 section 2, call site 1). A column outside the task's readable
//! and masked columns gets the rule's error whether it exists or not, before
//! any binder error (42703, an unknown function, a type error) could tell
//! the two apart. Sessions without a task are unchanged.
#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::Value,
    roles::{NewRole, TablePrivilege},
    task::{
        DenyRule, TaskError,
        context::{TaskContext, open_context},
        definition::{
            DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskName,
            WallClockMs,
        },
    },
};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// `merchant_id` exists but is hidden; `nosuch` does not exist.
const ROWS: &[&str] = &[
    "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
     attempt_count int, customer_email text, card_token text)",
    "INSERT INTO shipments VALUES (1, 42, 'failed', 1, 'ann@shop.example', 'tok_1'), \
     (2, 7, 'failed', 0, 'bob@shop.example', 'tok_2')",
];

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

struct Fixture {
    path: PathBuf,
    admin: Option<Admin>,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let path = common::fresh(name);
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let f = Self { path, admin: Some(admin) };
        for sql in ROWS {
            f.admin().sql_session().execute(sql, &[], None).unwrap();
        }
        let roles = f.admin().roles();
        for (name, task_only) in [("incident_controller", false), ("repair_agent", true)] {
            roles
                .create(NewRole { name: name.into(), login: true, ..NewRole::default() })
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
                    masked: vec![
                        ("customer_email".into(), None),
                        ("card_token".into(), Some(Value::Text("****".into()))),
                    ],
                    untrusted_text: Vec::new(),
                    allowed_write: None,
                    mutation_caps: MutationCaps::default(),
                    disclosure_caps: DisclosureCaps {
                        rows_read: 1000,
                        rows_returned: 1000,
                        bytes_returned: 1 << 20,
                    },
                    limits: LoadLimits::default(),
                    approval_threshold: 0,
                    expires: "2099-12-31 00:00:00+00".into(),
                },
                WallClockMs(now_ms()),
            )
            .unwrap();
        f
    }

    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }

    fn context(&self) -> TaskContext {
        open_context(
            &self.admin().sql_session(),
            &TaskName::new("inc-436").unwrap(),
            &"repair_agent".to_string(),
            [7; 32],
            WallClockMs(now_ms()),
            0,
        )
        .unwrap()
    }

    fn agent(&self) -> SqlSession {
        let mut s = self.admin().sql_session();
        s.bind_task(self.context()).unwrap();
        s
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin = None;
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn prepare_err(s: &mut SqlSession, sql: &str) -> CoreError {
    match s.prepare(sql) {
        Ok(_) => panic!("{sql}: prepared"),
        Err(e) => e,
    }
}

/// Each `(hidden, unknown)` pair fails at prepare with one error, equal in
/// value, SQLSTATE and message, and equal to `expected`.
fn same_error(f: &Fixture, pairs: &[(&str, &str)], expected: &CoreError) {
    for &(hidden, unknown) in pairs {
        let mut s = f.agent();
        let a = prepare_err(&mut s, hidden);
        let b = prepare_err(&mut s, unknown);
        assert_eq!(&a, expected, "{hidden}");
        assert_eq!(a, b, "{hidden} / {unknown}");
        assert_eq!(giggles_core::sqlstate(&a), giggles_core::sqlstate(&b), "{hidden}");
        assert_eq!(a.to_string(), b.to_string(), "{hidden}");
        assert_ne!(giggles_core::sqlstate(&a), "42703", "{hidden}");
    }
}

#[test]
fn nonexistent_and_hidden_column_get_same_error_at_prepare() {
    let f = Fixture::new("task-prepare-rules-columns");
    let denied = CoreError::Task(TaskError::Denied(DenyRule::ColumnNotAvailable));
    same_error(
        &f,
        &[
            ("SELECT merchant_id FROM shipments", "SELECT nosuch FROM shipments"),
            ("SELECT id, merchant_id AS m FROM shipments", "SELECT id, nosuch AS m FROM shipments"),
            ("SELECT id FROM shipments WHERE merchant_id = 7", "SELECT id FROM shipments WHERE nosuch = 7"),
            ("SELECT id FROM shipments ORDER BY merchant_id", "SELECT id FROM shipments ORDER BY nosuch"),
            (
                "SELECT id FROM shipments WHERE merchant_id IS NULL ORDER BY id",
                "SELECT id FROM shipments WHERE nosuch IS NULL ORDER BY id",
            ),
            ("SELECT ctid FROM shipments", "SELECT nosuch FROM shipments"),
        ],
        &denied,
    );
    // Inside a transaction too; the refusal aborts it like any prepare error.
    let mut s = f.agent();
    let _ = s.execute_unreleased("BEGIN", &[], None).unwrap();
    assert_eq!(prepare_err(&mut s, "SELECT nosuch FROM shipments"), denied);
    assert!(s.is_aborted());
}

#[test]
fn unknown_and_hidden_in_expression_same_error() {
    let f = Fixture::new("task-prepare-rules-expressions");
    let column = CoreError::Task(TaskError::Denied(DenyRule::ColumnNotAvailable));
    same_error(
        &f,
        &[
            (
                "SELECT lower(merchant_id::text) FROM shipments",
                "SELECT lower(nosuch::text) FROM shipments",
            ),
            ("SELECT id + merchant_id FROM shipments", "SELECT id + nosuch FROM shipments"),
            // A type error on the hidden column would come from the binder;
            // the rule error comes first.
            (
                "SELECT id FROM shipments WHERE merchant_id = 'not a number'",
                "SELECT id FROM shipments WHERE nosuch = 'not a number'",
            ),
            ("SELECT count(merchant_id) FROM shipments", "SELECT count(nosuch) FROM shipments"),
            (
                "SELECT id FROM shipments WHERE 1 / merchant_id > 0",
                "SELECT id FROM shipments WHERE 1 / nosuch > 0",
            ),
        ],
        &column,
    );
    // A function outside the allowlist: one error whether the function
    // exists or not, never 42883.
    let function = CoreError::Task(TaskError::Denied(DenyRule::FunctionNotAllowed));
    same_error(
        &f,
        &[
            ("SELECT md5(status) FROM shipments", "SELECT no_such_function(status) FROM shipments"),
            ("SELECT md5(status) FROM shipments", "SELECT md5(nosuch) FROM shipments"),
        ],
        &function,
    );
}

#[test]
fn readable_select_still_prepares() {
    let f = Fixture::new("task-prepare-rules-readable");
    let mut s = f.agent();
    for sql in [
        "SELECT id, status FROM shipments WHERE attempt_count > 0 ORDER BY id",
        "SELECT id, customer_email FROM shipments ORDER BY id DESC LIMIT 1",
        "SELECT count(*) FROM shipments WHERE status = 'failed'",
        "SELECT 1",
    ] {
        let p = s.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        s.describe(&p).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    // A star expands at prepare to the readable and masked columns, so
    // Describe shows the statement that runs.
    let p = s.prepare("SELECT * FROM shipments").unwrap();
    let names: Vec<String> =
        s.describe(&p).unwrap().columns.into_iter().map(|c| c.name).collect();
    assert_eq!(names, ["id", "status", "attempt_count", "customer_email", "card_token"]);
    // A prepared readable statement runs.
    let page = s.execute_prepared_unreleased(&p, &[], None).unwrap();
    let rows = s.release_page(page).unwrap().into_inner().rows;
    assert_eq!(rows.len(), 1);
}

#[test]
fn non_task_session_still_gets_42703() {
    let f = Fixture::new("task-prepare-rules-untasked");
    let mut s = f.admin().sql_session();
    // Parsing alone binds no column; Describe reports the missing one.
    let p = s.prepare("SELECT nosuch FROM shipments").unwrap();
    let e = s.describe(&p).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&e), "42703", "{e:?}");
    // The hidden-to-the-task column is an ordinary column here.
    let p = s.prepare("SELECT merchant_id FROM shipments ORDER BY merchant_id").unwrap();
    s.describe(&p).unwrap();
    let e = s.execute("SELECT nosuch FROM shipments", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&e), "42703", "{e:?}");
}
