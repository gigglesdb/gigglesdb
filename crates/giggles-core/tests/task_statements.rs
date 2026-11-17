//! GD-0061 R3-108: task statements executed through the core API
//! (`task::statements::execute`), with each statement parsed from its SQL
//! text: CREATE / REVOKE TASK, GRANT / REVOKE INSPECT ON TASK, and the SHOW
//! TASK / SHOW TASKS result (design-1 section 5: columns, visibility,
//! `expired`, canonical predicate text).
#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::relational::{SqlResult, Value};
use giggles_core::roles::{CatalogRole, NewRole, TablePrivilege};
use giggles_core::task::catalog::TaskDdlError;
use giggles_core::task::definition::WallClockMs;
use giggles_core::task::statements::{
    description, execute, parse, TaskOutcome, TaskStatement, SHOW_TASK_COLUMNS,
};
use giggles_core::{Admin, Engine, Limits};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// `EXPIRES '2099-12-31 00:00:00+00'` in Unix milliseconds.
const EXPIRES_MS: i64 = 4_102_358_400_000;

fn now() -> WallClockMs {
    WallClockMs(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
    )
}

/// A write task on `parcels`, issued by `controller`, run by `agent`,
/// approved by `lead`. Predicates are written in mixed case and spacing so
/// SHOW must print their canonical form.
fn create_sql(name: &str) -> String {
    format!(
        "CREATE TASK {name} \
           EXECUTOR agent APPROVER lead \
           ON parcels \
           VISIBLE ROWS WHERE (MERCHANT_ID=42) \
           READABLE (id, status) \
           MASKED (email) \
           ALLOW UPDATE (status) \
             OLD ROW WHERE (\"status\"='failed'  AND Id <= 10) \
             NEW ROW WHERE (Status = $$queued$$) \
             AT MOST 5 DISTINCT ROWS AND 6 TRANSITIONS \
             APPROVE ABOVE 2 \
           DISCLOSE AT MOST 100 ROWS READ, 50 ROWS RETURNED, 4096 BYTES RETURNED \
           WITH (statement_ms = 1500) \
           EXPIRES '2099-12-31 00:00:00+00'"
    )
}

/// A read-only task on `parcels`, issued by `other_issuer`, no approver.
fn create_read_only_sql(name: &str) -> String {
    format!(
        "CREATE TASK {name} \
           EXECUTOR agent \
           ON parcels \
           VISIBLE ROWS WHERE (merchant_id = 43) \
           READABLE (id) \
           DISCLOSE AT MOST 10 ROWS READ, 1024 BYTES RETURNED \
           EXPIRES '2099-12-31 00:00:00+00'"
    )
}

struct Fixture {
    path: PathBuf,
    admin: Option<Admin>,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let path = common::fresh(name);
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let f = Self {
            path,
            admin: Some(admin),
        };
        f.admin()
            .sql_session()
            .execute(
                "CREATE TABLE parcels (id bigint primary key, merchant_id bigint, \
                 status text, email text)",
                &[],
                None,
            )
            .unwrap();
        let roles = f.admin().roles();
        for (name, task_only, superuser) in [
            ("controller", false, false),
            ("other_issuer", false, false),
            ("lead", false, false),
            ("agent", true, false),
            ("auditor", false, false),
            ("stranger", false, false),
            ("dba", false, true),
        ] {
            roles
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    superuser,
                    ..NewRole::default()
                })
                .unwrap();
            if task_only {
                roles.set_task_only(name, true).unwrap();
            }
        }
        for name in ["controller", "other_issuer"] {
            roles
                .grant_table(
                    "parcels",
                    name,
                    &[TablePrivilege::Select, TablePrivilege::Update],
                )
                .unwrap();
        }
        f
    }

    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }

    fn role(&self, name: &str) -> CatalogRole {
        self.admin().roles().get(name).unwrap().unwrap()
    }

    /// Parses `sql` and executes it as `role` at `now`.
    fn run_at(&self, role: &str, sql: &str, now: WallClockMs) -> Result<TaskOutcome, TaskDdlError> {
        let stmt = parse(sql).unwrap_or_else(|| panic!("not a task statement: {sql}"));
        assert!(
            !matches!(stmt, TaskStatement::Error(_)),
            "{sql}: parsed to {stmt:?}"
        );
        execute(self.admin(), &self.role(role), &stmt, now)
    }

    fn run(&self, role: &str, sql: &str) -> Result<TaskOutcome, TaskDdlError> {
        self.run_at(role, sql, now())
    }

    fn tag(&self, role: &str, sql: &str) -> &'static str {
        match self.run(role, sql) {
            Ok(TaskOutcome::Tag(tag)) => tag,
            other => panic!("{sql} as {role}: {other:?}"),
        }
    }

    fn rows_at(&self, role: &str, sql: &str, now: WallClockMs) -> SqlResult {
        match self.run_at(role, sql, now) {
            Ok(TaskOutcome::Rows(desc, result)) => {
                let names: Vec<_> = desc.columns.iter().map(|c| c.name.as_str()).collect();
                assert_eq!(names, SHOW_TASK_COLUMNS, "{sql}");
                assert_eq!(result.columns, SHOW_TASK_COLUMNS, "{sql}");
                for row in &result.rows {
                    assert_eq!(row.values.len(), SHOW_TASK_COLUMNS.len(), "{sql}");
                }
                result
            }
            other => panic!("{sql} as {role}: {other:?}"),
        }
    }

    fn rows(&self, role: &str, sql: &str) -> SqlResult {
        self.rows_at(role, sql, now())
    }

    fn sqlstate(&self, role: &str, sql: &str) -> &'static str {
        match self.run(role, sql) {
            Err(e) => e.sqlstate,
            Ok(other) => panic!("{sql} as {role} succeeded: {other:?}"),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin = None;
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn col(result: &SqlResult, row: usize, name: &str) -> Value {
    let at = SHOW_TASK_COLUMNS.iter().position(|c| *c == name).unwrap();
    result.rows[row].values[at].clone()
}

fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

fn task_names(result: &SqlResult) -> Vec<String> {
    (0..result.rows.len())
        .map(|i| match col(result, i, "task") {
            Value::Text(name) => name,
            other => panic!("task column: {other:?}"),
        })
        .collect()
}

#[test]
fn create_show_revoke_roundtrip() {
    let f = Fixture::new("task_statements_roundtrip");
    assert_eq!(f.tag("controller", &create_sql("repair")), "CREATE TASK");
    let def = f.admin().tasks().get("repair").unwrap().unwrap();

    let shown = f.rows("controller", "SHOW TASK repair");
    assert_eq!(shown.rows.len(), 1);
    let expect = [
        ("task", text("repair")),
        ("id", Value::Int(def.id.0 as i64)),
        ("def_version", Value::Int(def.def_version as i64)),
        ("issuer", text("controller")),
        ("executor", text("agent")),
        ("approver", text("lead")),
        ("table_name", text("parcels")),
        ("readable", text("id, status")),
        ("masked", text("email")),
        ("untrusted_text", text("")),
        ("write_columns", text("status")),
        ("max_distinct_rows", Value::Int(5)),
        ("max_transitions", Value::Int(6)),
        ("max_rows_read", Value::Int(100)),
        ("max_rows_returned", Value::Int(50)),
        ("max_bytes_returned", Value::Int(4096)),
        ("approve_above", Value::Int(2)),
        ("revoked", Value::Bool(false)),
        ("expired", Value::Bool(false)),
        ("grants", text("")),
    ];
    for (name, value) in &expect {
        assert_eq!(&col(&shown, 0, name), value, "column {name}");
    }
    match col(&shown, 0, "limits") {
        Value::Text(limits) => assert!(
            limits.starts_with("statement_ms = 1500, rows_scanned = "),
            "{limits}"
        ),
        other => panic!("limits: {other:?}"),
    }
    assert!(
        matches!(col(&shown, 0, "expires_at"), Value::TimestampTz(_)),
        "expires_at is a timestamptz"
    );

    // The description of a non-SHOW statement is empty.
    let create = parse(&create_sql("other")).unwrap();
    assert!(description(&create).columns.is_empty());

    assert_eq!(f.tag("controller", "REVOKE TASK repair"), "REVOKE TASK");
    let after = f.rows("controller", "SHOW TASK repair");
    assert_eq!(col(&after, 0, "revoked"), Value::Bool(true));
    assert_eq!(
        col(&after, 0, "def_version"),
        Value::Int(def.def_version as i64 + 1),
        "REVOKE bumps def_version once"
    );
    // Revoking again succeeds with no write.
    assert_eq!(f.tag("controller", "REVOKE TASK repair"), "REVOKE TASK");
    let again = f.rows("controller", "SHOW TASK repair");
    assert_eq!(col(&again, 0, "def_version"), col(&after, 0, "def_version"));
}

#[test]
fn show_task_hidden_from_unrelated_role_42704() {
    let f = Fixture::new("task_statements_hidden");
    f.tag("controller", &create_sql("repair"));
    // A role with no relation to the task and an unknown name give the same
    // 42704, so existence does not leak.
    assert_eq!(f.sqlstate("stranger", "SHOW TASK repair"), "42704");
    assert_eq!(f.sqlstate("stranger", "SHOW TASK missing"), "42704");
    assert_eq!(f.sqlstate("controller", "SHOW TASK missing"), "42704");
    let hidden = f.run("stranger", "SHOW TASK repair").unwrap_err();
    let unknown = f.run("controller", "SHOW TASK \"Repair\"").unwrap_err();
    assert_eq!(
        hidden.message,
        unknown.message.replace("Repair", "repair"),
        "hidden and unknown read alike"
    );
    // The executor holds nothing, even though it is named in the task.
    assert_eq!(f.sqlstate("agent", "SHOW TASK repair"), "42704");
    // The named approver sees it.
    assert_eq!(f.rows("lead", "SHOW TASK repair").rows.len(), 1);
}

#[test]
fn show_tasks_filters_rows() {
    let f = Fixture::new("task_statements_filters");
    f.tag("controller", &create_sql("zeta"));
    f.tag("controller", &create_sql("alpha"));
    f.tag("other_issuer", &create_read_only_sql("mid"));

    assert_eq!(
        task_names(&f.rows("controller", "SHOW TASKS")),
        ["alpha", "zeta"],
        "issuer sees its own tasks, ordered by name"
    );
    assert_eq!(task_names(&f.rows("other_issuer", "SHOW TASKS")), ["mid"]);
    assert_eq!(
        task_names(&f.rows("lead", "SHOW TASKS")),
        ["alpha", "zeta"],
        "approver sees the tasks naming it"
    );
    assert_eq!(
        task_names(&f.rows("dba", "SHOW TASKS")),
        ["alpha", "mid", "zeta"],
        "superuser sees every task"
    );
    assert!(f.rows("stranger", "SHOW TASKS").rows.is_empty());
    assert!(f.rows("agent", "SHOW TASKS").rows.is_empty());

    // The read-only task prints NULL write columns and threshold.
    let mid = f.rows("dba", "SHOW TASK mid");
    for name in [
        "approver",
        "write_columns",
        "old_row",
        "new_row",
        "approve_above",
    ] {
        assert_eq!(col(&mid, 0, name), Value::Null, "column {name}");
    }
    assert_eq!(col(&mid, 0, "max_rows_returned"), Value::Int(10));
}

#[test]
fn inspect_grantee_sees_task() {
    let f = Fixture::new("task_statements_inspect");
    f.tag("controller", &create_sql("repair"));
    assert_eq!(f.sqlstate("auditor", "SHOW TASK repair"), "42704");

    assert_eq!(
        f.tag("controller", "GRANT INSPECT ON TASK repair TO auditor"),
        "GRANT"
    );
    let shown = f.rows("auditor", "SHOW TASK repair");
    assert_eq!(col(&shown, 0, "grants"), text("auditor (inspect)"));
    assert_eq!(task_names(&f.rows("auditor", "SHOW TASKS")), ["repair"]);
    // UNDO alone does not make a task visible.
    f.tag("controller", "GRANT UNDO ON TASK repair TO stranger");
    assert_eq!(f.sqlstate("stranger", "SHOW TASK repair"), "42704");
    assert_eq!(
        col(&f.rows("controller", "SHOW TASK repair"), 0, "grants"),
        text("auditor (inspect), stranger (undo)")
    );

    // Only the issuer or a superuser grants.
    assert_eq!(
        f.sqlstate("auditor", "GRANT INSPECT ON TASK repair TO stranger"),
        "42501"
    );

    assert_eq!(
        f.tag("controller", "REVOKE INSPECT ON TASK repair FROM auditor"),
        "REVOKE"
    );
    assert_eq!(f.sqlstate("auditor", "SHOW TASK repair"), "42704");
    assert!(f.rows("auditor", "SHOW TASKS").rows.is_empty());
}

#[test]
fn revoke_by_non_issuer_42501() {
    let f = Fixture::new("task_statements_revoke_42501");
    f.tag("controller", &create_sql("repair"));
    f.tag(
        "controller",
        "GRANT INSPECT, UNDO ON TASK repair TO auditor",
    );
    // Approver, grantee and an unrelated issuer may not revoke.
    for role in ["lead", "auditor", "other_issuer", "stranger"] {
        assert_eq!(f.sqlstate(role, "REVOKE TASK repair"), "42501", "{role}");
    }
    let def = f.admin().tasks().get("repair").unwrap().unwrap();
    assert!(!def.revoked);
    assert_eq!(
        col(&f.rows("controller", "SHOW TASK repair"), 0, "revoked"),
        Value::Bool(false)
    );
    // Revoking an unknown task is 42704.
    assert_eq!(f.sqlstate("controller", "REVOKE TASK missing"), "42704");
}

#[test]
fn superuser_may_revoke() {
    let f = Fixture::new("task_statements_superuser");
    f.tag("controller", &create_sql("repair"));
    let before = f.admin().tasks().get("repair").unwrap().unwrap();
    assert_eq!(f.tag("dba", "REVOKE TASK repair"), "REVOKE TASK");
    let after = f.admin().tasks().get("repair").unwrap().unwrap();
    assert!(after.revoked);
    assert_eq!(after.def_version, before.def_version + 1);
    let shown = f.rows("controller", "SHOW TASK repair");
    assert_eq!(col(&shown, 0, "revoked"), Value::Bool(true));
    assert_eq!(col(&shown, 0, "issuer"), text("controller"));
}

#[test]
fn show_reports_expired_without_write() {
    let f = Fixture::new("task_statements_expired");
    f.tag("controller", &create_sql("repair"));
    let before = f.admin().tasks().get("repair").unwrap().unwrap();
    assert_eq!(before.expires_at, WallClockMs(EXPIRES_MS));

    let just_before = f.rows_at(
        "controller",
        "SHOW TASK repair",
        WallClockMs(EXPIRES_MS - 1),
    );
    assert_eq!(col(&just_before, 0, "expired"), Value::Bool(false));
    // `expired` is `now >= expires_at`.
    for at in [EXPIRES_MS, EXPIRES_MS + 86_400_000] {
        let shown = f.rows_at("controller", "SHOW TASK repair", WallClockMs(at));
        assert_eq!(col(&shown, 0, "expired"), Value::Bool(true), "at {at}");
        assert_eq!(col(&shown, 0, "revoked"), Value::Bool(false), "at {at}");
        let listed = f.rows_at("controller", "SHOW TASKS", WallClockMs(at));
        assert_eq!(col(&listed, 0, "expired"), Value::Bool(true), "at {at}");
    }
    // SHOW wrote nothing: the stored definition is unchanged.
    let after = f.admin().tasks().get("repair").unwrap().unwrap();
    assert_eq!(*after, *before);
    assert!(!after.revoked);
}

#[test]
fn show_text_equals_canonical_predicates() {
    let f = Fixture::new("task_statements_canonical");
    f.tag("controller", &create_sql("repair"));
    let def = f.admin().tasks().get("repair").unwrap().unwrap();
    let write = def.allowed_write.as_ref().unwrap();
    let shown = f.rows("controller", "SHOW TASK repair");

    // SHOW prints the stored canonical text, byte for byte.
    assert_eq!(col(&shown, 0, "visible_rows"), text(&def.visible_rows.0));
    assert_eq!(col(&shown, 0, "old_row"), text(&write.old_row.0));
    assert_eq!(col(&shown, 0, "new_row"), text(&write.new_row.0));
    // And that text is the canonical form of what was written.
    assert_eq!(col(&shown, 0, "visible_rows"), text("merchant_id = 42"));
    assert_eq!(
        col(&shown, 0, "old_row"),
        text("status = 'failed' and id <= 10")
    );
    assert_eq!(col(&shown, 0, "new_row"), text("status = 'queued'"));

    // A task written with the canonical text shows identical predicates.
    let canonical_sql = format!(
        "CREATE TASK again \
           EXECUTOR agent APPROVER lead \
           ON parcels \
           VISIBLE ROWS WHERE ({}) \
           READABLE (id, status) \
           ALLOW UPDATE (status) \
             OLD ROW WHERE ({}) \
             NEW ROW WHERE ({}) \
             AT MOST 5 DISTINCT ROWS AND 6 TRANSITIONS \
             APPROVE ABOVE 2 \
           DISCLOSE AT MOST 100 ROWS READ, 4096 BYTES RETURNED \
           EXPIRES '2099-12-31 00:00:00+00'",
        def.visible_rows.0, write.old_row.0, write.new_row.0
    );
    f.tag("controller", &canonical_sql);
    let again = f.rows("controller", "SHOW TASK again");
    for name in ["visible_rows", "old_row", "new_row"] {
        assert_eq!(col(&again, 0, name), col(&shown, 0, name), "column {name}");
    }
}
