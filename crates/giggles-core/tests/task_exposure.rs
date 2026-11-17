//! GD-0061 R3-501: the exposure report of an existing task (design-3 section
//! 8, spec section 10), through the public API, with the task created from
//! its `CREATE TASK` text.
//!
//! The worked example (spec section 14): before the agent starts, the
//! controller reads the report: 31 rows visible today, at most 1,000 rows
//! read, at most 25 rows changed, `status` only. `deliveries` is reserved, so
//! the table is `shipments`. Merchant 42 owns rows 1-31 (1-29 failed inside
//! the incident window, 30 failed late, 31 delivered); 100 and 101 belong to
//! merchant 43.
#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::relational::{SqlResult, SqlRow, Value};
use giggles_core::roles::{CatalogRole, NewRole, TablePrivilege};
use giggles_core::task::context::{open_context, TaskContext};
use giggles_core::task::definition::{
    NewTask, TaskName, TaskObjectId, TaskPrivileges, WallClockMs,
};
use giggles_core::task::catalog::TaskDdlError;
use giggles_core::task::exposure::{draft_exposure, report, show_exposure, LISTENER_DETAIL};
use giggles_core::task::statements::{parse, TaskStatement};
use giggles_core::task::{
    protected_commit, Actor, Budget, OperationId, ProtectedRequest, ReadRecord, StatementClass,
    TaskError,
};
use giggles_core::{Admin, CoreError, Engine, Limits, SqlSession};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const TASK: &str = "inc-436";

/// The worked example's caps: 1,000 rows read and returned, 2 MiB, 25 and 25.
const DISCLOSE: &str = "DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED";
const MUTATE: &str = "AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS APPROVE ABOVE 5";

const OLD_ROW: &str = "status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00' \
                       AND failed_at < '2026-11-05 17:07:00+00'";

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

fn create_task_sql(disclose: &str, mutate: &str) -> String {
    format!(
        "CREATE TASK \"{TASK}\" \
           EXECUTOR repair_agent APPROVER oncall_lead \
           ON shipments \
           VISIBLE ROWS WHERE (merchant_id = 42) \
           READABLE (id, status, failed_at, attempt_count, note, reason) \
           MASKED (customer_email) \
           UNTRUSTED TEXT (reason) \
           ALLOW UPDATE (status) \
             OLD ROW WHERE ({OLD_ROW}) \
             NEW ROW WHERE (status = 'queued') \
             {mutate} \
           {disclose} \
           WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2) \
           EXPIRES '2099-12-31 00:00:00+00'"
    )
}

struct Fixture {
    path: PathBuf,
    admin: Option<Admin>,
    task: TaskObjectId,
}

impl Fixture {
    fn new(name: &str) -> Self {
        Self::with(name, DISCLOSE, MUTATE)
    }

    fn with(name: &str, disclose: &str, mutate: &str) -> Self {
        let mut f = Self::bare(name);
        f.create(disclose, mutate);
        f
    }

    /// The table, its rows, labels, roles and grants; no task.
    fn bare(name: &str) -> Self {
        let path = common::fresh(name);
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let f = Self {
            path,
            admin: Some(admin),
            task: TaskObjectId(0),
        };
        f.sql(
            "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
             failed_at timestamptz, attempt_count int, customer_email text, card_token text, \
             note text, reason text)",
        );
        let mut rows: Vec<String> = (1..=29)
            .map(|id| {
                format!(
                    "({id}, 42, 'failed', '2026-11-05 17:03:00+00', 1, 'c{id}@shop.example', \
                     'tok_{id}', 'n{id}', 'r{id}')"
                )
            })
            .collect();
        rows.push(
            "(30, 42, 'failed', '2026-11-05 17:10:00+00', 1, 'late@shop.example', 'tok_30', \
             'n30', 'r30')"
                .into(),
        );
        rows.push(
            "(31, 42, 'delivered', NULL, 0, 'done@shop.example', 'tok_31', 'n31', 'r31')".into(),
        );
        for id in [100, 101] {
            rows.push(format!(
                "({id}, 43, 'failed', '2026-11-05 17:03:00+00', 1, 'o{id}@shop.example', \
                 'tok_{id}', 'n{id}', 'r{id}')"
            ));
        }
        f.sql(&format!("INSERT INTO shipments VALUES {}", rows.join(", ")));
        f.sql("SECURITY LABEL FOR giggles ON COLUMN shipments.note IS 'untrusted_text'");
        f.sql("SECURITY LABEL FOR giggles ON COLUMN shipments.reason IS 'untrusted_text'");
        let roles = f.admin().roles();
        for (name, task_only, superuser) in [
            ("incident_controller", false, false),
            ("oncall_lead", false, false),
            ("repair_agent", true, false),
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
        for name in ["incident_controller", "repair_agent"] {
            roles
                .grant_table(
                    "shipments",
                    name,
                    &[TablePrivilege::Select, TablePrivilege::Update],
                )
                .unwrap();
        }
        f
    }

    /// Creates the task from its CREATE TASK text and grants INSPECT to the
    /// auditor.
    fn create(&mut self, disclose: &str, mutate: &str) {
        let issuer = self.role("incident_controller");
        let Some(TaskStatement::Create(new)) = parse(&create_task_sql(disclose, mutate)) else {
            panic!("CREATE TASK did not parse");
        };
        let tasks = self.admin().tasks();
        tasks.create(&issuer, *new, WallClockMs(now_ms())).unwrap();
        tasks
            .grant(&issuer, TASK, "auditor", TaskPrivileges::INSPECT, true)
            .unwrap();
        self.task = tasks.get(TASK).unwrap().unwrap().id;
    }

    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }

    fn sql(&self, statement: &str) {
        self.admin()
            .sql_session()
            .execute(statement, &[], None)
            .unwrap_or_else(|e| panic!("{statement}: {e:?}"));
    }

    fn role(&self, name: &str) -> CatalogRole {
        self.admin().roles().get(name).unwrap().unwrap()
    }

    /// The report as `role` sees it on an ordinary session.
    fn report(&self, role: &str) -> Result<SqlResult, CoreError> {
        show_exposure(&self.admin().sql_session(), &self.role(role), TASK, &[])
    }

    fn issuer_report(&self) -> SqlResult {
        self.report("incident_controller").unwrap()
    }

    /// A session bound to the task, with exact debits.
    fn agent(&self) -> SqlSession {
        let mut s = self.admin().sql_session();
        let ctx = open_context(
            &s,
            &TaskName::new(TASK).unwrap(),
            &"repair_agent".to_string(),
            [7; 32],
            WallClockMs(now_ms()),
            0,
        )
        .unwrap();
        s.bind_task(ctx).unwrap();
        s.set_disclosure_chunk(0, 0);
        s
    }

    fn context(&self) -> TaskContext {
        let def = self.admin().tasks().get(TASK).unwrap().unwrap();
        TaskContext {
            task: def.id,
            name: def.name.clone(),
            executor: def.executor.clone(),
            bound_def_version: def.def_version,
            credential_fingerprint: [7; 32],
        }
    }

    /// `OPERATION '<id>' <text>` through the protected commit.
    fn commit(&self, id: &str, text: &str) -> Result<(), TaskError> {
        let mut session = self.admin().sql_session();
        let ctx = self.context();
        let request = ProtectedRequest::Direct {
            statement: session.prepare(text).unwrap(),
            params: Vec::new(),
            text: Arc::from(text),
        };
        let op = OperationId::new(id.as_bytes()).unwrap();
        protected_commit(&mut session, Actor::Executor(&ctx), &op, request).map(|_| ())
    }

    fn records(&self) -> Vec<ReadRecord> {
        self.admin().task_read_log(self.task).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin = None;
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Runs `sql` on the agent session and releases its page.
fn agent_read(s: &mut SqlSession, sql: &str) -> Result<Vec<SqlRow>, CoreError> {
    let page = s.execute_unreleased(sql, &[], None)?;
    s.release_page(page).map(|released| released.into_inner().rows)
}

fn requeue(from: i64, to: i64) -> String {
    format!(
        "UPDATE shipments SET status = 'queued' \
         WHERE status = 'failed' AND id >= {from} AND id <= {to}"
    )
}

type Line = (String, String, Option<String>, Option<i64>);

fn lines(result: &SqlResult) -> Vec<Line> {
    assert_eq!(result.columns, ["section", "item", "detail", "value"]);
    result
        .rows
        .iter()
        .map(|row| {
            let text = |v: &Value| match v {
                Value::Text(t) => Some(t.clone()),
                Value::Null => None,
                other => panic!("{other:?}"),
            };
            let value = match &row.values[3] {
                Value::Int(n) => Some(*n),
                Value::Null => None,
                other => panic!("{other:?}"),
            };
            (
                text(&row.values[0]).unwrap(),
                text(&row.values[1]).unwrap(),
                text(&row.values[2]),
                value,
            )
        })
        .collect()
}

fn line(section: &str, item: &str, detail: Option<&str>, value: Option<i64>) -> Line {
    (
        section.into(),
        item.into(),
        detail.map(String::from),
        value,
    )
}

/// The value of `section.item`.
fn value(result: &SqlResult, section: &str, item: &str) -> Option<i64> {
    lines(result)
        .into_iter()
        .find(|l| l.0 == section && l.1 == item)
        .unwrap_or_else(|| panic!("no row {section}.{item}"))
        .3
}

fn detail(result: &SqlResult, section: &str, item: &str) -> Option<String> {
    lines(result)
        .into_iter()
        .find(|l| l.0 == section && l.1 == item)
        .unwrap_or_else(|| panic!("no row {section}.{item}"))
        .2
}

fn sqlstate(e: &CoreError) -> String {
    giggles_core::sqlstate(e).to_owned()
}

#[test]
fn report_matches_definition() {
    let f = Fixture::new("task-exposure-definition");
    let def = f.admin().tasks().get(TASK).unwrap().unwrap();
    let write = def.allowed_write.clone().unwrap();
    let extra = [("credential".to_string(), "cn=repair-agent-1".to_string())];
    let shown = report(
        &f.admin().sql_session(),
        &f.role("incident_controller"),
        &def,
        &extra,
    )
    .unwrap();
    let got = lines(&shown);
    let expires = &got[4];
    assert_eq!((expires.0.as_str(), expires.1.as_str()), ("task", "expires_at"));
    assert_eq!(expires.3, Some(def.expires_at.0));
    assert!(
        expires.2.as_deref().unwrap().starts_with("2099-12-31 00:00:00"),
        "{expires:?}"
    );
    let expected = vec![
        line("task", "name", Some(TASK), None),
        line("task", "issuer", Some("incident_controller"), None),
        line("task", "executor", Some("repair_agent"), None),
        line("task", "approver", Some("oncall_lead"), None),
        expires.clone(),
        line("task", "revoked", None, Some(0)),
        line("task", "def_version", None, Some(def.def_version as i64)),
        line("table", "shipments", None, None),
        line("column", "id", Some("readable"), None),
        line("column", "merchant_id", Some("hidden"), None),
        line("column", "status", Some("readable, writable"), None),
        line("column", "failed_at", Some("readable"), None),
        line("column", "attempt_count", Some("readable"), None),
        line("column", "customer_email", Some("masked"), None),
        line("column", "card_token", Some("hidden"), None),
        line("column", "note", Some("readable, untrusted:withheld"), None),
        line("column", "reason", Some("readable, untrusted:allowed"), None),
        line("visible_rows", "predicate", Some(def.visible_rows.0.as_str()), None),
        // The worked example: 31 rows visible today.
        line("visible_rows", "matching_now", None, Some(31)),
        // At most 1,000 rows read.
        line("disclosure_cap", "rows_read", None, Some(1000)),
        line("disclosure_cap", "rows_returned", None, Some(1000)),
        line("disclosure_cap", "bytes_returned", None, Some(2_097_152)),
        // At most 25 rows changed.
        line("mutation_cap", "distinct_rows", None, Some(25)),
        line("mutation_cap", "transitions", None, Some(25)),
        line("write", "old_row", Some(write.old_row.0.as_str()), None),
        line("write", "new_row", Some(write.new_row.0.as_str()), None),
        line("load_limit", "statement_ms", None, Some(2000)),
        line("load_limit", "rows_scanned", None, Some(100_000)),
        line("load_limit", "concurrent_statements", None, Some(2)),
        line("load_limit", "lock_wait_ms", None, Some(def.limits.lock_wait_ms as i64)),
        line(
            "load_limit",
            "pending_write_bytes",
            None,
            Some(def.limits.pending_write_bytes as i64),
        ),
        line("load_limit", "stored_previews", None, Some(def.limits.stored_previews as i64)),
        line("load_limit", "preview_ttl_s", None, Some(def.limits.preview_ttl_s as i64)),
        line("listener", "agent", Some(LISTENER_DETAIL), None),
        line("listener", "credential", Some("cn=repair-agent-1"), None),
        line("charged", "rows_read", None, Some(0)),
        line("charged", "rows_returned", None, Some(0)),
        line("charged", "bytes_returned", None, Some(0)),
        line("charged", "statements", None, Some(0)),
        line("charged", "untrusted_released", None, Some(0)),
        line("charged", "withheld_untrusted", None, Some(0)),
        line("charged", "reconciled", None, Some(1)),
        // No mutation head yet: zeros.
        line("changed", "distinct_rows", None, Some(0)),
        line("changed", "transitions", None, Some(0)),
        line("changed", "operations", None, Some(0)),
    ];
    assert_eq!(got, expected);
    assert!(def.visible_rows.0.contains("merchant_id"));
    assert!(write.old_row.0.contains("failed_at"));
    // Step 5: four failed rows requeued under `a1`: 4 of 25 used.
    f.commit("a1", &requeue(1, 4)).unwrap();
    let after = f.issuer_report();
    assert_eq!(value(&after, "changed", "distinct_rows"), Some(4));
    assert_eq!(value(&after, "changed", "transitions"), Some(4));
    assert_eq!(value(&after, "changed", "operations"), Some(1));
    // Requeued rows are still visible rows.
    assert_eq!(value(&after, "visible_rows", "matching_now"), Some(31));
}

#[test]
fn show_task_exposure_parses() {
    assert_eq!(
        parse("SHOW TASK EXPOSURE \"inc-436\""),
        Some(TaskStatement::ShowExposure(TASK.into()))
    );
    assert_eq!(
        parse("show task exposure t1"),
        Some(TaskStatement::ShowExposure("t1".into()))
    );
    // A task named exposure.
    assert_eq!(
        parse("SHOW TASK exposure"),
        Some(TaskStatement::Show(Some("exposure".into())))
    );
    assert_eq!(parse("SHOW TASK EXPOSURE t1 t2"), Some(TaskStatement::Error("42601")));
    assert_eq!(
        giggles_core::task::statements::command_tag(&TaskStatement::ShowExposure("t".into())),
        "SHOW"
    );
}

#[test]
fn matching_now_tracks_inserts() {
    let f = Fixture::new("task-exposure-inserts");
    assert_eq!(value(&f.issuer_report(), "visible_rows", "matching_now"), Some(31));
    f.sql(
        "INSERT INTO shipments VALUES (32, 42, 'failed', '2026-11-05 17:04:00+00', 1, \
         'new@shop.example', 'tok_32', 'n32', 'r32')",
    );
    assert_eq!(value(&f.issuer_report(), "visible_rows", "matching_now"), Some(32));
    // Another merchant's row is not visible.
    f.sql(
        "INSERT INTO shipments VALUES (102, 43, 'failed', NULL, 0, 'x@shop.example', \
         'tok_102', 'n102', 'r102')",
    );
    assert_eq!(value(&f.issuer_report(), "visible_rows", "matching_now"), Some(32));
    // Rows that move out of, or into, the scope follow.
    f.sql("UPDATE shipments SET merchant_id = 43 WHERE id IN (1, 2)");
    f.sql("UPDATE shipments SET merchant_id = 42 WHERE id = 100");
    assert_eq!(value(&f.issuer_report(), "visible_rows", "matching_now"), Some(31));
    f.sql("DELETE FROM shipments WHERE id = 3");
    assert_eq!(value(&f.issuer_report(), "visible_rows", "matching_now"), Some(30));
    // A larger table counts across engine pages.
    let more: Vec<String> = (1000..2200)
        .map(|id| format!("({id}, {}, 'queued', NULL, 0, NULL, NULL, NULL, NULL)", 42 + id % 2))
        .collect();
    f.sql(&format!("INSERT INTO shipments VALUES {}", more.join(", ")));
    assert_eq!(value(&f.issuer_report(), "visible_rows", "matching_now"), Some(630));
}

#[test]
fn charged_totals_equal_read_log() {
    let f = Fixture::new("task-exposure-charged");
    let mut s = f.agent();
    for sql in [
        "SELECT * FROM shipments WHERE status = 'failed'",
        "SELECT id, note, reason FROM shipments ORDER BY id",
        "SELECT id FROM shipments WHERE id <= 5",
        "SELECT 1",
    ] {
        agent_read(&mut s, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    // A denied statement is logged too.
    let e = agent_read(&mut s, "SELECT card_token FROM shipments").unwrap_err();
    assert_eq!(sqlstate(&e), "42501");
    drop(s);
    let shown = f.issuer_report();
    let records = f.records();
    let d = f.admin().task_disclosure(f.task).unwrap();
    let sum = |field: fn(&ReadRecord) -> u64| records.iter().map(field).sum::<u64>() as i64;
    assert_eq!(value(&shown, "charged", "rows_read"), Some(d.rows_read as i64));
    assert_eq!(value(&shown, "charged", "rows_read"), Some(sum(|r| r.charged_rows_read)));
    assert_eq!(
        value(&shown, "charged", "rows_returned"),
        Some(sum(|r| r.charged_rows_returned))
    );
    assert_eq!(
        value(&shown, "charged", "bytes_returned"),
        Some(sum(|r| r.charged_bytes))
    );
    assert_eq!(
        value(&shown, "charged", "untrusted_released"),
        Some(sum(|r| r.untrusted_released))
    );
    assert_eq!(
        value(&shown, "charged", "withheld_untrusted"),
        Some(sum(|r| r.withheld_untrusted))
    );
    assert_eq!(value(&shown, "charged", "statements"), Some(5));
    assert_eq!(records.len(), 5);
    assert_eq!(value(&shown, "charged", "reconciled"), Some(1));
    // Step 1 alone returned 30 failed rows; the second statement 31.
    assert_eq!(value(&shown, "charged", "rows_returned"), Some(30 + 31 + 5 + 1));
    // `note` is withheld and `reason` released, once per row in both reads.
    assert_eq!(value(&shown, "charged", "withheld_untrusted"), Some(61));
    assert_eq!(value(&shown, "charged", "untrusted_released"), Some(61));
    assert!(f.admin().task_read_verify(f.task).unwrap().ok());
}

#[test]
fn charged_never_exceeds_caps() {
    // Small disclosure caps; the worked example's mutation caps.
    let f = Fixture::with(
        "task-exposure-caps",
        "DISCLOSE AT MOST 40 ROWS READ, 10 ROWS RETURNED, 4096 BYTES RETURNED",
        MUTATE,
    );
    let check = |f: &Fixture| {
        let shown = f.issuer_report();
        for (charged, cap) in [
            (("charged", "rows_read"), ("disclosure_cap", "rows_read")),
            (("charged", "rows_returned"), ("disclosure_cap", "rows_returned")),
            (("charged", "bytes_returned"), ("disclosure_cap", "bytes_returned")),
            (("changed", "distinct_rows"), ("mutation_cap", "distinct_rows")),
            (("changed", "transitions"), ("mutation_cap", "transitions")),
        ] {
            let used = value(&shown, charged.0, charged.1).unwrap();
            let max = value(&shown, cap.0, cap.1).unwrap();
            assert!(used <= max, "{charged:?} {used} > {max}");
        }
        assert_eq!(value(&shown, "charged", "reconciled"), Some(1));
        shown
    };
    // Drive the disclosure budget to exhaustion, on an exact session and on
    // one with the default reservation chunk.
    let mut exact = f.agent();
    let mut chunked = f.agent();
    chunked.set_disclosure_chunk(256, 262_144);
    let mut exhausted = 0;
    for i in 0..20 {
        let s = if i % 2 == 0 { &mut exact } else { &mut chunked };
        match agent_read(s, "SELECT id, status FROM shipments WHERE id <= 3 ORDER BY id") {
            Ok(rows) => assert_eq!(rows.len(), 3),
            Err(CoreError::Task(TaskError::BudgetExceeded(_))) => exhausted += 1,
            Err(e) => panic!("{e:?}"),
        }
        check(&f);
    }
    assert!(exhausted > 0, "the disclosure budget was never exhausted");
    drop((exact, chunked));
    // Drive the mutation budget to exhaustion: five approved-size batches use
    // all 25 distinct rows, the sixth is refused.
    for (n, from) in [1, 6, 11, 16, 21].into_iter().enumerate() {
        f.commit(&format!("b{n}"), &requeue(from, from + 4)).unwrap();
        check(&f);
    }
    let e = f.commit("b5", &requeue(26, 29)).unwrap_err();
    assert!(
        matches!(
            e,
            TaskError::BudgetExceeded(Budget::DistinctRows | Budget::Transitions)
        ),
        "{e:?}"
    );
    let shown = check(&f);
    assert_eq!(value(&shown, "changed", "distinct_rows"), Some(25));
    assert_eq!(value(&shown, "changed", "operations"), Some(5));
}

#[test]
fn untrusted_columns_reported() {
    let f = Fixture::new("task-exposure-untrusted");
    let shown = f.issuer_report();
    assert_eq!(
        detail(&shown, "column", "note").as_deref(),
        Some("readable, untrusted:withheld")
    );
    assert_eq!(
        detail(&shown, "column", "reason").as_deref(),
        Some("readable, untrusted:allowed")
    );
    // A label on a writable column, and on a hidden one (never shown).
    f.sql("SECURITY LABEL FOR giggles ON COLUMN shipments.status IS 'untrusted_text'");
    f.sql("SECURITY LABEL FOR giggles ON COLUMN shipments.card_token IS 'untrusted_text'");
    f.sql("SECURITY LABEL FOR giggles ON COLUMN shipments.reason IS NULL");
    let shown = f.issuer_report();
    assert_eq!(
        detail(&shown, "column", "status").as_deref(),
        Some("readable, writable, untrusted:withheld")
    );
    assert_eq!(detail(&shown, "column", "card_token").as_deref(), Some("hidden"));
    assert_eq!(detail(&shown, "column", "reason").as_deref(), Some("readable"));
    // The masked column, labelled and not allowed, is withheld.
    f.sql("SECURITY LABEL FOR giggles ON COLUMN shipments.customer_email IS 'untrusted_text'");
    assert_eq!(
        detail(&f.issuer_report(), "column", "customer_email").as_deref(),
        Some("masked, untrusted:withheld")
    );
}

#[test]
fn requires_issuer_or_inspect() {
    let f = Fixture::new("task-exposure-privilege");
    // Issuer, approver, a granted inspector and a superuser may read it.
    for name in ["incident_controller", "oncall_lead", "auditor", "dba"] {
        let shown = f.report(name).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        assert_eq!(value(&shown, "disclosure_cap", "rows_read"), Some(1000), "{name}");
    }
    // The live count runs as the caller: the auditor has no SELECT.
    let auditor = f.report("auditor").unwrap();
    assert_eq!(value(&auditor, "visible_rows", "matching_now"), None);
    assert_eq!(
        detail(&auditor, "visible_rows", "matching_now").as_deref(),
        Some("requires SELECT on the table")
    );
    assert_eq!(value(&f.report("oncall_lead").unwrap(), "visible_rows", "matching_now"), None);
    assert_eq!(value(&f.report("dba").unwrap(), "visible_rows", "matching_now"), Some(31));
    f.admin()
        .roles()
        .grant_table("shipments", "auditor", &[TablePrivilege::Select])
        .unwrap();
    assert_eq!(value(&f.report("auditor").unwrap(), "visible_rows", "matching_now"), Some(31));
    // Anyone else, the executor included, is refused.
    for name in ["stranger", "repair_agent"] {
        let e = f.report(name).unwrap_err();
        assert_eq!(sqlstate(&e), "42501", "{name}");
    }
    // Revoking the grant takes the privilege away.
    let issuer = f.role("incident_controller");
    f.admin()
        .tasks()
        .grant(&issuer, TASK, "auditor", TaskPrivileges::INSPECT, false)
        .unwrap();
    assert_eq!(sqlstate(&f.report("auditor").unwrap_err()), "42501");
    // An unknown task.
    let e = show_exposure(&f.admin().sql_session(), &issuer, "no-such-task", &[]).unwrap_err();
    assert_eq!(sqlstate(&e), "42704");
}

#[test]
fn refused_in_task_session() {
    let f = Fixture::new("task-exposure-task-session");
    let s = f.agent();
    for role in ["incident_controller", "repair_agent"] {
        let e = show_exposure(&s, &f.role(role), TASK, &[]).unwrap_err();
        assert_eq!(
            e,
            CoreError::Task(TaskError::NotAllowedInTaskSession(StatementClass::Show))
        );
        assert_eq!(sqlstate(&e), "0A000");
    }
    let def = f.admin().tasks().get(TASK).unwrap().unwrap();
    let e = report(&s, &f.role("incident_controller"), &def, &[]).unwrap_err();
    assert_eq!(sqlstate(&e), "0A000");
    assert!(f.records().is_empty());
}

#[test]
fn live_count_not_charged_to_task() {
    let f = Fixture::new("task-exposure-not-charged");
    let mut s = f.agent();
    agent_read(&mut s, "SELECT id FROM shipments ORDER BY id").unwrap();
    drop(s);
    let before = f.admin().task_disclosure(f.task).unwrap();
    let records = f.records();
    assert_eq!(records.len(), 1);
    for _ in 0..3 {
        let shown = f.issuer_report();
        assert_eq!(value(&shown, "visible_rows", "matching_now"), Some(31));
        assert_eq!(value(&shown, "charged", "rows_read"), Some(31));
        assert_eq!(value(&shown, "charged", "statements"), Some(1));
    }
    let after = f.admin().task_disclosure(f.task).unwrap();
    assert_eq!(
        (after.rows_read, after.rows_returned, after.bytes_returned, after.next_read_seq),
        (before.rows_read, before.rows_returned, before.bytes_returned, before.next_read_seq)
    );
    assert_eq!(f.records(), records);
    assert!(f.admin().task_read_verify(f.task).unwrap().ok());
}

// ---------------------------------------------------------------------------
// R3-502: the report of a draft, `SHOW TASK EXPOSURE FOR CREATE TASK ...`.

/// The worked example's draft statement, parsed.
fn draft_statement(disclose: &str, mutate: &str) -> NewTask {
    let sql = format!("SHOW TASK EXPOSURE FOR {}", create_task_sql(disclose, mutate));
    match parse(&sql) {
        Some(TaskStatement::ShowExposureDraft(new)) => *new,
        other => panic!("not a draft: {other:?}"),
    }
}

impl Fixture {
    fn draft(&self, role: &str, new: &NewTask) -> Result<SqlResult, TaskDdlError> {
        draft_exposure(
            &self.admin().sql_session(),
            &self.role(role),
            new,
            WallClockMs(now_ms()),
            &[],
        )
    }
}

/// The rows a draft and a created task share: everything except the
/// `charged` and `changed` sections, `def_version` and the listener rows.
fn comparable(result: &SqlResult) -> Vec<Line> {
    lines(result)
        .into_iter()
        .filter(|l| {
            !matches!(l.0.as_str(), "charged" | "changed" | "listener")
                && !(l.0 == "task" && l.1 == "def_version")
        })
        .collect()
}

#[test]
fn draft_parses() {
    let new = draft_statement(DISCLOSE, MUTATE);
    let Some(TaskStatement::Create(created)) = parse(&create_task_sql(DISCLOSE, MUTATE)) else {
        panic!("CREATE TASK did not parse");
    };
    assert_eq!(new, *created);
    assert_eq!(
        giggles_core::task::statements::command_tag(&TaskStatement::ShowExposureDraft(
            Box::new(new)
        )),
        "SHOW"
    );
    // A task named for; a draft cut short; a draft with trailing text.
    assert_eq!(
        parse("SHOW TASK EXPOSURE for"),
        Some(TaskStatement::ShowExposure("for".into()))
    );
    assert_eq!(
        parse("SHOW TASK EXPOSURE FOR CREATE TASK"),
        Some(TaskStatement::Error("42601"))
    );
    let trailing = format!(
        "SHOW TASK EXPOSURE FOR {} extra",
        create_task_sql(DISCLOSE, MUTATE)
    );
    assert_eq!(parse(&trailing), Some(TaskStatement::Error("42601")));
}

#[test]
fn draft_report_equals_report_after_create() {
    let mut f = Fixture::bare("task-exposure-draft-equal");
    let new = draft_statement(DISCLOSE, MUTATE);
    let draft = f.draft("incident_controller", &new).unwrap();
    let got = lines(&draft);
    // A draft has no charged or changed rows, and version 0.
    assert!(
        got.iter().all(|l| l.0 != "charged" && l.0 != "changed"),
        "{got:?}"
    );
    assert_eq!(value(&draft, "task", "def_version"), Some(0));
    assert_eq!(detail(&draft, "listener", "agent").as_deref(), Some(LISTENER_DETAIL));
    assert_eq!(value(&draft, "visible_rows", "matching_now"), Some(31));
    f.create(DISCLOSE, MUTATE);
    let created = f.issuer_report();
    assert_eq!(value(&created, "task", "def_version"), Some(1));
    assert_eq!(comparable(&draft), comparable(&created));
    // The draft is the created report's prefix up to the listener, with
    // version 0; the created report adds 7 charged and 3 changed rows.
    let mut prefix = lines(&created);
    assert_eq!(prefix.len(), got.len() + 7 + 3);
    prefix.truncate(got.len());
    for l in &mut prefix {
        if l.0 == "task" && l.1 == "def_version" {
            l.3 = Some(0);
        }
    }
    assert_eq!(got, prefix);
}

#[test]
fn draft_stores_nothing() {
    let mut f = Fixture::bare("task-exposure-draft-stores-nothing");
    let new = draft_statement(DISCLOSE, MUTATE);
    for role in ["incident_controller", "dba"] {
        f.draft(role, &new)
            .unwrap_or_else(|e| panic!("{role}: {e:?}"));
    }
    let tasks = f.admin().tasks();
    assert!(tasks.get(TASK).unwrap().is_none());
    assert!(tasks.list().unwrap().is_empty());
    // No id was allocated: the task created afterwards gets the id a fresh
    // database gives its first task.
    f.create(DISCLOSE, MUTATE);
    let mut fresh = Fixture::bare("task-exposure-draft-stores-nothing-fresh");
    fresh.create(DISCLOSE, MUTATE);
    assert_eq!(f.task, fresh.task);
    // Nothing was charged to the new task either.
    assert_eq!(value(&f.issuer_report(), "charged", "statements"), Some(0));
    assert!(f.records().is_empty());
}

#[test]
fn draft_invalid_definition_same_error_as_create() {
    let mut f = Fixture::bare("task-exposure-draft-invalid");
    let issuer = "incident_controller";
    let worked = create_task_sql(DISCLOSE, MUTATE);
    let cases: Vec<(&str, String)> = vec![
        // Row 9: a column that does not exist.
        (issuer, worked.replace("READABLE (id,", "READABLE (id, nope,")),
        // Row 6: the executor must be task-only.
        (issuer, worked.replace("EXECUTOR repair_agent", "EXECUTOR stranger")),
        // Row 5: an unknown approver.
        (issuer, worked.replace("APPROVER oncall_lead", "APPROVER nobody")),
        // Row 4: an unknown table.
        (issuer, worked.replace("ON shipments", "ON parcels")),
        // Row 14: an expiry in the past.
        (issuer, worked.replace("2099-12-31", "2001-01-01")),
        // Row 10: a writable column must be readable.
        (
            issuer,
            worked.replace("ALLOW UPDATE (status)", "ALLOW UPDATE (customer_email)"),
        ),
        // Row 8: the issuer must hold SELECT and UPDATE on the table.
        ("auditor", worked.clone()),
        // A task-only role cannot issue tasks.
        ("repair_agent", worked.clone()),
    ];
    for (role, sql) in &cases {
        let Some(TaskStatement::Create(new)) = parse(sql) else {
            panic!("did not parse: {sql}");
        };
        let draft = f.draft(role, &new).unwrap_err();
        let create = f
            .admin()
            .tasks()
            .create(&f.role(role), (*new).clone(), WallClockMs(now_ms()))
            .unwrap_err();
        assert_eq!(draft, create, "{role}: {sql}");
    }
    assert!(f.admin().tasks().list().unwrap().is_empty());
    // A duplicate name, once the task exists.
    f.create(DISCLOSE, MUTATE);
    let new = draft_statement(DISCLOSE, MUTATE);
    let draft = f.draft(issuer, &new).unwrap_err();
    let create = f
        .admin()
        .tasks()
        .create(&f.role(issuer), new.clone(), WallClockMs(now_ms()))
        .unwrap_err();
    assert_eq!(draft, create);
    assert_eq!(draft.sqlstate, "42710");
    // In a task session the draft is refused as every task SHOW is.
    let s = f.agent();
    let e = draft_exposure(&s, &f.role(issuer), &new, WallClockMs(now_ms()), &[]).unwrap_err();
    assert_eq!(e.sqlstate, "0A000");
}
