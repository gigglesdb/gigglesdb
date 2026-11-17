//! GD-0061 R3-309: the read log (design-3 section 6). Every statement of a
//! task session gets one record `2A|task|read_seq`, refused ones included;
//! the records reconcile with the debits `2B` and the disclosure head; and
//! `SHOW TASK READS` shows them to the task's inspectors only.
#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::{
    operations::{Bindings, Compare, Expr, Request, ValueExpr},
    relational::{SqlRow, Value},
    roles::{NewRole, TablePrivilege},
    task::definition::TaskObjectId,
    task::{
        context::open_context,
        definition::{
            DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskName,
            TaskPrivileges, WallClockMs,
        },
        readlog::{
            self, class_code, outcome_of, show_reads, ENTRY_NATIVE, ENTRY_PGWIRE_EXTENDED,
            OUTCOME_BUDGET, OUTCOME_COMPLETE, OUTCOME_DENIED, OUTCOME_ERROR, OUTCOME_OPEN,
        },
        ReadRecord, StatementClass, TaskError,
    },
    Admin, CoreError, CrashPoint, Engine, Limits, SqlSession,
};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// Visible rows are `merchant_id = 42`: ids 1, 3 and 5. `note` and `reason`
/// are marked untrusted; the task allows `reason` only, so `note` is withheld.
const ROWS: &[&str] = &[
    "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
     attempt_count int, customer_email text, card_token text, note text, reason text)",
    "INSERT INTO shipments VALUES \
     (1, 42, 'failed', 1, 'ann@shop.example', 'tok_live_1111', 'n1', 'r1'), \
     (2, 7, 'failed', 0, 'bob@shop.example', 'tok_live_2222', 'n2', 'r2'), \
     (3, 42, 'queued', 2, 'cy@shop.example', 'tok_live_3333', 'n3', 'r3'), \
     (4, 9, 'failed', 0, 'di@shop.example', 'tok_live_4444', 'n4', 'r4'), \
     (5, 42, 'failed', 3, 'ed@shop.example', 'tok_live_5555', 'n5', 'r5'), \
     (6, 9, 'done', 0, 'fay@shop.example', 'tok_live_6666', 'n6', 'r6')",
    "SECURITY LABEL FOR giggles ON COLUMN shipments.note IS 'untrusted_text'",
    "SECURITY LABEL FOR giggles ON COLUMN shipments.reason IS 'untrusted_text'",
];

const ALL: &str = "SELECT id, status FROM shipments ORDER BY id";

fn caps(rows_read: u64, rows_returned: u64, bytes_returned: u64) -> DisclosureCaps {
    DisclosureCaps {
        rows_read,
        rows_returned,
        bytes_returned,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

struct Fixture {
    path: PathBuf,
    admin: Option<Admin>,
    task: TaskObjectId,
}

impl Fixture {
    fn new(name: &str, caps: DisclosureCaps) -> Self {
        let path = common::fresh(name);
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut f = Self {
            path,
            admin: Some(admin),
            task: TaskObjectId(0),
        };
        for sql in ROWS {
            f.admin().sql_session().execute(sql, &[], None).unwrap();
        }
        let roles = f.admin().roles();
        for (name, task_only, superuser) in [
            ("incident_controller", false, false),
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
        roles
            .grant_table(
                "shipments",
                "incident_controller",
                &[TablePrivilege::Select],
            )
            .unwrap();
        let issuer = roles.get("incident_controller").unwrap().unwrap();
        let tasks = f.admin().tasks();
        tasks
            .create(
                &issuer,
                NewTask {
                    name: "inc-436".into(),
                    executor: "repair_agent".into(),
                    approver: None,
                    schema: None,
                    table: "shipments".into(),
                    visible_rows: PredicateText("merchant_id = 42".into()),
                    readable: ["id", "status", "attempt_count", "note", "reason"]
                        .map(String::from)
                        .to_vec(),
                    masked: vec![
                        ("customer_email".into(), None),
                        ("card_token".into(), Some(Value::Text("****".into()))),
                    ],
                    untrusted_text: vec!["reason".into()],
                    allowed_write: None,
                    mutation_caps: MutationCaps::default(),
                    disclosure_caps: caps,
                    limits: LoadLimits::default(),
                    approval_threshold: 0,
                    expires: "2099-12-31 00:00:00+00".into(),
                },
                WallClockMs(now_ms()),
            )
            .unwrap();
        tasks
            .grant(&issuer, "inc-436", "auditor", TaskPrivileges::INSPECT, true)
            .unwrap();
        f.task = tasks.get("inc-436").unwrap().unwrap().id;
        f
    }

    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }

    /// A fresh session bound to the task, with exact debits (no reservation).
    fn exact(&self) -> SqlSession {
        let s = self.agent();
        s.set_disclosure_chunk(0, 0);
        s
    }

    /// A fresh session bound to the task, with the default reservation chunk.
    fn agent(&self) -> SqlSession {
        let mut s = self.admin().sql_session();
        let ctx = open_context(
            &s,
            &TaskName::new("inc-436").unwrap(),
            &"repair_agent".to_string(),
            [7; 32],
            WallClockMs(now_ms()),
            0,
        )
        .unwrap();
        s.bind_task(ctx).unwrap();
        s
    }

    fn records(&self) -> Vec<ReadRecord> {
        self.admin().task_read_log(self.task).unwrap()
    }

    fn last(&self) -> ReadRecord {
        self.records().pop().expect("a read-log record")
    }

    fn role(&self, name: &str) -> giggles_core::roles::CatalogRole {
        self.admin().roles().get(name).unwrap().unwrap()
    }

    fn verified(&self) -> readlog::Reconciliation {
        let check = self.admin().task_read_verify(self.task).unwrap();
        assert!(check.ok(), "{check:?}");
        check
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin = None;
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Runs `sql` on the buffered path and releases its page.
fn buffered(s: &mut SqlSession, sql: &str, params: &[Value]) -> Result<Vec<SqlRow>, CoreError> {
    let page = s.execute_unreleased(sql, params, None)?;
    s.release_page(page)
        .map(|released| released.into_inner().rows)
}

/// Runs `sql` through a cursor, releasing every page of `page_rows` rows.
fn cursor(s: &mut SqlSession, sql: &str, page_rows: usize) -> Result<Vec<SqlRow>, CoreError> {
    s.set_task_statement_text(sql);
    s.set_task_entry(ENTRY_PGWIRE_EXTENDED);
    let p = s.prepare(sql)?;
    let mut cursor = s.open_cursor(&p, &[])?;
    let mut rows = Vec::new();
    while !cursor.is_exhausted() {
        let page = cursor.fetch_unreleased(page_rows, 1 << 20)?;
        rows.extend(s.release_page(page)?.into_inner());
    }
    s.set_task_entry(readlog::ENTRY_PGWIRE_SIMPLE);
    Ok(rows)
}

fn read(projection: &[&str], filter: Option<Expr>) -> Request {
    Request::Read {
        table: "shipments".into(),
        filter,
        order: Vec::new(),
        limit: None,
        projection: projection.iter().map(|c| c.to_string()).collect(),
    }
}

fn field_eq(field: &str, value: ValueExpr) -> Expr {
    Expr::Compare(
        Box::new(Expr::Field(field.into())),
        Compare::Eq,
        Box::new(Expr::Value(value)),
    )
}

/// Runs a native request and releases its outcome; the error's SQLSTATE.
fn native(
    s: &mut SqlSession,
    request: &Request,
    bindings: &Bindings,
) -> Result<Vec<SqlRow>, String> {
    s.set_task_entry(ENTRY_NATIVE);
    let result = s
        .execute_native_unreleased(request, bindings)
        .map_err(|e| e.sqlstate().unwrap_or("XX000").to_owned())
        .and_then(|page| {
            s.release_page(page)
                .map(|released| released.into_inner().rows)
                .map_err(|e| giggles_core::sqlstate(&e).to_owned())
        });
    s.set_task_entry(readlog::ENTRY_PGWIRE_SIMPLE);
    result
}

fn sqlstate(e: &CoreError) -> String {
    giggles_core::sqlstate(e).to_owned()
}

#[test]
fn every_statement_logged_including_denied() {
    let f = Fixture::new("task-readlog-every", caps(1000, 1000, 1 << 20));
    let mut s = f.exact();
    let mut expected = 0;
    // One record per statement: (outcome, sqlstate, text, class).
    let mut check = |f: &Fixture, outcome: u8, state: &str, text: &str| {
        expected += 1;
        let records = f.records();
        assert_eq!(records.len(), expected, "{text}: {records:#?}");
        let r = records.last().unwrap();
        assert_eq!(r.read_seq, expected as u64, "{text}");
        assert_eq!(
            (r.outcome, r.sqlstate_text().as_str()),
            (outcome, state),
            "{text}"
        );
        assert_eq!(r.text, text);
        assert_eq!(r.credential_fingerprint, [7; 32]);
        r.clone()
    };
    // Allowed: buffered, cursor, table-free, native.
    assert_eq!(buffered(&mut s, ALL, &[]).unwrap().len(), 3);
    let r = check(&f, OUTCOME_COMPLETE, "00000", ALL);
    assert_eq!(
        (r.released_rows, r.pages, r.class),
        (3, 1, class_code(StatementClass::Select))
    );
    assert!(!r.columns.is_empty() && r.table_id != 0, "{r:?}");
    assert_eq!(cursor(&mut s, ALL, 2).unwrap().len(), 3);
    let r = check(&f, OUTCOME_COMPLETE, "00000", ALL);
    assert_eq!((r.released_rows, r.entry), (3, ENTRY_PGWIRE_EXTENDED));
    assert!(r.pages >= 2, "{r:?}");
    assert_eq!(buffered(&mut s, "SELECT 1", &[]).unwrap().len(), 1);
    check(&f, OUTCOME_COMPLETE, "00000", "SELECT 1");
    let request = read(&["id"], None);
    assert_eq!(native(&mut s, &request, &Bindings::new()).unwrap().len(), 3);
    let r = check(
        &f,
        OUTCOME_COMPLETE,
        "00000",
        &serde_json::to_string(&request).unwrap(),
    );
    assert_eq!(r.entry, ENTRY_NATIVE);
    // Refused at statement start by the column rules, buffered and cursor.
    let denied = "SELECT id FROM shipments WHERE card_token::int > 0";
    let e = buffered(&mut s, denied, &[]).unwrap_err();
    assert_eq!(sqlstate(&e), "42501");
    let r = check(&f, OUTCOME_DENIED, "42501", denied);
    assert_eq!(
        (r.charged_rows_read, r.released_rows, r.rows_scanned),
        (0, 0, 0)
    );
    let e = cursor(&mut s, denied, 2).unwrap_err();
    assert_eq!(sqlstate(&e), "42501");
    check(&f, OUTCOME_DENIED, "42501", denied);
    // A native request the column rules deny (a hidden column in the filter).
    let request = read(
        &["id"],
        Some(field_eq("merchant_id", ValueExpr::Literal(Value::Int(42)))),
    );
    let state = native(&mut s, &request, &Bindings::new()).unwrap_err();
    assert_eq!(state, "42501");
    check(
        &f,
        OUTCOME_DENIED,
        "42501",
        &serde_json::to_string(&request).unwrap(),
    );
    // A write outside the protected commit.
    let write = "UPDATE shipments SET status = 'queued' WHERE id = 1";
    let e = buffered(&mut s, write, &[]).unwrap_err();
    let state = sqlstate(&e);
    let r = check(&f, outcome_of(&state), &state, write);
    assert_eq!(r.outcome, OUTCOME_DENIED, "{e:?}");
    // A statement that does not parse, and one that fails as it runs.
    let e = buffered(&mut s, "SELEC id FROM shipments", &[]).unwrap_err();
    let state = sqlstate(&e);
    check(&f, outcome_of(&state), &state, "SELEC id FROM shipments");
    let e = buffered(&mut s, "SELECT 1 / 0", &[]).unwrap_err();
    assert_eq!(sqlstate(&e), "22012");
    check(&f, OUTCOME_ERROR, "22012", "SELECT 1 / 0");
    // An `admit()` refusal the server reports.
    let refused = CoreError::Task(TaskError::NotAllowedInTaskSession(StatementClass::Delete));
    s.log_refused_task_statement("DELETE FROM shipments", StatementClass::Delete, 0, &refused);
    let r = check(&f, OUTCOME_DENIED, "0A000", "DELETE FROM shipments");
    assert_eq!(r.class, class_code(StatementClass::Delete));
    // Refused statements charged nothing; the rest reconcile.
    let check = f.verified();
    assert_eq!((check.records, check.open_records), (expected as u64, 0));
    assert_eq!(check.head_rows_returned, 3 + 3 + 1 + 3);
}

#[test]
fn text_capped_fingerprint_over_full_text() {
    let f = Fixture::new("task-readlog-text", caps(1000, 1000, 1 << 20));
    let mut s = f.exact();
    // Two-byte characters, so the 1,024-byte cut can fall inside one.
    let sql = format!(
        "SELECT id FROM shipments WHERE status <> '{}' ORDER BY id",
        "é".repeat(700)
    );
    assert!(sql.len() > 1024);
    assert_eq!(buffered(&mut s, &sql, &[]).unwrap().len(), 3);
    let r = f.last();
    assert_eq!(r.text_len as usize, sql.len());
    assert!(
        r.text.len() <= 1024 && r.text.len() >= 1023,
        "{}",
        r.text.len()
    );
    assert!(sql.starts_with(&r.text));
    let full: [u8; 32] = Sha256::digest(sql.as_bytes()).into();
    let capped: [u8; 32] = Sha256::digest(r.text.as_bytes()).into();
    assert_eq!(r.statement_sha256, full);
    assert_ne!(r.statement_sha256, capped);
    // A short text is stored whole.
    buffered(&mut s, ALL, &[]).unwrap();
    let r = f.last();
    assert_eq!((r.text.as_str(), r.text_len as usize), (ALL, ALL.len()));
    let print = giggles_core::task::fingerprint_and_cap(&sql);
    assert_eq!(
        (print.sha256, print.text),
        (full, f.records()[0].text.clone())
    );
}

#[test]
fn parameters_not_stored() {
    let f = Fixture::new("task-readlog-params", caps(1000, 1000, 1 << 20));
    let mut s = f.exact();
    let secret = "secret-param-7731";
    let sql = "SELECT id FROM shipments WHERE status = $1 OR status <> $2 ORDER BY id";
    let rows = buffered(
        &mut s,
        sql,
        &[Value::Text(secret.into()), Value::Text("x".into())],
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
    let r = f.last();
    assert_eq!((r.param_count, r.text.as_str()), (2, sql));
    // Native: the binding's value is not in the canonical request.
    let request = read(
        &["id"],
        Some(field_eq("status", ValueExpr::Parameter("s".into()))),
    );
    let bindings = Bindings::from([("s".to_string(), Value::Text(secret.into()))]);
    native(&mut s, &request, &bindings).unwrap();
    let r = f.last();
    assert_eq!(r.param_count, 1);
    // Nowhere in the log, nor in what SHOW TASK READS prints.
    let all = format!("{:?}", f.records());
    assert!(!all.contains(secret), "{all}");
    let shown = show_reads(
        &f.admin().sql_session(),
        &f.role("incident_controller"),
        "inc-436",
        None,
        None,
    )
    .unwrap();
    let shown = format!("{shown:?}");
    assert!(!shown.contains(secret), "{shown}");
    assert!(shown.contains("$1"));
}

/// A small deterministic generator (xorshift64*).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn totals_equal_disclosure_head() {
    let f = Fixture::new("task-readlog-totals", caps(1_000_000, 1_000_000, 1 << 30));
    let mut sessions = [f.exact(), f.agent()];
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let projections = [
        "id, status",
        "id, note, reason",
        "id, customer_email, card_token",
        "id, attempt_count, reason, note, status",
    ];
    let mut received = 0u64;
    let statements = 200;
    for i in 0..statements {
        let s = &mut sessions[rng.below(2) as usize];
        let projection = projections[rng.below(projections.len() as u64) as usize];
        let bound = 1 + rng.below(6);
        let sql = format!("SELECT {projection} FROM shipments WHERE id <= {bound} ORDER BY id");
        let result = match rng.below(6) {
            0 | 1 => buffered(s, &sql, &[]),
            2 => cursor(s, &sql, 1 + rng.below(3) as usize),
            3 => native(s, &read(&["id", "note", "reason"], None), &Bindings::new())
                .map_err(|_| CoreError::Unsupported),
            4 => buffered(
                s,
                "SELECT id FROM shipments WHERE 1 / (merchant_id - 42) > 0",
                &[],
            ),
            _ => buffered(s, "SELECT 1, 'x'", &[]),
        };
        match result {
            Ok(rows) => received += rows.len() as u64,
            Err(e) => assert_eq!(sqlstate(&e), "42501", "statement {i}: {e:?}"),
        }
    }
    drop(sessions);
    let records = f.records();
    assert_eq!(records.len(), statements, "every statement has one record");
    assert!(records
        .iter()
        .all(|r| r.outcome == OUTCOME_COMPLETE || r.outcome == OUTCOME_DENIED));
    let sum = |field: fn(&ReadRecord) -> u64| -> u64 { records.iter().map(field).sum() };
    let check = f.verified();
    assert_eq!(sum(|r| r.charged_rows_read), check.head_rows_read);
    assert_eq!(sum(|r| r.charged_rows_returned), check.head_rows_returned);
    assert_eq!(sum(|r| r.charged_bytes), check.head_bytes_returned);
    assert_eq!(sum(|r| r.withheld_untrusted), check.head_withheld);
    assert_eq!(sum(|r| r.released_rows), received);
    assert!(check.head_withheld > 0 && sum(|r| r.untrusted_released) > 0);
    assert!(
        check.head_rows_returned >= received,
        "reservation only over-counts"
    );
    let d = f.admin().task_disclosure(f.task).unwrap();
    assert_eq!(
        (d.rows_read, d.rows_returned, d.bytes_returned),
        (
            check.head_rows_read,
            check.head_rows_returned,
            check.head_bytes_returned
        )
    );
    assert_eq!(d.next_read_seq, statements as u64 + 1);
}

#[test]
fn untrusted_release_and_withheld_counted() {
    let f = Fixture::new("task-readlog-untrusted", caps(1000, 1000, 1 << 20));
    let mut s = f.exact();
    let sql = "SELECT id, note, reason, customer_email FROM shipments ORDER BY id";
    let rows = buffered(&mut s, sql, &[]).unwrap();
    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert_eq!(row.values[1], Value::Null, "note is withheld");
        assert!(matches!(&row.values[2], Value::Text(r) if r.starts_with('r')));
        assert_eq!(row.values[3], Value::Null, "customer_email is masked");
    }
    let r = f.last();
    assert_eq!(
        (r.withheld_untrusted, r.untrusted_released, r.masked_values),
        (3, 3, 3)
    );
    assert_eq!(f.verified().head_withheld, 3);
    // Through a cursor too; the head counts both statements.
    cursor(&mut s, sql, 2).unwrap();
    let r = f.last();
    assert_eq!((r.withheld_untrusted, r.untrusted_released), (3, 3));
    assert!(r.pages >= 2, "{r:?}");
    assert_eq!(f.verified().head_withheld, 6);
}

#[test]
fn open_record_after_crash_reconciles() {
    let mut f = Fixture::new("task-readlog-crash", caps(1000, 1000, 1 << 20));
    let task = f.task;
    {
        let mut s = f.exact();
        buffered(&mut s, ALL, &[]).unwrap();
        let page = s.execute_unreleased(ALL, &[], None).unwrap();
        f.admin()
            .set_crash_point(CrashPoint::ErrorAfterCommit)
            .unwrap();
        // Charged, then the process "dies" before the statement ends.
        assert!(matches!(s.release_page(page), Err(CoreError::Storage(_))));
        f.admin().set_crash_point(CrashPoint::None).unwrap();
    }
    f.admin = None;
    let admin = Admin::open(&f.path, Limits::default()).unwrap();
    f.admin = Some(admin);
    let records = f.admin().task_read_log(task).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].outcome, OUTCOME_COMPLETE);
    let open = &records[1];
    assert_eq!(open.outcome, OUTCOME_OPEN);
    assert_eq!((open.charged_rows_read, open.charged_rows_returned), (3, 3));
    let check = f.verified();
    assert_eq!((check.records, check.open_records), (2, 1));
    assert_eq!(check.head_rows_returned, 6);
    // The session after the restart logs and reconciles as usual.
    let mut s = f.agent();
    buffered(&mut s, ALL, &[]).unwrap();
    drop(s);
    let r = f.last();
    assert_eq!((r.read_seq, r.outcome), (3, OUTCOME_COMPLETE));
    let check = f.verified();
    assert_eq!((check.records, check.open_records), (3, 1));
}

#[test]
fn show_requires_inspect_privilege() {
    let f = Fixture::new("task-readlog-show", caps(1000, 1000, 1 << 20));
    let mut s = f.exact();
    for _ in 0..3 {
        buffered(&mut s, ALL, &[]).unwrap();
    }
    let plain = f.admin().sql_session();
    for name in ["incident_controller", "auditor", "dba"] {
        let shown = show_reads(&plain, &f.role(name), "inc-436", None, None).unwrap();
        assert_eq!(shown.rows.len(), 3, "{name}");
        assert_eq!(shown.columns.len(), readlog::SHOW_COLUMNS.len());
        let at = |c: &str| shown.columns.iter().position(|x| x == c).unwrap();
        let row = &shown.rows[0].values;
        assert_eq!(row[at("read_seq")], Value::Int(1));
        assert_eq!(row[at("text")], Value::Text(ALL.into()));
        assert_eq!(row[at("outcome")], Value::Text("complete".into()));
        assert_eq!(
            row[at("credential_fingerprint")],
            Value::Text("07".repeat(32))
        );
    }
    for name in ["stranger", "repair_agent"] {
        let e = show_reads(&plain, &f.role(name), "inc-436", None, None).unwrap_err();
        assert_eq!(sqlstate(&e), "42501", "{name}");
    }
    // AFTER and LIMIT page through the log.
    let issuer = f.role("incident_controller");
    let page = show_reads(&plain, &issuer, "inc-436", Some(1), Some(1)).unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].values[0], Value::Int(2));
    assert!(show_reads(&plain, &issuer, "inc-436", Some(3), None)
        .unwrap()
        .rows
        .is_empty());
    let e = show_reads(&plain, &issuer, "inc-436", None, Some(1025)).unwrap_err();
    assert_eq!(sqlstate(&e), "22023");
    let e = show_reads(&plain, &issuer, "no-such-task", None, None).unwrap_err();
    assert_eq!(sqlstate(&e), "42704");
    // Revoking the grant takes the privilege away.
    f.admin()
        .tasks()
        .grant(
            &issuer,
            "inc-436",
            "auditor",
            TaskPrivileges::INSPECT,
            false,
        )
        .unwrap();
    let e = show_reads(&plain, &f.role("auditor"), "inc-436", None, None).unwrap_err();
    assert_eq!(sqlstate(&e), "42501");
}

#[test]
fn show_refused_in_task_session() {
    let f = Fixture::new("task-readlog-show-task", caps(1000, 1000, 1 << 20));
    let s = f.exact();
    let e = show_reads(&s, &f.role("incident_controller"), "inc-436", None, None).unwrap_err();
    assert_eq!(
        e,
        CoreError::Task(TaskError::NotAllowedInTaskSession(StatementClass::Show))
    );
    assert_eq!(sqlstate(&e), "0A000");
    assert!(f.records().is_empty());
}

#[test]
fn budget_left_matches_head() {
    let cap = caps(40, 10, 4096);
    for exact in [true, false] {
        let f = Fixture::new("task-readlog-left", cap);
        let mut s = if exact { f.exact() } else { f.agent() };
        let left_matches = |f: &Fixture| {
            let r = f.last();
            let d = f.admin().task_disclosure(f.task).unwrap();
            assert_eq!(
                (r.left_rows_read, r.left_rows_returned, r.left_bytes),
                (
                    cap.rows_read - d.rows_read,
                    cap.rows_returned - d.rows_returned,
                    cap.bytes_returned - d.bytes_returned
                ),
                "exact={exact} {r:?}"
            );
            r
        };
        for _ in 0..3 {
            assert_eq!(buffered(&mut s, ALL, &[]).unwrap().len(), 3);
            left_matches(&f);
        }
        // A table-free statement and a cursor keep the fields current.
        buffered(&mut s, "SELECT 1", &[]).unwrap();
        left_matches(&f);
        // The returned-rows cap stops the next full read: logged as budget.
        let e = buffered(&mut s, ALL, &[]).unwrap_err();
        assert_eq!(
            e,
            CoreError::Task(TaskError::BudgetExceeded(
                giggles_core::task::Budget::RowsReturned
            ))
        );
        let r = left_matches(&f);
        assert_eq!(
            (r.outcome, r.sqlstate_text().as_str()),
            (OUTCOME_BUDGET, "54T01")
        );
        assert_eq!(r.released_rows, 0);
        f.verified();
    }
}
