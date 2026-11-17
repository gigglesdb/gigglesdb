//! GD-0061 R3-708: native `preview { update } using task` and `apply change`
//! against SQL PREVIEW of the same UPDATE, on a v2 database.
//!
//! `deliveries` is reserved, so the table is `shipments` with the D9 columns.
//! Merchant 42's failed rows 1-40 fall inside the task's OLD ROW window.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::relational::StatementClock;
use giggles_core::roles::{NewRole, TablePrivilege};
use giggles_core::task::context::TaskContext;
use giggles_core::task::definition::WallClockMs;
use giggles_core::task::statements::{TaskStatement, parse};
use giggles_core::task::{Surface, preview};
use giggles_core::{Admin, Engine, Limits, SqlSession};
use giggles_language::{Bindings, ChangeOutcome, Error, LanguageVersion, Value, execute_change};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const V: LanguageVersion = LanguageVersion::Embedded022;
pub(crate) const TASK: &str = "inc-436";
/// 2026-11-05 18:00:00+00 in microseconds since 2000-01-01 UTC.
const PIN_US: i64 = 847_216_800_000_000;

pub(crate) struct Db {
    dir: PathBuf,
    pub(crate) admin: Admin,
}
impl Drop for Db {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

pub(crate) fn sql(admin: &Admin, statement: &str) -> Vec<Vec<Value>> {
    admin
        .sql_session()
        .execute(statement, &[], None)
        .unwrap_or_else(|e| panic!("{statement}: {e:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

pub(crate) fn queued(admin: &Admin) -> Value {
    sql(admin, "SELECT count(*) FROM shipments WHERE status = 'queued'")[0][0].clone()
}

/// A fresh v2 database with the D9 table, the roles and task `inc-436`
/// (APPROVE ABOVE 5, at most 25 rows), expiring in a day.
pub(crate) fn setup() -> Db {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "lang-preview-{}-{}-{}",
        std::process::id(),
        now_ms(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let admin = Admin::create_engine(&dir, Limits::default(), Engine::V2).unwrap();
    let db = Db { dir, admin };
    let admin = &db.admin;
    sql(
        admin,
        "CREATE TABLE shipments (merchant_id bigint, id bigint, status text, \
         failed_at timestamptz, attempt_count int, customer_email text, card_token text, \
         PRIMARY KEY (merchant_id, id))",
    );
    let mut rows: Vec<String> = (1..=40)
        .map(|id| format!("(42, {id}, 'failed', '2026-11-05 17:03:00+00', 1, 'c{id}@example.com', 'tok_{id}')"))
        .collect();
    rows.push("(43, 100, 'failed', '2026-11-05 17:03:00+00', 1, 'other@example.com', 'tok_100')".into());
    sql(admin, &format!("INSERT INTO shipments VALUES {}", rows.join(", ")));
    let roles = admin.roles();
    for (name, task_only) in [("incident_controller", false), ("oncall_lead", false), ("repair_agent", true)] {
        roles
            .create(NewRole { name: name.into(), login: true, ..NewRole::default() })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    for name in ["incident_controller", "repair_agent"] {
        roles
            .grant_table("shipments", name, &[TablePrivilege::Select, TablePrivilege::Update])
            .unwrap();
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    let expires = {
        // A day from now, as a UTC literal (seconds precision is enough).
        let secs = now_ms() / 1000 + 86_400;
        let days = secs.div_euclid(86_400);
        let rem = secs.rem_euclid(86_400);
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = yoe + era * 400 + i64::from(month <= 2);
        format!(
            "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}+00",
            rem / 3600,
            rem / 60 % 60,
            rem % 60
        )
    };
    let create = format!(
        "CREATE TASK \"{TASK}\" \
           EXECUTOR repair_agent APPROVER oncall_lead \
           ON shipments \
           VISIBLE ROWS WHERE (merchant_id = 42) \
           READABLE (id, status, failed_at, attempt_count) \
           MASKED (customer_email, card_token) \
           ALLOW UPDATE (status) \
             OLD ROW WHERE (status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00' \
                            AND failed_at < '2026-11-05 17:07:00+00') \
             NEW ROW WHERE (status = 'queued') \
             AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS \
             APPROVE ABOVE 5 \
           DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
           EXPIRES '{expires}'"
    );
    let Some(TaskStatement::Create(new)) = parse(&create) else {
        panic!("CREATE TASK did not parse");
    };
    admin.tasks().create(&issuer, *new, WallClockMs(now_ms())).unwrap();
    db
}

/// The agent session's binding, as the server builds it at connect.
pub(crate) fn context(admin: &Admin) -> TaskContext {
    let def = admin.tasks().get(TASK).unwrap().expect("task");
    TaskContext {
        task: def.id,
        name: def.name.clone(),
        executor: def.executor.clone(),
        bound_def_version: def.def_version,
        credential_fingerprint: [7; 32],
    }
}

pub(crate) fn agent(admin: &Admin, ctx: &TaskContext) -> SqlSession {
    let mut session = admin.sql_session();
    session.bind_task(ctx.clone()).unwrap();
    session
}

pub(crate) fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: PIN_US,
        statement_us: PIN_US + 1_000_000,
        zone: None,
        wall: Some(Arc::new(|| Ok(PIN_US + 5_000_000))),
    }
}

/// One native change statement in a fresh task session.
pub(crate) fn native(
    admin: &Admin,
    ctx: &TaskContext,
    text: &str,
    bindings: &Bindings,
) -> Result<ChangeOutcome, Error> {
    execute_change(&mut agent(admin, ctx), ctx, text, V, bindings, &clock())
}

fn bindings(pairs: &[(&str, Value)]) -> Bindings {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect()
}

const NATIVE_REQUEUE: &str = "preview { update many shipments where status = \"failed\" and id >= $lo \
     and id <= $hi set status = \"queued\" at most 25 } using task \"inc-436\"";

#[test]
fn native_and_sql_preview_of_same_update_agree_on_rows_and_count() {
    let db = setup();
    let ctx = context(&db.admin);
    let text = "UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id >= 1 AND id <= 12";
    let sql_summary = preview(&mut agent(&db.admin, &ctx), &ctx, Surface::Sql, text, &[], &clock()).unwrap();
    let ChangeOutcome::Preview(native_summary) = native(
        &db.admin,
        &ctx,
        NATIVE_REQUEUE,
        &bindings(&[("lo", Value::Int(1)), ("hi", Value::Int(12))]),
    )
    .unwrap() else {
        panic!("not a preview");
    };
    assert_eq!(sql_summary.rows, 12);
    assert_eq!(native_summary.rows, sql_summary.rows);
    // `surface` and `text` are hashed, so the digests differ.
    assert_ne!(native_summary.digest, sql_summary.digest);
    assert_eq!(queued(&db.admin), Value::Int(0));

    let store = db.admin.task_previews(ctx.task).unwrap();
    assert_eq!(store.previews.len(), 2);
    let by_id = |id| store.previews.iter().find(|p| p.id == id).unwrap();
    let (s, n) = (by_id(sql_summary.id), by_id(native_summary.id));
    assert_eq!((s.surface, n.surface), (Surface::Sql as u8, Surface::Native as u8));
    assert_eq!(s.counts, n.counts);
    let rows = |p: &giggles_core::task::preview::StoredPreview| {
        p.rows.iter().map(|r| (r.class, r.table, r.pk.clone(), r.version_read, r.after.clone())).collect::<Vec<_>>()
    };
    assert_eq!(rows(s), rows(n));
    assert_eq!(rows(n).len(), 12);
    // The stored native text is its SQL lowering, with positional parameters.
    assert_eq!(
        n.text,
        "UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id >= $1 AND id <= $2"
    );
    assert_eq!(n.params, vec![Value::Int(1), Value::Int(12)]);
}

#[test]
fn native_apply_commits_the_previewed_rows_and_maps_task_errors() {
    let db = setup();
    let ctx = context(&db.admin);
    let ChangeOutcome::Preview(summary) = native(
        &db.admin,
        &ctx,
        NATIVE_REQUEUE,
        &bindings(&[("lo", Value::Int(2)), ("hi", Value::Int(4))]),
    )
    .unwrap() else {
        panic!("not a preview");
    };
    assert_eq!(summary.rows, 3);
    let hex: String = summary.digest.iter().map(|b| format!("{b:02x}")).collect();
    let apply = "apply change $c digest $d once $op for 1h";
    let args = |digest: &str| {
        bindings(&[
            ("c", Value::Int(summary.id.0 as i64)),
            ("d", Value::Text(digest.to_owned())),
            ("op", Value::Text("op-native-1".into())),
        ])
    };
    // Another digest is ApprovalStale, 55T01 as over SQL; nothing written.
    let wrong = "0".repeat(64);
    assert_eq!(
        native(&db.admin, &ctx, apply, &args(&wrong)),
        Err(Error::Sql { sqlstate: "55T01".into() })
    );
    // A digest that is not 64 hex digits is 22023, as over SQL.
    assert_eq!(
        native(&db.admin, &ctx, apply, &args("abc")),
        Err(Error::Sql { sqlstate: "22023".into() })
    );
    assert_eq!(queued(&db.admin), Value::Int(0));
    let ChangeOutcome::Applied(receipt) = native(&db.admin, &ctx, apply, &args(&hex)).unwrap() else {
        panic!("not applied");
    };
    assert_eq!(receipt.rows_affected, 3);
    assert_eq!(receipt.operation_id, b"op-native-1".to_vec());
    assert_eq!(queued(&db.admin), Value::Int(3));
    // The same operation ID replays the same receipt and writes nothing more.
    let ChangeOutcome::Applied(again) = native(&db.admin, &ctx, apply, &args(&hex)).unwrap() else {
        panic!("not applied");
    };
    assert_eq!((again.seq, again.rows_affected), (receipt.seq, 3));
    assert_eq!(queued(&db.admin), Value::Int(3));
}
