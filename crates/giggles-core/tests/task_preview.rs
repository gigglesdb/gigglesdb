//! GD-0061 R3-703: PREVIEW, a dry run then a store (design-7 D7.2-D7.4),
//! through the public API, with the task created from `CREATE TASK` text.
//!
//! `deliveries` is reserved, so the table is `shipments`, with the columns
//! and the primary key of the D9 schema. Merchant 42's failed rows 1-40 fall
//! inside the OLD ROW window; row 41 is outside it, row 50 is delivered and
//! row 100 belongs to merchant 43.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::relational::{StatementClock, Value};
use giggles_core::roles::{NewRole, TablePrivilege};
use giggles_core::task::context::TaskContext;
use giggles_core::task::definition::WallClockMs;
use giggles_core::task::preview::PreviewStore;
use giggles_core::task::readlog::{class_code, OUTCOME_COMPLETE, OUTCOME_DENIED};
use giggles_core::task::statements::{parse, TaskStatement};
use giggles_core::task::{
    preview, Budget, DenyRule, PreviewId, PreviewSummary, StatementClass, Surface, TaskError,
};
use giggles_core::{Admin, Engine, Limits, SqlSession};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TASK: &str = "inc-436";
const DAY_MS: i64 = 86_400_000;
/// 2026-11-05 18:00:00+00 in microseconds since 2000-01-01 UTC.
const PIN_US: i64 = 847_216_800_000_000;

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

/// Unix milliseconds as a timestamptz literal in UTC.
fn timestamptz(ms: i64) -> String {
    let days = ms.div_euclid(DAY_MS);
    let rem = ms.rem_euclid(DAY_MS);
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
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{:03}+00",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}

fn sql(admin: &Admin, statement: &str) -> Vec<Vec<Value>> {
    admin
        .sql_session()
        .execute(statement, &[], None)
        .unwrap_or_else(|e| panic!("{statement}: {e:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn queued(admin: &Admin) -> i64 {
    match &sql(admin, "SELECT count(*) FROM shipments WHERE status = 'queued'")[0][0] {
        Value::Int(n) => *n,
        other => panic!("{other:?}"),
    }
}

/// The worked example's task, with `with` appended to its load limits.
fn create_task_sql(expires: &str, with: &str) -> String {
    format!(
        "CREATE TASK \"{TASK}\" \
           EXECUTOR repair_agent APPROVER oncall_lead \
           ON shipments \
           VISIBLE ROWS WHERE (merchant_id = 42) \
           READABLE (id, status, failed_at, attempt_count) \
           MASKED (customer_email, card_token) \
           ALLOW UPDATE (status, failed_at) \
             OLD ROW WHERE (status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00' \
                            AND failed_at < '2026-11-05 17:07:00+00') \
             NEW ROW WHERE (status = 'queued') \
             AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS \
             APPROVE ABOVE 5 \
           DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
           WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2{with}) \
           EXPIRES '{expires}'"
    )
}

/// A fresh v2 database with the D9 table, the roles and the task.
fn setup(expires_ms: i64, with: &str) -> (Dir, Admin) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let dir = Dir(std::env::temp_dir().join(format!(
        "task-preview-{}-{}-{}",
        std::process::id(),
        now_ms(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    sql(
        &admin,
        "CREATE TABLE shipments (merchant_id bigint, id bigint, status text, \
         failed_at timestamptz, attempt_count int, customer_email text, card_token text, \
         PRIMARY KEY (merchant_id, id))",
    );
    let mut rows: Vec<String> = (1..=40)
        .map(|id| {
            format!("(42, {id}, 'failed', '2026-11-05 17:03:00+00', 1, 'c{id}@example.com', 'tok_{id}')")
        })
        .collect();
    rows.push("(42, 41, 'failed', '2026-11-05 17:10:00+00', 1, 'late@example.com', 'tok_41')".into());
    rows.push("(42, 50, 'delivered', NULL, 0, 'done@example.com', 'tok_50')".into());
    rows.push("(43, 100, 'failed', '2026-11-05 17:03:00+00', 1, 'other@example.com', 'tok_100')".into());
    sql(&admin, &format!("INSERT INTO shipments VALUES {}", rows.join(", ")));
    let roles = admin.roles();
    for (name, task_only) in [
        ("incident_controller", false),
        ("oncall_lead", false),
        ("repair_agent", true),
    ] {
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
    let Some(TaskStatement::Create(new)) = parse(&create_task_sql(&timestamptz(expires_ms), with))
    else {
        panic!("CREATE TASK did not parse");
    };
    admin.tasks().create(&issuer, *new, WallClockMs(now_ms())).unwrap();
    (dir, admin)
}

/// The agent session's binding, as the server builds it at connect.
fn context(admin: &Admin) -> TaskContext {
    let def = admin.tasks().get(TASK).unwrap().expect("task");
    TaskContext {
        task: def.id,
        name: def.name.clone(),
        executor: def.executor.clone(),
        bound_def_version: def.def_version,
        credential_fingerprint: [7; 32],
    }
}

/// A task session bound to `ctx`.
fn agent(admin: &Admin, ctx: &TaskContext) -> SqlSession {
    let mut session = admin.sql_session();
    session.bind_task(ctx.clone()).unwrap();
    session
}

/// A statement clock whose statement instant is a second after its
/// transaction instant `PIN_US`, with a working wall clock.
fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: PIN_US,
        statement_us: PIN_US + 1_000_000,
        zone: None,
        wall: Some(Arc::new(|| Ok(PIN_US + 5_000_000))),
    }
}

fn run(admin: &Admin, ctx: &TaskContext, text: &str) -> Result<PreviewSummary, TaskError> {
    preview(&mut agent(admin, ctx), ctx, Surface::Sql, text, &[], &clock())
}

/// Requeue merchant 42's failed rows `from..=to` (merchant_id is hidden from
/// the task, so the agent filters by id; visible rows scope it to 42).
fn requeue(from: i64, to: i64) -> String {
    format!(
        "UPDATE shipments SET status = 'queued' \
         WHERE status = 'failed' AND id >= {from} AND id <= {to}"
    )
}

fn store(admin: &Admin, ctx: &TaskContext) -> PreviewStore {
    admin.task_previews(ctx.task).unwrap()
}

#[test]
fn preview_writes_no_user_rows_and_returns_count_and_digest() {
    let (_dir, admin) = setup(now_ms() + DAY_MS, "");
    let ctx = context(&admin);
    let before = sql(&admin, "SELECT * FROM shipments ORDER BY merchant_id, id");
    let started = now_ms() as u64;

    // Twelve rows: above APPROVE ABOVE 5, which a preview never checks.
    let first = run(&admin, &ctx, &requeue(1, 12)).unwrap();
    assert_eq!((first.id, first.rows), (PreviewId(1), 12));
    // No user row changed.
    assert_eq!(queued(&admin), 0);
    assert_eq!(sql(&admin, "SELECT * FROM shipments ORDER BY merchant_id, id"), before);

    // The digest returned is the stored one; the expiry is the default TTL.
    let stored = store(&admin, &ctx);
    assert_eq!((stored.next_preview, stored.stored_previews), (2, 1));
    assert_eq!(stored.previews.len(), 1);
    let header = &stored.previews[0];
    assert_eq!(header.id, PreviewId(1));
    assert_eq!(header.digest, first.digest);
    assert_eq!((header.kind, header.state, header.counts), (1, 0, [12, 0, 0, 0]));
    assert_eq!(header.expires_ms, header.created_ms + 3_600_000);
    assert!(header.created_ms >= started);
    assert_eq!(first.expires_at, WallClockMs(header.expires_ms as i64));

    // The same statement again is another preview: the next id and a
    // different digest (the id is hashed).
    let second = run(&admin, &ctx, &requeue(1, 12)).unwrap();
    assert_eq!((second.id, second.rows), (PreviewId(2), 12));
    assert_ne!(second.digest, first.digest);
    assert_eq!(store(&admin, &ctx).stored_previews, 2);
    assert_eq!(queued(&admin), 0);

    // One read-log record per preview, class Preview, nothing returned.
    let log = admin.task_read_log(ctx.task).unwrap();
    assert_eq!(log.len(), 2);
    for record in &log {
        assert_eq!(record.class, class_code(StatementClass::Preview));
        assert_eq!(record.outcome, OUTCOME_COMPLETE);
        assert_eq!((record.charged_rows_returned, record.released_rows), (0, 0));
        assert_eq!(record.text, requeue(1, 12));
    }
}

#[test]
fn preview_records_identity_version_and_after_values() {
    let (_dir, admin) = setup(now_ms() + DAY_MS, "");
    let ctx = context(&admin);
    let def = admin.tasks().get(TASK).unwrap().unwrap();
    let text = requeue(2, 4);
    let summary = run(&admin, &ctx, &text).unwrap();
    assert_eq!(summary.rows, 3);

    let stored = store(&admin, &ctx);
    let header = &stored.previews[0];
    assert_eq!(header.def_version, def.def_version);
    assert_eq!(header.table, def.table);
    assert_eq!(header.creator, "repair_agent");
    assert_eq!((header.surface, header.text.as_str()), (0, text.as_str()));
    assert!(header.params.is_empty());
    assert_eq!(header.clock_pin, None, "the statement read no instant");
    assert_ne!(header.schema_fp, [0; 32]);

    // Rows in ascending (table, pk): ids 2, 3, 4, each the full row with
    // only its status changed, and the version it was read at.
    let current = sql(
        &admin,
        "SELECT merchant_id, id, status, failed_at, attempt_count, customer_email, card_token \
         FROM shipments WHERE id IN (2, 3, 4) ORDER BY id",
    );
    assert_eq!(header.rows.len(), 3);
    let mut versions = std::collections::BTreeSet::new();
    for (row, now) in header.rows.iter().zip(&current) {
        assert_eq!((row.class, row.table, row.aux), (0, def.table, 0));
        let mut expected = now.clone();
        expected[2] = Value::Text("queued".into());
        assert_eq!(row.after, expected, "the after-value is the whole new row");
        assert_ne!(row.version_read, 0);
        assert!(versions.insert(row.version_read));
    }
    let keys: Vec<&Vec<u8>> = header.rows.iter().map(|row| &row.pk).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(keys, sorted, "rows are numbered in ascending primary-key order");

    // A later write changes the row version the preview recorded.
    sql(&admin, "UPDATE shipments SET attempt_count = 2 WHERE merchant_id = 42 AND id = 3");
    let again = run(&admin, &ctx, &text).unwrap();
    let stored = store(&admin, &ctx);
    let later = stored.previews.iter().find(|p| p.id == again.id).unwrap();
    assert_eq!(later.rows[0].version_read, header.rows[0].version_read);
    assert_ne!(later.rows[1].version_read, header.rows[1].version_read);
    assert_eq!(later.rows[1].after[4], Value::Int(2));
    assert_ne!(again.digest, summary.digest);
}

#[test]
fn preview_of_out_of_scope_row_is_denied_and_stores_nothing() {
    let (_dir, admin) = setup(now_ms() + DAY_MS, "");
    let ctx = context(&admin);
    // Row 41 is merchant 42's, but failed outside the OLD ROW window.
    let outside = "UPDATE shipments SET status = 'queued' WHERE id = 41";
    assert_eq!(
        run(&admin, &ctx, outside),
        Err(TaskError::Denied(DenyRule::OutsideAllowedWrite))
    );
    // A new value outside NEW ROW is denied too.
    let wrong_new = "UPDATE shipments SET status = 'delivered' WHERE id = 1";
    assert_eq!(
        run(&admin, &ctx, wrong_new),
        Err(TaskError::Denied(DenyRule::OutsideAllowedWrite))
    );
    // A column outside ALLOW UPDATE is denied before anything runs.
    let column = "UPDATE shipments SET attempt_count = 9 WHERE id = 1";
    assert!(matches!(run(&admin, &ctx, column), Err(TaskError::Denied(_))));
    // Merchant 43's row is not visible: it matches nothing.
    let hidden = run(&admin, &ctx, "UPDATE shipments SET status = 'queued' WHERE id = 100").unwrap();
    assert_eq!(hidden.rows, 0);

    // Only the empty preview was stored; the denials stored nothing and
    // took no id. No user row changed.
    let stored = store(&admin, &ctx);
    assert_eq!(stored.previews.len(), 1);
    assert_eq!(stored.previews[0].id, PreviewId(1));
    assert_eq!(stored.previews[0].rows.len(), 0);
    assert_eq!((stored.next_preview, stored.stored_previews), (2, 1));
    assert_eq!(queued(&admin), 0);

    // Each denial is in the read log as a denied Preview statement.
    let log = admin.task_read_log(ctx.task).unwrap();
    let outcomes: Vec<u8> = log.iter().map(|record| record.outcome).collect();
    assert_eq!(outcomes, [OUTCOME_DENIED, OUTCOME_DENIED, OUTCOME_DENIED, OUTCOME_COMPLETE]);
    assert!(log.iter().all(|record| record.class == class_code(StatementClass::Preview)));
    assert_eq!(log[0].sqlstate_text(), "42501");
}

#[test]
fn preview_limit_reached_is_budget_exceeded() {
    let (_dir, admin) = setup(now_ms() + DAY_MS, ", stored_previews = 2");
    let ctx = context(&admin);
    run(&admin, &ctx, &requeue(1, 2)).unwrap();
    run(&admin, &ctx, &requeue(3, 4)).unwrap();
    assert_eq!(
        run(&admin, &ctx, &requeue(5, 6)),
        Err(TaskError::BudgetExceeded(Budget::StoredPreviews))
    );
    let stored = store(&admin, &ctx);
    let ids: Vec<PreviewId> = stored.previews.iter().map(|p| p.id).collect();
    assert_eq!(ids, [PreviewId(1), PreviewId(2)]);
    assert_eq!((stored.next_preview, stored.stored_previews), (3, 2));
}

#[test]
fn expired_previews_are_swept_and_free_a_slot() {
    let (_dir, admin) = setup(now_ms() + DAY_MS, ", stored_previews = 1, preview_ttl_s = 1");
    let ctx = context(&admin);
    let first = run(&admin, &ctx, &requeue(1, 3)).unwrap();
    assert_eq!(store(&admin, &ctx).previews[0].rows.len(), 3);
    assert_eq!(
        run(&admin, &ctx, &requeue(4, 5)),
        Err(TaskError::BudgetExceeded(Budget::StoredPreviews))
    );
    // Wait out the first preview's one-second TTL.
    while now_ms() <= first.expires_at.0 {
        std::thread::sleep(Duration::from_millis(50));
    }
    let second = run(&admin, &ctx, &requeue(4, 5)).unwrap();
    assert_eq!(second.id, PreviewId(2));
    let stored = store(&admin, &ctx);
    assert_eq!(stored.previews.len(), 1, "the expired header is gone");
    assert_eq!(stored.previews[0].id, PreviewId(2));
    assert_eq!(stored.previews[0].rows.len(), 2);
    assert_eq!(stored.stored_rows, 2, "the expired preview's rows are gone");
    assert_eq!((stored.next_preview, stored.stored_previews), (3, 1));
}

#[test]
fn preview_with_clock_timestamp_is_rejected() {
    let (_dir, admin) = setup(now_ms() + DAY_MS, "");
    let ctx = context(&admin);
    // clock_timestamp() is outside the task's function list, so the column
    // rules refuse it before anything runs.
    for text in [
        "UPDATE shipments SET status = 'queued', failed_at = clock_timestamp() WHERE id = 1",
        "UPDATE shipments SET status = 'queued' WHERE id = 1 AND failed_at < clock_timestamp()",
    ] {
        assert_eq!(
            run(&admin, &ctx, text),
            Err(TaskError::Denied(DenyRule::FunctionNotAllowed)),
            "{text}"
        );
    }
    // A listed function whose value the preview cannot fix (the session
    // zone's date, D7.4) is caught by the clock's observation bits.
    for text in [
        "UPDATE shipments SET status = 'queued', failed_at = current_date WHERE id = 1",
        "UPDATE shipments SET status = 'queued' WHERE id = 1 AND failed_at < current_date",
    ] {
        assert_eq!(
            run(&admin, &ctx, text),
            Err(TaskError::Denied(DenyRule::NotMaterializable)),
            "{text}"
        );
    }
    let stored = store(&admin, &ctx);
    assert!(stored.previews.is_empty());
    assert_eq!(stored.next_preview, 1);
    assert_eq!(queued(&admin), 0);
}

#[test]
fn preview_with_now_pins_the_instant() {
    let (_dir, admin) = setup(now_ms() + DAY_MS, "");
    let ctx = context(&admin);
    let pinned = sql(&admin, "SELECT TIMESTAMPTZ '2026-11-05 18:00:00+00'")[0][0].clone();
    // now() and current_timestamp read the transaction instant, which the
    // header keeps as the pin.
    for text in [
        "UPDATE shipments SET status = 'queued', failed_at = now() WHERE id <= 2",
        "UPDATE shipments SET status = 'queued', failed_at = current_timestamp WHERE id <= 2",
    ] {
        let summary = run(&admin, &ctx, text).unwrap();
        assert_eq!(summary.rows, 2, "{text}");
        let stored = store(&admin, &ctx);
        let header = stored.previews.iter().find(|p| p.id == summary.id).unwrap();
        assert_eq!(header.clock_pin, Some(PIN_US), "{text}");
        for row in &header.rows {
            assert_eq!(row.after[3], pinned, "{text}");
        }
    }
    // Both statements give the same rows; the digests differ by id.
    let stored = store(&admin, &ctx);
    assert_eq!(stored.previews.len(), 2);
    assert_eq!(stored.previews[0].rows[0].after, stored.previews[1].rows[0].after);
    assert_ne!(stored.previews[0].digest, stored.previews[1].digest);
    assert_eq!(queued(&admin), 0);
}

#[test]
fn preview_expiry_is_capped_by_task_expiry() {
    // The task expires in a minute; the preview TTL is an hour.
    let expires = now_ms() + 60_000;
    let (_dir, admin) = setup(expires, "");
    let ctx = context(&admin);
    let def = admin.tasks().get(TASK).unwrap().unwrap();
    let summary = run(&admin, &ctx, &requeue(1, 1)).unwrap();
    assert_eq!(summary.expires_at, def.expires_at);
    let stored = store(&admin, &ctx);
    assert_eq!(stored.previews[0].expires_ms, def.expires_at.0 as u64);
    assert!(stored.previews[0].expires_ms < stored.previews[0].created_ms + 3_600_000);
}
