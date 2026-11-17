//! GD-0061 R3-308: the release protocol (design-3 section 4). A task page is
//! masked in memory, checked for validity, debited durably against the task's
//! disclosure caps (or covered by the session's reservation) and only then
//! handed over.
#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::{
    Admin, CoreError, CrashPoint, Engine, Limits, SqlSession,
    cancellation::Cancellation,
    operations::{
        Arithmetic, Bindings, Compare, Expr, Request, Transaction, TransactionStep, ValueExpr,
    },
    relational::{SqlResult, SqlRow, Value},
    roles::{NewRole, TablePrivilege},
    task::{
        Budget, DenyRule, TaskError,
        context::{TaskContext, open_context},
        definition::{
            DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskName,
            WallClockMs,
        },
        release::{Disclosure, row_cost, value_cost, with_release_probe},
        rows::{RowCounts, take_last_counts},
    },
    task::definition::TaskObjectId,
};
use std::cell::Cell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Barrier;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Visible rows are `merchant_id = 42`: ids 1, 3 and 5.
const ROWS: &[&str] = &[
    "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
     attempt_count int, customer_email text, card_token text)",
    "INSERT INTO shipments VALUES \
     (1, 42, 'failed', 1, 'ann@shop.example', 'tok_live_1111'), \
     (2, 7, 'failed', 0, 'bob@shop.example', 'tok_live_2222'), \
     (3, 42, 'queued', 2, 'cy@shop.example', 'tok_live_3333'), \
     (4, 9, 'failed', 0, 'di@shop.example', 'tok_live_4444'), \
     (5, 42, 'failed', 3, 'ed@shop.example', 'tok_live_5555'), \
     (6, 9, 'done', 0, 'fay@shop.example', 'tok_live_6666')",
];

const ALL: &str = "SELECT id, status FROM shipments ORDER BY id";

fn caps(rows_read: u64, rows_returned: u64, bytes_returned: u64) -> DisclosureCaps {
    DisclosureCaps { rows_read, rows_returned, bytes_returned }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

/// `ms` (Unix milliseconds, whole seconds) as `YYYY-MM-DD HH:MM:SS+00`.
fn utc_text(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil date from days since 1970-01-01 (H. Hinnant's algorithm).
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
}

struct Fixture {
    path: PathBuf,
    admin: Option<Admin>,
    task: TaskObjectId,
}

impl Fixture {
    fn new(name: &str, disclosure: DisclosureCaps) -> Self {
        Self::expiring(name, disclosure, "2099-12-31 00:00:00+00".into())
    }

    fn expiring(name: &str, disclosure: DisclosureCaps, expires: String) -> Self {
        let path = common::fresh(name);
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut f = Self { path, admin: Some(admin), task: TaskObjectId(0) };
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
                    disclosure_caps: disclosure,
                    limits: LoadLimits::default(),
                    approval_threshold: 0,
                    expires,
                },
                WallClockMs(now_ms()),
            )
            .unwrap();
        f.task = f.context().task;
        f
    }

    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }

    fn context(&self) -> TaskContext {
        let s = self.admin().sql_session();
        open_context(
            &s,
            &TaskName::new("inc-436").unwrap(),
            &"repair_agent".to_string(),
            [7; 32],
            WallClockMs(now_ms()),
            0,
        )
        .unwrap()
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
        s.bind_task(self.context()).unwrap();
        s
    }

    fn disclosure(&self) -> Disclosure {
        self.admin().task_disclosure(self.task).unwrap()
    }

    fn revoke(&self) {
        let issuer = self.admin().roles().get("incident_controller").unwrap().unwrap();
        self.admin().tasks().revoke(&issuer, "inc-436").unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin = None;
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Runs `sql` in `s` and releases its one page.
fn release(s: &mut SqlSession, sql: &str) -> Result<SqlResult, CoreError> {
    let page = s.execute_unreleased(sql, &[], None)?;
    assert!(page.is_task());
    s.release_page(page).map(|released| released.into_inner())
}

fn ids(rows: &[SqlRow]) -> Vec<Value> {
    rows.iter().map(|row| row.values[0].clone()).collect()
}

fn task(e: TaskError) -> CoreError {
    CoreError::Task(e)
}

/// Nothing was charged: no rows read or returned, no bytes, no debit. A
/// refused statement still has its read-log record (R3-309).
fn uncharged(d: &Disclosure) {
    assert_eq!((d.rows_read, d.rows_returned, d.bytes_returned), (0, 0, 0), "{d:?}");
    assert!(d.debits.is_empty(), "{d:?}");
}

/// The head equals the sum of the debit records (design-3 section 6).
fn reconciles(d: &Disclosure) {
    let sum = |f: fn(&giggles_core::task::release::DebitView) -> u64| -> u64 {
        d.debits.iter().map(f).sum()
    };
    assert_eq!(d.rows_read, sum(|x| x.rows_read), "{d:?}");
    assert_eq!(d.rows_returned, sum(|x| x.rows_returned + x.reserved_rows), "{d:?}");
    assert_eq!(d.bytes_returned, sum(|x| x.bytes + x.reserved_bytes), "{d:?}");
}

#[test]
fn masked_value_never_in_released_page() {
    let f = Fixture::new("task-release-mask", caps(1000, 1000, 1 << 20));
    let secrets = |rows: &[SqlRow]| {
        let text = format!("{rows:?}");
        assert!(!text.contains("@shop.example"), "{text}");
        assert!(!text.contains("tok_live"), "{text}");
    };
    // SQL, buffered: NULL for the plain mask, the constant for the other.
    let mut s = f.exact();
    let result = release(
        &mut s,
        "SELECT id, customer_email, card_token, status FROM shipments ORDER BY id",
    )
    .unwrap();
    secrets(&result.rows);
    assert_eq!(ids(&result.rows), [Value::Int(1), Value::Int(3), Value::Int(5)]);
    for row in &result.rows {
        assert_eq!(row.values[1], Value::Null);
        assert_eq!(row.values[2], Value::Text("****".into()));
    }
    // SQL, cursor, page by page.
    let p = s.prepare("SELECT card_token, id FROM shipments ORDER BY id").unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    let mut seen = Vec::new();
    while !cursor.is_exhausted() {
        let page = cursor.fetch_unreleased(2, 1 << 20).unwrap();
        seen.extend(s.release_page(page).unwrap().into_inner());
    }
    secrets(&seen);
    assert_eq!(seen.len(), 3);
    assert!(seen.iter().all(|row| row.values[0] == Value::Text("****".into())));
    // SELECT * runs expanded to the readable and masked columns.
    let star = release(&mut s, "SELECT * FROM shipments ORDER BY id").unwrap();
    secrets(&star.rows);
    assert_eq!(star.rows.len(), 3);
    // Native, empty projection (readable and masked).
    let read = Request::Read {
        table: "shipments".into(),
        filter: None,
        order: Vec::new(),
        limit: None,
        projection: Vec::new(),
    };
    let page = s.execute_native_unreleased(&read, &Bindings::new()).unwrap();
    let outcome = s.release_page(page).unwrap().into_inner();
    secrets(&outcome.rows);
    assert_eq!(outcome.rows.len(), 3);
    let at = |name: &str| outcome.columns.iter().position(|c| c == name).unwrap();
    for row in &outcome.rows {
        assert_eq!(row.values[at("customer_email")], Value::Null);
        assert_eq!(row.values[at("card_token")], Value::Text("****".into()));
    }
    // Every released page was charged.
    let d = f.disclosure();
    assert!(d.rows_returned >= 12, "{d:?}");
    reconciles(&d);
}

#[test]
fn debit_precedes_handover() {
    let f = Fixture::new("task-release-handover", caps(1000, 1000, 1 << 20));
    let mut s = f.exact();
    let page = s.execute_unreleased(ALL, &[], None).unwrap();
    let admin = f.admin().clone();
    let task = f.task;
    let seen = Rc::new(Cell::new(None));
    let probe = seen.clone();
    let released = with_release_probe(
        move || probe.set(Some(admin.task_disclosure(task).unwrap())),
        || s.release_page(page),
    )
    .unwrap()
    .into_inner();
    // The probe ran after the debit committed and before the page was handed
    // over: the committed head already charged every released row and byte.
    let at_probe = seen.take().expect("the probe ran before the hand-over");
    assert_eq!(released.rows.len(), 3);
    assert_eq!(at_probe.rows_returned, 3);
    let bytes: u64 = released.rows.iter().map(|row| row_cost(&row.values)).sum();
    assert_eq!(at_probe.bytes_returned, bytes);
    assert_eq!(at_probe.rows_read, 3);
    assert_eq!(at_probe.debits.len(), 1);
    assert_eq!(at_probe.read_logs, 1);
    assert_eq!(f.disclosure(), at_probe);
}

#[test]
fn budget_exceeded_releases_nothing_and_writes_nothing() {
    for (cap, budget) in [
        (caps(1000, 2, 1 << 20), Budget::RowsReturned),
        (caps(1000, 1000, 40), Budget::BytesReturned),
        (caps(2, 2, 1 << 20), Budget::RowsRead),
    ] {
        let f = Fixture::new("task-release-budget", cap);
        for mut s in [f.exact(), f.agent()] {
            assert_eq!(
                release(&mut s, ALL).map(|_| ()),
                Err(task(TaskError::BudgetExceeded(budget))),
                "{budget:?}"
            );
            assert_eq!(s.task_reservation(), Some(Default::default()));
        }
        // Nothing was returned: no returned rows or bytes and no reservation.
        // Each refused statement has its read-log record, and the rows it
        // read are charged at its end, saturating at the cap (R3-309).
        let d = f.disclosure();
        assert_eq!((d.rows_returned, d.bytes_returned, d.read_logs), (0, 0, 2), "{budget:?}");
        assert!(d.rows_read <= cap.rows_read, "{d:?}");
        assert!(d.debits.iter().all(|x| x.rows_returned + x.reserved_rows + x.bytes + x.reserved_bytes == 0));
        reconciles(&d);
    }
}

#[test]
fn two_sessions_race_last_allowance_one_wins() {
    for _ in 0..4 {
        let f = Fixture::new("task-release-race", caps(1000, 1, 1 << 20));
        let barrier = Barrier::new(2);
        let results: Vec<Result<usize, CoreError>> = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..2)
                .map(|_| {
                    let (f, barrier) = (&f, &barrier);
                    scope.spawn(move || {
                        let mut s = f.exact();
                        let page = s
                            .execute_unreleased("SELECT id FROM shipments WHERE id = 1", &[], None)
                            .unwrap();
                        barrier.wait();
                        s.release_page(page).map(|r| r.into_inner().rows.len())
                    })
                })
                .collect();
            threads.into_iter().map(|t| t.join().unwrap()).collect()
        });
        let won: Vec<_> = results.iter().filter(|r| r.is_ok()).collect();
        assert_eq!(won.len(), 1, "{results:?}");
        assert_eq!(won[0], &Ok(1));
        assert!(results.contains(&Err(task(TaskError::BudgetExceeded(Budget::RowsReturned)))));
        let d = f.disclosure();
        assert_eq!(d.rows_returned, 1);
        // One returned-row debit; the loser's read row is charged at its end.
        assert_eq!(d.debits.iter().filter(|x| x.rows_returned > 0).count(), 1);
        assert_eq!(d.read_logs, 2);
        reconciles(&d);
    }
}

#[test]
fn revocation_between_pages_stops_second_page() {
    let f = Fixture::new("task-release-revoke", caps(1000, 1000, 1 << 20));
    // The default reservation covers the second page: validity is still
    // checked on every page.
    let mut s = f.agent();
    let p = s.prepare(ALL).unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    let first = cursor.fetch_unreleased(1, 1 << 20).unwrap();
    assert_eq!(s.release_page(first).unwrap().into_inner().len(), 1);
    assert!(s.task_reservation().unwrap().rows > 0);
    let before = f.disclosure();
    f.revoke();
    let second = cursor.fetch_unreleased(1, 1 << 20);
    match second {
        Ok(page) => assert_eq!(s.release_page(page).map(|_| ()), Err(task(TaskError::Revoked))),
        Err(e) => assert_eq!(e, task(TaskError::Revoked)),
    }
    assert_eq!(s.still_valid(), Err(task(TaskError::Revoked)));
    assert_eq!(f.disclosure(), before);
}

#[test]
fn expiry_between_pages() {
    // Expires on the next whole second at least two seconds ahead.
    let expires_ms = (now_ms() / 1000 + 3) * 1000;
    let f = Fixture::expiring(
        "task-release-expiry",
        caps(1000, 1000, 1 << 20),
        utc_text(expires_ms),
    );
    let mut s = f.agent();
    let p = s.prepare(ALL).unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    let first = cursor.fetch_unreleased(1, 1 << 20).unwrap();
    assert_eq!(s.release_page(first).unwrap().into_inner().len(), 1);
    let second = cursor.fetch_unreleased(1, 1 << 20).unwrap();
    let before = f.disclosure();
    while now_ms() <= expires_ms + 50 {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(s.release_page(second).map(|_| ()), Err(task(TaskError::Expired)));
    assert_eq!(s.still_valid(), Err(task(TaskError::Expired)));
    assert_eq!(f.disclosure(), before);
}

#[test]
fn definition_change_denies() {
    // R3 has no ALTER TASK (a revoke bumps the version and is `Revoked`), so
    // the session is bound to a version the catalog does not hold.
    let f = Fixture::new("task-release-definition", caps(1000, 1000, 1 << 20));
    let mut ctx = f.context();
    ctx.bound_def_version += 1;
    let mut s = f.admin().sql_session();
    s.bind_task(ctx).unwrap();
    let changed = task(TaskError::Denied(DenyRule::DefinitionChanged));
    // Both are refused at statement start, before they run: the column rules
    // see the version mismatch for a table read and a table-free one alike.
    assert_eq!(release(&mut s, ALL).map(|_| ()), Err(changed.clone()));
    assert_eq!(release(&mut s, "SELECT 1").map(|_| ()), Err(changed.clone()));
    assert_eq!(s.still_valid(), Err(changed));
    let d = f.disclosure();
    uncharged(&d);
    assert_eq!(d.read_logs, 2);
}

#[test]
fn crash_before_commit_not_charged_not_sent() {
    let mut f = Fixture::new("task-release-crash-before", caps(1000, 1000, 1 << 20));
    let task_id = f.task;
    {
        let mut s = f.exact();
        let page = s.execute_unreleased(ALL, &[], None).unwrap();
        f.admin().set_crash_point(CrashPoint::ErrorBeforeCommit).unwrap();
        let e = s.release_page(page).map(|_| ()).unwrap_err();
        assert!(matches!(e, CoreError::Storage(_)), "{e:?}");
        f.admin().set_crash_point(CrashPoint::None).unwrap();
    }
    // Reopened from disk: nothing was charged.
    f.admin = None;
    let admin = Admin::open(&f.path, Limits::default()).unwrap();
    assert_eq!(admin.task_disclosure(task_id).unwrap(), Disclosure::default());
    f.admin = Some(admin);
}

#[test]
fn crash_after_commit_charged() {
    let mut f = Fixture::new("task-release-crash-after", caps(1000, 1000, 1 << 20));
    let task_id = f.task;
    {
        let mut s = f.exact();
        let page = s.execute_unreleased(ALL, &[], None).unwrap();
        f.admin().set_crash_point(CrashPoint::ErrorAfterCommit).unwrap();
        // The response is lost after the commit: the page is not handed over.
        let e = s.release_page(page).map(|_| ()).unwrap_err();
        assert!(matches!(e, CoreError::Storage(_)), "{e:?}");
        f.admin().set_crash_point(CrashPoint::None).unwrap();
    }
    // Reopened from disk: the page was charged (over-counting only).
    f.admin = None;
    let admin = Admin::open(&f.path, Limits::default()).unwrap();
    let d = admin.task_disclosure(task_id).unwrap();
    assert_eq!((d.rows_read, d.rows_returned), (3, 3));
    assert_eq!((d.debits.len(), d.read_logs), (1, 1));
    reconciles(&d);
    f.admin = Some(admin);
}

#[test]
fn reservation_forfeited_never_refunded() {
    let f = Fixture::new("task-release-forfeit", caps(1000, 1000, 1 << 20));
    let mut s = f.agent();
    assert_eq!(release(&mut s, ALL).unwrap().rows.len(), 3);
    // remaining 1000: max(3, min(256, 1000 / 8)) = 125 rows debited.
    let d = f.disclosure();
    assert_eq!(d.rows_returned, 125);
    assert_eq!(s.task_reservation().unwrap().rows, 122);
    // A second statement's page is covered by the reservation: its debit
    // charges only the rows it read (`rows_read` is always exact).
    assert_eq!(release(&mut s, ALL).unwrap().rows.len(), 3);
    let charged = f.disclosure();
    assert_eq!(charged.rows_returned, 125);
    assert_eq!(charged.rows_read, 6);
    let last = charged.debits.last().unwrap();
    assert_eq!((last.rows_read, last.rows_returned, last.reserved_rows), (3, 0, 0));
    assert_eq!(s.task_reservation().unwrap().rows, 119);
    // Session end forfeits the reservation: the head keeps it.
    drop(s);
    assert_eq!(f.disclosure(), charged);
    // A new session debits afresh; nothing was refunded.
    let mut t = f.agent();
    assert_eq!(t.task_reservation().unwrap(), Default::default());
    assert_eq!(release(&mut t, ALL).unwrap().rows.len(), 3);
    let after = f.disclosure();
    assert!(after.rows_returned > charged.rows_returned, "{after:?}");
    reconciles(&after);
}

#[test]
fn reservation_never_exceeds_remaining() {
    let cap = 40;
    let f = Fixture::new("task-release-remaining", caps(1000, cap, 1 << 20));
    let mut s = f.agent();
    let mut released = 0;
    loop {
        match release(&mut s, ALL) {
            Ok(result) => released += result.rows.len() as u64,
            Err(e) => {
                assert_eq!(e, task(TaskError::BudgetExceeded(Budget::RowsReturned)));
                break;
            }
        }
        let d = f.disclosure();
        let reserved = s.task_reservation().unwrap();
        // The head never passes the cap, and what was released plus what is
        // still reserved is exactly what was debited.
        assert!(d.rows_returned <= cap, "{d:?}");
        assert!(d.bytes_returned <= 1 << 20, "{d:?}");
        assert_eq!(released + reserved.rows, d.rows_returned, "{d:?} {reserved:?}");
        for debit in &d.debits {
            assert!(debit.reserved_rows <= cap);
        }
        reconciles(&d);
    }
    let d = f.disclosure();
    assert!(d.rows_returned <= cap);
    assert!(released <= cap);
    // A failed release forfeits what was reserved.
    assert_eq!(s.task_reservation(), Some(Default::default()));
}

#[test]
fn cancelled_statement_keeps_debits() {
    let f = Fixture::new("task-release-cancel", caps(1000, 1000, 1 << 20));
    let mut s = f.agent();
    let p = s.prepare(ALL).unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    let first = cursor.fetch_unreleased(1, 1 << 20).unwrap();
    s.release_page(first).unwrap();
    let charged = f.disclosure();
    assert!(charged.rows_returned >= 1);
    // The statement is cancelled before its next page is released.
    let second = cursor.fetch_unreleased(1, 1 << 20).unwrap();
    let cancel = Cancellation::new(None);
    assert!(cancel.cancel());
    let e = cancel.scope(|| s.release_page(second).map(|_| ())).unwrap_err();
    assert_eq!(e, CoreError::Cancelled);
    s.finish_task_statement(Err(&e));
    // Nothing is refunded: the debits stay, and rows read but never released
    // are charged at the statement's end.
    let after = f.disclosure();
    assert_eq!(after.rows_returned, charged.rows_returned);
    assert_eq!(after.bytes_returned, charged.bytes_returned);
    assert!(after.rows_read >= charged.rows_read, "{after:?}");
    assert!(after.debits.len() >= charged.debits.len());
    for (kept, before) in after.debits.iter().zip(&charged.debits) {
        assert_eq!(kept, before);
    }
    reconciles(&after);
}

#[test]
fn cost_is_fixed_per_value_type() {
    let f = Fixture::new("task-release-cost", caps(1000, 1000, 1 << 20));
    let row = f
        .admin()
        .sql_session()
        .execute(
            "SELECT NULL, true, 42::bigint, 1.5::float8, 1.5::real, DATE '2026-11-05', \
             TIME '17:00:00', TIMESTAMP '2026-11-05 17:00:00', \
             TIMESTAMPTZ '2026-11-05 17:00:00+00', TIMETZ '17:00:00+02', \
             INTERVAL '1 day', 12.340::numeric, 'héllo', '\\x0102'::bytea",
            &[],
            None,
        )
        .unwrap()
        .rows
        .remove(0)
        .values;
    // (value kind, cost = 1 + size)
    let table: [(&str, u64); 14] = [
        ("Null", 1),
        ("Bool", 2),
        ("Int", 9),
        ("Float8", 9),
        ("Float4", 5),
        ("Date", 5),
        ("Time", 9),
        ("Timestamp", 9),
        ("TimestampTz", 9),
        ("TimeTz", 13),
        ("Interval", 17),
        ("Numeric", 7),
        ("Text", 7),
        ("Bytes", 3),
    ];
    assert_eq!(row.len(), table.len());
    for (value, (kind, cost)) in row.iter().zip(table) {
        assert!(format!("{value:?}").starts_with(kind), "{value:?} is not {kind}");
        assert_eq!(value_cost(value), cost, "{kind}");
    }
    assert_eq!(row_cost(&row), 8 + table.iter().map(|(_, c)| c).sum::<u64>());
    assert_eq!(row_cost(&[]), 8);
    // The same values cost the same whatever the column holds them, and a
    // masked value costs what replaced it.
    assert_eq!(value_cost(&Value::Text("****".into())), 5);
    assert_eq!(value_cost(&Value::Int(i64::MIN)), value_cost(&Value::Int(0)));
}

// R3-308 follow-up: a statement the column rules deny is refused before it
// runs. No row is read and nothing is debited, so no execution error (22012,
// 22P02) or timing can tell the agent about a column it may not read.

/// Denied reads, each with the SQLSTATE it raises when it runs outside a
/// task. `merchant_id` is hidden and is 42 on every visible row, so
/// `merchant_id - 42` is zero on an in-scope row; `card_token` is masked and
/// does not cast to int; `ledger` is not the task table and `ledger.n` is 0.
const DENIED: &[(&str, DenyRule, &str)] = &[
    (
        "SELECT id FROM shipments WHERE 1 / (merchant_id - 42) > 0",
        DenyRule::ColumnNotAvailable,
        "22012",
    ),
    (
        "SELECT id FROM shipments ORDER BY 1 / (merchant_id - 42)",
        DenyRule::ColumnNotAvailable,
        "22012",
    ),
    (
        "SELECT id, 1 / (merchant_id - 42) FROM shipments",
        DenyRule::ColumnNotAvailable,
        "22012",
    ),
    (
        "SELECT sum(1 / (merchant_id - 42)) FROM shipments",
        DenyRule::ColumnNotAvailable,
        "22012",
    ),
    (
        "SELECT id FROM shipments WHERE card_token::int > 0",
        DenyRule::MaskedColumnPosition,
        "22P02",
    ),
    (
        "SELECT id, card_token::int FROM shipments",
        DenyRule::MaskedColumnPosition,
        "22P02",
    ),
    ("SELECT 1 / n FROM ledger", DenyRule::OtherTable, "22012"),
];

fn with_ledger(f: &Fixture) {
    let mut admin = f.admin().sql_session();
    admin
        .execute("CREATE TABLE ledger (id bigint primary key, n bigint)", &[], None)
        .unwrap();
    admin.execute("INSERT INTO ledger VALUES (1, 0)", &[], None).unwrap();
    f.admin()
        .roles()
        .grant_table("ledger", "repair_agent", &[TablePrivilege::Select])
        .unwrap();
}

/// Runs `sql` in `s` on the buffered path (`cursor == false`) or through a
/// cursor, releasing every page.
fn run_released(s: &mut SqlSession, sql: &str, cursor: bool) -> Result<(), CoreError> {
    if !cursor {
        let page = s.execute_unreleased(sql, &[], None)?;
        return s.release_page(page).map(|_| ());
    }
    let p = s.prepare(sql)?;
    let mut cursor = s.open_cursor(&p, &[])?;
    while !cursor.is_exhausted() {
        let page = cursor.fetch_unreleased(2, 1 << 20)?;
        s.release_page(page)?;
    }
    Ok(())
}

fn int(value: i64) -> Expr {
    Expr::Value(ValueExpr::Literal(Value::Int(value)))
}

/// `1 / (field - at) >= 0`: a division by zero on the row where `field = at`.
fn inverse_at(field: &str, at: i64) -> Expr {
    let difference = Expr::Arithmetic(
        Box::new(Expr::Field(field.into())),
        Arithmetic::Subtract,
        Box::new(int(at)),
    );
    let quotient = Expr::Arithmetic(Box::new(int(1)), Arithmetic::Divide, Box::new(difference));
    Expr::Compare(Box::new(quotient), Compare::Ge, Box::new(int(0)))
}

fn read_ids(filter: Expr) -> Request {
    Request::Read {
        table: "shipments".into(),
        filter: Some(filter),
        order: Vec::new(),
        limit: None,
        projection: vec!["id".into()],
    }
}

#[test]
fn denied_read_never_executes() {
    let f = Fixture::new("task-release-denied-exec", caps(1000, 1000, 1 << 20));
    with_ledger(&f);
    for &(sql, rule, untasked) in DENIED {
        // Outside a task the statement runs and fails on a row.
        let e = f.admin().sql_session().execute(sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&e), untasked, "{sql}: {e:?}");
        // In the task session the denial wins over the execution error, on
        // the buffered path and on the cursor path.
        for cursor in [false, true] {
            assert_eq!(
                run_released(&mut f.exact(), sql, cursor),
                Err(task(TaskError::Denied(rule))),
                "{sql} cursor={cursor}"
            );
        }
        // The refusal comes at statement start: no page is made.
        assert_eq!(
            f.exact().execute_unreleased(sql, &[], None).map(|_| ()),
            Err(task(TaskError::Denied(rule))),
            "{sql}"
        );
    }
    // The same probe on a readable column still runs in the scope.
    for cursor in [false, true] {
        let e = run_released(
            &mut f.exact(),
            "SELECT id FROM shipments WHERE 1 / (id - 3) > 0",
            cursor,
        )
        .unwrap_err();
        assert_eq!(giggles_core::sqlstate(&e), "22012", "{e:?}");
    }
    // Native: a request the rules deny is refused at binding (R3-304), never
    // with the 22012 its filter raises outside a task.
    let read = read_ids(inverse_at("merchant_id", 42));
    let e = f
        .admin()
        .sql_session()
        .execute_native(&read, &Bindings::new())
        .unwrap_err();
    assert_eq!(e.sqlstate(), Some("22012"), "{e:?}");
    let e = f
        .exact()
        .execute_native_unreleased(&read, &Bindings::new())
        .map(|_| ())
        .unwrap_err();
    assert_eq!(e.sqlstate(), Some("42501"), "{e:?}");
}

#[test]
fn denied_read_reads_no_rows() {
    let f = Fixture::new("task-release-denied-rows", caps(1000, 1000, 1 << 20));
    with_ledger(&f);
    // An allowed read is counted by the scope.
    let _ = take_last_counts();
    run_released(&mut f.exact(), ALL, false).unwrap();
    assert_eq!(take_last_counts(), Some(RowCounts { scanned: 6, read: 3 }));
    for &(sql, rule, _) in DENIED {
        for cursor in [false, true] {
            let _ = take_last_counts();
            let e = run_released(&mut f.exact(), sql, cursor).unwrap_err();
            assert_eq!(e, task(TaskError::Denied(rule)), "{sql}");
            // No scoped source was opened: not one row was pulled.
            assert_eq!(take_last_counts(), None, "{sql} cursor={cursor}");
        }
    }
    // A denied native request pulls no row either.
    let _ = take_last_counts();
    let read = read_ids(inverse_at("merchant_id", 42));
    assert!(f.exact().execute_native_unreleased(&read, &Bindings::new()).is_err());
    assert_eq!(take_last_counts(), None);
    // A native transaction block has no output plan yet: it is refused once
    // its steps are bound, before any step reads a row. Outside a task the
    // same block reaches row 3 and divides by zero.
    let block = Transaction {
        read_only: true,
        steps: vec![TransactionStep::Execute(read_ids(inverse_at("id", 3)))],
        emit: None,
    };
    let e = f
        .admin()
        .sql_session()
        .execute_native_transaction(&block, &Bindings::new())
        .unwrap_err();
    assert_eq!(e.sqlstate(), Some("22012"), "{e:?}");
    let _ = take_last_counts();
    let e = f
        .exact()
        .execute_native_transaction_unreleased(&block, &Bindings::new())
        .map(|_| ())
        .unwrap_err();
    assert_eq!(e.sqlstate(), Some("0A000"), "{e:?}");
    assert_eq!(take_last_counts(), None);
    // A step the rules deny keeps its own refusal.
    let denied_step = Transaction {
        read_only: true,
        steps: vec![TransactionStep::Execute(read_ids(inverse_at("merchant_id", 42)))],
        emit: None,
    };
    let e = f
        .exact()
        .execute_native_transaction_unreleased(&denied_step, &Bindings::new())
        .map(|_| ())
        .unwrap_err();
    assert_eq!(e.sqlstate(), Some("42501"), "{e:?}");
    assert_eq!(take_last_counts(), None);
}

#[test]
fn denied_read_writes_no_debit() {
    let f = Fixture::new("task-release-denied-debit", caps(1000, 1000, 1 << 20));
    with_ledger(&f);
    for session in [Fixture::exact, Fixture::agent] {
        for &(sql, rule, _) in DENIED {
            for cursor in [false, true] {
                let mut s = session(&f);
                let e = run_released(&mut s, sql, cursor).unwrap_err();
                assert_eq!(e, task(TaskError::Denied(rule)), "{sql}");
                // The statement's end charges no `rows_read`: none was read.
                s.finish_task_statement(Err(&e));
                assert_eq!(s.task_reservation(), Some(Default::default()), "{sql}");
                drop(s);
                // No charge and no debit (the refusal has its read-log record).
                uncharged(&f.disclosure());
            }
        }
    }
    // A refused native block charges nothing either.
    let block = Transaction {
        read_only: true,
        steps: vec![TransactionStep::Execute(read_ids(inverse_at("id", 99)))],
        emit: None,
    };
    let s = &mut f.exact();
    assert!(s.execute_native_transaction_unreleased(&block, &Bindings::new()).is_err());
    s.finish_task_statement(Ok(()));
    uncharged(&f.disclosure());
    // An allowed read in a session after a refusal is charged as usual.
    let mut s = f.exact();
    let e = run_released(&mut s, DENIED[0].0, false).unwrap_err();
    s.finish_task_statement(Err(&e));
    run_released(&mut s, ALL, false).unwrap();
    s.finish_task_statement(Ok(()));
    let d = f.disclosure();
    assert_eq!((d.rows_read, d.rows_returned), (3, 3), "{d:?}");
    reconciles(&d);
}
