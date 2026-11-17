//! GD-0061 R3-401: the statement guard (design-3 section 7). A task statement
//! runs within its `statement_ms`, at most `concurrent_statements` of a task
//! run at once, and all task sessions together stay under the all-agent
//! ceiling. Every test ends with no statement slot held.
#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::{
    operations::{Bindings, Request},
    relational::SqlRow,
    roles::{NewRole, TablePrivilege},
    task::{
        context::open_context,
        definition::{
            DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskName, WallClockMs,
        },
        Budget, TaskError,
    },
    Admin, Cancellation, CoreError, Cursor, Engine, Limits, SqlSession,
};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ROWS: &[&str] = &[
    "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
     attempt_count int)",
    "INSERT INTO shipments VALUES (1, 42, 'failed', 1), (2, 7, 'failed', 0), \
     (3, 42, 'queued', 2), (4, 9, 'failed', 0), (5, 42, 'failed', 3), (6, 42, 'done', 0)",
];

const ALL: &str = "SELECT id, status FROM shipments";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

struct Fixture {
    path: PathBuf,
    admin: Option<Admin>,
}

impl Fixture {
    /// Tasks `t1` (executor `agent1`) and `t2` (executor `agent2`) on
    /// `merchant_id = 42`, with the given statement time and concurrency.
    fn new(name: &str, limits: [(u64, u32); 2]) -> Self {
        Self::with_limits(name, Limits::default(), limits)
    }

    fn with_limits(name: &str, engine: Limits, limits: [(u64, u32); 2]) -> Self {
        let path = common::fresh(name);
        let admin = Admin::create_engine(&path, engine, Engine::V2).unwrap();
        let f = Self {
            path,
            admin: Some(admin),
        };
        for sql in ROWS {
            f.admin().sql_session().execute(sql, &[], None).unwrap();
        }
        let roles = f.admin().roles();
        for (name, task_only) in [("controller", false), ("agent1", true), ("agent2", true)] {
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
            .grant_table("shipments", "controller", &[TablePrivilege::Select])
            .unwrap();
        let issuer = roles.get("controller").unwrap().unwrap();
        for (n, (statement_ms, concurrent_statements)) in limits.into_iter().enumerate() {
            f.admin()
                .tasks()
                .create(
                    &issuer,
                    NewTask {
                        name: format!("t{}", n + 1),
                        executor: format!("agent{}", n + 1),
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
                            rows_read: 1_000_000,
                            rows_returned: 1_000_000,
                            bytes_returned: 1 << 30,
                        },
                        limits: LoadLimits {
                            statement_ms,
                            concurrent_statements,
                            ..LoadLimits::default()
                        },
                        approval_threshold: 0,
                        expires: "2099-12-31 00:00:00+00".into(),
                    },
                    WallClockMs(now_ms()),
                )
                .unwrap();
        }
        f
    }

    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }

    /// A fresh session bound to task `t<n>`.
    fn agent(&self, n: u32) -> SqlSession {
        let mut s = self.admin().sql_session();
        let ctx = open_context(
            &s,
            &TaskName::new(&format!("t{n}")).unwrap(),
            &format!("agent{n}"),
            [7; 32],
            WallClockMs(now_ms()),
            0,
        )
        .unwrap();
        s.bind_task(ctx).unwrap();
        s.set_disclosure_chunk(0, 0);
        s
    }

    fn slots(&self) -> (usize, u64) {
        self.admin().task_statement_slots()
    }

    /// The stop condition of every test: no slot is held.
    fn assert_idle(&self) {
        assert_eq!(self.slots(), (0, 0), "statement slots still held");
    }

    /// An application session holding the table's ACCESS EXCLUSIVE lock in
    /// an open transaction, so a read of it waits until cancelled.
    fn hold_table_lock(&self) -> SqlSession {
        let mut s = self.admin().sql_session();
        s.execute("BEGIN", &[], None).unwrap();
        s.execute("ALTER TABLE shipments ADD COLUMN extra int", &[], None)
            .unwrap();
        s
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin = None;
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Runs `sql` on the buffered path and releases its page.
fn buffered(s: &mut SqlSession, sql: &str) -> Result<Vec<SqlRow>, CoreError> {
    let page = s.execute_unreleased(sql, &[], None)?;
    s.release_page(page)
        .map(|released| released.into_inner().rows)
}

/// Runs a prepared statement on the buffered path and releases its page.
fn run_prepared(
    s: &mut SqlSession,
    p: &giggles_core::relational::Prepared,
) -> Result<Vec<SqlRow>, CoreError> {
    let page = s.execute_prepared_unreleased(p, &[], None)?;
    s.release_page(page)
        .map(|released| released.into_inner().rows)
}

fn open(s: &mut SqlSession, sql: &str) -> Result<Cursor, CoreError> {
    let p = s.prepare(sql)?;
    s.open_cursor(&p, &[])
}

/// Fetches one page of `rows` rows from a task cursor and releases it.
fn fetch(s: &SqlSession, cursor: &mut Cursor, rows: usize) -> Result<Vec<SqlRow>, CoreError> {
    let page = cursor.fetch_unreleased(rows, 1 << 20)?;
    s.release_page(page).map(|released| released.into_inner())
}

fn native_read(s: &mut SqlSession) -> Result<usize, String> {
    let request = Request::Read {
        table: "shipments".into(),
        filter: None,
        order: Vec::new(),
        limit: None,
        projection: vec!["id".into(), "status".into()],
    };
    let page = s
        .execute_native_unreleased(&request, &Bindings::new())
        .map_err(|e| e.sqlstate().unwrap_or("XX000").to_owned())?;
    s.release_page(page)
        .map(|released| released.into_inner().rows.len())
        .map_err(|e| giggles_core::sqlstate(&e).to_owned())
}

fn budget(budget: Budget) -> CoreError {
    CoreError::Task(TaskError::BudgetExceeded(budget))
}

#[test]
fn statement_time_cancels_scan() {
    let f = Fixture::new("task-limits-time", [(300, 2), (300, 2)]);
    let mut agent = f.agent(1);
    let p = agent.prepare(ALL).unwrap();
    assert_eq!(run_prepared(&mut agent, &p).unwrap().len(), 4);
    // The scan waits on a lock the application holds; the statement's own
    // time limit cancels it with 57014.
    let mut holder = f.hold_table_lock();
    let started = Instant::now();
    let error = run_prepared(&mut agent, &p).unwrap_err();
    let elapsed = started.elapsed();
    assert_eq!(giggles_core::sqlstate(&error), "57014", "{error:?}");
    assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(30), "{elapsed:?}");
    f.assert_idle();
    holder.execute("ROLLBACK", &[], None).unwrap();
    // The next statement gets a fresh deadline.
    assert_eq!(run_prepared(&mut agent, &p).unwrap().len(), 4);
    f.assert_idle();
}

#[test]
fn smaller_statement_timeout_still_wins() {
    // t1 has a long statement time; t2 a short one.
    let f = Fixture::new("task-limits-smaller", [(60_000, 2), (300, 2)]);
    let mut long = f.agent(1);
    let mut short = f.agent(2);
    let p_long = long.prepare(ALL).unwrap();
    let p_short = short.prepare(ALL).unwrap();
    let mut holder = f.hold_table_lock();
    // A caller deadline smaller than the task's statement time still wins.
    let started = Instant::now();
    let error = Cancellation::new(Some(Instant::now() + Duration::from_millis(300)))
        .scope(|| run_prepared(&mut long, &p_long))
        .unwrap_err();
    let elapsed = started.elapsed();
    assert_eq!(giggles_core::sqlstate(&error), "57014", "{error:?}");
    assert!(elapsed < Duration::from_secs(30), "{elapsed:?}");
    f.assert_idle();
    // A larger caller deadline does not extend the task's statement time.
    let started = Instant::now();
    let error = Cancellation::new(Some(Instant::now() + Duration::from_secs(600)))
        .scope(|| run_prepared(&mut short, &p_short))
        .unwrap_err();
    let elapsed = started.elapsed();
    assert_eq!(giggles_core::sqlstate(&error), "57014", "{error:?}");
    assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(30), "{elapsed:?}");
    f.assert_idle();
    holder.execute("ROLLBACK", &[], None).unwrap();
    f.assert_idle();
}

#[test]
fn third_concurrent_statement_refused_at_limit_two() {
    let f = Fixture::new("task-limits-concurrent", [(60_000, 2), (60_000, 2)]);
    let mut a = f.agent(1);
    let mut b = f.agent(1);
    let first = open(&mut a, ALL).unwrap();
    let second = open(&mut b, ALL).unwrap();
    assert_eq!(f.slots(), (2, 2));
    // The third statement of the task is refused at once, on every entry.
    assert_eq!(buffered(&mut a, ALL).unwrap_err(), budget(Budget::ConcurrentStatements));
    assert_eq!(
        open(&mut b, ALL).err().unwrap(),
        budget(Budget::ConcurrentStatements)
    );
    assert_eq!(native_read(&mut a), Err("54T01".to_owned()));
    assert_eq!(f.slots(), (2, 2));
    // Another task is not affected.
    assert_eq!(buffered(&mut f.agent(2), ALL).unwrap().len(), 4);
    drop(first);
    assert_eq!(f.slots(), (1, 1));
    assert_eq!(buffered(&mut a, ALL).unwrap().len(), 4);
    assert_eq!(native_read(&mut a), Ok(4));
    drop(second);
    f.assert_idle();
}

#[test]
fn slot_released_on_error_panic_and_cursor_drop() {
    let f = Fixture::new("task-limits-release", [(60_000, 1), (60_000, 1)]);
    let mut agent = f.agent(1);
    // An execution error.
    let error = buffered(&mut agent, "SELECT attempt_count / 0 FROM shipments").unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "22012", "{error:?}");
    f.assert_idle();
    // A refusal at the limit releases nothing it did not take.
    let cursor = open(&mut agent, ALL).unwrap();
    assert_eq!(f.slots(), (1, 1));
    assert_eq!(buffered(&mut agent, ALL).unwrap_err(), budget(Budget::ConcurrentStatements));
    assert_eq!(f.slots(), (1, 1));
    // Cursor drop.
    drop(cursor);
    f.assert_idle();
    // A panic while a cursor is open.
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _cursor = open(&mut agent, ALL).unwrap();
        assert_eq!(f.slots(), (1, 1));
        panic!("caller panics while the cursor is open");
    }));
    assert!(unwound.is_err());
    f.assert_idle();
    // Exhaustion releases the slot while the cursor is still alive.
    let mut cursor = open(&mut agent, ALL).unwrap();
    let mut rows = 0;
    while !cursor.is_exhausted() {
        rows += fetch(&agent, &mut cursor, 2).unwrap().len();
    }
    assert_eq!(rows, 4);
    f.assert_idle();
    drop(cursor);
    assert_eq!(buffered(&mut agent, ALL).unwrap().len(), 4);
    f.assert_idle();
}

#[test]
fn suspended_cursor_expires_after_statement_ms() {
    let f = Fixture::new("task-limits-cursor", [(1_000, 2), (1_000, 2)]);
    let mut agent = f.agent(1);
    let mut cursor = open(&mut agent, ALL).unwrap();
    assert_eq!(fetch(&agent, &mut cursor, 1).unwrap().len(), 1);
    assert_eq!(f.slots(), (1, 1));
    // The client suspends the cursor past the statement time.
    std::thread::sleep(Duration::from_millis(1_200));
    let error = fetch(&agent, &mut cursor, 1).unwrap_err();
    assert_eq!(error, CoreError::Cancelled);
    assert_eq!(giggles_core::sqlstate(&error), "57014");
    // The fetch closed the cursor and gave its slot back.
    f.assert_idle();
    assert!(cursor.is_exhausted());
    assert_eq!(fetch(&agent, &mut cursor, 1).unwrap_err(), CoreError::Cancelled);
    drop(cursor);
    f.assert_idle();
    assert_eq!(buffered(&mut agent, ALL).unwrap().len(), 4);
    f.assert_idle();
}

#[test]
fn all_agent_ceiling_refuses_across_tasks() {
    let f = Fixture::new("task-limits-ceiling", [(60_000, 2), (60_000, 2)]);
    f.admin().set_agent_statement_ceiling(2).unwrap();
    let mut one = f.agent(1);
    let mut two = f.agent(2);
    let first = open(&mut one, ALL).unwrap();
    let second = open(&mut two, ALL).unwrap();
    assert_eq!(f.slots(), (2, 2));
    // Task 1 is under its own limit of two, but all agents are at the ceiling.
    assert_eq!(buffered(&mut one, ALL).unwrap_err(), budget(Budget::AllAgents));
    assert_eq!(open(&mut f.agent(2), ALL).err().unwrap(), budget(Budget::AllAgents));
    assert_eq!(native_read(&mut one), Err("54T01".to_owned()));
    assert_eq!(f.slots(), (2, 2));
    // Applications are not counted against the ceiling.
    let rows = f.admin().sql_session().execute(ALL, &[], None).unwrap().rows;
    assert_eq!(rows.len(), 6);
    drop(second);
    assert_eq!(buffered(&mut one, ALL).unwrap().len(), 4);
    drop(first);
    f.assert_idle();
}

#[test]
fn ceiling_cannot_exceed_half_admitted() {
    let f = Fixture::new("task-limits-half", [(60_000, 2), (60_000, 2)]);
    let admin = f.admin();
    // Limits::default() admits 32 operations.
    assert_eq!(admin.set_agent_statement_ceiling(0), Err(CoreError::Limit));
    assert_eq!(admin.set_agent_statement_ceiling(17), Err(CoreError::Limit));
    assert_eq!(admin.set_agent_statement_ceiling(usize::MAX), Err(CoreError::Limit));
    admin.set_agent_statement_ceiling(16).unwrap();
    admin.set_agent_statement_ceiling(1).unwrap();
    // A refused setting leaves the ceiling as it was (1).
    let held = open(&mut f.agent(1), ALL).unwrap();
    assert_eq!(buffered(&mut f.agent(2), ALL).unwrap_err(), budget(Budget::AllAgents));
    drop(held);
    f.assert_idle();
    let small = Fixture::with_limits(
        "task-limits-half-small",
        Limits {
            admitted: 8,
            ..Limits::default()
        },
        [(60_000, 2), (60_000, 2)],
    );
    assert_eq!(small.admin().set_agent_statement_ceiling(5), Err(CoreError::Limit));
    small.admin().set_agent_statement_ceiling(4).unwrap();
    small.assert_idle();
}

#[test]
fn non_task_session_takes_no_slot() {
    let f = Fixture::new("task-limits-plain", [(60_000, 1), (60_000, 1)]);
    f.admin().set_agent_statement_ceiling(1).unwrap();
    let mut plain = f.admin().sql_session();
    assert_eq!(plain.execute(ALL, &[], None).unwrap().rows.len(), 6);
    let p = plain.prepare(ALL).unwrap();
    let mut cursor = plain.open_cursor(&p, &[]).unwrap();
    assert_eq!(cursor.fetch(2, 1 << 20).unwrap().len(), 2);
    f.assert_idle();
    // With the task and the ceiling both full, applications still run.
    let held = open(&mut f.agent(1), ALL).unwrap();
    assert_eq!(f.slots(), (1, 1));
    assert_eq!(plain.execute(ALL, &[], None).unwrap().rows.len(), 6);
    let mut other = plain.open_cursor(&p, &[]).unwrap();
    assert_eq!(other.fetch(10, 1 << 20).unwrap().len(), 6);
    let request = Request::Read {
        table: "shipments".into(),
        filter: None,
        order: Vec::new(),
        limit: None,
        projection: vec!["id".into()],
    };
    assert!(plain.execute_native(&request, &Bindings::new()).is_ok());
    assert_eq!(cursor.fetch(10, 1 << 20).unwrap().len(), 4);
    assert_eq!(f.slots(), (1, 1));
    drop(held);
    f.assert_idle();
}
