//! GD-0061 R3-310: the Execute path of a task session through the release
//! protocol, driven through `State::handle` with real Parse, Bind and Execute
//! bodies. `Select` is in `task::admit::ENABLED` since R3-315.
use super::*;
use crate::{AgentBinding, Service};
use giggles_core::{
    Admin, Engine, Limits,
    roles::{NewRole, TablePrivilege},
    task::{
        StatementClass,
        definition::{DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText},
        readlog::{ENTRY_PGWIRE_EXTENDED, class_code},
    },
};
use std::sync::Arc;

const ROWS: i64 = 600;
const SELECT: &str = "SELECT id, status FROM shipments WHERE merchant_id = 42 ORDER BY id";
static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// `shipments` with `ROWS` rows of merchant 42 and three of merchant 7; task
/// `inc-436` sees merchant 42, reads `id` and `merchant_id`, and gets `status`
/// masked as `***`.
fn fixture(rows_returned: u64) -> (Arc<Service>, Session) {
    let path = crate::test_support::fixture_root().join(format!(
        "task-release-ext-{}-{}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
    let values = (1..=ROWS)
        .map(|id| format!("({id}, 42, 'failed')"))
        .chain((1..=3).map(|i| format!("({}, 7, 'secret')", ROWS + i)))
        .collect::<Vec<_>>()
        .join(", ");
    for statement in [
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text)".to_owned(),
        format!("INSERT INTO shipments VALUES {values}"),
    ] {
        admin.sql_session().execute(&statement, &[], None).unwrap();
    }
    let roles = admin.roles();
    for (name, task_only) in [("incident_controller", false), ("agent_exec", true)] {
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
        roles
            .grant_table("shipments", name, &[TablePrivilege::Select])
            .unwrap();
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    let clock = crate::clock::Clock::new(0, 3600).unwrap();
    let now = crate::agent::wall_now(&clock).unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: "inc-436".into(),
                executor: "agent_exec".into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: vec!["id".into(), "merchant_id".into()],
                masked: vec![("status".into(), Some(Value::Text("***".into())))],
                untrusted_text: Vec::new(),
                allowed_write: None,
                mutation_caps: MutationCaps::default(),
                disclosure_caps: DisclosureCaps {
                    rows_read: 10_000,
                    rows_returned,
                    bytes_returned: 10_000_000,
                },
                limits: LoadLimits::default(),
                approval_threshold: 0,
                expires: "2999-01-01 00:00:00+00".into(),
            },
            now,
        )
        .unwrap();
    let fingerprint = "ab".repeat(32);
    let service = Service::with_agent(
        admin,
        vec![],
        vec![AgentBinding {
            fingerprint: fingerprint.clone(),
            executor: "agent_exec".into(),
            tasks: vec!["inc-436".into()],
        }],
        clock,
        crate::admission::ServiceLimits::default(),
    )
    .unwrap();
    let session = service.agent_session(&fingerprint, None).unwrap();
    // Exact debits: every page writes its own debit record.
    session.sql.set_disclosure_chunk(0, 0);
    (service, session)
}

fn state() -> State {
    State {
        output: None,
        diagnostic: Diagnostic::default(),
        admin_completed: false,
        implicit: false,
        settings: crate::pg_settings::SessionSettings::default(),
        statements: BTreeMap::new(),
        named_statement_bytes: 0,
        portals: BTreeMap::new(),
        cursors: cursor::Cursors::default(),
        drain: false,
        copy: None,
        copy_completion: vec![],
        copy_active: None,
    }
}

fn cstr(text: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(text.as_bytes());
    out.push(0);
}

/// Parse, Bind and Execute (no row limit) of `sql` on the unnamed statement
/// and portal; the bytes answered, or the first error.
fn extended(state: &mut State, session: &mut Session, sql: &str) -> R<Vec<u8>> {
    let mut out = Vec::new();
    let now = std::time::Instant::now();
    let mut parse = Vec::new();
    cstr("", &mut parse);
    cstr(sql, &mut parse);
    parse.extend(0u16.to_be_bytes());
    state.handle(b'P', &parse, session, &mut out, now)?;
    let mut bind = Vec::new();
    cstr("", &mut bind);
    cstr("", &mut bind);
    bind.extend([0, 0, 0, 0, 0, 0]);
    state.handle(b'B', &bind, session, &mut out, now)?;
    let mut execute = Vec::new();
    cstr("", &mut execute);
    execute.extend(0u32.to_be_bytes());
    match state.handle(b'E', &execute, session, &mut out, now) {
        Ok(_) => Ok(out),
        Err(code) => {
            // What reached the wire before the error.
            DATA_BEFORE_ERROR.with(|d| *d.borrow_mut() = out);
            Err(code)
        }
    }
}

thread_local! {
    static DATA_BEFORE_ERROR: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// The DataRow messages in `out`, as their text values.
fn data_rows(out: &[u8]) -> Vec<Vec<Option<String>>> {
    let mut rows = Vec::new();
    let mut at = 0;
    while at < out.len() {
        let tag = out[at];
        let length = u32::from_be_bytes(out[at + 1..at + 5].try_into().unwrap()) as usize;
        let body = &out[at + 5..at + 1 + length];
        at += 1 + length;
        if tag != b'D' {
            continue;
        }
        let count = u16::from_be_bytes([body[0], body[1]]) as usize;
        let mut i = 2;
        let mut values = Vec::new();
        for _ in 0..count {
            let n = i32::from_be_bytes(body[i..i + 4].try_into().unwrap());
            i += 4;
            if n < 0 {
                values.push(None);
            } else {
                values.push(Some(String::from_utf8_lossy(&body[i..i + n as usize]).into_owned()));
                i += n as usize;
            }
        }
        rows.push(values);
    }
    rows
}

/// Every chunk the Execute arm fetches is released before it is encoded:
/// 600 rows go out as three pages (256, 256, 88), each with its own debit,
/// the masked column carries its mask on the wire, rows outside the task's
/// scope never appear, and the statement has one read-log record with three
/// pages and its text.
#[test]
fn portal_paging_releases_each_fetch() {
    let (service, mut session) = fixture(10_000);
    let mut state = state();
    let out = extended(&mut state, &mut session, SELECT).unwrap();
    let rows = data_rows(&out);
    assert_eq!(rows.len(), ROWS as usize);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(
            row,
            &[Some((i + 1).to_string()), Some("***".to_owned())],
            "row {i}"
        );
    }
    let task = service.admin.tasks().get("inc-436").unwrap().unwrap().id;
    let disclosure = service.admin.task_disclosure(task).unwrap();
    assert_eq!(disclosure.rows_returned, ROWS as u64);
    assert_eq!(
        disclosure
            .debits
            .iter()
            .map(|d| d.rows_returned)
            .collect::<Vec<_>>(),
        [256, 256, 88],
        "{disclosure:?}"
    );
    let records = service.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0];
    assert_eq!(record.text, SELECT);
    assert_eq!(record.entry, ENTRY_PGWIRE_EXTENDED);
    assert_eq!(record.class, class_code(StatementClass::Select));
    assert_eq!(record.pages, 3);
    assert_eq!(record.released_rows, ROWS as u64);
    assert_eq!(record.masked_values, ROWS as u64);
}

/// A page the release refuses is never encoded: with 300 rows of budget the
/// first page (256 rows) goes out, the second is refused with 54T01 and none
/// of its rows reach the wire.
#[test]
fn page_over_budget_is_never_encoded() {
    let (service, mut session) = fixture(300);
    let mut state = state();
    assert_eq!(extended(&mut state, &mut session, SELECT), Err("54T01"));
    let sent = DATA_BEFORE_ERROR.with(|d| d.borrow().clone());
    assert_eq!(data_rows(&sent).len(), 256);
    let task = service.admin.tasks().get("inc-436").unwrap().unwrap().id;
    let disclosure = service.admin.task_disclosure(task).unwrap();
    assert_eq!(disclosure.rows_returned, 256);
    let records = service.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].released_rows, 256);
    assert_eq!(records[0].sqlstate_text(), "54T01");
}

/// A retained result is released once; before each later slice (`at > 0`)
/// only validity is checked again. The first slice is served with the
/// release; after revocation a later slice is refused (28T01). Outside a
/// task the check is a no-op.
#[test]
fn cached_returning_result_revalidated_per_slice() {
    let (service, mut session) = fixture(10_000);
    let prepared = session.prepare(SELECT).unwrap();
    let unreleased = session.execute_prepared_unreleased(&prepared, &[]).unwrap();
    assert!(unreleased.is_task());
    let released = session.release_page(unreleased).unwrap();
    assert_eq!(released.get().rows.len(), ROWS as usize);
    // The plain exit refuses a task session before the statement runs.
    assert_eq!(
        session.execute_prepared(&prepared, &[]).map(|_| ()),
        Err(CoreError::Task(giggles_core::task::TaskError::NotAllowedInTaskSession(
            StatementClass::Other
        )))
    );
    let mut diagnostic = Diagnostic::default();
    assert_eq!(revalidate(&session, 0, &mut diagnostic), Ok(()));
    assert_eq!(revalidate(&session, 2, &mut diagnostic), Ok(()));
    let issuer = service
        .admin
        .roles()
        .get("incident_controller")
        .unwrap()
        .unwrap();
    service.admin.tasks().revoke(&issuer, "inc-436").unwrap();
    // The first slice goes out with the release itself.
    assert_eq!(revalidate(&session, 0, &mut diagnostic), Ok(()));
    assert_eq!(revalidate(&session, 2, &mut diagnostic), Err("28T01"));
    // Outside a task: a move, and no revalidation.
    let mut plain = service.role_session("incident_controller").unwrap();
    let p = plain.prepare("SELECT id FROM shipments WHERE id = 1").unwrap();
    let unreleased = plain.execute_prepared_unreleased(&p, &[]).unwrap();
    assert!(!unreleased.is_task());
    assert_eq!(plain.release_page(unreleased).unwrap().get().rows.len(), 1);
    assert_eq!(revalidate(&plain, 5, &mut diagnostic), Ok(()));
}

// GD-0061 R3-311: the simple-query path, SQL cursors and COPY TO STDOUT.

/// A simple Query; the bytes answered (through ReadyForQuery), or the first
/// error with what reached the wire before it kept in `DATA_BEFORE_ERROR`.
fn simple(state: &mut State, session: &mut Session, sql: &str) -> R<Vec<u8>> {
    let mut out = Vec::new();
    let mut body = Vec::new();
    cstr(sql, &mut body);
    match state.handle(b'Q', &body, session, &mut out, std::time::Instant::now()) {
        Ok(_) => Ok(out),
        Err(code) => {
            DATA_BEFORE_ERROR.with(|d| *d.borrow_mut() = out);
            Err(code)
        }
    }
}

fn debits(service: &Service) -> Vec<u64> {
    service
        .admin
        .task_disclosure(service.admin.tasks().get("inc-436").unwrap().unwrap().id)
        .unwrap()
        .debits
        .iter()
        .map(|d| d.rows_returned)
        .collect()
}

fn returned(service: &Service) -> u64 {
    service
        .admin
        .task_disclosure(service.admin.tasks().get("inc-436").unwrap().unwrap().id)
        .unwrap()
        .rows_returned
}

/// A task SELECT sent as a simple Query is fetched chunk by chunk, each chunk
/// released before it is encoded: 600 rows in three pages with their own
/// debits, masked on the wire, one read-log record from the simple entry.
/// Over budget, the first page goes out and the second is refused (54T01)
/// with none of its rows on the wire.
#[test]
fn simple_select_released_per_chunk() {
    let (service, mut session) = fixture(10_000);
    let mut state = self::state();
    let out = simple(&mut state, &mut session, SELECT).unwrap();
    let rows = data_rows(&out);
    assert_eq!(rows.len(), ROWS as usize);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row, &[Some((i + 1).to_string()), Some("***".to_owned())], "row {i}");
    }
    assert_eq!(debits(&service), [256, 256, 88]);
    let task = service.admin.tasks().get("inc-436").unwrap().unwrap().id;
    let records = service.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0];
    assert_eq!(record.text, SELECT);
    assert_eq!(record.entry, giggles_core::task::readlog::ENTRY_PGWIRE_SIMPLE);
    assert_eq!(record.class, class_code(StatementClass::Select));
    assert_eq!(record.pages, 3);
    assert_eq!(record.released_rows, ROWS as u64);

    let (service, mut session) = fixture(300);
    let mut state = self::state();
    assert_eq!(simple(&mut state, &mut session, SELECT), Err("54T01"));
    let sent = DATA_BEFORE_ERROR.with(|d| d.borrow().clone());
    assert_eq!(data_rows(&sent).len(), 256);
    // Only the page that went out is charged as returned (rows read by the
    // refused page may still be debited, with no rows returned).
    assert_eq!(returned(&service), 256);
}

/// Runs one SQL cursor command through `Cursors::execute` as the simple path
/// would. On the wire a task session's cursor commands are refused before
/// this point (`task_detect`); calling it directly proves the cursor code
/// itself releases every chunk, so no task row could reach the wire from it.
fn cursor_command(
    state: &mut State,
    session: &mut Session,
    sql: &str,
    out: &mut Vec<u8>,
) -> R<()> {
    let command = cursor::parse(sql)?.expect("a cursor command");
    state.cursors.execute(
        &command,
        session,
        &[],
        &[],
        &mut state.settings,
        true,
        &state.output,
        out,
    )
}

/// Every FETCH chunk is released before it is encoded, and a MOVE releases
/// the rows it skips (they were read, so they are charged). With 300 rows of
/// budget: FETCH 200 (charged 200), MOVE 50 (charged 50, nothing sent), then
/// FETCH 100 is refused with 54T01 and none of its rows reaches the wire. A
/// MOVE that charged nothing would have let it through.
#[test]
fn declare_fetch_released_per_fetch() {
    let declare = format!("DECLARE c CURSOR WITH HOLD FOR {SELECT}");
    let (service, mut session) = fixture(10_000);
    let mut state = self::state();
    let mut out = Vec::new();
    cursor_command(&mut state, &mut session, &declare, &mut out).unwrap();
    assert_eq!(debits(&service), Vec::<u64>::new());
    let mut out = Vec::new();
    cursor_command(&mut state, &mut session, "FETCH 300 c", &mut out).unwrap();
    let rows = data_rows(&out);
    assert_eq!(rows.len(), 300);
    assert!(rows.iter().all(|row| row[1].as_deref() == Some("***")), "masked");
    assert_eq!(debits(&service), [256, 44]);
    let mut out = Vec::new();
    cursor_command(&mut state, &mut session, "MOVE 100 c", &mut out).unwrap();
    assert!(data_rows(&out).is_empty());
    assert_eq!(debits(&service), [256, 44, 100]);
    let mut out = Vec::new();
    cursor_command(&mut state, &mut session, "FETCH ALL c", &mut out).unwrap();
    let rows = data_rows(&out);
    assert_eq!(rows.len(), 200);
    assert_eq!(rows[0][0].as_deref(), Some("401"));
    assert_eq!(debits(&service), [256, 44, 100, 200]);

    let (service, mut session) = fixture(300);
    let mut state = self::state();
    let mut out = Vec::new();
    cursor_command(&mut state, &mut session, &declare, &mut out).unwrap();
    let mut out = Vec::new();
    cursor_command(&mut state, &mut session, "FETCH 200 c", &mut out).unwrap();
    assert_eq!(data_rows(&out).len(), 200);
    let mut out = Vec::new();
    cursor_command(&mut state, &mut session, "MOVE 50 c", &mut out).unwrap();
    assert_eq!(debits(&service), [200, 50]);
    let mut out = Vec::new();
    assert_eq!(
        cursor_command(&mut state, &mut session, "FETCH 100 c", &mut out),
        Err("54T01")
    );
    assert!(data_rows(&out).is_empty(), "a refused page is never encoded");
    assert_eq!(returned(&service), 250);
}

/// COPY TO STDOUT releases the whole result as one page before the
/// CopyOutResponse header: the rows go out masked under one debit, and a
/// result over budget is refused with nothing written at all.
#[test]
fn copy_out_released_before_first_byte() {
    let sql = format!("COPY ({SELECT}) TO STDOUT");
    let options = crate::auth::statements::copy_stdout_select(&sql)
        .unwrap()
        .expect("COPY TO STDOUT");
    let (service, mut session) = fixture(10_000);
    let mut state = self::state();
    let mut out = Vec::new();
    state.copy_stdout(&options, &mut session, &mut out).unwrap();
    assert_eq!(out.first(), Some(&b'H'));
    let text = String::from_utf8_lossy(&out);
    assert!(text.contains("1\t***\n") && text.contains("600\t***\n"), "masked");
    assert!(!text.contains("failed") && !text.contains("secret"), "masked");
    assert_eq!(debits(&service), [ROWS as u64]);

    let (service, mut session) = fixture(300);
    let mut state = self::state();
    let mut out = Vec::new();
    assert_eq!(
        state.copy_stdout(&options, &mut session, &mut out),
        Err("54T01")
    );
    assert!(out.is_empty(), "nothing before the release: {out:?}");
    assert_eq!(returned(&service), 0);
}

/// Outside a task session the converted paths are unchanged: a simple SELECT,
/// a cursor MOVE/FETCH and COPY TO STDOUT send their rows unmasked, and
/// release is a move (no disclosure records).
#[test]
fn non_task_simple_cursor_and_copy_release_is_a_move() {
    let (service, _task) = fixture(10_000);
    let mut session = service.role_session("incident_controller").unwrap();
    let mut state = self::state();
    let out = simple(&mut state, &mut session, SELECT).unwrap();
    let rows = data_rows(&out);
    assert_eq!(rows.len(), ROWS as usize);
    assert_eq!(rows[0][1].as_deref(), Some("failed"));
    let declare = format!("DECLARE c CURSOR WITH HOLD FOR {SELECT}");
    let mut out = Vec::new();
    cursor_command(&mut state, &mut session, &declare, &mut out).unwrap();
    cursor_command(&mut state, &mut session, "MOVE 10 c", &mut out).unwrap();
    let mut out = Vec::new();
    cursor_command(&mut state, &mut session, "FETCH 5 c", &mut out).unwrap();
    assert_eq!(data_rows(&out)[0][0].as_deref(), Some("11"));
    let options =
        crate::auth::statements::copy_stdout_select(&format!("COPY ({SELECT}) TO STDOUT"))
            .unwrap()
            .unwrap();
    let mut out = Vec::new();
    state.copy_stdout(&options, &mut session, &mut out).unwrap();
    assert!(String::from_utf8_lossy(&out).contains("600\tfailed\n"));
    assert_eq!(debits(&service), Vec::<u64>::new());
}
