//! Direct control paths driven through pgwire messages.
use super::*;
use giggles_core::{Admin, Engine, Limits, roles::NewRole};

static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn fixture() -> Session {
    let path = crate::test_support::fixture_root().join(format!("implicit-block-{}-{}-{}",
        SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    admin.roles().create(NewRole { name: "owner".into(), login: true, superuser: true, ..Default::default() }).unwrap();
    let service = crate::Service::new(admin, vec![], crate::clock::Clock::new(0, 3600).unwrap()).unwrap();
    service.role_session("owner").unwrap()
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
        copy_permit: None,
        execution_admission: None,
    }
}

fn cstr(text: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(text.as_bytes());
    out.push(0);
}

/// A simple Query; the bytes answered, or the error.
fn simple(state: &mut State, session: &mut Session, sql: &str) -> R<Vec<u8>> {
    let mut out = Vec::new();
    let mut body = Vec::new();
    cstr(sql, &mut body);
    state.handle(b'Q', &body, session, &mut out, std::time::Instant::now())?;
    Ok(out)
}

/// Parse, Bind, Describe portal, Execute (no row limit) and Sync on the
/// unnamed statement and portal; the bytes answered, or the first error.
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
    let mut describe = vec![b'P'];
    cstr("", &mut describe);
    state.handle(b'D', &describe, session, &mut out, now)?;
    let mut execute = Vec::new();
    cstr("", &mut execute);
    execute.extend(0u32.to_be_bytes());
    state.handle(b'E', &execute, session, &mut out, now)?;
    state.handle(b'S', &[], session, &mut out, now)?;
    Ok(out)
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

fn text_rows(out: &[u8]) -> Vec<Vec<String>> {
    data_rows(out)
        .into_iter()
        .map(|row| row.into_iter().map(Option::unwrap_or_default).collect())
        .collect()
}

/// No hidden name anywhere in the answer (every message, every byte).

#[test]
fn autocommit_controls_skip_run_control_and_ddl_falls_back() {
    let mut session = fixture();
    session.execute_sql("CREATE TABLE t (id INT PRIMARY KEY)", &[]).unwrap();
    let mut state = state();
    for extended_protocol in [false, true] {
        let query = if extended_protocol { extended } else { simple };
        session.control_calls = 0;
        session.fast_control_calls = 0;
        query(&mut state, &mut session, if extended_protocol { "INSERT INTO t VALUES (2)" } else { "INSERT INTO t VALUES (1); SELECT id FROM t" }).unwrap();
        let out = query(&mut state, &mut session, "SELECT id FROM t ORDER BY id").unwrap();
        assert!(!text_rows(&out).is_empty());
        assert!(!session.in_transaction());
        assert_eq!(session.control_calls, 0);
        assert!(session.fast_control_calls >= 2, "implicit BEGIN and COMMIT used the direct path");
        query(&mut state, &mut session, if extended_protocol { "CREATE TABLE ext (id INT)" } else { "SELECT id FROM t; CREATE TABLE sim (id INT)" }).unwrap();
        assert_eq!(session.control_calls, 1, "DDL COMMIT uses the SQL path");
    }
    session.control_calls = 0;
    simple(&mut state, &mut session, "BEGIN; COMMIT").unwrap();
    assert_eq!(session.control_calls, 2, "user controls retain SQL execution");
}

#[test]
fn timezone_set_local_and_rollback_refresh_memo() {
    let mut session = fixture();
    let mut state = state();
    let sql = "SELECT TIMESTAMPTZ '2024-01-01 00:00:00+00'::text";
    simple(&mut state, &mut session, "SET TimeZone = 'UTC'").unwrap();
    let utc = text_rows(&simple(&mut state, &mut session, sql).unwrap());
    simple(&mut state, &mut session, "SET TimeZone = 'Asia/Kolkata'").unwrap();
    let india = text_rows(&extended(&mut state, &mut session, sql).unwrap());
    assert_ne!(utc, india);
    assert!(india[0][0].ends_with("+05:30"));
    simple(&mut state, &mut session, "BEGIN").unwrap();
    simple(&mut state, &mut session, "SET LOCAL TimeZone = 'UTC'").unwrap();
    assert_eq!(text_rows(&simple(&mut state, &mut session, sql).unwrap()), utc);
    simple(&mut state, &mut session, "ROLLBACK").unwrap();
    assert_eq!(text_rows(&simple(&mut state, &mut session, sql).unwrap()), india);
}
