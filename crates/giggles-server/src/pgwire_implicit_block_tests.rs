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

fn metadata_parse(state: &mut State, session: &mut Session, name: &str) {
    let mut body = Vec::new();
    cstr(name, &mut body);
    cstr("SELECT v FROM t WHERE id = $1", &mut body);
    body.extend(0u16.to_be_bytes());
    state.handle(b'P', &body, session, &mut Vec::new(), std::time::Instant::now()).unwrap();
}
fn metadata_bind(state: &mut State, session: &mut Session, name: &str) {
    let mut body = Vec::new();
    cstr("", &mut body);
    cstr(name, &mut body);
    body.extend(0u16.to_be_bytes());
    body.extend(1u16.to_be_bytes());
    body.extend(1i32.to_be_bytes());
    body.push(b'1');
    body.extend(0u16.to_be_bytes());
    state.handle(b'B', &body, session, &mut Vec::new(), std::time::Instant::now()).unwrap();
}
fn metadata_describe_execute(state: &mut State, session: &mut Session) -> R<Vec<u8>> {
    let mut out = Vec::new();
    let now = std::time::Instant::now();
    state.handle(b'D', &[b'P', 0], session, &mut out, now)?;
    state.handle(b'E', &[0, 0, 0, 0, 0], session, &mut out, now)?;
    state.handle(b'S', &[], session, &mut out, now)?;
    Ok(out)
}
#[test]
fn metadata_cycle_counts() {
    let mut session = fixture();
    session.execute_sql("CREATE TABLE t (id int PRIMARY KEY, v int)", &[]).unwrap();
    session.execute_sql("INSERT INTO t VALUES (1, 7)", &[]).unwrap();
    session.execute_sql("CREATE TABLE serial_t (id serial PRIMARY KEY, v int)", &[]).unwrap();
    let mut state = state();
    for name in ["", "named"] {
        giggles_core::metadata_read_counts::take();
        metadata_parse(&mut state, &mut session, name);
        metadata_bind(&mut state, &mut session, name);
        assert_eq!(text_rows(&metadata_describe_execute(&mut state, &mut session).unwrap()), vec![vec!["7"]]);
        assert_eq!(giggles_core::metadata_read_counts::take(), if name.is_empty() { [7, 0, 9, 16, 0] } else { [7, 0, 12, 19, 0] }, "cycle {name:?}");
        if name.is_empty() {
            metadata_parse(&mut state, &mut session, name);
            metadata_bind(&mut state, &mut session, name);
            metadata_describe_execute(&mut state, &mut session).unwrap();
            assert_eq!(giggles_core::metadata_read_counts::take(), [7, 0, 8, 15, 0]);
        }
        if !name.is_empty() {
            metadata_bind(&mut state, &mut session, name);
            metadata_describe_execute(&mut state, &mut session).unwrap();
            assert_eq!(giggles_core::metadata_read_counts::take(), [7, 0, 7, 14, 0]);
        }
    }
    simple(&mut state, &mut session, "SELECT v FROM t WHERE id = 1").unwrap();
    assert_eq!(giggles_core::metadata_read_counts::take(), [0, 0, 9, 9, 0]);
    extended(&mut state, &mut session, "INSERT INTO serial_t (v) VALUES (8)").unwrap();
    assert_eq!(giggles_core::metadata_read_counts::take(), [7, 0, 9, 16, 0]);
}
#[test]
fn metadata_ddl_between_bind_and_execute() {
    for name in ["", "named"] {
        for ddl in ["ALTER TABLE t ADD COLUMN w int", "DROP TABLE t; CREATE TABLE t (id int PRIMARY KEY, v text)"] {
            let mut session = fixture();
            session.execute_sql("CREATE TABLE t (id int PRIMARY KEY, v int)", &[]).unwrap();
            session.execute_sql("INSERT INTO t VALUES (1, 7)", &[]).unwrap();
            let mut state = state();
            metadata_parse(&mut state, &mut session, name);
            metadata_bind(&mut state, &mut session, name);
            let mut other = session.service.role_session("owner").unwrap();
            let mut other_state = self::state();
            for statement in ddl.split("; ") {
                simple(&mut other_state, &mut other, statement).unwrap();
            }
            // Pinned on 26fe7e55 before the fast path: a bound portal describes
            // the new table and executes successfully, even after replacement.
            let out = metadata_describe_execute(&mut state, &mut session).unwrap();
            assert_eq!(out[0], b'T');
            let mut description = Reader { b: &out[5..], at: 0 };
            assert_eq!(description.u16().unwrap(), 1);
            assert_eq!(description.text().unwrap(), "v");
            description.u32().unwrap(); // table oid
            description.u16().unwrap(); // attribute number
            let oid = description.u32().unwrap();
            if ddl.starts_with("ALTER") {
                assert_eq!(text_rows(&out), vec![vec!["7"]]);
                assert_eq!(oid, 23);
            } else {
                assert!(text_rows(&out).is_empty());
                assert_eq!(oid, 25);
            }
        }
    }
}

fn parse_sql(state: &mut State, session: &mut Session, sql: &str) -> R<()> {
    let mut body = vec![0];
    cstr(sql, &mut body);
    body.extend(0u16.to_be_bytes());
    state.handle(b'P', &body, session, &mut vec![], std::time::Instant::now()).map(|_| ())
}

#[test]
fn unnamed_cache_hit_and_transaction_exclusion() {
    let mut session = fixture();
    session.execute_sql("CREATE TABLE t (id int PRIMARY KEY, v int)", &[]).unwrap();
    let mut state = state();
    giggles_core::metadata_read_counts::take();
    metadata_parse(&mut state, &mut session, "");
    let cold = giggles_core::metadata_read_counts::take();
    metadata_parse(&mut state, &mut session, "");
    let warm = giggles_core::metadata_read_counts::take();
    assert_eq!(cold[2], warm[2] + 1, "only core parse/bind is skipped");
    simple(&mut state, &mut session, "BEGIN").unwrap();
    metadata_parse(&mut state, &mut session, "");
    giggles_core::metadata_read_counts::take();
    metadata_parse(&mut state, &mut session, "");
    let transaction = giggles_core::metadata_read_counts::take();
    assert!(transaction[0] > 0);
    simple(&mut state, &mut session, "ROLLBACK").unwrap();
}

#[test]
fn unnamed_cache_catalog_and_search_path_invalidation() {
    let mut session = fixture();
    session.execute_sql("CREATE TABLE t (id int PRIMARY KEY, v int)", &[]).unwrap();
    session.execute_sql("INSERT INTO t VALUES (1, 7)", &[]).unwrap();
    let mut state = state();
    assert_eq!(text_rows(&extended(&mut state, &mut session, "SELECT * FROM t").unwrap()), vec![vec!["1", "7"]]);
    extended(&mut state, &mut session, "SELECT * FROM t").unwrap();
    let mut other = session.service.role_session("owner").unwrap();
    other.execute_sql("ALTER TABLE t ADD COLUMN w int", &[]).unwrap();
    assert_eq!(data_rows(&extended(&mut state, &mut session, "SELECT * FROM t").unwrap()), vec![vec![Some("1".into()), Some("7".into()), None]]);
    other.execute_sql("CREATE SCHEMA alternate", &[]).unwrap();
    other.execute_sql("CREATE TABLE alternate.t (v int)", &[]).unwrap();
    other.execute_sql("INSERT INTO alternate.t VALUES (42)", &[]).unwrap();
    extended(&mut state, &mut session, "SELECT * FROM t").unwrap();
    simple(&mut state, &mut session, "SET search_path = alternate").unwrap();
    assert_eq!(text_rows(&extended(&mut state, &mut session, "SELECT * FROM t").unwrap()), vec![vec!["42"]]);
}

#[test]
fn unnamed_cache_rechecks_revoked_select() {
    let mut owner = fixture();
    let mut owner_state = state();
    simple(&mut owner_state, &mut owner, "CREATE TABLE t (id int PRIMARY KEY, v int)").unwrap();
    simple(&mut owner_state, &mut owner, "CREATE ROLE reader LOGIN").unwrap();
    simple(&mut owner_state, &mut owner, "GRANT SELECT ON t TO reader").unwrap();
    let mut reader = owner.service.role_session("reader").unwrap();
    let mut reader_state = state();
    extended(&mut reader_state, &mut reader, "SELECT v FROM t").unwrap();
    extended(&mut reader_state, &mut reader, "SELECT v FROM t").unwrap();
    simple(&mut owner_state, &mut owner, "REVOKE SELECT ON t FROM reader").unwrap();
    assert_eq!(parse_sql(&mut reader_state, &mut reader, "SELECT v FROM t"), Err("42501"));
}

#[test]
fn unnamed_cache_lru_capacity() {
    let mut session = fixture();
    session.execute_sql("CREATE TABLE t (id int PRIMARY KEY)", &[]).unwrap();
    let mut state = state();
    let sql = "SELECT id FROM t WHERE id = 0";
    parse_sql(&mut state, &mut session, sql).unwrap();
    giggles_core::metadata_read_counts::take();
    parse_sql(&mut state, &mut session, sql).unwrap();
    let hit = giggles_core::metadata_read_counts::take();
    for id in 1..=64 {
        parse_sql(&mut state, &mut session, &format!("SELECT id FROM t WHERE id = {id}")).unwrap();
    }
    giggles_core::metadata_read_counts::take();
    parse_sql(&mut state, &mut session, sql).unwrap();
    let miss = giggles_core::metadata_read_counts::take();
    assert_eq!(miss[2], hit[2] + 1);
}
