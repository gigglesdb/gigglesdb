//! Direct control paths driven through pgwire messages.
use super::*;
use giggles_core::{Admin, Engine, Limits, roles::NewRole};

static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn fixture() -> Session {
    fixture_with_clock(crate::clock::Clock::new(0, 3600).unwrap())
}
fn fixture_with_clock(clock: crate::clock::Clock) -> Session {
    fixture_with_engine(clock, Engine::V2)
}
fn fixture_with_engine(clock: crate::clock::Clock, engine: Engine) -> Session {
    let path = crate::test_support::fixture_root().join(format!("implicit-block-{}-{}-{}",
        SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin.roles().create(NewRole { name: "owner".into(), login: true, superuser: true, ..Default::default() }).unwrap();
    let service = crate::Service::new(admin, vec![], clock).unwrap();
    service.role_session("owner").unwrap()
}
fn state() -> State {
    State {
        output: None,
        diagnostic: Diagnostic::default(),
        admin_completed: false,
        implicit: false,
        retain_durability_permits: false,
        settings: crate::pg_settings::SessionSettings::default(),
        statements: BTreeMap::new(),
        unnamed_cache: Vec::new(),
        next_statement_id: 0,
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
    extended_execute(state, session, sql, &mut out)?;
    state.handle(b'S', &[], session, &mut out, std::time::Instant::now())?;
    Ok(out)
}

fn extended_execute(
    state: &mut State,
    session: &mut Session,
    sql: &str,
    out: &mut Vec<u8>,
) -> R<()> {
    let now = std::time::Instant::now();
    metadata_parse_sql(state, session, "", sql, out, now)?;
    bind_execute(state, session, "", out, now)
}

fn extended_bind_execute(
    state: &mut State, session: &mut Session, name: &str, out: &mut Vec<u8>, now: std::time::Instant,
) -> R<()> {
    bind_execute(state, session, name, out, now)?;
    state.handle(b'S', &[], session, out, now)?;
    Ok(())
}

fn bind_execute(
    state: &mut State, session: &mut Session, name: &str, out: &mut Vec<u8>, now: std::time::Instant,
) -> R<()> {
    let mut bind = Vec::new();
    cstr("", &mut bind);
    cstr(name, &mut bind);
    bind.extend([0, 0, 0, 0, 0, 0]);
    state.handle(b'B', &bind, session, out, now)?;
    let mut describe = vec![b'P'];
    cstr("", &mut describe);
    state.handle(b'D', &describe, session, out, now)?;
    let mut execute = Vec::new();
    cstr("", &mut execute);
    execute.extend(0u32.to_be_bytes());
    state.handle(b'E', &execute, session, out, now)?;
    Ok(())
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
    metadata_parse_sql(state, session, name, "SELECT v FROM t WHERE id = $1",
        &mut Vec::new(), std::time::Instant::now()).unwrap();
}
fn metadata_parse_sql(
    state: &mut State, session: &mut Session, name: &str, sql: &str,
    out: &mut Vec<u8>, now: std::time::Instant,
) -> R<()> {
    let mut body = Vec::new();
    cstr(name, &mut body);
    cstr(sql, &mut body);
    body.extend(0u16.to_be_bytes());
    state.handle(b'P', &body, session, out, now).map(drop)
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
        // Bind adds dependency locking and current-grant authorization. Describe
        // adds four metadata reads to recheck live grants even with a cached shape.
        // Named/unnamed descriptions and plans remain cached.
        assert_eq!(giggles_core::metadata_read_counts::take(), [17, 0, 8, usize::from(name.is_empty()), 0], "cycle {name:?}");
        if name.is_empty() {
            metadata_parse(&mut state, &mut session, name);
            metadata_bind(&mut state, &mut session, name);
            metadata_describe_execute(&mut state, &mut session).unwrap();
            assert_eq!(giggles_core::metadata_read_counts::take(), [17, 0, 6, 0, 0]);
        }
        if !name.is_empty() {
            metadata_bind(&mut state, &mut session, name);
            metadata_describe_execute(&mut state, &mut session).unwrap();
            // Rebinding the cached named description also saves three reads.
            assert_eq!(giggles_core::metadata_read_counts::take(), [17, 0, 3, 0, 0]);
        }
    }
    simple(&mut state, &mut session, "SELECT v FROM t WHERE id = 1").unwrap();
    assert_eq!(giggles_core::metadata_read_counts::take(), [0, 0, 9, 0, 0]);
    extended(&mut state, &mut session, "INSERT INTO serial_t (v) VALUES (8)").unwrap();
    // The serial default's execution-time sequence privilege check (GD-0084 Q24)
    // reads the latest committed catalog once per statement.
    assert_eq!(giggles_core::metadata_read_counts::take(), [17, 0, 9, 0, 0]);

    session.set_postgres_surface();
    let now = std::time::Instant::now();
    let name = "serial_insert";
    let mut out = Vec::new();
    metadata_parse_sql(&mut state, &mut session, name,
        "INSERT INTO serial_t (v) VALUES (9), (10), (11)", &mut out, now).unwrap();
    extended_bind_execute(&mut state, &mut session, name, &mut out, now).unwrap();
    giggles_core::metadata_read_counts::take();
    extended_bind_execute(&mut state, &mut session, name, &mut out, now).unwrap();
    let warm = giggles_core::metadata_read_counts::take();
    session.clear_sequence_default_grants();
    extended_bind_execute(&mut state, &mut session, name, &mut out, now).unwrap();
    let guard_reads = giggles_core::metadata_read_counts::take();
    // Without grants the Guard reads one latest catalog image for all three
    // rows. Replaying the grant skips that read on an otherwise identical cycle.
    assert_eq!(warm[2] + 1, guard_reads[2], "without grants {guard_reads:?}, warm {warm:?}");
}

fn named_sequence_default() -> (Session, Session, State) {
    let mut owner = fixture();
    owner.set_postgres_surface();
    let roles = owner.service.admin.roles();
    for name in ["member", "other"] {
        roles.create(NewRole { name: name.into(), login: true, ..Default::default() }).unwrap();
    }
    let mut owner_state = state();
    for sql in [
        "CREATE EXTENSION \"uuid-ossp\"",
        "CREATE SEQUENCE memo_s",
        "CREATE TABLE memo_t (id UUID PRIMARY KEY DEFAULT \
         uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', nextval('memo_s')::text))",
        "GRANT INSERT, SELECT ON memo_t TO member",
        "GRANT owner TO member",
    ] {
        simple(&mut owner_state, &mut owner, sql).unwrap();
    }
    let mut member = owner.service.role_session("member").unwrap();
    member.set_postgres_surface();
    (owner, member, state())
}

fn prepare_sequence_default(state: &mut State, member: &mut Session) {
    metadata_parse_sql(state, member, "insert_default", "INSERT INTO memo_t DEFAULT VALUES RETURNING id",
        &mut Vec::new(), std::time::Instant::now()).unwrap();
}

fn execute_sequence_default(state: &mut State, member: &mut Session) -> R<()> {
    extended_bind_execute(state, member, "insert_default", &mut Vec::new(), std::time::Instant::now())
}

#[test]
fn named_default_membership_revoke_reaches_the_execution_guard() {
    use giggles_core::SequenceDefaultGrants;
    let (mut owner, mut member, mut member_state) = named_sequence_default();
    prepare_sequence_default(&mut member_state, &mut member);
    let hits = SequenceDefaultGrants::replay_hits();
    execute_sequence_default(&mut member_state, &mut member).unwrap();
    assert!(member.has_sequence_default_grants(), "the first execution must record the grant");
    simple(&mut state(), &mut owner, "REVOKE owner FROM member").unwrap();

    // The direct table grants survive, and the static dependency walk does
    // not see the sequence nested in the UUID default. Reaching this hook
    // proves Bind, Describe and full statement authorization still passed.
    let observed = std::rc::Rc::new(std::cell::Cell::new(None));
    let capture = observed.clone();
    Session::observe_sequence_default_execution(move |has_grant| capture.set(Some(has_grant)));
    assert_eq!(execute_sequence_default(&mut member_state, &mut member), Err("42501"));
    assert_eq!(observed.get(), Some(false), "execution reached without the invalidated grant");
    assert_eq!(SequenceDefaultGrants::replay_hits(), hits);
    assert_eq!(text_rows(&simple(&mut state(), &mut owner, "SELECT nextval('memo_s')").unwrap()), [["2"]]);
}

#[test]
fn named_default_replays_on_second_execution_in_repeatable_read() {
    use giggles_core::SequenceDefaultGrants;
    let (_, mut member, mut state) = named_sequence_default();
    simple(&mut state, &mut member, "BEGIN ISOLATION LEVEL REPEATABLE READ").unwrap();
    prepare_sequence_default(&mut state, &mut member);
    let hits = SequenceDefaultGrants::replay_hits();
    execute_sequence_default(&mut state, &mut member).unwrap();
    assert_eq!(SequenceDefaultGrants::replay_hits(), hits);
    execute_sequence_default(&mut state, &mut member).unwrap();
    assert_eq!(SequenceDefaultGrants::replay_hits(), hits + 1);
    simple(&mut state, &mut member, "ROLLBACK").unwrap();
}

#[test]
fn named_default_rechecks_owner_changed_after_authorization() {
    use giggles_core::SequenceDefaultGrants;
    let (mut owner, mut member, mut member_state) = named_sequence_default();
    // Keep the statement catalog version constant while the live owner
    // record changes between S2 admission and the execution Guard's draw.
    simple(&mut member_state, &mut member, "BEGIN ISOLATION LEVEL REPEATABLE READ").unwrap();
    prepare_sequence_default(&mut member_state, &mut member);
    execute_sequence_default(&mut member_state, &mut member).unwrap();
    let hits = SequenceDefaultGrants::replay_hits();
    execute_sequence_default(&mut member_state, &mut member).unwrap();
    assert_eq!(SequenceDefaultGrants::replay_hits(), hits + 1);

    let roles = owner.service.admin.roles();
    Session::observe_sequence_default_execution(move |has_grant| {
        assert!(has_grant, "the owner must change after S2 handed off the old grant");
        assert_eq!(roles.table_owner("memo_s").unwrap().as_deref(), Some("owner"));
        roles.set_table_owner("memo_s", "other").unwrap();
    });
    assert_eq!(execute_sequence_default(&mut member_state, &mut member), Err("42501"));
    assert_eq!(SequenceDefaultGrants::replay_hits(), hits + 1);
    simple(&mut member_state, &mut member, "ROLLBACK").unwrap();
    assert_eq!(text_rows(&simple(&mut state(), &mut owner, "SELECT nextval('memo_s')").unwrap()), [["3"]]);
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
            // Probe the same AccessExclusive conflict as DDL without blocking
            // this synchronous test thread. The network tests exercise DDL itself.
            simple(&mut other_state, &mut other, "BEGIN").unwrap();
            assert_eq!(simple(&mut other_state, &mut other,
                "LOCK TABLE t IN ACCESS EXCLUSIVE MODE NOWAIT"), Err("55P03"));
            simple(&mut other_state, &mut other, "ROLLBACK").unwrap();
            let out = metadata_describe_execute(&mut state, &mut session).unwrap();
            assert_eq!(out[0], b'T');
            assert_eq!(text_rows(&out), vec![vec!["7"]]);
            let mut description = Reader { b: &out[5..], at: 0 };
            assert_eq!(description.u16().unwrap(), 1);
            assert_eq!(description.text().unwrap(), "v");
            description.u32().unwrap();
            description.u16().unwrap();
            assert_eq!(description.u32().unwrap(), 23);
            for statement in ddl.split("; ") {
                simple(&mut other_state, &mut other, statement).unwrap();
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
    assert_eq!(cold[2], warm[2] + 2, "core parse/bind and the server description are skipped");
    simple(&mut state, &mut session, "BEGIN").unwrap();
    metadata_parse(&mut state, &mut session, "");
    giggles_core::metadata_read_counts::take();
    metadata_parse(&mut state, &mut session, "");
    let transaction = giggles_core::metadata_read_counts::take();
    assert!(transaction[0] > 0);
    simple(&mut state, &mut session, "ROLLBACK").unwrap();
}

#[test]
fn unnamed_parse_replays_authorization_on_postgres_surface() {
    let mut session = fixture();
    assert_eq!(session.service.admin.engine(), Engine::V2);
    session.execute_sql("CREATE TABLE t (id INT PRIMARY KEY)", &[]).unwrap();
    session.execute_sql("INSERT INTO t VALUES (1)", &[]).unwrap();
    session.set_postgres_surface();
    let mut state = state();
    let sql = "SELECT id FROM t";
    assert_eq!(text_rows(&extended(&mut state, &mut session, sql).unwrap()), [["1"]]);

    let hits = session.authorization_hits;
    let runs = session.authorization_full_runs;
    parse_sql(&mut state, &mut session, sql).unwrap();
    assert_eq!(session.authorization_hits, hits + 1);
    assert_eq!(session.authorization_full_runs, runs);
}

#[test]
fn named_and_unnamed_authorization_hit_in_explicit_transactions() {
    use giggles_core::roles::TablePrivilege::Select;

    let mut owner = fixture();
    owner.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", &[]).unwrap();
    owner.execute_sql("INSERT INTO t VALUES (1, 7)", &[]).unwrap();
    let roles = owner.service.admin.roles();
    roles.create(NewRole { name: "reader".into(), login: true, ..Default::default() }).unwrap();
    roles.grant_table("t", "reader", &[Select]).unwrap();

    for begin in ["BEGIN ISOLATION LEVEL READ COMMITTED", "BEGIN ISOLATION LEVEL REPEATABLE READ"] {
        for name in ["named", ""] {
            let mut reader = owner.service.role_session("reader").unwrap();
            reader.set_postgres_surface();
            let mut state = state();
            simple(&mut state, &mut reader, begin).unwrap();
            simple(&mut state, &mut reader, "SELECT 1").unwrap();
            metadata_parse(&mut state, &mut reader, name);
            metadata_bind(&mut state, &mut reader, name);
            assert_eq!(text_rows(&metadata_describe_execute(&mut state, &mut reader).unwrap()), [["7"]]);

            let hits = reader.authorization_hits;
            if name.is_empty() {
                metadata_parse(&mut state, &mut reader, name);
            }
            metadata_bind(&mut state, &mut reader, name);
            assert_eq!(text_rows(&metadata_describe_execute(&mut state, &mut reader).unwrap()), [["7"]]);
            assert!(reader.authorization_hits > hits, "{begin}, statement {name:?}");

            // A committed REVOKE by another session reaches the next Bind of the
            // same warmed statement (no re-Parse), even under REPEATABLE READ.
            roles.revoke_table("t", "reader", &[Select]).unwrap();
            let (hits, runs) = (reader.authorization_hits, reader.authorization_full_runs);
            let mut body = Vec::new();
            cstr("", &mut body);
            cstr(name, &mut body);
            body.extend(0u16.to_be_bytes());
            body.extend(1u16.to_be_bytes());
            body.extend(1i32.to_be_bytes());
            body.push(b'1');
            body.extend(0u16.to_be_bytes());
            assert_eq!(
                state.handle(b'B', &body, &mut reader, &mut Vec::new(), std::time::Instant::now()),
                Err("42501"),
                "{begin}, statement {name:?}"
            );
            assert_eq!(reader.authorization_hits, hits, "{begin}, statement {name:?}");
            assert!(reader.authorization_full_runs > runs, "{begin}, statement {name:?}");
            simple(&mut state, &mut reader, "ROLLBACK").unwrap();
            roles.grant_table("t", "reader", &[Select]).unwrap();
        }
    }
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
    assert_eq!(miss[2], hit[2] + 2);
}

#[test]
fn unnamed_cache_sql_byte_bound_and_discard() {
    let mut session = fixture();
    session.execute_sql("CREATE TABLE t (id int PRIMARY KEY)", &[]).unwrap();
    let mut state = state();
    for id in 0..3 {
        let sql = format!("SELECT id FROM t WHERE id = {id}{}", " ".repeat(40_000));
        parse_sql(&mut state, &mut session, &sql).unwrap();
        assert_eq!(state.unnamed_cache.len(), 1);
        assert!(state.unnamed_cache.iter().map(|entry| entry.sql_bytes).sum::<usize>() <= 64 * 1024);
    }
    let oversized = format!("SELECT id FROM t{}", " ".repeat(64 * 1024));
    // The v2 SQL budget now admits this statement; the 64 KiB cache
    // admission bound still excludes it without evicting the prior entry.
    parse_sql(&mut state, &mut session, &oversized).unwrap();
    assert_eq!(state.unnamed_cache.len(), 1);
    assert!(state.unnamed_cache.iter().all(|entry| entry.sql != oversized));
    for command in ["DISCARD ALL", "DISCARD PLANS", "RESET ROLE", "SET ROLE owner"] {
        parse_sql(&mut state, &mut session, "SELECT id FROM t").unwrap();
        assert!(!state.unnamed_cache.is_empty());
        // Unsupported DISCARD still invalidates conservatively.
        let _ = parse_sql(&mut state, &mut session, command);
        assert!(state.unnamed_cache.is_empty(), "{command}");
    }
}

#[test]
fn extended_implicit_block_survives_subsecond_backward_step() {
    use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let clock = crate::clock::Clock::new(0, 3600).unwrap();
    let active = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let enabled = active.clone();
    let sampled = calls.clone();
    let mut anchor = None;
    clock.set_sql_wall(Box::new(move || {
        let wall = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        if !enabled.load(Ordering::Relaxed) {
            return wall;
        }
        let t = *anchor.get_or_insert(Duration::from_secs(wall.as_secs()) + Duration::from_millis(500));
        let call = sampled.fetch_add(1, Ordering::Relaxed);
        if call == 0 { t } else { t - Duration::from_micros(1400) }
    }));
    let mut session = fixture_with_clock(clock);
    session.execute_sql("CREATE TABLE t (id INT PRIMARY KEY)", &[]).unwrap();
    let mut state = state();
    active.store(true, Ordering::Relaxed);
    let out = extended(&mut state, &mut session, "INSERT INTO t VALUES (1)").unwrap();
    assert!(calls.load(Ordering::Relaxed) >= 3, "BEGIN, Execute and Sync must sample the clock");
    assert!(!session.in_transaction());
    let mut at = 0;
    while at < out.len() {
        assert_ne!(out[at], b'E', "unexpected ErrorResponse");
        at += 1 + u32::from_be_bytes(out[at + 1..at + 5].try_into().unwrap()) as usize;
    }
    let out = extended(&mut state, &mut session, "SELECT id FROM t").unwrap();
    assert_eq!(text_rows(&out), vec![vec!["1"]]);
    assert!(!session.in_transaction());
}


fn named_reuse_case(begin_before_parse: bool, begin_after_parse: bool) {
    named_reuse_engine(Engine::V2, begin_before_parse, begin_after_parse);
}
fn named_reuse_engine(engine: Engine, begin_before_parse: bool, begin_after_parse: bool) {
    let mut session = fixture_with_engine(crate::clock::Clock::new(0, 3600).unwrap(), engine);
    session.execute_sql("CREATE TABLE reuse_t(a int)", &[]).unwrap();
    session.execute_sql("INSERT INTO reuse_t VALUES(7)", &[]).unwrap();
    let mut state = state();
    if begin_before_parse { simple(&mut state, &mut session, "BEGIN").unwrap(); }
    let mut parse = Vec::new();
    cstr("cached", &mut parse);
    cstr("SELECT * FROM reuse_t", &mut parse);
    parse.extend(0u16.to_be_bytes());
    state.handle(b'P', &parse, &mut session, &mut Vec::new(), std::time::Instant::now()).unwrap();
    let prepared = PREPARE_CALLS.with(std::cell::Cell::get);
    assert_eq!(state.statements["cached"].description_version.is_some(), engine == Engine::V2);
    if begin_after_parse { simple(&mut state, &mut session, "BEGIN").unwrap(); }
    for _ in 0..4 {
        let mut bind = Vec::new();
        cstr("", &mut bind);
        cstr("cached", &mut bind);
        bind.extend([0; 6]);
        let mut out = Vec::new();
        let now = std::time::Instant::now();
        state.handle(b'B', &bind, &mut session, &mut out, now).unwrap();
        state.handle(b'D', b"P\0", &mut session, &mut out, now).unwrap();
        state.handle(b'E', &[0; 5], &mut session, &mut out, now).unwrap();
        state.handle(b'S', &[], &mut session, &mut out, now).unwrap();
        assert_eq!(text_rows(&out), vec![vec!["7".to_owned()]]);
        assert_eq!(PREPARE_CALLS.with(std::cell::Cell::get), prepared, "unchanged Bind re-prepared");
    }
    if begin_before_parse || begin_after_parse { simple(&mut state, &mut session, "ROLLBACK").unwrap(); }
}
#[test]
fn unchanged_named_statement_reused_outside_transaction() {
    named_reuse_case(false, false);
}
#[test]
fn unchanged_named_statement_reused_after_begin() {
    named_reuse_case(false, true);
}
#[test]
fn unchanged_named_statement_prepared_inside_transaction_reused() {
    named_reuse_case(true, false);
}

#[test]
fn execute_rechecks_authority_after_bind() {
    use giggles_core::roles::TablePrivilege::Select;
    let mut owner = fixture();
    owner.execute_sql("CREATE TABLE t(id int PRIMARY KEY, v int)", &[]).unwrap();
    let roles = owner.service.admin.roles();
    roles.create(NewRole { name: "reader".into(), login: true, ..Default::default() }).unwrap();
    roles.grant_table("t", "reader", &[Select]).unwrap();
    let mut reader = owner.service.role_session("reader").unwrap();
    let mut state = state();
    metadata_parse(&mut state, &mut reader, "named");
    metadata_bind(&mut state, &mut reader, "named");
    roles.revoke_table("t", "reader", &[Select]).unwrap();
    assert_eq!(state.handle(b'E', &[0; 5], &mut reader, &mut Vec::new(),
        std::time::Instant::now()), Err("42501"));
    reader.sql.rollback();
}

#[test]
fn unknown_backend_legacy_named_statement_reused() {
    named_reuse_engine(Engine::Legacy, false, false);
    named_reuse_engine(Engine::Legacy, false, true);
    named_reuse_engine(Engine::Legacy, true, false);
}
#[test]
fn measure_bind_authorization_cost() {
    use giggles_core::roles::TablePrivilege::Select;
    let mut owner = fixture();
    owner.execute_sql("CREATE TABLE timing_t(a int)", &[]).unwrap();
    owner.execute_sql("INSERT INTO timing_t VALUES(7)", &[]).unwrap();
    let roles = owner.service.admin.roles();
    roles.create(NewRole { name: "timing_reader".into(), login: true, ..Default::default() }).unwrap();
    roles.grant_table("timing_t", "timing_reader", &[Select]).unwrap();
    let mut reader = owner.service.role_session("timing_reader").unwrap();
    let mut state = state();
    let mut parse = b"timed\0".to_vec();
    cstr("SELECT * FROM timing_t", &mut parse);
    parse.extend([0;2]);
    state.handle(b'P', &parse, &mut reader, &mut Vec::new(), std::time::Instant::now()).unwrap();
    let Plan::Sql(prepared) = &state.statements["timed"].p else { panic!("SQL plan") };
    let prepared = prepared.clone();
    reader.begin_implicit_block().unwrap();
    reader.sql.begin_prepared_statement();
    for _ in 0..100 {
        reader.authority_scope(|s| s.authorize_prepared_bind(&prepared)).unwrap();
    }
    let prepares = PREPARE_CALLS.with(std::cell::Cell::get);
    let count = 1000;
    let started = std::time::Instant::now();
    for _ in 0..count {
        std::hint::black_box(reader.authority_scope(|s| s.authorize_prepared_bind(&prepared)).unwrap());
    }
    let authorization_us = started.elapsed().as_secs_f64() * 1e6 / f64::from(count);
    reader.sql.rollback();
    let bind = b"\0timed\0\0\0\0\0\0\0";
    let started = std::time::Instant::now();
    for _ in 0..count {
        let mut out = Vec::new();
        let now = std::time::Instant::now();
        state.handle(b'B', bind, &mut reader, &mut out, now).unwrap();
        state.handle(b'E', &[0;5], &mut reader, &mut out, now).unwrap();
        state.handle(b'S', &[], &mut reader, &mut out, now).unwrap();
    }
    let cycle_us = started.elapsed().as_secs_f64() * 1e6 / f64::from(count);
    assert_eq!(PREPARE_CALLS.with(std::cell::Cell::get), prepares);
    eprintln!("BIND_AUTH_COST samples={count} authorization_us={authorization_us:.3} bind_execute_sync_us={cycle_us:.3} profile=dev role=non_superuser");
}

#[test]
fn unnamed_hit_drains_thread_local_error_diagnostics() {
    let mut session = fixture();
    let mut state = state();
    for sql in ["CREATE TABLE diagnostic_source(id INT)", "CREATE SCHEMA diagnostic_schema",
        "CREATE EXTENSION \"uuid-ossp\" WITH SCHEMA diagnostic_schema"] {
        session.execute_sql(sql, &[]).unwrap();
    }
    let sql = "SELECT * FROM diagnostic_source";
    extended(&mut state, &mut session, sql).unwrap();
    // Core errors can leave diagnostics for the wire layer to consume. Exercise
    // the hit on that same thread before any full prepare drains those slots.
    assert!(session.execute_sql("DROP SCHEMA diagnostic_schema", &[]).is_err());
    assert!(state.reuse_unnamed(sql, &[], &mut session).unwrap().is_some());
    assert_eq!(giggles_core::relational::take_sql_error_message(), None);
    assert_eq!(giggles_core::relational::take_sql_error_detail(), None);
    assert_eq!(giggles_core::relational::take_sql_error_hint(), None);
}

#[test]
fn unnamed_cache_rechecks_lowered_statement_budgets() {
    let mut session = fixture();
    session.execute_sql("CREATE TABLE t (id int PRIMARY KEY)", &[]).unwrap();
    let mut state = state();
    let sql = "SELECT id FROM t WHERE id IN (1,2,3,4,5,6,7,8,9,10)";
    parse_sql(&mut state, &mut session, sql).unwrap();
    assert!(!state.unnamed_cache.is_empty());
    simple(&mut state, &mut session, "SET gigglesdb.max_statement_tokens=16").unwrap();
    assert_eq!(parse_sql(&mut state, &mut session, sql), Err("54000"));
}

#[test]
fn unnamed_cache_budget_settings_for_non_superuser() {
    let owner = fixture();
    owner.service.admin.roles().create(NewRole {
        name: "budget_reader".into(), login: true, ..Default::default()
    }).unwrap();
    let mut session = owner.service.role_session("budget_reader").unwrap();
    let mut state = state();
    for (name, value, default) in [
        ("gigglesdb.max_statement_tokens", "64", "1000000"),
        ("gigglesdb.max_value_bytes", "65536", "67108864"),
    ] {
        simple(&mut state, &mut session, &format!("SET {name}={value}")).unwrap();
        assert_eq!(text_rows(&simple(&mut state, &mut session, &format!("SHOW {name}")).unwrap()), vec![vec![value]]);
        assert_eq!(text_rows(&extended(&mut state, &mut session,
            &format!("SELECT setting FROM pg_settings WHERE name='{name}'")).unwrap()), vec![vec![value]]);
        simple(&mut state, &mut session, &format!("RESET {name}")).unwrap();
        assert_eq!(text_rows(&simple(&mut state, &mut session, &format!("SHOW {name}")).unwrap()), vec![vec![default]]);
    }
}

// Inject the SQLSTATE at the dispatcher boundary: the local fixture has no
// replicated engine to fence without starting a cluster.
#[test]
fn unknown_outcome_rechecks_leadership_only_on_error() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    for rejects in [false, true] {
        for tag in *b"PBDESQd" {
            let mut session = fixture();
            let mut state = state();
            let calls = Arc::new(AtomicUsize::new(0));
            let gate_calls = calls.clone();
            assert!(
                session
                    .service
                    .set_statement_gate(Arc::new(move || {
                        gate_calls.fetch_add(1, Ordering::Relaxed);
                        if rejects {
                            Err("not leader".into())
                        } else {
                            Ok(())
                        }
                    }))
                    .is_ok()
            );
            let backend = session
                .service
                .cancellations
                .register(&session.fingerprint, session.epoch)
                .unwrap();
            let mut out = Vec::new();
            let outcome = state
                .buffered_message(
                    (tag, &[], std::time::Instant::now()),
                    &mut session,
                    &mut out,
                    backend.begin(true),
                    Some("08007"),
                    None,
                )
                .unwrap();
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            assert_eq!(outcome, (!rejects, rejects));
            let wire = String::from_utf8_lossy(&out);
            assert_eq!(wire.contains("57P01"), rejects);
            assert_eq!(
                wire.contains("08007"),
                !rejects || !matches!(tag, b'P' | b'B' | b'D')
            );
            if !rejects {
                assert!(
                    state
                        .buffered_message(
                            (b'S', &[], std::time::Instant::now()),
                            &mut session,
                            &mut Vec::new(),
                            backend.begin(true),
                            None,
                            None,
                        )
                        .unwrap()
                        .0
                );
                assert!(
                    !extended(&mut state, &mut session, "SELECT 1")
                        .unwrap()
                        .is_empty()
                );
                assert_eq!(calls.load(Ordering::Relaxed), 1);
            }
        }
    }
}

#[test]
fn unnamed_cache_sequence_dependency_equivalence() {
    let mut session = fixture();
    session.execute_sql("CREATE TABLE t (id serial PRIMARY KEY)", &[]).unwrap();
    session.execute_sql("CREATE SEQUENCE s OWNED BY t.id", &[]).unwrap();
    let mut state = state();
    for sql in [
        "WITH q AS (SELECT nextval('s') AS n) SELECT n FROM q",
        "SELECT (SELECT currval('s'))",
        "SELECT EXISTS (SELECT nextval('s'))",
        "SELECT 1 IN (SELECT nextval('s'))",
        "SELECT CASE WHEN true THEN nextval('s') ELSE currval('s') END",
        "SELECT coalesce(setval('s', 7), currval('s'))",
        "SELECT nextval('public.s')",
        "SELECT nextval('s'::regclass)",
    ] {
        parse_sql(&mut state, &mut session, sql).unwrap();
        let cached = state.unnamed_cache.iter().find(|entry| entry.sql == sql).unwrap();
        let unbound = Prepared::sequence_dependencies(sql);
        assert!(!unbound.is_empty(), "{sql}");
        assert_eq!(&*cached.sequence_dependencies, unbound.as_slice(), "{sql}");
        let Plan::Sql(bound) = &cached.p else { panic!("SQL plan expected") };
        println!("sequence equivalence: {sql}: bound_equal={} unbound={unbound:?} bound={:?}", bound.sequences() == unbound, bound.sequences());
    }
    for sql in [
        "CREATE TABLE defaults (id int DEFAULT nextval('s'))",
        "INSERT INTO t DEFAULT VALUES",
        "INSERT INTO t (id) VALUES (DEFAULT)",
    ] {
        parse_sql(&mut state, &mut session, sql).unwrap();
        assert!(state.statements[""].parse_context.is_none(), "{sql}");
        assert!(state.unnamed_cache.iter().all(|entry| entry.sql != sql));
    }
}

#[test]
fn returning_reply_w6_sync_sends_parameter_status_over_reply_limit() {
    let mut session = fixture();
    let mut observer = session.service.role_session("owner").unwrap();
    let mut state = state();
    simple(
        &mut state,
        &mut session,
        "CREATE TABLE sync_status (id int PRIMARY KEY)",
    )
    .unwrap();
    let path = format!("{}, public", vec!["s".repeat(63); 80].join(", "));
    simple(
        &mut state,
        &mut session,
        &format!("SET search_path TO {path}"),
    )
    .unwrap();

    let mut out = Vec::new();
    extended_execute(
        &mut state,
        &mut session,
        "INSERT INTO sync_status VALUES (1)",
        &mut out,
    )
    .unwrap();
    extended_execute(
        &mut state,
        &mut session,
        "SELECT set_config('search_path', 'public', true)",
        &mut out,
    )
    .unwrap();
    assert!(state.implicit);
    assert_eq!(state.settings.current.search_path, "public");

    // Supply a nearly full reply to the Sync handler. A normal wire Sync has
    // its own message buffer, so this boundary is exercised directly.
    let mut out = vec![0; MAX_RESPONSE - 800];
    let before = out.len();
    state
        .handle(b'S', &[], &mut session, &mut out, std::time::Instant::now())
        .unwrap();
    assert_eq!(&out[before..before + 1], b"S");
    assert!(
        out[before..]
            .windows(path.len())
            .any(|bytes| bytes == path.as_bytes())
    );
    assert!(out.len() > MAX_RESPONSE);
    assert!(out.ends_with(b"Z\0\0\0\x05I"));
    let seen = observer
        .execute_sql("SELECT id FROM sync_status", &[])
        .unwrap();
    assert_eq!(seen.rows.len(), 1);
}

#[test]
fn returning_reply_w7_end_of_query_notice_after_commit_and_begin() {
    let mut session = fixture();
    let mut observer = session.service.role_session("owner").unwrap();
    let mut state = state();
    simple(&mut state, &mut session, "CREATE TABLE t (id int PRIMARY KEY)").unwrap();
    let values = (1..=256).map(|id| format!("({id})")).collect::<Vec<_>>().join(",");
    let sql = format!(
        "BEGIN; INSERT INTO t VALUES {values} RETURNING \
         repeat('x', CASE WHEN id = 256 THEN 32906 ELSE 32756 END); COMMIT; BEGIN"
    );
    NOTICE_ON_ROLE_REFRESH.with(|notice| notice.set(true));
    let reply = simple(&mut state, &mut session, &sql);
    let seen = observer.execute_sql("SELECT id FROM t", &[]).unwrap();
    assert_eq!(seen.rows.len(), 256);
    let reply = reply.expect("end-of-query notice failed after durable COMMIT");
    assert!(!NOTICE_ON_ROLE_REFRESH.with(|notice| notice.get()));
    assert!(reply.len() > MAX_RESPONSE);

    let mut tail = Vec::new();
    let mut at = 0;
    while at < reply.len() {
        let length = u32::from_be_bytes(reply[at + 1..at + 5].try_into().unwrap()) as usize;
        let end = at + 1 + length;
        if matches!(reply[at], b'C' | b'N' | b'Z') {
            tail.push((reply[at], &reply[at + 5..end]));
        }
        at = end;
    }
    assert_eq!(tail.len(), 6);
    assert_eq!(tail[2], (b'C', b"COMMIT\0".as_slice()));
    assert_eq!(tail[3], (b'C', b"BEGIN\0".as_slice()));
    assert_eq!(tail[4].0, b'N');
    assert!(tail[4].1.ends_with(b"role reset after dispatch\0\0"));
    assert_eq!(tail[5], (b'Z', b"T".as_slice()));
}

thread_local! {
    static FAIL_ROLE_REFRESH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static NOTICE_ON_ROLE_REFRESH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// The socket worker runs on a blocking thread. Scope its fault to this
// service and CopyDone, after the transaction commits.
static DROP_COPY_ROLE: std::sync::Mutex<Option<std::sync::Weak<crate::Service>>> =
    std::sync::Mutex::new(None);

pub(super) fn post_dispatch_role_refresh(session: &mut Session, tag: u8) -> R<()> {
    if NOTICE_ON_ROLE_REFRESH.with(|notice| notice.replace(false)) {
        session.role_notices.push((true, "role reset after dispatch".into()));
    }
    if FAIL_ROLE_REFRESH.with(|fail| fail.replace(false)) {
        return Err("XX000");
    }
    if tag == b'c' {
        let mut fault = DROP_COPY_ROLE.lock().unwrap();
        if fault.as_ref().is_some_and(|service| {
            std::ptr::eq(service.as_ptr(), std::sync::Arc::as_ptr(&session.service))
        }) {
            fault.take();
            drop(fault);
            let Role::Database { name } = &session.role else {
                unreachable!()
            };
            session.service.admin.roles().drop(name).unwrap();
        }
    }
    Ok(())
}

fn check_copy_refresh(simple_query: Option<&str>, fail_on: u8, written: usize) {
    let mut session = fixture();
    let mut observer = session.service.role_session("owner").unwrap();
    let mut state = state();
    simple(
        &mut state,
        &mut session,
        "CREATE TABLE copy_refresh (id int PRIMARY KEY)",
    )
    .unwrap();
    let backend = crate::cancel::Registry::default().register("", 0).unwrap();
    let dispatch = |state: &mut State, session: &mut Session, tag, body: &[u8]| {
        let mut out = Vec::new();
        let active = state
            .copy_active
            .take()
            .unwrap_or_else(|| backend.begin(true));
        assert_eq!(
            state
                .buffered_message(
                    (tag, body, std::time::Instant::now()),
                    session,
                    &mut out,
                    active,
                    None,
                    None,
                )
                .unwrap(),
            (true, false)
        );
        out
    };
    if let Some(query) = simple_query {
        let mut body = Vec::new();
        cstr(query, &mut body);
        assert_eq!(dispatch(&mut state, &mut session, b'Q', &body)[0], b'G');
    } else {
        extended_execute(
            &mut state,
            &mut session,
            "COPY copy_refresh FROM STDIN",
            &mut Vec::new(),
        )
        .unwrap();
    }
    assert!(state.copy_receiving());
    dispatch(&mut state, &mut session, b'd', b"1\n");
    if fail_on == b'S' {
        assert!(dispatch(&mut state, &mut session, b'c', &[]).is_empty());
        assert!(
            observer
                .execute_sql("SELECT id FROM copy_refresh", &[])
                .unwrap()
                .rows
                .is_empty()
        );
    }

    FAIL_ROLE_REFRESH.with(|fail| fail.set(true));
    let body = if fail_on == b'f' { b"client cancelled\0".as_slice() } else { &[] };
    let out = dispatch(&mut state, &mut session, fail_on, body);
    let seen = observer
        .execute_sql("SELECT id FROM copy_refresh", &[])
        .unwrap();
    assert_eq!(seen.rows.len(), written);
    assert_eq!(out[0], b'E');
    let end = 1 + u32::from_be_bytes(out[1..5].try_into().unwrap()) as usize;
    let expected = if written == 0 {
        b"CXX000\0"
    } else {
        b"C08007\0"
    };
    assert!(
        out[5..end].windows(7).any(|field| field == expected),
        "{out:?}"
    );
    if written == 0 && simple_query.is_none() {
        assert_eq!(end, out.len());
        let sync = dispatch(&mut state, &mut session, b'S', &[]);
        let seen = observer
            .execute_sql("SELECT id FROM copy_refresh", &[])
            .unwrap();
        assert!(seen.rows.is_empty());
        assert_eq!(sync, b"Z\0\0\0\x05I");
    } else {
        assert_eq!(&out[end..], b"Z\0\0\0\x05I");
    }
    assert!(!state.drain);
}

#[test]
fn returning_reply_w4_copy_done_role_refresh_reports_unknown_and_readies_once() {
    check_copy_refresh(
        Some("COPY copy_refresh FROM STDIN; INSERT INTO copy_refresh VALUES (2)"),
        b'c',
        2,
    );
}

#[test]
fn returning_reply_w5_standalone_copy_done_role_refresh_reports_unknown_and_readies_once() {
    check_copy_refresh(Some("COPY copy_refresh FROM STDIN"), b'c', 1);
}

#[test]
fn returning_reply_w5_extended_copy_sync_role_refresh_reports_unknown_and_readies_once() {
    check_copy_refresh(None, b'S', 1);
}

#[test]
fn returning_reply_w6_extended_copy_done_error_discards_completion() {
    check_copy_refresh(None, b'c', 0);
}

#[test]
fn returning_reply_w7_simple_copy_fail_refresh_readies_once() {
    check_copy_refresh(Some("COPY copy_refresh FROM STDIN"), b'f', 0);
}

#[tokio::test]
async fn returning_reply_w5_socket_flushes_unknown_outcome_on_binding_loss() {
    let mut observer = fixture();
    observer
        .execute_sql("CREATE TABLE copy_refresh (id int PRIMARY KEY)", &[])
        .unwrap();
    let service = &observer.service;
    service
        .admin
        .roles()
        .create(NewRole {
            name: "copy_writer".into(),
            login: true,
            superuser: true,
            ..Default::default()
        })
        .unwrap();
    let writer = service.role_session("copy_writer").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let startup = crate::pgwire_startup::Startup {
            user: "copy_writer".into(),
            database: "postgres".into(),
            settings: Default::default(),
            timezone: "UTC".into(),
            task: None,
        };
        serve_buffered(&mut socket, writer, startup, false, &[])
            .await
            .unwrap();
    });
    async fn read(client: &mut tokio::net::TcpStream) -> Option<(u8, Vec<u8>)> {
        timeout(std::time::Duration::from_secs(10), async {
            let tag = match client.read_u8().await {
                Ok(tag) => tag,
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return None,
                Err(error) => panic!("{error}"),
            };
            let mut body = vec![0; client.read_u32().await.unwrap() as usize - 4];
            client.read_exact(&mut body).await.unwrap();
            Some((tag, body))
        })
        .await
        .unwrap()
    }
    while read(&mut client).await.unwrap().0 != b'Z' {}

    let mut request = Vec::new();
    frame(
        b'Q',
        b"COPY copy_refresh FROM STDIN; INSERT INTO copy_refresh VALUES (2)\0",
        &mut request,
    )
    .unwrap();
    client.write_all(&request).await.unwrap();
    assert_eq!(read(&mut client).await.unwrap().0, b'G');

    *DROP_COPY_ROLE.lock().unwrap() = Some(std::sync::Arc::downgrade(service));
    request.clear();
    frame(b'd', b"1\n", &mut request).unwrap();
    frame(b'c', &[], &mut request).unwrap();
    client.write_all(&request).await.unwrap();
    let mut reply = Vec::new();
    while let Some(message) = read(&mut client).await {
        reply.push(message);
    }
    server.await.unwrap();
    let seen = observer
        .execute_sql("SELECT id FROM copy_refresh", &[])
        .unwrap();
    assert_eq!(seen.rows.len(), 2);
    assert_eq!(reply.len(), 2, "{reply:?}");
    assert_eq!(reply[0].0, b'E');
    assert!(reply[0].1.windows(7).any(|field| field == b"C08007\0"));
    assert_eq!(reply[1], (b'Z', b"I".to_vec()));
}
