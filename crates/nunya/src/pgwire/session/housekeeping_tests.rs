// Session housekeeping uses the simple-query path without changing server settings.
const HOUSEKEEPING_SELECT: &str = "SELECT 1";

fn housekeeping_query(s: &mut Session, sql: &str) -> Actions {
    s.frontend(Frontend::Query(sql.into()))
}

fn assert_housekeeping_forwarded(out: Actions, sql: &str) {
    assert!(!out.close);
    assert!(out.client.is_empty());
    assert!(matches!(out.upstream.as_slice(), [Frontend::Query(query)] if query == sql));
}

fn assert_housekeeping_tag(out: Actions, tag: &str) {
    assert!(!out.close);
    assert!(out.upstream.is_empty());
    assert!(matches!(out.client.as_slice(), [Backend::CommandComplete(actual)] if actual == tag));
}

fn assert_housekeeping_error(out: &Actions, code: &str) {
    assert!(!out.close);
    assert!(out.upstream.is_empty());
    assert!(
        matches!(out.client.first(), Some(Backend::ErrorResponse(fields))
        if fields.iter().any(|(field, value)| *field == b'C' && value == code))
    );
}

fn housekeeping_sync(s: &mut Session, status: u8) {
    let sync = s.frontend(Frontend::Sync);
    assert!(!sync.close);
    assert!(sync.client.is_empty());
    assert!(matches!(sync.upstream.as_slice(), [Frontend::Sync]));
    assert_result_ready(s, status, status);
}

fn housekeeping_bind(statement: &str, portal: &str) -> Frontend {
    Frontend::Bind {
        portal: portal.into(),
        statement: statement.into(),
        param_formats: vec![],
        params: vec![],
        result_formats: vec![],
    }
}

#[test]
fn deallocate_all_waits_for_completion_and_preserves_unnamed_and_portals() {
    let f = Fixture::new();
    let mut s = f.session();
    begin_result_transaction(&mut s);
    for name in ["s1", ""] {
        queue_result_parse(&mut s, name, HOUSEKEEPING_SELECT);
        assert!(matches!(
            s.backend(Backend::ParseComplete).client.as_slice(),
            [Backend::ParseComplete]
        ));
        housekeeping_sync(&mut s, b'T');
    }
    queue_result_bind(&mut s, "s1", "existing");
    assert!(matches!(
        s.backend(Backend::BindComplete).client.as_slice(),
        [Backend::BindComplete]
    ));
    housekeeping_sync(&mut s, b'T');

    assert_housekeeping_forwarded(
        housekeeping_query(&mut s, "DEALLOCATE ALL"),
        "DEALLOCATE ALL",
    );
    assert!(s.prepared.contains_key("s1"));
    assert_housekeeping_tag(
        s.backend(Backend::CommandComplete("DEALLOCATE ALL".into())),
        "DEALLOCATE ALL",
    );
    assert_result_ready(&mut s, b'T', b'T');
    assert_eq!(s.prepared.len(), 1);
    assert!(s.prepared.contains_key(""));
    assert!(s.portals.contains_key("existing"));
    assert_housekeeping_error(&s.frontend(housekeeping_bind("s1", "unknown")), "NY004");
}

#[test]
fn deallocate_all_upstream_error_preserves_named_statement() {
    let f = Fixture::new();
    let mut s = f.session();
    queue_result_parse(&mut s, "s1", HOUSEKEEPING_SELECT);
    s.backend(Backend::ParseComplete);
    housekeeping_sync(&mut s, b'I');

    assert_housekeeping_forwarded(
        housekeeping_query(&mut s, "DEALLOCATE ALL"),
        "DEALLOCATE ALL",
    );
    let error = s.backend(Backend::ErrorResponse(vec![
        (b'S', "ERROR".into()),
        (b'C', "XX000".into()),
    ]));
    assert_housekeeping_error(&error, "XX000");
    assert_result_ready(&mut s, b'I', b'I');
    assert!(s.prepared.contains_key("s1"));
}

#[test]
fn deallocate_named_removes_only_confirmed_statement_and_handles_local_names() {
    let f = Fixture::new();
    let mut s = f.session();
    for name in ["s1", "s2"] {
        queue_result_parse(&mut s, name, HOUSEKEEPING_SELECT);
        s.backend(Backend::ParseComplete);
    }
    let local = s.frontend(Frontend::Parse {
        name: "Local Setting".into(),
        query: "SET application_name = 'test'".into(),
        param_types: vec![],
    });
    assert!(local.upstream.is_empty());
    assert!(matches!(local.client.as_slice(), [Backend::ParseComplete]));
    housekeeping_sync(&mut s, b'I');

    assert_housekeeping_forwarded(housekeeping_query(&mut s, "DEALLOCATE s1"), "DEALLOCATE s1");
    assert!(s.prepared.contains_key("s1"));
    assert_housekeeping_tag(
        s.backend(Backend::CommandComplete("DEALLOCATE".into())),
        "DEALLOCATE",
    );
    assert_result_ready(&mut s, b'I', b'I');
    assert!(!s.prepared.contains_key("s1"));
    assert!(s.prepared.contains_key("s2"));

    let sql = r#"DEALLOCATE PREPARE "Local Setting""#;
    let out = housekeeping_query(&mut s, sql);
    assert!(!out.close);
    assert!(out.upstream.is_empty());
    assert!(matches!(
        out.client.as_slice(),
        [Backend::CommandComplete(tag), Backend::ReadyForQuery(b'I')] if tag == "DEALLOCATE"
    ));
    assert!(!s.prepared.contains_key("Local Setting"));
    assert!(s.prepared.contains_key("s2"));
    assert!(
        f.identities.lock().unwrap().iter().any(|record| {
            record.statement == sql && matches!(record.outcome, Outcome::Allowed)
        })
    );
}

#[test]
fn deallocate_unknown_name_forwards_original_sql_and_upstream_error() {
    let f = Fixture::new();
    let mut s = f.session();
    begin_result_transaction(&mut s);
    let sql = r#"/* driver */ deallocate prepare "Unknown";"#;
    assert_housekeeping_forwarded(housekeeping_query(&mut s, sql), sql);
    let error = s.backend(Backend::ErrorResponse(vec![
        (b'S', "ERROR".into()),
        (b'C', "26000".into()),
    ]));
    assert_housekeeping_error(&error, "26000");
    assert_result_ready(&mut s, b'E', b'E');
    assert!(s.aborted);
}

#[test]
fn discard_all_forwards_deallocate_and_clears_named_statements_and_portals() {
    let f = Fixture::new();
    let (id, principal) = acquire_login_with_mode(&f, "postgres", Mode::ReadOnly);
    let mut s = f.for_access(id, principal);
    queue_result_parse(&mut s, "s1", HOUSEKEEPING_SELECT);
    queue_result_bind(&mut s, "s1", "p");
    receive_result_acks(&mut s);
    housekeeping_sync(&mut s, b'I');

    assert_housekeeping_forwarded(housekeeping_query(&mut s, "DISCARD ALL"), "DEALLOCATE ALL");
    assert!(s.prepared.contains_key("s1"));
    assert!(s.portals.contains_key("p"));
    assert_housekeeping_tag(
        s.backend(Backend::CommandComplete("DEALLOCATE ALL".into())),
        "DISCARD ALL",
    );
    assert_result_ready(&mut s, b'I', b'I');
    assert!(s.prepared.is_empty());
    assert!(s.portals.is_empty());
}

#[test]
fn discard_all_in_transaction_refuses_and_aborted_housekeeping_returns_25p02() {
    let f = Fixture::new();
    let mut s = f.session();
    begin_result_transaction(&mut s);
    assert_housekeeping_error(&housekeeping_query(&mut s, "DISCARD ALL"), "NY006");
    for sql in [
        "DEALLOCATE ALL",
        "DISCARD ALL",
        "RESET ALL",
        "DISCARD PLANS",
    ] {
        let out = housekeeping_query(&mut s, sql);
        assert_housekeeping_error(&out, "25P02");
        assert!(matches!(
            out.client.last(),
            Some(Backend::ReadyForQuery(b'E'))
        ));
    }
}

#[test]
fn reset_and_discard_plans_are_local_for_read_only_sessions() {
    let f = Fixture::new();
    let (id, principal) = acquire_login_with_mode(&f, "postgres", Mode::ReadOnly);
    let mut s = f.for_access(id, principal);
    for (sql, tag) in [
        ("RESET ALL", "RESET"),
        ("RESET application_name", "RESET"),
        ("DISCARD PLANS", "DISCARD PLANS"),
    ] {
        let out = housekeeping_query(&mut s, sql);
        assert!(!out.close);
        assert!(out.upstream.is_empty());
        assert!(matches!(
            out.client.as_slice(),
            [Backend::CommandComplete(actual), Backend::ReadyForQuery(b'I')] if actual == tag
        ));
        assert!(f.identities.lock().unwrap().iter().any(|record| {
            record.statement == sql && matches!(record.outcome, Outcome::Allowed)
        }));

        let parsed = s.frontend(Frontend::Parse {
            name: "local".into(),
            query: sql.into(),
            param_types: vec![],
        });
        assert!(parsed.upstream.is_empty());
        assert!(matches!(parsed.client.as_slice(), [Backend::ParseComplete]));
        let bound = s.frontend(housekeeping_bind("local", "local"));
        assert!(bound.upstream.is_empty());
        assert!(matches!(bound.client.as_slice(), [Backend::BindComplete]));
        assert_housekeeping_tag(
            s.frontend(Frontend::Execute {
                portal: "local".into(),
                max_rows: 0,
            }),
            tag,
        );
        housekeeping_sync(&mut s, b'I');
    }
}

#[test]
fn housekeeping_mixed_with_other_statements_refuses() {
    let f = Fixture::new();
    let mut s = f.session();
    for sql in [
        "RESET search_path; SELECT 1",
        "RESET role; SELECT 1",
        "RESET ALL; SELECT 1",
        "SELECT 1; DISCARD PLANS",
        "DEALLOCATE s1; SELECT 1",
        "SELECT 1; DEALLOCATE ALL",
        "SELECT 1; DISCARD ALL",
        "DEALLOCATE ALL; DEALLOCATE ALL",
    ] {
        assert_housekeeping_error(&housekeeping_query(&mut s, sql), "NY006");
    }
}

#[test]
fn psycopg_rollback_deallocate_all_leaves_idle_session_and_allows_reparse() {
    let f = Fixture::new();
    let (id, principal) = acquire_login_with_mode(&f, "postgres", Mode::ReadOnly);
    let mut s = f.for_access(id, principal);
    begin_result_transaction(&mut s);
    queue_result_parse(&mut s, "s1", HOUSEKEEPING_SELECT);
    queue_result_bind(&mut s, "s1", "p");
    queue_result_execute(&mut s, "p");
    receive_result_acks(&mut s);
    let row = s.backend(Backend::DataRow(vec![Some(1_i32.to_be_bytes().to_vec())]));
    assert!(matches!(row.client.as_slice(), [Backend::DataRow(_)]));
    s.release_done();
    assert_housekeeping_tag(
        s.backend(Backend::CommandComplete("SELECT 1".into())),
        "SELECT 1",
    );
    housekeeping_sync(&mut s, b'T');

    for (sql, upstream, tag) in [
        ("ROLLBACK", "ROLLBACK", "ROLLBACK"),
        ("DEALLOCATE ALL", "DEALLOCATE ALL", "DEALLOCATE ALL"),
    ] {
        assert_housekeeping_forwarded(housekeeping_query(&mut s, sql), upstream);
        assert_housekeeping_tag(s.backend(Backend::CommandComplete(upstream.into())), tag);
        assert_result_ready(&mut s, b'I', b'I');
    }
    assert!(!s.aborted);
    assert_eq!(s.status, b'I');
    queue_result_parse(&mut s, "s1", HOUSEKEEPING_SELECT);
    assert!(matches!(
        s.backend(Backend::ParseComplete).client.as_slice(),
        [Backend::ParseComplete]
    ));
    housekeeping_sync(&mut s, b'I');
}

#[test]
fn extended_parse_refuses_deallocate_and_discard_all() {
    for sql in ["DEALLOCATE s1", "DEALLOCATE ALL", "DISCARD ALL"] {
        let f = Fixture::new();
        let mut s = f.session();
        let out = s.frontend(Frontend::Parse {
            name: "s1".into(),
            query: sql.into(),
            param_types: vec![],
        });
        assert_housekeeping_error(&out, "NY006");
        assert!(!s.prepared.contains_key("s1"));
        housekeeping_sync(&mut s, b'I');
    }
}
