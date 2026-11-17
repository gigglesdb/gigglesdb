//! R3-206 (GD-0061 design-2 section 4, table 4(b)): the statement profile of a
//! task session on the agent pgwire listener. Every statement is admitted by
//! class before it runs; refused classes answer 0A000 with the class message.
//! In this build `task::admit::ENABLED` is `[Show, Set, Update, OutcomeLookup, Preview, Apply]`.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{Msg, Wire, agent_wire, error_of, ordinary_wire};
use giggles_core::task::{StatementClass, TaskError};

fn class_message(class: StatementClass) -> String {
    TaskError::NotAllowedInTaskSession(class).message().to_owned()
}

/// The reply is exactly one refusal of `class`, and the session is idle.
#[track_caller]
fn assert_refused(messages: &[Msg], class: StatementClass, sql: &str) {
    assert_eq!(
        error_of(messages),
        Some(("0A000".to_owned(), class_message(class))),
        "{sql}: {messages:?}"
    );
    assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{sql}: {messages:?}");
    assert!(
        !messages
            .iter()
            .any(|m| matches!(m, Msg::Row(_) | Msg::Complete(_))),
        "{sql}: {messages:?}"
    );
}

#[track_caller]
fn assert_ok(messages: &[Msg], sql: &str) {
    assert_eq!(error_of(messages), None, "{sql}: {messages:?}");
    assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{sql}: {messages:?}");
}

fn rows(messages: &[Msg]) -> Vec<Vec<Option<String>>> {
    messages
        .iter()
        .filter_map(|m| match m {
            Msg::Row(values) => Some(values.clone()),
            _ => None,
        })
        .collect()
}

fn completion(messages: &[Msg]) -> Option<&str> {
    messages.iter().find_map(|m| match m {
        Msg::Complete(tag) => Some(tag.as_str()),
        _ => None,
    })
}

/// Runs `sql` as a simple Query and through Parse/Bind/Describe/Execute.
async fn both(wire: &mut Wire, sql: &str) -> [Vec<Msg>; 2] {
    let simple = wire.simple(sql).await;
    let extended = wire.extended(sql, &[], 0).await;
    [simple, extended]
}

async fn refused_both(wire: &mut Wire, sql: &str, class: StatementClass) {
    for messages in both(wire, sql).await {
        assert_refused(&messages, class, sql);
    }
}

/// One case per refused row of table 4(b), plus the core classes that are not
/// enabled yet.
const REFUSED: &[(&str, StatementClass)] = &[
    ("DECLARE c CURSOR FOR SELECT 1", StatementClass::Cursor),
    ("FETCH 1 FROM c", StatementClass::Cursor),
    ("CLOSE c", StatementClass::Cursor),
    ("LOCK TABLE shipments IN ACCESS SHARE MODE", StatementClass::Lock),
    ("DROP TABLE shipments", StatementClass::Ddl),
    ("CREATE ROLE intruder", StatementClass::RoleStatement),
    ("GRANT SELECT ON shipments TO agent_exec", StatementClass::RoleStatement),
    ("BEGIN", StatementClass::TransactionControl),
    ("COMMIT", StatementClass::TransactionControl),
    ("ROLLBACK", StatementClass::TransactionControl),
    (
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        StatementClass::TransactionControl,
    ),
    (
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        StatementClass::TransactionControl,
    ),
    ("SAVEPOINT a", StatementClass::Savepoint),
    ("RELEASE SAVEPOINT a", StatementClass::Savepoint),
    ("ROLLBACK TO SAVEPOINT a", StatementClass::Savepoint),
    ("SELECT pg_advisory_lock(1)", StatementClass::Lock),
    (
        "SELECT name,setting::numeric AS setting FROM pg_show_all_settings() WHERE name IN ('autovacuum_analyze_threshold') ORDER BY name",
        StatementClass::Catalog,
    ),
    ("SELECT relname FROM pg_catalog.pg_class", StatementClass::Catalog),
    ("COPY shipments TO STDOUT", StatementClass::Copy),
    ("COPY (SELECT id FROM shipments) TO STDOUT", StatementClass::Copy),
    ("COPY shipments FROM STDIN", StatementClass::Copy),
    ("GIGGLES {\"kind\":\"ping\"}", StatementClass::Other),
    ("SELECT id FROM shipments", StatementClass::Select),
    ("INSERT INTO shipments VALUES (1, 42, 'new')", StatementClass::Insert),
    ("DELETE FROM shipments WHERE id = 1", StatementClass::Delete),
    ("TRUNCATE shipments", StatementClass::Delete),
    ("CREATE TABLE t2 (id bigint primary key)", StatementClass::Ddl),
    ("EXPLAIN SELECT id FROM shipments", StatementClass::Explain),
];

#[allow(non_snake_case, reason = "the test name is fixed by the R3-206 brief")]
#[tokio::test]
async fn every_refused_class_returns_0A000_with_class_message() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for (sql, class) in REFUSED {
        refused_both(&mut wire, sql, *class).await;
    }
    // The messages are the fixed class texts, not a generic one.
    assert_eq!(
        class_message(StatementClass::TransactionControl),
        "explicit transactions are not allowed in a task session"
    );
}

#[tokio::test]
async fn begin_commit_rollback_savepoint_refused() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for sql in ["BEGIN", "START TRANSACTION", "COMMIT", "END", "ROLLBACK", "ABORT"] {
        let [simple, extended] = both(&mut wire, sql).await;
        for messages in [simple, extended] {
            assert_eq!(error_of(&messages).map(|e| e.0), Some("0A000".into()), "{sql}");
            assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{sql}");
        }
    }
    for sql in ["SAVEPOINT s", "RELEASE s", "ROLLBACK TO s"] {
        refused_both(&mut wire, sql, StatementClass::Savepoint).await;
    }
}

#[tokio::test]
async fn multi_statement_query_refused() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for sql in [
        "SHOW application_name; SHOW DateStyle",
        "SET application_name = 'a'; SET application_name = 'b'",
        "SELECT 1; DELETE FROM shipments",
        "BEGIN; SELECT 1; COMMIT",
    ] {
        assert_refused(
            &wire.simple(sql).await,
            StatementClass::TransactionControl,
            sql,
        );
    }
    // Nothing from the refused batch ran.
    let show = wire.simple("SHOW application_name").await;
    assert_ok(&show, "SHOW");
    assert_ne!(rows(&show), vec![vec![Some("b".to_owned())]]);
    // A trailing semicolon or comments still make one statement; no statement is empty.
    assert_ok(&wire.simple("SELECT 1; -- done").await, "one");
    let empty = wire.simple(" ; ").await;
    assert_eq!(empty, vec![Msg::Empty, Msg::Ready(b'I')]);
}

#[tokio::test]
async fn execute_with_row_limit_refused() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for sql in ["SHOW application_name", "SELECT 1"] {
        let limited = wire.extended(sql, &[], 1).await;
        assert_refused(&limited, StatementClass::Cursor, sql);
        let all = wire.extended(sql, &[], 0).await;
        assert_ok(&all, sql);
        assert_eq!(rows(&all).len(), 1, "{sql}");
    }
}

#[tokio::test]
async fn copy_to_stdout_and_copy_from_refused() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for sql in [
        "COPY shipments TO STDOUT",
        "COPY shipments (id, status) TO STDOUT WITH (FORMAT csv)",
        "COPY (SELECT id FROM shipments) TO STDOUT",
        "COPY shipments FROM STDIN",
        "copy shipments from stdin with (format csv)",
    ] {
        refused_both(&mut wire, sql, StatementClass::Copy).await;
    }
}

#[tokio::test]
async fn explain_declare_fetch_lock_refused() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    refused_both(&mut wire, "EXPLAIN SELECT id FROM shipments", StatementClass::Explain).await;
    for sql in [
        "DECLARE c CURSOR FOR SELECT id FROM shipments",
        "FETCH NEXT FROM c",
        "MOVE NEXT FROM c",
        "CLOSE ALL",
    ] {
        refused_both(&mut wire, sql, StatementClass::Cursor).await;
    }
    for sql in [
        "LOCK TABLE shipments",
        "LOCK shipments IN ACCESS SHARE MODE",
        "SELECT pg_advisory_lock(7)",
        "SELECT pg_try_advisory_xact_lock(7)",
    ] {
        refused_both(&mut wire, sql, StatementClass::Lock).await;
    }
}

#[tokio::test]
async fn pg_catalog_and_information_schema_refused_but_select_1_version_allowed() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for sql in [
        "SELECT relname FROM pg_catalog.pg_class",
        "SELECT * FROM pg_class",
        "SELECT table_name FROM information_schema.tables",
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'shipments'",
        "SELECT rolname FROM pg_roles",
    ] {
        refused_both(&mut wire, sql, StatementClass::Catalog).await;
    }
    for (sql, expected) in [
        ("SELECT 1", Some("1".to_owned())),
        ("select 1;", Some("1".to_owned())),
        ("SELECT current_schema()", Some("public".to_owned())),
    ] {
        for messages in both(&mut wire, sql).await {
            assert_ok(&messages, sql);
            assert_eq!(rows(&messages), vec![vec![expected.clone()]], "{sql}");
            assert_eq!(completion(&messages), Some("SELECT 1"), "{sql}");
        }
    }
    for messages in both(&mut wire, "SELECT version()").await {
        assert_ok(&messages, "version");
        let version = rows(&messages)[0][0].clone().unwrap();
        assert!(version.starts_with("PostgreSQL"), "{version}");
    }
    // Describe of a relation-free probe works without a catalog snapshot.
    let described = wire.parse_describe("SELECT 1", &[]).await;
    assert_ok(&described, "describe");
    assert!(described.iter().any(|m| matches!(m, Msg::Columns(c) if c.len() == 1)));
}

#[tokio::test]
async fn set_allow_list_and_refusals() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for (sql, show, expected) in [
        ("SET application_name = 'agent'", "SHOW application_name", "agent"),
        ("SET extra_float_digits = 1", "SHOW extra_float_digits", "1"),
        ("SET DateStyle = 'ISO, MDY'", "SHOW DateStyle", "ISO, MDY"),
        ("SET TimeZone = 'UTC'", "SHOW TimeZone", "UTC"),
        ("SET IntervalStyle = 'iso_8601'", "SHOW IntervalStyle", "iso_8601"),
        ("SET statement_timeout = 5000", "SHOW statement_timeout", "5s"),
        ("RESET application_name", "SHOW application_name", ""),
    ] {
        for messages in both(&mut wire, sql).await {
            assert_ok(&messages, sql);
            assert!(
                completion(&messages).is_some_and(|tag| tag == "SET" || tag == "RESET"),
                "{sql}: {messages:?}"
            );
            let shown = wire.simple(show).await;
            assert_eq!(rows(&shown), vec![vec![Some(expected.to_owned())]], "{sql}");
        }
    }
    for sql in [
        "SET search_path = public",
        "SET search_path TO DEFAULT",
        "SET role = agent_exec",
        "SET ROLE agent_exec",
        "SET session_authorization = 'oncall_lead'",
        "SET SESSION AUTHORIZATION oncall_lead",
        "SET LOCAL application_name = 'x'",
        "SET LOCAL TimeZone = 'UTC'",
        "RESET ALL",
        "SET giggles.task = 'inc-437'",
        "SET client_min_messages = warning",
    ] {
        refused_both(&mut wire, sql, StatementClass::Set).await;
    }
    // The refusals changed nothing.
    let path = wire.simple("SHOW search_path").await;
    assert_eq!(rows(&path), vec![vec![Some("\"$user\", public".to_owned())]]);
}

#[tokio::test]
async fn show_allowed() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for sql in [
        "SHOW application_name",
        "SHOW search_path",
        "SHOW TimeZone",
        "SHOW DateStyle",
        "SHOW transaction_isolation",
        "SHOW server_version",
        "SHOW standard_conforming_strings",
    ] {
        for messages in both(&mut wire, sql).await {
            assert_ok(&messages, sql);
            assert_eq!(rows(&messages).len(), 1, "{sql}");
            assert_eq!(completion(&messages), Some("SHOW"), "{sql}");
        }
    }
}

#[tokio::test]
async fn set_config_and_advisory_functions_classified() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for messages in both(&mut wire, "SELECT set_config('application_name', 'via_fn', false)").await {
        assert_ok(&messages, "set_config");
        assert_eq!(rows(&messages), vec![vec![Some("via_fn".to_owned())]]);
    }
    for messages in both(&mut wire, "SELECT current_setting('application_name')").await {
        assert_ok(&messages, "current_setting");
        assert_eq!(rows(&messages), vec![vec![Some("via_fn".to_owned())]]);
    }
    for sql in [
        "SELECT set_config('application_name', 'x', true)",
        "SELECT set_config('search_path', 'other', false)",
    ] {
        refused_both(&mut wire, sql, StatementClass::Set).await;
    }
    for sql in [
        "SELECT pg_advisory_lock(1)",
        "SELECT pg_try_advisory_lock(1)",
        "SELECT pg_advisory_unlock(1)",
        "SELECT pg_advisory_xact_lock(1)",
    ] {
        refused_both(&mut wire, sql, StatementClass::Lock).await;
    }
}

#[tokio::test]
async fn no_explicit_transaction_slot_is_taken() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for sql in ["BEGIN", "BEGIN ISOLATION LEVEL READ COMMITTED", "START TRANSACTION"] {
        for messages in both(&mut wire, sql).await {
            assert_eq!(error_of(&messages).map(|e| e.0), Some("0A000".into()), "{sql}");
            // ReadyForQuery reports idle: no transaction block was opened.
            assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{sql}");
        }
        // A following statement is not inside a block either.
        for messages in both(&mut wire, "SELECT 1").await {
            assert_ok(&messages, "after BEGIN");
        }
    }
    // Neither does an allowed extended statement open an implicit one before Sync.
    for sql in ["SHOW application_name", "SET application_name = 'z'", "SELECT 1"] {
        let messages = wire.extended(sql, &[], 0).await;
        assert_ok(&messages, sql);
        assert!(!messages.contains(&Msg::Ready(b'T')), "{sql}");
    }
}

#[tokio::test]
async fn ordinary_session_behaviour_unchanged() {
    let (server, _agent, ordinary, _wire) = agent_wire().await;
    server
        .admin
        .roles()
        .grant_table(
            "shipments",
            "oncall_lead",
            &[giggles_core::roles::TablePrivilege::Select],
        )
        .unwrap();
    let (listener, mut wire) = ordinary_wire(&server, &ordinary).await;
    let begin = wire.simple("BEGIN").await;
    assert_eq!(begin.last(), Some(&Msg::Ready(b'T')), "{begin:?}");
    assert_eq!(completion(&wire.simple("SAVEPOINT a").await), Some("SAVEPOINT"));
    assert_eq!(
        completion(&wire.simple("LOCK TABLE shipments IN ACCESS SHARE MODE").await),
        Some("LOCK TABLE")
    );
    let commit = wire.simple("COMMIT").await;
    assert_eq!(commit.last(), Some(&Msg::Ready(b'I')));
    let batch = wire.simple("SHOW application_name; SHOW DateStyle").await;
    assert_eq!(error_of(&batch), None, "{batch:?}");
    assert_eq!(rows(&batch).len(), 2);
    for sql in [
        "SET search_path = public",
        "SET LOCAL application_name = 'x'",
        "RESET ALL",
        "SELECT id FROM shipments",
        "SELECT relname FROM pg_catalog.pg_class",
        "SELECT set_config('search_path', 'public', false)",
    ] {
        let messages = wire.simple(sql).await;
        assert_eq!(error_of(&messages), None, "{sql}: {messages:?}");
    }
    // A row limit suspends the portal instead of being refused.
    server
        .admin
        .sql_session()
        .execute(
            "INSERT INTO shipments VALUES (1, 42, 'failed'), (2, 42, 'failed')",
            &[],
            None,
        )
        .unwrap();
    let limited = wire.extended("SELECT id FROM shipments", &[], 1).await;
    assert_eq!(error_of(&limited), None, "{limited:?}");
    assert!(limited.contains(&Msg::Suspended), "{limited:?}");
    // DDL and COPY still reach their ordinary checks (not the task refusal).
    let drop = wire.simple("DROP TABLE shipments").await;
    assert_ne!(
        error_of(&drop),
        Some(("0A000".into(), class_message(StatementClass::Ddl)))
    );
    listener.shutdown().await.unwrap();
}
