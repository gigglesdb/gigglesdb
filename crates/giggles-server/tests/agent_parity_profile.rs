//! R3-1002: refused statement classes agree across SQL-credential agent surfaces.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::parity::{Case, Native, Observed, ParityFixture, Surface, agree};
use giggles_core::task::{DenyRule, StatementClass as Class, TaskError};
use serde_json::{Value, json};

struct Row {
    name: &'static str,
    class: &'static str,
    case: Case,
    expected_sqlstate: &'static str,
    expected: TaskError,
    native_na: Option<&'static str>,
    extended_na: Option<&'static str>,
    /// A row whose refusal does not yet match the design; asserted only by
    /// the ignored `known_bug_rows_refused_as_designed` and named in the table.
    known_bug: Option<&'static str>,
}

impl Row {
    fn na(&self, surface: Surface) -> Option<&str> {
        match surface {
            Surface::PgSimple => None,
            Surface::PgExtended => self.extended_na,
            Surface::HttpText | Surface::GrpcText => self.native_na,
        }
    }
}

fn rows() -> Vec<Row> {
    let mut rows = Vec::new();
    let mut add = |name, class, sql, native, expected, native_na| {
        let expected_sqlstate = match expected {
            TaskError::Denied(_) => "42501",
            TaskError::OperationIdRequired => "42T04",
            TaskError::NotAllowedInTaskSession(_) => "0A000",
            _ => unreachable!(),
        };
        rows.push(Row {
            name,
            class,
            case: Case::new(sql, native),
            expected_sqlstate,
            expected,
            native_na,
            extended_na: None,
            known_bug: None,
        });
    };
    use TaskError::NotAllowedInTaskSession as NotAllowed;
    add(
        "explicit_transaction",
        "TransactionControl",
        "BEGIN",
        "transaction { shipments where attempt_count = 2 select status }",
        NotAllowed(Class::TransactionControl),
        None,
    );
    add(
        "savepoint",
        "Savepoint",
        "SAVEPOINT parity_point",
        "",
        NotAllowed(Class::Savepoint),
        None,
    );
    add(
        "ddl",
        "Ddl",
        "DROP TABLE shipments",
        "",
        NotAllowed(Class::Ddl),
        None,
    );
    add(
        "role_statement",
        "RoleStatement",
        "CREATE ROLE parity_role",
        "",
        NotAllowed(Class::RoleStatement),
        Some("Native text 0.16 has no role or privilege statement form."),
    );
    add(
        "copy",
        "Copy",
        "COPY shipments TO STDOUT",
        "",
        NotAllowed(Class::Copy),
        Some("Native text 0.16 has no COPY form."),
    );
    add(
        "explain",
        "Explain",
        "EXPLAIN SELECT status FROM shipments",
        "",
        NotAllowed(Class::Explain),
        Some(
            "The native execute endpoint has no EXPLAIN request; lower_text rejects explain text before task admission.",
        ),
    );
    add(
        "cursor",
        "Cursor",
        "DECLARE parity_cursor CURSOR FOR SELECT status FROM shipments",
        "",
        NotAllowed(Class::Cursor),
        Some("Native text 0.16 has no cursor form."),
    );
    add(
        "lock",
        "Lock",
        "LOCK TABLE shipments IN ACCESS SHARE MODE",
        "",
        NotAllowed(Class::Lock),
        Some("Native text 0.16 has no explicit lock form."),
    );
    // D11 allows filtered catalogs; D12 refuses catalog subqueries touching user tables.
    add(
        "catalog_query",
        "Catalog/Subquery",
        "SELECT relname FROM pg_class WHERE relname IN (SELECT status FROM shipments)",
        "",
        TaskError::Denied(DenyRule::Subquery),
        Some("Native text 0.16 has no SQL catalog query form."),
    );
    // A multi-table join is refused before lookup, even for an absent second table.
    add(
        "join",
        "Other",
        "SELECT s.status FROM shipments s JOIN parity_other t ON s.status = t.status",
        "shipments s join parity_other t on s.status = t.status",
        NotAllowed(Class::Other),
        None,
    );
    add(
        "insert",
        "Insert",
        "INSERT INTO shipments (status) VALUES ('parity_value')",
        "insert shipments set status=\"parity_value\"",
        NotAllowed(Class::Insert),
        None,
    );
    add(
        "delete",
        "Delete",
        "DELETE FROM shipments WHERE attempt_count = 2",
        "delete shipments where attempt_count = 2",
        NotAllowed(Class::Delete),
        None,
    );
    add(
        "multi_step_block",
        "TransactionControl",
        "SELECT status FROM shipments; SELECT attempt_count FROM shipments",
        "transaction { shipments select status; shipments select attempt_count }",
        NotAllowed(Class::TransactionControl),
        None,
    );
    add(
        "write_without_operation_id",
        "Update",
        "UPDATE shipments SET status = 'parity_value' WHERE attempt_count = 2",
        "update shipments where attempt_count = 2 set status = \"parity_value\"",
        TaskError::OperationIdRequired,
        None,
    );
    // The execute endpoint's text lowerer excludes catalog DDL and joins. Use
    // their real structured native requests, rather than silently omitting them.
    for row in &mut rows {
        row.case.native = match row.name {
            "savepoint" => Native::Request(json!({"savepoint": {"name": "parity_point"}})),
            "ddl" => Native::Request(json!({"catalog": {"dropTable": {"table": "shipments"}}})),
            "join" => {
                let column = |source| json!({"column": {"source": source, "name": "status"}});
                Native::Request(json!({"typedRead": {"table": "shipments", "join": {
                    "table": "parity_other", "leftAlias": "s", "rightAlias": "t", "left": false,
                    "on": {"compare": {"left": column("s"), "comparison": "NATIVE_COMPARISON_EQ", "right": column("t")}}},
                    "projection": [{"expression": column("s"), "alias": "status"}]}}))
            }
            _ => row.case.native.clone(),
        };
    }
    rows.iter_mut()
        .find(|row| row.name == "catalog_query")
        .unwrap()
        .known_bug = Some(CATALOG_BUG);
    rows.iter_mut()
        .find(|row| row.name == "multi_step_block")
        .unwrap()
        .extended_na =
        Some("PostgreSQL extended Parse forbids multiple statements in one prepared statement.");
    rows
}

/// Bug found by R3-1002 (see the S15 report).
const CATALOG_BUG: &str = "R3-1002: application_schemas leaves out tables with a timestamptz column, so a catalog query with a subquery on shipments never reaches the D12 check and is refused with a plain 42501 instead of denied/Subquery";

async fn observe(f: &ParityFixture, row: &Row) -> Vec<(Surface, Observed)> {
    let mut observed = Vec::new();
    for surface in Surface::ALL {
        if let Some(reason) = row.na(surface) {
            assert!(
                !reason.trim().is_empty(),
                "{} {surface:?}: n/a needs a reason",
                row.name
            );
        } else {
            observed.push((surface, f.run(surface, &row.case).await));
        }
    }
    assert!(!observed.is_empty(), "{}: no applicable surfaces", row.name);
    observed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refusals_identical_on_every_surface() {
    let f = ParityFixture::start_with_credential().await;
    for row in rows().into_iter().filter(|row| row.known_bug.is_none()) {
        let observed = observe(&f, &row).await;
        for (surface, actual) in &observed {
            assert_eq!(
                actual.sqlstate.as_deref(),
                Some(row.expected_sqlstate),
                "{} {surface:?}: {actual:?}",
                row.name
            );
            assert_eq!(
                actual.task_error.as_deref(),
                row.expected.name(),
                "{} {surface:?}: {actual:?}",
                row.name
            );
            assert_eq!(
                actual.message.as_deref(),
                Some(row.expected.message()),
                "{} {surface:?}: {actual:?}",
                row.name
            );
            assert!(
                actual.columns.is_empty() && actual.rows.is_empty(),
                "{} {surface:?}: {actual:?}",
                row.name
            );
            assert_eq!(
                (actual.affected, &actual.transport),
                (None, &None),
                "{} {surface:?}: {actual:?}",
                row.name
            );
        }
        agree(&observed);
        assert_eq!(row.expected_sqlstate, row.expected.sqlstate());
    }
    f.stop().await;
}

fn table() -> Vec<Value> {
    rows().iter().map(|row| {
        let mut surfaces = serde_json::Map::new();
        for surface in Surface::ALL {
            let value = if let Some(reason) = row.na(surface) {
                assert!(!reason.trim().is_empty());
                json!({"n/a": reason})
            } else {
                match surface {
                    Surface::PgSimple | Surface::PgExtended => json!(row.case.sql),
                    _ => match &row.case.native {
                        Native::Text(text) => json!(text),
                        Native::Request(request) => request.clone(),
                    },
                }
            };
            surfaces.insert(format!("{surface:?}"), value);
        }
        json!({"case": row.name, "class": row.class, "known_bug": row.known_bug, "sqlstate": row.expected_sqlstate,
            "task_error": row.expected.name(), "message": row.expected.message(), "surfaces": surfaces})
    }).collect()
}

#[test]
fn denied_surface_table_is_current() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/r3/denied_surfaces.json");
    let mut expected = serde_json::to_vec_pretty(&table()).unwrap();
    expected.push(b'\n');
    if std::env::var("GIGGLES_BLESS_DENIED_SURFACES").as_deref() == Ok("1") {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &expected).unwrap();
    }
    let actual = std::fs::read(&path).unwrap_or_default();
    assert_eq!(
        actual, expected,
        "denied_surfaces.json is stale; regenerate with GIGGLES_BLESS_DENIED_SURFACES=1 cargo test --test all agent_parity_profile::denied_surface_table_is_current"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn message_never_contains_table_or_value_text() {
    let f = ParityFixture::start_with_credential().await;
    for row in rows() {
        for (surface, actual) in observe(&f, &row).await {
            let Some(message) = actual.message.as_deref() else {
                // A known-bug row has no task message; its plain error must
                // still not echo the statement.
                assert!(
                    row.known_bug.is_some(),
                    "{} {surface:?}: missing message: {actual:?}",
                    row.name
                );
                continue;
            };
            // Cases use status/attempt_count rather than the id column: the fixed
            // OperationIdRequired message uses the protocol term "operation id".
            // Check every SQL identifier and value used by the cases as tokens.
            let words: Vec<_> = message
                .split(|c: char| !c.is_alphanumeric() && c != '_')
                .collect();
            for forbidden in [
                "shipments",
                "merchant_id",
                "failed_at",
                "attempt_count",
                "customer_email",
                "card_token",
                "status",
                "relname",
                "pg_class",
                "parity_other",
                "parity_table",
                "parity_role",
                "parity_cursor",
                "parity_point",
                "parity_value",
                "2",
                "999",
            ] {
                assert!(
                    !words
                        .iter()
                        .any(|word| word.eq_ignore_ascii_case(forbidden)),
                    "{} {surface:?}: {message}",
                    row.name
                );
            }
            assert_eq!(message, row.expected.message(), "{} {surface:?}", row.name);
        }
    }
    f.stop().await;
}

/// Bug found by R3-1002: a same-table join is refused on every surface, but
/// not identically. SQL answers a plain 42501 that is not a task refusal,
/// while native answers `not_allowed` (0A000, class Other) as the two-table
/// join row does. Table 4(a) classifies every join as Other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "R3-1002: SQL self-join is refused with a generic 42501, native with NotAllowed(Other); see S15 report"]
async fn self_join_refusal_identical_on_every_surface() {
    let f = ParityFixture::start_with_credential().await;
    let column = |source| json!({"column": {"source": source, "name": "status"}});
    let mut case = Case::new(
        "SELECT s.status FROM shipments s JOIN shipments t ON s.status = t.status",
        "",
    );
    case.native = Native::Request(json!({"typedRead": {"table": "shipments", "join": {
        "table": "shipments", "leftAlias": "s", "rightAlias": "t", "left": false,
        "on": {"compare": {"left": column("s"), "comparison": "NATIVE_COMPARISON_EQ", "right": column("t")}}},
        "projection": [{"expression": column("s"), "alias": "status"}]}}));
    let expected = TaskError::NotAllowedInTaskSession(Class::Other);
    let observed = agree(&f.run_all(&case).await);
    assert_eq!(
        observed.sqlstate.as_deref(),
        Some(expected.sqlstate()),
        "{observed:?}"
    );
    assert_eq!(
        observed.task_error.as_deref(),
        expected.name(),
        "{observed:?}"
    );
    assert_eq!(
        observed.message.as_deref(),
        Some(expected.message()),
        "{observed:?}"
    );
    f.stop().await;
}

/// The rows marked `known_bug`, asserted as the design requires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "R3-1002: catalog subquery on a table with a timestamptz column misses the D12 check; see S15 report"]
async fn known_bug_rows_refused_as_designed() {
    let f = ParityFixture::start_with_credential().await;
    for row in rows().into_iter().filter(|row| row.known_bug.is_some()) {
        for (surface, actual) in observe(&f, &row).await {
            assert_eq!(
                actual.sqlstate.as_deref(),
                Some(row.expected_sqlstate),
                "{} {surface:?}",
                row.name
            );
            assert_eq!(
                actual.task_error.as_deref(),
                row.expected.name(),
                "{} {surface:?}: {actual:?}",
                row.name
            );
            assert_eq!(
                actual.message.as_deref(),
                Some(row.expected.message()),
                "{} {surface:?}",
                row.name
            );
        }
    }
    f.stop().await;
}
