//! GD-0042 SQL caller inventory. Cross-package entries are source/evidence links,
//! not assertions that this test executes those packages or qualifies a release.

use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{self, SqlError, SqlResult, Value},
};
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    format_version: u32,
    design_doc_id: String,
    scope: String,
    non_claims: Vec<String>,
    interfaces: Vec<Interface>,
    limits: Vec<Limit>,
    core_sql_cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Interface {
    id: String,
    state: String,
    sql_support: String,
    endpoint: String,
    authority: String,
    implemented: Vec<String>,
    restrictions: Vec<String>,
    unverified: Vec<String>,
    limit_ids: Vec<String>,
    docs: Vec<String>,
    sources: Vec<Source>,
    evidence: Vec<Evidence>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    path: String,
    needle: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Evidence {
    path: String,
    test: String,
    scope: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Limit {
    id: String,
    value: usize,
    unit: String,
    source: Source,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    sql: String,
    params: Vec<Value>,
    rows: Vec<Vec<Value>>,
    error: Option<SqlError>,
    reason: String,
}

fn inventory() -> Inventory {
    serde_json::from_str(include_str!("fixtures/sql_interfaces.json")).unwrap()
}

fn nonempty(values: &[String]) {
    assert!(!values.is_empty());
    assert!(values.iter().all(|v| !v.trim().is_empty()));
}

#[test]
fn sql_interface_inventory_keeps_authority_and_support_distinct() {
    let inventory = inventory();
    assert_eq!(inventory.format_version, 1);
    assert_eq!(
        inventory.design_doc_id,
        "81f3abec-f5f0-4909-9450-ed6767cb6916"
    );
    assert!(inventory.scope.contains("not a release attestation"));
    assert!(inventory.scope.contains("PostgreSQL 18"));
    nonempty(&inventory.non_claims);
    let limits: BTreeSet<_> = inventory.limits.iter().map(|l| l.id.as_str()).collect();
    assert_eq!(limits.len(), inventory.limits.len());
    let mut actual = BTreeSet::new();
    for interface in &inventory.interfaces {
        assert!(actual.insert((interface.id.as_str(), interface.sql_support.as_str())));
        assert!(matches!(
            interface.state.as_str(),
            "implemented" | "restricted" | "unverified"
        ));
        assert!(!interface.endpoint.trim().is_empty());
        assert!(!interface.authority.trim().is_empty());
        nonempty(&interface.implemented);
        nonempty(&interface.restrictions);
        nonempty(&interface.unverified);
        nonempty(&interface.docs);
        nonempty(&interface.limit_ids);
        assert!(!interface.sources.is_empty());
        assert!(!interface.evidence.is_empty());
        for limit in &interface.limit_ids {
            assert!(limits.contains(limit.as_str()), "unknown limit: {limit}");
        }
    }
    // In particular, the SDK and conformance harness cannot become SQL support
    // merely because they transport finite typed repair requests.
    let expected = BTreeSet::from([
        ("core_admin_relational", "relational"),
        ("core_executor_repair", "finite_repair"),
        ("local_json_admin", "relational"),
        ("local_json_executor", "finite_repair"),
        ("ffi_admin_session", "relational"),
        ("ffi_executor", "finite_repair"),
        ("giggles_sql_cli", "relational"),
        ("server_native_application", "relational"),
        ("server_pgwire_application", "relational"),
        ("server_task_repair", "finite_repair"),
        ("server_task_prepared", "finite_repair"),
        ("server_bundled_client", "transport_only"),
        ("consensus_sdk", "none"),
        ("client_conformance", "none"),
    ]);
    assert_eq!(actual, expected);
    let ids: BTreeSet<_> = inventory.interfaces.iter().map(|i| &i.id).collect();
    assert_eq!(ids.len(), inventory.interfaces.len());
    assert_eq!(inventory.core_sql_cases.len(), 8);
    let mut cases = BTreeSet::new();
    for case in &inventory.core_sql_cases {
        assert!(cases.insert(&case.id));
        assert!(!case.sql.trim().is_empty());
        assert!(!case.reason.trim().is_empty());
        if case.error.is_some() {
            assert!(case.rows.is_empty());
        }
    }
}

fn repository_file(relative: &str) -> PathBuf {
    let relative = Path::new(relative);
    assert!(
        relative
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
    );
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap()
        .canonicalize()
        .unwrap();
    let path = root.join(relative).canonicalize().unwrap();
    assert!(path.starts_with(&root) && path.is_file());
    path
}

fn source_exists(source: &Source) {
    assert!(!source.needle.trim().is_empty());
    let text = std::fs::read_to_string(repository_file(&source.path)).unwrap();
    assert!(
        text.contains(&source.needle),
        "missing anchor {:?} in {}",
        source.needle,
        source.path
    );
}

#[test]
fn sql_interface_sources_docs_and_named_test_evidence_exist() {
    let inventory = inventory();
    for interface in &inventory.interfaces {
        for doc in &interface.docs {
            assert_eq!(repository_file(doc).extension().unwrap(), "md");
        }
        for source in &interface.sources {
            source_exists(source);
        }
        for evidence in &interface.evidence {
            assert!(!evidence.scope.trim().is_empty());
            let text = std::fs::read_to_string(repository_file(&evidence.path)).unwrap();
            let declaration = format!("fn {}(", evidence.test);
            let position = text.find(&declaration).unwrap_or_else(|| {
                panic!("missing named test {} in {}", evidence.test, evidence.path)
            });
            // All references deliberately name actual test functions, including
            // functions in .inc files included by the server integration suite.
            let preceding: String = text[..position]
                .lines()
                .rev()
                .take(5)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                preceding.contains("#[test]") || preceding.contains("#[tokio::test"),
                "{} in {} is not annotated as a test",
                evidence.test,
                evidence.path
            );
        }
    }
    for limit in &inventory.limits {
        assert!(limit.value > 0);
        assert!(!limit.unit.trim().is_empty());
        source_exists(&limit.source);
    }
}

#[test]
fn sql_interface_inventory_matches_public_core_limits() {
    let inventory = inventory();
    for (id, expected) in [
        ("sql_bytes", relational::MAX_SQL),
        ("sql_rows", relational::MAX_ROWS),
        ("sql_value_bytes", relational::MAX_ROW_BYTES),
        ("sql_columns", relational::MAX_COLUMNS),
        ("sql_params", relational::MAX_PARAMS),
        ("sql_tables", relational::MAX_TABLES),
        ("sql_result_bytes", relational::MAX_RESULT_BYTES),
        ("sql_transactions", relational::MAX_TRANSACTIONS),
        ("local_input_bytes", giggles_core::local::MAX_INPUT),
        ("local_output_bytes", giggles_core::local::MAX_OUTPUT),
    ] {
        let limit = inventory
            .limits
            .iter()
            .find(|limit| limit.id == id)
            .unwrap();
        assert_eq!(limit.value, expected, "{id}");
    }
}

fn fixture() -> (Admin, SqlSession) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    // Database fixtures are retained. No test deletes shared agent artifacts.
    let path = std::env::temp_dir().join(format!(
        "giggles-sql-interface-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed),
    ));
    let admin = Admin::create(path, Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    for statement in [
        "CREATE TABLE app (id BIGINT PRIMARY KEY, title TEXT, active BOOLEAN NOT NULL)",
        "INSERT INTO app VALUES(1,'first',true)",
        "INSERT INTO app VALUES(2,'second',false)",
    ] {
        sql.execute(statement, &[], None).unwrap();
    }
    (admin, sql)
}

fn all_rows(sql: &mut SqlSession) -> SqlResult {
    sql.execute("SELECT * FROM app ORDER BY id LIMIT 1024", &[], None)
        .unwrap()
}

#[test]
fn core_sql_contract_corpus_preserves_values_versions_and_rejections() {
    let inventory = inventory();
    let (admin, mut sql) = fixture();
    let catalog = admin.catalog().unwrap();
    let baseline = all_rows(&mut sql);
    for case in inventory.core_sql_cases {
        let result = sql.execute(&case.sql, &case.params, None);
        match case.error {
            Some(error) => assert_eq!(result, Err(CoreError::Sql(error)), "{}", case.id),
            None => {
                let result = result.unwrap_or_else(|e| panic!("{}: {e:?}", case.id));
                assert_eq!(result.affected, 0, "{}", case.id);
                let values: Vec<_> = result.rows.into_iter().map(|row| row.values).collect();
                assert_eq!(values, case.rows, "{}", case.id);
            }
        }
        // Reads and rejected writes preserve rows, versions and catalog.
        assert_eq!(all_rows(&mut sql), baseline, "{}", case.id);
        assert_eq!(admin.catalog().unwrap(), catalog, "{}", case.id);
        assert!(!sql.in_transaction());
    }
}

#[test]
fn core_sql_prepared_cas_and_join_authorization_target_remain_distinct() {
    let (_, mut sql) = fixture();
    let point = sql.prepare("SELECT title FROM app WHERE id=$1").unwrap();
    let before = sql
        .execute_prepared(&point, &[Value::Int(1)], None)
        .unwrap();
    let version = before.rows[0].version;
    // SQL's successful equal-value update advances its version; native no-op
    // semantics do not silently change this existing SQL caller contract.
    assert_eq!(
        sql.execute(
            "UPDATE app SET title=$1 WHERE id=$2",
            &[Value::Text("first".into()), Value::Int(1)],
            Some(version),
        )
        .unwrap()
        .affected,
        1
    );
    let after = sql
        .execute_prepared(&point, &[Value::Int(1)], None)
        .unwrap();
    assert_eq!(after.rows[0].values, before.rows[0].values);
    assert!(after.rows[0].version > version);
    assert_eq!(
        sql.execute("DELETE FROM app WHERE id=1", &[], Some(version)),
        Err(CoreError::Sql(SqlError::Version))
    );
    assert_eq!(
        sql.execute_prepared(&point, &[Value::Int(1)], None)
            .unwrap(),
        after
    );

    sql.execute("CREATE TABLE other (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let join = sql
        .prepare("SELECT a.id FROM app a JOIN other b ON b.id=a.id LIMIT 2")
        .unwrap();
    assert_eq!(join.table(), Some("giggles_join"));
    // This checks the core authorization descriptor, not a simulated gateway.
    // Actual remote role behavior is evidenced by the separate server tests.
}
