//! Immutable-reference inventory, not a claim that the proposed v1 is implemented.
//! SQL controls stay SQL; only native statement entries enter the native parser.

use giggles_core::{
    Admin, CoreError, Limits,
    relational::{SqlError, Value, with_sql_parser_forbidden},
};
use giggles_language::{Bindings, Engine, Error, parse};
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Corpus {
    format_version: u32,
    design_doc_id: String,
    scope: String,
    profile: String,
    documents: Vec<Document>,
    execution_fixtures: Vec<Fixture>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    alias: String,
    id: String,
    revision: u32,
    title: String,
    revision_url: String,
    expected_fence_count: usize,
    blocks: Vec<Block>,
    expected_inline_count: usize,
    inline_examples: Vec<InlineExample>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InlineExample {
    ordinal: usize,
    line: usize,
    section: String,
    source_line: String,
    example: Example,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Block {
    ordinal: usize,
    opening_line: usize,
    closing_line: usize,
    section: String,
    language: String,
    source: String,
    examples: Vec<Example>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Example {
    id: String,
    start_byte: usize,
    end_byte: usize,
    source: String,
    interface: String,
    kind: String,
    expectation: String,
    prepare_error: Option<String>,
    reason: String,
    features: Vec<String>,
    execution_fixture: Option<String>,
    execution: Option<Execution>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    id: String,
    scope: String,
    setup_sql: Vec<String>,
    #[serde(default)]
    denied_sql: Vec<DeniedSql>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeniedSql {
    source: String,
    error: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Execution {
    expectation: String,
    reason: String,
    #[serde(default)]
    bindings: Bindings,
    #[serde(default)]
    positional_bindings: Vec<Value>,
    error: Option<Error>,
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<Vec<Value>>,
    verify_sql: Option<SqlVerification>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SqlVerification {
    source: String,
    rows: Vec<Vec<Value>>,
}

fn corpus() -> Corpus {
    serde_json::from_str(include_str!("fixtures/reference_examples.json")).unwrap()
}
impl Corpus {
    fn examples(&self) -> impl Iterator<Item = &Example> {
        self.documents.iter().flat_map(|d| {
            d.blocks
                .iter()
                .flat_map(|b| &b.examples)
                .chain(d.inline_examples.iter().map(|inline| &inline.example))
        })
    }
    fn admin(&self, example: &Example) -> Admin {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        // Fixtures are retained; this test does not delete database artifacts.
        let path = std::env::temp_dir().join(format!(
            "giggles-reference-{}-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        let admin = Admin::create(path, Limits::default()).unwrap();
        if let Some(id) = &example.execution_fixture {
            let fixture = self
                .execution_fixtures
                .iter()
                .find(|f| &f.id == id)
                .unwrap();
            let mut sql = admin.sql_session();
            for statement in &fixture.setup_sql {
                sql.execute(statement, &[], None).unwrap();
            }
            for denied in &fixture.denied_sql {
                let error = sql.execute(&denied.source, &[], None).unwrap_err();
                assert_eq!(
                    error,
                    CoreError::Sql(sql_error(&denied.error)),
                    "{}",
                    denied.source
                );
            }
        }
        admin
    }
}
fn sql_error(name: &str) -> SqlError {
    match name {
        "Reserved" => SqlError::Reserved,
        "Syntax" => SqlError::Syntax,
        "Unsupported" => SqlError::Unsupported,
        _ => panic!("unclassified expected SQL error {name}"),
    }
}

#[test]
fn reference_inventory_is_complete_and_traceable() {
    let corpus = corpus();
    assert_eq!(corpus.format_version, 1);
    assert_eq!(corpus.design_doc_id, "81f3abec-f5f0-4909-9450-ed6767cb6916");
    assert!(!corpus.scope.is_empty());
    assert!(corpus.profile.contains("no proposed-v1"));
    // A changed reference is a deliberate corpus update, not an implicit bump.
    let pins = [
        ("tour", "22093487-1032-431b-aefa-dd2088bb2bec", 1, 10, 13),
        ("writes", "9252b8cc-42d0-4d54-8764-0d4ffeb9c72b", 1, 12, 17),
        ("schemas", "0f861b66-2cf4-4d24-8a99-2a6b46381836", 2, 9, 32),
        (
            "permissions",
            "a614da1c-c7e4-4668-9229-46102a9c6a14",
            3,
            6,
            22,
        ),
        ("sql", "e5dadca6-9f0f-409b-8184-94fb2ea6960c", 2, 1, 3),
        (
            "postgresql",
            "da366690-816c-481e-93a0-f6563c096f0a",
            2,
            6,
            6,
        ),
    ];
    assert_eq!(corpus.documents.len(), pins.len());
    let mut ids = BTreeSet::new();
    let mut fenced_entries = 0;
    let mut inline_entries = 0;
    for (document, (alias, id, revision, fences, examples)) in corpus.documents.iter().zip(pins) {
        assert_eq!(document.alias, alias);
        assert_eq!(document.id, id);
        assert_eq!(document.revision, revision);
        assert_eq!(document.expected_fence_count, fences);
        assert_eq!(document.blocks.len(), fences);
        assert_eq!(
            document
                .blocks
                .iter()
                .map(|b| b.examples.len())
                .sum::<usize>(),
            examples
        );
        assert!(!document.title.is_empty());
        assert!(
            document
                .revision_url
                .ends_with(&format!("{id}?revision={revision}"))
        );
        let mut previous_closing_line = 0;
        for (index, block) in document.blocks.iter().enumerate() {
            assert_eq!(block.ordinal, index + 1);
            assert!(block.opening_line > previous_closing_line);
            assert_eq!(
                block.closing_line - block.opening_line - 1,
                block.source.lines().count()
            );
            previous_closing_line = block.closing_line;
            assert!(!block.section.is_empty());
            assert!(!block.examples.is_empty());
            assert!(block.source.ends_with('\n'));
            let mut end = 0;
            for (index, example) in block.examples.iter().enumerate() {
                assert_eq!(
                    example.id,
                    format!("{alias}-fence{:02}-example{:02}", block.ordinal, index + 1)
                );
                assert!(ids.insert(&example.id), "duplicate {}", example.id);
                fenced_entries += 1;
                assert!(example.start_byte >= end);
                assert!(
                    block.source[end..example.start_byte].trim().is_empty(),
                    "unclassified source before {}",
                    example.id
                );
                assert_eq!(
                    block.source.get(example.start_byte..example.end_byte),
                    Some(example.source.as_str()),
                    "source drift in {}",
                    example.id
                );
                end = example.end_byte;
                assert!(!example.source.trim().is_empty());
                assert!(!example.reason.is_empty());
                assert!(!example.features.is_empty());
                match example.interface.as_str() {
                    "grammar" => {
                        assert_eq!(example.kind, "fragment");
                        assert_eq!(example.expectation, "non-executable");
                        assert_eq!(block.language, "text");
                    }
                    "native" => {
                        assert_eq!(example.kind, "statement");
                        assert!(matches!(
                            example.expectation.as_str(),
                            "parse-supported" | "parse-rejected"
                        ));
                        assert_eq!(block.language, "text");
                    }
                    "sql" => {
                        assert_eq!(example.kind, "statement");
                        assert!(matches!(
                            example.expectation.as_str(),
                            "prepare-supported" | "prepare-rejected"
                        ));
                        assert!(
                            block.language == "sql" || alias == "sql" && block.language.is_empty()
                        );
                    }
                    other => panic!("unclassified interface {other} in {}", example.id),
                }
                let executable = example.expectation == "parse-supported"
                    || example.expectation == "prepare-supported";
                assert_eq!(example.execution.is_some(), executable);
                assert_eq!(example.execution_fixture.is_some(), executable);
                assert_eq!(
                    example.prepare_error.is_some(),
                    example.expectation == "prepare-rejected"
                );
                if let Some(execution) = &example.execution {
                    assert!(!execution.reason.is_empty());
                    assert!(matches!(
                        execution.expectation.as_str(),
                        "execute-supported" | "execute-rejected"
                    ));
                    assert_eq!(
                        execution.error.is_some(),
                        execution.expectation == "execute-rejected"
                    );
                }
            }
            assert!(
                block.source[end..].trim().is_empty(),
                "unclassified block suffix"
            );
        }
        let expected_inline = usize::from(matches!(alias, "writes" | "postgresql"));
        assert_eq!(document.expected_inline_count, expected_inline);
        assert_eq!(document.inline_examples.len(), expected_inline);
        for (index, inline) in document.inline_examples.iter().enumerate() {
            let example = &inline.example;
            assert_eq!(inline.ordinal, index + 1);
            assert_eq!(example.id, format!("{alias}-inline{:02}", index + 1));
            assert!(ids.insert(&example.id), "duplicate {}", example.id);
            inline_entries += 1;
            assert_eq!(inline.line, if alias == "writes" { 233 } else { 74 });
            assert!(!inline.section.is_empty());
            assert!(
                document
                    .blocks
                    .iter()
                    .all(|block| inline.line < block.opening_line
                        || inline.line > block.closing_line)
            );
            assert_eq!(
                inline.source_line.get(example.start_byte..example.end_byte),
                Some(example.source.as_str()),
                "inline source drift in {}",
                example.id
            );
            assert!(!example.reason.is_empty());
            assert!(!example.features.is_empty());
            assert_eq!(example.kind, "statement");
            assert!(example.execution.is_none());
            assert!(example.execution_fixture.is_none());
            match example.interface.as_str() {
                "native" => {
                    assert_eq!(example.expectation, "parse-rejected");
                    assert!(example.prepare_error.is_none());
                }
                "sql" => {
                    assert_eq!(example.expectation, "prepare-rejected");
                    assert_eq!(example.prepare_error.as_deref(), Some("Reserved"));
                }
                other => panic!("unclassified inline interface {other}"),
            }
        }
    }
    assert_eq!(fenced_entries, 93);
    assert_eq!(inline_entries, 2);
    assert_eq!(ids.len(), 95);
    for (expectation, count) in [
        ("parse-supported", 4),
        ("parse-rejected", 64),
        ("prepare-supported", 1),
        ("prepare-rejected", 17),
        ("non-executable", 9),
    ] {
        assert_eq!(
            corpus
                .examples()
                .filter(|e| e.expectation == expectation)
                .count(),
            count,
            "{expectation}"
        );
    }
    for fixture in &corpus.execution_fixtures {
        assert!(!fixture.scope.is_empty());
        assert!(!fixture.setup_sql.is_empty());
    }
}

#[test]
fn native_reference_examples_match_parser_expectations() {
    let corpus = corpus();
    for example in corpus.examples().filter(|e| e.interface == "native") {
        let parsed = parse(&example.source);
        match example.expectation.as_str() {
            "parse-supported" => assert!(parsed.is_ok(), "{}: {parsed:?}", example.id),
            "parse-rejected" => assert!(
                parsed.is_err(),
                "{} unexpectedly parsed; update the reference classification before claiming support: {}",
                example.id,
                example.reason
            ),
            _ => unreachable!(),
        }
    }
}

#[test]
fn parsed_native_reference_examples_respect_execution_boundaries() {
    let corpus = corpus();
    for example in corpus
        .examples()
        .filter(|e| e.expectation == "parse-supported")
    {
        let admin = corpus.admin(example);
        let engine = Engine::new(admin.clone());
        let expected = example.execution.as_ref().unwrap();
        assert_eq!(expected.expectation, "execute-rejected");
        let verify = expected.verify_sql.as_ref().unwrap();
        let catalog_before = admin.catalog().unwrap();
        assert!(
            catalog_before
                .1
                .iter()
                .all(|table| table.name != "deliveries")
        );
        let before = admin
            .sql_session()
            .execute(&verify.source, &[], None)
            .unwrap();
        assert_eq!(
            before
                .rows
                .iter()
                .map(|r| r.values.clone())
                .collect::<Vec<_>>(),
            verify.rows
        );
        let outcome =
            with_sql_parser_forbidden(|| engine.execute_text(&example.source, &expected.bindings));
        assert_eq!(
            outcome,
            Err(expected.error.clone().unwrap()),
            "{}",
            example.id
        );
        let after = admin
            .sql_session()
            .execute(&verify.source, &[], None)
            .unwrap();
        assert_eq!(after, before, "{} changed rows or versions", example.id);
        assert_eq!(
            admin.catalog().unwrap(),
            catalog_before,
            "{} changed catalog",
            example.id
        );
    }
}

#[test]
fn sql_reference_examples_keep_the_sql_interface() {
    let corpus = corpus();
    for example in corpus.examples().filter(|e| e.interface == "sql") {
        let admin = corpus.admin(example);
        let mut sql = admin.sql_session();
        let prepared = sql.prepare(&example.source);
        match example.expectation.as_str() {
            "prepare-supported" => {
                let prepared = prepared.unwrap_or_else(|e| panic!("{}: {e}", example.id));
                let expected = example.execution.as_ref().unwrap();
                assert_eq!(expected.expectation, "execute-supported");
                let description = sql.describe(&prepared).unwrap();
                assert_eq!(
                    description
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect::<Vec<_>>(),
                    expected.columns,
                    "{}",
                    example.id
                );
                let result = sql
                    .execute_prepared(&prepared, &expected.positional_bindings, None)
                    .unwrap();
                assert_eq!(result.columns, expected.columns, "{}", example.id);
                assert_eq!(
                    result
                        .rows
                        .into_iter()
                        .map(|r| r.values)
                        .collect::<Vec<_>>(),
                    expected.rows,
                    "{}",
                    example.id
                );
            }
            "prepare-rejected" => assert_eq!(
                prepared.unwrap_err(),
                CoreError::Sql(sql_error(example.prepare_error.as_deref().unwrap())),
                "{}: {}",
                example.id,
                example.reason
            ),
            _ => unreachable!(),
        }
    }
}
