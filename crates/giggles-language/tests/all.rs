// Single integration-test binary: every tests/*.rs file is a module here, so the
// crate links once instead of once per file. Files that re-run the test binary
// by exact test name (current_exe) stay separate [[test]] targets in Cargo.toml.
#[path = "arithmetic.rs"]
mod arithmetic;
#[path = "binding_limits.rs"]
mod binding_limits;
#[path = "bound_operations.rs"]
mod bound_operations;
#[path = "capabilities.rs"]
mod capabilities;
#[path = "case_expressions.rs"]
mod case_expressions;
#[path = "catalog_operations.rs"]
mod catalog_operations;
#[path = "computed_expressions.rs"]
mod computed_expressions;
#[path = "conformance.rs"]
mod conformance;
#[path = "engine_adapter.rs"]
mod engine_adapter;
#[path = "explain.rs"]
mod explain;
#[path = "insert_many.rs"]
mod insert_many;
#[path = "inspection.rs"]
mod inspection;
#[path = "inspection_capabilities.rs"]
mod inspection_capabilities;
#[path = "native_alter_parity.rs"]
mod native_alter_parity;
#[path = "native_block_query_parity.rs"] mod native_block_query_parity;
#[path = "native_bound_query_parity.rs"] mod native_bound_query_parity;
#[path = "native_column_defaults21_parity.rs"] mod native_column_defaults21_parity;
#[path = "native_constraint_parity.rs"]
mod native_constraint_parity;
#[path = "native_defaults_parity.rs"]
mod native_defaults_parity;
#[path = "native_drop_parity.rs"]
mod native_drop_parity;
#[path = "native_paging_parity.rs"]
mod native_paging_parity;
#[path = "native_parity.rs"]
mod native_parity;
#[path = "native_qualified_query_parity.rs"] mod native_qualified_query_parity;
#[path = "native_recovery21_parity.rs"] mod native_recovery21_parity;
#[path = "native_schema_parity.rs"]
mod native_schema_parity;
#[path = "native_sqlstate_parity.rs"]
mod native_sqlstate_parity;
#[path = "native_view_parity.rs"]
mod native_view_parity;
#[path = "native_view_read_parity.rs"]
mod native_view_read_parity;
#[path = "once.rs"]
mod once;
#[path = "once_capabilities.rs"]
mod once_capabilities;
#[path = "once_parser.rs"]
mod once_parser;
#[path = "parser.rs"]
mod parser;
#[path = "reference_examples.rs"]
mod reference_examples;
#[path = "returning.rs"]
mod returning;
#[path = "scalar_functions.rs"]
mod scalar_functions;
#[path = "sql_interfaces.rs"]
mod sql_interfaces;
#[path = "transactions.rs"]
mod transactions;
#[path = "upsert.rs"]
mod upsert;
/// With `autotests = false`, a new `tests/*.rs` file is silently skipped unless it
/// is registered. Fail instead: every file must be a module above or a [[test]].
#[test]
fn every_test_file_is_registered() {
    let all = include_str!("all.rs");
    let manifest = include_str!("../Cargo.toml");
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut missing = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("read tests dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let stem = path.file_stem().and_then(|s| s.to_str()).expect("utf-8 name");
        if stem == "all" {
            continue;
        }
        let as_module = all.contains(&format!("mod {stem};"));
        let as_target = manifest.contains(&format!("path = \"tests/{stem}.rs\""));
        if !as_module && !as_target {
            missing.push(stem.to_owned());
        }
    }
    assert!(
        missing.is_empty(),
        "tests/*.rs files not registered in tests/all.rs or Cargo.toml [[test]]: {missing:?}"
    );
}

#[path = "native_typed_params_parity.rs"] mod native_typed_params_parity;

#[path = "native_savepoint_parity.rs"] mod native_savepoint_parity;
#[path = "native_savepoint_recovery_parity.rs"] mod native_savepoint_recovery_parity;
#[path = "native_sequence21_parity.rs"] mod native_sequence21_parity;

#[path = "typed_read_parser.rs"] mod typed_read_parser;
#[path = "native_typed_read_parity.rs"] mod native_typed_read_parity;

#[path = "native_subquery_parity.rs"] mod native_subquery_parity;
#[path = "fk_native_drop.rs"] mod fk_native_drop;
