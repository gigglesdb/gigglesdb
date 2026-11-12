#[path = "constraint_metadata.rs"]
mod constraint_metadata;
// Single integration-test binary: every tests/*.rs file is a module here, so the
// crate links once instead of once per file. Files that re-run the test binary
// by exact test name (current_exe) stay separate [[test]] targets in Cargo.toml.
#[path = "catalog_absent_display.rs"]
mod catalog_absent_display;
#[path = "catalog_array_expansion.rs"]
mod catalog_array_expansion;
#[path = "catalog_description_state.rs"]
mod catalog_description_state;
#[path = "catalog_in_subquery.rs"]
mod catalog_in_subquery;
#[path = "catalog_join_selection.rs"]
mod catalog_join_selection;
#[path = "catalog_left_join_selection.rs"]
mod catalog_left_join_selection;
#[path = "catalog_natural_join.rs"]
mod catalog_natural_join;
#[path = "catalog_nominal_binding.rs"]
mod catalog_nominal_binding;
#[path = "catalog_resolution_scopes.rs"]
mod catalog_resolution_scopes;
#[path = "catalog_roles.rs"]
mod catalog_roles;
#[path = "catalog_scale_pushdown.rs"]
mod catalog_scale_pushdown;
#[path = "catalog_trigger_display.rs"]
mod catalog_trigger_display;
#[path = "catalog_type_acl.rs"]
mod catalog_type_acl;
#[path = "datagrip_batch.rs"]
mod datagrip_batch;
#[path = "datagrip_catalog_descriptors.rs"]
mod datagrip_catalog_descriptors;
#[path = "datagrip_static_catalog.rs"]
mod datagrip_static_catalog;
#[path = "datagrip_text_functions.rs"]
mod datagrip_text_functions;
#[path = "information_schema.rs"]
mod information_schema;
#[path = "metadata.rs"]
mod metadata;
#[path = "npgsql_startup.rs"]
mod npgsql_startup;
#[path = "numeric_column_typmod.rs"]
mod numeric_column_typmod;
#[path = "pg18_type_rows.rs"]
mod pg18_type_rows;
#[path = "psql_describe.rs"]
mod psql_describe;
#[path = "psql_functions.rs"]
mod psql_functions;
#[path = "query_boundaries.rs"]
mod query_boundaries;
#[path = "recovery_state.rs"]
mod recovery_state;
#[path = "secondary_indexes.rs"]
mod secondary_indexes;
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
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("utf-8 name");
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
#[path = "npgsql_get_schema.rs"]
mod npgsql_get_schema;

#[path = "pg_dump_catalog.rs"]
mod pg_dump_catalog;

#[path = "compat_metadata.rs"]
mod compat_metadata;

#[path = "logical_column_types.rs"]
mod logical_column_types;
#[path = "user_defined_types.rs"]
mod user_defined_types;

#[path = "datagrip_refresh.rs"]
mod datagrip_refresh;

#[path = "orm_regtype.rs"]
mod orm_regtype;
#[path = "gui_browse.rs"]
mod gui_browse;
