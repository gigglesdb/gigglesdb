// Each file keeps its own support modules, exactly as when it was its own binary.
#![allow(clippy::duplicate_mod)]
#[path = "view_select_network.rs"]
mod view_select_network;
#[path = "comments_network.rs"]
mod comments_network;
#[path = "constraint_catalog_network.rs"]
mod constraint_catalog_network;


// Single integration-test binary: every tests/*.rs file is a module here, so the
// crate links once instead of once per file. Files that re-run the test binary
// by exact test name (current_exe) stay separate [[test]] targets in Cargo.toml.
#[path = "aggregate_avg_distinct_authority.rs"]
mod aggregate_avg_distinct_authority;
#[path = "aggregate_filter_authority.rs"]
mod aggregate_filter_authority;
#[path = "alter_table_ownership.rs"]
mod alter_table_ownership;
#[path = "ambiguous_column_error.rs"]
mod ambiguous_column_error;
#[path = "api_contract.rs"]
mod api_contract;
#[path = "api_native.rs"]
mod api_native;
#[path = "api_typed_read.rs"] mod api_typed_read;
#[path = "api_schema_names.rs"] mod api_schema_names;
#[path = "api_network.rs"]
mod api_network;
#[path = "api_process.rs"]
mod api_process;
#[path = "api_protocol.rs"]
mod api_protocol;
#[path = "clock_network.rs"]
mod clock_network;
#[path = "constraints_network.rs"]
mod constraints_network;
#[path = "copy_network.rs"]
mod copy_network;
#[path = "datagrip_session.rs"]
mod datagrip_session;
#[path = "declared_nonint_parameter_network.rs"]
mod declared_nonint_parameter_network;
#[path = "declared_parameter_network.rs"]
mod declared_parameter_network;
#[path = "deferred_routine_role_network.rs"]
mod deferred_routine_role_network;
#[path = "dev_process.rs"]
mod dev_process;
#[path = "drop_table_network.rs"]
mod drop_table_network;
#[path = "e2_varchar_char_network.rs"]
mod e2_varchar_char_network;
#[path = "greatest_least_authority.rs"]
mod greatest_least_authority;
#[path = "in_subquery_authority.rs"]
mod in_subquery_authority;
#[path = "multi_statement_network.rs"]
mod multi_statement_network;
#[path = "pg_dump_preamble_network.rs"]
mod pg_dump_preamble_network;
#[path = "postgresql_network.rs"]
mod postgresql_network;
#[path = "primary_key_update_network.rs"]
mod primary_key_update_network;
#[path = "psql_table_owner_network.rs"]
mod psql_table_owner_network;
#[path = "qualified_routine_network.rs"]
mod qualified_routine_network;
#[path = "role_sessions.rs"]
mod role_sessions;
#[path = "role_sql_network.rs"]
mod role_sql_network;
#[path = "savepoints_network.rs"]
mod savepoints_network;
#[path = "secondary_index_catalog_network.rs"]
mod secondary_index_catalog_network;
#[path = "sequence_authority_network.rs"]
mod sequence_authority_network;
#[path = "sequence_ddl_network.rs"]
mod sequence_ddl_network;
#[path = "session_identity_network.rs"]
mod session_identity_network;
#[path = "session_settings_network.rs"]
mod session_settings_network;
#[path = "session_zone_float_network.rs"]
mod session_zone_float_network;
#[path = "startup_params_network.rs"]
mod startup_params_network;
#[path = "timetz_bind_session_zone_network.rs"]
mod timetz_bind_session_zone_network;
#[path = "transaction_modes_network.rs"]
mod transaction_modes_network;
#[path = "upsert_authority_network.rs"]
mod upsert_authority_network;
#[path = "upsert_expressions_network.rs"]
mod upsert_expressions_network;
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
#[path = "npgsql_get_schema_network.rs"]
mod npgsql_get_schema_network;

#[path = "logical_columns.rs"]
mod logical_columns;
#[path = "mixed_catalog_app_query.rs"]
mod mixed_catalog_app_query;
#[path = "sql_cursors_network.rs"]
mod sql_cursors_network;
#[path = "transaction_isolation_reporting.rs"]
mod transaction_isolation_reporting;

#[path = "key_rotation.rs"]
mod key_rotation;

#[path = "per_schema_names.rs"]
mod per_schema_names;

#[path = "streaming_cursor_network.rs"]
mod streaming_cursor_network;

#[path = "bulk_wire_admission.rs"]
mod bulk_wire_admission;
#[path = "v2_ddl_authority.rs"]
mod v2_ddl_authority;
#[path = "v2_uuid.rs"]
mod v2_uuid;

#[path = "qualified_wildcard.rs"]
mod qualified_wildcard;

#[path = "ctid_wire.rs"]
mod ctid_wire;

#[path = "truncate_wire.rs"]
mod truncate_wire;

#[path = "exists_authority.rs"]
mod exists_authority;

#[path = "prepared_cap_network.rs"]
mod prepared_cap_network;

#[path = "derived_authority.rs"]
mod derived_authority;
#[path = "comment_presentation.rs"]
mod comment_presentation;
#[path = "view_ddl.rs"]
mod view_ddl;
#[path = "prepared_rollback_network.rs"]
mod prepared_rollback_network;
#[path = "view_presentation.rs"]
mod view_presentation;
#[path = "savepoint_reuse_network.rs"]
mod savepoint_reuse_network;

#[path = "many_params_network.rs"]
mod many_params_network;

#[path = "select_points_network.rs"] mod select_points_network;

#[path = "deadlock_network.rs"] mod deadlock_network;

#[path = "api_text.rs"]
mod api_text;

#[path = "write_fence_network.rs"]
mod write_fence_network;

#[path = "write_fence_termination_network.rs"]
mod write_fence_termination_network;

#[path = "privilege_functions_network.rs"]
mod privilege_functions_network;

#[path = "fk_section_network.rs"]
mod fk_section_network;
#[path = "api_search_path.rs"] mod api_search_path;

#[path = "api_savepoints.rs"] mod api_savepoints;
#[path = "api_catalog.rs"] mod api_catalog;

#[path = "api_catalog_auth.rs"] mod api_catalog_auth;
#[path = "api_catalog_defaults.rs"] mod api_catalog_defaults;

#[path = "api_catalog_objects.rs"] mod api_catalog_objects;
#[path = "api_catalog_views.rs"] mod api_catalog_views;
#[path = "api_paging.rs"] mod api_paging;
#[path = "api_sequences.rs"] mod api_sequences;
#[path = "replication_refuse.rs"] mod replication_refuse;

#[path = "replication_gate.rs"] mod replication_gate;
#[path = "replication_runtime.rs"] mod replication_runtime;
#[path = "replication_process.rs"] mod replication_process;
#[path = "replication_partition_process.rs"] mod replication_partition_process;

#[path = "replication_replace_process.rs"] mod replication_replace_process;
#[path = "replication_harness.rs"] pub(crate) mod replication_harness;
#[path = "replication_lost_reply.rs"] mod replication_lost_reply;
#[path = "replication_history.rs"] mod replication_history;

#[path = "xact_status_network.rs"] mod xact_status_network;
#[path = "isolation_anomalies.rs"]
mod isolation_anomalies;
#[path = "task_only_role_sql.rs"]
mod task_only_role_sql;

#[path = "task_only_roles.rs"]
mod task_only_roles;

#[path = "role_switch_guard.rs"]
mod role_switch_guard;

#[path = "agent_bindings.rs"]
mod agent_bindings;
