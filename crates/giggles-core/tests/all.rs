// Each file keeps its own `mod common;`, exactly as when it was its own binary.
#![allow(clippy::duplicate_mod)]
#[path = "hash_text.rs"]
mod hash_text;
#[path = "datetime_input_grammar.rs"]
mod datetime_input_grammar;
#[path = "write_fence_embedded.rs"]
mod write_fence_embedded;
#[path = "native_paging.rs"]
mod native_paging;
#[path = "alter_add_column_if_not_exists.rs"]
mod alter_add_column_if_not_exists;
#[path = "check_column_ddl.rs"]
mod check_column_ddl;
#[path = "generated_columns.rs"]
mod generated_columns;
#[path = "gin_indexes.rs"]
mod gin_indexes;
#[path = "oid_casts.rs"]
mod oid_casts;
#[path = "native_once_v2_replay.rs"] mod native_once_v2_replay;
#[path = "typed_leaf_read.rs"]
mod typed_leaf_read;
#[path = "view_select.rs"]
mod view_select;
#[path = "comments.rs"]
mod comments;
#[path = "pg_admin_utility.rs"]
mod pg_admin_utility;

// Single integration-test binary: every tests/*.rs file is a module here, so the
// crate links once instead of once per file. Files that re-run the test binary
// by exact test name (current_exe) stay separate [[test]] targets in Cargo.toml.
#[path = "ambiguous_column_error.rs"]
mod ambiguous_column_error;
#[path = "arithmetic.rs"]
mod arithmetic;
#[path = "binding_limits.rs"]
mod binding_limits;
#[path = "bound_operations.rs"]
mod bound_operations;
#[path = "cancellation.rs"]
mod cancellation;
#[path = "case_expressions.rs"]
mod case_expressions;
#[path = "catalog_operations.rs"]
mod catalog_operations;
#[path = "computed_expressions.rs"]
mod computed_expressions;
#[path = "constraint_restrict_error.rs"]
mod constraint_restrict_error;
#[path = "copy_ingest.rs"]
mod copy_ingest;
#[path = "differential.rs"]
mod differential;
#[path = "drop_index_options.rs"]
mod drop_index_options;
#[path = "e2_assignment_numeric_char.rs"]
mod e2_assignment_numeric_char;
#[path = "e2_known_gap_bpchar.rs"]
mod e2_known_gap_bpchar;
#[path = "e2_string_truncation_error.rs"]
mod e2_string_truncation_error;
#[path = "e2_varchar_char_oracle.rs"]
mod e2_varchar_char_oracle;
#[path = "export.rs"]
mod export;
#[path = "float_text_input.rs"]
mod float_text_input;
#[path = "heap_native_operations.rs"]
mod heap_native_operations;
#[path = "insert_many.rs"]
mod insert_many;
#[path = "inspection.rs"]
mod inspection;
#[path = "limit_regression.rs"]
mod limit_regression;
#[path = "once_schema.rs"]
mod once_schema;
#[path = "operations.rs"]
mod operations;
#[path = "owner_key_hygiene.rs"]
mod owner_key_hygiene;
#[path = "predicate_unknown_literals.rs"]
mod predicate_unknown_literals;
#[path = "public_unique.rs"]
mod public_unique;
#[path = "unique_reporting.rs"]
mod unique_reporting;
#[path = "quoted_routines.rs"]
mod quoted_routines;
#[path = "read_explain.rs"]
mod read_explain;
#[path = "relational.rs"]
mod relational;
#[path = "relational_aggregate_filter.rs"]
mod relational_aggregate_filter;
#[path = "relational_aggregates_avg_distinct.rs"]
mod relational_aggregates_avg_distinct;
#[path = "relational_grouping.rs"]
mod relational_grouping;
#[path = "relational_heap.rs"]
mod relational_heap;
#[path = "relational_in_subquery.rs"]
mod relational_in_subquery;
#[path = "relational_in_subquery_contexts.rs"]
mod relational_in_subquery_contexts;
#[path = "relational_joins.rs"]
mod relational_joins;
#[path = "relational_like.rs"]
mod relational_like;
#[path = "relational_like_errors.rs"]
mod relational_like_errors;
#[path = "relational_queries.rs"]
mod relational_queries;
#[path = "relational_query_process.rs"]
mod relational_query_process;
#[path = "relational_scan_budget.rs"]
mod relational_scan_budget;
#[path = "relational_scans.rs"]
mod relational_scans;
#[path = "relational_searched_dml.rs"]
mod relational_searched_dml;
#[path = "returning.rs"]
mod returning;
#[path = "row_partition.rs"]
mod row_partition;
#[path = "scalar_functions.rs"]
mod scalar_functions;
#[path = "server_boundary.rs"]
mod server_boundary;
#[path = "sql_aggregate_error_codes.rs"]
mod sql_aggregate_error_codes;
#[path = "sql_aggregate_scalars.rs"]
mod sql_aggregate_scalars;
#[path = "sql_array_agg.rs"]
mod sql_array_agg;
#[path = "sql_alter_table.rs"]
mod sql_alter_table;
#[path = "sql_binary_functions.rs"]
mod sql_binary_functions;
#[path = "sql_bytea_convert.rs"]
mod sql_bytea_convert;
#[path = "sql_boolean_expressions.rs"]
mod sql_boolean_expressions;
#[path = "sql_bytea_input.rs"]
mod sql_bytea_input;
#[path = "sql_bytea_input_error_codes.rs"]
mod sql_bytea_input_error_codes;
#[path = "sql_cast_type_names.rs"]
mod sql_cast_type_names;
#[path = "sql_character_adversarial.rs"]
mod sql_character_adversarial;
#[path = "sql_character_functions.rs"]
mod sql_character_functions;
#[path = "sql_clock.rs"]
mod sql_clock;
#[path = "presentation_settings.rs"]
mod presentation_settings;
#[path = "sql_constraints.rs"]
mod sql_constraints;
#[path = "sql_copy_literal_parity.rs"]
mod sql_copy_literal_parity;
#[path = "sql_count_parameter_inference.rs"]
mod sql_count_parameter_inference;
#[path = "sql_cross_type_joins.rs"]
mod sql_cross_type_joins;
#[path = "sql_declared_parameter_assignment.rs"]
mod sql_declared_parameter_assignment;
#[path = "sql_declared_parameter_delete.rs"]
mod sql_declared_parameter_delete;
#[path = "sql_declared_parameter_insert.rs"]
mod sql_declared_parameter_insert;
#[path = "sql_declared_parameter_numeric.rs"]
mod sql_declared_parameter_numeric;
#[path = "sql_declared_parameter_types.rs"]
mod sql_declared_parameter_types;
#[path = "sql_declared_parameter_update.rs"]
mod sql_declared_parameter_update;
#[path = "sql_defaults_identity.rs"]
mod sql_defaults_identity;
#[path = "sql_degree_math.rs"]
mod sql_degree_math;
#[path = "sql_describe_parity.rs"]
mod sql_describe_parity;
#[path = "sql_e1_error_codes.rs"]
mod sql_e1_error_codes;
#[path = "sql_f17_error_codes.rs"]
mod sql_f17_error_codes;
#[path = "sql_f18b_error_codes.rs"]
mod sql_f18b_error_codes;
#[path = "sql_filter_planning.rs"]
mod sql_filter_planning;
#[path = "sql_format.rs"]
mod sql_format;
#[path = "sql_functions.rs"]
mod sql_functions;
#[path = "sql_greatest_least.rs"]
mod sql_greatest_least;
#[path = "sql_identifiers.rs"]
mod sql_identifiers;
#[path = "sql_immutable_planning.rs"]
mod sql_immutable_planning;
#[path = "sql_infix_arithmetic.rs"]
mod sql_infix_arithmetic;
#[path = "sql_insert_function_probe.rs"]
mod sql_insert_function_probe;
#[path = "sql_integer_cast_controls.rs"]
mod sql_integer_cast_controls;
#[path = "sql_integer_cast_oracle.rs"]
mod sql_integer_cast_oracle;
#[path = "sql_integer_formats.rs"]
mod sql_integer_formats;
#[path = "sql_integer_input_error_codes.rs"]
mod sql_integer_input_error_codes;
#[path = "sql_math_functions.rs"]
mod sql_math_functions;
#[path = "sql_md5_functions.rs"]
mod sql_md5_functions;
#[path = "sql_multiword_temporal_types.rs"]
mod sql_multiword_temporal_types;
#[path = "sql_no_from_aggregates.rs"]
mod sql_no_from_aggregates;
#[path = "sql_nonlinear_functions.rs"]
mod sql_nonlinear_functions;
#[path = "sql_numeric.rs"]
mod sql_numeric;
#[path = "sql_numeric_functions.rs"]
mod sql_numeric_functions;
#[path = "sql_pk_update.rs"]
mod sql_pk_update;
#[path = "sql_position_functions.rs"]
mod sql_position_functions;
#[path = "sql_prepared_filter_coercion.rs"]
mod sql_prepared_filter_coercion;
#[path = "sql_order_select_items.rs"]
mod sql_order_select_items;
#[path = "sql_prepared_function_contract.rs"]
mod sql_prepared_function_contract;
#[path = "sql_qualified_routines.rs"]
mod sql_qualified_routines;
#[path = "sql_quoted_column_identifiers.rs"]
mod sql_quoted_column_identifiers;
#[path = "sql_quoting_functions.rs"]
mod sql_quoting_functions;
#[path = "sql_returning.rs"]
mod sql_returning;
#[path = "sql_routing_rejection_classifier.rs"]
mod sql_routing_rejection_classifier;
#[path = "sql_savepoints.rs"]
mod sql_savepoints;
#[path = "sql_scalar_boundaries.rs"]
mod sql_scalar_boundaries;
#[path = "sql_scalar_compatibility.rs"]
mod sql_scalar_compatibility;
#[path = "sql_sequences.rs"]
mod sql_sequences;
#[path = "sql_session_zone_float_text.rs"]
mod sql_session_zone_float_text;
#[path = "sql_substring_order.rs"]
mod sql_substring_order;
#[path = "sql_temporal_arithmetic.rs"]
mod sql_temporal_arithmetic;
#[path = "sql_temporal_cast_matrix.rs"]
mod sql_temporal_cast_matrix;
#[path = "sql_temporal_functions.rs"]
mod sql_temporal_functions;
#[path = "sql_temporal_inference.rs"]
mod sql_temporal_inference;
#[path = "sql_temporal_isfinite.rs"]
mod sql_temporal_isfinite;
#[path = "sql_temporal_overloads.rs"]
mod sql_temporal_overloads;
#[path = "sql_temporal_storage.rs"]
mod sql_temporal_storage;
#[path = "sql_text_functions.rs"]
mod sql_text_functions;
#[path = "sql_time_zone_displacement.rs"]
mod sql_time_zone_displacement;
#[path = "sql_time_zone_word.rs"]
mod sql_time_zone_word;
#[path = "sql_trim_syntax.rs"]
mod sql_trim_syntax;
#[path = "sql_undefined_routine_codes.rs"]
mod sql_undefined_routine_codes;
#[path = "sql_unique_per_row.rs"]
mod sql_unique_per_row;
#[path = "sql_unknown_literal_assignment.rs"]
mod sql_unknown_literal_assignment;
#[path = "sql_update_expressions.rs"]
mod sql_update_expressions;
#[path = "sql_upsert_expressions.rs"]
mod sql_upsert_expressions;
#[path = "table_free_classification.rs"]
mod table_free_classification;
#[path = "temporal_constructors.rs"]
mod temporal_constructors;
#[path = "temporal_retained.rs"]
mod temporal_retained;
#[path = "transactions.rs"]
mod transactions;
#[path = "upgrade.rs"]
mod upgrade;
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
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("utf-8 name");
        if stem == "all" || stem == "common" {
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
#[path = "sql_text_order.rs"]
mod sql_text_order;

#[path = "numeric_column_typmod.rs"]
mod numeric_column_typmod;

#[path = "logical_columns.rs"]
mod logical_columns;
#[path = "relational_insert_select.rs"]
mod relational_insert_select;
#[path = "sql_insert_select_literals.rs"]
mod sql_insert_select_literals;
#[path = "sql_insert_select_sources.rs"]
mod sql_insert_select_sources;
#[path = "user_schemas.rs"]
mod user_schemas;

#[path = "user_defined_types.rs"]
mod user_defined_types;

#[path = "corruption_detection.rs"]
mod corruption_detection;
#[path = "per_schema_names.rs"]
mod per_schema_names;

#[path = "streaming_cursor.rs"]
mod streaming_cursor;

#[path = "r1_compat.rs"]
mod r1_compat;

#[path = "creation_defaults.rs"]
mod creation_defaults;

#[path = "v2_sql_fk_lock_tests.rs"]
mod v2_sql_fk_lock_tests;

#[path = "domain_cast_cascade.rs"]
mod domain_cast_cascade;

#[path = "key_bounds_offset.rs"]
mod key_bounds_offset;

#[path = "select_distinct.rs"]
mod select_distinct;

#[path = "qualified_wildcard.rs"]
mod qualified_wildcard;

#[path = "ctid.rs"]
mod ctid;

#[path = "check_schema.rs"]
mod check_schema;

#[path = "upsert_arbiter.rs"]
mod upsert_arbiter;

#[path = "truncate.rs"]
mod truncate;

#[path = "exists_public.rs"]
mod exists_public;

#[path = "exists_probe.rs"]
mod exists_probe;

#[path = "bare_alias.rs"]
mod bare_alias;

#[path = "orm_identity_metadata.rs"]
mod orm_identity_metadata;

#[path = "orm_casts.rs"]
mod orm_casts;

#[path = "orm_rows.rs"]
mod orm_rows;

#[path = "null_key_predicate.rs"]
mod null_key_predicate;

#[path = "temporal_column_precision.rs"]
mod temporal_column_precision;

#[path = "derived_orm.rs"]
mod derived_orm;
#[path = "view_ddl.rs"]
mod view_ddl;

#[path = "many_params.rs"]
mod many_params;
#[path = "typed_aggregate_read.rs"]
mod typed_aggregate_read;

#[path = "typed_join_read.rs"]
mod typed_join_read;

#[path = "authorization_reads.rs"]
mod authorization_reads;
#[path = "native_schema_resolution.rs"]
mod native_schema_resolution;
#[path = "native_typed_params.rs"]
mod native_typed_params;
#[path = "native_savepoints.rs"] mod native_savepoints;
#[path = "typed_subquery_read.rs"] mod typed_subquery_read;
#[path = "typed_insert_select.rs"] mod typed_insert_select;
#[path = "alter_drop_column_views.rs"]
mod alter_drop_column_views;

#[path = "native_alter_table.rs"]
mod native_alter_table;
#[path = "native_constraints.rs"] mod native_constraints;
#[path = "parity_legacy_populated_ddl.rs"] mod parity_legacy_populated_ddl;
#[path = "native_views.rs"]
mod native_views;
#[path = "parity_view_reads.rs"]
mod parity_view_reads;

#[path = "qualified_dml_targets.rs"] mod qualified_dml_targets;

#[path = "missing_schema_ddl.rs"] mod missing_schema_ddl;
#[path = "having_parameters.rs"] mod having_parameters;

#[path = "having_subqueries.rs"] mod having_subqueries;

#[path = "join_on_subqueries.rs"] mod join_on_subqueries;

#[path = "typed_read_qualified.rs"] mod typed_read_qualified;
#[path = "native_numeric_assignment.rs"] mod native_numeric_assignment;

#[path = "native_create_defaults.rs"] mod native_create_defaults;

#[path = "native_sequences.rs"] mod native_sequences;
#[path = "native_error_splits.rs"] mod native_error_splits;
#[path = "fk_target_codec.rs"]
mod fk_target_codec;

#[path = "fk_pk_birth.rs"] mod fk_pk_birth;

#[path = "fk_create_candidates.rs"] mod fk_create_candidates;

#[path = "fk_index_binding.rs"] mod fk_index_binding;

#[path = "fk_drop_index.rs"] mod fk_drop_index;

#[path = "fk_drop_constraint.rs"] mod fk_drop_constraint;

#[path = "fk_catalog_target.rs"] mod fk_catalog_target;

#[path = "fk_drop_durability.rs"] mod fk_drop_durability;

#[path = "fk_integration_main.rs"] mod fk_integration_main;
#[path = "native_once_v2_format.rs"]
mod native_once_v2_format;

#[path = "native_once_v2_execute.rs"] mod native_once_v2_execute;
#[path = "native_once_v2_terminal.rs"] mod native_once_v2_terminal;

#[path = "native_catalog_session.rs"]
mod native_catalog_session;
#[path = "parity_insert_duplicate_column.rs"]
mod parity_insert_duplicate_column;

#[path = "parity_update_eval_order.rs"] mod parity_update_eval_order;

#[path = "parity_sequence_schema.rs"] mod parity_sequence_schema;

#[path = "parity_native_check.rs"] mod parity_native_check;
#[path = "isolation_levels.rs"]
mod isolation_levels;

#[path = "task_dependencies.rs"] mod task_dependencies;
#[path = "task_labels.rs"] mod task_labels;
#[path = "v2_sum_avg_unbounded.rs"]
mod v2_sum_avg_unbounded;
#[path = "v2_returning_spill.rs"]
mod v2_returning_spill;
#[path = "v2_sort_spill.rs"]
mod v2_sort_spill;
#[path = "v2_group_spill.rs"]
mod v2_group_spill;
#[path = "v2_in_spill.rs"]
mod v2_in_spill;
#[path = "v2_per_value_limits.rs"]
mod v2_per_value_limits;
#[path = "v2_insert_select_spill.rs"]
mod v2_insert_select_spill;
#[path = "v2_insert_select_view_selfref.rs"]
mod v2_insert_select_view_selfref;
#[path = "pgparity_ordering.rs"] mod pgparity_ordering;

#[path = "pgparity_concat.rs"]
mod pgparity_concat;
#[path = "pgparity_typeof.rs"]
mod pgparity_typeof;
#[path = "task_rows.rs"] mod task_rows;
#[path = "task_columns_native.rs"] mod task_columns_native;
#[path = "task_release.rs"] mod task_release;
#[path = "task_readlog.rs"] mod task_readlog;
#[path = "task_protected_commit.rs"] mod task_protected_commit;
#[path = "task_prepare_rules.rs"] mod task_prepare_rules;
#[path = "replica_role_fk.rs"] mod replica_role_fk;
#[path = "insert_trailing_columns.rs"]
mod insert_trailing_columns;
#[path = "aggregate_cast_parity.rs"] mod aggregate_cast_parity;

#[path = "v2_on_conflict_caps.rs"]
mod v2_on_conflict_caps;
#[path = "task_approval.rs"] mod task_approval;
#[path = "task_preview.rs"] mod task_preview;
#[path = "task_apply.rs"] mod task_apply;
#[path = "task_legacy_ports.rs"] mod task_legacy_ports;
#[path = "task_undo.rs"] mod task_undo;
#[path = "task_limits.rs"] mod task_limits;
#[path = "task_exposure.rs"] mod task_exposure;
#[path = "task_statements.rs"] mod task_statements;
#[path = "expression_collation.rs"]
mod expression_collation;

#[path = "user_collation_ddl.rs"]
mod user_collation_ddl;

#[path = "column_collation_ddl.rs"] mod column_collation_ddl;
#[path = "column_collation_query.rs"] mod column_collation_query;
#[path = "sql_nondeterministic_lookup.rs"]
mod sql_nondeterministic_lookup;
#[path = "column_collation_index.rs"] mod column_collation_index;

#[path = "nondeterministic_unique.rs"] mod nondeterministic_unique;
#[path = "nondeterministic_foreign_key.rs"] mod nondeterministic_foreign_key;
#[path = "aggregate_collation_planning.rs"] mod aggregate_collation_planning;

#[path = "window_collation.rs"] mod window_collation;

#[path = "dml_collation.rs"]
mod dml_collation;

#[path = "paging_collation.rs"]
mod paging_collation;

#[path = "planner_collations.rs"]
mod planner_collations;

#[path = "sql_float_glibc.rs"]
mod sql_float_glibc;
#[path = "sql_extensions.rs"]
mod sql_extensions;
#[path = "sql_volatile_uuid.rs"]
mod sql_volatile_uuid;
#[path = "temporal_secondary_indexes.rs"]
mod temporal_secondary_indexes;

#[path = "sql_array_ordering.rs"]
mod sql_array_ordering;
#[path = "sql_array_validation.rs"]
mod sql_array_validation;
#[path = "sql_uuid_array.rs"]
mod sql_uuid_array;

#[path = "sql_numeric_domain.rs"]
mod sql_numeric_domain;

#[path = "lock_release_sessions.rs"]
mod lock_release_sessions;
#[path = "fk_row_locks.rs"]
mod fk_row_locks;
#[path = "sql_row_locks.rs"]
mod sql_row_locks;

#[path = "default_parse_cache.rs"]
mod default_parse_cache;

#[path = "catalog_read_fast_paths.rs"]
mod catalog_read_fast_paths;

#[path = "pgparity_scalars_w06.rs"]
mod pgparity_scalars_w06;

#[path = "sql_catalog_scale.rs"]
mod sql_catalog_scale;

#[path = "truncated_clauses.rs"]
mod truncated_clauses;

#[path = "ddl_scale_probe.rs"] mod ddl_scale_probe;

#[path = "partial_indexes.rs"]
mod partial_indexes;

#[path = "sql_dml_sources.rs"]
mod sql_dml_sources;

#[cfg(feature = "engine-rocksdb")]
#[path = "sql_large_insert_select.rs"]
mod sql_large_insert_select;

#[path = "sql_union_chain.rs"]
mod sql_union_chain;

#[path = "window_grouping_sets.rs"]
mod window_grouping_sets;
#[path = "unbounded_kv_reads.rs"]
mod unbounded_kv_reads;

#[path = "enum_array_defaults.rs"]
mod enum_array_defaults;

#[path = "enum_dml_overlap.rs"]
mod enum_dml_overlap;

#[path = "text_search.rs"]
mod text_search;

#[path = "sql_regex.rs"]
mod sql_regex;

#[path = "sql_union_resolution.rs"]
mod sql_union_resolution;

#[path = "sql_triggers_ddl.rs"]
mod sql_triggers_ddl;
#[cfg(feature = "engine-rocksdb")]
#[path = "sql_triggers_admission.rs"]
mod sql_triggers_admission;
#[cfg(feature = "engine-rocksdb")]
#[path = "sql_triggers_fire.rs"]
mod sql_triggers_fire;

#[path = "enum_array_check.rs"]
mod enum_array_check;

#[path = "sql_wide_tables.rs"]
mod sql_wide_tables;

#[path = "sql_named_arguments.rs"]
mod sql_named_arguments;

#[path = "sql_maintenance.rs"] mod sql_maintenance;

#[path = "sql_storage_options.rs"] mod sql_storage_options;
