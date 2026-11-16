//! Versioned inventory of the finite embedded profile, separate from `profile()`.
//!
//! Evidence names identify executable repository checks. They are not a claim
//! that a particular release, platform, or deployment passed those checks.

use crate::{
    LanguageVersion, MAX_BINDING_BYTES, MAX_BINDINGS, MAX_BOUND_VALUE_BYTES, MAX_DEPTH,
    MAX_EXPRESSION_NODES, MAX_INPUT, MAX_MUTATION_BYTES, MAX_ROWS, MAX_TOKENS, profile_for,
};
use giggles_core::operations::once as core_once;
use giggles_core::relational;
use serde::Serialize;

const DESIGN_DOC_ID: &str = "81f3abec-f5f0-4909-9450-ed6767cb6916";
const CAPABILITY_TESTS: &str = "crates/giggles-language/tests/capabilities.rs";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityInterface {
    RustText,
    RustTree,
    Cli,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentProfile {
    TrustedEmbedded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityState {
    /// Implemented in this finite profile, with named executable checks.
    Implemented,
    /// An executable rejection case exists for this interface.
    Rejected,
    /// No qualification is claimed for this profile and interface.
    Unverified,
}

#[derive(Clone, Debug, Serialize)]
pub struct TestEvidence {
    /// Repository-relative source path.
    pub path: &'static str,
    pub test: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReferenceRevision {
    pub name: &'static str,
    pub supporting_doc_id: &'static str,
    pub revision: u32,
    pub url: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct Capability {
    /// Together with profile, interface, deployment and feature, the entry key.
    pub language_version: &'static str,
    pub profile: &'static str,
    pub interface: CapabilityInterface,
    pub deployment: DeploymentProfile,
    pub feature: &'static str,
    pub state: CapabilityState,
    pub detail: &'static str,
    pub evidence: Vec<TestEvidence>,
    /// Complete native requests exercised by the named rejection test.
    /// Tree-only restrictions use hand-built trees in their evidence instead.
    pub rejection_examples: Vec<&'static str>,
}

/// Admission bounds, not performance estimates or wall-clock/work guarantees.
#[derive(Clone, Debug, Serialize)]
pub struct CapabilityLimits {
    pub text_bytes: usize,
    pub serialized_tree_bytes: usize,
    pub text_tokens: usize,
    pub expression_nodes: usize,
    pub expression_depth: usize,
    pub named_bindings: usize,
    /// Supplied values counted once each: Text/Bytes length; scalars/NULL 9.
    pub binding_payload_bytes: usize,
    /// Explicit resolved values, counting every literal/parameter occurrence.
    /// This is not a total heap or transport-decoding bound.
    pub bound_value_bytes: usize,
    pub default_page_rows: usize,
    pub result_page_rows: usize,
    /// The finite native engine requires a complete table within this bound.
    pub native_table_rows: usize,
    pub row_bytes: usize,
    /// Applies to the full core scan before native filtering and pagination.
    pub scan_result_bytes: usize,
    /// Aggregate selected postimages, including no-ops; stored rows also obey row_bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub computed_mutation_bytes: Option<usize>,
    /// All eligible projected values before paging; no additional per-output-row bound.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub computed_projection_bytes: Option<usize>,
    /// Complete explicit write result, charged before effects are staged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_result_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_items: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_locals: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emit_values: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retained_local_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emit_bytes: Option<usize>,
    /// Typed tuple admission; text is independently bounded by text_tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insert_input_rows: Option<usize>,
    /// Complete proposed stored rows before any INSERT MANY effects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insert_proposed_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_primary_key_columns: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_arguments: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub case_arms: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quoted_identifier_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_target_columns: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_input_outcomes: Option<usize>,
    /// The following once limits apply to the explicit trusted Rust APIs only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub once_key_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub once_namespace_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub once_canonical_intent_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub once_encoded_result_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub once_retained_keys: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub once_ledger_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub once_min_retention_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub once_max_retention_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub once_max_clock_lease_seconds: Option<u64>,
    /// Core's logical catalog-inspection metadata metric, not wire encoding size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inspection_metadata_bytes: Option<usize>,
    pub columns_per_table: usize,
    pub catalog_tables: usize,
    pub concurrent_explicit_sessions: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct CapabilityManifest {
    pub manifest_version: u32,
    pub language_version: &'static str,
    pub package_version: &'static str,
    pub profile: &'static str,
    pub design_doc_id: &'static str,
    pub references: Vec<ReferenceRevision>,
    pub limits: CapabilityLimits,
    pub capabilities: Vec<Capability>,
}

/// Inventory schema version 1 for the finite 0.1 language, not proposed native v1.
///
/// The existing `profile()` and CLI `capabilities` output remain unchanged.
/// Unsupported syntax is listed for text/CLI; the Rust tree exposes only its
/// finite public enum variants, so those syntax forms have no tree encoding.
pub fn capability_manifest() -> CapabilityManifest {
    capability_manifest_for(LanguageVersion::Embedded01)
}

/// Inventory for an explicitly selected finite language/profile.
pub fn capability_manifest_for(version: LanguageVersion) -> CapabilityManifest {
    let profile = profile_for(version);
    let mut capabilities = Vec::new();
    let mut add = |interface, feature, state, detail, evidence, rejection_examples| {
        capabilities.push(Capability {
            language_version: version.as_str(),
            profile: profile.name,
            interface,
            deployment: DeploymentProfile::TrustedEmbedded,
            feature,
            state,
            detail,
            evidence,
            rejection_examples,
        });
    };

    if matches!(version, LanguageVersion::Embedded015 | LanguageVersion::Embedded016 | LanguageVersion::Embedded017 | LanguageVersion::Embedded018 | LanguageVersion::Embedded019 | LanguageVersion::Embedded020) {
        add(
            CapabilityInterface::RustText,
            "numeric_parameters",
            CapabilityState::Implemented,
            "Numeric parameters for existing SQL-provisioned NUMERIC columns; shared core typmod coercion. No numeric literals, arithmetic or functions.",
            vec![evidence(
                "crates/giggles-language/tests/native_parity.rs",
                "numeric_parameters_match_sql",
            )],
            vec![],
        );
    }

    if matches!(version, LanguageVersion::Embedded016 | LanguageVersion::Embedded017 | LanguageVersion::Embedded018 | LanguageVersion::Embedded019 | LanguageVersion::Embedded020) {
        for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree, CapabilityInterface::Cli] {
            add(interface, "numeric_column_declaration", CapabilityState::Implemented,
                "Legacy native CREATE TABLE: unconstrained NUMERIC, required or nullable. No typmod; v2 catalog writes remain refused.",
                vec![evidence("crates/giggles-language/tests/native_parity.rs", "numeric_native_catalog_matches_sql")], vec![]);
        }
    }

    if version.supports_schema_names() {
        for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree, CapabilityInterface::Cli] {
            add(interface, "schema_qualified_tables", CapabilityState::Implemented,
                "Canonical schema.table names in reads, writes, blocks and table/index DDL; bare tables use the default public search path. Schema creation remains SQL-only.",
                vec![evidence("crates/giggles-language/tests/native_schema_parity.rs", "schema_dml_catalog_and_reopen_match_sql")], vec![]);
        }
    }

    if version.supports_r2_wave2() {
        for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree] {
            add(interface, "keyset_paging", CapabilityState::Implemented,
                "page READ [after (key values)] reads one page in an explicit order ending in the full primary key and returns the exclusive continuation key. Page size is the read's limit (default 100, max 1024). Bad order or key arity is Error::Bind; every page is a full scan.",
                vec![evidence("crates/giggles-language/tests/native_paging_parity.rs", "paging_round_trips_match_sql")], vec![]);
        }
        for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree, CapabilityInterface::Cli] {
            add(interface, "catalog_constraints", CapabilityState::Implemented,
                "ALTER TABLE ADD [CONSTRAINT name] UNIQUE/CHECK/FOREIGN KEY (with ON DELETE/ON UPDATE actions) and DROP CONSTRAINT [IF EXISTS] name through SQL's DDL executor. CHECK is a column compared with an int, text, bool or NULL literal. Legacy requires empty business state. DROP CONSTRAINT CASCADE currently returns SQL's 42601.",
                vec![evidence("crates/giggles-language/tests/native_constraint_parity.rs", "constraint_matrix_matches_sql")], vec![]);
            add(interface, "catalog_views", CapabilityState::Implemented,
                "CREATE VIEW name AS <typed read> and DROP VIEW [IF EXISTS] name [CASCADE|RESTRICT] through SQL's view executor. RESTRICT is the default; dependent views give 2BP01 unless CASCADE. Legacy returns SQL's 0A000; two-source views return SQL's 0A000.",
                vec![evidence("crates/giggles-language/tests/native_view_parity.rs", "view_matrix_matches_sql")], vec![]);
        }
    }

    if version.supports_r2_native() {
        for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree, CapabilityInterface::Cli] {
            for (feature, test, detail) in [
                ("subquery_predicates", "subquery_predicates_match_sql", "IN/NOT IN/EXISTS/NOT EXISTS with uncorrelated single-source subqueries and SQL NULL semantics; standalone queries only. IN/EXISTS inside JOIN ON or HAVING matches SQL."),
                ("insert_select", "insert_select_matches_sql", "Standalone INSERT FROM typed queries with SQL defaults, constraints and affected-row counts; omitted columns target all logical columns.") ] {
                add(interface, feature, CapabilityState::Implemented,
                    if version.supports_r2_wave2() && feature == "insert_select" { "INSERT FROM typed queries with SQL defaults, constraints and affected-row counts; omitted columns target all logical columns. Supported in blocks and frozen prepared handles in 0.20." } else { detail },
                    vec![evidence("crates/giggles-language/tests/native_subquery_parity.rs", test)], vec![]);
            }
        }
        for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree, CapabilityInterface::Cli] {
            add(interface, "typed_reads", CapabilityState::Implemented,
                if version.supports_r2_wave2() { "Aggregates, GROUP BY/HAVING, ORDER BY/LIMIT and INNER/LEFT joins with aliases and SQL semantics. Schema-qualified sources work; select required. 0.20 supports blocks, frozen prepared handles and nonexecuting explain_query; once remains unsupported." } else { "Aggregates (count/sum/min/max/avg), GROUP BY/HAVING, ORDER BY/LIMIT and two-source INNER/LEFT joins with aliases, executed as core typed reads with SQL semantics. Bare table names only; select is required; standalone statements only (not in blocks, bound handles, explain or once)." },
                vec![evidence("crates/giggles-language/tests/native_typed_read_parity.rs", "typed_reads_match_sql")], vec![]);
        }
        for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree, CapabilityInterface::Cli] {
            add(interface, "block_savepoints", CapabilityState::Implemented,
                "SAVEPOINT, ROLLBACK TO and RELEASE between plain block statements on one retained session; a failing step still rolls back the whole block. Blocks with savepoints admit no let/require/emit and cannot be prepared, lowered or used as once bodies.",
                vec![evidence("crates/giggles-language/tests/native_savepoint_parity.rs", "savepoint_blocks_match_sql")], vec![]);
        }
        for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree, CapabilityInterface::Cli] {
            add(interface, "typed_parameters", CapabilityState::Implemented,
                "Float4/Float8/date/time/timetz/timestamp/timestamptz/interval parameters in assignments and predicates; uuid/jsonb as text parameters use SQL coercion. Numeric stays assignment-only.",
                vec![evidence("crates/giggles-language/tests/native_typed_params_parity.rs", "typed_parameters_match_sql")], vec![]);
        }
        for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree, CapabilityInterface::Cli] {
            add(interface, "catalog_alter", CapabilityState::Implemented,
                "ALTER TABLE ADD/DROP/RENAME COLUMN and RENAME TO through SQL's ALTER executor. DROP COLUMN defaults to RESTRICT; dependent views give 2BP01 unless CASCADE. Legacy requires empty business state; v2 permits populated state.",
                vec![evidence("crates/giggles-language/tests/native_alter_parity.rs", "alter_matrix_matches_sql")], vec![]);
        }
    }

    if matches!(version, LanguageVersion::Embedded017 | LanguageVersion::Embedded018 | LanguageVersion::Embedded019 | LanguageVersion::Embedded020) {
        for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree, CapabilityInterface::Cli] {
            add(interface, "catalog_drop", CapabilityState::Implemented,
                "Standalone DROP TABLE/INDEX with default RESTRICT or explicit CASCADE/RESTRICT. Shared SQL dependencies and errors; DROP INDEX CASCADE removes FKs bound to a standalone index; legacy Unknown bindings remain Unsupported. Legacy requires empty business state; v2 permits populated state.",
                vec![evidence("crates/giggles-language/tests/native_drop_parity.rs", "drop_matrix_matches_sql")], vec![]);
        }
    }

    for interface in [
        CapabilityInterface::RustText,
        CapabilityInterface::RustTree,
        CapabilityInterface::Cli,
    ] {
        if version.supports_inspection() {
            add(
                interface,
                "local_discovery",
                CapabilityState::Implemented,
                "Explicit 0.12: SHOW CAPABILITIES returns the selected finite native capability manifest; SHOW LIMITS returns its admission limits. These are library-profile facts, not database/server authorization or performance claims. The CLI resolves both before opening the supplied path, including missing or locked paths; inspection accepts no bindings.",
                vec![
                    evidence(
                        "crates/giggles-language/tests/inspection_capabilities.rs",
                        "inspection_cli_local_discovery_precedes_database_open",
                    ),
                    evidence(
                        "crates/giggles-language/tests/inspection.rs",
                        "inspection_catalog_text_and_tree_match_without_sql_or_effects",
                    ),
                ],
                vec![],
            );
            add(
                interface,
                "catalog_inspection",
                CapabilityState::Implemented,
                "Explicit 0.12 trusted embedded catalog inspection: SHOW TABLES returns complete table summaries (stable ID/name, column count and primary-key column count); DESCRIBE TABLE returns the exact descriptor, including SQL SmallInt. Each catalog result carries core contract version, database domain and catalog epoch from one fresh committed view; JSON encodes the u128 domain as a canonical decimal string. No data rows, row counts, indexes, EXPLAIN, prepared inspection or policy authority. Logical metadata is admitted against 65536 bytes before cloning; this is not a network-encoding bound.",
                vec![
                    evidence(
                        "crates/giggles-language/tests/inspection.rs",
                        "inspection_catalog_text_and_tree_match_without_sql_or_effects",
                    ),
                    evidence(
                        "crates/giggles-language/tests/inspection_capabilities.rs",
                        "inspection_cli_catalog_and_version_selection_are_explicit",
                    ),
                ],
                vec![],
            );
        }
        if version.supports_explain() {
            add(
                interface,
                "bounded_simple_read_static_explain",
                CapabilityState::Implemented,
                "Explicit 0.13 trusted simple-read static explain using actual bindings and fresh catalog IDs. Whole-state storage read/validation still occurs; no logical business read, mutation or commit. Describes complete materialization (1024 ceiling), conditional reverse-PK certification, in-memory filter/order/projection/page; estimates unknown. No indexed optimization or task authorization.",
                vec![evidence(
                    "crates/giggles-language/tests/explain.rs",
                    "explain_text_tree_bindings_and_fresh_catalog",
                )],
                vec![],
            );
            for feature in [
                "full_reference_explain",
                "local_core_bundle",
                "index_creation",
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Unverified,
                    "Not qualified by bounded simple-read explain.",
                    vec![],
                    vec![],
                );
            }
        }
        if version != LanguageVersion::Embedded01 {
            add(
                interface,
                "integer_predicate_arithmetic",
                CapabilityState::Implemented,
                if version == LanguageVersion::Embedded02 {
                    "Explicit 0.2 only: checked signed-i64 unary minus and + - * / % in WHERE/WHEN/claim predicates. Integer division truncates toward zero, remainder follows the dividend sign; NULL and eager Boolean evaluation are preserved. No computed projections, SET, ordering, limits or caps."
                } else {
                    "Checked signed-i64 unary minus and + - * / % in predicates and the separately listed computed assignments/projections. Integer division truncates toward zero, remainder follows the dividend sign; NULL and eager Boolean evaluation are preserved. No arithmetic ordering, limits or caps."
                },
                vec![
                    evidence(
                        "crates/giggles-language/tests/arithmetic.rs",
                        "arithmetic_version_gates_cover_text_trees_and_bound_handles",
                    ),
                    evidence(
                        "crates/giggles-language/tests/arithmetic.rs",
                        "arithmetic_text_tree_and_prepared_execution_match_without_sql",
                    ),
                    evidence(
                        "crates/giggles-language/tests/arithmetic.rs",
                        "arithmetic_cli_version_selection_is_explicit_and_reported",
                    ),
                ],
                vec![],
            );
        }
        if version.supports_computed() {
            add(
                interface,
                "computed_assignments",
                CapabilityState::Implemented,
                "Explicit 0.3 or later: update/claim expressions read every RHS from the old row, then validate the complete postimage before atomic writes. Stored integer ranges/nullability, cap/guard order, claim consumption and no-op versions remain enforced. All selected postimages, including no-ops, share a 1 MiB payload bound; each stored row stays within 64 KiB.",
                vec![
                    evidence(
                        "crates/giggles-language/tests/computed_expressions.rs",
                        "computed_version_gates_preserve_legacy_statement_shapes",
                    ),
                    evidence(
                        "crates/giggles-language/tests/computed_expressions.rs",
                        "computed_native_text_tree_and_bound_requests_match_without_sql",
                    ),
                    evidence(
                        "crates/giggles-core/tests/computed_expressions.rs",
                        "computed_postimage_byte_budgets_cover_exact_bounds_and_noops",
                    ),
                ],
                vec![],
            );
            add(
                interface,
                "computed_projections",
                CapabilityState::Implemented,
                "Explicit 0.3 or later: single-table expressions require an alias unless they are field references; output names are unique. Field outputs retain catalog identity/type, computed outputs use ID 0, and untyped NULL outputs are rejected. All eligible output values are evaluated and charged against 1 MiB before paging, even with limit 0; there is no separate 64 KiB output-row bound.",
                vec![
                    evidence(
                        "crates/giggles-language/tests/computed_expressions.rs",
                        "computed_projection_metadata_and_hidden_page_errors_match",
                    ),
                    evidence(
                        "crates/giggles-language/tests/computed_expressions.rs",
                        "computed_cli_selects_version_three_explicitly",
                    ),
                    evidence(
                        "crates/giggles-core/tests/computed_expressions.rs",
                        "computed_projection_errors_and_bytes_cover_all_eligible_rows_before_paging",
                    ),
                ],
                vec![],
            );
        }
        if version.supports_return() {
            add(
                interface,
                "native_returning",
                CapabilityState::Implemented,
                "Explicit 0.4 or later: one write may return a projection or star. DELETE returns preimages; INSERT/update/claim return postimages, including matched no-ops. Bulk results have primary-key order and actual stored row versions. Expressions and the complete 1 MiB result budget are checked before staging. RETURN alone supplies no receipts, result paging or automatic retries.",
                vec![
                    evidence(
                        "crates/giggles-language/tests/returning.rs",
                        "returning_version_gates_and_contextual_keyword_preserve_legacy",
                    ),
                    evidence(
                        "crates/giggles-language/tests/returning.rs",
                        "returning_text_tree_and_prepared_paths_share_images_and_versions",
                    ),
                    evidence(
                        "crates/giggles-language/tests/returning.rs",
                        "returning_output_errors_and_limits_abort_writes",
                    ),
                    evidence(
                        "crates/giggles-language/tests/returning.rs",
                        "returning_cli_uses_explicit_version_four",
                    ),
                ],
                vec![],
            );
        }
        if version.supports_transactions() {
            for (feature, detail, test) in [
                (
                    "transaction_blocks",
                    "Explicit 0.5 or later: one snapshot, upfront binding, staged visibility and one final write commit; read-only blocks publish no generation. Separate transaction APIs preserve statement outcomes. No nesting, claim, DDL, per-item once or recovery; the ordinary block APIs supply no receipts.",
                    "transaction_text_tree_and_bound_execution_share_atomic_results",
                ),
                (
                    "typed_transaction_locals",
                    "Immutable typed local slots; singular RETURN writes expose row fields/row, writes expose changed/status success, reads expose materialized count/rows with completeness. Require accepts only true; scalar emit metadata represents untyped NULL explicitly.",
                    "transaction_local_shapes_and_upfront_binding_reject_invalid_blocks",
                ),
                (
                    "insert_values_expressions",
                    "Explicit 0.5 or later: one column-list VALUES tuple with bound scalar expressions; no target-row field scope. INSERT SET remains literal/parameter only.",
                    "transaction_version_gates_and_contextual_keywords_preserve_legacy",
                ),
                (
                    "guarded_bulk_writes",
                    "Explicit 0.5 or later: update/delete many check the complete cap first, then every guard before assignment evaluation and staging. A failed guard aborts all effects; claim guards remain unsupported.",
                    "transaction_version_gates_and_contextual_keywords_preserve_legacy",
                ),
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Implemented,
                    detail,
                    vec![evidence(
                        "crates/giggles-language/tests/transactions.rs",
                        test,
                    )],
                    vec![],
                );
            }
        }
        if version.supports_insert_many() {
            add(
                interface,
                "insert_many_values",
                CapabilityState::Implemented,
                "Explicit 0.6: capped VALUES tuples bind together with RETURN, then execution checks input count before local materialization, scans or RHS evaluation. All proposed rows, duplicate primary-key targets, existing-key conflicts, final table capacity and complete RETURN output are checked before staging. Empty typed row lists bind columns/cap/output but insert nothing; text requires a tuple. No source-fed inserts or conflict actions.",
                vec![
                    evidence(
                        "crates/giggles-language/tests/insert_many.rs",
                        "insert_many_version_gates_and_empty_tree_contract",
                    ),
                    evidence(
                        "crates/giggles-language/tests/insert_many.rs",
                        "insert_many_text_tree_and_bound_execution_match_without_sql",
                    ),
                    evidence(
                        "crates/giggles-language/tests/insert_many.rs",
                        "insert_many_caps_duplicates_and_returning_are_atomic",
                    ),
                    evidence(
                        "crates/giggles-language/tests/insert_many.rs",
                        "insert_many_cli_and_selected_manifest_match_version_six",
                    ),
                ],
                vec![],
            );
        }
        if version.supports_catalog() {
            add(
                interface,
                "native_create_table",
                CapabilityState::Implemented,
                "Explicit 0.7: standalone safe CREATE TABLE uses the common typed catalog path, with required-by-default bool/int32/int64/text/bytes and an explicit table-level primary key. Nullable primary-key columns are rejected. Execution requires empty business state, returns the committed catalog epoch and assigned table/column IDs, and returns PlanRequired on populated state. No schema namespaces, defaults, secondary constraints/indexes, partitioning or migration jobs.",
                vec![
                    evidence(
                        "crates/giggles-language/tests/catalog_operations.rs",
                        "catalog_version_gates_and_required_defaults_are_explicit",
                    ),
                    evidence(
                        "crates/giggles-language/tests/catalog_operations.rs",
                        "catalog_text_tree_and_bound_execution_match_without_sql",
                    ),
                    evidence(
                        "crates/giggles-language/tests/catalog_operations.rs",
                        "catalog_rejections_and_populated_policy_are_atomic",
                    ),
                    evidence(
                        "crates/giggles-language/tests/catalog_operations.rs",
                        "catalog_cli_empty_init_and_selected_manifest_match_version_seven",
                    ),
                ],
                vec![],
            );
            if interface != CapabilityInterface::Cli {
                add(
                    interface,
                    "immutable_catalog_preparation",
                    CapabilityState::Implemented,
                    "Separate immutable catalog handles retain an ID-free proposed definition, database domain, catalog epoch and exact native version/profile. Preparation binds without allocating IDs or deciding population policy; execution rechecks identity and fresh business-state emptiness. No serialization, credentials, retries or receipts.",
                    vec![evidence(
                        "crates/giggles-language/tests/catalog_operations.rs",
                        "catalog_text_tree_and_bound_execution_match_without_sql",
                    )],
                    vec![],
                );
            }
        }
        if version.supports_upsert() {
            for (feature, detail) in [
                (
                    "primary_key_upserts",
                    "Explicit 0.10: column-list VALUES inserts support ON CONFLICT over exactly the existing full primary-key column set, including reordered targets. DO NOTHING skips a winner; DO UPDATE requires every winner guard before any update RHS and applies simultaneous existing/incoming expressions. Every incoming tuple validates, caps count skipped inputs, and duplicate proposal identities abort atomically. No secondary unique keys or SQL grammar expansion.",
                ),
                (
                    "conflict_tuple_results",
                    "Conflict results preserve input order with Success/Skipped, changed and an optional index into PK-ordered RETURN rows. Skipped tuples return no row; no-op updates return the winner with its unchanged version. Optional conflict_results is absent from non-conflict results and Some([]) for an empty typed MANY upsert. matched counts inputs, affected counts non-skipped successes, and changed counts actual changes.",
                ),
                (
                    "conflict_locals_and_scope",
                    "Existing/Incoming fields bind only in conflict guard/SET; bare row fields there are rejected, while RETURN sees the final target. Singular skipped locals expose status=skipped and changed=0; absent row access fails Bind(local row). A preceding REQUIRE can abort before access; lazy CASE/COALESCE cannot hide eager local materialization. MANY keeps aggregate success status and exposes per-input conflict metadata.",
                ),
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Implemented,
                    detail,
                    vec![
                        evidence(
                            "crates/giggles-language/tests/upsert.rs",
                            "upsert_version_gates_and_contextual_names_preserve_legacy",
                        ),
                        evidence(
                            "crates/giggles-language/tests/upsert.rs",
                            "upsert_text_tree_prepared_and_local_outcomes_match_without_sql",
                        ),
                        evidence(
                            "crates/giggles-language/tests/upsert.rs",
                            "upsert_cli_and_selected_manifest_match_version_ten",
                        ),
                        evidence(
                            "crates/giggles-language/tests/upsert.rs",
                            "upsert_scope_caps_and_atomic_refusals_match_common_engine",
                        ),
                    ],
                    vec![],
                );
            }
        }
        if version.supports_case() {
            for (feature, detail) in [
                (
                    "case_expressions",
                    "Explicit 0.9: searched CASE selects the first TRUE condition; simple CASE evaluates its operand once and uses ordinary equality, so NULL never matches NULL. Only the chosen result executes; unmatched CASE returns ELSE or NULL. All conditions/results bind and consume shared input/local budgets upfront, with independent comparand/result type unification. At most 64 arms; computed metadata is nullable with synthetic ID 0, and integer results use BigInt.",
                ),
                (
                    "canonical_quoted_identifiers",
                    "Explicit 0.9: backticks spell existing canonical lowercase ASCII names, including syntax keywords, without normalization or a catalog format change. Names contain 1 to 63 bytes, start with a letter, and otherwise contain lowercase letters, digits or underscores. CASE/THEN/ELSE/END require quoting as text names in this profile; prior profiles preserve their grammar. Quoted names never dispatch literals or scalar calls. LET/@ variable names stay bare and permit keywords; quoted variables are rejected.",
                ),
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Implemented,
                    detail,
                    vec![
                        evidence(
                            "crates/giggles-language/tests/case_expressions.rs",
                            "case_version_gates_cover_typed_roots_and_keyword_compatibility",
                        ),
                        evidence(
                            "crates/giggles-language/tests/case_expressions.rs",
                            "case_text_tree_and_bound_execution_match_without_sql",
                        ),
                        evidence(
                            "crates/giggles-language/tests/case_expressions.rs",
                            "case_quoted_names_are_canonical_and_share_sql_catalog",
                        ),
                        evidence(
                            "crates/giggles-language/tests/case_expressions.rs",
                            "case_cli_and_selected_manifest_match_version_nine",
                        ),
                    ],
                    vec![],
                );
            }
        }
        if version.supports_scalar_functions() {
            for (feature, detail) in [
                (
                    "coalesce",
                    "Explicit 0.8: 2 to 64 compatible arguments bind and consume value/local budgets upfront. Scalar evaluation proceeds left-to-right until the first non-NULL value. If every argument has unknown NULL type, as in coalesce(null,null), the result is untyped: projected output is rejected, scalar emit has no data type, and typed outer/assignment contexts retain their checks. Known argument types survive runtime NULL values.",
                ),
                (
                    "integer_abs",
                    "Explicit 0.8: exactly one signed-integer or NULL argument; the signed minimum fails with Overflow. The result is a synthetic BigInt output, including typed NULL.",
                ),
                (
                    "value_length",
                    "Explicit 0.8: exactly one text, bytes or NULL argument; text counts Unicode code points, bytes count bytes, and NULL returns typed BigInt NULL. No normalization or grapheme counting.",
                ),
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Implemented,
                    detail,
                    vec![
                        evidence(
                            "crates/giggles-language/tests/scalar_functions.rs",
                            "scalar_function_version_gates_cover_every_expression_root",
                        ),
                        evidence(
                            "crates/giggles-language/tests/scalar_functions.rs",
                            "scalar_function_text_tree_and_bound_execution_match_without_sql",
                        ),
                        evidence(
                            "crates/giggles-language/tests/scalar_functions.rs",
                            "scalar_function_nulls_laziness_and_shared_limits_are_explicit",
                        ),
                        evidence(
                            "crates/giggles-language/tests/scalar_functions.rs",
                            "scalar_function_cli_and_selected_manifest_match_version_eight",
                        ),
                    ],
                    vec![],
                );
            }
        }
        for (feature, detail, test) in [
            (
                "paged_reads",
                "Single-table predicates, field projection, stable primary-key tie breakers and complete/matched counts; default page 100.",
                "read_order_projection_and_complete",
            ),
            (
                "typed_bindings",
                "Exact named binds for boolean, smallint, integer, bigint, text, bytes and NULL; no implicit text interpolation.",
                "nullable_and_bytes_bindings",
            ),
            (
                "literal_insert",
                "One row from literal or bound values; omitted columns are NULL subject to required-column checks.",
                "duplicate_insert_atomic",
            ),
            (
                "guarded_singular_writes",
                "UPDATE/DELETE count the full WHERE set before WHEN; zero and multiple matches fail.",
                "singular_cardinality_precedes_guard",
            ),
            (
                "atomic_bulk_caps",
                "UPDATE MANY/DELETE MANY require at most N; N+1 matches fail before effects. A read page size never truncates the mutation set.",
                "cardinality_beyond_default_page",
            ),
            (
                "consuming_claim",
                "One ordered candidate must leave eligibility; the result includes its stored postimage and row version.",
                "claim_consumption_and_empty",
            ),
            (
                "native_no_op_counts",
                "Unchanged updates affect matched rows but change zero rows and preserve versions; ordinary SQL semantics are separate.",
                "no_op_versions",
            ),
        ] {
            add(
                interface,
                feature,
                CapabilityState::Implemented,
                detail,
                vec![
                    evidence(
                        CAPABILITY_TESTS,
                        "advertised_operations_execute_through_each_interface",
                    ),
                    evidence("crates/giggles-language/tests/conformance.rs", test),
                ],
                vec![],
            );
        }
        add(
            interface,
            "bounded_complete_input",
            CapabilityState::Implemented,
            "Reject tables beyond the finite native row bound and scans beyond materialization limits before effects, including point mutations.",
            vec![
                evidence(
                    "crates/giggles-language/tests/conformance.rs",
                    "oversized_core_table_is_never_silently_truncated",
                ),
                evidence(
                    "crates/giggles-language/tests/conformance.rs",
                    "result_materialization_limit_fails_before_mutation",
                ),
            ],
            vec![],
        );
        for (feature, detail) in [
            (
                "full_native_v1",
                "The six pinned references describe design targets beyond this finite profile.",
            ),
            (
                "server_integration",
                "This inventory qualifies no native network protocol or server deployment.",
            ),
            (
                "postgresql_18_compatibility",
                "Shared local rows do not qualify PostgreSQL 18 semantics, drivers or migration workloads.",
            ),
            (
                "production_security",
                "Admin/file owners are trusted; no task authorization or security qualification is claimed.",
            ),
            (
                "cloud_deployment",
                "No hosted or distributed native deployment is qualified by this inventory.",
            ),
            (
                "performance_qualification",
                "No latency, throughput, timeout or scanned-work guarantee is claimed.",
            ),
        ] {
            add(
                interface,
                feature,
                CapabilityState::Unverified,
                detail,
                vec![],
                vec![],
            );
        }
    }

    for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree] {
        add(
            interface,
            "binding_payload_admission",
            CapabilityState::Implemented,
            "Supplied maps have count/payload limits before catalog or transaction admission. Explicit resolved value occurrences have a separate payload limit before cloning. This tightens previously unbounded binding acceptance; it does not bound transport decoding or total heap.",
            vec![
                evidence(
                    "crates/giggles-language/tests/binding_limits.rs",
                    "oversized_supplied_bindings_fail_across_all_native_entry_points",
                ),
                evidence(
                    "crates/giggles-language/tests/binding_limits.rs",
                    "repeated_parameters_and_explicit_literals_share_the_expanded_budget",
                ),
                evidence(
                    "crates/giggles-language/tests/binding_limits.rs",
                    "exact_binding_boundaries_succeed_without_charging_implicit_defaults",
                ),
            ],
            vec![],
        );
        add(
            interface,
            "immutable_bound_operations",
            CapabilityState::Implemented,
            if version.supports_r2_wave2() { "Preparation freezes real values and catalog/output metadata, including typed Query and INSERT FROM in 0.20, without executing; execution checks identity and uses fresh rows. Handles carry no authority credential or serialization format." } else { "Preparation freezes real values and catalog/output metadata without executing; each execution checks identity and uses fresh rows. Local handles carry no authority credential or serialization format." },
            vec![
                evidence(
                    "crates/giggles-language/tests/bound_operations.rs",
                    "bound_description_has_native_version_catalog_identity_and_typed_shape",
                ),
                evidence(
                    "crates/giggles-language/tests/bound_operations.rs",
                    "preparation_freezes_real_bindings_and_caller_owned_trees",
                ),
                evidence(
                    "crates/giggles-language/tests/bound_operations.rs",
                    "bound_execution_uses_fresh_rows_for_caps_guards_cardinality_and_claims",
                ),
            ],
            vec![],
        );
        add(
            interface,
            "per_execution_prepared_parameters",
            CapabilityState::Unverified,
            "No reusable parameter-template API is offered; prepare_bound requires and freezes all argument values.",
            vec![],
            vec![],
        );
        add(
            interface,
            "direct_engine_execution",
            CapabilityState::Implemented,
            "Native syntax lowers to core typed requests without SQL generation or SQL parsing.",
            vec![evidence(
                "crates/giggles-language/tests/engine_adapter.rs",
                "native_text_and_trees_execute_with_the_sql_parser_forbidden",
            )],
            vec![],
        );
        add(
            interface,
            "shared_sql_storage",
            CapabilityState::Implemented,
            "SQL and native observe the same catalog, rows and versions through shared engine mutation primitives.",
            vec![evidence(
                "crates/giggles-language/tests/engine_adapter.rs",
                "sql_and_native_share_rows_and_native_no_ops_preserve_versions",
            )],
            vec![],
        );
    }
    if version.supports_once() {
        for interface in [CapabilityInterface::RustText, CapabilityInterface::RustTree] {
            add(
                interface,
                "once",
                CapabilityState::Implemented,
                "Explicit 0.11 trusted Rust API: a write or writing transaction uses the unchanged core Once V1 contract with caller-persisted original database domain, tenant, namespace and request, plus a caller-certified non-reentrant clock. Text/Bytes keys have exact byte identity. Retained matching decisions replay before live schema binding; success, skipped/no-op and five terminal business failures retain original results and output schema. No automatic retry, serialized prepared handle, task authority or native compaction API.",
                vec![evidence(
                    "crates/giggles-language/tests/once.rs",
                    "once_text_tree_replay_and_schema_metadata_match_core",
                )],
                vec![],
            );
            add(
                interface,
                "receipts_and_outcome_lookup",
                CapabilityState::Implemented,
                "Explicit 0.11 trusted Rust outcome API requires the original persisted scope and a Text/Bytes key, but no clock. It reports Absent, InProgress, Retained or Expired without advancing durable generation. Retained receipts preserve accepting epoch, typed output metadata, fingerprint, deadline and original outcome; explicit core compaction alone replaces payloads with permanent tombstones. Restore never silently substitutes the new database domain.",
                vec![evidence(
                    "crates/giggles-language/tests/once.rs",
                    "once_text_tree_replay_and_schema_metadata_match_core",
                )],
                vec![],
            );
            add(
                interface,
                "once_literal_retention",
                CapabilityState::Implemented,
                "Retention is a bounded literal duration in the once header only: case-sensitive us/ms/s/m/h/d, directly or as duration(\"...\"). Execution requires exact positive whole seconds from 1 through 604800. No duration parameters, general duration Value, rounding or wall-clock default. All supplied bindings share the 256-name/64 KiB admission before splitting header/body; body occurrence and runtime-local charging keep their existing 64 KiB bound.",
                vec![
                    evidence(
                        "crates/giggles-language/tests/once_parser.rs",
                        "once_parser_accepts_reference_clause_order_and_exact_duration_units",
                    ),
                    evidence(
                        "crates/giggles-language/tests/once.rs",
                        "once_header_binding_admission_is_global_and_exact",
                    ),
                ],
                vec![],
            );
        }
        for (feature, example) in [
            (
                "once",
                "insert items set id=3,state=\"ready\" once \"request-3\" for 1h",
            ),
            ("receipts_and_outcome_lookup", "outcome \"request-3\""),
        ] {
            add(
                CapabilityInterface::Cli,
                feature,
                CapabilityState::Rejected,
                "The CLI has no caller-persisted original scope or certified clock. It rejects once and outcome inputs before opening the database, including explicit 0.11; ordinary 0.11 commands remain usable. Use the trusted Rust APIs with explicit context.",
                vec![evidence(
                    "crates/giggles-language/tests/once_capabilities.rs",
                    "once_cli_refuses_retry_context_before_open",
                )],
                vec![example],
            );
        }
    } else {
        for feature in ["once", "receipts_and_outcome_lookup"] {
            add(
                CapabilityInterface::RustTree,
                feature,
                CapabilityState::Rejected,
                "Once envelopes and outcome queries require explicit 0.11 and the separate trusted Rust APIs; earlier selected profiles reject these trees.",
                vec![evidence(
                    "crates/giggles-language/tests/once.rs",
                    "once_selected_version_and_context_are_required",
                )],
                vec![],
            );
        }
    }
    add(
        CapabilityInterface::Cli,
        "persistent_local_cli",
        CapabilityState::Implemented,
        "Explicit trusted SQL CREATE TABLE initialization and native run commands over a local database path; JSON outcomes and errors.",
        vec![evidence(
            "crates/giggles-language/tests/cli.rs",
            "persistent_init_run_reopen",
        )],
        vec![],
    );
    if !version.supports_transactions() {
        add(
            CapabilityInterface::RustTree,
            "guarded_bulk_writes",
            CapabilityState::Rejected,
            "A guard with Many or Claim, or a guard with a delete cap, is rejected; guards are singular-only in this profile.",
            vec![evidence(
                CAPABILITY_TESTS,
                "unsupported_tree_combination_has_zero_effects",
            )],
            vec![],
        );
    }

    if !version.supports_insert_many() {
        add(
            CapabilityInterface::RustTree,
            "insert_many_values",
            CapabilityState::Rejected,
            "INSERT MANY requires explicit 0.6, including empty row lists, RETURN wrappers and transaction steps.",
            vec![evidence(
                "crates/giggles-language/tests/insert_many.rs",
                "insert_many_version_gates_and_empty_tree_contract",
            )],
            vec![],
        );
    }

    if !version.supports_catalog() {
        add(
            CapabilityInterface::RustTree,
            "native_create_table",
            CapabilityState::Rejected,
            "Catalog trees and handles require explicit version 0.7 and the separate catalog APIs.",
            vec![evidence(
                "crates/giggles-language/tests/catalog_operations.rs",
                "catalog_version_gates_and_required_defaults_are_explicit",
            )],
            vec![],
        );
    }
    add(
        CapabilityInterface::RustTree,
        if version.supports_upsert() {
            "invalid_conflict_wrapper"
        } else {
            "primary_key_upserts"
        },
        CapabilityState::Rejected,
        if version.supports_upsert() {
            "Only optional outer RETURN -> ON CONFLICT -> direct INSERT is admitted; reversed, nested or non-insert wrappers fail before recursive traversal or effects."
        } else {
            "ON CONFLICT wrappers and scoped Existing/Incoming nodes require explicit 0.10 in every expression root and transaction step."
        },
        vec![evidence(
            "crates/giggles-language/tests/upsert.rs",
            if version.supports_upsert() {
                "upsert_scope_caps_and_atomic_refusals_match_common_engine"
            } else {
                "upsert_version_gates_and_contextual_names_preserve_legacy"
            },
        )],
        vec![],
    );
    if !version.supports_case() {
        add(
            CapabilityInterface::RustTree,
            "case_expressions",
            CapabilityState::Rejected,
            "CASE nodes require explicit 0.9 in every expression root, including nested transaction Require and scalar Emit expressions.",
            vec![evidence(
                "crates/giggles-language/tests/case_expressions.rs",
                "case_version_gates_cover_typed_roots_and_keyword_compatibility",
            )],
            vec![],
        );
    }
    if !version.supports_scalar_functions() {
        add(
            CapabilityInterface::RustTree,
            "scalar_function_calls",
            CapabilityState::Rejected,
            "Fixed scalar Call nodes require explicit 0.8, including nested arguments and transaction Require/Emit roots.",
            vec![evidence(
                "crates/giggles-language/tests/scalar_functions.rs",
                "scalar_function_version_gates_cover_every_expression_root",
            )],
            vec![],
        );
    }
    if version == LanguageVersion::Embedded01 {
        add(
            CapabilityInterface::RustTree,
            "arithmetic",
            CapabilityState::Rejected,
            "Arithmetic tree nodes require an explicitly selected newer language version; legacy execution and preparation reject them before effects.",
            vec![evidence(
                "crates/giggles-language/tests/arithmetic.rs",
                "arithmetic_version_gates_cover_text_trees_and_bound_handles",
            )],
            vec![],
        );
    }
    if !version.supports_computed() {
        add(
            CapabilityInterface::RustTree,
            "computed_statement_variants",
            CapabilityState::Rejected,
            "Computed read/update tree variants require explicit version 0.3, including when their expressions contain only legacy fields or values.",
            vec![evidence(
                "crates/giggles-language/tests/computed_expressions.rs",
                "computed_version_gates_preserve_legacy_statement_shapes",
            )],
            vec![],
        );
    }
    add(
        CapabilityInterface::RustTree,
        if version.supports_return() {
            "invalid_return_wrapper"
        } else {
            "native_returning"
        },
        CapabilityState::Rejected,
        if version.supports_return() {
            "A return wrapper requires one direct write; reads and nested wrappers are rejected before recursive inspection or effects."
        } else {
            "Return wrappers require explicitly selected version 0.4."
        },
        vec![evidence(
            "crates/giggles-language/tests/returning.rs",
            if version.supports_return() {
                "returning_native_wrapper_admission_and_alias_binding_are_bounded"
            } else {
                "returning_version_gates_and_contextual_keyword_preserve_legacy"
            },
        )],
        vec![],
    );

    if version.supports_explain() {
        for interface in [CapabilityInterface::RustText, CapabilityInterface::Cli] {
            add(
                interface,
                "bounded_explain_scope",
                CapabilityState::Rejected,
                "Static simple-read explanation only; writes, expression projections, analyze, nested explain, blocks and retry wrappers reject before effects.",
                vec![rejection_evidence(version)],
                vec![
                    "explain update items where id=1 set n=2",
                    "explain items select id+1 as n",
                    "explain analyze items",
                    "explain explain items",
                    "explain transaction { items; }",
                    "explain items once \"key\" for 1h",
                ],
            );
        }
    }

    // These are complete rejection fixtures, not proposed grammar fragments.
    for interface in [CapabilityInterface::RustText, CapabilityInterface::Cli] {
        if version == LanguageVersion::Embedded012 {
            for (feature, examples) in [
                (
                    "inspection_scope",
                    vec![
                        "show rows",
                        "show indexes",
                        "show tables where true",
                        "describe table missing",
                        "describe table items select id",
                        "explain items",
                        "transaction { show tables }",
                        "show tables once \"inspect\" for 1h",
                    ],
                ),
                (
                    "inspection_names",
                    vec![
                        "describe table public.items",
                        "describe table $table",
                        "describe table `Items`",
                    ],
                ),
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Rejected,
                    "Outside the four-command 0.12 inspection contract: unknown/malformed targets, row/schema/index inspection, EXPLAIN, query modifiers, parameters, blocks and once wrappers are not supported by this slice.",
                    vec![rejection_evidence(version)],
                    examples,
                );
            }
        }
        if version.supports_once() {
            for (feature, examples) in [
                (
                    "once_literal_retention_restrictions",
                    vec![
                        "insert items set id=3 once \"key\" for $retention",
                        "insert items set id=3 once \"key\" for 0s",
                        "insert items set id=3 once \"key\" for 1001ms",
                        "insert items set id=3 once \"key\" for 8d",
                        "insert items set id=3 once \"key\" for 1S",
                    ],
                ),
                (
                    "once_context_and_clause_order",
                    vec![
                        "once \"key\" insert items set id=3",
                        "items once \"key\" for 1h",
                        "transaction read only once \"key\" for 1h { items }",
                        "transaction { insert items set id=3 once \"key\" for 1h }",
                        "insert items set id=3 return id once \"key\" for 1h",
                    ],
                ),
                (
                    "once_key_types",
                    vec![
                        "insert items set id=3 once 1 for 1h",
                        "insert items set id=3 once null for 1h",
                        "outcome 1",
                        "outcome null",
                    ],
                ),
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Rejected,
                    "Outside the finite 0.11 header/whole-write contract. Duration parameters, nonpositive or fractional-second retention, unsupported key types, nested/per-item envelopes and misplaced clauses fail before effects; CLI envelopes additionally require unavailable trusted context.",
                    vec![rejection_evidence(version)],
                    examples,
                );
            }
        }
        for (feature, example) in [
            ("once", "once $key insert items set id=3,state=\"ready\""),
            ("receipts_and_outcome_lookup", "outcome $key"),
            (
                "atomic_blocks",
                "atomic { insert items set id=3,state=\"ready\"; }",
            ),
            (
                "native_schema_ddl",
                "create table other (id int64 primary key)",
            ),
            ("schema_jobs", "plan migration change_items"),
            ("task_authorization", "assume task $task"),
            ("decimal", "items where id=1.25"),
            (
                "uuid",
                "items where id=uuid(\"00000000-0000-0000-0000-000000000001\")",
            ),
            (
                "temporal_types",
                "items where state=timestamp(\"2026-11-05T12:00:00Z\")",
            ),
            ("json_documents", "items where state=json(\"{}\")"),
            ("joins", "items join other as o on id=o.id"),
            ("aggregation", "items group by state select state,count()"),
            ("arithmetic", "items where id+1=2"),
            ("computed_projections", "items select id+1"),
            ("arithmetic_ordering", "items order by id+1"),
            ("arithmetic_limits", "items limit 1+1"),
            (
                "arithmetic_caps",
                "delete many items where true at most 1+1",
            ),
            ("scalar_functions", "items where lower(state)=\"ready\""),
            ("quoted_identifiers", "`items`"),
            ("qualified_names", "public.items"),
            ("cursors", "items after $cursor limit 10"),
            ("historical_snapshots", "items at snapshot $snapshot"),
            ("query_budgets", "items budget scan_bytes 1024"),
            (
                "query_source_writes",
                "insert many into archive (id,state) from (items select id,state) at most 10",
            ),
            (
                "computed_assignments",
                "update items where id=1 set state=state",
            ),
            (
                "returning",
                "update items where id=1 set state=\"done\" returning id",
            ),
            (
                "upserts",
                "insert items set id=1,state=\"ready\" on conflict do nothing",
            ),
            ("savepoints", "savepoint optional_work"),
            ("stored_routines", "run repair_items($id)"),
            (
                "triggers",
                "create trigger audit_items after insert on items do insert audit set id=1",
            ),
            (
                "guarded_bulk_writes",
                "update many items where true when true set state=\"done\" at most 2",
            ),
        ] {
            if version.supports_once() && matches!(feature, "once" | "receipts_and_outcome_lookup")
            {
                continue;
            }
            if version.supports_quoted_identifiers() && feature == "quoted_identifiers" {
                continue;
            }
            if version != LanguageVersion::Embedded01 && feature == "arithmetic" {
                continue;
            }
            if version.supports_computed() && feature == "computed_assignments" {
                continue;
            }
            if version.supports_transactions() && feature == "guarded_bulk_writes" {
                continue;
            }
            let example = if version.supports_r2_native() && feature == "qualified_names" {
                "app.items select app.items.id"
            } else if version.supports_schema_names() && feature == "qualified_names" {
                "items select items.id"
            } else { example };
            let feature = if version.supports_upsert() && feature == "upserts" {
                "legacy_insert_set_conflict"
            } else if version.supports_upsert() && feature == "qualified_names" {
                "general_qualified_names"
            } else if version.supports_computed() && feature == "computed_projections" {
                "unaliased_computed_projections"
            } else if version.supports_return() && feature == "returning" {
                "sql_style_returning"
            } else if version.supports_transactions() && feature == "atomic_blocks" {
                "atomic_keyword"
            } else if version.supports_catalog() && feature == "native_schema_ddl" {
                "inline_primary_key"
            } else if version.supports_scalar_functions() && feature == "scalar_functions" {
                "other_scalar_functions"
            } else {
                feature
            };
            add(
                interface,
                feature,
                CapabilityState::Rejected,
                "Unavailable in the finite native profile; the named fixture must fail before effects.",
                vec![rejection_evidence(version)],
                vec![example],
            );
        }
        if version.supports_computed() {
            for (feature, detail, example) in [
                (
                    "computed_inserts",
                    "INSERT continues accepting literal/parameter values only.",
                    "insert items set id=1+1,state=\"ready\"",
                ),
                (
                    "untyped_null_projection",
                    "A computed output must have a bound type; an untyped NULL output is rejected.",
                    "items select null as missing",
                ),
                (
                    "duplicate_projection_names",
                    "Computed output names must be unique, including aliases and unchanged field names.",
                    "items select id as repeated,id as repeated",
                ),
            ] {
                add(
                    interface,
                    if version.supports_transactions() && feature == "computed_inserts" {
                        "computed_insert_set"
                    } else {
                        feature
                    },
                    CapabilityState::Rejected,
                    if version.supports_transactions() && feature == "computed_inserts" {
                        "INSERT SET remains literal/parameter only; the separate column-list VALUES form admits expressions."
                    } else {
                        detail
                    },
                    vec![rejection_evidence(version)],
                    vec![example],
                );
            }
        }
        if !version.supports_return() {
            add(
                interface,
                "native_returning",
                CapabilityState::Rejected,
                "The native return clause requires explicitly selected version 0.4.",
                vec![evidence(
                    "crates/giggles-language/tests/returning.rs",
                    "returning_version_gates_and_contextual_keyword_preserve_legacy",
                )],
                vec!["update items where id=1 set state=\"done\" return id"],
            );
        } else {
            add(
                interface,
                "invalid_return_clauses",
                CapabilityState::Rejected,
                "Return requires one write and a nonempty projection spelling; star stands alone, and the clause cannot repeat.",
                vec![rejection_evidence(version)],
                vec![
                    "items return id",
                    "update items where id=1 set state=\"done\" return",
                    "update items where id=1 set state=\"done\" return *,id",
                    "update items where id=1 set state=\"done\" return id return id",
                ],
            );
        }
        if version.supports_transactions() {
            for (feature, example) in [
                (
                    "nested_transactions",
                    "transaction { transaction { emit 1 } }",
                ),
                (
                    "claim_in_transactions",
                    "transaction { claim items where true order by id set state=\"done\" }",
                ),
                (
                    "writes_in_read_only_blocks",
                    "transaction read only { delete many items where true at most 2 }",
                ),
                (
                    "transaction_ddl",
                    "transaction { create table other (id bigint primary key) }",
                ),
                ("nonfinal_emit", "transaction { emit 1; items }"),
                (
                    "forward_local_references",
                    "transaction { emit @missing.count }",
                ),
                (
                    "local_scope_mismatch",
                    "transaction { let page=(items); emit @page.row }",
                ),
                (
                    "locals_in_value_clauses",
                    "transaction { let page=(items); items limit @page.count }",
                ),
                (
                    "target_fields_in_values",
                    "insert into items(id,state) values(id+1,\"ready\")",
                ),
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Rejected,
                    "Outside the finite transaction/VALUES contract; the selected-version fixture fails before durable effects.",
                    vec![rejection_evidence(version)],
                    vec![example],
                );
            }
        }
        if version.supports_insert_many() {
            for (feature, example) in [
                (
                    "uncapped_insert_many",
                    "insert many into items(id,state) values(3,\"ready\")",
                ),
                (
                    "multiple_tuples_without_many",
                    "insert into items(id,state) values(3,\"ready\"),(4,\"ready\")",
                ),
                (
                    "empty_insert_many_text",
                    "insert many into items(id,state) values() at most 1",
                ),
                (
                    "target_fields_in_insert_many",
                    "insert many into items(id,state) values(id+1,\"ready\") at most 1",
                ),
                (
                    "duplicate_insert_many_targets",
                    "insert many into items(id,state) values(3,\"ready\"),(3,\"ready\") at most 2",
                ),
                (
                    "insert_many_singular_local",
                    "transaction { let batch=(insert many into items(id,state) values(3,\"ready\") at most 1 return id); emit @batch.row }",
                ),
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Rejected,
                    "Outside the finite capped multirow VALUES contract; the selected-version fixture fails before durable effects.",
                    vec![rejection_evidence(version)],
                    vec![example],
                );
            }
        } else {
            add(
                interface,
                "insert_many_values",
                CapabilityState::Rejected,
                "Capped multirow VALUES requires explicit version 0.6.",
                vec![evidence(
                    "crates/giggles-language/tests/insert_many.rs",
                    "insert_many_version_gates_and_empty_tree_contract",
                )],
                vec![
                    "insert many into items(id,state) values(3,\"ready\"),(4,\"ready\") at most 2",
                ],
            );
        }
        if version.supports_catalog() {
            for (feature, example) in [
                (
                    "nullable_primary_key",
                    "create table new_items(id nullable int64,primary key(id))",
                ),
                (
                    "catalog_defaults",
                    "create table new_items(id int64,state text default \"ready\",primary key(id))",
                ),
                (
                    "catalog_extra_constraints",
                    "create table new_items(id int64,state text,constraint unique_state unique(state),primary key(id))",
                ),
                (
                    "catalog_partitioning",
                    "create table new_items(id int64,primary key(id)) partition by hash(id)",
                ),
                (
                    "catalog_sql_type_aliases",
                    "create table new_items(id bigint,primary key(id))",
                ),
                (
                    "catalog_smallint",
                    "create table new_items(id smallint,primary key(id))",
                ),
                (
                    "catalog_unknown_primary_key",
                    "create table new_items(id int64,primary key(missing))",
                ),
                (
                    "catalog_return",
                    "create table new_items(id int64,primary key(id)) return id",
                ),
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Rejected,
                    "Outside the finite standalone safe-create contract; the selected-version fixture fails before durable effects.",
                    vec![rejection_evidence(version)],
                    vec![example],
                );
            }
        } else {
            add(
                interface,
                "native_create_table",
                CapabilityState::Rejected,
                "Standalone native CREATE TABLE requires explicit version 0.7.",
                vec![evidence(
                    "crates/giggles-language/tests/catalog_operations.rs",
                    "catalog_version_gates_and_required_defaults_are_explicit",
                )],
                vec!["create table new_items(id int64,primary key(id))"],
            );
        }
        if version.supports_upsert() {
            for (feature, examples) in [
                (
                    "conflict_target_binding",
                    vec![
                        "insert into items(id,state,n) values(1,\"ready\",1) on conflict(state) do nothing",
                        "insert into items(id,state,n) values(1,\"ready\",1) on conflict(id,id) do nothing",
                        "insert into items(id,state,n) values(1,\"ready\",1) on conflict(missing) do nothing",
                    ],
                ),
                (
                    "conflict_action_binding",
                    vec![
                        "insert into items(id,state,n) values(1,\"ready\",1) on conflict(id) do update set id=incoming.id",
                        "insert into items(id,state,n) values(1,\"ready\",1) on conflict(id) do update set n=n",
                        "insert into items(id,state,n) values(1,\"ready\",1) on conflict(id) do update when existing.n set n=incoming.n",
                        "insert into items(id,state,n) values(1,\"ready\",1) on conflict(id) do update set n=incoming.missing",
                    ],
                ),
                (
                    "conflict_fields_outside_action",
                    vec![
                        "items select existing.n as n",
                        "insert into items(id,state,n) values(1,\"ready\",incoming.n) on conflict(id) do nothing",
                        "insert into items(id,state,n) values(1,\"ready\",1) on conflict(id) do nothing return incoming.id",
                        "transaction { emit existing.n }",
                    ],
                ),
                (
                    "conflict_clause_order",
                    vec![
                        "insert many into items(id) values(1) at most 1 on conflict(id) do nothing",
                        "insert into items(id) values(1) on conflict(id) do update set n=1 when true",
                    ],
                ),
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Rejected,
                    "Outside the finite full-primary-key conflict contract; the selected-version request fails before effects.",
                    vec![rejection_evidence(version)],
                    examples,
                );
            }
        } else {
            add(
                interface,
                "column_values_on_conflict",
                CapabilityState::Rejected,
                "Native ON CONFLICT syntax requires explicit 0.10.",
                vec![evidence(
                    "crates/giggles-language/tests/upsert.rs",
                    "upsert_version_gates_and_contextual_names_preserve_legacy",
                )],
                vec!["insert into items(id) values(1) on conflict(id) do nothing"],
            );
        }
        if version.supports_case() {
            for (feature, examples) in [
                (
                    "case_binding_types",
                    vec![
                        "items select case when 1 then 2 else 3 end as value",
                        "items select case when true then 1 else \"x\" end as value",
                        "items select case null when 1 then 1 when \"x\" then 2 end as value",
                        "items select case when true then 1 else missing end as value",
                    ],
                ),
                (
                    "untyped_case_projection",
                    vec!["items select case when true then null else null end as missing"],
                ),
                (
                    "unaliased_case_projection",
                    vec!["items select case when true then 1 else 2 end"],
                ),
                (
                    "noncanonical_quoted_identifiers",
                    vec![
                        "`Items`",
                        "`_items`",
                        "`public.items`",
                        "`\u{e9}`",
                        "``",
                        "`it``ems`",
                    ],
                ),
                ("quoted_function_dispatch", vec!["items where `abs`(1)=1"]),
                ("unquoted_case_keyword_field", vec!["items where case"]),
                (
                    "quoted_variable_names",
                    vec![
                        "transaction { let `case`=(items); emit @case.count; }",
                        "transaction { let case=(items); emit @`case`.count; }",
                    ],
                ),
            ] {
                add(
                    interface,
                    feature,
                    CapabilityState::Rejected,
                    "Outside the finite CASE and canonical-name contract; the selected-version fixture fails before effects.",
                    vec![rejection_evidence(version)],
                    examples,
                );
            }
        }
        if version.supports_scalar_functions() {
            for (feature, examples) in [
                (
                    "scalar_call_arity",
                    vec![
                        "items where coalesce()=1",
                        "items where coalesce(1)=1",
                        "items where abs()=1",
                        "items where abs(1,2)=1",
                        "items where length()=1",
                        "items where length(\"a\",\"b\")=1",
                    ],
                ),
                (
                    "scalar_call_types",
                    vec![
                        "items where abs(\"x\")=1",
                        "items where length(1)=1",
                        "items where coalesce(1,\"x\")=1",
                        "items where coalesce(1,missing)=1",
                    ],
                ),
                (
                    "untyped_coalesce_projection",
                    vec!["items select coalesce(null,null) as missing"],
                ),
                (
                    "case_expressions",
                    vec!["items select case when true then 1 else 2 end as result"],
                ),
                (
                    "calls_in_value_clauses",
                    vec![
                        "items limit abs(1)",
                        "delete many items where true at most abs(1)",
                        "insert items set id=abs(-1),state=\"ready\"",
                    ],
                ),
            ] {
                if version.supports_case() && feature == "case_expressions" {
                    continue;
                }
                add(
                    interface,
                    feature,
                    CapabilityState::Rejected,
                    "Outside the finite three-function contract; the selected-version fixture fails before durable effects.",
                    vec![rejection_evidence(version)],
                    examples,
                );
            }
        } else {
            add(
                interface,
                "scalar_function_calls",
                CapabilityState::Rejected,
                "Fixed scalar calls require explicit version 0.8; bare function-named fields remain ordinary identifiers.",
                vec![evidence(
                    "crates/giggles-language/tests/scalar_functions.rs",
                    "scalar_function_version_gates_cover_every_expression_root",
                )],
                vec!["items where abs(-1)=1"],
            );
        }
    }

    CapabilityManifest {
        manifest_version: 1,
        language_version: version.as_str(),
        package_version: env!("CARGO_PKG_VERSION"),
        profile: profile.name,
        design_doc_id: DESIGN_DOC_ID,
        references: [
            ("tour", "22093487-1032-431b-aefa-dd2088bb2bec", 1),
            ("writes", "9252b8cc-42d0-4d54-8764-0d4ffeb9c72b", 1),
            ("schema", "0f861b66-2cf4-4d24-8a99-2a6b46381836", 2),
            ("permissions", "a614da1c-c7e4-4668-9229-46102a9c6a14", 3),
            ("sql", "e5dadca6-9f0f-409b-8184-94fb2ea6960c", 2),
            ("postgresql", "da366690-816c-481e-93a0-f6563c096f0a", 2),
        ]
        .into_iter()
        .map(|(name, supporting_doc_id, revision)| ReferenceRevision {
            name,
            supporting_doc_id,
            revision,
            url: format!("https://app.archdocs.io/design-docs/{DESIGN_DOC_ID}/supporting/{supporting_doc_id}?revision={revision}"),
        })
        .collect(),
        limits: CapabilityLimits {
            text_bytes: MAX_INPUT,
            serialized_tree_bytes: MAX_INPUT,
            text_tokens: MAX_TOKENS,
            expression_nodes: MAX_EXPRESSION_NODES,
            expression_depth: MAX_DEPTH,
            named_bindings: MAX_BINDINGS,
            binding_payload_bytes: MAX_BINDING_BYTES,
            bound_value_bytes: MAX_BOUND_VALUE_BYTES,
            default_page_rows: profile.default_page,
            result_page_rows: MAX_ROWS,
            native_table_rows: MAX_ROWS,
            row_bytes: relational::MAX_ROW_BYTES,
            scan_result_bytes: relational::MAX_RESULT_BYTES,
            computed_mutation_bytes: version.supports_computed().then_some(MAX_MUTATION_BYTES),
            computed_projection_bytes: version.supports_computed().then_some(relational::MAX_RESULT_BYTES),
            return_result_bytes: version.supports_return().then_some(relational::MAX_RESULT_BYTES),
            transaction_items: version.supports_transactions().then_some(crate::MAX_TRANSACTION_ITEMS),
            transaction_locals: version.supports_transactions().then_some(crate::MAX_TRANSACTION_LOCALS),
            emit_values: version.supports_transactions().then_some(crate::MAX_EMIT_VALUES),
            retained_local_bytes: version.supports_transactions().then_some(crate::MAX_RETAINED_LOCAL_BYTES),
            emit_bytes: version.supports_transactions().then_some(crate::MAX_EMIT_BYTES),
            insert_input_rows: version.supports_insert_many().then_some(crate::MAX_INSERT_ROWS),
            insert_proposed_bytes: version.supports_insert_many().then_some(crate::MAX_INSERT_BYTES),
            catalog_primary_key_columns: version.supports_catalog().then_some(relational::MAX_COLUMNS),
            function_arguments: version.supports_scalar_functions().then_some(crate::MAX_FUNCTION_ARGUMENTS),
            case_arms: version.supports_case().then_some(crate::MAX_CASE_ARMS),
            quoted_identifier_bytes: version.supports_quoted_identifiers().then_some(63),
            conflict_target_columns: version.supports_upsert().then_some(relational::MAX_COLUMNS),
            conflict_input_outcomes: version.supports_upsert().then_some(crate::MAX_INSERT_ROWS),
            once_key_bytes: version.supports_once().then_some(core_once::MAX_ONCE_KEY_BYTES),
            once_namespace_bytes: version.supports_once().then_some(core_once::MAX_ONCE_NAMESPACE_BYTES),
            once_canonical_intent_bytes: version.supports_once().then_some(core_once::MAX_ONCE_INTENT_BYTES),
            once_encoded_result_bytes: version.supports_once().then_some(core_once::MAX_ONCE_RESULT_BYTES),
            once_retained_keys: version.supports_once().then_some(core_once::MAX_ONCE_RECORDS),
            once_ledger_bytes: version.supports_once().then_some(core_once::MAX_ONCE_LEDGER_BYTES),
            once_min_retention_seconds: version.supports_once().then_some(1),
            once_max_retention_seconds: version.supports_once().then_some(core_once::MAX_ONCE_RETENTION_SECONDS),
            once_max_clock_lease_seconds: version.supports_once().then_some(core_once::MAX_ONCE_CLOCK_LEASE_SECONDS),
            inspection_metadata_bytes: version.supports_inspection()
                .then_some(giggles_core::operations::MAX_INSPECTION_METADATA_BYTES),
            columns_per_table: relational::MAX_COLUMNS,
            catalog_tables: relational::MAX_TABLES,
            concurrent_explicit_sessions: relational::MAX_TRANSACTIONS,
        },
        capabilities,
    }
}

fn evidence(path: &'static str, test: &'static str) -> TestEvidence {
    TestEvidence { path, test }
}

fn rejection_evidence(version: LanguageVersion) -> TestEvidence {
    let (path, test) = match version {
        LanguageVersion::Embedded01 => (
            CAPABILITY_TESTS,
            "rejected_capabilities_fail_before_effects",
        ),
        LanguageVersion::Embedded02 => (
            "crates/giggles-language/tests/arithmetic.rs",
            "arithmetic_selected_manifest_rejections_use_selected_version",
        ),
        LanguageVersion::Embedded03 => (
            "crates/giggles-language/tests/computed_expressions.rs",
            "computed_selected_manifest_rejections_use_selected_version",
        ),
        LanguageVersion::Embedded04 => (
            "crates/giggles-language/tests/returning.rs",
            "returning_selected_manifest_rejections_use_selected_version",
        ),
        LanguageVersion::Embedded05 => (
            "crates/giggles-language/tests/transactions.rs",
            "transaction_cli_and_selected_manifest_match_version_five",
        ),
        LanguageVersion::Embedded06 => (
            "crates/giggles-language/tests/insert_many.rs",
            "insert_many_cli_and_selected_manifest_match_version_six",
        ),
        LanguageVersion::Embedded07 => (
            "crates/giggles-language/tests/catalog_operations.rs",
            "catalog_cli_empty_init_and_selected_manifest_match_version_seven",
        ),
        LanguageVersion::Embedded08 => (
            "crates/giggles-language/tests/scalar_functions.rs",
            "scalar_function_cli_and_selected_manifest_match_version_eight",
        ),
        LanguageVersion::Embedded09 => (
            "crates/giggles-language/tests/case_expressions.rs",
            "case_cli_and_selected_manifest_match_version_nine",
        ),
        LanguageVersion::Embedded010 => (
            "crates/giggles-language/tests/upsert.rs",
            "upsert_cli_and_selected_manifest_match_version_ten",
        ),
        LanguageVersion::Embedded011 => (
            "crates/giggles-language/tests/once_capabilities.rs",
            "once_selected_manifest_rejections_use_selected_version",
        ),
        LanguageVersion::Embedded020 => (
            "crates/giggles-language/tests/native_view_parity.rs",
            "view_profile_is_opt_in_and_inherits_nineteen",
        ),
        LanguageVersion::Embedded019 => (
            "crates/giggles-language/tests/native_alter_parity.rs",
            "alter_profile_is_opt_in_and_inherits_eighteen",
        ),
        LanguageVersion::Embedded018 => (
            "crates/giggles-language/tests/native_schema_parity.rs",
            "schema_profile_is_opt_in_and_inherits_seventeen",
        ),
        LanguageVersion::Embedded017 => (
            "crates/giggles-language/tests/native_drop_parity.rs",
            "drop_profile_is_opt_in_and_inherits_numeric",
        ),
        LanguageVersion::Embedded016 => (
            "crates/giggles-language/tests/native_parity.rs",
            "numeric_native_catalog_matches_sql",
        ),
        LanguageVersion::Embedded015 => (
            "crates/giggles-language/tests/native_parity.rs",
            "numeric_parameters_match_sql",
        ),
        LanguageVersion::Embedded014 => (
            "crates/giggles-language/tests/native_parity.rs",
            "index_text_is_opt_in_and_rejects_extended_forms",
        ),
        LanguageVersion::Embedded013 => (
            "crates/giggles-language/tests/explain.rs",
            "explain_selected_manifest_rejections_use_selected_version",
        ),
        LanguageVersion::Embedded012 => (
            "crates/giggles-language/tests/inspection_capabilities.rs",
            "inspection_selected_manifest_rejections_use_selected_version",
        ),
    };
    evidence(path, test)
}
