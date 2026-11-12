use super::*;
use serde_json::json;

fn status(check: Check, rows: Vec<Value>, options: &Options) -> ItemStatus {
    classify_rows(
        &Observation {
            check,
            object: None,
            sql: "SELECT test_result".into(),
            rows,
            error: None,
        },
        18,
        options,
        &|column| column.schema == "pg_catalog" && column.name == "int4",
    )
}

fn key() -> Value {
    json!({"name":"pk", "primary":true,"unique":true,"valid":true,"ready":true,"immediate":true,"not_null":true,"partial":false,"expression":false,"replica_identity":"d","identity_index":false,"columns":["id"]})
}

#[test]
fn every_key_safety_condition_is_required_and_full_is_not_a_chunk_key() {
    let options = Options::default();
    assert_eq!(
        status(Check::Key, vec![key()], &options),
        ItemStatus::Supported
    );
    for field in [
        "unique",
        "valid",
        "ready",
        "immediate",
        "not_null",
        "partial",
        "expression",
    ] {
        let mut row = key();
        row[field] = json!(!row[field].as_bool().unwrap());
        assert_eq!(
            status(Check::Key, vec![row], &options),
            ItemStatus::Unsupported,
            "{field}"
        );
    }
    assert_eq!(
        status(Check::Key, vec![json!({"replica_identity":"f"})], &options),
        ItemStatus::Unsupported
    );
    assert_eq!(
        status(Check::Key, vec![], &options),
        ItemStatus::Unsupported
    );
}

#[test]
fn replica_identity_requires_default_primary_selected_index_or_full() {
    let options = Options::default();
    assert_eq!(
        status(Check::ReplicaIdentity, vec![key()], &options),
        ItemStatus::Supported
    );
    let mut row = key();
    row["primary"] = json!(false);
    assert_eq!(
        status(Check::ReplicaIdentity, vec![row.clone()], &options),
        ItemStatus::RequiresExternalChange
    );
    row["replica_identity"] = json!("i");
    row["identity_index"] = json!(true);
    assert_eq!(
        status(Check::ReplicaIdentity, vec![row.clone()], &options),
        ItemStatus::Supported
    );
    row["replica_identity"] = json!("n");
    assert_eq!(
        status(Check::ReplicaIdentity, vec![row.clone()], &options),
        ItemStatus::RequiresExternalChange
    );
    row["replica_identity"] = json!("f");
    assert_eq!(
        status(Check::ReplicaIdentity, vec![row], &options),
        ItemStatus::Supported
    );
}

#[test]
fn wal_headroom_retention_and_external_disk_budget_boundaries() {
    let mut options = Options::default();
    assert_eq!(
        status(
            Check::WalLevel,
            vec![json!({"wal_level":"replica", "in_recovery":false})],
            &options
        ),
        ItemStatus::RequiresExternalChange
    );
    for check in [Check::SlotHeadroom, Check::WalSenderHeadroom] {
        assert_eq!(
            status(check, vec![json!({"maximum":10,"used":9})], &options),
            ItemStatus::Supported
        );
        assert_eq!(
            status(check, vec![json!({"maximum":10,"used":10})], &options),
            ItemStatus::RequiresExternalChange
        );
        assert_eq!(
            status(check, vec![json!({"maximum":10,"used":11})], &options),
            ItemStatus::RequiresExternalChange
        );
    }
    assert_eq!(
        status(Check::WalRetention, vec![json!({"bytes":0})], &options),
        ItemStatus::RequiresExternalChange
    );
    assert_eq!(
        status(Check::WalRetention, vec![json!({"bytes":-1})], &options),
        ItemStatus::SupportedWithTransformation
    );
    assert_eq!(
        status(
            Check::WalRetention,
            vec![json!({"bytes":options.minimum_wal_keep_bytes})],
            &options
        ),
        ItemStatus::Supported
    );
    assert_eq!(
        status(
            Check::WalDisk,
            vec![json!({"allocated_wal_bytes":10})],
            &options
        ),
        ItemStatus::Unverified
    );
    options.wal_disk = Some(DiskHeadroom {
        free_bytes: 1,
        required_bytes: 2,
        evidence: "source df, 2026-11-06".into(),
    });
    assert_eq!(
        status(Check::WalDisk, vec![], &options),
        ItemStatus::RequiresExternalChange
    );
    options.wal_disk.as_mut().unwrap().free_bytes = options.minimum_wal_keep_bytes;
    assert_eq!(
        status(Check::WalDisk, vec![], &options),
        ItemStatus::Supported
    );
}

#[test]
fn privileges_do_not_confuse_replication_membership_with_role_attributes() {
    let options = Options::default();
    for (check, rows) in [
        (
            Check::ReplicationPrivilege,
            vec![json!({"login":true,"replication":false,"superuser":false})],
        ),
        (Check::DatabaseCreate, vec![json!({"create":false})]),
        (
            Check::EventTriggerPrivilege,
            vec![json!({"superuser":false})],
        ),
        (
            Check::Grants,
            vec![json!({"select":true,"usage":true,"owner":false,"rls_active":false})],
        ),
    ] {
        assert_eq!(
            status(check, rows, &options),
            ItemStatus::RequiresExternalChange
        );
    }
    assert_eq!(
        status(
            Check::OriginGrants,
            vec![json!({"execute":true}); 10],
            &options
        ),
        ItemStatus::RequiresExternalChange
    );
    assert_eq!(
        status(
            Check::OriginGrants,
            vec![json!({"execute":true}); 11],
            &options
        ),
        ItemStatus::Supported
    );
    assert_eq!(
        status(
            Check::ReplicationPrivilege,
            vec![json!({"login":true,"replication":false,"superuser":true})],
            &options
        ),
        ItemStatus::Supported
    );
}

#[test]
fn nullable_duplicate_groups_and_large_objects_block() {
    let options = Options::default();
    for (check, rows, expected) in [
        (
            Check::NullableKeyDuplicates,
            vec![json!({"duplicate_groups":0})],
            ItemStatus::Supported,
        ),
        (
            Check::NullableKeyDuplicates,
            vec![json!({"duplicate_groups":1})],
            ItemStatus::RequiresExternalChange,
        ),
        (
            Check::LargeObjects,
            vec![json!({"count":1})],
            ItemStatus::Unsupported,
        ),
        (
            Check::LargeObjects,
            vec![json!({"count":0})],
            ItemStatus::Supported,
        ),
        (
            Check::PreparedTransactions,
            vec![json!({"gid":"prepared"})],
            ItemStatus::RequiresExternalChange,
        ),
        (
            Check::LongTransactions,
            vec![json!({"visible":false,"oldest_seconds":0.0})],
            ItemStatus::Unverified,
        ),
        (
            Check::LongTransactions,
            vec![json!({"visible":true,"oldest_seconds":300.0})],
            ItemStatus::RequiresExternalChange,
        ),
        (
            Check::LongTransactions,
            vec![json!({"visible":true,"oldest_seconds":299.0})],
            ItemStatus::Supported,
        ),
    ] {
        assert_eq!(status(check, rows, &options), expected, "{check:?}");
    }
}

#[test]
fn mapping_seam_receives_exact_type_domain_and_rejects_unknowns() {
    let options = Options::default();
    let row = json!({"schema":"pg_catalog","name":"int4","oid":23,"typmod":-1,"kind":"b","base_oid":0,"element_oid":0,"formatted":"integer"});
    assert_eq!(
        status(Check::ColumnTypes, vec![row.clone()], &options),
        ItemStatus::Supported
    );
    let column = column_type(&row).unwrap();
    assert_eq!(
        (
            column.oid,
            column.typmod,
            column.base_oid,
            column.element_oid
        ),
        (23, -1, 0, 0)
    );
    let mut unknown = row;
    unknown["schema"] = json!("extension");
    assert_eq!(
        status(Check::ColumnTypes, vec![unknown], &options),
        ItemStatus::Unsupported
    );
    assert_eq!(
        status(Check::ColumnTypes, vec![json!({})], &options),
        ItemStatus::Unsupported
    );
}

#[test]
fn triggers_require_explicit_effect_classification_and_replica_modes_need_review() {
    let mut options = Options::default();
    let mut trigger = json!({"qualified_name":"public.t.audit","enabled":"O","internal":false});
    assert_eq!(
        status(Check::Triggers, vec![trigger.clone()], &options),
        ItemStatus::Unverified
    );
    options
        .trigger_effects
        .insert("public.t.audit".into(), TriggerEffects::Replicated);
    assert_eq!(
        status(Check::Triggers, vec![trigger.clone()], &options),
        ItemStatus::SupportedWithTransformation
    );
    trigger["enabled"] = json!("A");
    assert_eq!(
        status(Check::Triggers, vec![trigger.clone()], &options),
        ItemStatus::Unverified
    );
    options
        .trigger_effects
        .insert("public.t.audit".into(), TriggerEffects::ExternalSideEffects);
    assert_eq!(
        status(Check::Triggers, vec![trigger.clone()], &options),
        ItemStatus::RequiresExternalChange
    );
    trigger["enabled"] = json!("D");
    assert_eq!(
        status(Check::Triggers, vec![trigger], &options),
        ItemStatus::SupportedWithTransformation
    );
}

#[test]
fn version_range_errors_and_sql_evidence_remain_explicit_blockers() {
    for (version, expected) in [
        (150000, ItemStatus::Unsupported),
        (160000, ItemStatus::Supported),
        (170002, ItemStatus::Supported),
        (180000, ItemStatus::Supported),
        (190000, ItemStatus::Unsupported),
    ] {
        let snapshot = Snapshot {
            observations: vec![Observation {
                check: Check::Version,
                object: None,
                sql: "SELECT current_setting('server_version_num')".into(),
                rows: vec![json!({"version_num":version})],
                error: None,
            }],
        };
        let report = classify("digest", &snapshot, &Options::default(), |_| true);
        assert_eq!(report.items[0].status, expected);
        assert!(
            report.items[0].evidence[0]
                .description
                .contains(&version.to_string())
        );
    }
    let snapshot = Snapshot {
        observations: vec![Observation {
            check: Check::WalDisk,
            object: None,
            sql: "SELECT pg_ls_waldir()".into(),
            rows: vec![],
            error: Some("SQLSTATE 42501".into()),
        }],
    };
    let report = classify("digest", &snapshot, &Options::default(), |_| true);
    assert_eq!(report.blocking().len(), 1);
    assert_eq!(report.items[0].status, ItemStatus::Unverified);
    assert!(report.items[0].evidence[0].description.contains("42501"));
    assert_eq!(report.items[0].evidence[0].source, "SELECT pg_ls_waldir()");
}

#[test]
fn inventories_and_readiness_related_transformations_never_claim_cutover_readiness() {
    let options = Options::default();
    for check in [
        Check::Views,
        Check::Routines,
        Check::Partitions,
        Check::Extensions,
        Check::SelectedObject,
    ] {
        assert_eq!(
            status(check, vec![json!({"name":"inventory"})], &options),
            ItemStatus::Unverified
        );
        assert_eq!(status(check, vec![], &options), ItemStatus::Supported);
    }
    for check in [
        Check::Sequences,
        Check::IdentityColumns,
        Check::GeneratedColumns,
    ] {
        assert_eq!(
            status(check, vec![json!({"column":"id", "select":true})], &options),
            ItemStatus::SupportedWithTransformation
        );
    }
    assert_eq!(
        status(
            Check::RestartLocks,
            vec![json!({"pending_restart":true})],
            &options
        ),
        ItemStatus::RequiresExternalChange
    );
    assert_eq!(
        status(Check::MutableKeys, vec![key()], &options),
        ItemStatus::SupportedWithTransformation
    );
}

#[test]
fn missing_foreign_unlogged_or_rls_filtered_tables_do_not_pass() {
    let options = Options::default();
    let valid = json!({"kind":"r","persistence":"p","rls_active":false});
    assert_eq!(
        status(Check::Table, vec![valid.clone()], &options),
        ItemStatus::Supported
    );
    assert_eq!(
        status(Check::Table, vec![], &options),
        ItemStatus::Unsupported
    );
    for (field, value) in [
        ("kind", json!("f")),
        ("persistence", json!("u")),
        ("rls_active", json!(true)),
    ] {
        let mut row = valid.clone();
        row[field] = value;
        assert_eq!(
            status(Check::Table, vec![row], &options),
            ItemStatus::RequiresExternalChange
        );
    }
    assert_eq!(
        status(Check::Sequences, vec![json!({"select":false})], &options),
        ItemStatus::RequiresExternalChange
    );
    assert_eq!(
        status(
            Check::WalLevel,
            vec![json!({"wal_level":"logical","in_recovery":true})],
            &options
        ),
        ItemStatus::RequiresExternalChange
    );
    assert_eq!(
        status(Check::Extensions, vec![json!({"name":"plpgsql"})], &options),
        ItemStatus::SupportedWithTransformation
    );
}
