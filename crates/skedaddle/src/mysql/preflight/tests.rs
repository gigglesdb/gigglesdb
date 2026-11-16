use super::*;
use crate::definition::ObjectKind;

fn options() -> Options {
    Options {
        estimated_copy_seconds: 100,
        planned_catchup_seconds: 20,
        selected_tables: vec![ObjectSelector {
            schema: "app".into(),
            name: "items".into(),
            kind: ObjectKind::Table,
            exclude_columns: vec![],
        }],
        ..Options::default()
    }
}
fn passing(version: &str, mode: &str) -> MysqlSnapshot {
    use MysqlCheck::*;
    let mut observations = Vec::new();
    for &check in CHECKS {
        let rows = match check {
            Version => vec![json!({"version":version,"comment":"MySQL Community Server - GPL"})],
            LogBin => vec![json!({"value":"1"})],
            Format => vec![json!({"value":"ROW"})],
            RowImage | RowMetadata => vec![json!({"value":"FULL"})],
            RowValueOptions | GtidExecuted => vec![json!({"value":""})],
            Checksum => vec![json!({"value":"CRC32"})],
            Retention => vec![json!({"value":"120"})],
            ExpireDays => vec![json!({"value":"0"})],
            GtidMode => vec![json!({"value":mode})],
            GtidConsistency => vec![json!({"value":"ON"})],
            Compression => vec![json!({"enabled":"OFF","cache":"18446744073709547520"})],
            Grants => vec![
                json!({"grant":"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'u'@'%'"}),
                json!({"grant":"GRANT SELECT ON `app`.`items` TO 'u'@'%'"}),
                json!({"grant":"GRANT INSERT, UPDATE, DELETE, CREATE ON `_skedaddle`.* TO 'u'@'%'"}),
            ],
            Roles => vec![json!({"value":"NONE"})],
            Skedaddle => vec![json!({"name":"_skedaddle"})],
            Identity => vec![json!({"uuid":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","id":"1"})],
            Table => vec![json!({"engine":"InnoDB"})],
            Key => vec![json!({"COLUMN_NAME":"id"})],
            Spatial => vec![json!({"COLUMN_NAME":"id","DATA_TYPE":"bigint"})],
        };
        let object =
            matches!(check, Table | Key | Spatial).then(|| options().selected_tables[0].clone());
        if check != ExpireDays || version.starts_with("8.0.") {
            observations.push(Observation {
                check,
                object,
                sql: check.sql().into(),
                rows,
                error: None,
            });
        }
    }
    MysqlSnapshot { observations }
}
fn blocking(snapshot: &MysqlSnapshot) -> Vec<String> {
    classify(snapshot, &options())
        .iter()
        .filter(|i| {
            i.criticality == Criticality::Critical
                && !matches!(
                    i.status,
                    ItemStatus::Supported | ItemStatus::SupportedWithTransformation
                )
        })
        .map(|i| i.id.clone())
        .collect()
}
fn replace(snapshot: &mut MysqlSnapshot, check: MysqlCheck, rows: Vec<Value>) {
    snapshot
        .observations
        .iter_mut()
        .find(|o| o.check == check)
        .unwrap()
        .rows = rows;
}
#[test]
fn supported_versions_and_gtid_modes() {
    for version in ["8.0.44", "8.4.7"] {
        for mode in ["ON", "OFF"] {
            assert!(blocking(&passing(version, mode)).is_empty());
        }
    }
}
macro_rules! fails {
    ($($name:ident: $check:ident => $rows:expr),+ $(,)?) => { $(#[test] fn $name() {
        let mut snapshot = passing("8.0.44", "ON");
        replace(&mut snapshot, MysqlCheck::$check, $rows);
        assert_eq!(blocking(&snapshot), [MysqlCheck::$check.id()]);
    })+ };
}
fails! {
    unsupported_version: Version => vec![json!({"version":"9.0.1","comment":"MySQL Community Server - GPL"})],
    log_bin_off: LogBin => vec![json!({"value":"0"})],
    statement_format: Format => vec![json!({"value":"STATEMENT"})],
    minimal_images: RowImage => vec![json!({"value":"MINIMAL"})],
    partial_json: RowValueOptions => vec![json!({"value":"PARTIAL_JSON"})],
    invalid_metadata: RowMetadata => vec![json!({"value":"UNKNOWN"})],
    invalid_checksum: Checksum => vec![json!({"value":"UNKNOWN"})],
    short_retention: Retention => vec![json!({"value":"119"})],
    transitional_on: GtidMode => vec![json!({"value":"ON_PERMISSIVE"})],
    transitional_off: GtidMode => vec![json!({"value":"OFF_PERMISSIVE"})],
    unknown_consistency: GtidConsistency => vec![json!({"value":"UNKNOWN"})],
    tagged_gtid: GtidExecuted => vec![json!({"value":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:tag:1"})],
    malformed_gtid: GtidExecuted => vec![json!({"value":"bad"})],
    excessive_cache: Compression => vec![json!({"enabled":"ON","cache":"16777217"})],
    unresolved_roles: Roles => vec![],
    no_install_schema: Skedaddle => vec![],
    bad_identity: Identity => vec![json!({"uuid":"not-uuid","id":"1"})],
    myisam: Table => vec![json!({"engine":"MyISAM"})],
    missing_table: Table => vec![],
    no_primary_key: Key => vec![],
    spatial_column: Spatial => vec![json!({"COLUMN_NAME":"shape","DATA_TYPE":"point"})],
    spatial_collection: Spatial => vec![json!({"COLUMN_NAME":"shape","DATA_TYPE":"geomcollection"})],
    malformed_column: Spatial => vec![json!({"COLUMN_NAME":"shape"})],
}
#[test]
fn query_error_missing_and_duplicate_evidence_block() {
    let mut stale = passing("8.4.7", "ON");
    let metadata = stale
        .observations
        .iter_mut()
        .find(|o| o.check == MysqlCheck::RowMetadata)
        .unwrap();
    metadata.rows = vec![json!({"value":"MINIMAL"})];
    metadata.error = Some("Query failed".into());
    assert_eq!(blocking(&stale), ["mysql_binlog_row_metadata"]);
    let mut snapshot = passing("8.4.7", "ON");
    let row = snapshot
        .observations
        .iter_mut()
        .find(|o| o.check == MysqlCheck::Format)
        .unwrap();
    row.error = Some("MySQL code 1227, SQLSTATE 42000 (message redacted)".into());
    assert_eq!(blocking(&snapshot), ["mysql_binlog_format"]);
    snapshot
        .observations
        .retain(|o| o.check != MysqlCheck::Format);
    assert_eq!(blocking(&snapshot), ["mysql_binlog_format"]);
    let row = passing("8.4.7", "ON")
        .observations
        .into_iter()
        .find(|o| o.check == MysqlCheck::Format)
        .unwrap();
    snapshot.observations.extend([row.clone(), row]);
    assert_eq!(blocking(&snapshot), ["mysql_binlog_format"]);
}
#[test]
fn advisories_and_compression_boundary() {
    let mut snapshot = passing("8.0.44", "OFF");
    replace(
        &mut snapshot,
        MysqlCheck::Version,
        vec![json!({"version":"8.0.44-MariaDB","comment":"MariaDB"})],
    );
    replace(
        &mut snapshot,
        MysqlCheck::RowMetadata,
        vec![json!({"value":"MINIMAL"})],
    );
    replace(
        &mut snapshot,
        MysqlCheck::Retention,
        vec![json!({"value":"0"})],
    );
    replace(
        &mut snapshot,
        MysqlCheck::Compression,
        vec![json!({"enabled":"ON","cache":"16777216"})],
    );
    assert!(blocking(&snapshot).is_empty());
    assert_eq!(
        classify(&snapshot, &options())
            .iter()
            .filter(|i| i.criticality == Criticality::Advisory)
            .count(),
        3
    );
    for comment in ["Percona Server (GPL)", "Unknown Vendor"] {
        replace(
            &mut snapshot,
            MysqlCheck::Version,
            vec![json!({"version":"8.0.44","comment":comment})],
        );
        assert!(
            classify(&snapshot, &options())
                .iter()
                .any(|i| i.id == "mysql_version_advisory")
        );
    }
}
#[test]
fn grants_require_global_replication_and_full_selected_table_select() {
    let mut snapshot = passing("8.4.7", "ON");
    for grant in [
        "GRANT ALL PRIVILEGES ON *.* TO 'u'@'%'",
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'u'@'%'",
        "GRANT SELECT ON `app`.* TO 'u'@'%'",
    ] {
        let mut rows = passing("8.4.7", "ON")
            .observations
            .into_iter()
            .find(|o| o.check == MysqlCheck::Grants)
            .unwrap()
            .rows;
        rows[1] = json!({"grant":grant});
        replace(&mut snapshot, MysqlCheck::Grants, rows);
        assert_eq!(
            blocking(&snapshot),
            if grant.contains("REPLICATION SLAVE") {
                vec!["mysql_privileges"]
            } else {
                vec![]
            }
        );
    }
    for grant in [
        "GRANT SELECT (`id`) ON `app`.`items` TO 'u'@'%'",
        "GRANT SELECT ON `other`.* TO 'u'@'%'",
        "GRANT ALL PRIVILEGES ON `*`.* TO 'u'@'%'",
    ] {
        let mut rows = passing("8.4.7", "ON")
            .observations
            .into_iter()
            .find(|o| o.check == MysqlCheck::Grants)
            .unwrap()
            .rows;
        rows[0] = json!({"grant":grant});
        replace(&mut snapshot, MysqlCheck::Grants, rows);
        assert_eq!(blocking(&snapshot), ["mysql_privileges"]);
    }
    assert_eq!(
        role_list("`r``one`@`%`,`r2`@`localhost`").unwrap(),
        "`r``one`@`%`,`r2`@`localhost`"
    );
    assert!(role_list("`r`@`%`;DROP TABLE t").is_none());
    assert!(
        redacted(mysql_async::Error::Server(mysql_async::ServerError {
            code: 1227,
            state: "42000".into(),
            message: "secret".into()
        }))
        .contains("redacted")
    );
}
#[test]
fn missing_select_install_grants_and_partial_revokes() {
    for (index, grant, blocker) in [
        (
            1,
            "GRANT SELECT (`id`) ON `app`.`items` TO 'u'@'%'",
            "mysql_privileges",
        ),
        (
            2,
            "GRANT INSERT, UPDATE, DELETE ON `_skedaddle`.* TO 'u'@'%'",
            "mysql_skedaddle_schema",
        ),
    ] {
        let mut snapshot = passing("8.4.7", "ON");
        snapshot
            .observations
            .iter_mut()
            .find(|o| o.check == MysqlCheck::Grants)
            .unwrap()
            .rows[index] = json!({"grant":grant});
        assert_eq!(blocking(&snapshot), [blocker]);
    }
    let mut snapshot = passing("8.0.44", "ON");
    snapshot
        .observations
        .iter_mut()
        .find(|o| o.check == MysqlCheck::Grants)
        .unwrap()
        .rows
        .push(json!({"grant":"REVOKE SELECT ON `app`.* FROM 'u'@'%'"}));
    assert!(grants(&snapshot).is_none());
    snapshot = passing("8.0.44", "ON");
    replace(
        &mut snapshot,
        MysqlCheck::Retention,
        vec![json!({"value":"100000"})],
    );
    replace(
        &mut snapshot,
        MysqlCheck::ExpireDays,
        vec![json!({"value":"1"})],
    );
    let options = Options {
        estimated_copy_seconds: 86401,
        ..options()
    };
    assert_eq!(
        classify(&snapshot, &options)
            .iter()
            .filter(|i| i.criticality == Criticality::Critical && i.status != ItemStatus::Supported)
            .map(|i| i.id.as_str())
            .collect::<Vec<_>>(),
        ["mysql_expire_logs_days"]
    );
}
