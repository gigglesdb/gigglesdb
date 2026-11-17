use super::super::{client::MysqlStatus, resume, schema::ColumnDescriptor};
use super::*;
const UUID: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
fn file() -> MysqlPosition {
    MysqlPosition::File {
        server_uuid: UUID.into(),
        file_seq: 1,
        offset: 4,
    }
}
fn descriptor() -> TableDescriptor {
    TableDescriptor {
        schema: "s".into(),
        table: "t".into(),
        primary_key: vec!["id".into()],
        columns: ["id", "v", "hidden"]
            .into_iter()
            .enumerate()
            .map(|(i, n)| ColumnDescriptor {
                name: n.into(),
                ordinal: i as u64 + 1,
                data_type: "int".into(),
                column_type: "int".into(),
                character_set: None,
                collation: None,
                nullable: false,
                generated: false,
                column_default: None,
                default_generated: false,
                generation_expression: None,
            })
            .collect(),
    }
}
fn build(d: TableDescriptor, columns: Vec<String>) -> MysqlStartupRecord {
    MysqlStartupRecord::build(
        &SourceIdentity {
            engine: "mysql".into(),
            system_id: "cluster".into(),
            lineage: "epoch".into(),
            db_name: "s".into(),
            stream: "main".into(),
        },
        GtidMode::OFF,
        MysqlVersion::V80,
        &file(),
        &[SelectedTable {
            schema: "s".into(),
            table: "t".into(),
            columns,
            primary_key: vec!["id".into()],
        }],
        &[d],
        vec!["mysql.v1".into(), "skva.v1".into()],
    )
    .unwrap()
}
fn record() -> MysqlStartupRecord {
    build(descriptor(), vec!["id".into(), "v".into()])
}
fn check(a: &MysqlStartupRecord, b: MysqlStartupRecord) -> std::result::Result<(), StartupDrift> {
    a.check_restart(&ObservedStartup {
        record: b,
        resume_plan: None,
    })
}
#[test]
fn round_trip_and_golden() {
    let r = record();
    let text = r.encode().unwrap();
    assert_eq!(MysqlStartupRecord::decode(&text).unwrap(), r);
    assert_eq!(text, GOLDEN);
    assert_eq!(r.start_position().unwrap(), file());
}
const GOLDEN: &str = r#"{"version":"skedaddle-mysql-startup-v1","source":["mysql","cluster","epoch","s","main"],"gtid_mode":"OFF","version_family":"8.0","start_position":[70,1,97,97,97,97,97,97,97,97,45,97,97,97,97,45,97,97,97,97,45,97,97,97,97,45,97,97,97,97,97,97,97,97,97,97,97,97,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,4],"tables":[{"selection":{"schema":"s","table":"t","columns":["id","v"],"primary_key":["id"]},"descriptor_digest":"347954e033eba55ab15ba2cb9ce21fec8a66102948857843b89ac127468257df"}],"mapping_versions":["mysql.v1","skva.v1"],"mapping_digest":"af3cde79d255fab6190ddcb2d8c53eb874196b3aa762ab8201c60d186c271a83","destination_schema_digest":"38f72352356217a87dca47fcf635f7d411f0120e53576beac7fa02fcd29f1dc1"}"#;
#[test]
fn strict_decode() {
    let r = record();
    let v = serde_json::to_value(r).unwrap();
    for mutation in 0..9 {
        let mut v = v.clone();
        match mutation {
            0 => {
                v["version"] = "future".into();
            }
            1 => {
                v.as_object_mut().unwrap().remove("version");
            }
            2 => {
                v["extra"] = true.into();
            }
            3 => {
                v["start_position"] = serde_json::json!([70, 2]);
            }
            4 => {
                v["tables"][0]["selection"]["extra"] = true.into();
            }
            5 => {
                v["mapping_digest"] = "XYZ".into();
            }
            6 => {
                v["version_family"] = "8.1".into();
            }
            7 => {
                v["tables"][0]["selection"]["columns"] = serde_json::json!(["id", "id"]);
            }
            _ => {
                v["start_position"][53] = 0.into();
            }
        }
        assert!(
            MysqlStartupRecord::decode(&v.to_string()).is_err(),
            "mutation {mutation}"
        );
    }
}
#[test]
fn each_drift() {
    use StartupDrift::*;
    let r = record();
    assert_eq!(check(&r, r.clone()), Ok(()));
    for (mutated, expected) in [
        (
            {
                let mut x = r.clone();
                x.source[1] = "other".into();
                x
            },
            IdentityChanged,
        ),
        (
            {
                let mut x = r.clone();
                x.gtid_mode = GtidMode::ON;
                x
            },
            GtidModeChanged,
        ),
        (
            {
                let mut x = r.clone();
                x.version_family = VersionFamily::V84;
                x
            },
            VersionFamilyChanged,
        ),
        (
            {
                let mut x = r.clone();
                x.tables.clear();
                x
            },
            TablesChanged,
        ),
        (
            {
                let mut x = r.clone();
                x.tables[0].selection.columns.reverse();
                x
            },
            ProjectionChanged {
                table: "s.t".into(),
            },
        ),
        (
            {
                let mut d = descriptor();
                d.columns[2].nullable = true;
                build(d, vec!["id".into(), "v".into()])
            },
            DescriptorChanged {
                table: "s.t".into(),
            },
        ),
        (
            {
                let mut x = r.clone();
                x.mapping_versions.push("v2".into());
                x
            },
            MappingChanged,
        ),
        (
            {
                let mut x = r.clone();
                x.mapping_digest = "0".repeat(64);
                x
            },
            MappingChanged,
        ),
        (
            {
                let mut x = r.clone();
                x.destination_schema_digest = "0".repeat(64);
                x
            },
            DestinationSchemaChanged,
        ),
    ] {
        assert_eq!(check(&r, mutated), Err(expected));
    }
}
#[test]
fn canonical_input_order() {
    let mut d = descriptor();
    d.columns.reverse();
    assert_eq!(record(), build(d, vec!["id".into(), "v".into()]));
}
#[test]
fn identity_failover_uses_resume_decision() {
    let mut r = record();
    let gtid = MysqlPosition::Gtid {
        server_uuid: UUID.into(),
        set: format!("{UUID}:1-3").parse().unwrap(),
        file: "bin.000001".into(),
        offset: 4,
    };
    r.gtid_mode = GtidMode::ON;
    r.start_position = gtid.encode().unwrap();
    let observed = MysqlStatus {
        server_uuid: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into(),
        executed: format!("{UUID}:1-4").parse().unwrap(),
        purged: Default::default(),
        gtid_on: true,
        file: "bin.000009".into(),
        offset: 100,
        binary_logs: vec![("bin.000009".into(), 100)],
    };
    let plan = resume::check(&gtid, &observed).unwrap();
    let mut live = r.clone();
    live.start_position = MysqlPosition::Gtid {
        server_uuid: observed.server_uuid.clone(),
        set: observed.executed.clone(),
        file: observed.file.clone(),
        offset: observed.offset,
    }
    .encode()
    .unwrap();
    assert_eq!(check(&r, live.clone()), Err(StartupDrift::IdentityChanged));
    assert_eq!(
        r.check_restart(&ObservedStartup {
            record: live.clone(),
            resume_plan: Some(plan.clone())
        }),
        Ok(())
    );
    let advanced = live.start_position().unwrap();
    let advanced_plan = resume::check(&advanced, &observed).unwrap();
    assert_eq!(
        r.check_restart(&ObservedStartup {
            record: live.clone(),
            resume_plan: Some(advanced_plan)
        }),
        Ok(())
    );
    live.source[1] = "other cluster".into();
    assert_eq!(
        r.check_restart(&ObservedStartup {
            record: live,
            resume_plan: Some(plan.clone())
        }),
        Err(StartupDrift::IdentityChanged)
    );
    let f = record();
    let mut live = f.clone();
    let mut bytes = live.start_position.clone();
    bytes[2] = b'b';
    live.start_position = bytes;
    assert_eq!(
        f.check_restart(&ObservedStartup {
            record: live,
            resume_plan: Some(plan.clone())
        }),
        Err(StartupDrift::IdentityChanged)
    );
    let empty = MysqlPosition::Gtid {
        server_uuid: UUID.into(),
        set: Default::default(),
        file: "bin.000001".into(),
        offset: 4,
    };
    r.start_position = empty.encode().unwrap();
    let mut live = r.clone();
    live.start_position[2] = b'b';
    assert_eq!(
        r.check_restart(&ObservedStartup {
            record: live,
            resume_plan: None
        }),
        Err(StartupDrift::IdentityChanged)
    );
}
#[test]
fn schema_stop_round_trip_strict() {
    let reasons = vec![
        CaptureStopReason::Ddl {
            tables: vec![("s".into(), "t".into())],
            statement: "ALTER TABLE t ADD x INT".into(),
        },
        CaptureStopReason::UnknownDdl {
            statement: "DDL".into(),
            reason: "unknown".into(),
        },
        CaptureStopReason::TableMap {
            schema: "s".into(),
            table: "t".into(),
            change: TableMapChange::ColumnCount {
                expected: 2,
                actual: 3,
            },
        },
        CaptureStopReason::TableMap {
            schema: "s".into(),
            table: "t".into(),
            change: TableMapChange::Column {
                ordinal: 1,
                name: "id".into(),
                field: "signedness",
            },
        },
    ];
    for reason in reasons {
        let stop = MysqlSchemaStopRecord::new(&reason, &file()).unwrap();
        let text = stop.encode().unwrap();
        let decoded = MysqlSchemaStopRecord::decode(&text).unwrap();
        assert_eq!(decoded, stop);
        assert_eq!(decoded.reason(), reason);
        assert_eq!(decoded.boundary().unwrap(), file());
        let v = serde_json::to_value(&stop).unwrap();
        for mutation in 0..5 {
            let mut v = v.clone();
            match mutation {
                0 => {
                    v["version"] = "future".into();
                }
                1 => {
                    v.as_object_mut().unwrap().remove("version");
                }
                2 => {
                    v["extra"] = true.into();
                }
                3 => {
                    v["boundary"] = serde_json::json!([70, 1]);
                }
                _ => {
                    let obj = v["reason"]
                        .as_object_mut()
                        .unwrap()
                        .values_mut()
                        .next()
                        .unwrap();
                    obj["extra"] = true.into();
                }
            }
            assert!(MysqlSchemaStopRecord::decode(&v.to_string()).is_err());
        }
    }
}
