use super::*;
use crate::mysql::schema::ColumnDescriptor;

fn table() -> Table {
    let column = ColumnDescriptor {
        name: "id".into(),
        ordinal: 1,
        data_type: "bigint".into(),
        column_type: "bigint".into(),
        character_set: None,
        collation: None,
        nullable: false,
        generated: false,
        column_default: None,
        default_generated: false,
        generation_expression: None,
    };
    Table {
        plans: vec![plan_column(&column).unwrap()],
        descriptor: TableDescriptor {
            schema: "public".into(),
            table: "rows".into(),
            columns: vec![column],
            primary_key: vec!["id".into()],
        },
    }
}
fn config() -> Config {
    Config::new("m11m3", 180000, vec![table()]).unwrap()
}

#[test]
fn saved_plans_must_match_every_descriptor_and_conversion_field() {
    qualify_plans(&table()).unwrap();
    for change in 0..8 {
        let mut t = table();
        match change {
            0 => t.plans.clear(),
            1 => t.plans[0].type_id.push('x'),
            2 => t.plans[0].type_modifier = 42,
            3 => t.plans[0].generated = true,
            4 => t.plans[0].source.nullable = true,
            5 => t.plans[0].ddl_type = "text".into(),
            6 => t.plans[0].pg_type = crate::mapping::PgType::Int4,
            _ => t.plans[0].check = Some("false".into()),
        }
        assert!(qualify_plans(&t).is_err(), "change {change}");
    }
}
#[test]
fn mysql_schema_drift_including_key_and_generated_expression_is_rejected() {
    let t = table();
    qualify_descriptors(
        std::slice::from_ref(&t),
        std::slice::from_ref(&t.descriptor),
    )
    .unwrap();
    for change in 0..6 {
        let mut d = t.descriptor.clone();
        match change {
            0 => d.primary_key.clear(),
            1 => d.columns[0].nullable = true,
            2 => d.columns[0].generation_expression = Some("1".into()),
            3 => d.columns[0].column_type = "bigint unsigned".into(),
            4 => d.schema.push('x'),
            _ => d.columns[0].column_default = Some("3".into()),
        }
        assert!(qualify_descriptors(std::slice::from_ref(&t), &[d]).is_err());
    }
    assert!(qualify_descriptors(&[t], &[]).is_err());
}
#[test]
fn publication_aliases_and_buffer_order_remain_pg_native() {
    let c = config();
    assert_eq!(alias(&table().descriptor), "\"public\".\"rows\"");
    let mut d = table().descriptor;
    d.schema = "P\"G".into();
    assert_eq!(alias(&d), "\"P\"\"G\".\"rows\"");
    assert_eq!(
        c.plans().keys().collect::<Vec<_>>(),
        c.descriptors().keys().collect::<Vec<_>>()
    );
    let w = c.writer_config(7);
    assert_eq!(w.prefix, "reverse-runner-v1");
    assert_eq!(w.migration_id, "m11m3");
    assert_eq!(w.stream, "m11m3_rev");
    assert_eq!(
        Config::new("ends_rev", 180000, vec![table()])
            .unwrap()
            .writer_config(1)
            .migration_id,
        "ends_rev"
    );
    let order = c.order();
    assert_eq!(order.domain(), "m11m3_rev");
    assert_eq!(
        order
            .compare(&255u64.to_be_bytes(), &256u64.to_be_bytes())
            .unwrap(),
        std::cmp::Ordering::Less
    );
    assert!(order.compare(&[0], &[1]).is_err());
    assert!(
        c.postgres()
            .start_command(32)
            .unwrap()
            .contains("origin 'none'")
    );
}
#[test]
fn source_identity_is_exact_and_malformed_observations_fail_closed() {
    let c = config();
    for rows in [
        vec![],
        vec![vec!["1".into()]],
        vec![vec!["1".into(), "".into(), "db".into()]],
    ] {
        assert!(source_identity(&rows, c.postgres()).is_err());
    }
    let s = source_identity(
        &vec![vec!["12".into(), "3".into(), "db".into()]],
        c.postgres(),
    )
    .unwrap();
    assert_eq!(s.engine, "postgres");
    assert_eq!(s.system_id, "12");
    assert_eq!(s.lineage, "3");
    assert_eq!(s.db_name, "db");
    assert_eq!(s.stream, "skedaddle_m11m3_rev");
}
#[test]
fn rejects_empty_duplicate_and_keyless_inventories() {
    assert!(Config::new("m11m3", 180000, vec![]).is_err());
    assert!(Config::new("m11m3", 180000, vec![table(), table()]).is_err());
    let mut t = table();
    t.descriptor.primary_key.clear();
    assert!(Config::new("m11m3", 180000, vec![t]).is_err());
}

#[test]
fn combined_proof_checks_strict_activation_identity_and_schema() {
    let c = config();
    // Adversarial serialized proof tests only; production preparation is the
    // only constructor and returns after the final journal callback succeeds.
    let ready: Readiness = serde_json::from_value(serde_json::json!({
        "format":1, "capture": {"installation":"m11m3", "slot": {
            "slot":"skedaddle_m11m3_rev", "consistent_point":32
        }},
        "source": {"engine":"postgres", "system_id":"12", "lineage":"3",
            "db_name":"db", "stream":"skedaddle_m11m3_rev"},
        "schema_digest":c.schema.digest(), "mysql_server_uuid":"mysql-instance"
    }))
    .unwrap();
    assert!(ready.check_activation(&c, 32).is_err());
    assert!(ready.check_activation(&c, 31).is_err());
    ready.check_activation(&c, 33).unwrap();
    let intent = Intent::Ready(ready.clone());
    let encoded = serde_json::to_vec(&intent).unwrap();
    let Intent::Ready(recovered) = serde_json::from_slice(&encoded).unwrap() else {
        panic!()
    };
    assert_eq!(recovered, ready);
    for change in 0..6 {
        let mut bad = ready.clone();
        match change {
            0 => bad.schema_digest[0] ^= 1,
            1 => bad.source.engine = "mysql".into(),
            2 => bad.source.stream.push('x'),
            3 => bad.mysql_server_uuid.clear(),
            4 => bad.source.lineage.clear(),
            _ => bad.format = 2,
        }
        assert!(bad.check_activation(&c, 33).is_err());
    }
    assert!(
        ready
            .check_activation(&Config::new("other", 180000, vec![table()]).unwrap(), 33)
            .is_err()
    );
}

#[test]
fn capture_preparation_faults_precede_any_database_effect() {
    struct NoDatabase;
    impl fence::Connection for NoDatabase {
        fn query(&mut self, _: &str, _: Duration) -> std::result::Result<fence::Rows, String> {
            panic!("fault must prevent all SQL");
        }
    }
    let c = config();
    assert_eq!(
        c.pg.prepare_capture_bound(&mut NoDatabase, None, Duration::ZERO, |_, _, _| panic!(
            "timeout before callback"
        ))
        .unwrap_err(),
        pg::Error::Invalid("setup timeout")
    );
    assert_eq!(
        c.pg.prepare_capture_bound(
            &mut NoDatabase,
            None,
            Duration::from_secs(1),
            |_, endpoint, intent| {
                assert_eq!(endpoint, pg::Endpoint::Destination);
                assert_eq!(*intent, pg::Intent::Publication);
                Err(pg::Error::NotFenced)
            }
        )
        .unwrap_err(),
        pg::Error::NotFenced
    );
}

#[test]
fn sync_pg_runtime_callbacks_are_isolated_and_current_thread_is_rejected() {
    let outer = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let inner = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_eq!(
        outer.block_on(async { blocking(|| Ok(inner.block_on(async { 42 }))).unwrap() }),
        42
    );
    let current = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(matches!(
        current.block_on(async {
            blocking(|| -> Result<()> { panic!("must reject before effects") })
        }),
        Err(Error::Invalid(_))
    ));
}
