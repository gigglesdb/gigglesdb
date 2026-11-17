use super::*;
fn installer() -> MysqlInstaller {
    let source = MysqlConfig {
        host: "secret-host".into(),
        port: 1,
        ca: "secret-ca".into(),
        user: "secret-user".into(),
        password: "secret-password".into(),
        profile: "mysql84".into(),
        gtid: "ON".into(),
        container_id: "secret-container".into(),
        max_decompressed_bytes: 1024,
    };
    let destination = pg::tls::Config::new(
        "host=secret-pg password=secret-destination"
            .parse()
            .unwrap(),
        "secret-pg-ca",
    );
    let identity = SourceIdentity {
        engine: "mysql".into(),
        system_id: "cluster".into(),
        lineage: "epoch".into(),
        db_name: "s".into(),
        stream: "main".into(),
    };
    let mut table = control::descriptor();
    table.schema = "s".into();
    table.table = "t".into();
    table.columns.truncate(1);
    let selected = SelectedTable {
        schema: "s".into(),
        table: "t".into(),
        columns: vec!["id".into()],
        primary_key: vec!["id".into()],
    };
    MysqlInstaller::new(source, destination, identity, vec![table], vec![selected]).unwrap()
}
#[test]
fn plan_order_and_marker_scope() {
    let i = installer();
    assert_eq!(
        i.plan(),
        &[
            Installation::ControlSchema,
            Installation::SourceStartup,
            Installation::DestinationSchema,
            Installation::ProgressTables
        ]
    );
    for &s in i.plan() {
        if s != Installation::SourceStartup {
            assert!(i.markers_after(s).unwrap().is_empty());
        }
    }
    assert!(i.markers_after(Installation::SourceStartup).is_err());
}
#[tokio::test]
async fn pg_steps_rejected_before_connections() {
    let mut i = installer();
    for step in [
        Installation::Publication,
        Installation::DdlTrigger,
        Installation::Slot,
    ] {
        assert!(
            matches!(i.install_step(step).await, Err(InstallerError::UnsupportedStep(s)) if s == step)
        );
        assert!(
            matches!(i.install(step).await, Err(crate::coordinator::Error::Mysql(MysqlError::Installer(e)))
            if matches!(*e, InstallerError::UnsupportedStep(s) if s == step))
        );
    }
    assert!(i.source.is_none() && i.destination.is_none());
}
#[test]
fn stable_nonsecret_identity() {
    let mut i = installer();
    let before = i.identity();
    assert!(!before.contains("secret"));
    let json: serde_json::Value = serde_json::from_str(&before).unwrap();
    assert_eq!(json["connector"], "mysql");
    assert_eq!(json["projection"][0]["columns"][0], "id");
    assert_eq!(
        json["destination_schema_digest"].as_str().unwrap().len(),
        64
    );
    i.source_config.password = "changed".into();
    i.source_config.host = "another".into();
    assert_eq!(before, i.identity());
}
#[tokio::test]
async fn recovered_record_reused_without_connecting() {
    let mut i = installer();
    let record = MysqlStartupRecord::build(
        &i.identity,
        GtidMode::OFF,
        MysqlVersion::V84,
        &MysqlPosition::File {
            server_uuid: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
            file_seq: 1,
            offset: 4,
        },
        &i.selected,
        &i.tables,
        vec![MAPPING_VERSION.into(), VALUE_VERSION.into()],
    )
    .unwrap();
    i.startup_record = Some(record.clone());
    for _ in 0..2 {
        i.install(Installation::SourceStartup).await.unwrap();
    }
    assert_eq!(i.startup_record.as_ref(), Some(&record));
    assert!(i.source.is_none());
    let m = i
        .markers_after(Installation::SourceStartup)
        .unwrap()
        .remove(0);
    assert_eq!(m.stream, "source-startup");
    assert_eq!(m.label, "installed");
    assert_eq!(MysqlStartupRecord::decode(&m.marker).unwrap(), record);
}
