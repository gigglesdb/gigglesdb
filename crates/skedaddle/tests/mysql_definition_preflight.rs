//! Real definition/runner preflight, using only harness-provisioned MySQL.
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use skedaddle::{
    MigrationDefinition,
    capability::ItemStatus,
    mysql::{client::MysqlConfig, preflight::Options},
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

struct Tables {
    config: MysqlConfig,
    names: Vec<String>,
}
impl Drop for Tables {
    fn drop(&mut self) {
        let config = self.config.clone();
        let names = self.names.clone();
        let clean = std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    let Ok(mut client) = config.connect().await else {
                        return false;
                    };
                    for name in names {
                        if client
                            .conn
                            .query_drop(format!("DROP TABLE IF EXISTS type_zoo.`{name}`"))
                            .await
                            .is_err()
                        {
                            return false;
                        }
                    }
                    client.conn.disconnect().await.is_ok()
                })
        })
        .join()
        .unwrap_or(false);
        assert!(
            clean,
            "MySQL scoped table cleanup failed (details redacted)"
        );
    }
}
fn secret(root: &Path, name: &str, value: &str) -> String {
    let path = root.join(name);
    fs::write(&path, value).unwrap();
    format!("file:{}", path.to_str().unwrap().replace('\\', "/"))
}
fn options(config: &MysqlConfig) -> Options {
    Options {
        estimated_copy_seconds: 10,
        planned_catchup_seconds: 20,
        max_decompressed_bytes: config.max_decompressed_bytes,
        selected_tables: vec![],
    }
}
async fn assess(
    def: &MigrationDefinition,
    config: &MysqlConfig,
    destination: bool,
) -> skedaddle::CapabilityReport {
    let result = if destination {
        skedaddle::runner::assess_mysql(def, options(config)).await
    } else {
        skedaddle::runner::assess_mysql_source(def, options(config)).await
    };
    result.expect("definition runner preflight failed (redacted)")
}
#[tokio::test]
async fn mysql_definition_preflight_clean_mapping_and_declared_mode() {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; use a mysql80|mysql84 harness lane"
        );
        return;
    };
    let config: MysqlConfig = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    // Runs on every harness variant: declare the observed version family and GTID mode.
    let version = match config.profile.as_str() {
        "mysql80" => "8.0",
        "mysql84" => "8.4",
        other => panic!("unexpected MySQL harness profile {other}"),
    };
    let observed = config.gtid.clone();
    let opposite = if observed == "ON" { "OFF" } else { "ON" };
    let base =
        PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("harness test root required"));
    let root = base.join(format!(
        "definition-preflight-{}",
        uuid::Uuid::new_v4().simple()
    ));
    fs::create_dir_all(&root).unwrap();
    let readme = include_str!("../README.md");
    let mut value: Value = serde_json::from_str(
        readme
            .split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )
    .unwrap();
    value["buffer"]["backend"] =
        json!({"type":"local-fs", "options":{"path":root.join("buffer").to_str().unwrap()}});
    value["source"] = json!({"engine":"mysql", "version":version, "tls_ca_ref":secret(&root,"mysql-ca",config.ca.to_str().unwrap()),
        "mysql":{"host":config.host, "port":config.port, "user":secret(&root,"mysql-user",&config.user),
        "password":secret(&root,"mysql-password",&config.password), "gtid_mode":observed.as_str()}});
    let destination = if let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") {
        let pg: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        if let (Some(dsn), Some(ca)) = (pg["pg_apply"].as_str(), pg["ca_pem"].as_str()) {
            value["destination"]["connection_ref"] = json!(secret(&root, "pg-dsn", dsn));
            value["destination"]["tls_ca_ref"] = json!(secret(&root, "pg-ca", ca));
            true
        } else {
            eprintln!(
                "MYSQL PREFLIGHT: MySQL-side only; PG config lacks pg_apply/ca_pem, destination remains Unverified"
            );
            false
        }
    } else {
        eprintln!(
            "MYSQL PREFLIGHT: MySQL-side only; SKEDADDLE_TEST_CONFIG absent, destination remains Unverified"
        );
        false
    };
    let names = vec![
        format!("f2_clean_{}", std::process::id()),
        format!("f2_bad_{}", std::process::id()),
    ];
    let cleanup = Tables {
        config: config.clone(),
        names: names.clone(),
    };
    let mut client = config
        .connect()
        .await
        .unwrap_or_else(|_| panic!("MySQL harness connect failed (redacted)"));
    client.conn.query_drop(format!("CREATE TABLE type_zoo.`{}` (id BIGINT PRIMARY KEY, body VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin) ENGINE=InnoDB",names[0])).await.unwrap_or_else(|_| panic!("clean DDL failed (redacted)"));
    client.conn.query_drop(format!("CREATE TABLE type_zoo.`{}` (id BIGINT PRIMARY KEY, location POINT, legacy VARCHAR(40) CHARACTER SET latin1) ENGINE=InnoDB",names[1])).await.unwrap_or_else(|_| panic!("mapping DDL failed (redacted)"));
    client
        .conn
        .disconnect()
        .await
        .unwrap_or_else(|_| panic!("disconnect failed (redacted)"));
    value["selected_objects"] = json!([{"schema":"type_zoo","name":names[0],"kind":"table"}]);
    let def = MigrationDefinition::from_json(&value.to_string()).unwrap();
    let report = assess(&def, &config, destination).await;
    assert_eq!(report.definition_digest, def.canonical_digest().unwrap());
    let blockers: Vec<_> = report
        .blocking()
        .into_iter()
        .filter(|i| i.id.starts_with("source."))
        .map(|i| &i.id)
        .collect();
    assert!(blockers.is_empty(), "clean MySQL blockers: {blockers:?}");
    assert!(
        !serde_json::to_string(&report)
            .unwrap()
            .contains(&config.password)
    );
    assert!(
        report
            .items
            .iter()
            .all(|i| !i.id.contains("wal_level") && !i.id.contains("slot_headroom"))
    );
    if !destination {
        assert!(
            report
                .blocking()
                .iter()
                .any(|i| i.id == "destination.pg.preflight")
        );
    }
    value["source"]["mysql"]["gtid_mode"] = json!(opposite);
    let report = assess(
        &MigrationDefinition::from_json(&value.to_string()).unwrap(),
        &config,
        destination,
    )
    .await;
    assert!(
        report
            .blocking()
            .iter()
            .any(|i| i.id == "source.mysql.declared_gtid_mode"
                && i.status == ItemStatus::RequiresExternalChange)
    );
    value["source"]["mysql"]["gtid_mode"] = json!(observed.as_str());
    value["selected_objects"][0]["name"] = json!(names[1]);
    let report = assess(
        &MigrationDefinition::from_json(&value.to_string()).unwrap(),
        &config,
        destination,
    )
    .await;
    for column in ["location", "legacy"] {
        assert!(
            report
                .blocking()
                .iter()
                .any(|i| i.id == format!("source.mysql.mapping.{column}")
                    && !i.remediation.is_empty())
        );
    }
    // CLI remains offline at check, and redacts endpoint credential references.
    let path = root.join("definition.json");
    fs::write(&path, value.to_string()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .arg("check")
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let envelope: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(envelope["outcome"], "blocked");
    let (pause, receiver) = tokio::sync::watch::channel(false);
    let result = skedaddle::runner::run(
        &root,
        "unwired",
        &def,
        false,
        skedaddle::engine::StopAt::CatchingUp,
        receiver,
    )
    .await;
    let Err(skedaddle::runner::RunError::Preparation(error)) = result else {
        panic!("MySQL runtime must refuse")
    };
    assert!(matches!(
        error.kind,
        skedaddle::runner::FailureKind::NotWired
    ));
    assert_eq!(error.what, "MySQL source runtime not wired yet");
    drop(pause);
    drop(cleanup);
}
