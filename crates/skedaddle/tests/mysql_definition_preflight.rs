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
    exercise(false).await;
}
#[tokio::test]
async fn mysql_forward_prepare_projects_columns_and_refuses_primary_key_exclusion() {
    exercise(true).await;
}
async fn exercise(prepare: bool) {
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
        if let (Some(dsn), Some(ca)) = (
            pg["pg_apply"].as_str().or_else(|| pg["dsn"].as_str()),
            pg["ca_pem"].as_str(),
        ) {
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
        format!("f2_clean_{}_{}", std::process::id(), prepare as u8),
        format!("f2_bad_{}_{}", std::process::id(), prepare as u8),
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
    if prepare {
        if !destination {
            eprintln!(
                "MYSQL SKIPPED: prepare requires SKEDADDLE_TEST_CONFIG PG destination; enable SKEDADDLE_MYSQL_WITH_PG=1"
            );
            return;
        }
        let mut projected = def.clone();
        projected.selected_objects[0].exclude_columns = vec!["body".into()];
        let prepared = skedaddle::runner::mysql_forward::prepare(&projected, options(&config))
            .await
            .unwrap_or_else(|e| panic!("prepare failed: {}", e.what));
        assert_eq!(
            prepared.descriptors[0]
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["id"]
        );
        assert_eq!(prepared.startup_inputs.selected[0].columns, ["id"]);
        assert_eq!(
            prepared.mapped_schema.digest(),
            skedaddle::mysql::dest_schema::build(&prepared.descriptors)
                .unwrap()
                .digest()
        );
        assert!(
            prepared
                .mapped_schema
                .statements()
                .iter()
                .all(|s| !s.contains("\"body\""))
        );
        projected.selected_objects[0].exclude_columns = vec!["id".into()];
        let Err(error) =
            skedaddle::runner::mysql_forward::prepare(&projected, options(&config)).await
        else {
            panic!("excluded key accepted")
        };
        assert!(matches!(
            error.kind,
            skedaddle::runner::FailureKind::Blocked
        ));
        assert_eq!(error.column.as_deref(), Some("id"));
        return;
    }
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
    if destination {
        assert!(
            report.blocking().is_empty(),
            "clean paired assessment blocked: {:?}",
            report.blocking()
        );
        assert!(
            report
                .items
                .iter()
                .any(|i| i.id == "destination.pg.mapped_schema"
                    && i.status == ItemStatus::Supported
                    && i.evidence.iter().any(|e| e.description
                        == "created at start by engine-install-destination-schema-v1"))
        );
        let prepared = skedaddle::runner::mysql_forward::prepare(&def, options(&config))
            .await
            .unwrap_or_else(|e| panic!("prepare failed: {}", e.what));
        let ddl = format!(
            "CREATE SCHEMA IF NOT EXISTS type_zoo; CREATE TABLE type_zoo.\"{}\" (id TEXT PRIMARY KEY)",
            names[0]
        );
        prepared.destination().batch_execute(&ddl).await.unwrap();
        let mismatched = assess(&def, &config, true).await;
        prepared
            .destination()
            .batch_execute(&format!("DROP TABLE type_zoo.\"{}\"", names[0]))
            .await
            .unwrap();
        assert!(
            mismatched
                .blocking()
                .iter()
                .any(|i| i.id == "destination.pg.mapped_schema"
                    && i.status == ItemStatus::Unsupported
                    && i.object.as_ref().is_some_and(|o| o.name == names[0])
                    && i.evidence.iter().any(|e| e.description.contains(&names[0]))),
            "mismatching destination must block mapped-schema assessment"
        );
        for statement in prepared.mapped_schema.statements() {
            prepared
                .destination()
                .batch_execute(statement)
                .await
                .unwrap();
        }
        let matching = assess(&def, &config, true).await;
        prepared
            .destination()
            .batch_execute(&format!("DROP TABLE type_zoo.\"{}\"", names[0]))
            .await
            .unwrap();
        assert!(
            matching.blocking().is_empty(),
            "matching destination blocked: {:?}",
            matching.blocking()
        );
        assert!(matching.items.iter().any(|i| {
            i.id == "destination.pg.mapped_schema"
                && i.status == ItemStatus::Supported
                && i.evidence
                    .iter()
                    .any(|e| e.description == "Existing destination matches mapped schema")
        }));
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
    // Start normally follows Define; the runtime journals its source binding
    // before refusing an unwired connector, without creating apply work.
    let journal = skedaddle::runner::open_journal(&root, "unwired")
        .await
        .unwrap();
    let mut owner = skedaddle::coordinator::epoch::Coordinator::observer(
        journal,
        skedaddle::coordinator::local::Local::create(root.join("seed-local")).unwrap(),
        "definition-preflight",
    )
    .unwrap();
    owner.acquire().await.unwrap();
    owner
        .commit(skedaddle::coordinator::Transition::Define {
            definition: Box::new(def.clone()),
        })
        .await
        .unwrap();
    drop(owner);
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
    // Missing durable assessment refuses before any connection or binding effect.
    match result {
        Err(skedaddle::runner::RunError::Preparation(error)) => {
            assert!(matches!(
                &error.kind,
                skedaddle::runner::FailureKind::InvalidInput
            ));
        }
        _ => panic!("MySQL run must reach a typed preparation failure"),
    }
    if !destination {
        return;
    }
    let state = skedaddle::runner::open_journal(&root, "unwired")
        .await
        .unwrap()
        .load(true)
        .await
        .unwrap();
    assert_eq!(state.source_binding, None);
    assert_eq!(
        state.lifecycle.state,
        skedaddle::coordinator::State::Defined
    );
    assert!(state.progress.is_empty());
    assert!(state.intent_roots.is_empty());
    assert!(state.chunk_plans.is_empty());
    drop(pause);
    drop(cleanup);
}
