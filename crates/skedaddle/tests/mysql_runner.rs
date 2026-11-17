//! Real CLI assess/start and library resume, in one harness-owned paired cell.
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use skedaddle::{MigrationDefinition, coordinator::State, mysql::client::MysqlConfig};
use std::{fs, path::Path, process::Command};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn checked_run(
    result: std::result::Result<skedaddle::engine::Reached, skedaddle::runner::RunError>,
) -> Result<skedaddle::engine::Reached> {
    result.map_err(|e| match e {
        skedaddle::runner::RunError::Engine(e) => format!("runner engine: {e}").into(),
        skedaddle::runner::RunError::Preparation(e) => {
            format!("runner preparation: {}", e.what).into()
        }
    })
}
fn secret(root: &Path, name: &str, value: &str) -> String {
    let path = root.join(name);
    fs::write(&path, value).unwrap();
    format!("file:{}", path.to_str().unwrap().replace('\\', "/"))
}
fn cli(root: &Path, args: &[&str], code: i32) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .arg("--state-root")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    let result: Value = serde_json::from_slice(&output.stdout).expect("CLI JSON");
    assert_eq!(output.status.code(), Some(code), "CLI response: {result}");
    result
}
#[tokio::test]
async fn live_copy_dml_pause_drop_resume_and_quiet_catchup() -> Result {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: mysql_runner requires MySQL harness with SKEDADDLE_MYSQL_WITH_PG=1"
        );
        return Ok(());
    };
    let cfg: MysqlConfig = serde_json::from_slice(&fs::read(path)?)?;
    let pg: Value = serde_json::from_slice(&fs::read(
        std::env::var_os("SKEDADDLE_TEST_CONFIG").ok_or("paired PG required")?,
    )?)?;
    let root = std::path::PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT").ok_or("test root required")?,
    )
    .join(format!("mysql-runner-{}", uuid::Uuid::new_v4().simple()));
    fs::create_dir_all(&root)?;
    let readme = include_str!("../README.md");
    let mut value: Value = serde_json::from_str(
        readme
            .split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )?;
    value["buffer"]["backend"] = json!({"type":"local-fs","options":{"path":root.join("buffer")}});
    value["source"] = json!({"engine":"mysql", "version":if cfg.profile == "mysql84" {"8.4"} else {"8.0"},
        "tls_ca_ref":secret(&root,"mysql-ca",cfg.ca.to_str().unwrap()),
        "mysql":{"host":cfg.host,"port":cfg.port,"user":secret(&root,"mysql-user",&cfg.user),
        "password":secret(&root,"mysql-password",&cfg.password),"gtid_mode":cfg.gtid}});
    value["destination"]["connection_ref"] = json!(secret(
        &root,
        "pg-dsn",
        pg["pg_apply"]
            .as_str()
            .or(pg["dsn"].as_str())
            .ok_or("dsn required")?
    ));
    value["destination"]["tls_ca_ref"] = json!(secret(
        &root,
        "pg-ca",
        pg["ca_pem"].as_str().ok_or("CA required")?
    ));
    let schema = format!("f4b1_{}", uuid::Uuid::new_v4().simple());
    value["selected_objects"] = json!([{"kind":"table","schema":schema,"name":"items"}]);
    let definition = MigrationDefinition::from_json(&value.to_string())?;
    let mut source = cfg.connect().await?;
    source
        .conn
        .query_drop("DROP TABLE IF EXISTS _skedaddle.watermark")
        .await?;
    source
        .conn
        .query_drop(format!("CREATE DATABASE `{schema}`"))
        .await?;
    source
        .conn
        .query_drop(format!(
            "CREATE TABLE `{schema}`.items (id INT PRIMARY KEY, v INT)"
        ))
        .await?;
    source
        .conn
        .query_drop(format!(
            "INSERT INTO `{schema}`.items VALUES {}",
            (1..=6000)
                .map(|i| format!("({i},{i})"))
                .collect::<Vec<_>>()
                .join(",")
        ))
        .await?;
    let destination = skedaddle::pg::tls::Config::new(
        pg["pg_apply"]
            .as_str()
            .or(pg["dsn"].as_str())
            .unwrap()
            .parse()?,
        pg["ca_pem"].as_str().unwrap(),
    );
    let (pg_client, driver, _) =
        skedaddle::pg::tls::connect(&destination, &skedaddle::security::TlsPolicy::Required)
            .await?;
    tokio::spawn(driver);
    let result: Result = async {
        let path = root.join("definition.json"); fs::write(&path,value.to_string())?;
        let init = cli(&root,&["init",path.to_str().unwrap()],0);
        let id = init["data"]["id"].as_str().ok_or("init ID missing")?;
        let journal = skedaddle::runner::open_journal(&root,id).await?;
        let (_keep, receiver) = tokio::sync::watch::channel(false);
        assert!(matches!(skedaddle::runner::run(&root,id,&definition,false,skedaddle::engine::StopAt::CatchingUp,receiver).await,
            Err(skedaddle::runner::RunError::Preparation(skedaddle::runner::Failure {kind:skedaddle::runner::FailureKind::InvalidInput,..}))));
        let before = journal.load(true).await?;
        assert_eq!(before.lifecycle.state,State::Defined); assert!(before.progress.is_empty()); assert!(before.intent_roots.is_empty());
        cli(&root,&["assess",id,"--estimated-copy-seconds","10","--planned-catchup-seconds","20","--max-decompressed-bytes",&cfg.max_decompressed_bytes.to_string()],0);
        assert_eq!(journal.load(true).await?.lifecycle.state,State::Assessed);
        let (_keep, receiver) = tokio::sync::watch::channel(false);
        assert!(matches!(skedaddle::runner::run(&root,id,&definition,false,
            skedaddle::engine::StopAt::VerifiedReady,receiver).await,
            Err(skedaddle::runner::RunError::Preparation(skedaddle::runner::Failure {
                kind:skedaddle::runner::FailureKind::Blocked,..}))));
        assert_eq!(journal.load(true).await?.lifecycle.state,State::Assessed);
        // GTID OFF currently fails before copy starts. Flip this assertion when
        // the binding v2 proposal (GD-0064 entry 131) lands.
        if cfg.gtid == "OFF" {
            let (_keep, receiver) = tokio::sync::watch::channel(false);
            let result = tokio::time::timeout(std::time::Duration::from_secs(120),
                skedaddle::runner::run(&root,id,&definition,false,
                    skedaddle::engine::StopAt::CatchingUp,receiver)).await?;
            assert!(matches!(result,
                Err(skedaddle::runner::RunError::Engine(skedaddle::coordinator::Error::RecoveryRequired(message)))
                    if message.contains("MySQL startup/binding order domain mismatch")));
            let exists: bool = pg_client.query_one("SELECT to_regclass($1) IS NOT NULL", &[&format!("{schema}.items")]).await?.get(0);
            if exists {
                let rows: i64 = pg_client.query_one(&format!("SELECT count(*) FROM {schema}.items"), &[]).await?.get(0);
                assert_eq!(rows,0,"GTID OFF must fail before any copy/apply rows");
            }
            let replayed = skedaddle::runner::open_journal(&root,id).await?.load(true).await?;
            assert!(replayed.chunk_attempts.is_empty());
            assert!(replayed.progress.contains_key("source-startup"));
            return Ok(());
        }
        // The real runner owns every adapter. Poll durable chunk starts to
        // schedule DML and pause while copy is active, without replacing DB IO.
        let (pause, receiver) = tokio::sync::watch::channel(false);
        let mut running = Box::pin(skedaddle::runner::run(&root,id,&definition,false,
            skedaddle::engine::StopAt::UntilPaused,receiver));
        let mutate_and_pause = async {
            loop {
                let state = journal.load(true).await?;
                if !state.chunk_attempts.is_empty() { break; }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            source.conn.query_drop(format!("INSERT INTO `{schema}`.items VALUES (9001,11)")).await?;
            source.conn.query_drop(format!("UPDATE `{schema}`.items SET v=42 WHERE id=2500")).await?;
            source.conn.query_drop(format!("DELETE FROM `{schema}`.items WHERE id=2600")).await?;
            source.conn.query_drop(format!("UPDATE `{schema}`.items SET id=9500 WHERE id=2700")).await?;
            pause.send_replace(true);
            Ok::<(),Box<dyn std::error::Error + Send + Sync>>(())
        };
        let (paused, ()) = tokio::time::timeout(std::time::Duration::from_secs(120), async {
            tokio::try_join!(async { checked_run(running.as_mut().await) }, mutate_and_pause)
        }).await??;
        assert_eq!(paused.state,State::Paused);
        drop(running);
        let state = journal.load(true).await?;
        let startup = state.progress.get("source-startup").ok_or("startup missing")?.clone();
        let plans = state.chunk_plans.clone();
        assert_eq!(plans.len(),1);
        // Kill the resumed future mid-copy after a new attempt is durably
        // started. Dropping it forces restart from retained writer/v3 receipts.
        let attempts = state.chunk_attempts.clone();
        let (_keep, receiver) = tokio::sync::watch::channel(false);
        {
            let mut running = Box::pin(skedaddle::runner::run(&root,id,&definition,true,
                skedaddle::engine::StopAt::UntilPaused,receiver));
            let started = async {
                loop {
                    let state = journal.load(true).await?;
                    let has_apply_progress: bool = pg_client.query_one(
                        "SELECT EXISTS (SELECT 1 FROM _skedaddle.apply_progress_v3 WHERE stream=$1)",
                        &[&format!("skedaddle-{id}-binlog")],
                    ).await?.get(0);
                    if state.chunk_attempts != attempts && state.lifecycle.state == State::Copying && has_apply_progress {
                        return Ok::<(),Box<dyn std::error::Error + Send + Sync>>(());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            };
            tokio::select! {
                result = &mut running => { checked_run(result)?; panic!("worker ended before mid-copy drop"); },
                result = tokio::time::timeout(std::time::Duration::from_secs(120), started) => {result??;}
            }
        }
        let (_keep, receiver) = tokio::sync::watch::channel(false);
        let reached = tokio::time::timeout(std::time::Duration::from_secs(120),
            skedaddle::runner::run(&root,id,&definition,true,skedaddle::engine::StopAt::CatchingUp,receiver)).await?;
        assert_eq!(checked_run(reached)?.state,State::CatchingUp);
        // Run the same live workers beyond the lifecycle boundary until the
        // quiet destination equals a complete ordered source read, then pause.
        source.conn.query_drop(format!("INSERT INTO `{schema}`.items VALUES (9901,77)")).await?;
        let expected: Vec<(i32,Option<i32>)> = source.conn.query(format!("SELECT id,v FROM `{schema}`.items ORDER BY id")).await?;
        let (pause, receiver) = tokio::sync::watch::channel(false);
        let running = skedaddle::runner::run(&root,id,&definition,true,skedaddle::engine::StopAt::UntilPaused,receiver);
        let compare = async {
            loop {
                let rows = pg_client.query(&format!("SELECT id,v FROM {schema}.items ORDER BY id"), &[]).await?;
                let actual = rows.iter().map(|r| (r.get::<_,i32>(0),r.get::<_,Option<i32>>(1))).collect::<Vec<_>>();
                if actual == expected { pause.send_replace(true); break; }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Ok::<(),Box<dyn std::error::Error + Send + Sync>>(())
        };
        let (paused, ()) = tokio::time::timeout(std::time::Duration::from_secs(120), async {
            tokio::try_join!(async { checked_run(running.await) },compare)
        }).await??;
        assert_eq!(paused.state,State::Paused);
        // The pre-assessment observer has no buffer binding. Reopen after copy
        // so replay authenticates the now-present ChunkDone row references.
        let journal = skedaddle::runner::open_journal(&root,id).await?;
        let reopened = journal.load(true).await?;
        assert_eq!(reopened.progress.get("source-startup"),Some(&startup)); assert_eq!(reopened.chunk_plans,plans);
        let mut after = 0; let mut planned = 0; let mut starts = 0;
        loop { let page = journal.history_page(after,128).await?; if page.is_empty() {break;}
            for entry in page { after = entry.seq; match entry.transition {
                skedaddle::coordinator::Transition::ChunkPlanned {..} => planned += 1,
                skedaddle::coordinator::Transition::Progress {stream,..} if stream == "source-startup" => starts += 1, _ => {} } }
        }
        assert_eq!((planned,starts),(1,1));
        let (_keep, receiver) = tokio::sync::watch::channel(false);
        let reached = tokio::time::timeout(std::time::Duration::from_secs(120),
            skedaddle::runner::run(&root,id,&definition,true,skedaddle::engine::StopAt::CatchingUp,receiver)).await?;
        assert_eq!(checked_run(reached)?.state,State::CatchingUp);
        let (_keep, receiver) = tokio::sync::watch::channel(false);
        assert!(matches!(skedaddle::runner::run(&root,id,&definition,true,
            skedaddle::engine::StopAt::VerifiedReady,receiver).await,
            Err(skedaddle::runner::RunError::Preparation(skedaddle::runner::Failure {
                kind:skedaddle::runner::FailureKind::Blocked,what,..}))
                if what == "MySQL verified-ready evidence producers are unavailable"));
        // Exercise the same authenticated public observer used by CLI status.
        let status = cli(&root,&["status",id],0);
        assert_eq!(status["data"]["state"]["lifecycle"]["state"],"catching-up");
        let verification = &status["data"]["verification"];
        assert_eq!(verification["verification"],false);
        assert_eq!(verification["completed_report"],Value::Null);
        let facts = verification["readiness"].as_object().ok_or("readiness facts missing")?;
        let blocking = facts.iter().filter(|(_,fact)| fact.get("ready") == Some(&json!(false)))
            .map(|(name,fact)| (name.as_str(),fact["blocker"]["code"].as_str().unwrap()))
            .collect::<std::collections::BTreeMap<_,_>>();
        assert_eq!(blocking,std::collections::BTreeMap::from([
            ("apply_lag","apply-lag-over-budget"),
            ("buffer_contract","buffer-contract-mismatch"),
            ("destination_validity","destination-invalid"),
            ("fence_risks","fence-risk-unacknowledged"),
            ("reverse_prerequisites","reverse-prerequisites-missing"),
            ("schema_jobs","schema-boundary-pending"),
            ("sequences_syncable","sequences-not-syncable"),
        ]));
        let mysql_gaps = facts.iter().filter(|(_,fact)| fact["blocker"]["reason"].as_str().is_some_and(|reason| reason.starts_with("MySQL source:")))
            .map(|(name,_)| name.as_str()).collect::<std::collections::BTreeSet<_>>();
        assert_eq!(mysql_gaps,std::collections::BTreeSet::from([
            "destination_validity","sequences_syncable","reverse_prerequisites","fence_risks",
        ]));
        source.conn.query_drop(format!("ALTER TABLE `{schema}`.items MODIFY id BIGINT NOT NULL")).await?;
        let (_keep, receiver) = tokio::sync::watch::channel(false);
        assert!(matches!(skedaddle::runner::run(&root,id,&definition,true,skedaddle::engine::StopAt::CatchingUp,receiver).await,
            Err(skedaddle::runner::RunError::Engine(skedaddle::coordinator::Error::RecoveryRequired(message))) if message == "MySQL saved chunk key drift"));
        assert_eq!(journal.load(true).await?.chunk_plans, plans);
        Ok(())
    }.await;
    pg_client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await?;
    source
        .conn
        .query_drop(format!("DROP DATABASE `{schema}`"))
        .await?;
    result
}
