//! Real CLI assess/start and library resume, in one harness-owned paired cell.
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use skedaddle::{MigrationDefinition, coordinator::State, mysql::client::MysqlConfig};
use std::{fs, path::Path, process::Command};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
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
async fn live_assess_start_and_resume_keep_startup_and_plans() -> Result {
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
        .query_drop(format!("INSERT INTO `{schema}`.items VALUES (1,2),(2,3)"))
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
        let (_, receiver) = tokio::sync::watch::channel(false);
        assert!(matches!(skedaddle::runner::run(&root,id,&definition,false,skedaddle::engine::StopAt::CatchingUp,receiver).await,
            Err(skedaddle::runner::RunError::Preparation(skedaddle::runner::Failure {kind:skedaddle::runner::FailureKind::InvalidInput,..}))));
        let before = journal.load(true).await?;
        assert_eq!(before.lifecycle.state,State::Defined); assert!(before.progress.is_empty()); assert!(before.intent_roots.is_empty());
        cli(&root,&["assess",id,"--estimated-copy-seconds","10","--planned-catchup-seconds","20","--max-decompressed-bytes",&cfg.max_decompressed_bytes.to_string()],0);
        assert_eq!(journal.load(true).await?.lifecycle.state,State::Assessed);
        // Engine errors currently use the CLI's infrastructure envelope; the
        // native typed NotWired is checked directly on the same runner below.
        cli(&root,&["start",id],6);
        let state = journal.load(true).await?;
        assert_eq!(state.lifecycle.state,State::Copying);
        assert!(state.progress.contains_key("mysql-assessment"));
        let startup = state.progress.get("source-startup").ok_or("startup missing")?.clone();
        skedaddle::mysql::startup::MysqlStartupRecord::decode(&startup.0)?;
        assert_eq!(state.chunk_plans.len(),1);
        let plans = state.chunk_plans.clone();
        let count: Option<u64> = source.conn.query_first("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='_skedaddle' AND table_name='watermark'").await?;
        assert_eq!(count,Some(1));
        for table in [format!("{schema}.items"),"_skedaddle.apply_progress_v3".into()] {
            assert!(pg_client.query_one("SELECT to_regclass($1) IS NOT NULL",&[&table]).await?.get::<_,bool>(0));
        }
        source.conn.query_drop(format!("INSERT INTO `{schema}`.items VALUES (3,4)")).await?;
        let (_, receiver) = tokio::sync::watch::channel(false);
        assert!(matches!(skedaddle::runner::run(&root,id,&definition,true,skedaddle::engine::StopAt::CatchingUp,receiver).await,
            Err(skedaddle::runner::RunError::Engine(skedaddle::coordinator::Error::SourceBinding(skedaddle::runner::SourceBindingError::NotWired(skedaddle::runner::SourceConnector::Mysql))))));
        let reopened = journal.load(true).await?;
        assert_eq!(reopened.progress.get("source-startup"),Some(&startup)); assert_eq!(reopened.chunk_plans,plans);
        let mut after = 0; let mut planned = 0; let mut starts = 0;
        loop { let page = journal.history_page(after,128).await?; if page.is_empty() {break;}
            for entry in page { after = entry.seq; match entry.transition {
                skedaddle::coordinator::Transition::ChunkPlanned {..} => planned += 1,
                skedaddle::coordinator::Transition::Progress {stream,..} if stream == "source-startup" => starts += 1, _ => {} } }
        }
        assert_eq!((planned,starts),(1,1));
        source.conn.query_drop(format!("ALTER TABLE `{schema}`.items MODIFY id BIGINT NOT NULL")).await?;
        let (_, receiver) = tokio::sync::watch::channel(false);
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
