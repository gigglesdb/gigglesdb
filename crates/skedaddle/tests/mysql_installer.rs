//! One harness-owned MySQL + PG18 cell, with real local journal and databases.
#[path = "support/root.rs"]
mod test_root;

use mysql_async::prelude::Queryable;
use skedaddle::{
    coordinator::{
        Event, State, Transition,
        epoch::Coordinator,
        journal::{Journal, local_store},
        local::Local,
    },
    engine::{Engine, Installation, Installer},
    event::SourceIdentity,
    mysql::{
        MysqlPosition,
        client::MysqlConfig,
        control::install_watermark_table,
        dest_schema,
        installer::{InstallerError, MysqlInstaller},
        schema::load_descriptors,
        startup::{MysqlStartupRecord, SelectedTable},
    },
    pg,
    security::TlsPolicy,
};
use std::{sync::Arc, time::Duration};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn live_engine_install_retry_and_mismatch() -> Result {
    let Some(mysql_path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: mysql_installer needs the MySQL harness and SKEDADDLE_MYSQL_WITH_PG=1"
        );
        return Ok(());
    };
    let pg_path = std::env::var_os("SKEDADDLE_TEST_CONFIG")
        .ok_or("MySQL installer qualification requires PG destination (--with-pg)")?;
    let cfg: MysqlConfig = serde_json::from_slice(&std::fs::read(mysql_path)?)?;
    let pg_cfg: serde_json::Value = serde_json::from_slice(&std::fs::read(pg_path)?)?;
    // MySQL harness uses dsn; the general PG matrix uses pg_apply.
    let destination = pg::tls::Config::new(
        pg_cfg["pg_apply"]
            .as_str()
            .or(pg_cfg["dsn"].as_str())
            .ok_or("PG destination missing")?
            .parse()?,
        pg_cfg["ca_pem"].as_str().ok_or("CA missing")?,
    );
    let (mut pg_client, driver, evidence) =
        pg::tls::connect(&destination, &TlsPolicy::Required).await?;
    assert!(evidence.encrypted());
    tokio::spawn(driver);
    assert!(
        pg_client
            .query_one("SHOW server_version_num", &[])
            .await?
            .get::<_, String>(0)
            .starts_with("18")
    );
    let mut source = cfg.connect().await?;
    let count: Option<u64> = source.conn.query_first("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='_skedaddle' AND table_name='watermark'").await?;
    assert_eq!(
        count,
        Some(0),
        "fresh harness must not preload the watermark table"
    );
    let schema = format!("f3a_{}", uuid::Uuid::new_v4().simple());
    source
        .conn
        .query_drop(format!("CREATE DATABASE `{schema}`"))
        .await?;
    source
        .conn
        .query_drop(format!(
            "CREATE TABLE `{schema}`.items (id INT NOT NULL PRIMARY KEY, v INT)"
        ))
        .await?;
    let result: Result = async {
        let tables = load_descriptors(&mut source.conn, &[(schema.clone(), "items".into())]).await?;
        let selected = vec![SelectedTable { schema: schema.clone(), table: "items".into(),
            columns: vec!["id".into(), "v".into()], primary_key: vec!["id".into()] }];
        let identity = SourceIdentity { engine: "mysql".into(), system_id: "f3a-cluster".into(),
            lineage: "test".into(), db_name: schema.clone(), stream: "main".into() };
        let mut installer = MysqlInstaller::new(cfg.clone(), destination.clone(), identity.clone(), tables.clone(), selected.clone())?;
        let root = crate::test_root::scratch_root()
            .join(format!("mysql-installer-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("journal"))?;
        let journal = Journal::new(Arc::new(local_store(root.join("journal"))?), "installer", "test", Duration::from_secs(3))?;
        let mut coordinator = Coordinator::observer(journal.clone(), Local::create(root.join("local"))?, "installer-test")?;
        coordinator.acquire().await?;
        coordinator.commit(Transition::Lifecycle { event: Event::Advance { to: State::Assessed } }).await?;
        let engine = Engine::new(coordinator, journal.clone());
        engine.start(&mut installer).await?;
        assert_eq!(engine.runtime().state().await.lifecycle.state, State::Copying);
        let history = journal.history_page(2, 128).await?;
        let markers: Vec<_> = history.iter().filter_map(|e| match &e.transition {
            Transition::Progress { stream, marker, position } if stream == "source-startup" => {
                assert_eq!(position, "installed"); Some(marker)
            }, _ => None,
        }).collect();
        assert_eq!(markers.len(), 1);
        let record = MysqlStartupRecord::decode(markers[0])?;
        assert_eq!(installer.startup_record.as_ref(), Some(&record));
        let status = source.status().await?;
        let expected = if status.gtid_on {
            MysqlPosition::Gtid { set: status.executed, server_uuid: status.server_uuid, file: status.file, offset: status.offset }
        } else {
            MysqlPosition::File { server_uuid: status.server_uuid,
                file_seq: status.file.rsplit('.').next().unwrap().parse()?, offset: status.offset }
        };
        assert_eq!(record.start_position()?, expected);
        let count: Option<u64> = source.conn.query_first("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='_skedaddle' AND table_name='watermark'").await?;
        assert_eq!(count, Some(1));
        install_watermark_table(&mut source.conn).await?;
        for table in [format!("{schema}.items"), "_skedaddle.apply_progress_v3".into(), "_skedaddle.apply_receipts_v3".into()] {
            assert!(pg_client.query_one("SELECT to_regclass($1) IS NOT NULL", &[&table]).await?.get::<_, bool>(0));
        }
        source.conn.query_drop(format!("INSERT INTO `{schema}`.items VALUES (1, 2)")).await?;
        for &step in installer.plan() { installer.install(step).await?; }
        assert_eq!(installer.startup_record.as_ref(), Some(&record));
        assert_eq!(installer.markers_after(Installation::SourceStartup)?.remove(0).marker, *markers[0]);
        assert!(dest_schema::verify_destination(&mut pg_client, &dest_schema::build(&tables)?).await?.is_empty());
        // Existing table with wrong type must fail via the typed adapter, including engine conversion.
        pg_client.batch_execute(&format!("ALTER TABLE {schema}.items ALTER COLUMN v TYPE bigint")).await?;
        let mut mismatch = MysqlInstaller::new(cfg.clone(), destination.clone(), identity, tables, selected)?;
        assert!(matches!(mismatch.install_step(Installation::DestinationSchema).await,
            Err(InstallerError::Destination(dest_schema::Error::Mismatches(_)))));
        assert!(matches!(mismatch.install(Installation::DestinationSchema).await,
            Err(skedaddle::coordinator::Error::Mysql(skedaddle::mysql::client::MysqlError::Installer(e)))
                if matches!(*e, InstallerError::Destination(dest_schema::Error::Mismatches(_)))));
        Ok(())
    }.await;
    let pg_cleanup = pg_client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    let mysql_cleanup = source
        .conn
        .query_drop(format!("DROP DATABASE `{schema}`"))
        .await;
    pg_cleanup?;
    mysql_cleanup?;
    result
}
