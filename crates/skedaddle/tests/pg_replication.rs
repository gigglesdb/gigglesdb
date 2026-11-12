//! Harness-only real PostgreSQL test. Never starts infrastructure.
//!
//! SKEDADDLE_TEST_CONFIG must contain `pg` (admin connection string) and
//! `replication`: {host, port, server_name, user, password, database, ca_pem}.
//! The harness must enable TLS, SCRAM and wal_level=logical; ca_pem is a path.
//! The existing non-TLS P2/P3 lane is deliberately not a qualification for this.

use std::{error::Error, io::BufReader, panic::AssertUnwindSafe, time::Duration};

use futures::FutureExt;
use rustls::RootCertStore;
use skedaddle::pg::{
    pgoutput::{CopyData, Decoder, Limits, Message},
    replication::{
        Checkpoint, ConnectionConfig, RecoveryRequired, ReplicationConnection, ReplicationError,
        parse_lsn,
    },
};
use tokio_postgres::Client;

type TestResult = Result<(), Box<dyn Error>>;

async fn inactive(admin: &Client, slot: &str) {
    for _ in 0..100 {
        let row = admin
            .query_one(
                "SELECT active FROM pg_replication_slots WHERE slot_name=$1",
                &[&slot],
            )
            .await
            .unwrap();
        if !row.get::<_, bool>(0) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("replication slot did not become inactive");
}

async fn receive_commit(connection: &mut ReplicationConnection, durable: u64, marker: &str) -> u64 {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut seen = false;
        loop {
            let message = connection.next(durable).await.unwrap();
            if let CopyData::XLogData { data, .. } = message.decode().unwrap() {
                match Decoder::default().decode(data).unwrap() {
                    Message::LogicalMessage {
                        prefix, content, ..
                    } => {
                        assert_eq!(prefix, "p5b");
                        assert_eq!(content, marker.as_bytes());
                        seen = true;
                    }
                    Message::Commit(commit) if seen => return commit.end_lsn,
                    _ => {}
                }
            }
        }
    })
    .await
    .expect("no logical-message transaction received")
}

#[tokio::test]
async fn tls_scram_capture_feedback_cancellation_and_reconnect() -> TestResult {
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        eprintln!(
            "SKIP pg_replication: requires harness TLS/SCRAM PG16/17/18 config in SKEDADDLE_TEST_CONFIG"
        );
        return Ok(());
    };
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let Some(cfg) = json.get("replication") else {
        eprintln!(
            "SKIP pg_replication: harness config has no replication TLS fixture; P2/P3 PG lane is insufficient"
        );
        return Ok(());
    };
    let field = |name: &str| {
        cfg[name]
            .as_str()
            .expect("missing replication fixture string")
            .to_owned()
    };
    let mut roots = RootCertStore::empty();
    let mut pem = BufReader::new(std::fs::File::open(field("ca_pem"))?);
    for cert in rustls_pemfile::certs(&mut pem) {
        roots.add(cert?)?;
    }
    let mut config = ConnectionConfig {
        host: field("host"),
        port: cfg["port"].as_u64().expect("port") as u16,
        server_name: field("server_name"),
        user: field("user"),
        password: field("password"),
        database: field("database"),
        roots,
        connect_timeout: Duration::from_secs(10),
        status_interval: Duration::from_millis(50),
        limits: Limits::default(),
    };
    // Admin traffic is confined to the harness endpoint; the replication client
    // itself always validates TLS and requires verified TLS.
    let (admin, driver) = tls_connect(json["pg"].as_str().expect("admin pg")).await?;
    let driver = tokio::spawn(driver);
    let id = uuid::Uuid::new_v4().simple().to_string();
    let slot = format!("p5b_{id}");
    let publication = format!("p5b_pub_{id}");
    let publications = vec![publication.clone()];
    admin
        .batch_execute(&format!("CREATE PUBLICATION {publication}"))
        .await?;

    let result = AssertUnwindSafe(async {
        let original_name = config.server_name.clone();
        config.server_name = "invalid.p5b.example".into();
        assert!(
            ReplicationConnection::connect(&config).await.is_err(),
            "hostname validation must reject wrong name"
        );
        config.server_name = original_name;
        let password = config.password.clone();
        config.password.push_str("-wrong");
        assert!(
            ReplicationConnection::connect(&config).await.is_err(),
            "wrong password must fail SCRAM"
        );
        config.password = password;
        let trusted = config.roots.clone();
        config.roots = RootCertStore::empty();
        assert!(
            ReplicationConnection::connect(&config).await.is_err(),
            "empty trust must fail closed"
        );
        config.roots = trusted;

        let mut connection = ReplicationConnection::connect(&config).await?;
        let created = connection.create_slot(&slot).await?;
        let checkpoint = Checkpoint {
            lineage: connection.lineage().clone(),
            slot: slot.clone(),
            durable_lsn: created.consistent_point,
        };
        connection
            .start(&slot, checkpoint.durable_lsn, &publications)
            .await?;
        admin
            .query_one(
                "SELECT pg_logical_emit_message(true, 'p5b', $1::text)",
                &[&id],
            )
            .await?;
        let end = receive_commit(&mut connection, checkpoint.durable_lsn, &id).await;
        assert!(end > checkpoint.durable_lsn);
        // Repeated cancellation must neither lose the next WAL record nor
        // acknowledge the observed WAL end. Exercise the idle periodic schedule.
        let _ = tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                connection.next(checkpoint.durable_lsn).await.unwrap();
            }
        })
        .await;
        let row = admin
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1",
                &[&slot],
            )
            .await?;
        assert!(parse_lsn(&row.get::<_, String>(0))? <= checkpoint.durable_lsn);
        drop(connection);
        inactive(&admin, &slot).await;

        let mut replay =
            ReplicationConnection::reconnect(&config, &checkpoint, &publications).await?;
        assert_eq!(
            receive_commit(&mut replay, checkpoint.durable_lsn, &id).await,
            end
        );
        let _ = tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                replay.next(end).await.unwrap();
            }
        })
        .await;
        let row = admin
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1",
                &[&slot],
            )
            .await?;
        assert_eq!(parse_lsn(&row.get::<_, String>(0))?, end);
        drop(replay);
        inactive(&admin, &slot).await;

        let mut wrong = checkpoint.clone();
        wrong.lineage.system_id ^= 1;
        assert!(matches!(
            ReplicationConnection::reconnect(&config, &wrong, &publications).await,
            Err(ReplicationError::RecoveryRequired(
                RecoveryRequired::SystemIdChanged { .. }
            ))
        ));
        wrong = checkpoint.clone();
        wrong.lineage.timeline += 1;
        assert!(matches!(
            ReplicationConnection::reconnect(&config, &wrong, &publications).await,
            Err(ReplicationError::RecoveryRequired(
                RecoveryRequired::TimelineChanged { .. }
            ))
        ));
        // A slot advanced past the supplied checkpoint cannot silently skip WAL.
        assert!(matches!(
            ReplicationConnection::reconnect(&config, &checkpoint, &publications).await,
            Err(ReplicationError::RecoveryRequired(
                RecoveryRequired::PositionUnavailable { .. }
            ))
        ));
        admin
            .query_one("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await?;
        assert!(matches!(
            ReplicationConnection::reconnect(&config, &checkpoint, &publications).await,
            Err(ReplicationError::RecoveryRequired(
                RecoveryRequired::MissingSlot(_)
            ))
        ));
        Ok(()) as TestResult
    })
    .catch_unwind()
    .await;
    // Remove test-owned objects after errors and assertion panics as well.
    for _ in 0..100 {
        let rows = admin
            .query(
                "SELECT active FROM pg_replication_slots WHERE slot_name=$1",
                &[&slot],
            )
            .await?;
        if rows.is_empty() || !rows[0].get::<_, bool>(0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let cleanup = admin.batch_execute(&format!("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='{slot}' AND NOT active; DROP PUBLICATION IF EXISTS {publication};")).await;
    drop(admin);
    driver.abort();
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup?;
    Ok(())
}

// The harness CA is explicit; missing trust is a failure once a DB gate is set.
fn tls_config(postgres: tokio_postgres::Config) -> skedaddle::pg::tls::Config {
    let path = std::env::var_os("SKEDADDLE_TEST_CONFIG").expect("TLS harness config required");
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).expect("read TLS harness config"))
            .expect("TLS harness JSON");
    let ca_pem = config["ca_pem"]
        .as_str()
        .or_else(|| config["replication"]["ca_pem"].as_str())
        .expect("harness ca_pem or replication.ca_pem required");
    skedaddle::pg::tls::Config::new(postgres, ca_pem)
}

async fn tls_connect(
    dsn: &str,
) -> std::result::Result<
    (tokio_postgres::Client, skedaddle::pg::tls::Driver),
    skedaddle::pg::tls::Error,
> {
    let config = tls_config(dsn.parse().expect("harness PostgreSQL configuration"));
    let (client, driver, transport) =
        skedaddle::pg::tls::connect(&config, &skedaddle::security::TlsPolicy::Required).await?;
    assert!(transport.encrypted());
    assert!(transport.warnings.is_empty());
    Ok((client, driver))
}
