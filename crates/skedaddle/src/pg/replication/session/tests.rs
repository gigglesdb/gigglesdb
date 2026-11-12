//! Protocol fixtures only: live TLS/SCRAM/database qualification is pg_source.
use super::*;
use crate::pg::{
    pgoutput::Limits,
    replication::{Checkpoint, ConnectionConfig, Lineage, PostgresVersion, ReplicationConnection},
    source,
};
use rustls::RootCertStore;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

fn config() -> ConnectionConfig {
    ConnectionConfig {
        host: "unused".into(),
        port: 0,
        server_name: "unused".into(),
        user: "unused".into(),
        password: "unused".into(),
        database: "db".into(),
        roots: RootCertStore::empty(),
        connect_timeout: Duration::from_millis(100),
        status_interval: Duration::from_secs(1),
        limits: Limits::default(),
    }
}

fn checkpoint() -> Checkpoint {
    Checkpoint {
        lineage: Lineage {
            system_id: 123,
            timeline: 1,
            xlogpos: 256,
            database: "db".into(),
        },
        slot: "slot".into(),
        durable_lsn: 32,
    }
}

async fn query(peer: &mut DuplexStream) -> String {
    assert_eq!(peer.read_u8().await.unwrap(), b'Q');
    let len = peer.read_u32().await.unwrap();
    assert!((5..1024).contains(&len));
    let mut bytes = vec![0; len as usize - 4];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(bytes.pop(), Some(0));
    String::from_utf8(bytes).unwrap()
}

async fn frame(peer: &mut DuplexStream, tag: u8, payload: &[u8]) {
    peer.write_u8(tag).await.unwrap();
    peer.write_u32(payload.len() as u32 + 4).await.unwrap();
    peer.write_all(payload).await.unwrap();
}

async fn result(peer: &mut DuplexStream, values: Option<&[Option<&str>]>) {
    if let Some(values) = values {
        let mut payload = (values.len() as u16).to_be_bytes().to_vec();
        for value in values {
            match value {
                Some(s) => {
                    payload.extend((s.len() as u32).to_be_bytes());
                    payload.extend(s.as_bytes());
                }
                None => payload.extend((-1_i32).to_be_bytes()),
            }
        }
        frame(peer, b'D', &payload).await;
    }
    frame(peer, b'C', b"OK\0").await;
    frame(peer, b'Z', b"I").await;
}

async fn identify(peer: &mut DuplexStream) {
    assert_eq!(query(peer).await, "IDENTIFY_SYSTEM");
    result(
        peer,
        Some(&[Some("123"), Some("1"), Some("0/100"), Some("db")]),
    )
    .await;
}

async fn setup_peer(peer: &mut DuplexStream, zone: &str, digits: &str, strings: &str) {
    assert_eq!(query(peer).await, REQUIRED_SETTINGS_SQL);
    // Actual SET can emit ParameterStatus before its completion.
    frame(peer, b'S', b"DateStyle\0ISO, MDY\0").await;
    frame(peer, b'S', b"TimeZone\0UTC\0").await;
    result(peer, None).await;
    for (name, value) in [
        ("DateStyle", "ISO, MDY"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", zone),
        ("extra_float_digits", digits),
        ("client_encoding", "UTF8"),
        ("standard_conforming_strings", strings),
    ] {
        assert_eq!(query(peer).await, format!("SHOW {name}"));
        result(peer, Some(&[Some(value)])).await;
    }
}

async fn start_peer(peer: &mut DuplexStream) {
    identify(peer).await;
    assert_eq!(
        query(peer).await,
        "SELECT slot_name, plugin, slot_type, database, restart_lsn::text, confirmed_flush_lsn::text, wal_status FROM pg_replication_slots WHERE slot_name = 'slot'"
    );
    result(
        peer,
        Some(&[
            Some("slot"),
            Some("pgoutput"),
            Some("logical"),
            Some("db"),
            Some("0/10"),
            Some("0/20"),
            Some("reserved"),
        ]),
    )
    .await;
    assert_eq!(
        query(peer).await,
        super::super::start_command("slot", 32, &["pub".into()]).unwrap()
    );
    frame(peer, b'W', &[0, 0, 0]).await;
}

#[tokio::test]
async fn initial_and_reconnect_setup_readback_precede_start() {
    for reconnect in [false, true] {
        let (client, mut peer) = tokio::io::duplex(64);
        let ((), ()) = tokio::join!(
            async {
                let mut connection = ReplicationConnection::initialize(
                    Wire::new(client, Limits::default()).unwrap(),
                    PostgresVersion::V17,
                    &config(),
                )
                .await
                .unwrap();
                assert_eq!(
                    connection.session_settings(),
                    &SessionSettings {
                        date_style: "ISO, MDY".into(),
                        interval_style: "postgres".into(),
                        time_zone: "UTC".into(),
                        extra_float_digits: 3,
                        client_encoding: "UTF8".into(),
                        standard_conforming_strings: true,
                    }
                );
                if reconnect {
                    source::resume(&mut connection, &checkpoint(), &["pub".into()])
                        .await
                        .unwrap();
                } else {
                    source::verify_connected(&connection, connection.lineage()).unwrap();
                    connection.start("slot", 32, &["pub".into()]).await.unwrap();
                }
            },
            async {
                identify(&mut peer).await;
                setup_peer(&mut peer, "UTC", "3", "on").await;
                start_peer(&mut peer).await;
            }
        );
    }
}

#[tokio::test]
async fn initial_and_reconnect_mismatch_fail_before_start() {
    for reconnect in [false, true] {
        let (client, mut peer) = tokio::io::duplex(64);
        tokio::join!(
            async {
                let mut connection = ReplicationConnection::initialize(
                    Wire::new(client, Limits::default()).unwrap(),
                    PostgresVersion::V17,
                    &config(),
                )
                .await
                .unwrap();
                // A SHOW result, rather than the requested SET or ParameterStatus,
                // is what verification must use on every fresh connection.
                let result = if reconnect {
                    source::resume(&mut connection, &checkpoint(), &["pub".into()]).await
                } else {
                    source::verify_connected(&connection, connection.lineage())
                };
                assert!(matches!(result, Err(source::Error::RecoveryRequired(_))));
                drop(connection);
            },
            async {
                identify(&mut peer).await;
                setup_peer(&mut peer, "Europe/London", "3", "on").await;
                assert_eq!(
                    peer.read(&mut [0]).await.unwrap(),
                    0,
                    "no START on mismatch"
                );
            }
        );
    }
}

#[tokio::test]
async fn malformed_readback_fails_closed() {
    for (digits, strings) in [("NaN", "on"), ("3", "yes")] {
        let (client, mut peer) = tokio::io::duplex(64);
        tokio::join!(
            async {
                let mut wire = Wire::new(client, Limits::default()).unwrap();
                assert!(matches!(setup(&mut wire).await, Err(E::Invalid(_))));
            },
            setup_peer(&mut peer, "UTC", digits, strings)
        );
    }
}

#[tokio::test]
async fn public_setup_hook_is_bounded_and_poisoned_on_timeout() {
    let (client, mut peer) = tokio::io::duplex(64);
    tokio::join!(
        async {
            let mut connection = ReplicationConnection::initialize(
                Wire::new(client, Limits::default()).unwrap(),
                PostgresVersion::V17,
                &config(),
            )
            .await
            .unwrap();
            assert!(matches!(
                connection.setup_mapping_session().await,
                Err(E::Timeout)
            ));
            assert!(connection.setup_mapping_session().await.is_err());
            assert!(connection.start("slot", 32, &["pub".into()]).await.is_err());
        },
        async {
            identify(&mut peer).await;
            setup_peer(&mut peer, "UTC", "3", "on").await;
            assert_eq!(query(&mut peer).await, REQUIRED_SETTINGS_SQL);
            assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        }
    );
}
