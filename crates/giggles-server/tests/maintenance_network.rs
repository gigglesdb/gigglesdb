use super::session_settings_network::{Fixture, wire_cycle, wire_query};
use giggles_core::Engine;
use giggles_server::pgwire::z;
use tokio::{io::AsyncWriteExt, net::TcpStream};
async fn connect(f: &Fixture, role: &str) -> TcpStream {
    connect_database(f, role, "postgres").await
}
async fn connect_database(f: &Fixture, role: &str, database: &str) -> TcpStream {
    let mut s = TcpStream::connect(f.pg.address).await.unwrap();
    let mut b = 196608u32.to_be_bytes().to_vec();
    for (k, v) in [("user", role), ("database", database)] {
        z(k, &mut b);
        z(v, &mut b);
    }
    b.push(0);
    s.write_u32((b.len() + 4) as u32).await.unwrap();
    s.write_all(&b).await.unwrap();
    wire_cycle(&mut s).await;
    s
}
fn field(mut body: &[u8], key: u8) -> Option<&str> {
    while let Some((&k, rest)) = body.split_first() {
        if k == 0 {
            return None;
        }
        let end = rest.iter().position(|&v| v == 0).unwrap();
        if k == key {
            return Some(std::str::from_utf8(&rest[..end]).unwrap());
        }
        body = &rest[end + 1..];
    }
    None
}
#[tokio::test]
async fn maintenance_tags_transaction_recovery_and_warning() {
    let f = Fixture::with_engine(if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    })
    .await;
    let mut s = connect(&f, "owner").await;
    for (sql, tag) in [
        ("CREATE TABLE t (v int)", "CREATE TABLE"),
        ("VACUUM ANALYZE t", "VACUUM"),
        ("ANALYZE t", "ANALYZE"),
        ("REINDEX TABLE t", "REINDEX"),
        ("CHECKPOINT", "CHECKPOINT"),
        ("BEGIN", "BEGIN"),
    ] {
        let m = wire_query(&mut s, sql).await;
        assert!(!m.iter().any(|m| m.0 == b'E'), "{sql}: {m:?}");
        assert_eq!(
            m.iter().find(|m| m.0 == b'C').unwrap().1,
            format!("{tag}\0").as_bytes()
        );
    }
    let m = wire_query(&mut s, "VACUUM t").await;
    let e = &m.iter().find(|m| m.0 == b'E').unwrap().1;
    assert_eq!(field(e, b'C'), Some("25001"));
    assert_eq!(
        field(e, b'M'),
        Some("VACUUM cannot run inside a transaction block")
    );
    let m = wire_query(&mut s, "SELECT 1").await;
    let e = &m.iter().find(|m| m.0 == b'E').unwrap().1;
    assert_eq!(field(e, b'C'), Some("25P02"));
    for sql in ["ROLLBACK", "SELECT 1", "CREATE ROLE plain LOGIN"] {
        let m = wire_query(&mut s, sql).await;
        assert!(!m.iter().any(|m| m.0 == b'E'), "{m:?}");
    }
    let m = wire_query(&mut s, "VACUUM t; SELECT 1").await;
    let e = &m.iter().find(|m| m.0 == b'E').unwrap().1;
    assert_eq!(field(e, b'C'), Some("25001"));
    assert_eq!(
        field(e, b'M'),
        Some("VACUUM cannot run inside a transaction block")
    );
    let m = wire_query(&mut s, "SELECT 1").await;
    assert!(!m.iter().any(|m| m.0 == b'E'));
    let m = wire_query(&mut s, "GRANT ALL ON t TO plain").await;
    assert!(!m.iter().any(|m| m.0 == b'E'), "{m:?}");
    let mut plain = connect(&f, "plain").await;
    let m = wire_query(&mut plain, "VACUUM t").await;
    assert!(!m.iter().any(|m| m.0 == b'E'), "{m:?}");
    let n = &m.iter().find(|m| m.0 == b'N').unwrap().1;
    assert_eq!(field(n, b'S'), Some("WARNING"));
    assert_eq!(field(n, b'C'), Some("01000"));
    assert_eq!(
        field(n, b'M'),
        Some(r#"permission denied to vacuum "t", skipping it"#)
    );
}

#[tokio::test]
async fn maintenance_database_owner_diagnostic() {
    let f = Fixture::with_engine(if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    })
    .await;
    let mut owner = connect(&f, "owner").await;
    let m = wire_query(&mut owner, "CREATE ROLE plain LOGIN").await;
    assert!(!m.iter().any(|m| m.0 == b'E'));
    let mut plain = connect(&f, "plain").await;
    for sql in ["REINDEX DATABASE", "REINDEX SYSTEM"] {
        let m = wire_query(&mut plain, sql).await;
        let e = &m.iter().find(|m| m.0 == b'E').unwrap().1;
        assert_eq!(field(e, b'C'), Some("42501"));
        assert_eq!(field(e, b'M'), Some("must be owner of database postgres"));
    }
}

#[tokio::test]
async fn maintenance_nondefault_database_diagnostic() {
    let mut f = Fixture::with_engine(if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    })
    .await;
    f.pg.shutdown().await.unwrap();
    let config = giggles_server::auth::PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: giggles_server::auth::TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: std::collections::BTreeMap::new(),
        databases: vec!["o5".into()],
        loopback_trust: true,
        connections: 8,
    };
    f.pg = giggles_server::auth::serve_postgresql(
        config.listen,
        f.service.clone(),
        std::sync::Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let mut s = connect_database(&f, "owner", "o5").await;
    let m = wire_query(&mut s, "CREATE ROLE plain LOGIN").await;
    assert!(!m.iter().any(|m| m.0 == b'E'), "{m:?}");
    let mut plain = connect_database(&f, "plain", "o5").await;
    for sql in ["REINDEX DATABASE", "REINDEX SYSTEM"] {
        let m = wire_query(&mut plain, sql).await;
        let e = &m.iter().find(|m| m.0 == b'E').unwrap().1;
        assert_eq!(field(e, b'C'), Some("42501"));
        assert_eq!(field(e, b'M'), Some("must be owner of database o5"));
    }
}
