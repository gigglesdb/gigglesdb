use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

async fn frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let tag = stream.read_u8().await.unwrap();
    let len = stream.read_u32().await.unwrap();
    assert!((4..65536).contains(&len));
    let mut body = vec![0; len as usize - 4];
    stream.read_exact(&mut body).await.unwrap();
    (tag, body)
}

#[tokio::test]
async fn case_folded_timezone_and_duplicate_on_postgresql_listener() {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "startup-params-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            superuser: true,
            login: true,
            ..NewRole::default()
        })
        .unwrap();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: BTreeMap::new(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 8,
    };
    let listener = Arc::new(config.build().unwrap());
    let pg = serve_postgresql(config.listen, service.clone(), listener)
        .await
        .unwrap();
    for fields in [
        vec![("timezone", "UTC")],
        vec![("TIMEZONE", "UTC")],
        vec![("timezone", "America/Chicago")],
        vec![("TimeZone", "UTC"), ("timezone", "UTC")],
    ] {
        let duplicate = fields.len() == 2;
        let expected = fields[0].1;
        let mut stream = TcpStream::connect(pg.address).await.unwrap();
        let mut body = 196608u32.to_be_bytes().to_vec();
        for (key, value) in [("user", "owner"), ("database", "postgres")]
            .into_iter()
            .chain(fields)
        {
            body.extend(key.as_bytes());
            body.push(0);
            body.extend(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        stream.write_u32((body.len() + 4) as u32).await.unwrap();
        stream.write_all(&body).await.unwrap();
        if duplicate {
            let (tag, body) = frame(&mut stream).await;
            assert_eq!(tag, b'E');
            assert!(body.split(|b| *b == 0).any(|field| field == b"C08P01"));
        } else {
            let mut timezone = None;
            loop {
                let (tag, body) = frame(&mut stream).await;
                assert_ne!(tag, b'E', "startup rejected: {body:?}");
                if tag == b'S' {
                    let fields: Vec<_> = body.split(|b| *b == 0).collect();
                    if fields[0] == b"TimeZone" {
                        timezone = Some(fields[1].to_vec());
                    }
                }
                if tag == b'Z' {
                    break;
                }
            }
            assert_eq!(timezone.as_deref(), Some(expected.as_bytes()));
        }
    }
    pg.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
