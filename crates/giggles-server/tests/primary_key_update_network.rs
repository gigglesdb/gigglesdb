use giggles_core::{
    Admin, Limits,
    roles::{NewRole, SchemaPrivilege},
};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc, time::SystemTime};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

#[tokio::test]
async fn primary_key_update_moves_rows_and_checks_wire_authorization() {
    let root = std::path::PathBuf::from(std::env::var_os("TEMP").expect("task TEMP"));
    assert!(
        root.to_string_lossy()
            .replace('\\', "/")
            .to_ascii_lowercase()
            .starts_with("j:/gigglesdb-work/")
    );
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = root.join(format!(
        "primary-key-update-network-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
    for name in ["owner", "other"] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                login: true,
                password: Some(Verifier::generate("pw")),
                ..NewRole::default()
            })
            .unwrap();
        admin
            .roles()
            .grant_schema(
                "public",
                name,
                &[SchemaPrivilege::Create, SchemaPrivilege::Usage],
            )
            .unwrap();
    }
    let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
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
    let listener = config.build().unwrap();
    let running = serve_postgresql(config.listen, service.clone(), Arc::new(listener))
        .await
        .unwrap();
    let mut config = Config::new();
    config
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("owner")
        .password("pw")
        .dbname("postgres");
    let (owner, connection) = config.connect(NoTls).await.unwrap();
    let owner_connection = tokio::spawn(connection);
    config.user("other");
    let (other, connection) = config.connect(NoTls).await.unwrap();
    let other_connection = tokio::spawn(connection);

    for extended in [false, true] {
        let table = if extended { "pk_extended" } else { "pk_simple" };
        owner
            .simple_query(&format!(
                "CREATE TABLE {table}(id BIGINT PRIMARY KEY, payload TEXT); \
                 INSERT INTO {table} VALUES(1, 'first'), (2, 'second')"
            ))
            .await
            .unwrap();
        let sql = format!("UPDATE {table} SET id=id+10, payload='moved'");
        if extended {
            assert_eq!(owner.execute(&sql, &[]).await.unwrap(), 2);
        } else {
            let messages = owner.simple_query(&sql).await.unwrap();
            assert!(matches!(
                messages.as_slice(),
                [SimpleQueryMessage::CommandComplete(2)]
            ));
        }
        let rows = owner
            .query(
                &format!("SELECT id, payload FROM {table} ORDER BY id LIMIT 10"),
                &[],
            )
            .await
            .unwrap();
        let values: Vec<(i64, String)> = rows.iter().map(|row| (row.get(0), row.get(1))).collect();
        assert_eq!(values, vec![(11, "moved".into()), (12, "moved".into())]);
        let sql = format!("UPDATE {table} SET id=99 WHERE id=11");
        let error = if extended {
            other.execute(&sql, &[]).await.unwrap_err()
        } else {
            other.simple_query(&sql).await.unwrap_err()
        };
        assert_eq!(error.code().unwrap().code(), "42501");
        assert_eq!(
            owner
                .query(&format!("SELECT id FROM {table} WHERE id=11 LIMIT 10"), &[])
                .await
                .unwrap()
                .len(),
            1
        );
    }
    assert_update_tag(running.address).await;
    drop(owner);
    drop(other);
    owner_connection.await.unwrap().unwrap();
    other_connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(service);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

async fn assert_update_tag(address: std::net::SocketAddr) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0owner\0database\0postgres\0\0");
    stream
        .write_all(&((startup.len() + 4) as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&startup).await.unwrap();
    loop {
        let (tag, body) = read_message(&mut stream).await;
        assert_ne!(tag, b'E', "{body:?}");
        if tag == b'Z' {
            break;
        }
    }
    let sql = b"UPDATE pk_simple SET id=id+100\0";
    stream.write_all(b"Q").await.unwrap();
    stream
        .write_all(&((sql.len() + 4) as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(sql).await.unwrap();
    let mut commands = vec![];
    loop {
        let (tag, body) = read_message(&mut stream).await;
        assert_ne!(tag, b'E', "{body:?}");
        if tag == b'C' {
            commands.push(body);
        }
        if tag == b'Z' {
            break;
        }
    }
    assert_eq!(commands, vec![b"UPDATE 2\0".to_vec()]);
}
async fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let tag = stream.read_u8().await.unwrap();
        let size = stream.read_u32().await.unwrap();
        let mut body = vec![0; size as usize - 4];
        stream.read_exact(&mut body).await.unwrap();
        (tag, body)
    })
    .await
    .expect("wire response timeout")
}
