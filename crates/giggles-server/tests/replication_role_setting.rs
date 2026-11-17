use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

async fn frame(s: &mut TcpStream) -> (u8, Vec<u8>) {
    let tag = s.read_u8().await.unwrap();
    let len = s.read_u32().await.unwrap();
    assert!((4..65536).contains(&len));
    let mut body = vec![0; len as usize - 4];
    s.read_exact(&mut body).await.unwrap();
    (tag, body)
}
async fn query(s: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    s.write_u8(b'Q').await.unwrap();
    s.write_u32((sql.len() + 5) as u32).await.unwrap();
    s.write_all(sql.as_bytes()).await.unwrap();
    s.write_u8(0).await.unwrap();
    let mut frames = vec![];
    loop {
        let f = frame(s).await;
        if f.0 == b'Z' {
            break;
        }
        frames.push(f);
    }
    frames
}
async fn extended(s: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    extended_params(s, sql, &[]).await
}
async fn extended_params(s: &mut TcpStream, sql: &str, params: &[&str]) -> Vec<(u8, Vec<u8>)> {
    let mut parse = vec![0];
    parse.extend(sql.as_bytes());
    parse.extend([0, 0, 0]);
    let mut bind = vec![0, 0, 0, 0];
    bind.extend((params.len() as u16).to_be_bytes());
    for param in params {
        bind.extend((param.len() as i32).to_be_bytes());
        bind.extend(param.as_bytes());
    }
    bind.extend([0, 0]);
    for (tag, body) in [
        (b'P', parse),
        (b'B', bind),
        (b'E', vec![0, 0, 0, 0, 0]),
        (b'S', vec![]),
    ] {
        s.write_u8(tag).await.unwrap();
        s.write_u32((body.len() + 4) as u32).await.unwrap();
        s.write_all(&body).await.unwrap();
    }
    let mut frames = vec![];
    loop {
        let f = frame(s).await;
        if f.0 == b'Z' {
            break;
        }
        frames.push(f);
    }
    frames
}
fn values(frames: &[(u8, Vec<u8>)]) -> Vec<String> {
    let mut values = vec![];
    for (_, body) in frames.iter().filter(|f| f.0 == b'D') {
        let count = u16::from_be_bytes(body[..2].try_into().unwrap());
        let mut at = 2;
        for _ in 0..count {
            let len = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
            at += 4;
            assert!(len >= 0);
            values.push(String::from_utf8(body[at..at + len as usize].to_vec()).unwrap());
            at += len as usize;
        }
    }
    values
}
fn error(frames: &[(u8, Vec<u8>)], code: &str, message: &str, hint: Option<&str>) {
    let errors: Vec<_> = frames.iter().filter(|f| f.0 == b'E').collect();
    assert_eq!(errors.len(), 1, "{frames:?}");
    let fields: Vec<_> = errors[0].1.split(|b| *b == 0).collect();
    assert!(
        fields.contains(&format!("C{code}").as_bytes()),
        "{fields:?}"
    );
    assert!(
        fields.contains(&format!("M{message}").as_bytes()),
        "{fields:?}"
    );
    if let Some(hint) = hint {
        assert!(
            fields.contains(&format!("H{hint}").as_bytes()),
            "{fields:?}"
        );
    } else {
        assert!(!fields.iter().any(|f| f.first() == Some(&b'H')));
    }
}
fn success(frames: &[(u8, Vec<u8>)]) {
    assert!(!frames.iter().any(|f| f.0 == b'E'), "{frames:?}");
    assert!(frames.iter().any(|f| f.0 == b'C'), "{frames:?}");
}
fn sqlstate(frames: &[(u8, Vec<u8>)], code: &str) {
    let errors: Vec<_> = frames.iter().filter(|f| f.0 == b'E').collect();
    assert_eq!(errors.len(), 1, "{frames:?}");
    assert!(
        errors[0]
            .1
            .split(|b| *b == 0)
            .any(|f| f == format!("C{code}").as_bytes()),
        "{frames:?}"
    );
}
async fn copy_child(s: &mut TcpStream, data: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let sql = "COPY replica_child FROM STDIN";
    s.write_u8(b'Q').await.unwrap();
    s.write_u32((sql.len() + 5) as u32).await.unwrap();
    s.write_all(sql.as_bytes()).await.unwrap();
    s.write_u8(0).await.unwrap();
    assert_eq!(frame(s).await.0, b'G');
    for (tag, body) in [(b'd', data), (b'c', &[][..])] {
        s.write_u8(tag).await.unwrap();
        s.write_u32((body.len() + 4) as u32).await.unwrap();
        s.write_all(body).await.unwrap();
    }
    let mut frames = vec![];
    loop {
        let f = frame(s).await;
        if f.0 == b'Z' {
            break;
        }
        frames.push(f);
    }
    frames
}
async fn replica_fk_writes(s: &mut TcpStream) {
    success(&query(s, "CREATE TABLE replica_parent (id BIGINT, token BIGINT, PRIMARY KEY (id, token)); CREATE TABLE replica_child (pk BIGINT PRIMARY KEY, id BIGINT, token BIGINT, FOREIGN KEY (id, token) REFERENCES replica_parent(id, token) ON DELETE CASCADE)").await);
    success(&query(s, "GRANT INSERT ON replica_child TO reader").await);
    // N5e: exercise both insert and conflict-update arms before the parent arrives.
    success(&query(s, "BEGIN; SET LOCAL session_replication_role = replica; INSERT INTO replica_child VALUES (1, 1, 1) ON CONFLICT (pk) DO UPDATE SET token = 2; INSERT INTO replica_child VALUES (1, 1, 2) ON CONFLICT (pk) DO UPDATE SET token = 2; COMMIT; INSERT INTO replica_parent VALUES (1, 2)").await);
    assert_eq!(
        values(&query(s, "SELECT token FROM replica_child WHERE pk = 1").await),
        ["2"]
    );
    sqlstate(&query(s, "BEGIN; INSERT INTO replica_child VALUES (2, 2, 2) ON CONFLICT (pk) DO UPDATE SET token = 2").await, "23503");
    success(&query(s, "ROLLBACK").await);
    success(&query(s, "SET session_replication_role = replica").await);
    success(&query(s, "INSERT INTO replica_child VALUES (3, 3, 3)").await);
    success(&query(s, "INSERT INTO replica_child VALUES (4, 4, 4)").await);
    success(&query(s, "RESET session_replication_role").await);
    sqlstate(
        &query(s, "INSERT INTO replica_child VALUES (5, 5, 5)").await,
        "23503",
    );
    success(&query(s, "BEGIN; SET session_replication_role = replica; ROLLBACK").await);
    sqlstate(
        &query(s, "INSERT INTO replica_child VALUES (6, 6, 6)").await,
        "23503",
    );
    success(&query(s, "BEGIN; SET LOCAL session_replication_role = replica").await);
    success(
        &extended_params(
            s,
            "INSERT INTO replica_child VALUES ($1, $2, $3)",
            &["7", "7", "7"],
        )
        .await,
    );
    success(&query(s, "COMMIT").await);
    success(&query(s, "BEGIN; SET LOCAL session_replication_role = replica").await);
    success(&copy_child(s, b"8\t8\t8\n").await);
    success(&query(s, "COMMIT").await);
    sqlstate(&copy_child(s, b"9\t9\t9\n").await, "23503");
    success(&query(s, "BEGIN; SET LOCAL session_replication_role = replica; DELETE FROM replica_parent WHERE id = 1; COMMIT").await);
    assert_eq!(
        values(&query(s, "SELECT pk FROM replica_child WHERE pk = 1").await),
        ["1"]
    );
    assert!(values(&query(s, "SELECT id FROM replica_parent WHERE id = 1").await).is_empty());
}
#[tokio::test]
async fn pg_settings_replication_role_live() {
    let path = std::env::temp_dir().join(format!(
        "replication-role-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
    for (name, superuser) in [("admin", true), ("reader", false)] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                superuser,
                login: true,
                ..Default::default()
            })
            .unwrap();
    }
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
    for user in ["admin", "reader"] {
        let mut s = TcpStream::connect(pg.address).await.unwrap();
        let mut body = 196608u32.to_be_bytes().to_vec();
        for (k, v) in [("user", user), ("database", "postgres")] {
            body.extend(k.as_bytes());
            body.push(0);
            body.extend(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        s.write_u32((body.len() + 4) as u32).await.unwrap();
        s.write_all(&body).await.unwrap();
        loop {
            let f = frame(&mut s).await;
            assert_ne!(f.0, b'E', "{f:?}");
            if f.0 == b'Z' {
                break;
            }
        }
        let rows = query(&mut s, "SHOW session_replication_role").await;
        assert_eq!(values(&rows), ["origin"]);
        assert!(rows.iter().any(|(tag, b)| {
            *tag == b'T'
                && b.windows(b"session_replication_role\0".len())
                    .any(|w| w == b"session_replication_role\0")
        }));
        if user == "reader" {
            for sql in [
                "SET session_replication_role = replica",
                "SET LOCAL session_replication_role = replica",
                "RESET session_replication_role",
                "SELECT set_config('session_replication_role','replica',false)",
                "SELECT set_config('session_replication_role','replica',true)",
            ] {
                error(
                    &query(&mut s, sql).await,
                    "42501",
                    "permission denied to set parameter \"session_replication_role\"",
                    None,
                );
            }
            error(
                &extended(&mut s, "SET session_replication_role = replica").await,
                "42501",
                "permission denied to set parameter \"session_replication_role\"",
                None,
            );
            error(
                &extended(
                    &mut s,
                    "SELECT set_config('session_replication_role','replica',false)",
                )
                .await,
                "42501",
                "permission denied to set parameter \"session_replication_role\"",
                None,
            );
            assert_eq!(
                values(&query(&mut s, "SELECT current_setting('session_replication_role')").await),
                ["origin"]
            );
        } else {
            for sql in [
                "SET session_replication_role = 'Bogus'",
                "SELECT set_config('session_replication_role','Bogus',false)",
            ] {
                error(
                    &query(&mut s, sql).await,
                    "22023",
                    "invalid value for parameter \"session_replication_role\": \"Bogus\"",
                    Some("Available values: origin, replica, local."),
                );
            }
            let frames = extended(&mut s, "SET session_replication_role = local").await;
            assert!(!frames.iter().any(|f| f.0 == b'E'), "{frames:?}");
            assert_eq!(values(&extended(&mut s, "SELECT current_setting('session_replication_role'), current_setting('intervalstyle')").await), ["local", "postgres"]);
            let frames = query(&mut s,"SET session_replication_role = REPLICA; SELECT current_setting('session_replication_role'), current_setting('intervalstyle'); BEGIN; SET LOCAL session_replication_role = local; SHOW session_replication_role; COMMIT; SHOW session_replication_role; BEGIN; SET session_replication_role = origin; ROLLBACK; SHOW session_replication_role; RESET session_replication_role; SHOW session_replication_role").await;
            assert!(!frames.iter().any(|f| f.0 == b'E'), "{frames:?}");
            assert_eq!(
                values(&frames),
                [
                    "replica", "postgres", "local", "replica", "replica", "origin"
                ]
            );
            let frames = query(&mut s, "SELECT set_config('session_replication_role','REPLICA',false); SHOW session_replication_role; BEGIN; SELECT set_config('session_replication_role','local',true); COMMIT; SHOW session_replication_role; RESET ALL; SHOW session_replication_role").await;
            assert!(!frames.iter().any(|f| f.0 == b'E'), "{frames:?}");
            assert_eq!(
                values(&frames),
                ["replica", "replica", "local", "replica", "origin"]
            );
        }
        if user == "admin" {
            replica_fk_writes(&mut s).await;
        } else {
            success(&query(&mut s, "BEGIN").await);
            sqlstate(
                &query(&mut s, "SET LOCAL session_replication_role = replica").await,
                "42501",
            );
            success(&query(&mut s, "ROLLBACK; BEGIN").await);
            sqlstate(
                &query(&mut s, "INSERT INTO replica_child VALUES (10, 10, 10)").await,
                "23503",
            );
            success(&query(&mut s, "ROLLBACK").await);
        }
        drop(s);
    }
    pg.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
