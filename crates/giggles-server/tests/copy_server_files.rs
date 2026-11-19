//! Raw PostgreSQL-v3 COPY sequences qualified against PostgreSQL 18.6.

use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Duration, timeout},
};
static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    server: Running,
    path: PathBuf,
}
impl Fixture {
    async fn with_engine(engine: giggles_core::Engine) -> Self {
        Self::with_collisions(engine, false).await
    }
    async fn with_collisions(engine: giggles_core::Engine, collisions: bool) -> Self {
        let path = std::env::temp_dir().join(format!(
            "copy-network-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                superuser: true,
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "denied".into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        let mut sql = admin.sql_session();
        sql.execute(
            if engine == giggles_core::Engine::Legacy {
                "CREATE TABLE t (i INT PRIMARY KEY, x TEXT CHECK (x <> 'bad'), n BIGINT)"
            } else {
                "CREATE TABLE t (i INT PRIMARY KEY, x TEXT, n BIGINT)"
            },
            &[],
            None,
        )
        .unwrap();
        drop(sql);
        admin.roles().set_table_owner("t", "owner").unwrap();
        let admin = if collisions {
            drop(admin);
            // Exact main-era JSON shape and empty-valued membership in a legacy database.
            let db = redb::Database::open(path.join("db.redb/data.redb")).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(redb::TableDefinition::<&[u8], &[u8]>::new("giggles_roles_v1")).unwrap();
                for (i, name) in ["pg_read_all_data", "pg_write_all_data", "pg_execute_server_program", "pg_read_server_files", "pg_write_server_files"].iter().enumerate() {
                    let bytes = serde_json::to_vec(&giggles_core::roles::CatalogRole {
                        name: (*name).into(), oid: 18000+i as u32, superuser: false, login: true,
                        createrole: false, password: None, generation: 1, task_only: false, inherit: true,
                    }).unwrap();
                    table.insert(format!("role/{name}").as_bytes(), bytes.as_slice()).unwrap();
                }
                table.insert(b"member/pg_execute_server_program\0denied".as_slice(), b"".as_slice()).unwrap();
            }
            txn.commit().unwrap();
            drop(db);
            Admin::open(path.join("db.redb"), Limits::default()).unwrap()
        } else { admin };
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
            connections: 16,
        };
        let listener = config.build().unwrap();
        let server = serve_postgresql(config.listen, service, Arc::new(listener))
            .await
            .unwrap();
        Self { server, path }
    }
    async fn connect(&self) -> TcpStream {
        self.connect_as("owner").await
    }
    async fn connect_as(&self, user: &str) -> TcpStream {
        self.connect_with_key(user).await.0
    }
    async fn connect_with_key(&self, user: &str) -> (TcpStream, Vec<u8>) {
        let mut s = TcpStream::connect(self.server.address).await.unwrap();
        let mut b = 196608i32.to_be_bytes().to_vec();
        b.extend(b"user\0");
        b.extend(user.as_bytes());
        b.extend(b"\0database\0postgres\0\0");
        s.write_all(&((b.len() + 4) as i32).to_be_bytes())
            .await
            .unwrap();
        s.write_all(&b).await.unwrap();
        let replies = read(&mut s).await;
        assert_eq!(replies.last().unwrap().0, b'Z');
        let key = replies
            .into_iter()
            .find(|(t, _)| *t == b'K')
            .expect("BackendKeyData")
            .1;
        assert_eq!(key.len(), 8);
        (s, key)
    }
    async fn stop(self) {
        self.server.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}
async fn send(s: &mut TcpStream, t: u8, b: &[u8]) {
    let mut out = vec![t];
    out.extend(((b.len() + 4) as i32).to_be_bytes());
    out.extend(b);
    s.write_all(&out).await.unwrap();
}
async fn read(s: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut rows = vec![];
    loop {
        let tag = match timeout(
            if rows.is_empty() {
                Duration::from_secs(5)
            } else {
                Duration::from_millis(100)
            },
            s.read_u8(),
        )
        .await
        {
            Ok(Ok(t)) => t,
            Err(_) => break,
            Ok(Err(e)) => panic!("read tag: {e}"),
        };
        let n = s.read_i32().await.unwrap();
        assert!(n >= 4);
        let mut b = vec![0; (n - 4) as usize];
        s.read_exact(&mut b).await.unwrap();
        rows.push((tag, b));
    }
    rows
}
fn tags(rows: &[(u8, Vec<u8>)]) -> Vec<u8> {
    rows.iter().map(|r| r.0).collect()
}
fn error(rows: &[(u8, Vec<u8>)], code: &str) {
    assert!(
        rows.iter()
            .any(|(t, b)| *t == b'E' && b.windows(6).any(|x| x == format!("C{code}").as_bytes())),
        "{rows:?}"
    );
}
async fn query(s: &mut TcpStream, q: &str) -> Vec<(u8, Vec<u8>)> {
    let mut b = q.as_bytes().to_vec();
    b.push(0);
    send(s, b'Q', &b).await;
    read(s).await
}

fn field(rows: &[(u8, Vec<u8>)], key: u8) -> String {
    let bytes = &rows.iter().find(|r| r.0 == b'E').expect("ErrorResponse").1;
    bytes
        .split(|b| *b == 0)
        .find(|v| v.first() == Some(&key))
        .map(|v| String::from_utf8(v[1..].to_vec()).unwrap())
        .unwrap_or_default()
}
fn literal(path: &std::path::Path) -> String {
    format!(
        "'{}'",
        path.to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''")
    )
}
#[tokio::test]
async fn files_formats_paths_and_queries() {
    let f = Fixture::with_engine(giggles_core::Engine::Legacy).await;
    let mut s = f.connect().await;
    query(&mut s, "INSERT INTO t VALUES (1, 'one', 7)").await;
    for format in ["text", "csv", "binary"] {
        let path = literal(&f.path.join(format));
        let r = query(&mut s, &format!("COPY t TO {path} (FORMAT {format})")).await;
        assert_eq!(tags(&r), b"CZ", "{r:?}");
        assert_eq!(r[0].1, b"COPY 1\0");
        let stdout = query(&mut s, &format!("COPY t TO STDOUT (FORMAT {format})")).await;
        let expected: Vec<u8> = stdout
            .into_iter()
            .filter(|(tag, _)| *tag == b'd')
            .flat_map(|(_, bytes)| bytes)
            .collect();
        assert_eq!(std::fs::read(f.path.join(format)).unwrap(), expected);
        query(&mut s, "DELETE FROM t").await;
        let r = query(&mut s, &format!("COPY t FROM {path} (FORMAT {format})")).await;
        assert_eq!(tags(&r), b"CZ", "{r:?}");
        assert_eq!(r[0].1, b"COPY 1\0");
    }
    let path = f.path.join("query");
    let r = query(
        &mut s,
        &format!(
            "COPY (SELECT i,x FROM t) TO {} (FORMAT csv, HEADER)",
            literal(&path)
        ),
    )
    .await;
    assert_eq!(tags(&r), b"CZ", "{r:?}");
    assert_eq!(std::fs::read(path).unwrap(), b"i,x\n1,one\n");
    std::fs::write(f.path.join("db.redb").join("relative"), b"2\ttwo\t8\n").unwrap();
    let r = query(&mut s, "COPY t FROM 'relative'").await;
    assert_eq!(tags(&r), b"CZ", "{r:?}");
    for (sql, code, message) in [
        (
            "COPY t TO 'relative'".to_owned(),
            "42602",
            "relative path not allowed for COPY to file".to_owned(),
        ),
        (
            format!("COPY t FROM {}", literal(&f.path.join("missing"))),
            "58P01",
            format!(
                "could not open file \"{}\" for reading: No such file or directory",
                f.path.join("missing").to_string_lossy().replace('\\', "/")
            ),
        ),
        (
            format!("COPY t FROM {}", literal(&f.path)),
            "42809",
            format!(
                "\"{}\" is a directory",
                f.path.to_string_lossy().replace('\\', "/")
            ),
        ),
        (
            format!("COPY t TO {}", literal(&f.path)),
            "42809",
            format!(
                "could not open file \"{}\" for writing: Is a directory",
                f.path.to_string_lossy().replace('\\', "/")
            ),
        ),
    ] {
        let r = query(&mut s, &sql).await;
        error(&r, code);
        assert_eq!(field(&r, b'M'), message);
        if code == "58P01" {
            assert_eq!(
                field(&r, b'H'),
                "COPY FROM instructs the PostgreSQL server process to read a file. You may want a client-side facility such as psql's \\copy."
            );
        }
    }
    f.stop().await;
}
#[tokio::test]
async fn external_privileges_precede_table_and_path_checks() {
    let f = Fixture::with_engine(giggles_core::Engine::Legacy).await;
    let mut s = f.connect_as("denied").await;
    for (sql, message, role, action) in [
        (
            "COPY absent FROM 'missing'",
            "permission denied to COPY from a file",
            "pg_read_server_files",
            "from a file",
        ),
        (
            "COPY absent TO 'relative'",
            "permission denied to COPY to a file",
            "pg_write_server_files",
            "to a file",
        ),
        (
            "COPY absent FROM PROGRAM 'missing'",
            "permission denied to COPY to or from an external program",
            "pg_execute_server_program",
            "to or from an external program",
        ),
        (
            "COPY absent TO PROGRAM 'missing'",
            "permission denied to COPY to or from an external program",
            "pg_execute_server_program",
            "to or from an external program",
        ),
    ] {
        let r = query(&mut s, sql).await;
        error(&r, "42501");
        assert_eq!(field(&r, b'M'), message);
        assert_eq!(
            field(&r, b'D'),
            format!("Only roles with privileges of the \"{role}\" role may COPY {action}.")
        );
        assert_eq!(
            field(&r, b'H'),
            "Anyone can COPY to stdout or from stdin. psql's \\copy command also works for anyone."
        );
    }
    f.stop().await;
}
#[tokio::test]
async fn programs_roundtrip_and_exit_details() {
    let f = Fixture::with_engine(giggles_core::Engine::Legacy).await;
    let mut s = f.connect().await;
    query(&mut s, "INSERT INTO t VALUES (1, 'one', 7)").await;
    #[cfg(windows)]
    let (write, read) = ("findstr \"^\" > roundtrip", "type roundtrip");
    #[cfg(unix)]
    let (write, read) = ("cat > roundtrip", "cat roundtrip");
    let r = query(&mut s, &format!("COPY t TO PROGRAM '{write}'")).await;
    assert_eq!(tags(&r), b"CZ", "{r:?}");
    query(&mut s, "DELETE FROM t").await;
    let r = query(&mut s, &format!("COPY t FROM PROGRAM '{read}'")).await;
    assert_eq!(tags(&r), b"CZ", "{r:?}");
    // A row error takes precedence over the program's failing exit status.
    #[cfg(windows)]
    let bad = "echo invalid& exit /b 7";
    #[cfg(unix)]
    let bad = "printf 'invalid\\n'; exit 7";
    let r = query(
        &mut s,
        &format!("COPY t FROM PROGRAM '{}'", bad.replace('\'', "''")),
    )
    .await;
    error(&r, "22P02");
    for code in [7, 126, 127] {
        #[cfg(windows)]
        let command = format!("exit /b {code}");
        #[cfg(unix)]
        let command = format!("exit {code}");
        let r = query(&mut s, &format!("COPY t FROM PROGRAM '{command}'")).await;
        error(&r, "38000");
        assert_eq!(field(&r, b'M'), format!("program \"{command}\" failed"));
        assert_eq!(
            field(&r, b'D'),
            match code {
                126 => "command not executable".into(),
                127 => "command not found".into(),
                _ => format!("child process exited with exit code {code}"),
            }
        );
    }
    f.stop().await;
}

#[tokio::test]
async fn main_format_stored_capability_names_are_ordinary_roles() {
    let f = Fixture::with_collisions(giggles_core::Engine::Legacy, true).await;
    for (name, sql) in [
        ("pg_read_all_data", "SELECT * FROM t"),
        ("pg_write_all_data", "INSERT INTO t VALUES (9, 'x', 1)"),
        ("pg_execute_server_program", "COPY absent TO PROGRAM 'missing'"),
        ("pg_read_server_files", "COPY absent FROM 'missing'"),
        ("pg_write_server_files", "COPY absent TO 'missing'"),
        ("denied", "COPY absent TO PROGRAM 'missing'"),
    ] {
        let mut s = f.connect_as(name).await;
        let result = query(&mut s, sql).await;
        error(&result, "42501");
    }
    // Explicit grants still work for the stored identity.
    let mut owner = f.connect().await;
    let grant = query(&mut owner, "GRANT SELECT ON t TO pg_read_all_data").await;
    assert!(!tags(&grant).contains(&b'E'), "{grant:?}");
    let mut reader = f.connect_as("pg_read_all_data").await;
    let read = query(&mut reader, "SELECT * FROM t").await;
    assert!(!tags(&read).contains(&b'E'), "{read:?}");
    f.stop().await;
}
