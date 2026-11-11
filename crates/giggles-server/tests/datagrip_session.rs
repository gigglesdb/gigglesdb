//! Replay the complete failing DataGrip capture through real PostgreSQL frames.
#[path = "wire_support/mod.rs"]
mod wire_support;

use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use wire_support::{bind, cstr, execute, parse, parse_oids, pg_read, pg_send, pg_sync};

const QUERY_IDS: [&str; 6] = [
    "7209c469ae70459683e52463ffca8884ef6749fee3a10938a86038e2d6847ee1",
    "5c38acc809e7448b4431731bff8bb4a57f1b810f5130eddb043a75c947328af6",
    "ce55afd5c285256cee19c24138585f984372963ea0563f45304f188a55e868e6",
    "11ea5a7167dc8f283e70839be3e325a76d73651104347388eba835b755e4849c",
    "a531fd2c61434a20a5566159cd425d2cd68ef5f8efe7063f4b4f3ea3fb5c0cc6",
    "dd9c1e167e9df273702fe9b35e7398a32fa02d343a0a06c44e90d3e44c25528f",
];

#[derive(Deserialize)]
struct Capture {
    session: String,
    statement: String,
    file: String,
    detail: Option<String>,
}

struct Fixture {
    pg: Running,
    path: PathBuf,
}

impl Fixture {
    async fn new(engine: Engine) -> Self {
        let path = std::env::temp_dir().join(format!(
            "datagrip-session-{engine:?}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&path).unwrap();
        let database = match engine {
            Engine::Legacy => path.join("db.redb"),
            Engine::V2 => path.join("db-v2"),
        };
        let admin = Admin::create_engine(database, Limits::default(), engine).unwrap();
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
        // Same loopback PostgreSQL listener as the existing authority/network tests.
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
        let pg = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
            .await
            .unwrap();
        Self { pg, path }
    }

    async fn connect(&self) -> TcpStream {
        let mut stream = TcpStream::connect(self.pg.address).await.unwrap();
        let mut startup = 196608u32.to_be_bytes().to_vec();
        for value in ["user", "owner", "database", "postgres", ""] {
            cstr(value, &mut startup);
        }
        stream.write_u32((startup.len() + 4) as u32).await.unwrap();
        stream.write_all(&startup).await.unwrap();
        loop {
            let (tag, body) = pg_read(&mut stream).await;
            assert_ne!(tag, b'E', "startup: {}", String::from_utf8_lossy(&body));
            if tag == b'Z' {
                return stream;
            }
        }
    }

    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

// Translate JDBC markers only in SQL, preserving the '?' in captured -- comments.
// The only marker shapes in this frozen capture are ::oid, ::varchar::xid and
// bare schema-OID IN-list markers. Explicit OIDs match JDBC's parameter types.
fn jdbc_sql(sql: &str) -> (String, Vec<u32>) {
    let mut translated = String::new();
    let mut oids = vec![];
    for line in sql.lines() {
        let (code, comment) = line.split_once("--").unwrap_or((line, ""));
        for (at, ch) in code.char_indices() {
            if ch == '?' {
                let suffix = &code[at + 1..];
                oids.push(if suffix.starts_with("::varchar::xid") {
                    1043
                } else {
                    assert!(suffix.starts_with("::oid") || suffix.trim_start().starts_with(')'));
                    26
                });
                translated.push_str(&format!("${}", oids.len()));
            } else {
                translated.push(ch);
            }
        }
        if code.len() != line.len() {
            translated.push_str("--");
            translated.push_str(comment);
        }
        translated.push('\n');
    }
    (translated.trim_end().to_owned(), oids)
}

#[derive(Default, Debug)]
struct ResultSet {
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
}

impl ResultSet {
    fn values(&self, name: &str) -> impl Iterator<Item = &str> {
        let at = self.columns.iter().position(|column| column == name);
        self.rows.iter().filter_map(move |row| row[at?].as_deref())
    }

    fn max_xid(&self) -> Option<u32> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, name)| {
                name.ends_with("state_number")
                    || matches!(
                        name.as_str(),
                        "con_state_id" | "transaction_id" | "current_txid" | "xmin" | "xid"
                    )
            })
            .flat_map(|(at, _)| {
                self.rows.iter().filter_map(move |row| {
                    row[at]
                        .as_deref()
                        .map(|value| value.parse::<u32>().expect("catalog xid"))
                })
            })
            .max()
    }
}

async fn query(
    stream: &mut TcpStream,
    capture: &Capture,
    sql: &str,
    oids: &[u32],
    xid: u32,
) -> ResultSet {
    let xid = xid.to_string();
    let inputs = oids
        .iter()
        .map(|oid| Some(if *oid == 26 { "2200" } else { &xid }))
        .collect::<Vec<_>>();
    let prepared = if oids.is_empty() {
        parse("", sql)
    } else {
        parse_oids("", sql, oids)
    };
    pg_send(stream, b'P', &prepared).await;
    pg_send(stream, b'B', &bind("", "", &inputs, false)).await;
    pg_send(stream, b'D', b"P\0").await;
    pg_send(stream, b'E', &execute("", 0)).await;
    let frames = pg_sync(stream).await;
    let mut result = ResultSet::default();
    for (tag, body) in &frames {
        assert_ne!(
            *tag,
            b'E',
            "session {} statement {} ({}): {}\n{sql}",
            capture.session,
            capture.statement,
            capture.file,
            String::from_utf8_lossy(body)
        );
        if *tag == b'T' {
            let count = u16::from_be_bytes(body[..2].try_into().unwrap());
            let mut at = 2;
            for _ in 0..count {
                let end = at + body[at..].iter().position(|byte| *byte == 0).unwrap();
                result
                    .columns
                    .push(String::from_utf8(body[at..end].to_vec()).unwrap());
                at = end + 19;
            }
            assert_eq!(at, body.len());
        } else if *tag == b'D' {
            let count = u16::from_be_bytes(body[..2].try_into().unwrap());
            let mut at = 2;
            let mut row = vec![];
            for _ in 0..count {
                let length = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                at += 4;
                row.push(if length == -1 {
                    None
                } else {
                    assert!(length >= 0);
                    let end = at + length as usize;
                    let value = String::from_utf8(body[at..end].to_vec()).unwrap();
                    at = end;
                    Some(value)
                });
            }
            assert_eq!(at, body.len());
            assert_eq!(row.len(), result.columns.len());
            result.rows.push(row);
        }
    }
    for expected in *b"12CZ" {
        assert!(
            frames.iter().any(|(tag, _)| *tag == expected),
            "statement {} missing completion {}",
            capture.statement,
            expected as char
        );
    }
    result
}

async fn replay(engine: Engine) {
    let mut captures: Vec<Capture> =
        serde_json::from_str(include_str!("fixtures/datagrip_session/manifest.json")).unwrap();
    assert_eq!(captures.len(), 173);
    let reconnect: Vec<Capture> =
        serde_json::from_str(include_str!("fixtures/datagrip_reconnect/manifest.json")).unwrap();
    assert_eq!(reconnect.len(), 107);
    assert_eq!(
        reconnect
            .iter()
            .map(|c| c.session.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["1060084418", "1060084419", "1060084420"])
    );
    captures.extend(reconnect);
    let reconnect_root =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/datagrip_reconnect");
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/datagrip_session");
    let fixture = Fixture::new(engine).await;
    let mut stream = fixture.connect().await;
    let mut session = captures[0].session.clone();
    let mut sessions = vec![session.clone()];
    let mut max_xid = None;
    let mut initial_xid = None;
    let mut refresh_xid = None;
    let mut hashes = BTreeSet::new();
    let mut created = false;
    let mut refreshed_tables = BTreeSet::new();
    let mut refreshed_columns = BTreeSet::new();
    for capture in &captures {
        if capture.session != session {
            pg_send(&mut stream, b'X', &[]).await;
            drop(stream);
            stream = fixture.connect().await;
            session.clone_from(&capture.session);
            sessions.push(session.clone());
            if matches!(session.as_str(), "1060084418" | "1060084419" | "1060084420") {
                refresh_xid = max_xid;
            }
        }
        let capture_root = if matches!(session.as_str(), "1060084418" | "1060084419" | "1060084420")
        {
            &reconnect_root
        } else {
            &root
        };
        let sql = std::fs::read_to_string(capture_root.join(&capture.file)).unwrap();
        assert!(!sql.contains('\r'), "LF fixture {}", capture.file);
        let (sql, oids) = jdbc_sql(&sql);
        // The server hashes the trimmed PostgreSQL SQL, after JDBC renumbering.
        let hash = hex::encode(Sha256::digest(sql.as_bytes()));
        if let Some(detail) = &capture.detail {
            let expected = detail.split("query_id=").nth(1).unwrap();
            assert_eq!(hash, expected, "captured query_id {}", capture.file);
        }
        hashes.insert(hash);
        if capture.statement == "94685774" {
            assert!(!created);
            assert_eq!(session, "1060084415");
            refresh_xid = Some(max_xid.expect("initial load returned catalog xid"));
            initial_xid = refresh_xid;
            created = true;
        }
        let result = query(
            &mut stream,
            capture,
            &sql,
            &oids,
            refresh_xid.or(max_xid).unwrap_or(0),
        )
        .await;
        // On reconnect JDBC saves current_txid for the following xid filters.
        // Fall back to the last state seen until that result arrives.
        if let Some(xid) = result.values("current_txid").next() {
            refresh_xid = Some(xid.parse::<u32>().expect("current transaction xid"));
        }
        if let Some(xid) = result.max_xid() {
            max_xid = Some(max_xid.unwrap_or(0).max(xid));
        }
        if !created {
            assert!(!result.values("table_name").any(|name| name == "table_name"));
            assert!(!result.values("column_name").any(|name| name == "test"));
        }
        if matches!(
            capture.statement.as_str(),
            "94685802" | "94685862" | "94685924" | "94685976"
        ) {
            assert!(sql.contains("pg_catalog.age("), "incremental tables query");
            assert!(
                result.values("table_name").any(|name| name == "table_name"),
                "{}: {result:?}",
                capture.file
            );
            assert!(result.max_xid().unwrap() >= refresh_xid.unwrap());
            refreshed_tables.insert(session.clone());
        }
        if matches!(capture.statement.as_str(), "94685815" | "94685875") {
            assert!(sql.contains("pg_catalog.age("), "incremental columns query");
            assert!(
                result.values("column_name").any(|name| name == "test"),
                "{}: {result:?}",
                capture.file
            );
            refreshed_columns.insert(session.clone());
        }
    }
    assert!(created);
    assert_eq!(
        sessions,
        [
            "1060084413",
            "1060084414",
            "1060084415",
            "1060084416",
            "1060084417",
            "1060084418",
            "1060084419",
            "1060084420"
        ]
    );
    for expected in QUERY_IDS {
        assert!(
            hashes.contains(expected),
            "missing failing query {expected}"
        );
    }
    let refresh_sessions = BTreeSet::from(["1060084416".to_owned(), "1060084417".to_owned()]);
    assert_eq!(refreshed_columns, refresh_sessions);
    assert_eq!(
        refreshed_tables,
        BTreeSet::from([
            "1060084416".to_owned(),
            "1060084417".to_owned(),
            "1060084418".to_owned(),
            "1060084419".to_owned(),
        ])
    );
    let visible = query(
        &mut stream,
        captures.last().unwrap(),
        "SELECT relname AS table_name FROM pg_catalog.pg_class WHERE relname='table_name'",
        &[],
        max_xid.unwrap(),
    )
    .await;
    assert!(
        visible
            .values("table_name")
            .any(|name| name == "table_name")
    );
    println!(
        "DataGrip {engine:?}: {} statements, zero ErrorResponses; initial xid {}, final xid {}; table_name/test found in refresh sessions; table_name visible after all reconnects",
        captures.len(),
        initial_xid.unwrap(),
        max_xid.unwrap()
    );
    pg_send(&mut stream, b'X', &[]).await;
    drop(stream);
    fixture.stop().await;
}

#[tokio::test]
async fn captured_refresh_legacy() {
    replay(Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn captured_refresh_v2() {
    replay(Engine::V2).await;
}
