//! GD-0084: PostgreSQL 18.6 implicit-block and prepared-plan wire oracle.
//! Regenerate with the ignored generate_pg18_fixture test and GD0084_PG_ADDR,
//! GD0084_PG_DIGEST, GD0084_PG_DATE set; PostgreSQL must use trust auth.
#![cfg(feature = "engine-rocksdb")]
#[path = "wire_support/mod.rs"]
#[allow(dead_code)]
mod wire_support;
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, serve_postgresql},
    clock::Clock,
};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use wire_support::{bind, cstr, execute, parse, pg_read};

const NAMES: [&str; 12] = [
    "simple_single_error",
    "simple_middle_error",
    "simple_success",
    "extended_middle_error",
    "extended_success",
    "extended_bind_error",
    "named_reexecute",
    "explicit_extended",
    "unnamed_ddl",
    "named_ddl",
    "simple_ddl",
    "timezone_rollback",
];
struct Fixture {
    pg: Running,
    path: PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "gd0084-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db"), Limits::default(), Engine::V2).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "postgres".into(),
                superuser: true,
                login: true,
                ..Default::default()
            })
            .unwrap();
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let config: PostgresqlConfig = serde_json::from_value(json!({
            "listen":"127.0.0.1:0", "tls":"disabled", "loopback_trust":true
        }))
        .unwrap();
        let pg = serve_postgresql(
            "127.0.0.1:0".parse().unwrap(),
            service,
            Arc::new(config.build().unwrap()),
        )
        .await
        .unwrap();
        Self { pg, path }
    }
    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}
async fn connect(address: &str) -> TcpStream {
    let mut s = TcpStream::connect(address).await.unwrap();
    s.set_nodelay(true).unwrap();
    let mut b = 196608u32.to_be_bytes().to_vec();
    for v in ["user", "postgres", "database", "postgres", ""] {
        cstr(v, &mut b);
    }
    s.write_u32((b.len() + 4) as u32).await.unwrap();
    s.write_all(&b).await.unwrap();
    loop {
        let (tag, body) = pg_read(&mut s).await;
        assert_ne!(tag, b'E', "startup: {body:?}");
        if tag == b'Z' {
            return s;
        }
    }
}
fn frame(tag: u8, body: &[u8], out: &mut Vec<u8>) {
    out.push(tag);
    out.extend(((body.len() + 4) as u32).to_be_bytes());
    out.extend(body);
}
// Preserve every backend message type except backend-specific ParameterStatus.
// For the four semantic messages preserve SQLSTATE, tag, rows, and tx status.
fn normalize(tag: u8, b: &[u8]) -> Value {
    let kind = (tag as char).to_string();
    match tag {
        b'E' => {
            let mut rest = b;
            let mut code = None;
            while rest.first().is_some_and(|v| *v != 0) {
                let n = rest[1..].iter().position(|v| *v == 0).unwrap() + 1;
                if rest[0] == b'C' {
                    code = Some(String::from_utf8(rest[1..n].to_vec()).unwrap());
                }
                rest = &rest[n + 1..];
            }
            json!([kind, code.unwrap()])
        }
        b'C' => json!([kind, std::str::from_utf8(&b[..b.len() - 1]).unwrap()]),
        b'Z' => json!([kind, (b[0] as char).to_string()]),
        b'D' => {
            let count = u16::from_be_bytes(b[..2].try_into().unwrap());
            let mut at = 2;
            let mut row = vec![];
            for _ in 0..count {
                let len = i32::from_be_bytes(b[at..at + 4].try_into().unwrap());
                at += 4;
                if len < 0 {
                    row.push(Value::Null);
                } else {
                    row.push(json!(
                        std::str::from_utf8(&b[at..at + len as usize]).unwrap()
                    ));
                    at += len as usize;
                }
            }
            json!([kind, row])
        }
        _ => json!([kind]),
    }
}
async fn exchange(s: &mut TcpStream, bytes: Vec<u8>) -> Value {
    s.write_all(&bytes).await.unwrap();
    let mut messages = vec![];
    loop {
        let (tag, b) = pg_read(s).await;
        if tag != b'S' {
            messages.push(normalize(tag, &b));
        }
        if tag == b'Z' {
            return json!(messages);
        }
    }
}
async fn query(s: &mut TcpStream, sql: &str) -> Value {
    let mut b = vec![];
    let mut text = vec![];
    cstr(sql, &mut text);
    frame(b'Q', &text, &mut b);
    exchange(s, b).await
}
fn pbe(out: &mut Vec<u8>, name: &str, sql: &str, params: &[Option<&str>]) {
    frame(b'P', &parse(name, sql), out);
    be(out, name, params, false);
}
fn be(out: &mut Vec<u8>, name: &str, params: &[Option<&str>], describe: bool) {
    frame(b'B', &bind("", name, params, false), out);
    if describe {
        frame(b'D', b"P\0", out);
    }
    frame(b'E', &execute("", 0), out);
}
async fn sync(s: &mut TcpStream, mut b: Vec<u8>) -> Value {
    frame(b'S', &[], &mut b);
    exchange(s, b).await
}
async fn scenario(address: &str, index: usize) -> Value {
    let mut a = connect(address).await;
    // Each case owns a fresh table (separate GigglesDB instance on replay).
    let table = format!("gd0084_t_{index}");
    let setup = query(
        &mut a,
        &format!("CREATE TABLE {table} (id int PRIMARY KEY, v text)"),
    )
    .await;
    assert_eq!(setup, json!([["C", "CREATE TABLE"], ["Z", "I"]]));
    let setup = query(
        &mut a,
        &format!("INSERT INTO {table} VALUES (1,'a'),(2,'b')"),
    )
    .await;
    assert_eq!(setup, json!([["C", "INSERT 0 2"], ["Z", "I"]]));
    let ins = |id| {
        format!(
            "INSERT INTO {table} VALUES ({id},'{}')",
            match id {
                1 => "dup",
                3 => "c",
                _ => "d",
            }
        )
    };
    let mut steps = vec![];
    match index {
        0 => {
            steps.push(query(&mut a, &ins(1)).await);
            steps.push(query(&mut a, "SELECT 7").await);
        }
        1 => steps.push(query(&mut a, &format!("{}; {}; {}", ins(3), ins(1), ins(4))).await),
        2 => steps.push(query(&mut a, &format!("{}; {}", ins(3), ins(4))).await),
        3..=5 => {
            let mut b = vec![];
            pbe(&mut b, "", &ins(3), &[]);
            if index == 5 {
                pbe(
                    &mut b,
                    "",
                    &format!("INSERT INTO {table} VALUES ($1,'bad')"),
                    &[Some("x")],
                );
            } else {
                pbe(&mut b, "", &ins(if index == 3 { 1 } else { 4 }), &[]);
            }
            if index != 4 {
                pbe(&mut b, "", &ins(4), &[]);
            }
            steps.push(sync(&mut a, b).await);
        }
        6 => {
            let mut b = vec![];
            frame(
                b'P',
                &parse("saved", &format!("INSERT INTO {table} VALUES ($1,$2)")),
                &mut b,
            );
            steps.push(sync(&mut a, b).await);
            for (id, v) in [("3", "c"), ("1", "dup"), ("4", "d")] {
                let mut b = vec![];
                be(&mut b, "saved", &[Some(id), Some(v)], false);
                steps.push(sync(&mut a, b).await);
            }
        }
        7 => {
            for sql in ["BEGIN".into(), ins(3), ins(1), "COMMIT".into()] {
                let mut b = vec![];
                pbe(&mut b, "", &sql, &[]);
                steps.push(sync(&mut a, b).await);
            }
        }
        8..=10 => {
            let sql = format!("SELECT * FROM {table} WHERE id = $1");
            let name = if index == 9 { "saved" } else { "" };
            if index == 10 {
                steps.push(query(&mut a, &format!("SELECT * FROM {table} ORDER BY id")).await);
            } else {
                let mut b = vec![];
                frame(b'P', &parse(name, &sql), &mut b);
                steps.push(sync(&mut a, b).await);
            }
            let mut other = connect(address).await;
            let ddl = if index == 9 {
                format!("ALTER TABLE {table} ALTER COLUMN v TYPE int USING length(v)")
            } else {
                format!("ALTER TABLE {table} ADD COLUMN w int")
            };
            steps.push(query(&mut other, &ddl).await);
            drop(other);
            if index == 10 {
                steps.push(query(&mut a, &format!("SELECT * FROM {table} ORDER BY id")).await);
            } else {
                let mut b = vec![];
                be(&mut b, name, &[Some("1")], true);
                steps.push(sync(&mut a, b).await);
            }
        }
        11 => {
            assert_eq!(
                query(&mut a, "SET TimeZone='UTC'").await,
                json!([["C", "SET"], ["Z", "I"]])
            );
            steps.push(query(&mut a, "SHOW TimeZone").await);
            steps.push(query(&mut a, "SET TimeZone='America/New_York'; SELECT 1/0").await);
            steps.push(query(&mut a, "SHOW TimeZone").await);
        }
        _ => unreachable!(),
    }
    let state = query(&mut a, &format!("SELECT * FROM {table} ORDER BY id")).await;
    json!({"steps": steps, "state": state})
}
async fn replay(index: usize) {
    let f = Fixture::new().await;
    let actual = scenario(&f.pg.address.to_string(), index).await;
    f.stop().await;
    let oracle: Value =
        serde_json::from_str(include_str!("fixtures/gd0084/implicit_blocks_pg18.json")).unwrap();
    if let Ok(dir) = std::env::var("GD0084_CAPTURE") {
        std::fs::write(
            PathBuf::from(dir).join(format!("{}.json", NAMES[index])),
            serde_json::to_string_pretty(&actual).unwrap(),
        )
        .unwrap();
    }
    assert_eq!(
        actual, oracle["scenarios"][NAMES[index]],
        "{}",
        NAMES[index]
    );
}
macro_rules! case {
    ($name:ident, $index:expr, $reason:literal) => {
        #[tokio::test]
        #[ignore = $reason]
        async fn $name() {
            replay($index).await;
        }
    };
    ($name:ident, $index:expr) => {
        #[tokio::test]
        async fn $name() {
            replay($index).await;
        }
    };
}
case!(simple_single_error, 0);
case!(simple_middle_error, 1);
case!(simple_success, 2);
case!(extended_middle_error, 3);
case!(extended_success, 4);
case!(extended_bind_error, 5);
case!(named_reexecute, 6);
case!(explicit_extended, 7);
case!(
    unnamed_ddl,
    8,
    "GD-0084: post-DDL unnamed Bind/Describe succeeds then Execute gives XX000; PG rejects Bind with 0A000"
);
case!(
    named_ddl,
    9,
    "GD-0084: ALTER COLUMN TYPE USING is rejected with 42601; PG alters then rejects cached result type with 0A000"
);
case!(simple_ddl, 10);
case!(timezone_rollback, 11);

#[tokio::test]
#[ignore = "oracle generator: requires disposable PostgreSQL 18.6 and GD0084_PG_* environment"]
async fn generate_pg18_fixture() {
    let address = std::env::var("GD0084_PG_ADDR").unwrap();
    let digest = std::env::var("GD0084_PG_DIGEST").unwrap();
    let date = std::env::var("GD0084_PG_DATE").unwrap();
    let mut scenarios = serde_json::Map::new();
    for (i, name) in NAMES.iter().enumerate() {
        scenarios.insert((*name).into(), scenario(&address, i).await);
    }
    let fixture = json!({"header": {"postgres": "18.6", "image_repo_digest": digest,
        "captured_utc_date": date, "normalization": "message type; E SQLSTATE; C command tag; D text row/null; Z transaction status; omit ParameterStatus"}, "scenarios": scenarios});
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/gd0084/implicit_blocks_pg18.json"
        ),
        format!("{}\n", serde_json::to_string_pretty(&fixture).unwrap()),
    )
    .unwrap();
}
