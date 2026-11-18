//! GD-0084: PostgreSQL 18.6 pipelined extended-protocol wire oracle.
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

const NAMES: [&str; 8] = [
    "many_cycles",
    "error_mid_block",
    "multi_sync_error",
    "bind_error_mid",
    "explicit_tx_pipeline",
    "named_reuse",
    "describe_mix",
    "flush_mid",
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
// Exactly one write per pipeline, regardless of its number of Syncs.
async fn exchange(s: &mut TcpStream, bytes: Vec<u8>, ready: usize) -> Value {
    s.write_all(&bytes).await.unwrap();
    let mut messages = vec![];
    let mut seen = 0;
    loop {
        let (tag, b) = pg_read(s).await;
        if tag != b'S' {
            messages.push(normalize(tag, &b));
        }
        if tag == b'Z' {
            seen += 1;
            if seen == ready {
                return json!(messages);
            }
        }
    }
}
fn simple(out: &mut Vec<u8>, sql: &str) {
    let mut text = vec![];
    cstr(sql, &mut text);
    frame(b'Q', &text, out);
}
async fn query(s: &mut TcpStream, sql: &str) -> Value {
    let mut b = vec![];
    simple(&mut b, sql);
    exchange(s, b, 1).await
}
fn pbe(out: &mut Vec<u8>, sql: &str, params: &[Option<&str>]) {
    frame(b'P', &parse("", sql), out);
    be(out, "", params);
}
fn be(out: &mut Vec<u8>, name: &str, params: &[Option<&str>]) {
    frame(b'B', &bind("", name, params, false), out);
    frame(b'E', &execute("", 0), out);
}
fn sync(out: &mut Vec<u8>) {
    frame(b'S', &[], out);
}
async fn scenario(address: &str, index: usize) -> Value {
    let mut a = connect(address).await;
    let table = format!("gd0084_pipeline_{index}");
    assert_eq!(
        query(
            &mut a,
            &format!("CREATE TABLE {table} (id int PRIMARY KEY)")
        )
        .await,
        json!([["C", "CREATE TABLE"], ["Z", "I"]])
    );
    let ins = format!("INSERT INTO {table} VALUES ($1::int) RETURNING id");
    let count = format!("SELECT count(*) FROM {table}");
    let mut b = vec![];
    let mut steps = vec![];
    let ready = match index {
        0 => {
            for id in 1..=100 {
                pbe(&mut b, &ins, &[Some(&id.to_string())]);
                sync(&mut b);
            }
            100
        }
        1 => {
            for id in ["1", "1", "3"] {
                pbe(&mut b, &ins, &[Some(id)]);
            }
            sync(&mut b);
            pbe(&mut b, &count, &[]);
            sync(&mut b);
            2
        }
        2 => {
            pbe(&mut b, &ins, &[Some("1")]);
            sync(&mut b);
            // Insert first so the error must roll back this cycle's write.
            pbe(&mut b, &ins, &[Some("2")]);
            pbe(&mut b, "SELECT 1/0", &[]);
            sync(&mut b);
            pbe(&mut b, &ins, &[Some("3")]);
            sync(&mut b);
            3
        }
        3 => {
            pbe(&mut b, &ins, &[Some("1")]);
            // Reuse the parsed unnamed statement; invalid input must fail Bind.
            be(&mut b, "", &[Some("invalid")]);
            sync(&mut b);
            pbe(&mut b, &ins, &[Some("3")]);
            sync(&mut b);
            2
        }
        4 => {
            pbe(&mut b, "BEGIN", &[]);
            pbe(&mut b, &ins, &[Some("1")]);
            pbe(&mut b, &ins, &[Some("2")]);
            pbe(&mut b, &ins, &[Some("1")]);
            // Both this bare Execute and the next statement must be skipped.
            frame(b'E', &execute("", 0), &mut b);
            pbe(&mut b, &ins, &[Some("3")]);
            sync(&mut b);
            pbe(&mut b, "ROLLBACK", &[]);
            sync(&mut b);
            pbe(&mut b, &ins, &[Some("4")]);
            sync(&mut b);
            3
        }
        5 => {
            frame(b'P', &parse("saved", &ins), &mut b);
            for id in 1..=50 {
                be(&mut b, "saved", &[Some(&id.to_string())]);
            }
            sync(&mut b);
            1
        }
        6 => {
            for id in ["1", "2"] {
                frame(b'P', &parse("", &ins), &mut b);
                frame(b'D', b"S\0", &mut b);
                frame(b'B', &bind("", "", &[Some(id)], false), &mut b);
                frame(b'D', b"P\0", &mut b);
                frame(b'E', &execute("", 0), &mut b);
                sync(&mut b);
                simple(&mut b, &count);
            }
            4
        }
        7 => {
            pbe(&mut b, &ins, &[Some("1")]);
            frame(b'H', &[], &mut b);
            a.write_all(&b).await.unwrap();
            let mut messages = vec![];
            loop {
                let (tag, body) = pg_read(&mut a).await;
                if tag != b'S' {
                    messages.push(normalize(tag, &body));
                }
                assert_ne!(tag, b'Z', "Flush must not end the implicit block");
                if tag == b'C' {
                    break;
                }
            }
            steps.push(json!(messages));
            b.clear();
            sync(&mut b);
            1
        }
        _ => unreachable!(),
    };
    steps.push(exchange(&mut a, b, ready).await);
    // Observe committed state from a separate session, including count and order.
    let mut other = connect(address).await;
    let state = query(
        &mut other,
        &format!("{count}; SELECT id FROM {table} ORDER BY id"),
    )
    .await;
    json!({"steps": steps, "state": state})
}
async fn replay(index: usize) {
    let f = Fixture::new().await;
    let actual = scenario(&f.pg.address.to_string(), index).await;
    f.stop().await;
    let oracle: Value =
        serde_json::from_str(include_str!("fixtures/gd0084/pipeline_pg18.json")).unwrap();
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
case!(many_cycles, 0);
case!(error_mid_block, 1);
case!(multi_sync_error, 2);
case!(bind_error_mid, 3);
case!(explicit_tx_pipeline, 4);
case!(named_reuse, 5);
case!(describe_mix, 6);
case!(flush_mid, 7);

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
            "/tests/fixtures/gd0084/pipeline_pg18.json"
        ),
        format!("{}\n", serde_json::to_string_pretty(&fixture).unwrap()),
    )
    .unwrap();
}
