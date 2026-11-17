//! Real server tests. Set GIGGLES_SERVER_BIN to run; no database mocks.
use skedaddle::{
    gigglesdb::fence::{Fence, FenceConfig, Intent, Step, require_capability},
    pg::fence::{Connection, FenceError, Rows, Table},
};
use std::{
    collections::BTreeSet,
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
struct Server {
    child: Child,
    path: std::path::PathBuf,
    port: u16,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
struct Db {
    runtime: tokio::runtime::Runtime,
    client: Client,
    port: u16,
}
impl Db {
    fn connect(port: u16, role: &str) -> Result<Self, tokio_postgres::Error> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (client, connection) = runtime.block_on(tokio_postgres::connect(
            &format!("host=127.0.0.1 port={port} user={role} password=n7_fixture dbname=postgres"),
            NoTls,
        ))?;
        runtime.spawn(async move {
            let _ = connection.await;
        });
        Ok(Self {
            runtime,
            client,
            port,
        })
    }
    fn sql(&mut self, sql: &str) -> Rows {
        self.query(sql, Duration::from_secs(10)).unwrap()
    }
}
impl Connection for Db {
    fn query(&mut self, sql: &str, timeout: Duration) -> Result<Rows, String> {
        self.runtime.block_on(async {
            let messages = tokio::time::timeout(timeout, self.client.simple_query(sql))
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| format!("{sql}: {e:?}"))?;
            Ok(messages
                .into_iter()
                .filter_map(|m| match m {
                    SimpleQueryMessage::Row(r) => Some(
                        (0..r.len())
                            .map(|i| r.get(i).unwrap_or("").to_owned())
                            .collect(),
                    ),
                    _ => None,
                })
                .collect())
        })
    }
}
fn setup() -> Option<(Server, Db, Fence)> {
    let Some(binary) = std::env::var_os("GIGGLES_SERVER_BIN") else {
        eprintln!("SKIP gigglesdb_fence: GIGGLES_SERVER_BIN unset (real server required)");
        return None;
    };
    let root = std::path::PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set SKEDADDLE_TEST_ROOT"),
    );
    let path = root.join(format!(
        "n7-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let mut child = Command::new(binary)
        .args(["--dev", "--data"])
        .arg(&path)
        .args(["--port", "0", "--engine", "legacy"])
        .env("GIGGLES_OWNER_PASSWORD", "n7_fixture")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut first = String::new();
    reader.read_line(&mut first).unwrap();
    let port = first
        .split_whitespace()
        .next()
        .unwrap()
        .strip_prefix("postgresql=127.0.0.1:")
        .unwrap()
        .parse()
        .unwrap();
    let server = Server { child, path, port };
    let mut db = Db::connect(port, "postgres").unwrap();
    db.sql("CREATE ROLE app LOGIN PASSWORD 'n7_fixture'");
    db.sql("CREATE ROLE asleep NOLOGIN PASSWORD 'n7_fixture'");
    db.sql("CREATE TABLE public.n7 (id integer)");
    db.sql("GRANT SELECT, INSERT, UPDATE ON TABLE public.n7 TO app");
    db.sql("GRANT DELETE ON TABLE public.n7 TO asleep");
    let mut fence = Fence::new(FenceConfig {
        applications: BTreeSet::from(["app".into(), "asleep".into()]),
        tables: vec![Table {
            schema: "public".into(),
            name: "n7".into(),
            owner: "postgres".into(),
        }],
        deadline_ms: 10_000,
    })
    .unwrap();
    fence.bind_migration("n7-live").unwrap();
    Some((server, db, fence))
}
fn acl(db: &mut Db) -> Rows {
    db.sql("SELECT relacl FROM pg_class WHERE relname='n7'")
}
#[test]
fn roundtrip_login_transaction_and_noop() {
    let Some((server, mut db, mut fence)) = setup() else {
        return;
    };
    let before = acl(&mut db);
    let mut app = Db::connect(server.port, "app").unwrap();
    app.sql("BEGIN");
    app.sql("INSERT INTO public.n7 VALUES (1)");
    let mut intents = vec![];
    let mut journal = |i: &Intent| {
        intents.push(i.clone());
        Ok(())
    };
    while fence.next != Step::Complete {
        fence
            .execute_next(&mut db, &mut journal, &mut || Ok(()))
            .unwrap();
    }
    assert!(app.query("COMMIT", Duration::from_secs(5)).is_err());
    match Db::connect(db.port, "app") {
        Ok(_) => panic!("fenced role logged in"),
        Err(e) => assert_eq!(e.code().unwrap().code(), "28P01"),
    }
    fence.prove(&mut db).unwrap();
    fence
        .execute_next(
            &mut db,
            &mut |_| panic!("completed fence journalled again"),
            &mut || panic!("completed hook reran"),
        )
        .unwrap();
    assert_eq!(db.sql("SELECT count(*) FROM public.n7")[0][0], "0");
    fence.unfence(&mut db, &mut |_| Ok(())).unwrap();
    assert_eq!(acl(&mut db), before);
    let _app = Db::connect(server.port, "app").unwrap();
    assert!(Db::connect(server.port, "asleep").is_err());
    fence.unfence(&mut db, &mut |_| Ok(())).unwrap();
    assert_eq!(acl(&mut db), before);
}
#[test]
fn crash_replay_and_unfence() {
    let Some((_server, mut db, mut fence)) = setup() else {
        return;
    };
    let before = acl(&mut db);
    fence
        .execute_next(&mut db, &mut |_| Ok(()), &mut || Ok(()))
        .unwrap();
    let mut saved = None;
    fence
        .execute_next(
            &mut db,
            &mut |i| {
                if let Intent::Save(f) = i {
                    saved = Some(serde_json::to_string(f).unwrap());
                }
                Ok(())
            },
            &mut || Ok(()),
        )
        .unwrap();
    fence
        .execute_next(&mut db, &mut |_| Ok(()), &mut || Ok(()))
        .unwrap();
    assert_eq!(fence.next, Step::NoLogin);
    // Last durable Save predates mutation; replay retains the original grants.
    let mut recovered: Fence = serde_json::from_str(&saved.unwrap()).unwrap();
    recovered
        .execute_next(&mut db, &mut |_| Ok(()), &mut || Ok(()))
        .unwrap();
    recovered.unfence(&mut db, &mut |_| Ok(())).unwrap();
    assert_eq!(acl(&mut db), before);
}
#[test]
fn owner_and_superuser_block() {
    let Some((_server, mut db, _)) = setup() else {
        return;
    };
    let mut fence = Fence::new(FenceConfig {
        applications: BTreeSet::from(["postgres".into()]),
        tables: vec![Table {
            schema: "public".into(),
            name: "n7".into(),
            owner: "postgres".into(),
        }],
        deadline_ms: 10_000,
    })
    .unwrap();
    fence.bind_migration("n7-owner").unwrap();
    fence
        .execute_next(&mut db, &mut |_| Ok(()), &mut || Ok(()))
        .unwrap();
    assert!(matches!(
        fence.prove(&mut db),
        Err(FenceError::ResidualBlocked(_))
    ));
    assert!(
        fence
            .residuals
            .iter()
            .any(|r| r.principal.kind == skedaddle::pg::fence::ResidualKind::Superuser)
    );
    assert!(
        fence
            .residuals
            .iter()
            .any(|r| r.principal.kind == skedaddle::pg::fence::ResidualKind::TableOwner)
    );
    assert!(matches!(
        fence.execute_next(&mut db, &mut |_| Ok(()), &mut || Ok(())),
        Err(FenceError::ResidualBlocked(_))
    ));
    assert!(fence.saved.is_none());
    let _still_login = Db::connect(db.port, "postgres").unwrap();
}
#[test]
fn live_capabilities_and_missing_refusal() {
    let Some((_server, mut db, _)) = setup() else {
        return;
    };
    let rows = db.sql("SHOW giggles_capabilities");
    require_capability(&rows).unwrap();
    // Older-server advertisement is not configurable. Exercise its pure guard
    // with the real response with the required token removed.
    let missing = rows
        .into_iter()
        .map(|r| {
            r.into_iter()
                .map(|s| {
                    s.split(',')
                        .filter(|v| *v != "role_session_termination")
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect()
        })
        .collect();
    assert!(require_capability(&missing).is_err());
}

#[path = "gigglesdb_fence/source_fence.rs"]
mod source_fence;
