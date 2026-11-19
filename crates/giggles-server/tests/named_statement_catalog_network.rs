//! PostgreSQL 18.6 basis: named plans revalidate after another session commits DDL.
#![cfg(feature = "engine-rocksdb")]

use giggles_core::{Admin, Engine, Limits, roles::NewRole};
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
use tokio_postgres::NoTls;

async fn case(label: &str, setup: &str, sql: &str, change: &str, expected: Option<&str>) {
    let path = std::env::temp_dir().join(format!("named-stale-{}-{label}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    for (name, superuser) in [("owner", true), ("r", false)] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                login: true,
                superuser: superuser || (name == "r" && label == "bind-drift-granted"),
                ..Default::default()
            })
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
        connections: 4,
    };
    let pg = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let mut clients = Vec::new();
    let mut tasks = Vec::new();
    for role in [
        "owner",
        if matches!(label, "e" | "h" | "seq" | "implicit-seq") {
            "r"
        } else {
            "owner"
        },
    ] {
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(pg.address.port())
            .user(role)
            .dbname("postgres")
            .connect(NoTls)
            .await
            .unwrap();
        clients.push(client);
        tasks.push(tokio::spawn(connection));
    }
    for sql in setup.split(';').filter(|sql| !sql.trim().is_empty()) {
        clients[0].batch_execute(sql).await.unwrap();
    }
    if matches!(label, "seq" | "implicit-seq" | "seq-control") {
        admin
            .roles()
            .set_table_owner(if label == "implicit-seq" { "s2" } else { "s" }, "r")
            .unwrap();
    }
    if matches!(label, "g" | "i") {
        clients[1]
            .batch_execute("SET search_path = a,b")
            .await
            .unwrap();
    }
    let result = if label.starts_with("bind-drift") {
        if label.contains("view") {
            admin.roles().grant_table("v", "r", &[giggles_core::roles::TablePrivilege::Select]).unwrap();
        }
        bind_drift_probe(pg.address, &clients[0], &admin, label).await;
        None
    } else if label.starts_with("bind-auth") {
        if label.contains("view") {
            admin
                .roles()
                .grant_table("v", "r", &[giggles_core::roles::TablePrivilege::Select])
                .unwrap();
        }
        bind_authority_probe(pg.address, &clients[0], &admin, label).await;
        None
    } else if label.starts_with("describe-") {
        describe_authority_probe(pg.address, &clients[0], label).await;
        None
    } else if label == "suspended" {
        suspended_probe(pg.address, &clients[0]).await;
        None
    } else if label.starts_with("portal") {
        portal_probe(pg.address, &clients[0], &admin, label, change).await;
        None
    } else {
        let statement = clients[1].prepare(sql).await.unwrap();
        if label == "txn-before" || label == "txn-rr" {
            clients[1]
                .batch_execute(if label == "txn-rr" {
                    "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1"
                } else {
                    "BEGIN; SELECT 1"
                })
                .await
                .unwrap();
        }
        if matches!(label, "seq" | "implicit-seq") {
            admin
                .roles()
                .set_table_owner(if label == "implicit-seq" { "s2" } else { "s" }, "owner")
                .unwrap();
        }
        for sql in change.split(';').filter(|sql| !sql.trim().is_empty()) {
            clients[0].batch_execute(sql).await.unwrap();
        }
        if matches!(label, "seq" | "implicit-seq") {
            let sequence_sql = if label == "seq" {
                "SELECT nextval('s')"
            } else {
                "INSERT INTO t2(a) VALUES(1)"
            };
            assert_eq!(
                clients[1]
                    .prepare(sequence_sql)
                    .await
                    .unwrap_err()
                    .as_db_error()
                    .unwrap()
                    .code()
                    .code(),
                "42501"
            );
        }
        if label == "txn-after" {
            clients[1].batch_execute("BEGIN").await.unwrap();
        }
        let value = 7i32;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
            if label == "f" { &[&value] } else { &[] };
        let result = clients[1].query(&statement, params).await;
        if label.starts_with("txn-") {
            {
                assert_eq!(
                    result
                        .as_ref()
                        .unwrap_err()
                        .as_db_error()
                        .unwrap()
                        .code()
                        .code(),
                    "0A000"
                );
                assert_eq!(
                    clients[1]
                        .batch_execute("SELECT 1")
                        .await
                        .unwrap_err()
                        .as_db_error()
                        .unwrap()
                        .code()
                        .code(),
                    "25P02"
                );
            }
            clients[1].batch_execute("ROLLBACK").await.unwrap();
            assert_eq!(
                clients[1]
                    .query(&statement, &[])
                    .await
                    .unwrap_err()
                    .as_db_error()
                    .unwrap()
                    .code()
                    .code(),
                "0A000"
            );
            assert_eq!(clients[1].prepare(sql).await.unwrap().columns().len(), 2);
        }
        if matches!(label, "seq" | "implicit-seq" | "seq-control") {
            let fresh = clients[1].prepare(sql).await.unwrap_err();
            assert_eq!(
                fresh.as_db_error().unwrap().code(),
                result.as_ref().unwrap_err().as_db_error().unwrap().code()
            );
        }
        Some(result)
    };
    drop(clients);
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    pg.shutdown().await.unwrap();
    drop(service);
    drop(admin);
    for attempt in 0..20 {
        match std::fs::remove_dir_all(&path) {
            Ok(()) => break,
            Err(error) if attempt == 19 => panic!("cleanup: {error}"),
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
        }
    }
    let Some(result) = result else {
        return;
    };
    if let Some(code) = expected {
        let error = result.expect_err("stale named statement unexpectedly succeeded");
        let error = error.as_db_error().expect("database error");
        assert_eq!(error.code().code(), code, "{error}");
        if code == "0A000" {
            assert_eq!(error.message(), "cached plan must not change result type");
        }
    } else {
        let rows = result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
        assert_eq!(rows[0].columns()[0].name(), "a");
        assert_eq!(rows[0].get::<_, i32>(0), 7);
    }
}

#[tokio::test]
async fn star_add_column() {
    case(
        "a",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7)",
        "SELECT * FROM t",
        "ALTER TABLE t ADD COLUMN b int",
        Some("0A000"),
    )
    .await;
}
#[tokio::test]
async fn star_drop_column() {
    case(
        "b",
        "CREATE TABLE t(a int,b int); INSERT INTO t VALUES(7,8)",
        "SELECT * FROM t",
        "ALTER TABLE t DROP COLUMN b",
        Some("0A000"),
    )
    .await;
}
#[tokio::test]
async fn explicit_column_add_column() {
    case(
        "c",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7)",
        "SELECT a FROM t",
        "ALTER TABLE t ADD COLUMN b int",
        None,
    )
    .await;
}
#[tokio::test]
async fn drop_table() {
    case(
        "d",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7)",
        "SELECT a FROM t",
        "DROP TABLE t",
        Some("42P01"),
    )
    .await;
}
#[tokio::test]
async fn revoke_select() {
    case(
        "e",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7); GRANT SELECT ON t TO r",
        "SELECT a FROM t",
        "REVOKE SELECT ON t FROM r",
        Some("42501"),
    )
    .await;
}
#[tokio::test]
async fn insert_drop_column() {
    case(
        "f",
        "CREATE TABLE t(a int,b int)",
        "INSERT INTO t(a) VALUES($1)",
        "ALTER TABLE t DROP COLUMN a",
        Some("42703"),
    )
    .await;
}

#[tokio::test]
async fn search_path_rebind_after_drop() {
    case(
        "g",
        "CREATE SCHEMA a; CREATE SCHEMA b; CREATE TABLE a.t(x int); CREATE TABLE b.t(x int); INSERT INTO a.t VALUES(1); INSERT INTO b.t VALUES(7)",
        "SELECT x AS a FROM t",
        "DROP TABLE a.t",
        None,
    )
    .await;
}

#[tokio::test]
async fn revoked_existing_dependency_precedes_missing_relation() {
    case(
        "h",
        "CREATE TABLE t(a int); CREATE TABLE u(a int); GRANT SELECT ON t TO r; GRANT SELECT ON u TO r",
        "SELECT t.a FROM t CROSS JOIN u",
        "REVOKE SELECT ON t FROM r; DROP TABLE u",
        Some("42501"),
    )
    .await;
}

#[tokio::test]
async fn search_path_rebind_preserves_result_type() {
    case(
        "i",
        "CREATE SCHEMA a; CREATE SCHEMA b; CREATE TABLE a.t(x int); CREATE TABLE b.t(x text)",
        "SELECT x AS a FROM t",
        "DROP TABLE a.t",
        Some("0A000"),
    )
    .await;
}

#[tokio::test]
async fn explicit_sequence_authority_before_missing_relation() {
    case(
        "seq",
        "CREATE SEQUENCE s; CREATE TABLE t(a int); GRANT SELECT ON t TO r",
        "SELECT nextval('s'), t.a FROM t",
        "DROP TABLE t",
        Some("42501"),
    )
    .await;
}
#[tokio::test]
async fn implicit_sequence_authority_before_missing_relation() {
    case("implicit-seq", "CREATE SEQUENCE s2; CREATE TABLE t(a int); CREATE TABLE t2(id int DEFAULT nextval('s2'), a int); GRANT SELECT ON t TO r; GRANT INSERT ON t2 TO r",
        "INSERT INTO t2(a) SELECT a FROM t", "DROP TABLE t", Some("42501")).await;
}
#[tokio::test]
async fn sequence_authority_intact_missing_relation() {
    case(
        "seq-control",
        "CREATE SEQUENCE s; CREATE TABLE t(a int); GRANT SELECT ON t TO r",
        "SELECT nextval('s'), t.a FROM t",
        "DROP TABLE t",
        Some("42P01"),
    )
    .await;
}
#[tokio::test]
async fn catalog_changed_before_begin() {
    case(
        "txn-after",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7)",
        "SELECT * FROM t",
        "ALTER TABLE t ADD COLUMN b int",
        Some("0A000"),
    )
    .await;
}
#[tokio::test]
async fn catalog_changed_after_begin() {
    case(
        "txn-before",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7)",
        "SELECT * FROM t",
        "ALTER TABLE t ADD COLUMN b int",
        Some("0A000"),
    )
    .await;
}
#[tokio::test]
async fn repeatable_read_without_snapshot_revalidates() {
    case(
        "txn-rr",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7)",
        "SELECT * FROM t",
        "ALTER TABLE t ADD COLUMN b int",
        Some("0A000"),
    )
    .await;
}

async fn receive(stream: &mut TcpStream, end: u8) -> Vec<(u8, Vec<u8>)> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut messages = Vec::new();
        loop {
            let tag = stream.read_u8().await.unwrap();
            let len = stream.read_u32().await.unwrap();
            assert!((4..65536).contains(&len));
            let mut body = vec![0; len as usize - 4];
            stream.read_exact(&mut body).await.unwrap();
            messages.push((tag, body));
            if tag == end {
                return messages;
            }
        }
    })
    .await
    .unwrap()
}
async fn send(stream: &mut TcpStream, frames: &[(u8, &[u8])]) {
    let mut packet = Vec::new();
    for (tag, body) in frames {
        giggles_server::pgwire::frame(*tag, body, &mut packet).unwrap();
    }
    stream.write_all(&packet).await.unwrap();
}
async fn portal_probe(
    address: std::net::SocketAddr,
    ddl: &tokio_postgres::Client,
    admin: &Admin,
    label: &str,
    change: &str,
) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let body = b"\0\x03\0\0user\0owner\0database\0postgres\0\0";
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
    receive(&mut stream, b'Z').await;
    let explicit = label.contains("explicit");
    if explicit {
        send(&mut stream, &[(b'Q', b"BEGIN\0")]).await;
        receive(&mut stream, b'Z').await;
    }
    let mut parse = b"s\0".to_vec();
    giggles_server::pgwire::z(
        if label.contains("nested") {
            "SELECT * FROM v2"
        } else if label.contains("view") {
            "SELECT * FROM v"
        } else {
            "SELECT * FROM t"
        },
        &mut parse,
    );
    parse.extend_from_slice(&[0, 0]);
    send(
        &mut stream,
        &[
            (b'P', &parse),
            (b'B', b"p\0s\0\0\0\0\0\0\0"),
            (b'D', b"Pp\0"),
            (b'H', b""),
        ],
    )
    .await;
    let bound = receive(&mut stream, b'T').await;
    assert!(!bound.iter().any(|(tag, _)| *tag == b'E'));
    assert_eq!(&bound.last().unwrap().1[..2], &[0, 1]);
    if explicit {
        send(&mut stream, &[(b'S', b"")]).await;
        receive(&mut stream, b'Z').await;
    }
    let changed = ddl.batch_execute(change);
    tokio::pin!(changed);
    // Wait for a queued request, or the direct DROP path's NOWAIT rejection.
    // Neither outcome relies on elapsed time as evidence of lock protection.
    let queued = tokio::select! {
        result = &mut changed => {
            assert!(!label.contains("view"), "view DDL must wait: {result:?}");
            assert_eq!(result.unwrap_err().as_db_error().unwrap().code().code(), "55P03");
            false
        },
        barrier = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while admin.waiting_lock_count() == 0 {
                tokio::task::yield_now().await;
            }
        }) => {
            barrier.expect("DDL lock request queued");
            true
        },
    };
    send(&mut stream, &[(b'E', b"p\0\0\0\0\0"), (b'S', b"")]).await;
    let executed = receive(&mut stream, b'Z').await;
    assert!(
        !executed.iter().any(|(tag, _)| *tag == b'E'),
        "{label}: {executed:?}"
    );
    assert_eq!(executed.iter().filter(|(tag, _)| *tag == b'D').count(), 1);
    let row = &executed.iter().find(|(tag, _)| *tag == b'D').unwrap().1;
    assert_eq!(&row[..2], &[0, 1]);
    assert_eq!(&row[6..], b"7");
    if explicit {
        if queued {
            assert!(admin.waiting_lock_count() > 0, "DDL remains queued until COMMIT");
        }
        send(&mut stream, &[(b'Q', b"COMMIT\0")]).await;
        receive(&mut stream, b'Z').await;
    }
    if queued {
        tokio::time::timeout(std::time::Duration::from_secs(10), changed)
            .await
            .unwrap()
            .unwrap();
    }
    send(&mut stream, &[(b'X', b"")]).await;
}
#[tokio::test]
async fn portal_ddl_between_bind_and_execute() {
    for (label, change) in [
        ("portal-add", "ALTER TABLE t ADD COLUMN b int"),
        ("portal-drop", "DROP TABLE t"),
        ("portal-explicit-add", "ALTER TABLE t ADD COLUMN b int"),
        ("portal-explicit-drop", "DROP TABLE t"),
    ] {
        case(
            label,
            "CREATE TABLE t(a int); INSERT INTO t VALUES(7)",
            "",
            change,
            None,
        )
        .await;
    }
}

async fn suspended_probe(address: std::net::SocketAddr, owner: &tokio_postgres::Client) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let body = b"\0\x03\0\0user\0r\0database\0postgres\0\0";
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
    receive(&mut stream, b'Z').await;
    let mut parse = b"s\0".to_vec();
    giggles_server::pgwire::z("SELECT a FROM t ORDER BY a", &mut parse);
    parse.extend_from_slice(&[0, 0]);
    send(
        &mut stream,
        &[
            (b'P', &parse),
            (b'B', b"p\0s\0\0\0\0\0\0\0"),
            (b'E', b"p\0\0\0\0\x02"),
            (b'H', b""),
        ],
    )
    .await;
    let first = receive(&mut stream, b's').await;
    assert_eq!(first.iter().filter(|(tag, _)| *tag == b'D').count(), 2);
    assert!(!first.iter().any(|(tag, _)| *tag == b'E'));
    assert_eq!(
        first
            .iter()
            .filter(|(tag, _)| *tag == b'D')
            .map(|(_, row)| {
                assert_eq!(&row[..2], &[0, 1]);
                &row[6..]
            })
            .collect::<Vec<_>>(),
        vec![b"1".as_slice(), b"2".as_slice()]
    );
    owner
        .batch_execute("REVOKE SELECT ON t FROM r")
        .await
        .unwrap();
    send(&mut stream, &[(b'E', b"p\0\0\0\0\0"), (b'H', b"")]).await;
    let rest = receive(&mut stream, b'C').await;
    assert_eq!(rest.iter().filter(|(tag, _)| *tag == b'D').count(), 3);
    assert!(!rest.iter().any(|(tag, _)| *tag == b'E'));
    assert_eq!(
        rest.iter()
            .filter(|(tag, _)| *tag == b'D')
            .map(|(_, row)| {
                assert_eq!(&row[..2], &[0, 1]);
                &row[6..]
            })
            .collect::<Vec<_>>(),
        vec![b"3".as_slice(), b"4".as_slice(), b"5".as_slice()]
    );
    send(&mut stream, &[(b'B', b"p2\0s\0\0\0\0\0\0\0"), (b'S', b"")]).await;
    let rebound = receive(&mut stream, b'Z').await;
    let code = rebound
        .iter()
        .find(|(tag, _)| *tag == b'E')
        .and_then(|(_, body)| {
            body.split(|byte| *byte == 0)
                .find_map(|field| field.strip_prefix(b"C"))
        })
        .map(|code| String::from_utf8_lossy(code).into_owned());
    send(&mut stream, &[(b'X', b"")]).await;
    assert_eq!(
        code.as_deref(),
        Some("42501"),
        "new Bind response: {rebound:?}"
    );
}
#[tokio::test]
async fn suspended_portal_retains_start_authority_after_revoke() {
    case(
        "suspended",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(1),(2),(3),(4),(5); GRANT SELECT ON t TO r",
        "",
        "",
        None,
    )
    .await;
}

#[tokio::test]
async fn view_portal_locks_underlying_table_until_sync() {
    case(
        "portal-view",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7); CREATE VIEW v AS SELECT a FROM t",
        "",
        "ALTER TABLE t ADD COLUMN b int",
        None,
    )
    .await;
}
#[tokio::test]
async fn nested_view_portal_locks_underlying_table_until_commit() {
    case("portal-explicit-nested-view", "CREATE TABLE t(a int); INSERT INTO t VALUES(7); CREATE VIEW v AS SELECT a FROM t; CREATE VIEW v2 AS SELECT a FROM v",
        "", "ALTER TABLE t ADD COLUMN b int", None).await;
}

fn response_code(messages: &[(u8, Vec<u8>)]) -> Option<&str> {
    messages
        .iter()
        .find(|(tag, _)| *tag == b'E')
        .and_then(|(_, body)| {
            body.split(|byte| *byte == 0)
                .find_map(|field| field.strip_prefix(b"C"))
        })
        .map(|code| std::str::from_utf8(code).unwrap())
}
async fn bind_authority_probe(
    address: std::net::SocketAddr,
    owner: &tokio_postgres::Client,
    admin: &Admin,
    label: &str,
) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let startup = b"\0\x03\0\0user\0r\0database\0postgres\0\0";
    stream.write_u32((startup.len() + 4) as u32).await.unwrap();
    stream.write_all(startup).await.unwrap();
    receive(&mut stream, b'Z').await;
    let sql = if label.contains("view") {
        "SELECT * FROM v"
    } else {
        "SELECT * FROM t"
    };
    let mut parse = b"s\0".to_vec();
    giggles_server::pgwire::z(sql, &mut parse);
    parse.extend_from_slice(&[0, 0]);
    send(&mut stream, &[(b'P', &parse), (b'S', b"")]).await;
    assert_eq!(response_code(&receive(&mut stream, b'Z').await), None);
    let transaction = label.ends_with("rc") || label.ends_with("rr");
    if transaction {
        let begin = if label.ends_with("rr") {
            b"BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT * FROM t\0".as_slice()
        } else {
            b"BEGIN; SELECT * FROM t\0".as_slice()
        };
        send(&mut stream, &[(b'Q', begin)]).await;
        assert_eq!(response_code(&receive(&mut stream, b'Z').await), None);
    }
    if label.contains("view") {
        // This profile's SQL GRANT/REVOKE validates physical tables only.
        admin
            .roles()
            .revoke_table("v", "r", &[giggles_core::roles::TablePrivilege::Select])
            .unwrap();
    } else {
        owner
            .batch_execute("REVOKE SELECT ON t FROM r")
            .await
            .unwrap();
    }
    let contended = label.contains("locked");
    if contended {
        owner
            .batch_execute("BEGIN; LOCK TABLE t IN ACCESS EXCLUSIVE MODE")
            .await
            .unwrap();
    }
    send(&mut stream, &[(b'B', b"p\0s\0\0\0\0\0\0\0"), (b'S', b"")]).await;
    let early = tokio::time::timeout(
        std::time::Duration::from_millis(750),
        receive(&mut stream, b'Z'),
    )
    .await;
    if contended {
        owner.batch_execute("ROLLBACK").await.unwrap();
    }
    let immediate = early.is_ok();
    let rebound = match early {
        Ok(messages) => messages,
        Err(_) => receive(&mut stream, b'Z').await,
    };
    if transaction {
        send(&mut stream, &[(b'Q', b"ROLLBACK\0")]).await;
        receive(&mut stream, b'Z').await;
    }
    // Fresh Parse is the ordering oracle, after recovering the aborted block.
    parse[0] = b'f';
    send(&mut stream, &[(b'P', &parse), (b'S', b"")]).await;
    let fresh = receive(&mut stream, b'Z').await;
    send(&mut stream, &[(b'X', b"")]).await;
    assert!(
        immediate,
        "{label}: Bind waited for the protected relation lock"
    );
    assert_eq!(response_code(&fresh), Some("42501"));
    assert_eq!(
        response_code(&rebound),
        response_code(&fresh),
        "{label}: {rebound:?}"
    );
    assert!(
        !rebound.iter().any(|(tag, _)| *tag == b'2'),
        "unauthorized Bind completed"
    );
}
#[tokio::test]
async fn revoked_bind_checks_current_grants_outside_transaction() {
    case(
        "bind-auth-outside",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7); GRANT SELECT ON t TO r",
        "",
        "",
        None,
    )
    .await;
}
#[tokio::test]
async fn revoked_bind_checks_current_grants_read_committed() {
    case(
        "bind-auth-rc",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7); GRANT SELECT ON t TO r",
        "",
        "",
        None,
    )
    .await;
}
#[tokio::test]
async fn revoked_bind_checks_current_grants_repeatable_read() {
    case(
        "bind-auth-rr",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7); GRANT SELECT ON t TO r",
        "",
        "",
        None,
    )
    .await;
}
#[tokio::test]
async fn revoked_bind_precedes_exclusive_table_lock() {
    case(
        "bind-auth-locked-table",
        "CREATE TABLE t(a int); GRANT SELECT ON t TO r",
        "",
        "",
        None,
    )
    .await;
}
#[tokio::test]
async fn unreadable_view_bind_precedes_underlying_exclusive_lock() {
    case("bind-auth-locked-view", "CREATE TABLE t(a int); CREATE VIEW v AS SELECT a FROM t; GRANT SELECT ON t TO r", "", "", None).await;
}

// Start Bind while its old grants are valid, then change the catalog under its wait.
async fn bind_drift_probe(
    address: std::net::SocketAddr,
    owner: &tokio_postgres::Client,
    admin: &Admin,
    label: &str,
) {
    let view = label.contains("view");
    let between = label.contains("between");
    let cancelled = label.contains("cancel");
    let recreate = label == "bind-drift-recreate" || label == "bind-drift-granted";
    let granted = label == "bind-drift-granted";
    let mut stream = TcpStream::connect(address).await.unwrap();
    let startup = b"\0\x03\0\0user\0r\0database\0postgres\0\0";
    stream.write_u32((startup.len() + 4) as u32).await.unwrap();
    stream.write_all(startup).await.unwrap();
    receive(&mut stream, b'Z').await;
    let mut parse = b"s\0".to_vec();
    giggles_server::pgwire::z(if view { "SELECT a FROM v" } else { "SELECT a FROM t" }, &mut parse);
    parse.extend_from_slice(&[0, 0]);
    send(&mut stream, &[(b'P', &parse), (b'S', b"")]).await;
    assert_eq!(response_code(&receive(&mut stream, b'Z').await), None);
    if view {
        send(&mut stream, &[(b'Q', b"BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1\0")]).await;
        assert_eq!(response_code(&receive(&mut stream, b'Z').await), None);
    }
    let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancellation = Arc::new(std::sync::Mutex::new(None));
    if cancelled {
        let cancellation = cancellation.clone();
        admin.set_decision_hook(Some(Arc::new(move |point| {
            if point == giggles_core::DecisionPoint::PreparedBindAuthorized {
                *cancellation.lock().unwrap() = giggles_core::cancellation::Cancellation::current();
            }
        })));
    }
    if between {
        let (reached, release) = (reached.clone(), release.clone());
        admin.set_decision_hook(Some(Arc::new(move |point| {
            if point == giggles_core::DecisionPoint::PreparedBindAuthorized {
                reached.store(true, std::sync::atomic::Ordering::Release);
                while !release.load(std::sync::atomic::Ordering::Acquire) { std::thread::yield_now(); }
            }
        })));
    } else {
        owner.batch_execute(if view {
            "BEGIN; CREATE OR REPLACE VIEW v(a) AS SELECT b FROM t"
        } else { "BEGIN; LOCK TABLE t IN ACCESS EXCLUSIVE MODE" }).await.unwrap();
    }
    send(&mut stream, &[(b'B', b"p\0s\0\0\0\0\0\0\0"), (b'H', b"")]).await;
    let terminal = if granted { b'2' } else { b'E' };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while if between { !reached.load(std::sync::atomic::Ordering::Acquire) }
            else { admin.waiting_lock_count() == 0 } {
            tokio::task::yield_now().await;
        }
    }).await.expect("Bind reached deterministic barrier");
    // SQL GRANT/REVOKE is disallowed in an explicit transaction (25001).
    // Change grants after Bind waits, while SQL DDL still holds the old lock.
    if !granted {
        admin
            .roles()
            .revoke_table(if view { "v" } else { "t" }, "r", &[giggles_core::roles::TablePrivilege::Select])
            .unwrap();
    }
    // The control uses a superuser reader: DDL holds the role-store writer
    // until commit, so an out-of-band GRANT here would deadlock the setup.
    if cancelled {
        assert!(cancellation.lock().unwrap().as_ref().expect("Bind cancellation scope").cancel());
    } else {
        owner.batch_execute(if view { "DROP VIEW v" } else { "DROP TABLE t" }).await.unwrap();
    }
    if recreate {
        owner.batch_execute("CREATE TABLE t(a int)").await.unwrap();
    }
    if !between && !cancelled { owner.batch_execute("COMMIT").await.unwrap(); }
    release.store(true, std::sync::atomic::Ordering::Release);
    admin.set_decision_hook(None);
    let rebound = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        receive(&mut stream, terminal),
    )
    .await
    .expect("Bind response after DDL commit");
    assert_eq!(
        response_code(&rebound),
        if granted { None } else { Some("42501") },
        "{label}: {rebound:?}"
    );
    if cancelled { owner.batch_execute("ROLLBACK").await.unwrap(); }
    if recreate && !granted {
        // No Sync yet: a wrongly acquired replacement lock would still be held.
        owner
            .batch_execute("BEGIN; LOCK TABLE t IN ACCESS EXCLUSIVE MODE NOWAIT; ROLLBACK")
            .await
            .unwrap();
    }
    send(&mut stream, &[(b'S', b"")]).await;
    receive(&mut stream, b'Z').await;
    if view {
        send(&mut stream, &[(b'Q', b"ROLLBACK\0")]).await;
        receive(&mut stream, b'Z').await;
    }
    parse[0] = b'f';
    send(&mut stream, &[(b'P', &parse), (b'S', b"")]).await;
    let fresh = receive(&mut stream, b'Z').await;
    assert_eq!(
        response_code(&fresh),
        response_code(&rebound),
        "{label}: fresh Parse"
    );
    send(&mut stream, &[(b'X', b"")]).await;
}

#[tokio::test]
async fn bind_wait_reauthorizes_after_revoke_and_drop() {
    case(
        "bind-drift-drop",
        "CREATE TABLE t(a int); GRANT SELECT ON t TO r",
        "",
        "",
        None,
    )
    .await;
}

#[tokio::test]
async fn bind_wait_does_not_lock_unreadable_replacement() {
    case(
        "bind-drift-recreate",
        "CREATE TABLE t(a int); GRANT SELECT ON t TO r",
        "",
        "",
        None,
    )
    .await;
}

#[tokio::test]
async fn bind_wait_accepts_readable_replacement() {
    case(
        "bind-drift-granted",
        "CREATE TABLE t(a int); GRANT SELECT ON t TO r",
        "",
        "",
        None,
    )
    .await;
}

#[tokio::test]
async fn bind_rr_view_wait_reauthorizes_after_revoke_drop() {
    case("bind-drift-view", "CREATE TABLE t(a int,b int); CREATE VIEW v AS SELECT a FROM t; GRANT SELECT ON t TO r", "", "", None).await;
}
#[tokio::test]
async fn bind_view_reauthorizes_drift_between_authorization_and_locking() {
    case("bind-drift-view-between", "CREATE TABLE t(a int); CREATE VIEW v AS SELECT a FROM t; GRANT SELECT ON t TO r", "", "", None).await;
}

#[tokio::test]
async fn bind_cancelled_wait_after_revoke_reports_authorization() {
    case("bind-drift-cancel", "CREATE TABLE t(a int); GRANT SELECT ON t TO r", "", "", None).await;
}

// Describe must check live grants even when the cached shape belongs to a pinned snapshot.
async fn describe_authority_probe(
    address: std::net::SocketAddr,
    owner: &tokio_postgres::Client,
    label: &str,
) {
    for role in ["r", "owner"] {
        for portal in [false, true] {
            owner.batch_execute("GRANT SELECT ON t TO r").await.unwrap();
            let mut stream = TcpStream::connect(address).await.unwrap();
            let mut startup = b"\0\x03\0\0user\0".to_vec();
            giggles_server::pgwire::z(role, &mut startup);
            startup.extend_from_slice(b"database\0postgres\0\0");
            stream.write_u32((startup.len() + 4) as u32).await.unwrap();
            stream.write_all(&startup).await.unwrap();
            receive(&mut stream, b'Z').await;
            let mut parse = b"s\0".to_vec();
            giggles_server::pgwire::z("SELECT * FROM t", &mut parse);
            parse.extend_from_slice(&[0, 0]);
            send(&mut stream, &[(b'P', &parse), (b'S', b"")]).await;
            assert_eq!(response_code(&receive(&mut stream, b'Z').await), None);
            let begin = match label {
                "describe-rr" => b"BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1\0".as_slice(),
                "describe-serializable" => b"BEGIN ISOLATION LEVEL SERIALIZABLE; SELECT 1\0".as_slice(),
                _ => b"BEGIN ISOLATION LEVEL READ COMMITTED; SELECT 1\0".as_slice(),
            };
            send(&mut stream, &[(b'Q', begin)]).await;
            assert_eq!(response_code(&receive(&mut stream, b'Z').await), None);
            if portal {
                send(&mut stream, &[(b'B', b"p\0s\0\0\0\0\0\0\0"), (b'S', b"")]).await;
                assert_eq!(response_code(&receive(&mut stream, b'Z').await), None);
            }
            let describe = if portal { b"Pp\0" } else { b"Ss\0" };
            // Still-granted control, before the revoke, uses the same cached plan.
            send(&mut stream, &[(b'D', describe), (b'S', b"")]).await;
            let granted = receive(&mut stream, b'Z').await;
            assert_eq!(response_code(&granted), None);
            assert!(granted.iter().any(|(tag, _)| *tag == b'T'));
            owner.batch_execute("REVOKE SELECT ON t FROM r").await.unwrap();
            send(&mut stream, &[(b'D', describe), (b'S', b"")]).await;
            let described = receive(&mut stream, b'Z').await;
            assert_eq!(response_code(&described), if role == "r" { Some("42501") } else { None }, "{label}, {role}, portal={portal}: {described:?}");
            if role == "r" {
                assert!(!described.iter().any(|(tag, _)| matches!(tag, b't' | b'T' | b'n')));
            } else {
                assert!(described.iter().any(|(tag, _)| *tag == b'T'));
            }
            send(&mut stream, &[(b'Q', b"ROLLBACK\0")]).await;
            receive(&mut stream, b'Z').await;
            parse[0] = b'f';
            send(&mut stream, &[(b'P', &parse), (b'S', b"")]).await;
            assert_eq!(response_code(&receive(&mut stream, b'Z').await), response_code(&described));
            send(&mut stream, &[(b'X', b"")]).await;
        }
    }
}
#[tokio::test]
async fn describe_checks_current_grants_read_committed() {
    case("describe-rc", "CREATE TABLE t(a int); GRANT SELECT ON t TO r", "", "", None).await;
}
#[tokio::test]
async fn describe_checks_current_grants_repeatable_read() {
    case("describe-rr", "CREATE TABLE t(a int); GRANT SELECT ON t TO r", "", "", None).await;
}
#[tokio::test]
async fn describe_checks_current_grants_serializable() {
    case("describe-serializable", "CREATE TABLE t(a int); GRANT SELECT ON t TO r", "", "", None).await;
}
