//! Real process coverage for the durable embedded write fence and token-file option.
#![cfg(feature = "engine-rocksdb")]
mod api_support;

use api_support::Fixture;
use giggles_core::{Engine, roles::NewRole};
use serde_json::json;
use std::{net::SocketAddr, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
};
use tokio_postgres::NoTls;

const TOKEN: &str = "e4d-test-holder-secret";
const WRONG: &str = "e4d-test-wrong-secret";

async fn scenario(token: Option<&str>, allowed: bool) {
    let fixture = Fixture::catalog_empty_engine(Engine::V2).await;
    let path = fixture.path.clone();
    fixture
        .admin
        .roles()
        .create(NewRole {
            name: "root".into(),
            login: true,
            superuser: true,
            ..NewRole::default()
        })
        .unwrap();
    let mut sql = fixture.admin.sql_session();
    for statement in [
        "CREATE TABLE fenced_rows (id BIGINT PRIMARY KEY)",
        "INSERT INTO fenced_rows VALUES (1)",
        "CREATE SEQUENCE fenced_seq",
    ] {
        sql.execute(statement, &[], None).unwrap();
    }
    drop(sql);
    let epoch = fixture
        .admin
        .install_write_fence(TOKEN.as_bytes(), "server test")
        .unwrap();
    fixture.stop().await;
    let mut config = json!({
        "database":path.join("db.redb"),"native":"127.0.0.1:0","postgres":"127.0.0.1:0",
        "postgresql":{"listen":"127.0.0.1:0","tls":"disabled","loopback_trust":true},
        "certificate":path.join("server.pem"),"private_key":path.join("server.key.TEST-ONLY"),
        "client_ca":path.join("ca.pem"),"bindings":[],"connections":32,
        "skew_seconds":0,"max_pause_seconds":3600
    });
    if let Some(token) = token {
        let file = path.join("holder-token.TEST-ONLY");
        std::fs::write(&file, format!("{token}\r\n")).unwrap();
        config["write_fence_token_file"] = json!(file);
    }
    let file = path.join("server.json");
    std::fs::write(&file, serde_json::to_vec(&config).unwrap()).unwrap();
    let stderr_path = path.join("server.stderr.log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-server"))
        .arg(file)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(std::fs::File::create(&stderr_path).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(30), stdout.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    let address: SocketAddr = line
        .split_whitespace()
        .find_map(|field| field.strip_prefix("postgresql="))
        .unwrap_or_else(|| {
            panic!(
                "readiness={line}; stderr={}",
                std::fs::read_to_string(&stderr_path).unwrap()
            )
        })
        .parse()
        .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(address.port())
        .user("root")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    client
        .simple_query("SELECT * FROM fenced_rows")
        .await
        .unwrap();
    for statement in [
        "INSERT INTO fenced_rows VALUES (2)",
        "UPDATE fenced_rows SET id = 3 WHERE id = 1",
        "DELETE FROM fenced_rows",
        "SELECT nextval('fenced_seq')",
        "CREATE TABLE holder_created (id BIGINT)",
    ] {
        eprintln!("checking {statement}");
        let result = client.simple_query(statement).await;
        if allowed {
            result.unwrap();
        } else {
            assert_fenced(result.expect_err(statement), epoch);
        }
    }
    {
        eprintln!("checking COPY");
        let sink = client
            .copy_in("COPY fenced_rows FROM STDIN (FORMAT BINARY)")
            .await
            .unwrap();
        let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(
            sink,
            &[tokio_postgres::types::Type::INT8],
        );
        let mut writer = std::pin::pin!(writer);
        writer.as_mut().write(&[&4_i64]).await.unwrap();
        let result = writer.as_mut().finish().await;
        if allowed {
            assert_eq!(result.unwrap(), 1);
        } else {
            assert_fenced(result.unwrap_err(), epoch);
        }
    }
    eprintln!("checking extended INSERT");
    // Extended protocol goes through its own diagnostic path.
    let result = client
        .execute("INSERT INTO fenced_rows VALUES (5)", &[])
        .await;
    if allowed {
        assert_eq!(result.unwrap(), 1);
    } else {
        assert_fenced(result.unwrap_err(), epoch);
    }
    drop(client);
    connection.await.unwrap().unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"shutdown\n")
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(30), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    stdout.read_line(&mut line).await.unwrap();
    let stderr = std::fs::read_to_string(stderr_path).unwrap();
    assert_eq!(
        stderr.contains("write fence holder token loaded from file"),
        token.is_some()
    );
    for secret in [TOKEN, WRONG] {
        assert!(!stderr.contains(secret));
        assert!(!line.contains(secret));
    }
}

fn assert_fenced(error: tokio_postgres::Error, epoch: u64) {
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "25006");
    assert_eq!(
        error.message(),
        format!("database is write-fenced (epoch {epoch})")
    );
    assert!(!error.message().contains(TOKEN));
    assert!(!error.message().contains(WRONG));
}

#[tokio::test]
async fn without_token_reads_succeed_and_writes_are_fenced() {
    scenario(None, false).await;
}
#[tokio::test]
async fn holder_token_file_permits_writes() {
    scenario(Some(TOKEN), true).await;
}
#[tokio::test]
async fn wrong_token_file_keeps_writes_fenced() {
    scenario(Some(WRONG), false).await;
}

#[tokio::test]
async fn empty_token_and_cluster_combination_are_rejected_before_open() {
    let path = std::env::temp_dir().join(format!(
        "holder-config-{}-{}",
        std::process::id(),
        api_support::now()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let token_file = path.join("token.TEST-ONLY");
    std::fs::write(&token_file, b"\n").unwrap();
    let mut config = json!({
        "database":path.join("must-not-open"),"native":"127.0.0.1:0","postgres":"127.0.0.1:0",
        "certificate":"unused.pem","private_key":"unused.key","client_ca":"unused.pem",
        "bindings":[],"connections":32,"skew_seconds":0,"max_pause_seconds":3600,
        "write_fence_token_file":token_file
    });
    for cluster in [false, true] {
        if cluster {
            config["cluster"] = json!({"node_id":1,"cluster":"0123456789abcdef0123456789abcdef",
                "listen":"127.0.0.1:7101","log_dir":"unused"});
        }
        let file = path.join("config.json");
        std::fs::write(&file, serde_json::to_vec(&config).unwrap()).unwrap();
        let output = tokio::time::timeout(
            Duration::from_secs(20),
            Command::new(env!("CARGO_BIN_EXE_giggles-server"))
                .arg(file)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains(if cluster {
                "cluster: cannot be combined with write_fence_token_file"
            } else {
                "write fence holder token file is empty"
            }),
            "{stderr}"
        );
        assert!(!path.join("must-not-open").exists());
    }
    std::fs::remove_dir_all(path).unwrap();
}
