mod api_role_support;
use api_role_support::{FIRST, http, proof};
use serde_json::json;
use std::net::SocketAddr;
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    time::timeout,
};
use tokio_postgres::{Client, NoTls};

const BOUND: Duration = Duration::from_secs(30);
const PASSWORD: &str = "Dev-pass_1";
const RESET_PASSWORD: &str = "Reset-pass_2";
static SERIAL: AtomicU64 = AtomicU64::new(0);

fn fixture() -> PathBuf {
    let root = std::env::var_os("GIGGLES_DEV_PROCESS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!(
            "dev-process-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed),
        ));
    for subdir in ["cwd", "tmp"] {
        std::fs::create_dir_all(root.join(subdir)).unwrap();
    }
    root
}

fn command(root: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_giggles-server"));
    command
        .current_dir(root.join("cwd"))
        .env("TEMP", root.join("tmp"))
        .env("TMP", root.join("tmp"))
        .env("CARGO_TARGET_DIR", root.join("target"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    command
}

// Keep ownership through every assertion and timeout, including startup failures.
struct Server(Child, PathBuf, Option<SocketAddr>, Option<SocketAddr>);
impl Drop for Server {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            #[cfg(windows)]
            if let Some(pid) = self.0.id() {
                let _ = std::process::Command::new("taskkill")
                    .args(["/T", "/F", "/PID", &pid.to_string()])
                    .output();
            }
            let _ = self.0.start_kill();
        }
    }
}
impl Server {
    async fn start(root: &Path, first: bool) -> (Self, String) {
        Self::start_mode(root, first, false).await
    }

    async fn start_mode(root: &Path, first: bool, trust: bool) -> (Self, String) {
        let mut command = command(root);
        command.stderr(Stdio::from(
            std::fs::File::create(root.join("stderr")).unwrap(),
        ));
        command.arg("--dev");
        if trust {
            command.arg("--trust-local");
        }
        command.arg("--data").arg(root.join("data")).args([
            "--port",
            "0",
            "--owner",
            "owner",
            "--database",
            "app",
        ]);
        if first {
            command.env("GIGGLES_OWNER_PASSWORD", PASSWORD);
        } else {
            command.env_remove("GIGGLES_OWNER_PASSWORD");
        }
        let mut server = Self(command.spawn().unwrap(), root.join("stderr"), None, None);
        let mut lines = BufReader::new(server.0.stdout.take().unwrap()).lines();
        let (address_line, url) = timeout(BOUND, async {
            let address = lines
                .next_line()
                .await
                .unwrap()
                .expect("startup address line missing");
            let url = lines
                .next_line()
                .await
                .unwrap()
                .expect("startup URL line missing");
            (address, url)
        })
        .await
        .expect("server did not advertise readiness within 30 seconds");
        let address = address_line
            .strip_prefix("postgresql=")
            .unwrap()
            .strip_suffix(" preview=single-host")
            .expect("exact address banner")
            .parse::<std::net::SocketAddr>()
            .unwrap();
        assert_eq!(address.ip(), std::net::Ipv4Addr::LOCALHOST);
        assert_ne!(address.port(), 0);
        let credentials = if first { "owner:Dev-pass_1" } else { "owner" };
        assert_eq!(url, format!("postgres://{credentials}@{address}/app"));
        let http = timeout(BOUND, lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.2 = Some(
            http.strip_prefix("http: http://")
                .unwrap()
                .strip_suffix("/v1/sessions (role sign-in)")
                .unwrap()
                .parse()
                .unwrap(),
        );
        let grpc = timeout(BOUND, lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.3 = Some(
            grpc.strip_prefix("grpc: ")
                .unwrap()
                .strip_suffix(" (h2c)")
                .unwrap()
                .parse()
                .unwrap(),
        );
        let connection_url = if first {
            url
        } else {
            format!("postgres://owner:{PASSWORD}@{address}/app")
        };
        (server, connection_url)
    }

    async fn shutdown(mut self) -> String {
        self.0
            .stdin
            .take()
            .unwrap()
            .write_all(b"shutdown\n")
            .await
            .unwrap();
        let status = timeout(BOUND, self.0.wait())
            .await
            .expect("shutdown exceeded 30 seconds")
            .unwrap();
        assert!(status.success(), "server exit: {status}");
        std::fs::read_to_string(&self.1).unwrap()
    }
}

async fn connect(url: &str) -> Client {
    let (client, connection) = timeout(BOUND, tokio_postgres::connect(url, NoTls))
        .await
        .expect("connect exceeded 30 seconds")
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_default_admits_100_postgresql_sessions_and_reuses_a_slot() {
    use giggles_server::api::proto;
    use tokio::{io::AsyncReadExt, net::TcpStream, task::JoinSet};
    use tokio_postgres::{
        SimpleQueryMessage,
        error::{Severity, SqlState},
    };

    async fn select_one(client: &Client) {
        let messages = client.simple_query("SELECT 1").await.unwrap();
        assert!(matches!(
            &messages[..],
            [
                SimpleQueryMessage::RowDescription(_),
                SimpleQueryMessage::Row(row),
                SimpleQueryMessage::CommandComplete(1),
            ] if row.get(0) == Some("1")
        ));
    }

    let root = fixture();
    let (server, url) = Server::start(&root, true).await;
    timeout(BOUND, async {
        let mut opening = JoinSet::new();
        for _ in 0..100 {
            let url = url.clone();
            opening.spawn(async move {
                let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
                let driver = tokio::spawn(connection);
                select_one(&client).await;
                (client, driver)
            });
        }
        let mut sessions = Vec::new();
        while let Some(session) = opening.join_next().await {
            sessions.push(session.unwrap());
        }
        assert_eq!(sessions.len(), 100);

        // Both API listeners can still answer while the PostgreSQL pool is full.
        let mut grpc =
            proto::giggles_client::GigglesClient::connect(format!("http://{}", server.3.unwrap()))
                .await
                .unwrap();
        let error = grpc
            .open_session(proto::OpenSessionRequest {
                role: Some("owner".into()),
                database: Some("app".into()),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        let request = json!({"role":"owner","database":"app"});
        assert_eq!(
            http(server.2.unwrap(), "POST", "/v1/sessions", None, request)
                .await
                .0,
            401
        );
        drop(grpc);

        let config: tokio_postgres::Config = url.parse().unwrap();
        let port = config.get_ports()[0];
        let mut excess = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        for code in [80877104, 80877103] {
            excess.write_u32(8).await.unwrap();
            excess.write_u32(code).await.unwrap();
            assert_eq!(excess.read_u8().await.unwrap(), b'N');
        }
        let startup = b"\0\x03\0\0user\0owner\0database\0app\0\0";
        excess.write_u32((startup.len() + 4) as u32).await.unwrap();
        excess.write_all(startup).await.unwrap();
        assert_eq!(excess.read_u8().await.unwrap(), b'E');
        let expected = b"SFATAL\0VFATAL\0C53300\0Msorry, too many clients already\0\0";
        assert_eq!(excess.read_u32().await.unwrap() as usize, expected.len() + 4);
        let mut reply = vec![0; expected.len()];
        excess.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply, expected);
        assert_eq!(excess.read(&mut [0]).await.unwrap(), 0);

        let (client, driver) = sessions.pop().unwrap();
        drop(client);
        driver.await.unwrap().unwrap();
        // The client driver may finish sending Terminate before the server reclaims the slot.
        let replacement = loop {
            match tokio_postgres::connect(&url, NoTls).await {
                Ok(session) => break session,
                Err(error) => {
                    let error = error.as_db_error().expect("server admission error");
                    assert_eq!(error.code(), &SqlState::TOO_MANY_CONNECTIONS);
                    assert_eq!(error.parsed_severity(), Some(Severity::Fatal));
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        };
        let (client, connection) = replacement;
        let driver = tokio::spawn(connection);
        select_one(&client).await;
        sessions.push((client, driver));
        for (client, driver) in sessions {
            drop(client);
            driver.await.unwrap().unwrap();
        }
    })
    .await
    .expect("dev connection-limit scenario exceeded bound");
    let stderr = server.shutdown().await;
    assert!(stderr.contains(
        "protocol=postgresql rejected stage=admission reason=too_many_connections"
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn dev_binary_crud_password_restart_and_filesystem_contract() {
    timeout(BOUND * 4, async {
        let root = fixture();
        let (server, url) = Server::start(&root, true).await;
        let wrong = url.replace(PASSWORD, "wrong-password");
        let error = tokio_postgres::connect(&wrong, NoTls)
            .await
            .err()
            .expect("wrong password accepted");
        assert_eq!(
            error.code().expect("authentication SQLSTATE").code(),
            "28P01"
        );
        let client = connect(&url).await;
        client
            .batch_execute("CREATE TABLE items (id BIGINT PRIMARY KEY, body TEXT)")
            .await
            .unwrap();
        assert_eq!(
            client
                .execute("INSERT INTO items VALUES ($1, $2)", &[&1i64, &"one"])
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            client
                .execute("INSERT INTO items VALUES (2, 'two')", &[])
                .await
                .unwrap(),
            1
        );
        let mut rows: Vec<(i64, String)> = client
            .query("SELECT * FROM items LIMIT 10", &[])
            .await
            .unwrap()
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        rows.sort();
        assert_eq!(rows, [(1, "one".into()), (2, "two".into())]);
        assert_eq!(
            client
                .execute("UPDATE items SET body = 'after' WHERE id = 1", &[])
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            client
                .execute("DELETE FROM items WHERE id = 2", &[])
                .await
                .unwrap(),
            1
        );
        drop(client);
        server.shutdown().await;

        let (server, url) = Server::start(&root, false).await;
        let client = connect(&url).await;
        let rows = client
            .query("SELECT * FROM items LIMIT 10", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i64>(0), 1);
        assert_eq!(rows[0].get::<_, String>(1), "after");
        drop(client);
        server.shutdown().await;
        let admin =
            giggles_core::Admin::open(root.join("data/db"), giggles_core::Limits::default())
                .unwrap();
        assert_eq!(admin.snapshot().is_err(), cfg!(feature = "engine-rocksdb"));
        drop(admin);
        // Recovery is offline: even an occupied selected port must not prevent it.
        let reserved = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = reserved.local_addr().unwrap().port();
        let (status, stdout, stderr) = reset(&root, "owner", port).await;
        assert!(stderr.contains("shown once"), "{stderr}");
        assert!(status.success());
        assert_eq!(
            stdout,
            format!("postgres://owner:{RESET_PASSWORD}@127.0.0.1:{port}/app\n")
        );
        drop(reserved);
        assert!(std::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).is_err());

        let (server, old_url) = Server::start(&root, false).await;
        let error = tokio_postgres::connect(&old_url, NoTls)
            .await
            .err()
            .expect("old password accepted after reset");
        assert_eq!(error.code().unwrap().code(), "28P01");
        let client = connect(&old_url.replace(PASSWORD, RESET_PASSWORD)).await;
        let rows = client
            .query("SELECT * FROM items LIMIT 10", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, String>(1), "after");
        drop(client);
        server.shutdown().await;
        assert_eq!(std::fs::read_dir(root.join("cwd")).unwrap().count(), 0);
        let entries: Vec<_> = std::fs::read_dir(root.join("data"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(entries, [std::ffi::OsString::from("db")]);
        assert!(root.join("data/db").is_dir());
    })
    .await
    .expect("dev process scenario exceeded its overall bound");
}

#[tokio::test]
async fn invalid_argument_exits_with_usage_status() {
    let root = fixture();
    let mut process = Server(
        command(&root).arg("--bogus").spawn().unwrap(),
        root.join("stderr"),
        None,
        None,
    );
    let status = timeout(BOUND, process.0.wait())
        .await
        .expect("invalid CLI argument hung")
        .unwrap();
    assert_eq!(status.code(), Some(2));
    assert_eq!(std::fs::read_dir(root.join("cwd")).unwrap().count(), 0);
    assert!(!root.join("data").exists());
}

// Capture finite command output in files so timeout/Drop retains ownership and
// neither output pipe can deadlock while the child exits.
async fn reset(root: &Path, owner: &str, port: u16) -> (std::process::ExitStatus, String, String) {
    let mut command = command(root);
    command
        .args([
            "--dev",
            "--reset-owner-password",
            "--owner",
            owner,
            "--database",
            "app",
            "--port",
            &port.to_string(),
        ])
        .arg("--data")
        .arg(root.join("data"))
        .env("GIGGLES_OWNER_PASSWORD", RESET_PASSWORD)
        .stdout(Stdio::from(
            std::fs::File::create(root.join("stdout")).unwrap(),
        ));
    command.stderr(Stdio::from(
        std::fs::File::create(root.join("reset-stderr")).unwrap(),
    ));
    let mut process = Server(
        command.spawn().unwrap(),
        root.join("reset-stderr"),
        None,
        None,
    );
    let status = timeout(BOUND, process.0.wait())
        .await
        .expect("offline reset exceeded 30 seconds")
        .unwrap();
    (
        status,
        std::fs::read_to_string(root.join("stdout")).unwrap(),
        std::fs::read_to_string(root.join("reset-stderr")).unwrap(),
    )
}

#[tokio::test]
async fn reset_rejects_missing_database_missing_owner_and_live_database() {
    let absent = fixture();
    let (status, stdout, _) = reset(&absent, "owner", 0).await;
    assert_eq!(status.code(), Some(1));
    assert!(stdout.is_empty());
    assert!(!absent.join("data").exists());

    let root = fixture();
    let (server, _) = Server::start(&root, true).await;
    let (status, stdout, stderr) = reset(&root, "owner", 0).await;
    assert_eq!(status.code(), Some(1));
    assert!(stdout.is_empty());
    assert!(stderr.contains("stop the running server first"), "{stderr}");
    server.shutdown().await;
    let (status, stdout, _) = reset(&root, "absent_owner", 0).await;
    assert_eq!(status.code(), Some(1));
    assert!(stdout.is_empty());
    let (server, url) = Server::start(&root, false).await;
    drop(connect(&url).await);
    server.shutdown().await;
}

#[tokio::test]
async fn trust_local_warns_and_accepts_passwordless_connection() {
    let root = fixture();
    let (server, url) = Server::start_mode(&root, true, true).await;
    drop(connect(&url.replace(":Dev-pass_1", "")).await);
    let stderr = server.shutdown().await;
    assert!(stderr.contains("WARNING: --trust-local"), "{stderr}");
}

#[tokio::test]
async fn existing_database_without_roles_prints_new_credentials_once() {
    let root = fixture();
    std::fs::create_dir_all(root.join("data")).unwrap();
    drop(
        giggles_core::Admin::create(root.join("data/db"), giggles_core::Limits::default()).unwrap(),
    );
    // first=true requires the URL to include the newly bootstrapped password,
    // even though the database directory existed before this process started.
    let (server, url) = Server::start(&root, true).await;
    drop(connect(&url).await);
    let stderr = server.shutdown().await;
    assert!(stderr.contains("shown once"), "{stderr}");
    assert!(!stderr.contains("password unchanged"), "{stderr}");
}

#[tokio::test]
async fn default_server_preserves_existing_legacy_engine_and_content() {
    use giggles_core::{Admin, Limits};
    let root = fixture();
    std::fs::create_dir_all(root.join("data")).unwrap();
    let path = root.join("data/db");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let metadata = admin.authority_metadata().unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE kept (id BIGINT PRIMARY KEY, body TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO kept VALUES (1, 'legacy')", &[], None)
        .unwrap();
    let before = session.execute("SELECT * FROM kept", &[], None).unwrap();
    drop(session);
    drop(admin);
    let (server, url) = Server::start(&root, true).await;
    let client = connect(&url).await;
    let rows = client.query("SELECT * FROM kept", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "legacy");
    drop(client);
    server.shutdown().await;
    let admin = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(admin.authority_metadata().unwrap(), metadata);
    assert!(admin.snapshot().is_ok());
    assert_eq!(
        admin
            .sql_session()
            .execute("SELECT * FROM kept", &[], None)
            .unwrap(),
        before
    );
    drop(admin);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn dev_role_apis_trust_crud_h2c_and_scram() {
    use giggles_server::api::{json::decode_execute, proto};
    timeout(BOUND * 4, async {
        let root = fixture();
        let (server, _) = Server::start_mode(&root, true, true).await;
        let addr = server.2.unwrap();
        let open = json!({"role":"owner","database":"app"});
        let (status, reply) = http(addr, "POST", "/v1/sessions", None, open.clone()).await;
        assert_eq!(status, 201, "{reply}");
        let token = reply["sessionId"].as_str().unwrap();
        for sql in [
            "CREATE TABLE api_items (id BIGINT PRIMARY KEY, body TEXT)",
            "INSERT INTO api_items VALUES (1, 'hello')",
            "SELECT * FROM api_items",
        ] {
            let (status, reply) = http(
                addr,
                "POST",
                "/v1/session/execute",
                Some(token),
                json!({"sql":{"sql":sql}}),
            )
            .await;
            assert_eq!(status, 200, "{reply}");
            assert_eq!(reply["code"], "ok", "{reply}");
            if sql.starts_with("SELECT") {
                assert_eq!(
                    reply["sql"]["rows"][0]["values"],
                    json!([{"intValue":"1"},{"textValue":"hello"}])
                );
            }
        }
        assert_eq!(
            http(addr, "DELETE", "/v1/session", Some(token), json!(null))
                .await
                .0,
            204
        );
        assert_eq!(
            http(
                addr,
                "POST",
                "/v1/sessions",
                None,
                json!({"role":"owner","database":"app","password":PASSWORD})
            )
            .await
            .0,
            401
        );
        let mut grpc =
            proto::giggles_client::GigglesClient::connect(format!("http://{}", server.3.unwrap()))
                .await
                .unwrap();
        let opened = grpc
            .open_session(proto::OpenSessionRequest {
                role: Some("owner".into()),
                database: Some("app".into()),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let mut request =
            tonic::Request::new(decode_execute(br#"{"sql":{"sql":"SELECT 1"}}"#).unwrap());
        request
            .metadata_mut()
            .insert("x-giggles-session", opened.session_id.parse().unwrap());
        let reply = grpc.execute(request).await.unwrap().into_inner();
        assert_eq!(reply.code, "ok");
        assert_eq!(reply.sql.unwrap().rows.len(), 1);
        drop(grpc);
        server.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();

        let root = fixture();
        let (server, url) = Server::start(&root, true).await;
        let addr = server.2.unwrap();
        assert_eq!(http(addr, "POST", "/v1/sessions", None, open).await.0, 401);
        let (status, reply) = http(
            addr,
            "POST",
            "/v1/sessions",
            None,
            json!({"role":"owner","database":"app","scram":{"clientFirst":FIRST}}),
        )
        .await;
        assert_eq!(status, 200);
        let challenge = &reply["scram"];
        // Read the credential printed by the actual child, rather than deriving a verifier.
        let password = url
            .strip_prefix("postgres://owner:")
            .unwrap()
            .split('@')
            .next()
            .unwrap();
        let (final_message, expected) = proof(password, challenge["serverFirst"].as_str().unwrap());
        let (status, reply) = http(
            addr,
            "POST",
            "/v1/sessions",
            None,
            json!({"scram":{"exchange":challenge["exchange"],"clientFinal":final_message}}),
        )
        .await;
        assert_eq!(status, 201, "{reply}");
        assert_eq!(reply["scramServerFinal"], expected);
        let stderr = server.shutdown().await;
        assert!(stderr.contains("sign in with SCRAM"));
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("dev API scenario exceeded bound");
}
