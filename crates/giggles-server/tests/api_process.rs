mod api_support;

use api_support::Fixture;
use bytes::Bytes;
use giggles_server::{
    api::proto::{ExecuteRequest, OpenSessionRequest, PingRequest, execute_request},
    tls,
    wire::{
        client::{Client, ClientTls, Credential},
        frame::FrameKind,
    },
};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use serde_json::{Value, json};
use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
};
use tokio_rustls::TlsConnector;

struct Case {
    path: PathBuf,
    config: Value,
}
impl Case {
    async fn new() -> Self {
        let fixture = Fixture::new().await;
        let path = std::fs::canonicalize(&fixture.path).unwrap();
        let bindings = serde_json::to_value(&fixture.bindings).unwrap();
        fixture.stop().await;
        let config = json!({
            "database":path.join("db.redb"),"native":"127.0.0.1:0","postgres":"127.0.0.1:0",
            "certificate":path.join("server.pem"),"private_key":path.join("server.key.TEST-ONLY"),
            "client_ca":path.join("ca.pem"),"bindings":bindings,"connections":32,
            "skew_seconds":0,"max_pause_seconds":3600
        });
        Self { path, config }
    }
    fn spawn(&self, label: &str) -> Child {
        self.spawn_control(label, true)
    }
    fn spawn_control(&self, label: &str, stdin_control: bool) -> Child {
        let file = self.path.join(format!("{label}.TEST-ONLY.json"));
        std::fs::write(&file, serde_json::to_vec_pretty(&self.config).unwrap()).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_giggles-server"));
        if !stdin_control {
            command.arg("--no-stdin-control");
        }
        command
            .arg(file)
            .stdin(if stdin_control {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(self.path.join(format!("{label}.stderr.log"))).unwrap())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    }
    async fn start(&self, label: &str) -> Process {
        let mut child = self.spawn(label);
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(20), output.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        std::fs::write(self.path.join(format!("{label}.ready.log")), &line).unwrap();
        assert!(
            line.contains("preview=single-host"),
            "missing readiness: {line}"
        );
        let addresses = line
            .split_whitespace()
            .filter_map(|field| {
                let (name, value) = field.split_once('=')?;
                value.parse().ok().map(|address| (name.to_owned(), address))
            })
            .collect();
        Process {
            child,
            output,
            addresses,
        }
    }
    async fn http(
        &self,
        process: &Process,
        method: &str,
        route: &str,
        token: Option<&str>,
        body: Value,
    ) -> (u16, Value) {
        let cfg = tls::client(
            tls::certs(self.path.join("app.pem")).unwrap(),
            tls::key(self.path.join("app.key.TEST-ONLY")).unwrap(),
            tls::certs(self.path.join("ca.pem")).unwrap(),
        )
        .unwrap();
        let socket = TcpStream::connect(process.addresses["http"]).await.unwrap();
        let stream = TlsConnector::from(cfg)
            .connect(ServerName::try_from("localhost").unwrap(), socket)
            .await
            .unwrap();
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let task = tokio::spawn(connection);
        let mut request = hyper::Request::builder()
            .method(method)
            .uri(route)
            .header("host", "localhost")
            .header("content-type", "application/json");
        if let Some(token) = token {
            request = request.header("x-giggles-session", token);
        }
        let body = if method == "DELETE" {
            vec![]
        } else {
            serde_json::to_vec(&body).unwrap()
        };
        let response = tokio::time::timeout(
            Duration::from_secs(15),
            sender.send_request(request.body(Full::new(Bytes::from(body))).unwrap()),
        )
        .await
        .unwrap()
        .unwrap();
        let status = response.status().as_u16();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        task.abort();
        let _ = task.await;
        (
            status,
            if bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&bytes).unwrap()
            },
        )
    }
    async fn open(&self, p: &Process) -> String {
        let (status, body) = self.http(p, "POST", "/v1/sessions", None, json!({})).await;
        assert_eq!(status, 201, "{body}");
        body["sessionId"].as_str().unwrap().into()
    }
    async fn sql(&self, p: &Process, token: &str, sql: &str) -> Value {
        let (status, body) = self
            .http(
                p,
                "POST",
                "/v1/session/execute",
                Some(token),
                json!({"sql":{"sql":sql}}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        body
    }
}
struct Process {
    child: Child,
    output: BufReader<tokio::process::ChildStdout>,
    addresses: BTreeMap<String, SocketAddr>,
}
impl Process {
    async fn control(&mut self, command: &str) -> String {
        self.child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(format!("{command}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(15), self.output.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        line
    }
    async fn stop(mut self) {
        self.child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"shutdown\n")
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(20), self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
}

#[tokio::test]
async fn legacy_config_and_each_optional_listener_start_and_shutdown() {
    let mut c = Case::new().await;
    for (label, http, grpc) in [
        ("legacy", false, false),
        ("http", true, false),
        ("grpc", false, true),
        ("both", true, true),
    ] {
        c.config.as_object_mut().unwrap().remove("http");
        c.config.as_object_mut().unwrap().remove("grpc");
        if http {
            c.config["http"] = json!("127.0.0.1:0");
        }
        if grpc {
            c.config["grpc"] = json!("127.0.0.1:0");
        }
        let p = c.start(label).await;
        assert!(p.addresses.contains_key("native") && p.addresses.contains_key("postgres"));
        assert_eq!(p.addresses.contains_key("http"), http);
        assert_eq!(p.addresses.contains_key("grpc"), grpc);
        if http {
            let token = c.open(&p).await;
            assert_eq!(c.sql(&p, &token, "BEGIN").await["code"], "ok");
        }
        let addresses = p.addresses.clone();
        p.stop().await;
        for address in addresses.values() {
            assert!(TcpStream::connect(address).await.is_err());
        }
    }
}

#[tokio::test]
async fn bin_config_native_alias_starts_wire_listener() {
    let case = Case::new().await;
    let process = case.start("native-alias").await;
    let address = process.addresses["native"];
    assert!(!process.addresses.contains_key("wire"));

    let ca = case.path.join("ca.pem");
    let cert = case.path.join("app.pem");
    let key = case.path.join("app.key.TEST-ONLY");
    let (mut client, _) = Client::connect(
        address,
        Some(ClientTls {
            ca: std::fs::read(&ca).unwrap(),
            cert: Some(std::fs::read(&cert).unwrap()),
            key: Some(std::fs::read(&key).unwrap()),
            server_name: "localhost".into(),
        }),
        OpenSessionRequest::default(),
        Credential::None,
    )
    .await
    .unwrap();
    let reply = client
        .execute(ExecuteRequest {
            request: Some(execute_request::Request::Ping(PingRequest {})),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(reply.code, "ok");
    client.goodbye().await.unwrap();

    let cfg = tls::client(
        tls::certs(&cert).unwrap(),
        tls::key(&key).unwrap(),
        tls::certs(&ca).unwrap(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let socket = TcpStream::connect(address).await.unwrap();
        let mut stream = TlsConnector::from(cfg)
            .connect(ServerName::try_from("localhost").unwrap(), socket)
            .await
            .unwrap();
        let body = br#"{"kind":"Ping"}"#;
        stream
            .write_u32(body.len().try_into().unwrap())
            .await
            .unwrap();
        stream.write_all(body).await.unwrap();
        stream.flush().await.unwrap();
        // The wire error's header starts with the old frame's length field.
        // Its next byte is the wire error kind, where JSON would start with '{'.
        let length = stream.read_u32().await.unwrap();
        assert!((8..=1024).contains(&length));
        let mut frame = vec![0; length as usize];
        stream.read_exact(&mut frame).await.unwrap();
        assert_eq!(frame[0], FrameKind::ConnError as u8);
        match stream.read_u8().await {
            Err(error) => assert!(matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            )),
            Ok(byte) => panic!("legacy probe received more data: {byte}"),
        }
    })
    .await
    .expect("legacy probe must be rejected and disconnected");

    process.stop().await;
    let stderr = std::fs::read_to_string(case.path.join("native-alias.stderr.log")).unwrap();
    let warning = r#"warning: config key "native" is deprecated; use "wire""#;
    assert_eq!(
        stderr.lines().filter(|line| *line == warning).count(),
        1,
        "{stderr}"
    );
}

#[tokio::test]
async fn bin_config_native_and_wire_conflict() {
    let mut case = Case::new().await;
    let native_socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wire_socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let native = native_socket.local_addr().unwrap();
    let wire = wire_socket.local_addr().unwrap();
    drop((native_socket, wire_socket));
    case.config["native"] = json!(native);
    case.config["wire"] = json!(wire);

    let output = tokio::time::timeout(
        Duration::from_secs(20),
        case.spawn("native-wire-conflict").wait_with_output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr =
        std::fs::read_to_string(case.path.join("native-wire-conflict.stderr.log")).unwrap();
    assert!(
        stderr.contains("native is a deprecated alias for wire; set only wire"),
        "{stderr}"
    );
    assert!(TcpStream::connect(native).await.is_err());
    assert!(TcpStream::connect(wire).await.is_err());
}

#[tokio::test]
async fn later_bind_failure_and_invalid_config_never_report_ready() {
    let mut c = Case::new().await;
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reserved = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let native = reserved.local_addr().unwrap();
    drop(reserved);
    c.config["native"] = json!(native.to_string());
    c.config["http"] = json!(occupied.local_addr().unwrap().to_string());
    let output = tokio::time::timeout(
        Duration::from_secs(20),
        c.spawn("bind-failure").wait_with_output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(TcpStream::connect(native).await.is_err());
    c.config["http"] = json!("127.0.0.1:0");
    c.config["api_limits"] = json!({"sessions":0});
    let output = tokio::time::timeout(
        Duration::from_secs(20),
        c.spawn("invalid-limits").wait_with_output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(TcpStream::connect(native).await.is_err());
}

#[tokio::test]
async fn binary_reload_shutdown_and_restart_fence_handles_and_staged_rows() {
    let mut c = Case::new().await;
    c.config["http"] = json!("127.0.0.1:0");
    c.config["grpc"] = json!("127.0.0.1:0");
    let bindings_file = c.path.join("bindings.TEST-ONLY.json");
    std::fs::write(
        &bindings_file,
        serde_json::to_vec(&c.config["bindings"]).unwrap(),
    )
    .unwrap();
    c.config["bindings_file"] = json!(bindings_file);
    let mut p = c.start("before-restart").await;
    let token = c.open(&p).await;
    assert_eq!(
        c.sql(
            &p,
            &token,
            "INSERT INTO app (id,body) VALUES (1,'committed')"
        )
        .await["code"],
        "ok"
    );
    assert_eq!(c.sql(&p, &token, "BEGIN").await["code"], "ok");
    assert_eq!(
        c.sql(&p, &token, "INSERT INTO app (id) VALUES (2)").await["code"],
        "ok"
    );
    std::fs::write(&bindings_file, b"[]").unwrap();
    assert!(p.control("reload").await.contains("bindings reloaded"));
    std::fs::write(
        &bindings_file,
        serde_json::to_vec(&c.config["bindings"]).unwrap(),
    )
    .unwrap();
    assert!(p.control("reload").await.contains("bindings reloaded"));
    assert_eq!(
        c.http(
            &p,
            "POST",
            "/v1/session/execute",
            Some(&token),
            json!({"ping":{}})
        )
        .await
        .0,
        404
    );
    let token = c.open(&p).await;
    let rows = c
        .sql(&p, &token, "SELECT id FROM app ORDER BY id LIMIT 10")
        .await;
    assert_eq!(rows["sql"]["rows"].as_array().unwrap().len(), 1);
    assert_eq!(c.sql(&p, &token, "BEGIN").await["code"], "ok");
    assert_eq!(
        c.sql(&p, &token, "INSERT INTO app (id) VALUES (3)").await["code"],
        "ok"
    );
    p.stop().await;
    let p = c.start("after-restart").await;
    assert_eq!(
        c.http(
            &p,
            "POST",
            "/v1/session/execute",
            Some(&token),
            json!({"ping":{}})
        )
        .await
        .0,
        404
    );
    let token = c.open(&p).await;
    let rows = c
        .sql(&p, &token, "SELECT id FROM app ORDER BY id LIMIT 10")
        .await;
    assert_eq!(rows["sql"]["rows"].as_array().unwrap().len(), 1);
    assert_eq!(rows["sql"]["rows"][0]["values"][0]["intValue"], "1");
    p.stop().await;
}

#[tokio::test]
async fn no_stdin_control_accepts_connections_with_closed_stdin() {
    let case = Case::new().await;
    let mut child = case.spawn_control("no-stdin", false);
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(20), output.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    assert!(line.contains("preview=single-host"), "{line}");
    let address = line
        .split_whitespace()
        .find_map(|field| field.strip_prefix("native="))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(child.try_wait().unwrap().is_none());
    let connection = TcpStream::connect(address).await.unwrap();
    drop(connection);
    #[cfg(unix)]
    {
        assert!(
            std::process::Command::new("kill")
                .args(["-TERM", &child.id().unwrap().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success(), "{status}");
    }
    // Windows has no SIGTERM equivalent; exercise closed stdin and connectivity.
    #[cfg(not(unix))]
    child.kill().await.unwrap();
}
