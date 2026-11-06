mod api_support;

use api_support::Fixture;
use bytes::Bytes;
use giggles_server::tls;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use serde_json::{Value, json};
use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
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
        let file = self.path.join(format!("{label}.TEST-ONLY.json"));
        std::fs::write(&file, serde_json::to_vec_pretty(&self.config).unwrap()).unwrap();
        Command::new(env!("CARGO_BIN_EXE_giggles-server"))
            .arg(file)
            .stdin(Stdio::piped())
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
