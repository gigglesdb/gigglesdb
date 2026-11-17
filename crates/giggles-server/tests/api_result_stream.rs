#![cfg(feature = "engine-rocksdb")]
//! Spilled API results retain the unary document and release temp files on every exit.
pub(crate) mod api_support;
use api_support::{Fixture, rpc, sql};
use bytes::Bytes;
use giggles_core::Engine;
use giggles_server::api::{json as wire_json, proto};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use rustls::pki_types::ServerName;
use serde_json::{Value, json};
use std::{collections::BTreeSet, io::Read, path::PathBuf, time::Duration};
use tokio::{net::TcpStream, task::JoinHandle};
use tokio_rustls::TlsConnector;

pub(crate) static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// Match a fixture-specific marker before truncating anything: other modules in
// the merged test binary may also have spills open in this process.
pub(crate) struct SpillProbe {
    pub(crate) marker: String,
    known: BTreeSet<PathBuf>,
}
impl SpillProbe {
    pub(crate) fn spill_files(&mut self) -> Vec<PathBuf> {
        let prefix = format!("giggles-returning-{}-", std::process::id());
        for entry in std::fs::read_dir(std::env::temp_dir()).unwrap().flatten() {
            if !entry.file_name().to_string_lossy().starts_with(&prefix) {
                continue;
            }
            let mut bytes = [0; 2048];
            if let Ok(mut file) = std::fs::File::open(entry.path())
                && let Ok(len) = file.read(&mut bytes)
                && bytes[..len]
                    .windows(self.marker.len())
                    .any(|w| w == self.marker.as_bytes())
            {
                self.known.insert(entry.path());
            }
        }
        self.known
            .iter()
            .filter(|path| path.exists())
            .cloned()
            .collect()
    }
    pub(crate) async fn cleaned(&mut self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !self.spill_files().is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("spill removed");
    }
}

pub(crate) async fn stop(f: Fixture) {
    let path = f.path.clone();
    f.stop().await;
    std::fs::remove_dir_all(path).unwrap();
}

pub(crate) async fn seed(f: &Fixture, count: usize, width: usize) -> SpillProbe {
    let marker = f.path.file_name().unwrap().to_string_lossy().into_owned();
    let payload = format!("{marker}{}", "x".repeat(width - marker.len()));
    let admin = f.admin.clone();
    tokio::task::spawn_blocking(move || {
        use giggles_core::relational::Value as V;
        let mut session = admin.sql_session();
        let insert = session
            .prepare("INSERT INTO app(id,body) VALUES($1,$2)")
            .unwrap();
        for start in (1..=count).step_by(100) {
            session.execute("BEGIN", &[], None).unwrap();
            for id in start..=(start + 99).min(count) {
                session
                    .execute_prepared(
                        &insert,
                        &[V::Int(id as i64), V::Text(payload.clone())],
                        None,
                    )
                    .unwrap();
            }
            session.execute("COMMIT", &[], None).unwrap();
        }
    })
    .await
    .unwrap();
    SpillProbe {
        marker,
        known: BTreeSet::new(),
    }
}

pub(crate) async fn http_stream(
    f: &Fixture,
    token: &str,
    request: Value,
    h2: bool,
) -> (hyper::body::Incoming, JoinHandle<()>) {
    let socket = TcpStream::connect(f.http.address).await.unwrap();
    socket.set_nodelay(true).unwrap();
    let tls = TlsConnector::from(f.client_config(&f.app, h2))
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap();
    let request = hyper::Request::builder()
        .method("POST")
        .uri(format!(
            "https://localhost:{}/v1/session/execute",
            f.http.address.port()
        ))
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("x-giggles-session", token)
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&request).unwrap(),
        )))
        .unwrap();
    let (response, task) = if h2 {
        let (mut sender, connection) =
            hyper::client::conn::http2::Builder::new(TokioExecutor::new())
                .initial_stream_window_size(64 * 1024)
                .handshake(TokioIo::new(tls))
                .await
                .unwrap();
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        (sender.send_request(request).await.unwrap(), task)
    } else {
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
            .await
            .unwrap();
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        (sender.send_request(request).await.unwrap(), task)
    };
    assert_eq!(response.status(), 200);
    (response.into_body(), task)
}

pub(crate) fn typed_read() -> Value {
    json!({"typedRead":{"table":"app","projection":[
        {"expression":{"column":{"name":"id"}}},
        {"expression":{"column":{"name":"body"}}}],
        "orderBy":[{"expression":{"column":{"name":"id"}},"descending":false}]}})
}

pub(crate) fn check_rows(rows: &[Value], count: usize, width: usize, marker: &str) {
    let expected = format!("{marker}{}", "x".repeat(width - marker.len()));
    assert_eq!(rows.len(), count);
    assert_eq!(rows[0]["values"][0]["intValue"], "1");
    assert_eq!(rows[count - 1]["values"][0]["intValue"], count.to_string());
    assert_eq!(rows[0]["values"][1]["textValue"], expected);
    assert_eq!(rows[count - 1]["values"][1]["textValue"], expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_and_native_stream_every_row_with_metadata_and_cleanup() {
    let _serial = SERIAL.lock().await;
    let f = Fixture::catalog_empty_engine(Engine::V2).await;
    let mut probe = seed(&f, 6000, 1000).await;
    let token = f.open_http(&f.app, true).await;
    let mut client = f.grpc_client(&f.app).await;
    let grpc_token = client
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;
    let query = "SELECT id,body FROM app ORDER BY id";
    let unary = client
        .execute(rpc(&grpc_token, sql(query)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(unary.code, "54000");
    for (request, field) in [
        (json!({"sql":{"sql":query}}), "sql"),
        (typed_read(), "operationOutcome"),
    ] {
        let (mut body, task) = http_stream(&f, &token, request.clone(), true).await;
        let mut bytes = body
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .to_vec();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!probe.spill_files().is_empty(), "live HTTP spill");
        let started = std::time::Instant::now();
        let mut throttle_bytes = 0;
        while let Some(frame) = body.frame().await {
            let data = frame.unwrap().into_data().unwrap();
            throttle_bytes += data.len();
            bytes.extend_from_slice(&data);
            drop(data);
            // Throttle bytes, since HTTP/2 may deliver very small DATA frames.
            if field == "sql" && throttle_bytes >= 16 * 1024 {
                tokio::time::sleep(Duration::from_millis(30)).await;
                throttle_bytes %= 16 * 1024;
            }
        }
        if field == "sql" {
            assert!(started.elapsed() > Duration::from_secs(10));
        }
        assert!(bytes.len() > 5 * 1024 * 1024);
        let reply: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reply["code"], "ok");
        check_rows(
            reply[field]["rows"].as_array().unwrap(),
            6000,
            1000,
            &probe.marker,
        );
        if field == "operationOutcome" {
            assert_eq!(reply[field]["matched"], "6000");
        }
        let small_request = if field == "sql" {
            json!({"sql":{"sql":"SELECT id,body FROM app ORDER BY id LIMIT 1"}})
        } else {
            let mut r = typed_read();
            r["typedRead"]["limit"] = json!("1");
            r
        };
        let small = f.exec_http(&f.app, &token, small_request, true).await;
        assert_eq!(
            reply.as_object().unwrap().keys().collect::<Vec<_>>(),
            small.as_object().unwrap().keys().collect::<Vec<_>>()
        );
        assert_eq!(
            reply[field].as_object().unwrap().keys().collect::<Vec<_>>(),
            small[field].as_object().unwrap().keys().collect::<Vec<_>>()
        );
        task.abort();
        probe.cleaned().await;

        let wire = wire_json::decode_execute(&serde_json::to_vec(&request).unwrap()).unwrap();
        let mut stream = client
            .execute_stream(rpc(&grpc_token, wire))
            .await
            .unwrap()
            .into_inner();
        let head = stream.message().await.unwrap().unwrap();
        assert_eq!(head.code, "ok");
        assert!(!probe.spill_files().is_empty(), "live gRPC spill");
        assert_eq!(head.transaction_state, proto::TransactionState::Idle as i32);
        let mut expected_head = reply.clone();
        expected_head[field]["rows"] = json!([]);
        assert_eq!(
            serde_json::from_slice::<Value>(&wire_json::encode_reply(&head).unwrap()).unwrap(),
            expected_head
        );
        let mut all_rows = vec![];
        while let Some(message) = stream.message().await.unwrap() {
            assert!(message.encoded_len() <= 64 * 1024);
            assert_eq!(message.code, "");
            assert_eq!(message.version, 0);
            assert_eq!(message.transaction_state, 0);
            let rows = if field == "sql" {
                message.sql.unwrap().rows
            } else {
                let Some(proto::execute_reply::NativeResult::OperationOutcome(outcome)) =
                    message.native_result
                else {
                    panic!("native continuation")
                };
                assert_eq!(outcome.matched, 0);
                outcome.rows
            };
            all_rows.extend(rows);
        }
        assert_eq!(all_rows.len(), 6000);
        assert_eq!(
            all_rows.first().unwrap().values[0].value,
            Some(proto::value::Value::IntValue(1))
        );
        assert_eq!(
            all_rows.last().unwrap().values[0].value,
            Some(proto::value::Value::IntValue(6000))
        );
        probe.cleaned().await;
    }
    stop(f).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spill_read_failure_reaches_http_parser_and_grpc_status() {
    let _serial = SERIAL.lock().await;
    let f = Fixture::catalog_empty_engine(Engine::V2).await;
    let mut probe = seed(&f, 2000, 24000).await;
    let token = f.open_http(&f.app, false).await;
    let query = "SELECT id,body FROM app ORDER BY id";
    for h2 in [false, true] {
        let (mut body, task) = http_stream(&f, &token, json!({"sql":{"sql":query}}), h2).await;
        let mut bytes = body
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .to_vec();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let files = probe.spill_files();
        assert_eq!(files.len(), 1);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&files[0])
            .unwrap()
            .set_len(0)
            .unwrap();
        let mut failed = false;
        while let Some(frame) = body.frame().await {
            match frame {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        bytes.extend_from_slice(&data);
                    }
                }
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        assert!(failed, "truncated spill must fail buffered HTTP readers");
        let text = String::from_utf8(bytes).unwrap();
        assert!(
            text.contains("\"streamError\""),
            "missing stream error terminator"
        );
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["sql"]["streamError"]["code"], "58030");
        task.abort();
        probe.cleaned().await;
    }
    let mut client = f.grpc_client(&f.app).await;
    let token = client
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;
    let mut stream = client
        .execute_stream(rpc(&token, sql(query)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stream.message().await.unwrap().unwrap().code, "ok");
    let files = probe.spill_files();
    assert_eq!(files.len(), 1);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&files[0])
        .unwrap()
        .set_len(0)
        .unwrap();
    let error = loop {
        match stream.message().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("truncated spill succeeded"),
            Err(error) => break error,
        }
    };
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(error.message().contains("58030"));
    probe.cleaned().await;

    // Dropping a backpressured receiver must also stop its blocking producer.
    let http_token = f.open_http(&f.app, true).await;
    let (mut body, task) = http_stream(&f, &http_token, json!({"sql":{"sql":query}}), true).await;
    body.frame().await.unwrap().unwrap();
    assert!(!probe.spill_files().is_empty());
    drop(body);
    task.abort();
    probe.cleaned().await;
    let mut stream = client
        .execute_stream(rpc(&token, sql(query)))
        .await
        .unwrap()
        .into_inner();
    stream.message().await.unwrap().unwrap();
    assert!(!probe.spill_files().is_empty());
    drop(stream);
    probe.cleaned().await;
    stop(f).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_v2_and_legacy_replies_are_byte_identical_and_stream_once() {
    let _serial = SERIAL.lock().await;
    for engine in [Engine::V2, Engine::Legacy] {
        let f = Fixture::catalog_empty_engine(engine).await;
        let mut probe = seed(&f, 3, 128).await;
        let mut client = f.grpc_client(&f.app).await;
        let token = client
            .open_session(proto::OpenSessionRequest::default())
            .await
            .unwrap()
            .into_inner()
            .session_id;
        let query = "SELECT id,body FROM app ORDER BY id";
        let unary = client
            .execute(rpc(&token, sql(query)))
            .await
            .unwrap()
            .into_inner();
        let http_token = f.open_http(&f.app, false).await;
        let (status, bytes) = f
            .http(
                &f.app,
                "POST",
                "/v1/session/execute",
                Some(&http_token),
                serde_json::to_vec(&json!({"sql":{"sql":query}})).unwrap(),
                false,
            )
            .await
            .unwrap();
        assert_eq!(status, 200);
        assert_eq!(bytes, wire_json::encode_reply(&unary).unwrap());
        let mut stream = client
            .execute_stream(rpc(&token, sql(query)))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(stream.message().await.unwrap().unwrap(), unary);
        assert!(stream.message().await.unwrap().is_none());
        probe.cleaned().await;
        stop(f).await;
    }
}
