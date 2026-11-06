mod api_support;
use api_support::{Fixture, TestCert};
use bytes::Bytes;
use giggles_server::api::ApiLimits;
use http_body::{Body, Frame};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::ServerName;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{TlsConnector, client::TlsStream};

async fn tls(f: &Fixture, grpc: bool, h2: bool) -> TlsStream<TcpStream> {
    tls_as(f, grpc, h2, &f.app).await
}

async fn tls_as(f: &Fixture, grpc: bool, h2: bool, certificate: &TestCert) -> TlsStream<TcpStream> {
    let address = if grpc { f.grpc.address } else { f.http.address };
    let socket = TcpStream::connect(address).await.unwrap();
    TlsConnector::from(f.client_config(certificate, h2))
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap()
}

async fn frame(
    stream: &mut TlsStream<TcpStream>,
    kind: u8,
    flags: u8,
    id: u32,
    payload: &[u8],
) -> io::Result<()> {
    let length = (payload.len() as u32).to_be_bytes();
    let mut header = Vec::from(&length[1..]);
    header.extend_from_slice(&[kind, flags]);
    header.extend_from_slice(&id.to_be_bytes());
    stream.write_all(&header).await?;
    stream.write_all(payload).await?;
    stream.flush().await
}

async fn read_frame(stream: &mut TlsStream<TcpStream>) -> io::Result<(u8, u8, u32, Vec<u8>)> {
    let mut header = [0; 9];
    stream.read_exact(&mut header).await?;
    let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
    assert!(length <= 16 * 1024, "server frame bound");
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).await?;
    Ok((
        header[3],
        header[4],
        u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fffffff,
        payload,
    ))
}

async fn raw_h2(f: &Fixture, initial_window: u32) -> TlsStream<TcpStream> {
    raw_h2_as(f, initial_window, &f.app).await
}

async fn raw_h2_as(
    f: &Fixture,
    initial_window: u32,
    certificate: &TestCert,
) -> TlsStream<TcpStream> {
    let mut stream = tls_as(f, false, true, certificate).await;
    stream
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    let mut settings = vec![0, 4];
    settings.extend_from_slice(&initial_window.to_be_bytes());
    frame(&mut stream, 4, 0, 0, &settings).await.unwrap();
    loop {
        let (kind, flags, _, payload) = read_frame(&mut stream).await.unwrap();
        if kind == 4 && flags == 0 {
            let settings: Vec<_> = payload
                .as_chunks::<6>()
                .0
                .iter()
                .map(|value| {
                    (
                        u16::from_be_bytes(value[..2].try_into().unwrap()),
                        u32::from_be_bytes(value[2..].try_into().unwrap()),
                    )
                })
                .collect();
            assert!(
                settings.contains(&(3, 16)),
                "advertised 16 streams: {settings:?}"
            );
            assert!(
                settings.contains(&(6, 16 * 1024)),
                "advertised header bound: {settings:?}"
            );
            assert_eq!(
                settings
                    .iter()
                    .find(|(key, _)| *key == 1)
                    .map_or(4096, |(_, n)| *n),
                4096
            );
            frame(&mut stream, 4, 1, 0, &[]).await.unwrap();
            return stream;
        }
    }
}

// Only tests hand-write malformed protocol traffic. Production uses Hyper/tonic.
fn integer(bytes: &mut Vec<u8>, prefix: u8, bits: u8, value: usize) {
    let maximum = (1usize << bits) - 1;
    if value < maximum {
        bytes.push(prefix | value as u8);
        return;
    }
    bytes.push(prefix | maximum as u8);
    let mut value = value - maximum;
    while value >= 128 {
        bytes.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    bytes.push(value as u8);
}

fn literal(bytes: &mut Vec<u8>, index: usize, name: &str, value: &str) {
    integer(bytes, 0, 4, index);
    if index == 0 {
        integer(bytes, 0, 7, name.len());
        bytes.extend_from_slice(name.as_bytes());
    }
    integer(bytes, 0, 7, value.len());
    bytes.extend_from_slice(value.as_bytes());
}

fn headers(path: &str, token: Option<&str>) -> Vec<u8> {
    let mut bytes = vec![0x83, 0x87]; // POST, https
    literal(&mut bytes, 4, "", path);
    literal(&mut bytes, 1, "", "localhost");
    literal(&mut bytes, 31, "", "application/json");
    if let Some(token) = token {
        literal(&mut bytes, 0, "x-giggles-session", token);
    }
    bytes
}

async fn send_json(stream: &mut TlsStream<TcpStream>, id: u32, token: &str, json: &[u8]) {
    frame(
        stream,
        1,
        4,
        id,
        &headers("/v1/session/execute", Some(token)),
    )
    .await
    .unwrap();
    frame(stream, 0, 1, id, json).await.unwrap();
}

async fn expect_closed(stream: &mut TlsStream<TcpStream>) {
    tokio::time::timeout(Duration::from_secs(13), async {
        loop {
            match read_frame(stream).await {
                Err(_) => break,
                Ok((7, _, _, _)) => break,
                Ok(_) => {}
            }
        }
    })
    .await
    .expect("bounded connection completion");
}

#[tokio::test]
async fn h2_settings_stream_limit_hpack_and_continuation_bounds() {
    let f = Fixture::new().await;
    let mut stream = raw_h2(&f, 65535).await;
    for id in (1..=33).step_by(2) {
        frame(&mut stream, 1, 4, id, &headers("/v1/sessions", None))
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (kind, _, id, payload) = read_frame(&mut stream).await.unwrap();
            if kind == 3 && id == 33 {
                assert_eq!(u32::from_be_bytes(payload.try_into().unwrap()), 7);
                break;
            }
            assert_ne!(kind, 7, "stream overflow should be bounded by refusal");
        }
    })
    .await
    .expect("17th stream refused");
    drop(stream);

    let mut stream = raw_h2(&f, 65535).await;
    let mut block = Vec::new();
    integer(&mut block, 0x20, 5, 4097); // forbidden HPACK table growth
    block.extend(headers("/v1/sessions", None));
    frame(&mut stream, 1, 5, 1, &block).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), expect_closed(&mut stream))
        .await
        .unwrap();
    drop(stream);

    let mut stream = raw_h2(&f, 65535).await;
    frame(&mut stream, 1, 0, 1, &headers("/v1/sessions", None))
        .await
        .unwrap();
    for _ in 0..7 {
        if frame(&mut stream, 9, 0, 1, &[0]).await.is_err() {
            break;
        }
    }
    tokio::time::timeout(Duration::from_secs(3), expect_closed(&mut stream))
        .await
        .unwrap();
    drop(stream);
    let mut stream = raw_h2(&f, 65535).await;
    frame(&mut stream, 1, 0, 1, &headers("/v1/sessions", None))
        .await
        .unwrap();
    // A stalled header block cannot admit other application or PING traffic.
    frame(&mut stream, 6, 0, 0, &[0; 8]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), expect_closed(&mut stream))
        .await
        .unwrap();
    drop(stream);
    f.stop().await;
}

#[tokio::test]
async fn h2_ping_only_does_not_extend_application_idle() {
    let f = Fixture::new().await;
    let stream = raw_h2(&f, 65535).await;
    let (mut reader, mut writer) = tokio::io::split(stream);
    let started = Instant::now();
    let ping = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if writer
                .write_all(&[0, 0, 8, 6, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8])
                .await
                .is_err()
            {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
    });
    let mut received = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(13), reader.read_to_end(&mut received))
        .await
        .expect("PING-only deadline");
    assert!(started.elapsed() >= Duration::from_secs(9));
    assert!(
        received
            .windows(8)
            .any(|bytes| bytes == [1, 2, 3, 4, 5, 6, 7, 8]),
        "PING was processed"
    );
    ping.abort();
    let _ = ping.await;
    f.stop().await;
}

#[tokio::test]
async fn http_slow_body_chunk_limit_and_framing_do_not_open_sessions() {
    let f = Fixture::with_limits(ApiLimits { sessions: 1 }).await;
    let mut stream = tls(&f, false, false).await;
    stream.write_all(b"POST /v1/sessions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{").await.unwrap();
    stream.flush().await.unwrap();
    let started = Instant::now();
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(13), stream.read_to_end(&mut response))
        .await
        .expect("slow body deadline");
    assert!(started.elapsed() >= Duration::from_secs(9));
    drop(stream);
    let stream = tls(&f, false, false).await;
    let chunk_started = Instant::now();
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut response = Vec::new();
    let (sent, read) = tokio::time::timeout(Duration::from_secs(13), async {
        tokio::join!(async {
            writer.write_all(b"POST /v1/sessions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n40001\r\n").await?;
            writer.write_all(&vec![b' '; giggles_server::MAX_FRAME + 1]).await?;
            writer.write_all(b"\r\n0\r\n\r\n").await?;
            writer.flush().await
        }, reader.read_to_end(&mut response))
    }).await.expect("bounded duplex oversized request");
    assert!(
        String::from_utf8_lossy(&response).starts_with("HTTP/1.1 413"),
        "chunked response after {:?}: {:?}; sent={sent:?}; read={read:?}",
        chunk_started.elapsed(),
        String::from_utf8_lossy(&response)
    );
    drop(reader);
    drop(writer);
    let mut stream = tls(&f, false, false).await;
    stream.write_all(b"POST /v1/sessions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n{}POST /v1/sessions HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\nContent-Type: application/json\r\n\r\n{}").await.unwrap();
    let _ = stream.flush().await;
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut response))
        .await
        .unwrap();
    assert!(!String::from_utf8_lossy(&response).contains("201 Created"));
    drop(stream);
    let token = f.open_http(&f.app, false).await;
    assert_eq!(token.len(), 64);
    f.stop().await;
}

#[tokio::test]
async fn h2_output_backpressure_holds_session_until_timeout() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    let mut stream = raw_h2(&f, 1).await;
    send_json(&mut stream, 1, &token, br#"{"sql":{"sql":"BEGIN"}}"#).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (kind, _, id, bytes) = read_frame(&mut stream).await.unwrap();
            if kind == 0 && id == 1 {
                assert_eq!(bytes.len(), 1);
                break;
            }
        }
    })
    .await
    .unwrap();
    let (status, _) = f
        .http(
            &f.app,
            "POST",
            "/v1/session/execute",
            Some(&token),
            br#"{"ping":{}}"#.to_vec(),
            true,
        )
        .await
        .unwrap();
    assert_eq!(status, 409, "pending bytes own the busy lease");
    expect_closed(&mut stream).await;
    drop(stream);
    let result = f
        .exec_http(&f.app, &token, serde_json::json!({"ping":{}}), false)
        .await;
    assert_eq!(
        result["transactionState"],
        "TRANSACTION_STATE_IN_TRANSACTION"
    );
    f.stop().await;
}

#[tokio::test]
async fn h2_reset_after_commit_preserves_durable_rows_and_session() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    f.exec_http(
        &f.app,
        &token,
        serde_json::json!({"sql":{"sql":"BEGIN"}}),
        false,
    )
    .await;
    f.exec_http(
        &f.app,
        &token,
        serde_json::json!({"sql":{"sql":"INSERT INTO app (id) VALUES (71)"}}),
        false,
    )
    .await;
    let mut stream = raw_h2(&f, 1).await;
    send_json(&mut stream, 1, &token, br#"{"sql":{"sql":"COMMIT"}}"#).await;
    loop {
        let (kind, _, id, _) = read_frame(&mut stream).await.unwrap();
        if kind == 0 && id == 1 {
            break;
        }
    }
    assert_eq!(
        f.admin
            .sql_session()
            .execute("SELECT id FROM app WHERE id=71", &[], None)
            .unwrap()
            .rows
            .len(),
        1
    );
    frame(&mut stream, 3, 0, 1, &8u32.to_be_bytes())
        .await
        .unwrap();
    // A following request is ordered after the RST frame on this connection.
    frame(&mut stream, 6, 0, 0, &[9; 8]).await.unwrap();
    loop {
        let (kind, flags, _, _) = read_frame(&mut stream).await.unwrap();
        if kind == 6 && flags == 1 {
            break;
        }
    }
    drop(stream);
    let result = f
        .exec_http(&f.app, &token, serde_json::json!({"ping":{}}), false)
        .await;
    assert_eq!(result["transactionState"], "TRANSACTION_STATE_IDLE");
    f.stop().await;
}

#[tokio::test]
async fn lost_task_ack_reconciles_once_and_expired_retry_cannot_undo_commit() {
    use giggles_core::{Key, OperationId, RowId, Status, TaskId, TenantId};
    let f = Fixture::new().await;
    let token = f.open_http(&f.task, false).await;
    let key = Key {
        tenant: TenantId(42),
        row: RowId(1),
    };
    let before = f.admin.snapshot().unwrap().rows[&key].clone();
    let request = serde_json::json!({"execute":{"request":{"operation":"713","selection":{"keys":{"items":[{"tenant":"42","row":"1"}]}},"column":"COLUMN_STATUS","after":"DELIVERY_STATUS_QUEUED"}}});
    let mut stream = raw_h2_as(&f, 1, &f.task).await;
    send_json(
        &mut stream,
        1,
        &token,
        &serde_json::to_vec(&request).unwrap(),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (kind, _, id, _) = read_frame(&mut stream).await.unwrap();
            if kind == 0 && id == 1 {
                break;
            }
        }
    })
    .await
    .unwrap();
    // Discard the reply before any complete JSON acknowledgement is available.
    frame(&mut stream, 3, 0, 1, &8u32.to_be_bytes())
        .await
        .unwrap();
    frame(&mut stream, 6, 0, 0, &[7; 8]).await.unwrap();
    loop {
        let (kind, flags, _, _) = read_frame(&mut stream).await.unwrap();
        if kind == 6 && flags == 1 {
            break;
        }
    }
    drop(stream);
    let committed = f.admin.snapshot().unwrap();
    assert_eq!(committed.rows[&key].status, Status::Queued);
    assert_eq!(committed.rows[&key].version, before.version + 1);
    assert_eq!(committed.tasks[&TaskId(1)].transitions, 1);
    assert!(
        committed
            .operations
            .contains_key(&(TaskId(1), OperationId(713)))
    );
    let initial_accounting = f.admin.task_resources(TaskId(1)).unwrap().unwrap().used;
    let status = f
        .exec_http(
            &f.task,
            &token,
            serde_json::json!({"status":{"operation":"713"}}),
            false,
        )
        .await;
    assert_eq!(status["code"], "committed");
    let repeated = f.exec_http(&f.task, &token, request.clone(), true).await;
    assert_eq!(repeated["code"], "committed");
    assert_eq!(repeated["digest"], status["digest"]);
    let mut expired = request;
    expired["budgetMs"] = serde_json::json!(0);
    let (code, _) = f
        .http(
            &f.task,
            "POST",
            "/v1/session/execute",
            Some(&token),
            serde_json::to_vec(&expired).unwrap(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(code, 408);
    let after = f.admin.snapshot().unwrap();
    assert_eq!(after.rows, committed.rows);
    assert_eq!(after.operations, committed.operations);
    assert_eq!(after.tasks[&TaskId(1)].transitions, 1);
    let accounting = f.admin.task_resources(TaskId(1)).unwrap().unwrap().used;
    assert!(accounting.read_rows >= initial_accounting.read_rows);
    assert!(accounting.disclosed_rows >= initial_accounting.disclosed_rows);
    assert!(accounting.encoded_bytes >= initial_accounting.encoded_bytes);
    f.stop().await;
}

async fn raw_grpc<B>(f: &Fixture, body: B, headers: &[(&str, &str)]) -> u32
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let stream = tls(f, true, true).await;
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut request = http::Request::builder()
        .method("POST")
        .uri("https://localhost/giggles.v1.Giggles/OpenSession")
        .header("content-type", "application/grpc");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        sender.send_request(request.body(body).unwrap()),
    )
    .await
    .unwrap()
    .unwrap();
    let mut status = response
        .headers()
        .get("grpc-status")
        .map(|value| value.to_str().unwrap().parse::<u32>().unwrap());
    let mut metadata_bytes = response
        .headers()
        .iter()
        .map(|(name, value)| name.as_str().len() + value.len() + 4)
        .sum::<usize>();
    let mut body = response.into_body();
    while let Some(frame) = body.frame().await {
        if let Ok(trailers) = frame.unwrap().into_trailers() {
            metadata_bytes += trailers
                .iter()
                .map(|(name, value)| name.as_str().len() + value.len() + 4)
                .sum::<usize>();
            status = trailers
                .get("grpc-status")
                .map(|value| value.to_str().unwrap().parse().unwrap());
        }
    }
    assert!(
        metadata_bytes + 64 <= 512,
        "fixed headers, trailers and HTTP/2 framing allowance"
    );
    task.abort();
    let _ = task.await;
    status.expect("terminal grpc-status")
}

#[tokio::test]
async fn grpc_unary_compression_metadata_and_message_limits() {
    let f = Fixture::new().await;
    for (message, headers, expected) in [
        (vec![], vec![], 3),
        (vec![0, 0, 0, 0, 1], vec![], 3),
        (vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0], vec![], 3),
        (vec![0, 255, 255, 255, 255], vec![], 8),
        (vec![1, 0, 0, 0, 0], vec![], 12),
        (vec![0, 0, 0, 0, 0], vec![("grpc-encoding", "gzip")], 12),
        (vec![0, 0, 0, 0, 0], vec![("grpc-timeout", "1s")], 3),
        (
            vec![0, 0, 0, 0, 0],
            vec![("grpc-timeout", "1S"), ("grpc-timeout", "2S")],
            3,
        ),
        (vec![0, 0, 0, 0, 0], vec![("grpc-timeout", "0n")], 4),
    ] {
        assert_eq!(
            raw_grpc(&f, Full::new(Bytes::from(message)), &headers).await,
            expected
        );
    }
    assert_eq!(
        raw_grpc(&f, Full::new(Bytes::from_static(&[0, 0, 0, 0, 0])), &[]).await,
        0
    );
    f.stop().await;
}

struct PendingBody;
impl Body for PendingBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        Poll::Pending
    }
}

#[tokio::test]
async fn grpc_deadline_includes_incomplete_body_and_session_capacity_recovers() {
    let f = Fixture::with_limits(ApiLimits { sessions: 1 }).await;
    let started = Instant::now();
    assert_eq!(
        raw_grpc(&f, PendingBody, &[("grpc-timeout", "20m")]).await,
        4
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(
        raw_grpc(&f, Full::new(Bytes::from_static(&[0, 0, 0, 0, 0])), &[]).await,
        0
    );
    f.stop().await;
}

#[tokio::test]
async fn header_count_and_decoded_size_reject_before_dispatch() {
    let f = Fixture::with_limits(ApiLimits { sessions: 1 }).await;
    let headers = vec![("x-extra", "value"); 70];
    let result = f
        .http_headers(
            &f.app,
            "POST",
            "/v1/sessions",
            None,
            b"{}".to_vec(),
            true,
            &headers,
        )
        .await;
    assert!(result.is_err() || result.unwrap().0 == 400);
    let large = "x".repeat(17 * 1024);
    let result = f
        .http_headers(
            &f.app,
            "POST",
            "/v1/sessions",
            None,
            b"{}".to_vec(),
            true,
            &[("x-large", &large)],
        )
        .await;
    assert!(
        result
            .as_ref()
            .map_or(true, |(status, _)| matches!(status, 400 | 431)),
        "oversized headers: {result:?}"
    );
    assert_eq!(f.open_http(&f.app, true).await.len(), 64);
    f.stop().await;
}
