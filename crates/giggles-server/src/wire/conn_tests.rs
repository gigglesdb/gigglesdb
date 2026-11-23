use super::{
    conn::{ConnectionStats, WireLimits, read_handshake, serve},
    exec::CallHook,
    frame::{Flags, FrameKind, HEADER_LEN, encode_message, write_preface},
    messages::{
        WireBatch, WireBatchEntry, WireBatchMode, WireCancel, WireConnError, WireReady, WireReply,
        wire_batch_entry,
    },
};
use crate::{
    Binding, Role, Service,
    api::{ApiLimits, ApiRuntime, ConnectionIdentity, error::ApiError, proto},
    clock::Clock,
};
use giggles_core::{Admin, Engine, Limits, relational::Value};
use prost::Message;
use proto::{ExecuteRequest, execute_request::Request};
use std::{
    collections::BTreeSet,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, duplex},
    task::JoinHandle,
    time::timeout,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    runtime: Arc<ApiRuntime>,
    identity: ConnectionIdentity,
}

impl Fixture {
    fn new(rows: usize) -> Self {
        Self::with_engine(rows, Engine::Legacy)
    }

    fn with_engine(rows: usize, engine: Engine) -> Self {
        let path = crate::test_support::fixture_root().join(format!(
            "wire-conn-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Relaxed),
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap();
        let mut sql = admin.sql_session();
        sql.execute(
            "CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT)",
            &[],
            None,
        )
        .unwrap();
        let insert = sql.prepare("INSERT INTO app VALUES ($1, $2)").unwrap();
        let body = Value::Text("x".repeat(8_000));
        sql.execute("BEGIN", &[], None).unwrap();
        for id in 1..=rows {
            sql.execute_prepared(&insert, &[Value::Int(id as i64), body.clone()], None)
                .unwrap();
        }
        sql.execute("COMMIT", &[], None).unwrap();

        let fingerprint = "11".repeat(32);
        let service = Service::new(
            admin.clone(),
            vec![Binding {
                fingerprint: fingerprint.clone(),
                role: Role::Application {
                    domain: admin.authority_metadata().unwrap().0.to_string(),
                    tables: BTreeSet::from(["app".into()]),
                },
            }],
            Clock::new(0, 3600).unwrap(),
        )
        .unwrap();
        let runtime = ApiRuntime::new(service, ApiLimits { sessions: 1 }).unwrap();
        let identity = runtime.authenticate(&fingerprint).unwrap();
        Self { runtime, identity }
    }

    fn connect(
        &self,
        capacity: usize,
        limits: WireLimits,
        hook: Option<Arc<CallHook>>,
        identity: Result<ConnectionIdentity, ApiError>,
    ) -> Connection {
        let (client, server) = duplex(capacity);
        let writes = Arc::new(AtomicUsize::new(0));
        let panic_read = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(ConnectionStats::default());
        let socket = self.runtime.service().admission.connect().unwrap();
        let task = tokio::spawn(serve(
            CountingWriter {
                stream: server,
                writes: writes.clone(),
                panic_read: panic_read.clone(),
            },
            self.runtime.clone(),
            identity,
            limits,
            socket,
            stats.clone(),
            hook,
        ));
        Connection {
            client,
            task,
            writes,
            panic_read,
            stats,
        }
    }

    async fn ready(
        &self,
        capacity: usize,
        limits: WireLimits,
        hook: Option<Arc<CallHook>>,
    ) -> Connection {
        let expected = limits.max_in_flight;
        let mut connection = self.connect(capacity, limits, hook, Ok(self.identity.clone()));
        send(&mut connection.client, &hello()).await;
        let (kind, flags, id, body) = recv(&mut connection.client).await;
        assert_eq!((kind, flags, id), (FrameKind::Ready as u8, 0, 1));
        let ready = WireReady::decode(body.as_slice()).unwrap();
        assert_eq!(ready.max_in_flight as usize, expected);
        assert_eq!(ready.max_frame as usize, crate::MAX_FRAME);
        assert_eq!(ready.protocol_minor, 1);
        assert_eq!((ready.max_handles, ready.max_shapes), (256, 256));
        assert!(!ready.session.unwrap().session_id.is_empty());
        connection.writes.store(0, Relaxed);
        connection
    }
}

struct CountingWriter {
    stream: DuplexStream,
    writes: Arc<AtomicUsize>,
    panic_read: Arc<AtomicBool>,
}

impl AsyncRead for CountingWriter {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        assert!(!self.panic_read.load(Relaxed), "injected wire read panic");
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for CountingWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.stream).poll_write(cx, buf);
        if matches!(result, Poll::Ready(Ok(n)) if n > 0) {
            self.writes.fetch_add(1, Relaxed);
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

struct Connection {
    client: DuplexStream,
    task: JoinHandle<io::Result<()>>,
    writes: Arc<AtomicUsize>,
    panic_read: Arc<AtomicBool>,
    stats: Arc<ConnectionStats>,
}

impl Connection {
    async fn finish(self) {
        drop(self.client);
        timeout(TEST_TIMEOUT, self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(self.stats.reader_tasks.load(Relaxed), 0);
    }
}

fn hello() -> Vec<u8> {
    let mut bytes = Vec::new();
    write_preface(&mut bytes);
    encode_message(
        &mut bytes,
        FrameKind::Hello,
        Flags(0),
        1,
        &proto::OpenSessionRequest::default(),
    );
    bytes
}

fn execute(bytes: &mut Vec<u8>, id: u32, request: Request) {
    encode_message(
        bytes,
        FrameKind::Execute,
        Flags(0),
        id,
        &ExecuteRequest {
            request: Some(request),
            ..Default::default()
        },
    );
}

fn ping(bytes: &mut Vec<u8>, id: u32) {
    execute(bytes, id, Request::Ping(proto::PingRequest {}));
}

fn sql(bytes: &mut Vec<u8>, id: u32, sql: &str) {
    execute(
        bytes,
        id,
        Request::Sql(proto::SqlRequest {
            sql: sql.into(),
            params: vec![],
        }),
    );
}

fn cancel(bytes: &mut Vec<u8>, id: u32, target: u32) {
    encode_message(
        bytes,
        FrameKind::Cancel,
        Flags(0),
        id,
        &WireCancel {
            target_request_id: target,
        },
    );
}

async fn send(stream: &mut DuplexStream, bytes: &[u8]) {
    timeout(TEST_TIMEOUT, stream.write_all(bytes))
        .await
        .unwrap()
        .unwrap();
}

async fn recv(stream: &mut DuplexStream) -> (u8, u8, u32, Vec<u8>) {
    timeout(TEST_TIMEOUT, async {
        let mut header = [0; HEADER_LEN];
        stream.read_exact(&mut header).await.unwrap();
        let len = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize - 8;
        assert!(len <= crate::MAX_FRAME);
        let mut body = vec![0; len];
        stream.read_exact(&mut body).await.unwrap();
        (
            header[4],
            header[5],
            u32::from_be_bytes(header[8..].try_into().unwrap()),
            body,
        )
    })
    .await
    .unwrap()
}

async fn wire_reply(stream: &mut DuplexStream, id: u32) -> (WireReply, usize) {
    let mut bytes = Vec::new();
    let mut frames = 0;
    loop {
        let (kind, flags, actual_id, body) = recv(stream).await;
        assert_eq!((kind, actual_id), (FrameKind::Reply as u8, id), "{body:?}");
        frames += 1;
        bytes.extend(body);
        if flags == 0 {
            return (WireReply::decode(bytes.as_slice()).unwrap(), frames);
        }
        assert_eq!(flags, Flags::MORE);
    }
}

async fn reply(stream: &mut DuplexStream, id: u32) -> (proto::ExecuteReply, usize) {
    let (wire, frames) = wire_reply(stream, id).await;
    (wire.reply.unwrap(), frames)
}

async fn error(stream: &mut DuplexStream, id: u32, code: &str) {
    let (kind, flags, actual_id, body) = recv(stream).await;
    assert_eq!(
        (kind, flags, actual_id),
        (FrameKind::ConnError as u8, 0, id)
    );
    assert_eq!(WireConnError::decode(body.as_slice()).unwrap().code, code);
}

async fn eof(stream: &mut DuplexStream) {
    assert_eq!(
        timeout(TEST_TIMEOUT, stream.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

async fn until(mut condition: impl FnMut() -> bool) {
    timeout(TEST_TIMEOUT, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn sequential_replies_skip_batch_timer() {
    let fixture = Fixture::new(0);
    let mut connection = fixture.ready(4096, WireLimits::default(), None).await;
    let mut bytes = Vec::new();
    for id in 2..22 {
        bytes.clear();
        ping(&mut bytes, id);
        send(&mut connection.client, &bytes).await;
        assert_eq!(reply(&mut connection.client, id).await.0.code, "ok");
    }
    assert_eq!(connection.stats.timer_waits.load(Relaxed), 0);
    connection.finish().await;
}

#[tokio::test]
async fn replies_ready_together_use_one_write() {
    let fixture = Fixture::new(0);
    let mut connection = fixture.ready(4096, WireLimits::default(), None).await;
    let mut bytes = Vec::new();
    for id in 2..18 {
        ping(&mut bytes, id);
    }
    send(&mut connection.client, &bytes).await;
    for id in 2..18 {
        assert_eq!(reply(&mut connection.client, id).await.0.code, "ok");
    }
    assert!(connection.writes.load(Relaxed) <= 2);
    connection.finish().await;
}

#[tokio::test]
async fn full_pipeline_sees_cancel_and_preserves_deferred_order() {
    let fixture = Fixture::new(0);
    let hook = Arc::new(CallHook::new(true));
    let mut connection = fixture
        .ready(
            4096,
            WireLimits {
                max_in_flight: 4,
                ..WireLimits::default()
            },
            Some(hook.clone()),
        )
        .await;
    let mut bytes = Vec::new();
    for id in 2..6 {
        ping(&mut bytes, id);
    }
    send(&mut connection.client, &bytes).await;
    until(|| connection.stats.in_flight.load(Relaxed) == 4 && hook.calls.load(Relaxed) == 1).await;

    bytes.clear();
    ping(&mut bytes, 6);
    cancel(&mut bytes, 7, 6);
    cancel(&mut bytes, 8, 2);
    send(&mut connection.client, &bytes).await;
    error(&mut connection.client, 2, "DEADLINE_EXCEEDED").await;
    for id in 3..6 {
        assert_eq!(reply(&mut connection.client, id).await.0.code, "ok");
    }
    error(&mut connection.client, 6, "DEADLINE_EXCEEDED").await;
    assert_eq!(hook.calls.load(Relaxed), 4);
    connection.finish().await;
}

#[tokio::test]
async fn full_pipeline_rejects_reuse_of_deferred_id() {
    let fixture = Fixture::new(0);
    let hook = Arc::new(CallHook::new(true));
    let mut connection = fixture
        .ready(
            4096,
            WireLimits {
                max_in_flight: 2,
                ..WireLimits::default()
            },
            Some(hook),
        )
        .await;
    let mut bytes = Vec::new();
    for id in [2, 3, 4, 4] {
        ping(&mut bytes, id);
    }
    send(&mut connection.client, &bytes).await;
    error(&mut connection.client, 0, "protocol").await;
    eof(&mut connection.client).await;
    connection.finish().await;
}

#[tokio::test]
async fn full_pipeline_goodbye_discards_queue_without_cancelling_running_call() {
    check_full_pipeline_goodbye().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_full_pipeline_goodbye_discards_queue_without_cancelling_running_call() {
    check_full_pipeline_goodbye().await;
}

async fn check_full_pipeline_goodbye() {
    let fixture = Fixture::new(0);
    let hook = Arc::new(CallHook::new(true));
    let mut connection = fixture
        .ready(
            4096,
            WireLimits {
                max_in_flight: 2,
                ..WireLimits::default()
            },
            Some(hook.clone()),
        )
        .await;
    let mut bytes = Vec::new();
    ping(&mut bytes, 2);
    ping(&mut bytes, 3);
    send(&mut connection.client, &bytes).await;
    until(|| connection.stats.in_flight.load(Relaxed) == 2 && hook.calls.load(Relaxed) == 1)
        .await;

    bytes.clear();
    encode_message(&mut bytes, FrameKind::Goodbye, Flags(0), 4, &());
    send(&mut connection.client, &bytes).await;
    connection.client.shutdown().await.unwrap();
    until(|| connection.stats.reader_tasks.load(Relaxed) == 0).await;
    hook.release();

    eof(&mut connection.client).await;
    assert!(!hook.cancelled.load(Relaxed));
    assert_eq!(hook.calls.load(Relaxed), 1);
    connection.finish().await;
}

#[tokio::test]
async fn aborting_connection_stops_reader_with_peer_still_open() {
    let fixture = Fixture::new(0);
    let connection = fixture.ready(4096, WireLimits::default(), None).await;
    until(|| connection.stats.reader_tasks.load(Relaxed) == 1).await;

    connection.task.abort();
    assert!(connection.task.await.unwrap_err().is_cancelled());
    until(|| connection.stats.reader_tasks.load(Relaxed) == 0).await;
}

#[tokio::test]
async fn reader_panic_ends_connection_and_cancels_running_request() {
    let fixture = Fixture::new(0);
    let hook = Arc::new(CallHook::new(true));
    let mut connection = fixture
        .ready(4096, WireLimits::default(), Some(hook.clone()))
        .await;
    let mut bytes = Vec::new();
    ping(&mut bytes, 2);
    send(&mut connection.client, &bytes).await;
    until(|| hook.calls.load(Relaxed) == 1).await;

    connection.panic_read.store(true, Relaxed);
    send(&mut connection.client, &[0]).await;
    let error = timeout(TEST_TIMEOUT, connection.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(error.to_string(), "wire reader task ended");
    assert!(hook.cancelled.load(Relaxed));
    assert_eq!(connection.stats.reader_tasks.load(Relaxed), 0);
}

#[tokio::test]
async fn cancel_of_completed_request_does_not_cancel_reused_id() {
    let fixture = Fixture::new(0);
    let mut connection = fixture.ready(4096, WireLimits::default(), None).await;
    let mut bytes = Vec::new();
    ping(&mut bytes, 2);
    send(&mut connection.client, &bytes).await;
    assert_eq!(reply(&mut connection.client, 2).await.0.code, "ok");

    bytes.clear();
    cancel(&mut bytes, 3, 2);
    ping(&mut bytes, 2);
    send(&mut connection.client, &bytes).await;
    assert_eq!(reply(&mut connection.client, 2).await.0.code, "ok");
    connection.finish().await;
}

#[tokio::test]
async fn idle_connection_ends_after_session_is_closed() {
    let fixture = Fixture::new(0);
    let mut connection = fixture.connect(
        4096,
        WireLimits::default(),
        None,
        Ok(fixture.identity.clone()),
    );
    send(&mut connection.client, &hello()).await;
    let (kind, _, _, body) = recv(&mut connection.client).await;
    assert_eq!(kind, FrameKind::Ready as u8);
    let ready = WireReady::decode(body.as_slice()).unwrap();
    fixture
        .runtime
        .close(&fixture.identity, &ready.session.unwrap().session_id)
        .unwrap();
    error(&mut connection.client, 0, "SESSION_NOT_FOUND").await;
    eof(&mut connection.client).await;
    connection.finish().await;
}

#[tokio::test]
async fn batch_entries_count_against_in_flight() {
    check_stop_on_error_batch_in_flight().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_batch_entries_count_against_in_flight() {
    check_stop_on_error_batch_in_flight().await;
}

async fn check_stop_on_error_batch_in_flight() {
    let fixture = Fixture::new(0);
    let hook = Arc::new(CallHook::new(true));
    let mut connection = fixture
        .ready(
            4096,
            WireLimits {
                max_in_flight: 4,
                ..WireLimits::default()
            },
            Some(hook.clone()),
        )
        .await;
    let mut bytes = Vec::new();
    for (header_id, ids) in [(2, 2..12), (22, 12..22)] {
        encode_message(
            &mut bytes,
            FrameKind::Batch,
            Flags(0),
            header_id,
            &WireBatch {
                mode: WireBatchMode::StopOnError as i32,
                entries: ids
                    .map(|request_id| WireBatchEntry {
                        request_id,
                        entry: Some(wire_batch_entry::Entry::Execute(ExecuteRequest {
                            request: Some(Request::Ping(proto::PingRequest {})),
                            ..Default::default()
                        })),
                    })
                    .collect(),
            },
        );
    }
    cancel(&mut bytes, 23, 16);
    ping(&mut bytes, 22);
    send(&mut connection.client, &bytes).await;
    until(|| {
        connection.stats.in_flight.load(Relaxed) == 4
            && hook.calls.load(Relaxed) == 1
            && connection.stats.decoder_len.load(Relaxed) == 0
            && connection.stats.read_bytes.load(Relaxed) == bytes.len()
    })
    .await;
    hook.release();
    for id in 2..16 {
        assert_eq!(reply(&mut connection.client, id).await.0.code, "ok");
    }
    error(&mut connection.client, 16, "DEADLINE_EXCEEDED").await;
    for id in 17..22 {
        assert_eq!(
            recv(&mut connection.client).await,
            (FrameKind::Reply as u8, Flags::SKIPPED, id, vec![])
        );
    }
    assert_eq!(reply(&mut connection.client, 22).await.0.code, "ok");
    #[cfg(feature = "test-hooks")]
    assert_eq!(connection.stats.peak_in_flight(), 4);
    assert_eq!(hook.calls.load(Relaxed), 15);
    connection.finish().await;
}

#[tokio::test]
async fn full_pipeline_stops_reading_at_lookahead_frame_limit() {
    let fixture = Fixture::new(0);
    let hook = Arc::new(CallHook::new(true));
    let mut connection = fixture
        .ready(
            128,
            WireLimits {
                max_in_flight: 4,
                ..WireLimits::default()
            },
            Some(hook.clone()),
        )
        .await;
    let mut bytes = Vec::new();
    for id in 2..10 {
        ping(&mut bytes, id);
    }
    send(&mut connection.client, &bytes).await;
    until(|| {
        connection.stats.in_flight.load(Relaxed) == 4
            && hook.calls.load(Relaxed) == 1
            && connection.stats.decoder_len.load(Relaxed) == 0
            && connection.stats.read_bytes.load(Relaxed) == bytes.len()
    })
    .await;
    let read = connection.stats.read_bytes.load(Relaxed);
    bytes.clear();
    for id in 10..40 {
        ping(&mut bytes, id);
    }
    assert!(
        timeout(Duration::from_millis(30), connection.client.write_all(&bytes))
            .await
            .is_err()
    );
    assert_eq!(connection.stats.read_bytes.load(Relaxed), read);
    hook.release();
    connection.finish().await;
}

#[tokio::test]
async fn full_pipeline_stops_reading_at_lookahead_byte_limit() {
    let fixture = Fixture::new(0);
    let hook = Arc::new(CallHook::new(true));
    let mut connection = fixture
        .ready(16 * 1024, WireLimits::default(), Some(hook.clone()))
        .await;
    let mut bytes = Vec::new();
    for id in 2..66 {
        ping(&mut bytes, id);
    }
    send(&mut connection.client, &bytes).await;
    until(|| connection.stats.in_flight.load(Relaxed) == 64 && hook.calls.load(Relaxed) == 1).await;

    let before = bytes.len();
    let cap = 1024 * 1024 + crate::MAX_FRAME;
    bytes.clear();
    let large = vec![0; crate::MAX_FRAME];
    for id in 66..74 {
        super::frame::encode_frame(&mut bytes, FrameKind::Prepare, Flags(0), id, &large);
    }
    assert!(
        timeout(Duration::from_millis(100), connection.client.write_all(&bytes))
            .await
            .is_err()
    );
    assert_eq!(connection.stats.read_bytes.load(Relaxed) - before, cap);
    assert!(connection.stats.peak_decoder_len.load(Relaxed) <= cap);
    assert_eq!(hook.calls.load(Relaxed), 1);
    hook.release();
    connection.finish().await;
}

#[tokio::test]
async fn full_pipeline_drains_buffered_frames_before_reading_again() {
    let fixture = Fixture::new(0);
    let mut connection = fixture
        .ready(
            32 * 1024,
            WireLimits {
                max_in_flight: 4,
                ..WireLimits::default()
            },
            None,
        )
        .await;
    let mut bytes = Vec::new();
    for id in 2..2048 {
        ping(&mut bytes, id);
    }
    send(&mut connection.client, &bytes).await;
    assert_eq!(reply(&mut connection.client, 2).await.0.code, "ok");
    for id in 3..2048 {
        assert_eq!(reply(&mut connection.client, id).await.0.code, "ok");
    }
    let peak = connection.stats.peak_decoder_len.load(Relaxed);
    assert!(
        peak < 9 * 1024,
        "decoder retained {peak} bytes of small requests"
    );
    connection.finish().await;
}

#[tokio::test]
async fn pending_reply_cap_stops_execution() {
    let fixture = Fixture::new(1);
    let hook = Arc::new(CallHook::new(false));
    let mut connection = fixture
        .ready(
            128,
            WireLimits {
                max_pending_reply_bytes: 1024,
                ..WireLimits::default()
            },
            Some(hook.clone()),
        )
        .await;
    let mut bytes = Vec::new();
    for id in 2..4 {
        sql(&mut bytes, id, "SELECT body FROM app");
    }
    send(&mut connection.client, &bytes).await;
    until(|| connection.stats.pending_bytes.load(Relaxed) >= 1024).await;
    assert!(
        timeout(Duration::from_millis(30), &mut connection.task)
            .await
            .is_err()
    );
    assert_eq!(hook.calls.load(Relaxed), 1);
    assert!(connection.stats.pending_bytes.load(Relaxed) < 9_000);
    for id in 2..4 {
        assert_eq!(
            reply(&mut connection.client, id)
                .await
                .0
                .sql
                .unwrap()
                .rows
                .len(),
            1
        );
    }
    connection.finish().await;
}

#[tokio::test]
async fn request_id_reuse_is_protocol_error() {
    let fixture = Fixture::new(0);
    let hook = Arc::new(CallHook::new(true));
    let mut connection = fixture
        .ready(4096, WireLimits::default(), Some(hook.clone()))
        .await;
    let mut bytes = Vec::new();
    ping(&mut bytes, 2);
    send(&mut connection.client, &bytes).await;
    until(|| hook.calls.load(Relaxed) == 1).await;
    send(&mut connection.client, &bytes).await;
    until(|| hook.cancelled.load(Relaxed)).await;
    error(&mut connection.client, 0, "protocol").await;
    eof(&mut connection.client).await;
    connection.finish().await;
}

#[tokio::test]
async fn finished_id_can_be_reused_out_of_order() {
    let fixture = Fixture::new(0);
    let mut connection = fixture.ready(4096, WireLimits::default(), None).await;
    for id in [90, 2, 90] {
        let mut bytes = Vec::new();
        ping(&mut bytes, id);
        send(&mut connection.client, &bytes).await;
        assert_eq!(reply(&mut connection.client, id).await.0.code, "ok");
    }
    connection.finish().await;
}

#[tokio::test]
async fn frame_before_hello_is_protocol_error() {
    let fixture = Fixture::new(0);
    let mut connection = fixture.connect(
        4096,
        WireLimits::default(),
        None,
        Ok(fixture.identity.clone()),
    );
    let mut bytes = Vec::new();
    write_preface(&mut bytes);
    ping(&mut bytes, 1);
    send(&mut connection.client, &bytes).await;
    error(&mut connection.client, 0, "protocol").await;
    eof(&mut connection.client).await;
    connection.finish().await;
}

/// The preface and HELLO share one IO_TIMEOUT (spec section 2). A client that
/// finishes the preface late and then drips the HELLO header is closed at the
/// first deadline (about 10 s), not 10 s after the preface (about 18 s).
#[tokio::test]
async fn dripping_preface_and_hello_share_one_deadline() {
    let fixture = Fixture::new(0);
    let mut connection = fixture.connect(
        4096,
        WireLimits::default(),
        None,
        Ok(fixture.identity.clone()),
    );
    let started = std::time::Instant::now();
    let bytes = hello();
    send(&mut connection.client, &bytes[..7]).await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    send(&mut connection.client, &bytes[7..9]).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    send(&mut connection.client, &bytes[9..10]).await;
    let closed = timeout(Duration::from_secs(3), connection.client.read(&mut [0]))
        .await
        .expect("the connection is closed by the shared deadline");
    assert!(matches!(closed, Ok(0) | Err(_)), "{closed:?}");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(9500) && elapsed < Duration::from_secs(12),
        "{elapsed:?}"
    );
    let result = timeout(TEST_TIMEOUT, connection.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
}

#[tokio::test]
async fn unexpected_frame_during_auth_is_protocol_error() {
    let (mut client, mut server) = duplex(64);
    let mut bytes = Vec::new();
    ping(&mut bytes, 3);
    send(&mut client, &bytes).await;
    assert!(
        read_handshake::<_, proto::ScramContinue>(
            &mut server,
            FrameKind::Auth,
            tokio::time::Instant::now() + TEST_TIMEOUT,
        )
        .await
            .unwrap_err()
            .is_protocol()
    );
}

#[tokio::test]
async fn missing_identity_is_fatal_after_hello() {
    let fixture = Fixture::new(0);
    let mut connection = fixture.connect(
        4096,
        WireLimits::default(),
        None,
        Err(ApiError::Unauthenticated),
    );
    send(&mut connection.client, &hello()).await;
    error(&mut connection.client, 0, "UNAUTHENTICATED").await;
    eof(&mut connection.client).await;
    connection.finish().await;
}

#[tokio::test]
async fn cancel_unknown_id_ignored() {
    let fixture = Fixture::new(0);
    let mut connection = fixture.ready(4096, WireLimits::default(), None).await;
    let mut bytes = Vec::new();
    cancel(&mut bytes, 2, 999);
    ping(&mut bytes, 3);
    send(&mut connection.client, &bytes).await;
    assert_eq!(reply(&mut connection.client, 3).await.0.code, "ok");
    connection.finish().await;
}

#[tokio::test]
async fn cancel_queued_entry_replies_cancelled_without_running() {
    let fixture = Fixture::new(0);
    let hook = Arc::new(CallHook::new(true));
    let mut connection = fixture
        .ready(4096, WireLimits::default(), Some(hook.clone()))
        .await;
    let mut bytes = Vec::new();
    ping(&mut bytes, 2);
    send(&mut connection.client, &bytes).await;
    until(|| hook.calls.load(Relaxed) == 1).await;
    bytes.clear();
    ping(&mut bytes, 3);
    cancel(&mut bytes, 4, 3);
    send(&mut connection.client, &bytes).await;
    until(|| {
        connection.stats.in_flight.load(Relaxed) == 2
            && connection.stats.decoder_len.load(Relaxed) == 0
    })
    .await;
    hook.release();
    assert_eq!(reply(&mut connection.client, 2).await.0.code, "ok");
    error(&mut connection.client, 3, "DEADLINE_EXCEEDED").await;
    assert_eq!(hook.calls.load(Relaxed), 1);
    connection.finish().await;
}

#[tokio::test]
async fn buffers_shrink_after_large_reply() {
    let fixture = Fixture::new(130);
    let mut connection = fixture.ready(64 * 1024, WireLimits::default(), None).await;
    let mut bytes = Vec::new();
    sql(&mut bytes, 2, "SELECT body FROM app ORDER BY id");
    send(&mut connection.client, &bytes).await;
    assert_eq!(
        reply(&mut connection.client, 2)
            .await
            .0
            .sql
            .unwrap()
            .rows
            .len(),
        130
    );
    bytes.clear();
    ping(&mut bytes, 3);
    send(&mut connection.client, &bytes).await;
    assert_eq!(reply(&mut connection.client, 3).await.0.code, "ok");
    until(|| connection.stats.pending_bytes.load(Relaxed) == 0).await;
    assert!(connection.stats.write_capacity.load(Relaxed) <= 64 * 1024);
    connection.finish().await;
}

#[tokio::test]
async fn oversized_reply_is_split_into_more_frames() {
    check_oversized_reply_split().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_oversized_reply_is_split_into_more_frames() {
    check_oversized_reply_split().await;
}

async fn check_oversized_reply_split() {
    let fixture = Fixture::new(40);
    let mut connection = fixture.ready(64 * 1024, WireLimits::default(), None).await;
    let mut bytes = Vec::new();
    sql(&mut bytes, 2, "SELECT body FROM app ORDER BY id");
    ping(&mut bytes, 3);
    send(&mut connection.client, &bytes).await;
    let (large, frames) = reply(&mut connection.client, 2).await;
    assert!(frames > 1);
    assert_eq!(large.sql.unwrap().rows.len(), 40);
    assert_eq!(reply(&mut connection.client, 3).await.0.code, "ok");
    connection.finish().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn spilled_reply_is_reassembled_before_next_reply() {
    check_spilled_reply_reassembly().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_spilled_reply_is_reassembled_before_next_reply() {
    check_spilled_reply_reassembly().await;
}

#[cfg(feature = "engine-rocksdb")]
async fn check_spilled_reply_reassembly() {
    let fixture = Fixture::with_engine(140, Engine::V2);
    let mut connection = fixture.ready(64 * 1024, WireLimits::default(), None).await;
    let mut bytes = Vec::new();
    sql(&mut bytes, 2, "SELECT body FROM app ORDER BY id");
    ping(&mut bytes, 3);
    send(&mut connection.client, &bytes).await;
    let (wire, frames) = wire_reply(&mut connection.client, 2).await;
    assert!(
        frames > 5,
        "expected streamed row chunks, got {frames} frames"
    );
    assert_eq!(
        wire.shapes,
        [proto::WireShapeRef {
            position: 0,
            shape_id: Some(0),
            columns: vec!["body".into()],
            type_oids: vec![25],
        }]
    );
    let mut large = wire.reply.unwrap();
    assert!(large.sql.as_ref().unwrap().columns.is_empty());
    super::shapes::expand(&mut Vec::new(), &mut large, &wire.shapes).unwrap();
    let sql = large.sql.unwrap();
    assert_eq!(sql.columns, ["body"]);
    assert_eq!(sql.rows.len(), 140);
    assert_eq!(reply(&mut connection.client, 3).await.0.code, "ok");
    connection.finish().await;
}

#[tokio::test]
async fn eof_with_open_transaction_closes_session() {
    let fixture = Fixture::new(0);
    let mut connection = fixture.ready(4096, WireLimits::default(), None).await;
    let mut bytes = Vec::new();
    execute(&mut bytes, 2, Request::Begin(proto::BeginRequest {}));
    sql(&mut bytes, 3, "INSERT INTO app VALUES (1, 'uncommitted')");
    send(&mut connection.client, &bytes).await;
    assert_eq!(reply(&mut connection.client, 2).await.0.code, "ok");
    assert_eq!(reply(&mut connection.client, 3).await.0.code, "ok");
    connection.finish().await;
    assert_eq!(fixture.runtime.service().blocking.in_use(), 0);
    // With one slot, a replacement cannot open until the old session is gone.
    let mut replacement = fixture.ready(4096, WireLimits::default(), None).await;
    bytes.clear();
    sql(&mut bytes, 2, "SELECT id FROM app");
    send(&mut replacement.client, &bytes).await;
    assert!(
        reply(&mut replacement.client, 2)
            .await
            .0
            .sql
            .unwrap()
            .rows
            .is_empty()
    );
    replacement.finish().await;
}

#[tokio::test]
async fn ready_reports_protocol_minor_1() {
    let fixture = Fixture::new(0);
    let connection = fixture.ready(4096, WireLimits::default(), None).await;
    connection.finish().await;
}
