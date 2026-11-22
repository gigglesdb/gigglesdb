#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::parity::{APP, ParityFixture};
use giggles_core::relational::{Value, relation_key};
use giggles_server::{
    MAX_FRAME, MAX_RESPONSE,
    api::proto,
    wire::{
        client::{BatchEntry, BatchOutcome, Client},
        frame::{
            Direction, Flags, FrameDecoder, FrameHeader, FrameKind, HEADER_LEN, encode_message,
            write_preface,
        },
        listen::ConnectionStats,
        messages::{WireBatchMode, WireConnError, WireReply, WireRun},
    },
};
use prost::Message;
use rustls::pki_types::ServerName;
use std::{io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
    time::{sleep, timeout},
};
use tokio_rustls::{TlsConnector, client::TlsStream};

const TEST_TIMEOUT: Duration = Duration::from_secs(3);

fn sql(statement: &str) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: statement.into(),
            params: vec![],
        })),
        ..Default::default()
    }
}

async fn wait_for(reason: &str, mut condition: impl FnMut() -> bool) {
    timeout(TEST_TIMEOUT, async {
        while !condition() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {reason}"));
}

async fn read_frame(
    stream: &mut (impl AsyncRead + Unpin),
    decoder: &mut FrameDecoder,
) -> io::Result<(FrameHeader, Vec<u8>)> {
    loop {
        if let Some((header, body)) = decoder.next_frame().unwrap() {
            let body = decoder.body(body).to_vec();
            decoder.consume_decoded();
            return Ok((header, body));
        }
        if stream.read_buf(decoder.read_buf()).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
}

async fn backpressured_wire(f: &ParityFixture) -> (TlsStream<TcpStream>, FrameDecoder) {
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(64 * 1024).unwrap();
    let socket = socket.connect(f.wire.address).await.unwrap();
    let mut stream = TlsConnector::from(Arc::new(f.sql_app.client(&f.ca)))
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap();
    let mut decoder = FrameDecoder::new(Direction::ServerToClient, MAX_FRAME);
    let mut bytes = Vec::new();
    write_preface(&mut bytes);
    encode_message(
        &mut bytes,
        FrameKind::Hello,
        Flags(0),
        1,
        &proto::OpenSessionRequest::default(),
    );
    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();
    let (ready, _) = read_frame(&mut stream, &mut decoder).await.unwrap();
    assert_eq!(
        (ready.kind, ready.flags, ready.request_id),
        (FrameKind::Ready, Flags(0), 1)
    );
    (stream, decoder)
}

fn seed_wide_shipments(f: &ParityFixture) {
    seed_wide_shipment_rows(f, 1024);
}

fn seed_wide_shipment_rows(f: &ParityFixture, rows: i64) {
    let mut session = f.admin.sql_session();
    let insert = session
        .prepare("INSERT INTO shipments (merchant_id, id, status) VALUES (97, $1, $2)")
        .unwrap();
    let payload = Value::Text("w".repeat(48 * 1024));
    for start in (1..=rows).step_by(64) {
        session.execute("BEGIN", &[], None).unwrap();
        for id in start..=(start + 63).min(rows) {
            session
                .execute_prepared(&insert, &[Value::Int(id), payload.clone()], None)
                .unwrap();
        }
        session.execute("COMMIT", &[], None).unwrap();
    }
}

async fn raw_run(client: &mut Client, handle: u32) -> WireReply {
    let id = client
        .send_run(WireRun {
            handle,
            ..Default::default()
        })
        .await
        .unwrap();
    let (header, body) = timeout(TEST_TIMEOUT, client.recv_raw_frame())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (header.kind, header.flags, header.request_id),
        (FrameKind::Reply, Flags(0), id)
    );
    WireReply::decode(body.as_slice()).unwrap()
}

fn native_columns(reply: proto::ExecuteReply) -> Vec<String> {
    assert_eq!(reply.code, "ok", "{reply:?}");
    let Some(proto::execute_reply::NativeResult::OperationOutcome(outcome)) = reply.native_result
    else {
        panic!("missing read outcome: {reply:?}");
    };
    assert_eq!(outcome.rows.len(), 1);
    outcome.columns
}

async fn prepare_select(client: &mut Client) -> u32 {
    timeout(
        TEST_TIMEOUT,
        client.prepare("parity.items where id=1", Some("0.18")),
    )
    .await
    .expect("PREPARE reply")
    .unwrap()
    .unwrap()
}

async fn assert_native_shape(client: &mut Client, handle: u32, shape_id: u32, columns: &[&str]) {
    let wire = raw_run(client, handle).await;
    assert_eq!(
        wire.shapes,
        [proto::WireShapeRef {
            position: 1,
            shape_id: Some(shape_id),
            columns: columns.iter().map(|name| (*name).into()).collect(),
        }]
    );
    assert!(native_columns(wire.reply.unwrap()).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// Session revocation is always fatal (conn.rs request_error), so the server's
// tentative shape rollback cannot be observed here: the reply must never
// complete, the error carries the request ID, and the connection then ends.
// Rollback and shape-id reuse are observed by the spill-read test below.
async fn wire_lease_failure_after_shape_head_ends_connection_without_completing_reply() {
    let f = ParityFixture::start().await;
    seed_wide_shipments(&f);
    let registry_baseline = f.ordinary_api.test_registry_entry_count();
    let (mut stream, mut decoder) = timeout(TEST_TIMEOUT, backpressured_wire(&f))
        .await
        .expect("wire handshake");
    let peer = stream.get_ref().0.local_addr().unwrap();
    let stats = f.ordinary_api.test_wire_connection(peer).unwrap();
    let mut bytes = Vec::new();
    let id = 2;
    encode_message(
        &mut bytes,
        FrameKind::Execute,
        Flags(0),
        id,
        &sql("SELECT id, status AS revoked_body FROM shipments WHERE merchant_id=97 ORDER BY id"),
    );
    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();

    let (first, body) = timeout(TEST_TIMEOUT, read_frame(&mut stream, &mut decoder))
        .await
        .expect("reply head")
        .unwrap();
    assert_eq!((first.kind, first.request_id), (FrameKind::Reply, id));
    let head = WireReply::decode(body.as_slice()).unwrap();
    let reply = head.reply.unwrap();
    assert_eq!(reply.code, "ok", "reply head: {:?}", reply.message);
    assert_eq!(first.flags, Flags(Flags::MORE));
    assert_eq!(
        head.shapes,
        [proto::WireShapeRef {
            position: 0,
            shape_id: Some(0),
            columns: vec!["id".into(), "revoked_body".into()],
        }]
    );
    let sql = reply.sql.unwrap();
    assert!(sql.columns.is_empty());
    assert!(sql.rows.is_empty());

    // The first-MORE hook has no resume operation. Stop reading at the head
    // and wait for the pending cap before revoking the session. A 48 MiB reply
    // cannot fit in the 16 MiB pending cap and the socket buffers.
    wait_for("the pending reply cap", || {
        stats.peak_pending_reply_bytes() >= 2 * MAX_RESPONSE
    })
    .await;
    f.service.reload_bindings(vec![]).unwrap();
    f.ordinary_api.invalidate_bindings();

    timeout(TEST_TIMEOUT, async {
        let mut more_frames = 1;
        loop {
            let (header, body) = read_frame(&mut stream, &mut decoder).await.unwrap();
            assert_eq!(header.request_id, id);
            if header.kind == FrameKind::ConnError {
                assert_eq!(header.flags, Flags(0));
                let error = WireConnError::decode(body.as_slice()).unwrap();
                assert_eq!(error.code, "UNAUTHENTICATED");
                break;
            }
            // Frames queued before revocation may arrive. None completes the reply.
            assert_eq!(
                (header.kind, header.flags),
                (FrameKind::Reply, Flags(Flags::MORE))
            );
            more_frames += 1;
        }
        assert!(
            more_frames > 1,
            "expected multiple pre-revocation MORE frames"
        );
        let error = read_frame(&mut stream, &mut decoder).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    })
    .await
    .expect("terminal request error followed by EOF");
    wait_for("the revoked session to leave the registry", || {
        f.ordinary_api.test_registry_entry_count() == registry_baseline
    })
    .await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_run_redefines_shape_after_add_drop_and_rename() {
    let f = ParityFixture::start().await;
    f.sql(&format!("CREATE SCHEMA parity AUTHORIZATION {APP}"));
    f.sql("CREATE TABLE parity.items (id BIGINT PRIMARY KEY, body TEXT, obsolete TEXT)");
    f.sql("INSERT INTO parity.items VALUES (1, 'seed', 'drop me')");
    f.admin
        .roles()
        .set_table_owner(&relation_key("parity", "items"), APP)
        .unwrap();

    // Raw reads deliberately bypass Client's shape table. Use a second,
    // identically prepared connection to exercise Client's expansion.
    let mut raw = f.wire_client_for(&f.app).await;
    let mut expanded = f.wire_client_for(&f.app).await;
    let raw_handle = prepare_select(&mut raw).await;
    let expanded_handle = prepare_select(&mut expanded).await;

    assert_native_shape(&mut raw, raw_handle, 0, &["id", "body", "obsolete"]).await;
    let baseline = timeout(TEST_TIMEOUT, expanded.run(expanded_handle, vec![]))
        .await
        .expect("baseline RUN reply")
        .unwrap();
    assert_eq!(native_columns(baseline), ["id", "body", "obsolete"]);

    let changes: [(u32, &str, &[&str]); 3] = [
        (
            1,
            "ADD COLUMN external_column TEXT",
            &["id", "body", "obsolete", "external_column"],
        ),
        (
            2,
            "DROP COLUMN obsolete",
            &["id", "body", "external_column"],
        ),
        (
            3,
            "RENAME COLUMN body TO label",
            &["id", "label", "external_column"],
        ),
    ];
    for (shape_id, ddl, columns) in changes {
        f.sql(&format!("ALTER TABLE parity.items {ddl}"));
        assert_native_shape(&mut raw, raw_handle, shape_id, columns).await;
        assert_native_shape(&mut raw, raw_handle, shape_id, &[]).await;
        for _ in 0..2 {
            let reply = timeout(TEST_TIMEOUT, expanded.run(expanded_handle, vec![]))
                .await
                .unwrap_or_else(|_| panic!("RUN timed out after {ddl}"))
                .unwrap();
            assert_eq!(native_columns(reply), columns, "after {ddl}");
        }
    }

    raw.goodbye().await.unwrap();
    expanded.goodbye().await.unwrap();
    f.stop().await;
}

fn ping() -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
        ..Default::default()
    }
}

fn connection_stats(f: &ParityFixture, client: &Client) -> Arc<ConnectionStats> {
    f.ordinary_api
        .test_wire_connection(client.local_addr())
        .expect("stats for the authenticated wire connection")
}

async fn execute_ok(client: &mut Client, request: proto::ExecuteRequest) -> proto::ExecuteReply {
    let reply = timeout(TEST_TIMEOUT, client.execute(request))
        .await
        .expect("EXECUTE reply")
        .unwrap();
    assert_eq!(reply.code, "ok", "{:?}", reply.message);
    reply
}

fn insert_shipment(id: usize) -> proto::ExecuteRequest {
    sql(&format!(
        "INSERT INTO shipments (merchant_id, id, status) VALUES (96, {id}, 'batch')"
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_batch_disconnect_mid_batch_rolls_back() {
    let f = ParityFixture::start().await;
    let mut holder = f.wire_client_for(&f.sql_app).await;
    execute_ok(&mut holder, sql("BEGIN")).await;
    execute_ok(&mut holder, insert_shipment(2)).await;
    let registry_baseline = f.ordinary_api.test_registry_entry_count();

    let mut client = f.wire_client_for(&f.sql_app).await;
    execute_ok(&mut client, sql("BEGIN")).await;
    let stats = connection_stats(&f, &client);
    let limit = client.max_in_flight();
    let entries = (1..=limit + 8)
        .map(|id| BatchEntry::Execute(insert_shipment(id)))
        .collect::<Vec<_>>();
    assert!(entries.len() > limit + 1);
    let ids = timeout(TEST_TIMEOUT, client.send_batch(WireBatchMode::All, entries))
        .await
        .expect("send INSERT batch")
        .unwrap();
    let (id, result) = timeout(TEST_TIMEOUT, client.recv_batch_entry())
        .await
        .expect("first batch INSERT reply")
        .unwrap();
    assert_eq!(id, ids[0]);
    assert!(matches!(result, BatchOutcome::Reply(reply) if reply.code == "ok"));
    // One entry succeeded and the second is blocked. A full admitted queue
    // leaves the rest of this same BATCH outside the advertised limit.
    wait_for("a blocked batch INSERT with a full admitted queue", || {
        f.admin.waiting_lock_count() == 1
            && f.service.blocking_permits_held() == 1
            && stats.queued_requests() + 1 == limit
            && stats.peak_in_flight() == limit
            && f.ordinary_api.test_registry_entry_count() == registry_baseline + 1
    })
    .await;
    drop(client);

    wait_for(
        "the disconnected batch session to release its lock wait and permit",
        || {
            f.admin.waiting_lock_count() == 0
                && f.service.blocking_permits_held() == 0
                && f.ordinary_api.test_registry_entry_count() == registry_baseline
        },
    )
    .await;
    execute_ok(&mut holder, sql("ROLLBACK")).await;
    holder.goodbye().await.unwrap();

    let mut fresh = f.wire_client_for(&f.sql_app).await;
    let selected = execute_ok(
        &mut fresh,
        sql("SELECT id FROM shipments WHERE merchant_id=96"),
    )
    .await;
    assert!(selected.sql.unwrap().rows.is_empty());
    execute_ok(&mut fresh, ping()).await;
    fresh.goodbye().await.unwrap();
    f.stop().await;
}

const WIDE_READ: &str = "SELECT id, status FROM shipments WHERE merchant_id=97 ORDER BY id";
const WIDE_ROWS: i64 = 64;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_batch_never_reading_client_is_capped() {
    let f = ParityFixture::start().await;
    seed_wide_shipment_rows(&f, WIDE_ROWS);
    let mut pinger = f.wire_client(false).await;
    let registry_baseline = f.ordinary_api.test_registry_entry_count();
    let mut unread = f.wire_client_for(&f.sql_app).await;
    let stats = connection_stats(&f, &unread);
    let limit = unread.max_in_flight();
    let entries = vec![sql(WIDE_READ); 2 * limit]
        .into_iter()
        .map(BatchEntry::Execute)
        .collect::<Vec<_>>();
    assert!(entries.len() > limit);
    timeout(TEST_TIMEOUT, unread.send_batch(WireBatchMode::All, entries))
        .await
        .expect("send wide-read batch")
        .unwrap();
    wait_for("an outstanding write to back up the unread batch", || {
        stats.peak_in_flight() == limit
            && stats.pending_reply_bytes() > 0
            && stats.queued_requests() > 0
    })
    .await;

    timeout(TEST_TIMEOUT, async {
        for _ in 0..50 {
            execute_ok(&mut pinger, ping()).await;
        }
    })
    .await
    .expect("50 pings while the batch client is not reading");
    assert!(Arc::ptr_eq(&stats, &connection_stats(&f, &unread)));
    assert!(stats.pending_reply_bytes() > 0 && stats.queued_requests() > 0);
    let peak = stats.peak_pending_reply_bytes();
    assert!(
        peak <= 2 * MAX_RESPONSE + MAX_FRAME + HEADER_LEN,
        "peak {peak}"
    );
    drop(unread);
    wait_for(
        "the unread batch session and blocking permits to be released",
        || {
            f.service.blocking_permits_held() == 0
                && f.ordinary_api.test_registry_entry_count() == registry_baseline
        },
    )
    .await;
    pinger.goodbye().await.unwrap();
    f.stop().await;
}

async fn assert_split_rows(client: &mut Client, id: u32, expected: &[proto::SqlRow]) {
    let mut encoded = Vec::new();
    let mut more_frames = 0;
    loop {
        let (header, body) = client.recv_raw_frame().await.unwrap();
        assert_eq!((header.kind, header.request_id), (FrameKind::Reply, id));
        encoded.extend(body);
        if header.flags == Flags(0) {
            break;
        }
        assert_eq!(header.flags, Flags(Flags::MORE));
        more_frames += 1;
    }
    assert!(more_frames > 0, "expected a MORE-split batch reply");
    let reply = WireReply::decode(encoded.as_slice())
        .unwrap()
        .reply
        .unwrap();
    assert_eq!(reply.code, "ok", "{:?}", reply.message);
    assert_eq!(reply.sql.unwrap().rows, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_batch_more_split_then_small_entries_keep_order_and_skipped() {
    let f = ParityFixture::start().await;
    seed_wide_shipment_rows(&f, WIDE_ROWS);
    let mut holder = f.wire_client_for(&f.sql_app).await;
    let expected = execute_ok(&mut holder, sql(WIDE_READ))
        .await
        .sql
        .unwrap()
        .rows;
    assert_eq!(expected.len(), WIDE_ROWS as usize);
    let lock = sql("UPDATE shipments SET status='locked' WHERE merchant_id=42 AND id=1");
    let entries = [
        sql(WIDE_READ),
        ping(),
        sql("INSERT INTO shipments (merchant_id, id, status) VALUES (97, 1, 'duplicate')"),
        ping(),
        ping(),
    ];

    for mode in [WireBatchMode::StopOnError, WireBatchMode::All] {
        execute_ok(&mut holder, sql("BEGIN")).await;
        execute_ok(&mut holder, lock.clone()).await;
        let mut client = f.wire_client_for(&f.sql_app).await;
        let stats = connection_stats(&f, &client);
        let limit = client.max_in_flight();
        let (blocked, fillers, ids) = timeout(TEST_TIMEOUT, async {
            let blocked = client.send_execute(lock.clone()).await.unwrap();
            // Leave exactly three admission slots for the batch so its last
            // two entries are deferred while the lock holder keeps it queued.
            let mut fillers = Vec::new();
            for _ in 0..limit.checked_sub(4).expect("four admission slots") {
                fillers.push(client.send_execute(ping()).await.unwrap());
            }
            let batch = entries.iter().cloned().map(BatchEntry::Execute).collect();
            let ids = client.send_batch(mode, batch).await.unwrap();
            (blocked, fillers, ids)
        })
        .await
        .expect("send the blocked pipeline and split-reply batch");
        wait_for(
            "the blocked request, fillers and batch to reach the server",
            || {
                f.admin.waiting_lock_count() == 1
                    && f.service.blocking_permits_held() == 1
                    && stats.received_frames() == fillers.len() + 2
            },
        )
        .await;
        execute_ok(&mut holder, sql("ROLLBACK")).await;

        timeout(TEST_TIMEOUT, async {
            for id in std::iter::once(blocked).chain(fillers) {
                let (received, result) = client.recv().await.unwrap();
                assert_eq!(received, id);
                assert_eq!(result.unwrap().code, "ok");
            }
            assert_eq!(stats.peak_in_flight(), limit);
            assert_split_rows(&mut client, ids[0], &expected).await;
            for (index, &id) in ids.iter().enumerate().skip(1) {
                let (header, body) = client.recv_raw_frame().await.unwrap();
                assert_eq!((header.kind, header.request_id), (FrameKind::Reply, id));
                if mode == WireBatchMode::StopOnError && index >= 3 {
                    assert_eq!(header.flags, Flags(Flags::SKIPPED));
                    assert!(body.is_empty());
                } else {
                    assert_eq!(header.flags, Flags(0));
                    let reply = WireReply::decode(body.as_slice()).unwrap().reply.unwrap();
                    assert_eq!(reply.code, if index == 2 { "23505" } else { "ok" });
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out reading the {mode:?} batch replies"));
        execute_ok(&mut client, ping()).await;
        client.goodbye().await.unwrap();
    }
    holder.goodbye().await.unwrap();
    f.stop().await;
}

const ADMISSION_INSERT: &str = "insert shipments set merchant_id=95,id=$id,status=\"admitted\"";
const ADMISSION_LANGUAGE: &str = "0.18";
const ADMISSION_BUDGET_MS: u32 = 500;

fn admission_binding(value: proto::value::Value) -> Vec<proto::NativeBinding> {
    vec![proto::NativeBinding {
        name: Some("id".into()),
        value: Some(proto::Value { value: Some(value) }),
    }]
}

async fn admission_client(address: std::net::SocketAddr, role: &str) -> Client {
    let (client, _) = Client::connect(
        address,
        None,
        proto::OpenSessionRequest {
            role: Some(role.into()),
            database: Some("postgres".into()),
            ..Default::default()
        },
        giggles_server::wire::client::Credential::None,
    )
    .await
    .unwrap();
    client
}

async fn admission_handle(client: &mut Client) -> u32 {
    timeout(
        TEST_TIMEOUT,
        client.prepare(ADMISSION_INSERT, Some(ADMISSION_LANGUAGE)),
    )
    .await
    .expect("PREPARE reply")
    .unwrap()
    .unwrap()
}

async fn assert_admission_refusal(
    client: &mut Client,
    handle: u32,
    bindings: Vec<proto::NativeBinding>,
    expected: giggles_server::api::error::ApiError,
) {
    fn outcome(
        reply: Result<proto::ExecuteReply, WireConnError>,
    ) -> (FrameKind, String, Option<String>) {
        match reply {
            Ok(reply) => (FrameKind::Reply, reply.code, reply.message),
            Err(error) => (FrameKind::ConnError, error.code, Some(error.message)),
        }
    }

    let execute = proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Text(
            proto::NativeTextRequest {
                text: ADMISSION_INSERT.into(),
                language_version: Some(ADMISSION_LANGUAGE.into()),
                bindings: bindings.clone(),
            },
        )),
        budget_ms: Some(ADMISSION_BUDGET_MS),
        ..Default::default()
    };
    let (direct, prepared) = timeout(TEST_TIMEOUT, async {
        let id = client.send_execute(execute).await.unwrap();
        let (received, direct) = client.recv().await.unwrap();
        assert_eq!(received, id);
        let id = client
            .send_run(WireRun {
                handle,
                bindings,
                budget_ms: Some(ADMISSION_BUDGET_MS),
                ..Default::default()
            })
            .await
            .unwrap();
        let (received, prepared) = client.recv().await.unwrap();
        assert_eq!(received, id);
        (outcome(direct), outcome(prepared))
    })
    .await
    .expect("EXECUTE and RUN refusals");

    assert_eq!(prepared, direct, "RUN must match EXECUTE");
    assert_eq!(
        direct,
        (
            FrameKind::ConnError,
            expected.code().into(),
            Some(expected.message().into()),
        )
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_run_admission_refusals_match_execute_under_saturated_permits() {
    use agent_support::parity::APPROVER;
    use giggles_core::roles::TablePrivilege;
    use giggles_server::{
        admission::RoleStatementLimits,
        api::{ApiLimits, ApiRuntime, error::ApiError},
        auth::RoleApiConfig,
        wire::listen::serve_role_wire_bound,
    };
    use tokio::{net::TcpListener, sync::watch};

    // MAX_BLOCKING is private to the server. The observed held count below
    // must reach the full pool, as in durable_release.rs.
    const BLOCKING_CAPACITY: usize = 32;
    const LOCK_ROW: &str = "UPDATE shipments SET status='lock wait' WHERE merchant_id=42 AND id=1";
    const INSERTED: &str = "SELECT id FROM shipments WHERE merchant_id=95 ORDER BY id";

    let f = ParityFixture::start().await;
    f.admin
        .roles()
        .grant_table("shipments", APPROVER, &[TablePrivilege::Insert])
        .unwrap();
    let config: RoleApiConfig = serde_json::from_value(serde_json::json!({
        "wire": "127.0.0.1:0",
        "tls": "disabled",
        "loopback_trust": true,
        "connections": BLOCKING_CAPACITY + 4,
    }))
    .unwrap();
    let settings = Arc::new(config.build().unwrap());
    let socket = TcpListener::bind(settings.wire.unwrap()).await.unwrap();
    let runtime = ApiRuntime::new_role(
        f.service.clone(),
        ApiLimits {
            sessions: BLOCKING_CAPACITY + 4,
        },
        settings.clone(),
    )
    .unwrap();
    let (_open, gate) = watch::channel(true);
    let wire = serve_role_wire_bound(socket, runtime.clone(), settings, gate)
        .await
        .unwrap();

    let mut app = admission_client(wire.address, APP).await;
    let mut other = admission_client(wire.address, APPROVER).await;
    let app_handle = admission_handle(&mut app).await;
    let other_handle = admission_handle(&mut other).await;
    let binding = admission_binding(proto::value::Value::IntValue(1));
    let limits = |concurrent| RoleStatementLimits {
        concurrent,
        queue: 0,
        wait_ms: 10,
    };
    let mut holder = f.admin.sql_session();
    holder.execute("BEGIN", &[], None).unwrap();
    holder.execute(LOCK_ROW, &[], None).unwrap();

    f.service.set_role_statement_limits(limits(1)).unwrap();
    let mut waiter = admission_client(wire.address, APP).await;
    let waiting_id = waiter.send_execute(sql(LOCK_ROW)).await.unwrap();
    wait_for("one role permit held by a row-lock waiter", || {
        f.admin.waiting_lock_count() == 1 && f.service.blocking_permits_held() == 1
    })
    .await;
    assert_eq!(f.service.role_pool_counts(), [(0, 1, 0)]);
    assert_admission_refusal(
        &mut app,
        app_handle,
        binding.clone(),
        ApiError::ResourceExhausted,
    )
    .await;
    assert!(f.sql(INSERTED).is_empty());

    holder.execute("ROLLBACK", &[], None).unwrap();
    let (id, reply) = timeout(TEST_TIMEOUT, waiter.recv()).await.unwrap().unwrap();
    assert_eq!(id, waiting_id);
    assert_eq!(reply.unwrap().code, "ok");
    waiter.goodbye().await.unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, app.run(app_handle, binding.clone()))
            .await
            .unwrap()
            .unwrap()
            .code,
        "ok"
    );
    assert_eq!(f.sql(INSERTED), [vec![Value::Int(1)]]);
    let next_handle = admission_handle(&mut app).await;
    assert_eq!(next_handle, app_handle + 1);
    app.close_handle(next_handle).await.unwrap();

    f.service
        .set_role_statement_limits(limits(BLOCKING_CAPACITY - 1))
        .unwrap();
    holder.execute("BEGIN", &[], None).unwrap();
    holder.execute(LOCK_ROW, &[], None).unwrap();
    let mut waiters = Vec::with_capacity(BLOCKING_CAPACITY);
    for role in std::iter::repeat_n(APP, BLOCKING_CAPACITY - 1).chain([APPROVER]) {
        waiters.push(admission_client(wire.address, role).await);
    }
    for waiter in &mut waiters {
        waiter.send_execute(sql(LOCK_ROW)).await.unwrap();
    }
    wait_for("all blocking permits held by row-lock waiters", || {
        f.admin.waiting_lock_count() == BLOCKING_CAPACITY
            && f.service.blocking_permits_held() == BLOCKING_CAPACITY
    })
    .await;
    let mut role_pools = f.service.role_pool_counts();
    role_pools.sort_unstable();
    assert_eq!(
        role_pools,
        [
            (0, BLOCKING_CAPACITY - 1, 0),
            (BLOCKING_CAPACITY - 2, BLOCKING_CAPACITY - 1, 0)
        ]
    );

    // APPROVER still has room in its role pool. APP has neither a role
    // statement permit nor a blocking permit.
    let binding = admission_binding(proto::value::Value::IntValue(2));
    assert_admission_refusal(
        &mut other,
        other_handle,
        binding.clone(),
        ApiError::DeadlineExceeded,
    )
    .await;
    assert_admission_refusal(
        &mut app,
        app_handle,
        binding.clone(),
        ApiError::ResourceExhausted,
    )
    .await;
    // Float8 is valid on the wire but unsupported as a native 0.18 binding.
    let bad_binding = admission_binding(proto::value::Value::Float8Bits(1.25_f64.to_bits()));
    assert_admission_refusal(&mut app, app_handle, bad_binding, ApiError::InvalidArgument).await;
    assert_eq!(f.admin.waiting_lock_count(), BLOCKING_CAPACITY);
    assert_eq!(f.service.blocking_permits_held(), BLOCKING_CAPACITY);

    drop(waiters);
    wait_for(
        "all disconnected lock waiters and their permits to be released",
        || f.admin.waiting_lock_count() == 0 && f.service.blocking_permits_held() == 0,
    )
    .await;
    holder.execute("ROLLBACK", &[], None).unwrap();
    drop(holder);
    assert_eq!(f.sql(INSERTED), [vec![Value::Int(1)]]);
    assert_eq!(
        timeout(TEST_TIMEOUT, other.run(other_handle, binding))
            .await
            .unwrap()
            .unwrap()
            .code,
        "ok"
    );
    assert_eq!(f.sql(INSERTED), [vec![Value::Int(1)], vec![Value::Int(2)]]);

    app.goodbye().await.unwrap();
    other.goodbye().await.unwrap();
    wire.shutdown().await.unwrap();
    runtime.shutdown().await;
    f.stop().await;
}

fn marked_wire_spill(marker: &str) -> Option<std::path::PathBuf> {
    use std::{fs, io::Read};

    let prefix = format!("giggles-returning-{}-", std::process::id());
    let mut files = fs::read_dir(std::env::temp_dir())
        .unwrap()
        .flatten()
        .filter_map(|entry| {
            if !entry.file_name().to_string_lossy().starts_with(&prefix) {
                return None;
            }
            let path = entry.path();
            let mut bytes = [0; 2048];
            let len = fs::File::open(&path).ok()?.read(&mut bytes).ok()?;
            bytes[..len]
                .windows(marker.len())
                .any(|window| window == marker.as_bytes())
                .then_some(path)
        });
    let path = files.next();
    assert!(
        files.next().is_none(),
        "more than one spill matched {marker}"
    );
    path
}

async fn send_spill_query(stream: &mut TlsStream<TcpStream>, id: u32, query: &str) {
    let mut bytes = Vec::new();
    encode_message(&mut bytes, FrameKind::Execute, Flags(0), id, &sql(query));
    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();
}

async fn spill_shape_head(
    stream: &mut TlsStream<TcpStream>,
    decoder: &mut FrameDecoder,
    id: u32,
) -> proto::WireShapeRef {
    let (header, body) = timeout(TEST_TIMEOUT, read_frame(stream, decoder))
        .await
        .expect("spill reply head")
        .unwrap();
    assert_eq!(
        (header.kind, header.flags, header.request_id),
        (FrameKind::Reply, Flags(Flags::MORE), id)
    );
    let head = WireReply::decode(body.as_slice()).unwrap();
    let reply = head.reply.unwrap();
    assert_eq!(reply.code, "ok", "{:?}", reply.message);
    let sql = reply.sql.unwrap();
    assert!(sql.columns.is_empty() && sql.rows.is_empty());
    let [shape] = head.shapes.try_into().expect("one SQL shape definition");
    shape
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_spill_read_failure_after_shape_head_rolls_back_shape() {
    use giggles_server::api::error::ApiError;

    let f = ParityFixture::start().await;
    seed_wide_shipments(&f);
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let marker = format!("wire-spill-fault-{}", hex::encode(nonce));
    let query = format!(
        "SELECT id, '{marker}' || status AS spill_fault_body \
         FROM shipments WHERE merchant_id=97 ORDER BY id"
    );
    let registry_baseline = f.ordinary_api.test_registry_entry_count();
    let blocking_baseline = f.service.blocking_permits_held();
    let (mut stream, mut decoder) = timeout(TEST_TIMEOUT, backpressured_wire(&f))
        .await
        .expect("wire handshake");
    let peer = stream.get_ref().0.local_addr().unwrap();
    let stats = f.ordinary_api.test_wire_connection(peer).unwrap();

    let id = 2;
    send_spill_query(&mut stream, id, &query).await;
    let shape = spill_shape_head(&mut stream, &mut decoder, id).await;
    assert!(shape.shape_id.is_some(), "{shape:?}");
    assert_eq!(shape.position, 0);
    assert_eq!(shape.columns, ["id", "spill_fault_body"]);

    // Stop reading at the head. The 48 MiB reply exceeds the pending cap
    // and socket buffers, leaving the spill producer blocked with rows left.
    wait_for("the spill reply to reach the pending cap", || {
        stats.peak_pending_reply_bytes() >= 2 * MAX_RESPONSE
    })
    .await;
    let spill = marked_wire_spill(&marker).expect("fixture's live reply spill");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&spill)
        .unwrap()
        .set_len(0)
        .unwrap();

    let stream_timeout = Duration::from_secs(10);
    timeout(stream_timeout, async {
        loop {
            let (header, body) = read_frame(&mut stream, &mut decoder).await.unwrap();
            assert_eq!(header.request_id, id);
            if header.kind == FrameKind::ConnError {
                assert_eq!(header.flags, Flags(0));
                let error = WireConnError::decode(body.as_slice()).unwrap();
                assert_eq!(
                    (error.code.as_str(), error.message.as_str()),
                    (ApiError::Internal.code(), ApiError::Internal.message())
                );
                break;
            }
            assert_eq!(
                (header.kind, header.flags),
                (FrameKind::Reply, Flags(Flags::MORE)),
                "a failed spill must not complete its REPLY"
            );
        }
    })
    .await
    .expect("terminal spill read error");
    wait_for("the truncated spill to be removed", || !spill.exists()).await;

    let retry = id + 1;
    send_spill_query(&mut stream, retry, &query).await;
    assert_eq!(
        spill_shape_head(&mut stream, &mut decoder, retry).await,
        shape
    );
    timeout(stream_timeout, async {
        loop {
            let (header, _) = read_frame(&mut stream, &mut decoder).await.unwrap();
            assert_eq!((header.kind, header.request_id), (FrameKind::Reply, retry));
            if header.flags == Flags(0) {
                break;
            }
            assert_eq!(header.flags, Flags(Flags::MORE));
        }
    })
    .await
    .expect("complete retried reply on the same connection");
    drop(stream);

    let mut client = f.wire_client_for(&f.sql_app).await;
    let reply = timeout(stream_timeout, client.execute(sql(&query)))
        .await
        .expect("client decodes the full spill reply")
        .unwrap();
    assert_eq!(reply.code, "ok", "{:?}", reply.message);
    let sql = reply.sql.unwrap();
    assert_eq!(sql.columns, shape.columns);
    assert_eq!(sql.rows.len(), 1024);
    client.goodbye().await.unwrap();
    wait_for(
        "wire registry and blocking permits to return to baseline",
        || {
            f.ordinary_api.test_registry_entry_count() == registry_baseline
                && f.service.blocking_permits_held() == blocking_baseline
        },
    )
    .await;
    f.stop().await;
}
