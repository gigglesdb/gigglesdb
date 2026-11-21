#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::parity::{APP, ParityFixture, grpc_client, http_call, with_session};
use giggles_core::relational::relation_key;
use giggles_server::{
    Binding, IO_TIMEOUT, MAX_FRAME, MAX_RESPONSE, Role, Service,
    admission::ServiceLimits,
    api::{ApiLimits, ApiRuntime, json as wire_json, proto},
    clock::Clock,
    wire::{
        client::{Client, ClientTls, Credential, WireError},
        frame::{Flags, FrameKind, HEADER_LEN, PREFACE_LEN},
        listen::{ConnectionStats, serve_wire_bound},
        messages::{WireConnError, WireReply},
    },
};
use prost::Message;
use rustls::pki_types::ServerName;
use serde_json::{Value, json};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::{Instant, sleep, timeout, timeout_at},
};
use tokio_rustls::TlsConnector;

fn execute(request: Value) -> proto::ExecuteRequest {
    wire_json::decode_execute(&serde_json::to_vec(&request).unwrap()).unwrap()
}

fn ping() -> proto::ExecuteRequest {
    execute(json!({"ping": {}}))
}

fn sql(statement: &str) -> proto::ExecuteRequest {
    execute(json!({"sql": {"sql": statement}}))
}

fn text(statement: &str) -> proto::ExecuteRequest {
    execute(json!({"text": {"text": statement, "languageVersion": "0.18"}}))
}

fn wire_error(error: &io::Error) -> &WireError {
    assert_eq!(error.kind(), io::ErrorKind::Other);
    error
        .get_ref()
        .unwrap()
        .downcast_ref::<WireError>()
        .unwrap()
}

async fn wait_for(reason: &str, mut condition: impl FnMut() -> bool) {
    timeout(Duration::from_secs(5), async {
        while !condition() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {reason}"));
}

fn connection_stats(f: &ParityFixture, client: &Client) -> Arc<ConnectionStats> {
    f.ordinary_api
        .test_wire_connection(client.local_addr())
        .expect("stats for the authenticated wire connection")
}

async fn ok_sql(client: &mut Client, statement: &str) -> proto::ExecuteReply {
    let reply = client.execute(sql(statement)).await.unwrap();
    assert_eq!(reply.code, "ok", "{statement}: {reply:?}");
    reply
}

fn insert_shipment(id: i64) -> proto::ExecuteRequest {
    sql(&format!(
        "INSERT INTO shipments (merchant_id, id, status) VALUES (96, {id}, 'pipeline')"
    ))
}

fn seed_wide_shipments(f: &ParityFixture, rows: i64) {
    use giggles_core::relational::Value as SqlValue;
    let mut session = f.admin.sql_session();
    let insert = session
        .prepare("INSERT INTO shipments (merchant_id, id, status) VALUES (97, $1, $2)")
        .unwrap();
    let payload = SqlValue::Text("w".repeat(16_384));
    session.execute("BEGIN", &[], None).unwrap();
    for id in 1..=rows {
        session
            .execute_prepared(&insert, &[SqlValue::Int(id), payload.clone()], None)
            .unwrap();
    }
    session.execute("COMMIT", &[], None).unwrap();
}

#[tokio::test]
async fn wire_disconnect_mid_pipeline_rolls_back() {
    let f = ParityFixture::start().await;
    for blocked in [false, true] {
        let mut holder = if blocked {
            let mut holder = f.wire_client_for(&f.sql_app).await;
            ok_sql(&mut holder, "BEGIN").await;
            assert_eq!(
                holder.execute(insert_shipment(101)).await.unwrap().code,
                "ok"
            );
            Some(holder)
        } else {
            None
        };

        let mut client = f.wire_client_for(&f.sql_app).await;
        ok_sql(&mut client, "BEGIN").await;
        assert_eq!(
            client.execute(insert_shipment(100)).await.unwrap().code,
            "ok"
        );
        let stats = connection_stats(&f, &client);
        if !blocked {
            stats.hold_queued_requests();
        }
        for id in 101..111 {
            client.send_execute(insert_shipment(id)).await.unwrap();
        }
        if blocked {
            wait_for("a running insert and nine queued inserts", || {
                f.admin.waiting_lock_count() == 1
                    && f.service.blocking_permits_held() == 1
                    && stats.queued_requests() == 9
            })
            .await;
        } else {
            wait_for("all ten inserts to queue", || stats.queued_requests() == 10).await;
        }
        drop(client);

        if let Some(mut holder) = holder.take() {
            // The disconnected request must clean up while the lock is still held.
            wait_for(
                "the disconnected waiter to release its session and permit",
                || {
                    f.admin.waiting_lock_count() == 0
                        && f.service.blocking_permits_held() == 0
                        && f.ordinary_api.test_registry_entry_count() == 1
                },
            )
            .await;
            ok_sql(&mut holder, "ROLLBACK").await;
            holder.goodbye().await.unwrap();
        }
        let mut fresh = f.wire_client_for(&f.sql_app).await;
        let selected = ok_sql(&mut fresh, "SELECT id FROM shipments WHERE merchant_id=96").await;
        assert!(selected.sql.unwrap().rows.is_empty(), "blocked={blocked}");
        fresh.goodbye().await.unwrap();
        wait_for(
            "all blocking permits returned and the ordinary registry empty",
            || {
                f.service.blocking_permits_held() == 0
                    && f.ordinary_api.test_registry_entry_count() == 0
            },
        )
        .await;
    }
    f.stop().await;
}

#[tokio::test]
async fn wire_never_reading_client_is_capped() {
    let f = ParityFixture::start().await;
    seed_wide_shipments(&f, 64);
    let mut unread = f.wire_client_for(&f.sql_app).await;
    let stats = connection_stats(&f, &unread);
    let read = sql("SELECT id, status FROM shipments WHERE merchant_id=97 ORDER BY id");
    let mut pinger = f.wire_client(false).await;
    for _ in 0..200 {
        unread.send_execute(read.clone()).await.unwrap();
    }
    wait_for(
        "the unread connection to reach the pending reply cap",
        || stats.peak_pending_reply_bytes() >= 2 * MAX_RESPONSE,
    )
    .await;
    for _ in 0..50 {
        assert_eq!(pinger.execute(ping()).await.unwrap().code, "ok");
    }
    assert!(Arc::ptr_eq(&stats, &connection_stats(&f, &unread)));
    let peak = stats.peak_pending_reply_bytes();
    assert!(
        peak <= 2 * MAX_RESPONSE + MAX_FRAME + HEADER_LEN,
        "peak {peak}"
    );
    let frames = stats.peak_in_flight();
    assert!(
        (1..=unread.max_in_flight()).contains(&frames),
        "parsed {frames} outstanding frames, advertised limit {}",
        unread.max_in_flight()
    );
    drop(unread);
    pinger.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn wire_large_result_more_frames_then_small_reply() {
    let f = ParityFixture::start().await;
    seed_wide_shipments(&f, 192);
    let read = sql("SELECT id, status FROM shipments WHERE merchant_id=97 ORDER BY id");
    let mut grpc = grpc_client(f.grpc.address, &f.ca, &f.sql_app).await;
    let token = grpc
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;
    let mut stream = grpc
        .execute_stream(with_session(&token, read.clone()))
        .await
        .unwrap()
        .into_inner();
    let mut expected = stream.message().await.unwrap().expect("gRPC stream head");
    assert_eq!(expected.code, "ok");
    let mut chunks = 0;
    while let Some(chunk) = stream.message().await.unwrap() {
        expected
            .sql
            .as_mut()
            .unwrap()
            .rows
            .extend(chunk.sql.unwrap().rows);
        chunks += 1;
    }
    assert!(chunks > 1);
    assert_eq!(expected.sql.as_ref().unwrap().rows.len(), 192);
    grpc.close_session(with_session(&token, proto::CloseSessionRequest {}))
        .await
        .unwrap();

    let mut client = f.wire_client_for(&f.sql_app).await;
    let large = client.send_execute(read.clone()).await.unwrap();
    let small = client.send_execute(ping()).await.unwrap();
    let mut frames = Vec::new();
    let mut encoded = Vec::new();
    loop {
        let (frame, body) = client.recv_raw_frame().await.unwrap();
        assert_eq!((frame.request_id, frame.kind), (large, FrameKind::Reply));
        encoded.extend(body);
        frames.push(frame.flags);
        if frame.flags.0 & Flags::MORE == 0 {
            break;
        }
    }
    let (last, more) = frames.split_last().unwrap();
    assert!(!more.is_empty());
    assert!(more.iter().all(|flags| *flags == Flags(Flags::MORE)));
    assert_eq!(*last, Flags(0));
    assert_eq!(
        WireReply::decode(encoded.as_slice())
            .unwrap()
            .reply
            .unwrap(),
        expected
    );
    let (id, result) = client.recv().await.unwrap();
    assert_eq!(id, small);
    assert_eq!(result.unwrap().code, "ok");

    connection_stats(&f, &client).hold_reply_after_first_more();
    let large = client.send_execute(read).await.unwrap();
    let (first, _) = client.recv_raw_frame().await.unwrap();
    assert_eq!(
        (first.request_id, first.kind, first.flags),
        (large, FrameKind::Reply, Flags(Flags::MORE))
    );
    client.send_cancel(large).await.unwrap();
    let small = client.send_execute(ping()).await.unwrap();
    let (last, body) = client.recv_raw_frame().await.unwrap();
    assert_eq!(
        (last.request_id, last.kind, last.flags),
        (large, FrameKind::ConnError, Flags(0))
    );
    assert_eq!(
        WireConnError::decode(body.as_slice()).unwrap().code,
        "DEADLINE_EXCEEDED"
    );
    let (id, result) = client.recv().await.unwrap();
    assert_eq!(id, small);
    assert_eq!(result.unwrap().code, "ok");
    client.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn wire_cancel_running_request() {
    let f = ParityFixture::start().await;
    let mut holder = f.wire_client_for(&f.sql_app).await;
    let mut waiter = f.wire_client_for(&f.sql_app).await;
    let statement = "UPDATE shipments SET status='cancelled' WHERE merchant_id=42 AND id=1";
    ok_sql(&mut holder, "BEGIN").await;
    ok_sql(&mut holder, statement).await;

    let mut grpc = grpc_client(f.grpc.address, &f.ca, &f.sql_app).await;
    let token = grpc
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;
    let mut expiring = sql(statement);
    expiring.budget_ms = Some(1000);
    let (expected, ()) = tokio::join!(
        grpc.execute(with_session(&token, expiring)),
        wait_for("the gRPC update to wait on the row lock", || f
            .admin
            .waiting_lock_count()
            == 1)
    );
    let expected = expected.unwrap().into_inner();
    assert_eq!(expected.code, "57014");
    grpc.close_session(with_session(&token, proto::CloseSessionRequest {}))
        .await
        .unwrap();

    let id = waiter.send_execute(sql(statement)).await.unwrap();
    wait_for("the wire update to wait on the row lock", || {
        f.admin.waiting_lock_count() == 1
    })
    .await;
    waiter.send_cancel(id).await.unwrap();
    let (received, result) = timeout(Duration::from_secs(5), waiter.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received, id);
    assert_eq!(result.unwrap(), expected);
    assert_eq!(f.admin.waiting_lock_count(), 0);
    assert_eq!(waiter.execute(ping()).await.unwrap().code, "ok");
    ok_sql(&mut holder, "ROLLBACK").await;
    waiter.goodbye().await.unwrap();
    holder.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn wire_binding_reload_ends_session() {
    let f = ParityFixture::start().await;
    let mut client = f.wire_client(false).await;
    let (status, opened) = http_call(
        f.http.address,
        (&f.ca, &f.app),
        "POST",
        "/v1/sessions",
        None,
        Some(&json!({})),
    )
    .await;
    assert_eq!(status, 201);
    let token = opened["sessionId"].as_str().unwrap();
    assert_eq!(client.execute(ping()).await.unwrap().code, "ok");

    f.service.reload_bindings(vec![]).unwrap();
    f.ordinary_api.invalidate_bindings();
    let (status, http) = http_call(
        f.http.address,
        (&f.ca, &f.app),
        "POST",
        "/v1/session/execute",
        Some(token),
        Some(&json!({"ping": {}})),
    )
    .await;
    assert_eq!(status, 401);
    assert_eq!(http["error"]["code"], "UNAUTHENTICATED");
    let error = read_fatal(&mut client).await;
    assert_eq!(error.code, http["error"]["code"]);
    assert_eq!(error.message, http["error"]["message"]);
    f.stop().await;
}

const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

async fn expect_closed(client: &mut Client) {
    let error = timeout(CLOSE_TIMEOUT, client.recv_raw_frame())
        .await
        .expect("server closes the connection")
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
}

async fn read_fatal(client: &mut Client) -> WireConnError {
    let (header, body) = timeout(CLOSE_TIMEOUT, client.recv_raw_frame())
        .await
        .expect("server sends an idle connection error")
        .unwrap();
    assert_eq!(
        (header.kind, header.flags, header.request_id),
        (FrameKind::ConnError, Flags(0), 0)
    );
    expect_closed(client).await;
    WireConnError::decode(body.as_slice()).unwrap()
}

#[tokio::test]
async fn wire_cancel_with_full_pipeline() {
    let f = ParityFixture::start().await;
    let mut holder = f.wire_client_for(&f.sql_app).await;
    let statement = "UPDATE shipments SET status='locked' WHERE merchant_id=42 AND id=1";
    ok_sql(&mut holder, "BEGIN").await;
    ok_sql(&mut holder, statement).await;

    for disconnect in [false, true] {
        let mut waiter = f.wire_client_for(&f.sql_app).await;
        let stats = connection_stats(&f, &waiter);
        let peer = waiter.local_addr();
        assert_eq!(waiter.max_in_flight(), 64);
        let deadline = Instant::now() + CLOSE_TIMEOUT;
        let id = waiter.send_execute(sql(statement)).await.unwrap();
        assert_eq!(id, 1);
        for _ in 1..64 {
            waiter.send_execute(ping()).await.unwrap();
        }
        wait_for("a locked request and 63 queued requests", || {
            f.admin.waiting_lock_count() == 1 && stats.queued_requests() == 63
        })
        .await;

        if disconnect {
            drop(waiter);
        } else {
            timeout_at(deadline, async {
                waiter.send_cancel(id).await.unwrap();
                let (received, result) = waiter.recv().await.unwrap();
                assert_eq!(received, id);
                assert_eq!(result.unwrap().code, "57014");
                for id in 2..=64 {
                    let (received, result) = waiter.recv().await.unwrap();
                    assert_eq!(received, id);
                    assert_eq!(result.unwrap().code, "ok");
                }
                waiter.goodbye().await.unwrap();
            })
            .await
            .expect("CANCEL drains the full pipeline before the lock is released");
        }
        wait_for(
            "the full pipeline to return its session and blocking permit",
            || {
                f.admin.waiting_lock_count() == 0
                    && f.service.blocking_permits_held() == 0
                    && f.ordinary_api.test_registry_entry_count() == 1
                    && f.ordinary_api.test_wire_connection(peer).is_none()
            },
        )
        .await;
    }

    ok_sql(&mut holder, "ROLLBACK").await;
    holder.goodbye().await.unwrap();
    f.stop().await;
}

async fn connect_with_certificate(
    address: std::net::SocketAddr,
    ca: &agent_support::TestCert,
    cert: Option<&agent_support::TestCert>,
) -> io::Result<Client> {
    let tls = ClientTls {
        ca: ca.cert.pem().into_bytes(),
        cert: cert.map(|cert| cert.cert.pem().into_bytes()),
        key: cert.map(|cert| cert.key.serialize_pem().into_bytes()),
        server_name: "localhost".into(),
    };
    Client::connect(
        address,
        Some(tls),
        proto::OpenSessionRequest::default(),
        Credential::None,
    )
    .await
    .map(|(client, _)| client)
}

#[tokio::test]
async fn wire_idle_connection_ends_after_binding_revoked() {
    let f = ParityFixture::start().await;
    let binding = Binding {
        fingerprint: f.app.fingerprint(),
        role: Role::Database { name: APP.into() },
    };
    let service = Service::with_limits(
        f.admin.clone(),
        vec![binding.clone()],
        Clock::new(0, 3600).unwrap(),
        ServiceLimits {
            sessions: 1,
            ..ServiceLimits::default()
        },
    )
    .unwrap();
    let runtime = ApiRuntime::new(service.clone(), ApiLimits::default()).unwrap();
    let (_start, gate) = watch::channel(true);
    let wire = serve_wire_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        runtime.clone(),
        f.tls.clone(),
        1,
        gate,
    )
    .await
    .unwrap();
    let mut client = connect_with_certificate(wire.address, &f.ca, Some(&f.app))
        .await
        .unwrap();
    assert!(
        connect_with_certificate(wire.address, &f.ca, Some(&f.app))
            .await
            .is_err()
    );

    service.reload_bindings(vec![]).unwrap();
    assert_eq!(read_fatal(&mut client).await.code, "UNAUTHENTICATED");
    wait_for(
        "the revoked connection and its registry entry to be released",
        || {
            runtime.test_wire_connection(client.local_addr()).is_none()
                && runtime.test_registry_entry_count() == 0
        },
    )
    .await;
    service.reload_bindings(vec![binding]).unwrap();
    connect_with_certificate(wire.address, &f.ca, Some(&f.app))
        .await
        .expect("both single-connection permits are available")
        .goodbye()
        .await
        .unwrap();
    wire.shutdown().await.unwrap();
    runtime.shutdown().await;
    f.stop().await;
}

async fn expect_request_revocation(request: proto::ExecuteRequest) {
    let f = ParityFixture::start().await;
    let mut client = f.wire_client(false).await;
    // Hold only idle checks so the request path observes the revocation.
    connection_stats(&f, &client).hold_idle_checks();
    f.service.reload_bindings(vec![]).unwrap();
    let id = client.send_execute(request).await.unwrap();
    let (received, error) = timeout(CLOSE_TIMEOUT, client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received, id);
    assert_eq!(error.unwrap_err().code, "UNAUTHENTICATED");
    expect_closed(&mut client).await;
    f.stop().await;
}

#[tokio::test]
async fn wire_binding_reload_ends_session_without_sweep() {
    expect_request_revocation(ping()).await;
}

#[tokio::test]
async fn wire_revoked_connection_malformed_request_still_unauthenticated() {
    expect_request_revocation(proto::ExecuteRequest {
        budget_ms: Some(10_001),
        ..ping()
    })
    .await;
}

#[tokio::test]
async fn wire_listeners_reject_missing_bindings() {
    let f = ParityFixture::start().await;
    let unbound = agent_support::certificate(Some(&f.ca), false);
    for (address, cert, runtime) in [
        (f.wire.address, Some(&unbound), &f.ordinary_api),
        (f.agent_wire.address, None, &f.agent_api),
    ] {
        let error = connect_with_certificate(address, &f.ca, cert)
            .await
            .err()
            .expect("unbound client cannot obtain a session");
        assert_eq!(wire_error(&error).code, "UNAUTHENTICATED");
        assert_eq!(runtime.test_registry_entry_count(), 0);
    }
    f.stop().await;
}

#[tokio::test]
async fn wire_ping_and_sql_round_trip() {
    let f = ParityFixture::start().await;
    let mut client = f.wire_client_for(&f.sql_app).await;
    assert_eq!(client.execute(ping()).await.unwrap().code, "ok");
    let reply = client
        .execute(sql(
            "SELECT id AS answer FROM shipments WHERE merchant_id=42 AND id=1",
        ))
        .await
        .unwrap();
    assert_eq!(reply.code, "ok");
    let result = reply.sql.unwrap();
    assert_eq!(result.columns, ["answer"]);
    assert_eq!(
        result.rows[0].values[0].value,
        Some(proto::value::Value::IntValue(1))
    );
    let replies = client.pipeline(vec![ping(), ping()]).await.unwrap();
    assert_eq!(replies.len(), 2);
    assert!(replies.iter().all(|reply| reply.code == "ok"));
    client.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn wire_execute_equivalence_matrix() {
    // Each transport runs the same mutations and sequence calls on its own database.
    let wire = ParityFixture::start().await;
    let grpc = ParityFixture::start().await;
    for f in [&wire, &grpc] {
        f.sql(&format!("CREATE SCHEMA parity AUTHORIZATION {APP}"));
        f.sql("CREATE TABLE parity.items (id BIGINT PRIMARY KEY, body TEXT)");
        f.sql("INSERT INTO parity.items VALUES (1, 'seed'), (2, 'delete in transaction')");
        f.sql("CREATE SEQUENCE parity.ids");
        let roles = f.admin.roles();
        for name in ["items", "ids"] {
            roles
                .set_table_owner(&relation_key("parity", name), APP)
                .unwrap();
        }
    }

    let mut sql_client = wire.wire_client_for(&wire.sql_app).await;
    let mut sql_reference = grpc_client(grpc.grpc.address, &grpc.ca, &grpc.sql_app).await;
    let sql_token = sql_reference
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;
    for (name, request, ok) in [
        ("ping", ping(), true),
        (
            "sql select",
            sql("SELECT status FROM shipments WHERE merchant_id=42 AND id=1"),
            true,
        ),
        (
            "sql insert",
            sql("INSERT INTO shipments (merchant_id, id, status) VALUES (42, 100, 'queued')"),
            true,
        ),
        (
            "sql error",
            sql("SELECT * FROM missing_wire_table"),
            false,
        ),
    ] {
        let expected = sql_reference
            .execute(with_session(&sql_token, request.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(expected.code == "ok", ok, "{name} {expected:?}");
        assert_eq!(
            sql_client.execute(request).await.unwrap(),
            expected,
            "{name}"
        );
    }
    sql_client.goodbye().await.unwrap();

    let mut client = wire.wire_client(false).await;
    let mut reference = grpc_client(grpc.grpc.address, &grpc.ca, &grpc.app).await;
    let token = reference
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;

    let cases = [
        (
            "native read",
            text("parity.items where id=1 select id,body"),
            "ok",
        ),
        (
            "native insert",
            text("insert parity.items set id=3,body=\"native\""),
            "ok",
        ),
        (
            "native update",
            text("update parity.items where id=3 set body=\"changed\""),
            "ok",
        ),
        (
            "native delete",
            text("delete parity.items where id=3"),
            "ok",
        ),
        (
            "native transaction",
            text("transaction { insert parity.items set id=4; delete parity.items where id=2 }"),
            "ok",
        ),
        (
            "typed read",
            execute(json!({"typedRead": {"table": "parity.items",
                "projection": [{"expression": {"column": {"name": "id"}}}],
                "orderBy": [{"expression": {"column": {"name": "id"}}, "descending": false}]
            }})),
            "ok",
        ),
        (
            "create table",
            execute(
                json!({"catalog": {"createTable": {"table": "parity.created",
                    "columns": [{"name": "id", "dataType": "BigInt", "notNull": true}],
                    "primaryKey": ["id"]
                }}}),
            ),
            "ok",
        ),
        (
            "drop table",
            execute(json!({"catalog": {"dropTable": {"table": "parity.created"}}})),
            "ok",
        ),
        (
            "nextval",
            execute(json!({"sequence": {"nextval": {"name": "parity.ids"}}})),
            "ok",
        ),
        ("show search path", execute(json!({"searchPath": {}})), "ok"),
        ("begin", execute(json!({"begin": {}})), "ok"),
        (
            "savepoint",
            execute(json!({"savepoint": {"name": "s"}})),
            "ok",
        ),
        (
            "rollback to",
            execute(json!({"rollbackTo": {"name": "s"}})),
            "ok",
        ),
        ("release", execute(json!({"release": {"name": "s"}})), "ok"),
        ("commit", execute(json!({"commit": {}})), "ok"),
        ("begin again", execute(json!({"begin": {}})), "ok"),
        ("rollback", execute(json!({"rollback": {}})), "ok"),
    ];
    for (name, request, code) in cases {
        let expected = reference
            .execute(with_session(&token, request.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(expected.code, code, "{name} {expected:?}");
        let mut actual = client.execute(request).await.unwrap();
        let mut expected = expected;
        // Both transports run against one database, so the second run of a DDL
        // request sees a later catalog epoch; compare it as an ordering instead.
        let mut epochs = [0u64; 2];
        for (slot, reply) in [&mut expected, &mut actual].into_iter().enumerate() {
            if let Some(proto::execute_reply::NativeResult::CatalogOutcome(outcome)) =
                &mut reply.native_result
            {
                epochs[slot] = std::mem::take(&mut outcome.catalog_epoch);
            }
        }
        assert!(epochs[1] >= epochs[0], "{name} catalog epoch");
        assert_eq!(actual, expected, "{name}");
    }

    for request in [
        proto::ExecuteRequest {
            budget_ms: Some(10_001),
            ..ping()
        },
        proto::ExecuteRequest::default(),
    ] {
        let expected = reference
            .execute(with_session(&token, request.clone()))
            .await
            .unwrap_err();
        assert_eq!(expected.code(), tonic::Code::InvalidArgument);
        let actual = client.execute(request).await.unwrap_err();
        assert_eq!(wire_error(&actual).code, "INVALID_ARGUMENT");
        assert_eq!(wire_error(&actual).message, expected.message());
    }
    assert_eq!(client.execute(ping()).await.unwrap().code, "ok");
    client.goodbye().await.unwrap();
    wire.stop().await;
    grpc.stop().await;
}

#[tokio::test]
async fn wire_pipelines_64_in_order() {
    let f = ParityFixture::start().await;
    let mut client = f.wire_client(false).await;
    for id in 1..=64 {
        assert_eq!(client.send_execute(ping()).await.unwrap(), id);
    }
    for id in 1..=64 {
        let (actual, reply) = client.recv().await.unwrap();
        assert_eq!(actual, id);
        assert_eq!(reply.unwrap().code, "ok");
    }
    client.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn wire_session_counts_against_slots() {
    let f = ParityFixture::start_with_api_limits(ApiLimits { sessions: 2 }).await;
    let first = f.wire_client(false).await;
    let second = f.wire_client(false).await;
    let error = f
        .try_wire_client(false)
        .await
        .err()
        .expect("third wire open");
    let (status, http) = http_call(
        f.http.address,
        (&f.ca, &f.app),
        "POST",
        "/v1/sessions",
        None,
        Some(&json!({})),
    )
    .await;
    assert_eq!(status, 429);
    assert_eq!(http["error"]["code"], "RESOURCE_EXHAUSTED");
    assert_eq!(wire_error(&error).code, http["error"]["code"]);
    assert_eq!(wire_error(&error).message, http["error"]["message"]);
    first.goodbye().await.unwrap();
    second.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn wire_agent_forms_only_on_agent_listener() {
    let f = ParityFixture::start().await;
    let request = execute(json!({"taskPreview": {"text": {
        "text": "update shipments where id=1 set status=\"queued\""
    }}}));

    let mut ordinary = grpc_client(f.grpc.address, &f.ca, &f.app).await;
    let token = ordinary
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;
    let refused = ordinary
        .execute(with_session(&token, request.clone()))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    let mut client = f.wire_client(false).await;
    let error = client.execute(request.clone()).await.unwrap_err();
    assert_eq!(wire_error(&error).code, "INVALID_ARGUMENT");
    assert_eq!(wire_error(&error).message, refused.message());
    client.goodbye().await.unwrap();

    let token = f.open_grpc().await.unwrap();
    let mut expected = grpc_client(f.agent_grpc.address, &f.ca, &f.agent)
        .await
        .execute(with_session(&token, request.clone()))
        .await
        .unwrap()
        .into_inner();
    let mut agent = f.wire_client(true).await;
    let mut actual = agent.execute(request).await.unwrap();

    // Each preview has its own change ID, digest and expiry.
    use proto::value::Value::{IntValue, TextValue, TimestamptzBinary};
    for reply in [&mut expected, &mut actual] {
        assert_eq!(reply.code, "ok", "{reply:?}");
        let result = reply.sql.as_mut().unwrap();
        assert_eq!(
            result.columns,
            ["change_id", "digest", "row_count", "expires_at"]
        );
        assert_eq!(result.rows.len(), 1);
        let values = &mut result.rows[0].values;
        assert_eq!(values.len(), 4);
        assert!(matches!(values[0].value.take(), Some(IntValue(id)) if id > 0));
        assert!(matches!(values[1].value.take(), Some(TextValue(digest))
            if hex::decode(&digest).is_ok_and(|digest| digest.len() == 32)));
        assert_eq!(values[2].value, Some(IntValue(1)));
        assert!(
            matches!(values[3].value.take(), Some(TimestamptzBinary(bytes)) if bytes.len() == 8)
        );
    }
    assert_eq!(actual, expected);
    agent.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn wire_bad_preface_inside_tls_closes() {
    let f = ParityFixture::start().await;
    let config = Arc::new(f.app.client(&f.ca));
    let socket = TcpStream::connect(f.wire.address).await.unwrap();
    let mut stream = TlsConnector::from(config)
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap();
    stream.write_all(&[b'x'; PREFACE_LEN]).await.unwrap();
    stream.flush().await.unwrap();

    let mut received = Vec::new();
    let closed = timeout(IO_TIMEOUT, stream.take(1024).read_to_end(&mut received))
        .await
        .expect("server closes after a bad preface");
    match closed {
        Ok(bytes) => assert!(bytes < 1024, "server must close"),
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof),
    }
    let mut decoder = giggles_server::wire::frame::FrameDecoder::new(
        giggles_server::wire::frame::Direction::ServerToClient,
        giggles_server::MAX_FRAME,
    );
    decoder.read_buf().extend_from_slice(&received);
    let (header, body) = decoder.next_frame().unwrap().unwrap();
    assert_eq!(
        header.kind,
        giggles_server::wire::frame::FrameKind::ConnError
    );
    assert_eq!(header.request_id, 0);
    let error = giggles_server::wire::messages::WireConnError::decode(decoder.body(body)).unwrap();
    assert_eq!(error.code, "protocol");
    assert!(decoder.next_frame().unwrap().is_none());
    f.stop().await;
}
