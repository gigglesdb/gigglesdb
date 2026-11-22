#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::parity::{APP, ParityFixture};
use giggles_core::{relational::relation_key, roles::NewRole};
use giggles_server::{
    Binding, IO_TIMEOUT, MAX_FRAME, Role, Service,
    admission::ServiceLimits,
    api::{ApiLimits, ApiRuntime, json as wire_json, proto},
    auth::scram::Verifier,
    clock::Clock,
    wire::{
        client::{BatchEntry, BatchOutcome, Client, ClientTls, Credential, WireError},
        frame::{
            Direction, Flags, FrameDecoder, FrameHeader, FrameKind, encode_frame, encode_message,
            write_preface,
        },
        listen::serve_wire_bound,
        messages::{
            WireBatch, WireBatchEntry, WireBatchMode, WireCloseHandle, WireConnError, WirePrepare,
            WirePrepared, WireReply, WireRun, wire_batch_entry,
        },
    },
};
use prost::Message;
use rustls::pki_types::ServerName;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::{sleep, timeout},
};
use tokio_rustls::TlsConnector;

async fn frame(
    stream: &mut (impl AsyncRead + Unpin),
    decoder: &mut FrameDecoder,
) -> (FrameHeader, Vec<u8>) {
    loop {
        if let Some((header, body)) = decoder.next_frame().unwrap() {
            let body = decoder.body(body).to_vec();
            decoder.consume_decoded();
            return (header, body);
        }
        assert_ne!(
            timeout(IO_TIMEOUT, stream.read_buf(decoder.read_buf()))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }
}

async fn raw_wire(f: &ParityFixture) -> (tokio_rustls::client::TlsStream<TcpStream>, FrameDecoder) {
    let mut stream = TlsConnector::from(Arc::new(f.app.client(&f.ca)))
        .connect(
            ServerName::try_from("localhost").unwrap(),
            TcpStream::connect(f.wire.address).await.unwrap(),
        )
        .await
        .unwrap();
    let mut decoder = FrameDecoder::new(Direction::ServerToClient, MAX_FRAME);
    let mut out = Vec::new();
    write_preface(&mut out);
    encode_message(
        &mut out,
        FrameKind::Hello,
        Flags(0),
        1,
        &proto::OpenSessionRequest::default(),
    );
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();
    assert_eq!(
        frame(&mut stream, &mut decoder).await.0.kind,
        FrameKind::Ready
    );
    (stream, decoder)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_invalid_frames_end_the_connection() {
    let f = ParityFixture::start().await;
    let ping = proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
        ..Default::default()
    };
    let entry = |id| WireBatchEntry {
        request_id: id,
        entry: Some(wire_batch_entry::Entry::Execute(ping.clone())),
    };
    let batch = |ids: Vec<u32>| WireBatch {
        mode: 0,
        entries: ids.into_iter().map(entry).collect(),
    };
    let cases = [
        ("empty", 9, batch(vec![]).encode_to_vec()),
        ("duplicate entry ID", 9, batch(vec![3, 3]).encode_to_vec()),
        ("zero entry ID", 9, batch(vec![0]).encode_to_vec()),
        ("in-flight entry ID", 9, batch(vec![3, 2]).encode_to_vec()),
        ("in-flight header ID", 2, batch(vec![3]).encode_to_vec()),
        ("zero header ID", 0, batch(vec![3]).encode_to_vec()),
        (
            "too many entries",
            9,
            batch((10..1035).collect()).encode_to_vec(),
        ),
        ("undecodable body", 9, vec![0xff]),
        (
            "unknown mode",
            9,
            WireBatch {
                mode: 7,
                ..batch(vec![3])
            }
            .encode_to_vec(),
        ),
        (
            "missing entry body",
            9,
            WireBatch {
                mode: 0,
                entries: vec![WireBatchEntry {
                    request_id: 3,
                    entry: None,
                }],
            }
            .encode_to_vec(),
        ),
    ];
    for (case, id, body) in cases {
        let (mut stream, mut decoder) = raw_wire(&f).await;
        let mut out = Vec::new();
        if case.starts_with("in-flight") {
            f.ordinary_api
                .test_wire_connection(stream.get_ref().0.local_addr().unwrap())
                .unwrap()
                .hold_queued_requests();
            encode_message(&mut out, FrameKind::Execute, Flags(0), 2, &ping);
        }
        encode_frame(&mut out, FrameKind::Batch, Flags(0), id, &body);
        stream.write_all(&out).await.unwrap();
        stream.flush().await.unwrap();
        let (header, body) = frame(&mut stream, &mut decoder).await;
        assert_eq!(
            (header.kind, header.request_id),
            (FrameKind::ConnError, 0),
            "{case}"
        );
        assert_eq!(
            WireConnError::decode(body.as_slice()).unwrap().code,
            "protocol",
            "{case}"
        );
        match timeout(IO_TIMEOUT, stream.read(&mut [0])).await.unwrap() {
            Ok(n) => assert_eq!(n, 0, "{case}"),
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof, "{case}"),
        }
    }
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelined_prepare_run_close_see_each_other() {
    let f = ParityFixture::start().await;
    let (mut stream, mut decoder) = raw_wire(&f).await;

    let mut out = Vec::new();
    let run = WireRun {
        handle: 0,
        ..Default::default()
    };
    encode_message(
        &mut out,
        FrameKind::Prepare,
        Flags(0),
        2,
        &WirePrepare {
            text: "shipments where id=1 select id".into(),
            language_version: Some("0.18".into()),
        },
    );
    encode_message(&mut out, FrameKind::Run, Flags(0), 3, &run);
    encode_message(
        &mut out,
        FrameKind::CloseHandle,
        Flags(0),
        4,
        &WireCloseHandle { handle: 0 },
    );
    encode_message(&mut out, FrameKind::Run, Flags(0), 5, &run);
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();

    let (header, body) = frame(&mut stream, &mut decoder).await;
    assert_eq!((header.kind, header.request_id), (FrameKind::Prepared, 2));
    assert_eq!(WirePrepared::decode(body.as_slice()).unwrap().handle, 0);
    for id in [3, 4] {
        let (header, body) = frame(&mut stream, &mut decoder).await;
        assert_eq!((header.kind, header.request_id), (FrameKind::Reply, id));
        assert_eq!(
            WireReply::decode(body.as_slice())
                .unwrap()
                .reply
                .unwrap()
                .code,
            "ok"
        );
    }
    let (header, body) = frame(&mut stream, &mut decoder).await;
    assert_eq!((header.kind, header.request_id), (FrameKind::ConnError, 5));
    assert_eq!(
        WireConnError::decode(body.as_slice()).unwrap().code,
        "INVALID_ARGUMENT"
    );
    drop(stream);
    f.stop().await;
}

const VERSION: &str = "0.18";
const READ: &str = "parity.items where id=$id";
const UPDATE: &str = "update parity.items where id=$id set body=\"changed\"";

async fn expect_revoked_handle_request(out: Vec<u8>) {
    let f = ParityFixture::start().await;
    let mut reference = f.wire_client(false).await;
    let (mut stream, mut decoder) = raw_wire(&f).await;
    let mut setup = Vec::new();
    encode_handle_prepare(&mut setup, 2);
    stream.write_all(&setup).await.unwrap();
    stream.flush().await.unwrap();
    let (header, body) = frame(&mut stream, &mut decoder).await;
    assert_eq!((header.kind, header.request_id), (FrameKind::Prepared, 2));
    assert_eq!(WirePrepared::decode(body.as_slice()).unwrap().handle, 0);
    // Let the next request detect the revoked binding on each connection.
    for addr in [
        reference.local_addr(),
        stream.get_ref().0.local_addr().unwrap(),
    ] {
        f.ordinary_api
            .test_wire_connection(addr)
            .unwrap()
            .hold_idle_checks();
    }
    f.service.reload_bindings(vec![]).unwrap();

    let execute = proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
        ..Default::default()
    };
    let id = reference.send_execute(execute).await.unwrap();
    let (received, result) = timeout(IO_TIMEOUT, reference.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received, id);
    let expected = result.unwrap_err();
    assert_eq!(expected.code, "UNAUTHENTICATED");

    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();
    let (header, body) = frame(&mut stream, &mut decoder).await;
    assert_eq!(
        (header.kind, header.flags, header.request_id),
        (FrameKind::ConnError, Flags(0), 3)
    );
    assert_eq!(WireConnError::decode(body.as_slice()).unwrap(), expected);
    assert!(decoder.next_frame().unwrap().is_none());
    match timeout(IO_TIMEOUT, stream.read(&mut [0])).await.unwrap() {
        Ok(n) => assert_eq!(n, 0),
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof),
    }
    f.stop().await;
}

fn encode_handle_prepare(out: &mut Vec<u8>, id: u32) {
    encode_message(
        out,
        FrameKind::Prepare,
        Flags(0),
        id,
        &WirePrepare {
            text: "shipments where id=1 select id".into(),
            language_version: Some(VERSION.into()),
        },
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_checks_revoked_binding() {
    let mut out = Vec::new();
    encode_handle_prepare(&mut out, 3);
    expect_revoked_handle_request(out).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_handle_checks_revoked_binding() {
    let mut out = Vec::new();
    encode_message(
        &mut out,
        FrameKind::CloseHandle,
        Flags(0),
        3,
        &WireCloseHandle { handle: 0 },
    );
    expect_revoked_handle_request(out).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelined_prepare_checks_revoked_binding_before_run() {
    let mut out = Vec::new();
    encode_handle_prepare(&mut out, 3);
    encode_message(
        &mut out,
        FrameKind::Run,
        Flags(0),
        4,
        &WireRun {
            handle: 1,
            ..Default::default()
        },
    );
    expect_revoked_handle_request(out).await;
}

fn text(
    statement: &str,
    bindings: Vec<proto::NativeBinding>,
    operation_id: Option<&str>,
) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Text(
            proto::NativeTextRequest {
                text: statement.into(),
                language_version: Some(VERSION.into()),
                bindings,
            },
        )),
        operation_id: operation_id.map(str::to_owned),
        ..Default::default()
    }
}

fn binding(value: proto::value::Value) -> Vec<proto::NativeBinding> {
    vec![proto::NativeBinding {
        name: Some("id".into()),
        value: Some(proto::Value { value: Some(value) }),
    }]
}

fn sql(statement: impl Into<String>) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: statement.into(),
            params: vec![],
        })),
        ..Default::default()
    }
}

fn row_ids(reply: proto::ExecuteReply) -> Vec<i64> {
    assert_eq!(reply.code, "ok");
    reply
        .sql
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match row.values[0].value {
            Some(proto::value::Value::IntValue(id)) => id,
            _ => panic!("expected an integer ID: {row:?}"),
        })
        .collect()
}

fn batch_reply(outcome: BatchOutcome) -> proto::ExecuteReply {
    match outcome {
        BatchOutcome::Reply(reply) => reply,
        _ => panic!("expected a reply: {outcome:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_all_runs_every_entry() {
    let f = ParityFixture::start().await;
    let mut client = f.wire_client_for(&f.sql_app).await;
    let entries = vec![
        BatchEntry::Execute(sql("SELECT 10")),
        BatchEntry::Execute(proto::ExecuteRequest::default()),
        BatchEntry::Execute(sql("SELECT 30")),
    ];
    let mut results = client
        .batch(WireBatchMode::All, entries)
        .await
        .unwrap()
        .into_iter();
    assert_eq!(row_ids(batch_reply(results.next().unwrap())), [10]);
    assert!(matches!(results.next(), Some(BatchOutcome::Error(error))
        if error.code == "INVALID_ARGUMENT"));
    assert_eq!(row_ids(batch_reply(results.next().unwrap())), [30]);
    assert!(results.next().is_none());
    client.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_stop_on_error_skips_rest() {
    let f = ParityFixture::start().await;
    let mut client = f.wire_client_for(&f.sql_app).await;
    for (mode, merchant, expected) in [
        (WireBatchMode::StopOnError, 96, &[1, 2][..]),
        (WireBatchMode::All, 97, &[1, 2, 4, 5][..]),
    ] {
        let entries = [1, 2, 2, 4, 5].map(|id| {
            BatchEntry::Execute(sql(format!(
                "INSERT INTO shipments (merchant_id, id, status) VALUES ({merchant}, {id}, 'batch')"
            )))
        });
        let results = client.batch(mode, Vec::from(entries)).await.unwrap();
        assert_eq!(results.len(), 5);
        for (index, result) in results.into_iter().enumerate() {
            if mode == WireBatchMode::StopOnError && index >= 3 {
                assert!(matches!(result, BatchOutcome::Skipped), "{result:?}");
            } else {
                assert_eq!(
                    batch_reply(result).code,
                    if index == 2 { "23505" } else { "ok" }
                );
            }
        }
        let query = sql(format!(
            "SELECT id FROM shipments WHERE merchant_id={merchant} ORDER BY id"
        ));
        assert_eq!(row_ids(client.execute(query).await.unwrap()), expected);
    }
    client.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_mixes_execute_and_run_entries() {
    let f = ParityFixture::start().await;
    let mut client = f.wire_client(false).await;
    let statement = "shipments where id=$id select id";
    let handle = prepare(&mut client, statement).await;
    let first_bindings = binding(proto::value::Value::IntValue(1));
    let second = text(
        statement,
        binding(proto::value::Value::IntValue(2)),
        None,
    );
    let expected = [
        client
            .execute(text(statement, first_bindings.clone(), None))
            .await
            .unwrap(),
        client.execute(second.clone()).await.unwrap(),
    ];
    assert!(expected.iter().all(|reply| reply.code == "ok"));
    let ids = client
        .send_batch(
            WireBatchMode::StopOnError,
            vec![
                BatchEntry::Run {
                    handle,
                    bindings: first_bindings,
                },
                BatchEntry::Execute(second),
                BatchEntry::Run {
                    handle,
                    bindings: binding(proto::value::Value::NumericValue("1.25".into())),
                },
                BatchEntry::Run {
                    handle,
                    bindings: vec![],
                },
            ],
        )
        .await
        .unwrap();
    let following = client
        .send_execute(text(
            statement,
            binding(proto::value::Value::IntValue(3)),
            None,
        ))
        .await
        .unwrap();
    for (&expected_id, expected_reply) in ids[..2].iter().zip(expected) {
        let (id, result) = client.recv_batch_entry().await.unwrap();
        assert_eq!(id, expected_id);
        assert_eq!(batch_reply(result), expected_reply);
    }
    let (id, result) = client.recv_batch_entry().await.unwrap();
    assert_eq!(id, ids[2]);
    assert!(matches!(result, BatchOutcome::Error(error) if error.code == "INVALID_ARGUMENT"));
    let (id, result) = client.recv_batch_entry().await.unwrap();
    assert_eq!(id, ids[3]);
    assert!(matches!(result, BatchOutcome::Skipped));
    let (id, result) = client.recv().await.unwrap();
    assert_eq!(id, following);
    assert_eq!(result.unwrap().code, "ok");
    client.goodbye().await.unwrap();
    f.stop().await;
}

fn wire_error(error: &std::io::Error) -> &WireError {
    error.get_ref().unwrap().downcast_ref().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_controls_spend_shared_api_rate() {
    let f = ParityFixture::start().await;
    let service = Service::with_limits(
        f.admin.clone(),
        vec![Binding {
            fingerprint: f.app.fingerprint(),
            role: Role::Database { name: APP.into() },
        }],
        Clock::new(0, 3600).unwrap(),
        ServiceLimits {
            sessions: 8,
            burst: 7,
            per_second: 1,
        },
    )
    .unwrap();
    let runtime = ApiRuntime::new(service, ApiLimits::default()).unwrap();
    let (_start, gate) = watch::channel(true);
    let wire = serve_wire_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        runtime.clone(),
        f.tls.clone(),
        8,
        gate,
    )
    .await
    .unwrap();
    let tls = ClientTls {
        ca: f.ca.cert.pem().into_bytes(),
        cert: Some(f.app.cert.pem().into_bytes()),
        key: Some(f.app.key.serialize_pem().into_bytes()),
        server_name: "localhost".into(),
    };
    let (mut client, _) = Client::connect(
        wire.address,
        Some(tls),
        proto::OpenSessionRequest::default(),
        Credential::None,
    )
    .await
    .unwrap();

    // One connection and six attempts, including failed controls, spend the burst.
    let statement = "shipments where id=1 select id";
    let kept = prepare(&mut client, statement).await;
    assert_eq!(client.run(kept, vec![]).await.unwrap().code, "ok");
    let closed = prepare(&mut client, statement).await;
    client.close_handle(closed).await.unwrap();
    let invalid_prepare = client
        .prepare("(", Some(VERSION))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(invalid_prepare.code, "INVALID_ARGUMENT");
    let invalid_close = client.close_handle(closed).await.unwrap_err();
    assert_eq!(wire_error(&invalid_close).code, "INVALID_ARGUMENT");

    let code = |error| wire_error(&error).code.clone();
    let ping = proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
        ..Default::default()
    };
    let execute = client.execute(ping).await.map_err(code);
    let prepare = client
        .prepare(statement, Some(VERSION))
        .await
        .unwrap()
        .map_err(|error| error.code);
    let close = client.close_handle(kept).await.map_err(code);
    let refused = "RESOURCE_EXHAUSTED".to_owned();
    assert_eq!(
        (prepare, close, execute.map(|reply| reply.code)),
        (Err(refused.clone()), Err(refused.clone()), Err(refused)),
        "(PREPARE, CLOSE_HANDLE, EXECUTE)"
    );

    sleep(Duration::from_secs(1)).await;
    assert_eq!(client.run(kept, vec![]).await.unwrap().code, "ok");
    client.goodbye().await.unwrap();
    wire.shutdown().await.unwrap();
    runtime.shutdown().await;
    f.stop().await;
}

async fn prepare(client: &mut Client, statement: &str) -> u32 {
    client
        .prepare(statement, Some(VERSION))
        .await
        .unwrap()
        .unwrap()
}

async fn control(client: &mut Client, command: serde_json::Value) {
    let request = wire_json::decode_execute(&serde_json::to_vec(&command).unwrap()).unwrap();
    let reply = client.execute(request).await.unwrap();
    assert_eq!(reply.code, "ok", "{command}: {reply:?}");
}

async fn compare(
    prepared: &mut Client,
    direct: &mut Client,
    statement: &str,
    handle: u32,
    id: i64,
    code: &str,
) -> proto::ExecuteReply {
    let bindings = binding(proto::value::Value::IntValue(id));
    let expected = direct
        .execute(text(statement, bindings.clone(), None))
        .await
        .unwrap();
    assert_eq!(expected.code, code, "{statement}: {expected:?}");
    let actual = prepared.run(handle, bindings).await.unwrap();
    assert_eq!(actual, expected, "{statement}");
    actual
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_equals_execute_for_each_form() {
    // Apply the same writes to independent databases and retain the same
    // prepared handles across catalog and authority changes.
    let prepared = ParityFixture::start().await;
    let direct = ParityFixture::start().await;
    for f in [&prepared, &direct] {
        f.sql(&format!("CREATE SCHEMA parity AUTHORIZATION {APP}"));
        f.sql("CREATE TABLE parity.items (id BIGINT PRIMARY KEY, body TEXT)");
        f.sql("INSERT INTO parity.items VALUES (1, 'seed')");
        f.admin
            .roles()
            .set_table_owner(&relation_key("parity", "items"), APP)
            .unwrap();
        f.admin
            .roles()
            .create(NewRole {
                name: "handle_admin".into(),
                superuser: true,
                login: true,
                password: Some(Verifier::generate("handle-test-password")),
                ..Default::default()
            })
            .unwrap();
    }
    let mut client = prepared.wire_client(false).await;
    let mut reference = direct.wire_client(false).await;
    let statements = [
        READ,
        "insert parity.items set id=$id,body=\"native\"",
        UPDATE,
        "delete parity.items where id=$id",
        "transaction { insert parity.items set id=$id; delete parity.items where id=$id }",
    ];
    let mut handles = Vec::new();
    for statement in statements {
        handles.push(prepare(&mut client, statement).await);
    }
    let column_count = |reply: proto::ExecuteReply| {
        let Some(proto::execute_reply::NativeResult::OperationOutcome(outcome)) =
            reply.native_result
        else {
            panic!("missing read outcome: {reply:?}");
        };
        outcome.columns.len()
    };
    for (phase, read_columns) in [(0, 2), (1, 3), (2, 3), (3, 3)] {
        match phase {
            1 => {
                for f in [&prepared, &direct] {
                    f.sql("ALTER TABLE parity.items ADD COLUMN external_column TEXT");
                }
            }
            2 | 3 => {
                for c in [&mut client, &mut reference] {
                    control(c, json!({"begin": {}})).await;
                    if phase == 3 {
                        control(c, json!({"savepoint": {"name": "before_ddl"}})).await;
                    }
                    control(
                        c,
                        json!({"catalog": {"alterTable": {
                            "table": "parity.items",
                            "addColumn": {"name": "transactional_column", "dataType": "Text"}
                        }}}),
                    )
                    .await;
                }
                let reply = compare(&mut client, &mut reference, READ, handles[0], 1, "ok").await;
                assert_eq!(column_count(reply), 4, "phase {phase} with uncommitted DDL");
                let rollback = if phase == 3 {
                    json!({"rollbackTo": {"name": "before_ddl"}})
                } else {
                    json!({"rollback": {}})
                };
                for c in [&mut client, &mut reference] {
                    control(c, rollback.clone()).await;
                }
                if phase == 3 {
                    let reply =
                        compare(&mut client, &mut reference, READ, handles[0], 1, "ok").await;
                    assert_eq!(
                        column_count(reply),
                        read_columns,
                        "after rollback to savepoint"
                    );
                    for c in [&mut client, &mut reference] {
                        control(c, json!({"commit": {}})).await;
                    }
                }
            }
            _ => {}
        }
        for ((statement, &handle), id) in statements.into_iter().zip(&handles).zip([1, 2, 2, 2, 3])
        {
            let reply = compare(&mut client, &mut reference, statement, handle, id, "ok").await;
            if statement == READ {
                assert_eq!(column_count(reply), read_columns, "phase {phase}");
            }
        }
    }

    // Use separate administrator sessions to grant and then revoke the only
    // membership that supplies access to the table.
    let mut admins = Vec::new();
    for f in [&prepared, &direct] {
        let admin = f
            .password("handle_admin", "handle-test-password")
            .await
            .unwrap();
        admin
            .batch_execute("CREATE ROLE handle_owner")
            .await
            .unwrap();
        admin
            .batch_execute(&format!("GRANT handle_owner TO {APP}"))
            .await
            .unwrap();
        f.admin
            .roles()
            .set_table_owner(&relation_key("parity", "items"), "handle_owner")
            .unwrap();
        admins.push(admin);
    }
    compare(&mut client, &mut reference, READ, handles[0], 1, "ok").await;
    for admin in admins {
        admin
            .batch_execute(&format!("REVOKE handle_owner FROM {APP}"))
            .await
            .unwrap();
    }
    for ((statement, handle), id) in statements.into_iter().zip(handles).zip([1, 2, 2, 2, 3]) {
        compare(&mut client, &mut reference, statement, handle, id, "42501").await;
    }
    client.goodbye().await.unwrap();
    reference.goodbye().await.unwrap();
    prepared.stop().await;
    direct.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parameter_admission_fails_at_run() {
    let f = ParityFixture::start().await;
    let mut client = f.wire_client(false).await;
    let statement = "shipments where id=$id select id";
    let handle = prepare(&mut client, statement).await;
    let numeric = binding(proto::value::Value::NumericValue("1.25".into()));
    let expected = client
        .execute(text(statement, numeric.clone(), None))
        .await
        .unwrap_err();
    let actual = client.run(handle, numeric).await.unwrap_err();
    assert_eq!(wire_error(&expected).code, "INVALID_ARGUMENT");
    assert_eq!(wire_error(&actual).code, wire_error(&expected).code);
    assert_eq!(wire_error(&actual).message, wire_error(&expected).message);
    let integer = binding(proto::value::Value::IntValue(1));
    assert_eq!(client.run(handle, integer).await.unwrap().code, "ok");
    client.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_session_refuses_handle_like_text() {
    let f = ParityFixture::start().await;
    let mut client = f.wire_client(true).await;
    let statement = "delete shipments where id=$id";
    let handle = prepare(&mut client, statement).await;
    let bindings = binding(proto::value::Value::IntValue(1));
    let expected = client
        .execute(text(statement, bindings.clone(), None))
        .await
        .unwrap();
    assert_ne!(expected.code, "ok", "task delete must be refused");
    assert_eq!(
        client.run(handle, bindings.clone()).await.unwrap(),
        expected
    );

    let update = "update shipments where id=$id set status=\"queued\"";
    let handle = prepare(&mut client, update).await;
    let expected = client
        .execute(text(update, bindings.clone(), Some("handle-update")))
        .await
        .unwrap();
    assert_eq!(expected.code, "ok", "{expected:?}");
    let replies = client
        .pipeline_run(vec![WireRun {
            handle,
            bindings,
            operation_id: Some("handle-update".into()),
            ..Default::default()
        }])
        .await
        .unwrap();
    assert_eq!(replies, [expected]);
    client.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handles_are_per_connection() {
    let f = ParityFixture::start().await;
    let mut first = f.wire_client(false).await;
    let mut second = f.wire_client(false).await;
    let statement = "shipments where id=1 select id";
    let handle = prepare(&mut first, statement).await;
    let missing = second.run(handle, vec![]).await.unwrap_err();
    assert_eq!(wire_error(&missing).code, "INVALID_ARGUMENT");
    assert_eq!(first.run(handle, vec![]).await.unwrap().code, "ok");
    first.close_handle(handle).await.unwrap();
    assert_eq!(
        wire_error(&first.close_handle(handle).await.unwrap_err()).code,
        "INVALID_ARGUMENT"
    );
    assert_eq!(prepare(&mut first, statement).await, handle);
    first.goodbye().await.unwrap();
    second.goodbye().await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_budget_and_operation_id_match_execute() {
    let f = ParityFixture::start().await;
    let mut client = f.wire_client(false).await;
    for (statement, budget, operation_id, code) in [
        (
            "shipments where id=$id select id",
            Some(10_001),
            None,
            "INVALID_ARGUMENT",
        ),
        (
            "shipments where id=$id select id",
            Some(0),
            None,
            "DEADLINE_EXCEEDED",
        ),
        (
            "shipments where id=$id select id",
            None,
            Some("wrong-kind"),
            "INVALID_ARGUMENT",
        ),
        (
            "update shipments where id=$id set status=\"queued\"",
            None,
            Some("bad id"),
            "INVALID_ARGUMENT",
        ),
        (
            "update shipments where id=$id set status=\"queued\"",
            None,
            Some("valid-id"),
            "INVALID_ARGUMENT",
        ),
    ] {
        let handle = prepare(&mut client, statement).await;
        let bindings = binding(proto::value::Value::IntValue(1));
        let mut reference = text(statement, bindings.clone(), operation_id);
        reference.budget_ms = budget;
        let expected = client.execute(reference).await.unwrap_err();
        let id = client
            .send_run(WireRun {
                handle,
                bindings,
                budget_ms: budget,
                operation_id: operation_id.map(str::to_owned),
            })
            .await
            .unwrap();
        let (received, actual) = client.recv().await.unwrap();
        assert_eq!(received, id);
        let actual = actual.unwrap_err();
        assert_eq!(actual.code, code, "{statement}");
        assert_eq!(actual.code, wire_error(&expected).code);
        assert_eq!(actual.message, wire_error(&expected).message);
    }
    client.goodbye().await.unwrap();
    f.stop().await;
}
