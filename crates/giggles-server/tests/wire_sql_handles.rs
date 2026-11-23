#![cfg(feature = "engine-rocksdb")]
mod api_support;
use api_support::Fixture;
use giggles_core::{
    relational::relation_key,
    roles::{NewRole, TablePrivilege},
};
use giggles_server::{
    api::{ApiLimits, ApiRuntime, net::serve_role_bound, proto},
    auth::RoleApiConfig,
    wire::{
        client::{BatchEntry, BatchOutcome, Client, ClientTls, Credential, WireError},
        frame::{Flags, FrameKind},
        listen::serve_role_wire_bound,
        messages::{WireBatchMode, WireConnError, WirePrepare, WireRun},
    },
};
use prost::Message;
use serde_json::json;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::watch};

async fn fixture() -> (Fixture, giggles_server::Running, Client, u32) {
    let mut f = Fixture::catalog_empty_engine(giggles_core::Engine::V2).await;
    f.admin
        .roles()
        .create(NewRole {
            name: "oid_owner".into(),
            superuser: true,
            login: true,
            ..Default::default()
        })
        .unwrap();
    let wire_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config: RoleApiConfig = serde_json::from_value(json!({
        "http":"127.0.0.1:0", "wire":wire_listener.local_addr().unwrap().to_string(), "tls":"required",
        "certificate": f.path.join("server.pem"), "private_key": f.path.join("server.key.TEST-ONLY"),
        "client_ca": f.path.join("ca.pem"),
        "certificates": { (giggles_server::tls::fingerprint(f.app.cert.der())): "oid_owner" }
    })).unwrap();
    let settings = Arc::new(config.build().unwrap());
    let runtime =
        ApiRuntime::new_role(f.service.clone(), ApiLimits::default(), settings.clone()).unwrap();
    let (_, gate) = watch::channel(true);
    let http = serve_role_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        runtime.clone(),
        settings.clone(),
        gate.clone(),
        false,
    )
    .await
    .unwrap();
    let grpc = serve_role_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        runtime.clone(),
        settings.clone(),
        gate.clone(),
        true,
    )
    .await
    .unwrap();
    std::mem::replace(&mut f.http, http)
        .shutdown()
        .await
        .unwrap();
    std::mem::replace(&mut f.grpc, grpc)
        .shutdown()
        .await
        .unwrap();
    f.runtime.shutdown().await;
    f.runtime = runtime;
    let wire = serve_role_wire_bound(wire_listener, f.runtime.clone(), settings, gate)
        .await
        .unwrap();
    let tls = ClientTls {
        ca: f.ca.cert.pem().into_bytes(),
        cert: Some(f.app.cert.pem().into_bytes()),
        key: Some(f.app.key.serialize_pem().into_bytes()),
        server_name: "localhost".into(),
    };
    let (client, ready) = Client::connect(
        wire.address,
        Some(tls),
        proto::OpenSessionRequest {
            role: Some("oid_owner".into()),
            database: Some("postgres".into()),
            ..Default::default()
        },
        Credential::None,
    )
    .await
    .unwrap();
    assert_eq!(client.protocol_minor(), 1);
    setup(&f, "CREATE TABLE t (id integer PRIMARY KEY, name text)");
    setup(
        &f,
        "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
    );
    (f, wire, client, ready.max_handles)
}

fn setup(f: &Fixture, query: &str) {
    f.service
        .role_session("oid_owner")
        .unwrap()
        .execute_sql(query, &[])
        .unwrap_or_else(|error| panic!("{query}: {error:?}"));
}

fn params(id: i64) -> Vec<proto::Value> {
    vec![proto::Value {
        value: Some(proto::value::Value::IntValue(id)),
    }]
}

fn sql(text: &str, params: Vec<proto::Value>) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: text.into(),
            params,
        })),
        ..Default::default()
    }
}

async fn prepared(c: &mut Client, text: &str) -> u32 {
    c.prepare_sql(text).await.unwrap().unwrap()
}

async fn finish(f: Fixture, wire: giggles_server::Running, c: Client) {
    c.goodbye().await.unwrap();
    wire.shutdown().await.unwrap();
    f.stop().await;
}

async fn request_error(c: &mut Client, run: WireRun) {
    let id = c.send_run(run).await.unwrap();
    let (received, result) = c.recv().await.unwrap();
    assert_eq!(received, id);
    assert_eq!(result.unwrap_err().code, "INVALID_ARGUMENT");
    assert_eq!(c.execute(sql("SELECT 1", vec![])).await.unwrap().code, "ok");
}

#[tokio::test]
async fn sql_prepare_and_run_with_params() {
    let (f, wire, mut c, _) = fixture().await;
    let h = prepared(&mut c, "SELECT id, name FROM t WHERE id = $1").await;
    for (id, name) in [(1, "one"), (2, "two")] {
        let reply = c.run_sql(h, params(id)).await.unwrap();
        assert_eq!(reply.code, "ok");
        let result = reply.sql.unwrap();
        assert_eq!(result.columns, ["id", "name"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].values,
            vec![
                params(id).remove(0),
                proto::Value {
                    value: Some(proto::value::Value::TextValue(name.into()))
                }
            ]
        );
    }
    finish(f, wire, c).await;
}

#[tokio::test]
async fn sql_handle_reuse_matches_execute() {
    let (f, wire, mut c, _) = fixture().await;
    let text = "SELECT id, name FROM t WHERE id = $1";
    let h = prepared(&mut c, text).await;
    for id in [1, 2, 3] {
        let expected = c.execute(sql(text, params(id))).await.unwrap();
        assert_eq!(expected.code, "ok");
        assert_eq!(c.run_sql(h, params(id)).await.unwrap(), expected);
    }
    finish(f, wire, c).await;
}

#[tokio::test]
async fn sql_handle_run_rechecks_privilege_after_revoke() {
    let (f, wire, owner, _) = fixture().await;
    f.admin
        .roles()
        .create(NewRole {
            name: "handle_reader".into(),
            login: true,
            password: Some(giggles_server::auth::scram::Verifier::generate(
                "reader-password",
            )),
            ..Default::default()
        })
        .unwrap();
    let table = relation_key("public", "t");
    f.admin
        .roles()
        .grant_table(&table, "handle_reader", &[TablePrivilege::Select])
        .unwrap();
    // Sign in as the reader over the role listener.
    let tls = ClientTls {
        ca: f.ca.cert.pem().into_bytes(),
        cert: None,
        key: None,
        server_name: "localhost".into(),
    };
    let (mut reader, _) = Client::connect(
        wire.address,
        Some(tls),
        proto::OpenSessionRequest {
            role: Some("handle_reader".into()),
            database: Some("postgres".into()),
            ..Default::default()
        },
        Credential::Scram {
            role: "handle_reader".into(),
            password: "reader-password".into(),
            database: None,
        },
    )
    .await
    .unwrap();
    let h = prepared(&mut reader, "SELECT id FROM t WHERE id = $1").await;
    assert_eq!(reader.run_sql(h, params(1)).await.unwrap().code, "ok");
    f.admin
        .roles()
        .revoke_table(&table, "handle_reader", &[TablePrivilege::Select])
        .unwrap();
    assert_eq!(reader.run_sql(h, params(1)).await.unwrap().code, "42501");
    reader.goodbye().await.unwrap();
    finish(f, wire, owner).await;
}

#[tokio::test]
async fn sql_handle_sees_schema_change() {
    let (f, wire, mut c, _) = fixture().await;
    let h = prepared(&mut c, "SELECT * FROM t").await;
    assert_eq!(
        c.run_sql(h, vec![]).await.unwrap().sql.unwrap().columns,
        ["id", "name"]
    );
    setup(&f, "ALTER TABLE t ADD COLUMN extra bigint");
    let result = c.run_sql(h, vec![]).await.unwrap().sql.unwrap();
    assert_eq!(result.columns, ["id", "name", "extra"]);
    assert_eq!(result.column_types, [23, 25, 20]);
    finish(f, wire, c).await;
}

#[tokio::test]
async fn sql_handle_syntax_error_at_run() {
    let (f, wire, mut c, _) = fixture().await;
    let h = prepared(&mut c, "SELECT * FROM").await;
    let expected = c.execute(sql("SELECT * FROM", vec![])).await.unwrap();
    for _ in 0..2 {
        assert_eq!(c.run_sql(h, vec![]).await.unwrap(), expected);
    }
    assert_eq!(expected.code, "42601");
    finish(f, wire, c).await;
}

#[tokio::test]
async fn sql_handle_in_batch_all_and_stop_on_error() {
    let (f, wire, mut c, _) = fixture().await;
    let h = prepared(&mut c, "SELECT id FROM t WHERE id = $1").await;
    let bad = prepared(&mut c, "SELECT missing_column FROM t").await;
    let native = c
        .prepare("t where id=1 select id", Some("0.18"))
        .await
        .unwrap()
        .unwrap();
    let good = c.run_sql(h, params(2)).await.unwrap();
    let native_reply = c.run(native, vec![]).await.unwrap();
    for mode in [WireBatchMode::All, WireBatchMode::StopOnError] {
        let outcomes = c
            .batch(
                mode,
                vec![
                    BatchEntry::RunRequest(WireRun {
                        handle: h,
                        params: params(2),
                        ..Default::default()
                    }),
                    BatchEntry::Run {
                        handle: native,
                        bindings: vec![],
                    },
                    BatchEntry::RunRequest(WireRun {
                        handle: bad,
                        ..Default::default()
                    }),
                    BatchEntry::Execute(sql("SELECT 1", vec![])),
                    BatchEntry::RunRequest(WireRun {
                        handle: h,
                        params: params(2),
                        ..Default::default()
                    }),
                ],
            )
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 5);
        for (i, outcome) in outcomes.into_iter().enumerate() {
            if mode == WireBatchMode::StopOnError && i > 2 {
                assert!(matches!(outcome, BatchOutcome::Skipped));
            } else {
                let BatchOutcome::Reply(reply) = outcome else {
                    panic!("{outcome:?}")
                };
                match i {
                    0 | 4 => assert_eq!(reply, good),
                    1 => assert_eq!(reply, native_reply),
                    2 => assert_eq!(reply.code, "42703"),
                    _ => assert_eq!(reply.code, "ok"),
                }
            }
        }
    }
    finish(f, wire, c).await;
}

#[tokio::test]
async fn sql_handle_close_and_reuse_slot() {
    let (f, wire, mut c, _) = fixture().await;
    let first = prepared(&mut c, "SELECT 1").await;
    let last = prepared(&mut c, "SELECT 2").await;
    c.close_handle(first).await.unwrap();
    c.close_handle(last).await.unwrap();
    request_error(
        &mut c,
        WireRun {
            handle: last,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        c.prepare("t select id", Some("0.18"))
            .await
            .unwrap()
            .unwrap(),
        last
    );
    assert_eq!(prepared(&mut c, "SELECT 3").await, first);
    finish(f, wire, c).await;
}

#[tokio::test]
async fn sql_handle_rejects_bindings() {
    let (f, wire, mut c, _) = fixture().await;
    let handle = prepared(&mut c, "SELECT 1").await;
    request_error(
        &mut c,
        WireRun {
            handle,
            bindings: vec![proto::NativeBinding {
                name: Some("id".into()),
                value: Some(params(1).remove(0)),
            }],
            ..Default::default()
        },
    )
    .await;
    finish(f, wire, c).await;
}

#[tokio::test]
async fn native_handle_rejects_params() {
    let (f, wire, mut c, _) = fixture().await;
    let handle = c
        .prepare("t select id", Some("0.18"))
        .await
        .unwrap()
        .unwrap();
    request_error(
        &mut c,
        WireRun {
            handle,
            params: params(1),
            ..Default::default()
        },
    )
    .await;
    finish(f, wire, c).await;
}

#[tokio::test]
async fn sql_handle_rejects_operation_id() {
    let (f, wire, mut c, _) = fixture().await;
    let handle = prepared(&mut c, "SELECT 1").await;
    request_error(
        &mut c,
        WireRun {
            handle,
            operation_id: Some("op".into()),
            ..Default::default()
        },
    )
    .await;
    finish(f, wire, c).await;
}

#[tokio::test]
async fn sql_prepare_refuses_language_version_and_blank() {
    let (f, wire, c, _) = fixture().await;
    let (mut stream, mut decoder) = raw_role(&f, wire.address).await;
    for request in [
        WirePrepare {
            text: "SELECT 1".into(),
            language_version: Some("0.18".into()),
            sql: Some(true),
        },
        WirePrepare {
            text: " \n\t".into(),
            sql: Some(true),
            ..Default::default()
        },
    ] {
        // Raw frame access lets this test send deliberately invalid PREPARE fields.
        let mut bytes = Vec::new();
        giggles_server::wire::frame::encode_message(
            &mut bytes,
            FrameKind::Prepare,
            Flags(0),
            900,
            &request,
        );
        stream.write_all(&bytes).await.unwrap();
        let (header, body) = raw_frame(&mut stream, &mut decoder).await;
        assert_eq!(
            (header.kind, header.request_id),
            (FrameKind::ConnError, 900)
        );
        assert_eq!(
            WireConnError::decode(body.as_slice()).unwrap().code,
            "INVALID_ARGUMENT"
        );
    }
    let mut bytes = Vec::new();
    giggles_server::wire::frame::encode_message(
        &mut bytes,
        FrameKind::Execute,
        Flags(0),
        901,
        &sql("SELECT 1", vec![]),
    );
    stream.write_all(&bytes).await.unwrap();
    assert_eq!(
        raw_frame(&mut stream, &mut decoder).await.0.kind,
        FrameKind::Reply
    );
    drop(stream);
    finish(f, wire, c).await;
}

#[tokio::test]
async fn handle_cap_counts_sql_and_native() {
    let (f, wire, mut c, cap) = fixture().await;
    for i in 0..cap {
        let handle = if i % 2 == 0 {
            prepared(&mut c, "SELECT 1").await
        } else {
            c.prepare("t select id", Some("0.18"))
                .await
                .unwrap()
                .unwrap()
        };
        assert_eq!(handle, i);
    }
    assert_eq!(
        c.prepare_sql("SELECT 1").await.unwrap().unwrap_err().code,
        "RESOURCE_EXHAUSTED"
    );
    assert_eq!(
        c.prepare("t select id", Some("0.18"))
            .await
            .unwrap()
            .unwrap_err()
            .code,
        "RESOURCE_EXHAUSTED"
    );
    c.close_handle(0).await.unwrap();
    assert_eq!(prepared(&mut c, "SELECT 1").await, 0);
    finish(f, wire, c).await;
}

#[tokio::test]
async fn sql_handle_run_empty_result_typed() {
    let (f, wire, mut c, _) = fixture().await;
    let h = prepared(&mut c, "SELECT id FROM t WHERE false").await;
    let reply = c.run_sql(h, vec![]).await.unwrap();
    assert_eq!(reply.code, "ok");
    let result = reply.sql.unwrap();
    assert_eq!(result.column_types, [23]);
    assert!(result.rows.is_empty());
    finish(f, wire, c).await;
}

#[tokio::test]
async fn old_client_without_sql_flag_unchanged() {
    let (f, wire, mut c, _) = fixture().await;
    let text = "t where id=1 select id";
    let handle = c.prepare(text, Some("0.18")).await.unwrap().unwrap();
    let expected = c
        .execute(proto::ExecuteRequest {
            request: Some(proto::execute_request::Request::Text(
                proto::NativeTextRequest {
                    text: text.into(),
                    language_version: Some("0.18".into()),
                    bindings: vec![],
                },
            )),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(expected.code, "ok");
    assert_eq!(c.run(handle, vec![]).await.unwrap(), expected);
    finish(f, wire, c).await;
}

#[tokio::test]
async fn sql_handle_run_equals_execute_with_budget() {
    let (f, wire, mut c, _) = fixture().await;
    let text = "SELECT id FROM t WHERE id = $1";
    let handle = prepared(&mut c, text).await;
    for (budget, code) in [(10_001, "INVALID_ARGUMENT"), (0, "DEADLINE_EXCEEDED")] {
        let mut request = sql(text, params(1));
        request.budget_ms = Some(budget);
        let expected = c.execute(request).await.unwrap_err();
        let expected = expected
            .get_ref()
            .unwrap()
            .downcast_ref::<WireError>()
            .unwrap();
        let id = c
            .send_run(WireRun {
                handle,
                params: params(1),
                budget_ms: Some(budget),
                ..Default::default()
            })
            .await
            .unwrap();
        let (received, actual) = c.recv().await.unwrap();
        assert_eq!(received, id);
        let actual = actual.unwrap_err();
        assert_eq!(actual.code, code);
        assert_eq!(
            (&actual.code, &actual.message),
            (&expected.code, &expected.message)
        );
    }
    finish(f, wire, c).await;
}

use giggles_server::wire::frame::{
    Direction, FrameDecoder, FrameHeader, encode_message, write_preface,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

async fn raw_frame(
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
            tokio::time::timeout(
                giggles_server::IO_TIMEOUT,
                stream.read_buf(decoder.read_buf())
            )
            .await
            .unwrap()
            .unwrap(),
            0
        );
    }
}

async fn raw_role(
    f: &Fixture,
    address: std::net::SocketAddr,
) -> (
    tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
    FrameDecoder,
) {
    let mut stream = tokio_rustls::TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(
                giggles_server::tls::roots(vec![f.ca.cert.der().clone()]).unwrap(),
            )
            .with_client_auth_cert(
                vec![f.app.cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(f.app.key.serialize_der()).into(),
            )
            .unwrap(),
    ))
    .connect(
        rustls::pki_types::ServerName::try_from("localhost").unwrap(),
        tokio::net::TcpStream::connect(address).await.unwrap(),
    )
    .await
    .unwrap();
    let mut decoder = FrameDecoder::new(Direction::ServerToClient, giggles_server::MAX_FRAME);
    let mut out = Vec::new();
    write_preface(&mut out);
    encode_message(
        &mut out,
        FrameKind::Hello,
        Flags(0),
        1,
        &proto::OpenSessionRequest {
            role: Some("oid_owner".into()),
            database: Some("postgres".into()),
            ..Default::default()
        },
    );
    stream.write_all(&out).await.unwrap();
    assert_eq!(
        raw_frame(&mut stream, &mut decoder).await.0.kind,
        FrameKind::Ready
    );
    (stream, decoder)
}
