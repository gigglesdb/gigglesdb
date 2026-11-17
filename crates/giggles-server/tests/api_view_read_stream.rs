#![cfg(feature = "engine-rocksdb")]
//! View expansion spills belong to the native reader, never its transport.
use super::api_result_stream::api_support::{Fixture, rpc};
use super::api_result_stream::{SERIAL, check_rows, http_stream, seed, stop, typed_read};
use giggles_core::Engine;
use giggles_server::api::{json as wire_json, proto};
use http_body_util::BodyExt;
use serde_json::{Value, json};

fn native_read(filtered: bool) -> Value {
    let mut read = json!({"table":"app_view","order":[{"field":"id","descending":true}],
        "limit":{"literal":{"intValue":"5"}},"projection":["id"]});
    if filtered {
        read["filter"] = json!({"compare":{"left":{"field":"id"},"comparison":"NATIVE_COMPARISON_GT",
            "right":{"value":{"literal":{"intValue":"5997"}}}}});
    }
    json!({"operation":{"request":{"read":read},"bindings":[]}})
}

fn allow_view(f: &Fixture) {
    f.admin
        .roles()
        .create(giggles_core::roles::NewRole {
            name: "view_reader".into(),
            login: true,
            superuser: true,
            ..Default::default()
        })
        .unwrap();
    let mut bindings = f.bindings.clone();
    bindings[0].role = giggles_server::Role::Database {
        name: "view_reader".into(),
    };
    f.service.reload_bindings(bindings).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_view_native_filter_and_limit_own_their_spill() {
    let _serial = SERIAL.lock().await;
    let f = Fixture::catalog_empty_engine(Engine::V2).await;
    let mut probe = seed(&f, 6000, 1000).await;
    f.admin
        .sql_session()
        .execute("CREATE VIEW app_view AS SELECT id,body FROM app", &[], None)
        .unwrap();
    allow_view(&f);
    let token = f.open_http(&f.app, false).await;
    let mut client = f.grpc_client(&f.app).await;
    let grpc_token = client
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;
    for filtered in [true, false] {
        let request = native_read(filtered);
        let expected: Vec<_> = (if filtered { 5998..=6000 } else { 5996..=6000 })
            .rev()
            .map(|id| json!([{ "intValue":id.to_string() }]))
            .collect();
        for h2 in [false, true] {
            let (body, task) = http_stream(&f, &token, request.clone(), h2).await;
            let bytes = body.collect().await.unwrap().to_bytes();
            let reply: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(reply["code"], "ok", "{reply}");
            let outcome = &reply["operationOutcome"];
            assert_eq!(outcome["matched"], if filtered { "3" } else { "6000" });
            assert_eq!(outcome["complete"], filtered);
            assert_eq!(outcome["columns"], json!(["id"]));
            assert_eq!(
                outcome["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row["values"].clone())
                    .collect::<Vec<_>>(),
                expected
            );
            task.abort();
            probe.cleaned().await;
        }
        let wire = wire_json::decode_execute(&serde_json::to_vec(&request).unwrap()).unwrap();
        let mut stream = client
            .execute_stream(rpc(&grpc_token, wire))
            .await
            .unwrap()
            .into_inner();
        let head = stream.message().await.unwrap().unwrap();
        assert_eq!(head.code, "ok");
        let reply: Value =
            serde_json::from_slice(&wire_json::encode_reply(&head).unwrap()).unwrap();
        assert_eq!(
            reply["operationOutcome"]["matched"],
            if filtered { "3" } else { "6000" }
        );
        assert_eq!(reply["operationOutcome"]["complete"], filtered);
        assert_eq!(reply["operationOutcome"]["columns"], json!(["id"]));
        assert_eq!(
            reply["operationOutcome"]["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["values"].clone())
                .collect::<Vec<_>>(),
            expected
        );
        assert!(stream.message().await.unwrap().is_none());
        probe.cleaned().await;
    }
    stop(f).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_view_streams_every_row_and_cleans_spills() {
    let _serial = SERIAL.lock().await;
    let f = Fixture::catalog_empty_engine(Engine::V2).await;
    let mut probe = seed(&f, 6000, 1000).await;
    f.admin
        .sql_session()
        .execute("CREATE VIEW app_view AS SELECT id,body FROM app", &[], None)
        .unwrap();
    allow_view(&f);
    let mut request = typed_read();
    request["typedRead"]["table"] = json!("app_view");
    let token = f.open_http(&f.app, false).await;
    for h2 in [false, true] {
        let (mut body, task) = http_stream(&f, &token, request.clone(), h2).await;
        let mut bytes = body
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .to_vec();
        assert!(!probe.spill_files().is_empty(), "live HTTP view spill");
        while let Some(frame) = body.frame().await {
            bytes.extend_from_slice(&frame.unwrap().into_data().unwrap());
        }
        let reply: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reply["code"], "ok");
        assert_eq!(reply["operationOutcome"]["matched"], "6000");
        check_rows(
            reply["operationOutcome"]["rows"].as_array().unwrap(),
            6000,
            1000,
            &probe.marker,
        );
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
    let wire = wire_json::decode_execute(&serde_json::to_vec(&request).unwrap()).unwrap();
    let mut stream = client
        .execute_stream(rpc(&token, wire))
        .await
        .unwrap()
        .into_inner();
    let head = stream.message().await.unwrap().unwrap();
    assert_eq!(head.code, "ok");
    assert!(!probe.spill_files().is_empty(), "live gRPC view spill");
    let Some(proto::execute_reply::NativeResult::OperationOutcome(outcome)) = head.native_result
    else {
        panic!("view outcome")
    };
    assert_eq!(outcome.matched, 6000);
    assert!(outcome.rows.is_empty());
    let mut rows = vec![];
    while let Some(reply) = stream.message().await.unwrap() {
        let Some(proto::execute_reply::NativeResult::OperationOutcome(outcome)) =
            reply.native_result
        else {
            panic!("view continuation")
        };
        assert_eq!(outcome.matched, 0);
        rows.extend(outcome.rows);
    }
    assert_eq!(rows.len(), 6000);
    for (offset, row) in rows.iter().enumerate() {
        assert_eq!(
            row.values[0].value,
            Some(proto::value::Value::IntValue(offset as i64 + 1))
        );
        let Some(proto::value::Value::TextValue(body)) = &row.values[1].value else {
            panic!("body")
        };
        assert_eq!(body.len(), 1000);
        assert!(body.starts_with(&probe.marker));
    }
    probe.cleaned().await;
    stop(f).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_legacy_native_read_matches_unary_bytes() {
    let _serial = SERIAL.lock().await;
    let f = Fixture::catalog_empty_engine(Engine::Legacy).await;
    let mut probe = seed(&f, 3, 128).await;
    let mut request = native_read(false);
    request["operation"]["request"]["read"]["table"] = json!("app");
    let wire = wire_json::decode_execute(&serde_json::to_vec(&request).unwrap()).unwrap();
    let mut client = f.grpc_client(&f.app).await;
    let token = client
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;
    let unary = client
        .execute(rpc(&token, wire.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(unary.code, "ok");
    let http_token = f.open_http(&f.app, false).await;
    for h2 in [false, true] {
        let (body, task) = http_stream(&f, &http_token, request.clone(), h2).await;
        assert_eq!(
            body.collect().await.unwrap().to_bytes().as_ref(),
            wire_json::encode_reply(&unary).unwrap()
        );
        task.abort();
    }
    let mut stream = client
        .execute_stream(rpc(&token, wire))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stream.message().await.unwrap().unwrap(), unary);
    assert!(stream.message().await.unwrap().is_none());
    probe.cleaned().await;
    stop(f).await;
}
