//! Native keyset paging (core read_page) over both transports.
mod api_support;
use api_support::{Fixture, rpc};
use giggles_server::api::json as wire_json;
use serde_json::{Value, json};
async fn execute(f: &Fixture, token: &str, request: Value, grpc: bool) -> Value {
    if grpc {
        let wire = wire_json::decode_execute(&serde_json::to_vec(&request).unwrap()).unwrap();
        let reply = f.grpc_client(&f.app).await.execute(rpc(token, wire)).await.unwrap().into_inner();
        serde_json::from_slice(&wire_json::encode_reply(&reply).unwrap()).unwrap()
    } else {
        let (status, bytes) = f.http(&f.app, "POST", "/v1/session/execute", Some(token),
            serde_json::to_vec(&request).unwrap(), false).await.unwrap();
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&bytes));
        serde_json::from_slice(&bytes).unwrap()
    }
}
fn read(table: &str, limit: i64) -> Value {
    json!({"read":{"table":table,"order":[{"field":"id","descending":false}],
        "limit":{"literal":{"intValue":limit.to_string()}},"projection":["id","body"]}})
}
fn paged(request: Value, after: Option<&Value>) -> Value {
    let page = match after { Some(after) => json!({"after":after}), None => json!({}) };
    json!({"operation":{"request":request,"page":page}})
}
fn values(rows: &Value) -> Vec<Value> {
    rows.as_array().unwrap().iter().map(|row| row["values"].clone()).collect()
}
async fn walk(f: &Fixture, token: &str, table: &str, limit: i64, grpc: bool) -> (Vec<Value>, Vec<usize>) {
    let (mut rows, mut lengths, mut after) = (vec![], vec![], None::<Value>);
    loop {
        let reply = execute(f, token, paged(read(table, limit), after.as_ref()), grpc).await;
        assert_eq!(reply["code"], "ok", "{reply}");
        let outcome = &reply["operationOutcome"];
        lengths.push(outcome["rows"].as_array().unwrap().len());
        rows.extend(values(&outcome["rows"]));
        assert_eq!(outcome["complete"], outcome.get("nextPage").is_none(), "{reply}");
        match outcome.get("nextPage") {
            Some(next) => after = Some(next["after"].clone()),
            None => return (rows, lengths),
        }
        assert!(lengths.len() < 20, "paging did not terminate");
    }
}

#[tokio::test]
async fn pages_concatenate_to_sql_order() {
    use giggles_core::{operations as ops, relational::Value as V};
    for grpc in [false, true] {
        let f = Fixture::catalog_empty().await;
        let token = f.open_http(&f.app, false).await;
        assert_eq!(walk(&f, &token, "app", 3, grpc).await, (vec![], vec![0]));
        let mut s = f.admin.sql_session();
        s.execute("INSERT INTO app(id, body) VALUES (5,'e'),(1,'a'),(7,'g'),(3,'c'),(2,'b'),(6,'f'),(4,'d')", &[], None).unwrap();
        let expected = execute(&f, &token, json!({"sql":{"sql":"SELECT id, body FROM app ORDER BY id"}}), grpc).await;
        let (rows, lengths) = walk(&f, &token, "app", 3, grpc).await;
        assert_eq!(rows, values(&expected["sql"]["rows"]));
        assert_eq!(lengths, vec![3, 3, 1]);
        // The continuation equals core's own read_page continuation.
        let engine = ops::Engine::new(f.admin.clone());
        let request = ops::Request::Read { table: "app".into(), filter: None,
            order: vec![ops::Sort { field: "id".into(), descending: false }],
            limit: Some(ops::ValueExpr::Literal(V::Int(3))), projection: vec!["id".into(), "body".into()] };
        let first = engine.read_page(&request, None, &ops::Bindings::new()).unwrap();
        assert_eq!(first.next, Some(vec![V::Int(3)]));
        let reply = execute(&f, &token, paged(read("app", 3), None), grpc).await;
        assert_eq!(reply["operationOutcome"]["nextPage"]["after"], json!([{"intValue":"3"}]), "{reply}");
        // A committed row inserted between pages is seen by the next page, as with core.
        s.execute("INSERT INTO app(id, body) VALUES (8,'h')", &[], None).unwrap();
        let reply = execute(&f, &token, paged(read("app", 3), Some(&json!([{"intValue":"6"}]))), grpc).await;
        assert_eq!(values(&reply["operationOutcome"]["rows"]).len(), 2);
        assert!(reply["operationOutcome"].get("nextPage").is_none());
        // Inside a transaction the page sees the transaction's own writes; ROLLBACK discards them.
        assert_eq!(execute(&f, &token, json!({"begin":{}}), grpc).await["code"], "ok");
        let insert = json!({"operation":{"request":{"insert":{"table":"app","values":[
            {"column":"id","value":{"literal":{"intValue":"9"}}},{"column":"body","value":{"literal":{"textValue":"i"}}}]}}}});
        assert_eq!(execute(&f, &token, insert, grpc).await["code"], "ok");
        let reply = execute(&f, &token, paged(read("app", 3), Some(&json!([{"intValue":"7"}]))), grpc).await;
        assert_eq!(values(&reply["operationOutcome"]["rows"]).len(), 2, "{reply}");
        assert_eq!(execute(&f, &token, json!({"rollback":{}}), grpc).await["code"], "ok");
        assert_eq!(walk(&f, &token, "app", 3, grpc).await.0.len(), 8);
        drop(s);
        f.stop().await;
    }
}

#[tokio::test]
async fn paging_errors_match_core_and_authority_comes_first() {
    use giggles_core::{operations as ops, relational::Value as V};
    for grpc in [false, true] {
        let f = Fixture::catalog_empty().await;
        f.admin.sql_session().execute("INSERT INTO app(id, body) VALUES (1,'a'),(2,'b')", &[], None).unwrap();
        let token = f.open_http(&f.app, false).await;
        let engine = ops::Engine::new(f.admin.clone());
        let request = ops::Request::Read { table: "app".into(), filter: None,
            order: vec![ops::Sort { field: "id".into(), descending: false }],
            limit: Some(ops::ValueExpr::Literal(V::Int(1))), projection: vec!["id".into(), "body".into()] };
        for (after, core_after) in [
            (json!([{"intValue":"1"},{"intValue":"2"}]), vec![V::Int(1), V::Int(2)]),
            (json!([{"textValue":"x"}]), vec![V::Text("x".into())]),
        ] {
            let expected = engine.read_page(&request, Some(&core_after), &ops::Bindings::new()).unwrap_err();
            let reply = execute(&f, &token, paged(read("app", 1), Some(&after)), grpc).await;
            assert_eq!(reply["code"], "native_error", "{reply}");
            let encoded = format!("{expected:?}");
            match &expected {
                ops::Error::Sql { sqlstate } => assert_eq!(reply["nativeError"]["sqlstate"], sqlstate.as_str(), "{reply} vs {encoded}"),
                _ => assert!(reply["nativeError"]["sqlstate"].is_null(), "{reply} vs {encoded}"),
            }
        }
        // Paging applies only to reads; the core error class is preserved.
        let insert = json!({"insert":{"table":"app","values":[{"column":"id","value":{"literal":{"intValue":"9"}}}]}});
        let reply = execute(&f, &token, paged(insert, None), grpc).await;
        assert_eq!(reply["nativeError"]["kind"], "NATIVE_ERROR_CLASS_UNSUPPORTED", "{reply}");
        assert_eq!(f.admin.sql_session().execute("SELECT count(*) FROM app", &[], None).unwrap().rows[0].values[0], V::Int(2));
        // A table outside the Application allowlist is refused before the continuation is checked.
        let reply = execute(&f, &token, paged(read("other", 1), Some(&json!([{"textValue":"bad"},{"textValue":"arity"}]))), grpc).await;
        assert_eq!(reply["code"], "42501", "{reply}");
        f.stop().await;
    }
}

#[tokio::test]
async fn malformed_page_requests() {
    let f = Fixture::catalog_empty().await;
    let token = f.open_http(&f.app, false).await;
    for page in [json!(1), json!({"x":1}), json!({"after":1}), json!({"after":[1]}), json!(null)] {
        let body = json!({"operation":{"request":read("app", 1),"page":page}});
        let (status, reply) = f.http(&f.app, "POST", "/v1/session/execute", Some(&token), serde_json::to_vec(&body).unwrap(), false).await.unwrap();
        assert_eq!(status, 400, "{body}: {}", String::from_utf8_lossy(&reply));
    }
    use giggles_server::api::proto as p;
    let mut request = wire_json::decode_execute(&serde_json::to_vec(&paged(read("app", 1), None)).unwrap()).unwrap();
    if let Some(p::execute_request::Request::Operation(op)) = &mut request.request {
        op.page = Some(p::NativePage { after: vec![p::Value { value: Some(p::value::Value::IntValue(1)) }; 1601] });
    } else { unreachable!() }
    assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token, request)).await.unwrap_err().code(), tonic::Code::InvalidArgument);
    f.stop().await;
}
