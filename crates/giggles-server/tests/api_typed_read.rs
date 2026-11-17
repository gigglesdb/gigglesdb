//! Typed reads use the SQL kernel through real authenticated transports.
mod api_support;
use api_support::{Fixture, rpc};
use giggles_server::api::{codec, json as wire_json};
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
fn column(name: &str) -> Value { json!({"column":{"name":name}}) }
fn qualified(source: &str, name: &str) -> Value { json!({"column":{"source":source,"name":name}}) }
fn aggregate(function: &str) -> Value { json!({"aggregate":{"function":function}}) }
fn compare(left: Value, op: &str, right: Value) -> Value {
    json!({"compare":{"left":left,"comparison":format!("NATIVE_COMPARISON_{op}"),"right":right}})
}
fn literal(n: i64) -> Value { json!({"literal":{"intValue":n.to_string()}}) }
fn aggregate_read() -> Value {
    json!({"typedRead":{"table":"app","projection":[
        {"expression":column("body"),"alias":"body"},
        {"expression":aggregate("count_star"),"alias":"n"},
        {"expression":{"aggregate":{"function":"sum","column":{"name":"id"}}},"alias":"total"}],
        "groupBy":[column("body")],"having":compare(aggregate("count_star"),"GT",literal(0)),
        "orderBy":[{"expression":column("body"),"descending":false}],"limit":"10"}})
}
fn join_read(table: &str, left: bool) -> Value {
    json!({"typedRead":{"table":"app","join":{"table":table,"leftAlias":"a","rightAlias":"b",
        "left":left,"on":compare(qualified("a","id"), if left {"LT"} else {"EQ"},qualified("b","id"))},
        "projection":[{"expression":qualified("a","id"),"alias":"l"},
            {"expression":qualified("b","id"),"alias":"r"}],
        "filter":compare(qualified("a","id"),"GT",literal(1)),
        "orderBy":[{"expression":qualified("a","id"),"descending":false},
            {"expression":qualified("b","id"),"descending":false,"nullsFirst":true}],"limit":"10"}})
}

#[tokio::test]
async fn aggregate_and_join_rows_equal_sql_in_retained_sessions() {
    let f = Fixture::new().await;
    f.admin.sql_session().execute("INSERT INTO app(id,body) VALUES (1,'a'),(2,'a'),(3,'b')", &[], None).unwrap();
    let token = f.open_http(&f.app, false).await;
    for grpc in [false, true] {
        execute(&f, &token, json!({"begin":{}}), grpc).await;
        execute(&f, &token, json!({"sql":{"sql":"INSERT INTO app(id,body) VALUES (4,'c')"}}), grpc).await;
        let mut grouped_join = join_read("app", false);
        grouped_join["typedRead"]["projection"][1] = json!({"expression":{"aggregate":{
            "function":"sum","column":{"source":"b","name":"id"}}},"alias":"total"});
        grouped_join["typedRead"]["groupBy"] = json!([qualified("a","id")]);
        grouped_join["typedRead"]["orderBy"] = json!([{"expression":qualified("a","id"),"descending":false}]);
        for (request, sql) in [
            (aggregate_read(), "SELECT body,count(*) AS n,sum(id) AS total FROM app GROUP BY body HAVING count(*)>0 ORDER BY body LIMIT 10"),
            (join_read("app", false), "SELECT a.id AS l,b.id AS r FROM app a INNER JOIN app b ON a.id=b.id WHERE a.id>1 ORDER BY a.id,b.id NULLS FIRST LIMIT 10"),
            (join_read("app", true), "SELECT a.id AS l,b.id AS r FROM app a LEFT JOIN app b ON a.id<b.id WHERE a.id>1 ORDER BY a.id,b.id NULLS FIRST LIMIT 10"),
            (grouped_join, "SELECT a.id AS l,sum(b.id) AS total FROM app a INNER JOIN app b ON a.id=b.id WHERE a.id>1 GROUP BY a.id ORDER BY a.id LIMIT 10"),
        ] {
            let reply = execute(&f, &token, request, grpc).await;
            assert_eq!(reply["code"], "ok", "{reply}");
            assert_eq!(reply["transactionState"], "TRANSACTION_STATE_IN_TRANSACTION");
            let sql_reply = execute(&f, &token, json!({"sql":{"sql":sql}}), grpc).await;
            assert_eq!(sql_reply["code"], "ok", "{sql_reply}");
            assert_eq!(reply["operationOutcome"]["columns"], sql_reply["sql"]["columns"]);
            assert_eq!(reply["operationOutcome"]["rows"], sql_reply["sql"]["rows"]);
            assert_eq!(reply["operationOutcome"]["affected"], "0");
        }
        execute(&f, &token, json!({"rollback":{}}), grpc).await;
    }
    f.stop().await;
}

#[tokio::test]
async fn all_sources_authorize_before_missing_table_or_column_binding() {
    use giggles_core::roles::{NewRole, TablePrivilege::Select};
    let f = Fixture::new().await;
    let app_token = f.open_http(&f.app, false).await;
    for grpc in [false, true] {
        for table in ["other", "missing"] {
            assert_eq!(execute(&f, &app_token, join_read(table, true), grpc).await["code"], "42501");
        }
    }
    f.admin.roles().create(NewRole { name: "reader".into(), login: true, ..NewRole::default() }).unwrap();
    f.admin.roles().grant_table("app", "reader", &[Select]).unwrap();
    let mut bindings = f.bindings.clone();
    bindings[0].role = giggles_server::Role::Database { name: "reader".into() };
    f.service.reload_bindings(bindings).unwrap();
    let token = f.open_http(&f.app, false).await;
    for grpc in [false, true] {
        assert_eq!(execute(&f, &token, aggregate_read(), grpc).await["code"], "ok");
        for table in ["other", "missing"] {
            let mut request = join_read(table, false);
            request["typedRead"]["projection"][0]["expression"] = column("absent");
            assert_eq!(execute(&f, &token, request, grpc).await["code"], "42501");
        }
    }
    f.admin.roles().grant_table("other", "reader", &[Select]).unwrap();
    assert_eq!(execute(&f, &token, join_read("other", false), true).await["code"], "ok");
    f.admin.roles().revoke_table("app", "reader", &[Select]).unwrap();
    assert_eq!(execute(&f, &token, join_read("other", false), false).await["code"], "42501");
    f.stop().await;
}

#[tokio::test]
async fn malformed_typed_reads_reject_before_session_lease() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    execute(&f, &token, json!({"begin":{}}), false).await;
    let mut cases = vec![json!({"typedRead":{"table":"app"}})];
    for (key, value) in [("projection", json!([{"expression":aggregate("unknown")} ])),
        ("filter", column("id")), ("groupBy", json!([literal(1)])),
        ("orderBy", json!([{"expression":compare(column("id"),"EQ",literal(1)),"descending":false}]))] {
        let mut request = aggregate_read();
        request["typedRead"][key] = value;
        cases.push(request);
    }
    for request in cases {
        let bytes = serde_json::to_vec(&request).unwrap();
        let (status, _) = f.http(&f.app, "POST", "/v1/session/execute", Some(&token), bytes.clone(), false).await.unwrap();
        assert_eq!(status, 400, "{request}");
        let wire = wire_json::decode_execute(&bytes).unwrap();
        assert!(codec::decode_execute(wire.clone()).is_err());
        assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token, wire)).await.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
    for bytes in [br#"{"typedRead":{"table":"app","unknown":true}}"#.as_slice(),
        br#"{"typedRead":{"table":"app","table":"other"}}"#, br#"{"typedRead":{"table":"app","limit":"01"}}"#] {
        assert!(wire_json::decode_execute(bytes).is_err());
    }
    let reply = execute(&f, &token, aggregate_read(), true).await;
    assert_eq!(reply["code"], "ok");
    assert_eq!(reply["transactionState"], "TRANSACTION_STATE_IN_TRANSACTION");
    let mut missing = aggregate_read();
    missing["typedRead"]["projection"][0]["expression"] = column("absent");
    let reply = execute(&f, &token, missing, true).await;
    assert_eq!(reply["nativeError"]["kind"], "NATIVE_ERROR_CLASS_SQL");
    assert_eq!(reply["nativeError"]["sqlstate"], "42703");
    assert_eq!(reply["transactionState"], "TRANSACTION_STATE_ABORTED");
    execute(&f, &token, json!({"rollback":{}}), false).await;
    f.stop().await;
}

#[test]
fn protobuf_read_shapes_are_bounded_before_recursive_conversion() {
    use giggles_server::api::{error::ApiError, proto as p};
    use p::native_read_expr::Expression as E;
    let leaf = p::NativeReadExpr { expression: Some(E::Literal(p::Value {
        value: Some(p::value::Value::IntValue(1)),
    })) };
    let request = |expression| p::ExecuteRequest {
        request: Some(p::execute_request::Request::TypedRead(p::NativeTypedRead {
            table: Some("app".into()), projection: vec![p::NativeReadProjection {
                expression: Some(expression), alias: None,
            }], ..Default::default()
        })), budget_ms: None, operation_id: None,
    };
    let mut deep = leaf.clone();
    for _ in 0..25 { deep = p::NativeReadExpr { expression: Some(E::NotExpr(Box::new(deep))) }; }
    assert_eq!(codec::decode_execute(request(deep)).unwrap_err(), ApiError::InvalidArgument);
    let mut wide = leaf;
    for _ in 0..12 {
        wide = p::NativeReadExpr { expression: Some(E::AndExpr(Box::new(p::NativeReadLogic {
            left: Some(Box::new(wide.clone())), right: Some(Box::new(wide)),
        }))) };
    }
    assert_eq!(codec::decode_execute(request(wide)).unwrap_err(), ApiError::InvalidArgument);
}
