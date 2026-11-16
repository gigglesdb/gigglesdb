//! Native CREATE/DROP SEQUENCE and nextval/currval/setval over both transports, compared with SQL.
mod api_support;
use api_support::{Fixture, rpc};
use giggles_core::relational::Value as V;
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
fn code(reply: &Value) -> String {
    if reply["code"] == "native_error" && !reply["nativeError"]["sqlstate"].is_null() {
        assert_eq!(reply["nativeError"]["kind"], "NATIVE_ERROR_CLASS_SQL", "{reply}");
        reply["nativeError"]["sqlstate"].as_str().unwrap().into()
    } else { reply["code"].as_str().unwrap().into() }
}
fn create(name: &str) -> Value { json!({"catalog":{"createSequence":{"name":name}}}) }
fn drop(name: &str, cascade: bool) -> Value { json!({"catalog":{"dropSequence":{"name":name,"cascade":cascade}}}) }
fn call(kind: &str, name: &str) -> Value { json!({"sequence":{kind:{"name":name}}}) }
fn setval(name: &str, value: i64, called: Option<bool>) -> Value {
    let mut v = json!({"name":name,"value":value.to_string()});
    if let Some(called) = called { v["isCalled"] = json!(called); }
    json!({"sequence":{"setval":v}})
}
/// Compare a native reply with the SQL statement's result: same SQLSTATE, or same column and value.
fn same(reply: &Value, sql: Result<giggles_core::relational::SqlResult, giggles_core::CoreError>, what: &str) {
    match sql {
        Err(error) => assert_eq!(code(reply), giggles_server::sqlstate(&error), "{what}: {reply}"),
        Ok(result) => {
            assert_eq!(code(reply), "ok", "{what}: {reply}");
            if result.columns.is_empty() { return; }
            assert_eq!(reply["sql"]["columns"], json!(result.columns), "{what}: {reply}");
            let V::Int(value) = result.rows[0].values[0] else { panic!("{what}: {result:?}") };
            assert_eq!(reply["sql"]["rows"][0]["values"], json!([{"intValue": value.to_string()}]), "{what}: {reply}");
        }
    }
}
async fn database(f: &Fixture, role: &str) -> String {
    let mut b = f.bindings.clone();
    b[0].role = giggles_server::Role::Database { name: role.into() };
    f.service.reload_bindings(b).unwrap();
    f.open_http(&f.app, false).await
}
fn roles(f: &Fixture) {
    use giggles_core::roles::NewRole;
    for (name, superuser) in [("owner", true), ("reader", false), ("other", false)] {
        f.admin.roles().create(NewRole { name: name.into(), login: true, superuser, ..Default::default() }).unwrap();
    }
    f.service.role_session("owner").unwrap().execute_sql("CREATE SCHEMA app", &[]).unwrap();
}

#[tokio::test]
async fn sequences_match_sql_twins() {
    for grpc in [false, true] {
        let f = Fixture::catalog_empty().await;
        let twin = Fixture::catalog_empty().await;
        roles(&f); roles(&twin);
        let token = database(&f, "owner").await;
        let mut sql = twin.service.role_session("owner").unwrap();
        let reply = execute(&f, &token, create("app.s"), grpc).await;
        same(&reply, sql.execute_sql("CREATE SEQUENCE app.s", &[]), "create");
        assert_eq!(reply["catalogOutcome"]["table"], "app.s", "{reply}");
        assert_eq!(reply["catalogOutcome"]["columns"], json!([]), "{reply}");
        for (request, q) in [
            (create("app.s"), "CREATE SEQUENCE app.s"),
            (call("currval", "app.s"), "SELECT currval('app.s')"),
            (call("nextval", "app.s"), "SELECT nextval('app.s')"),
            (call("nextval", "app.s"), "SELECT nextval('app.s')"),
            (call("currval", "app.s"), "SELECT currval('app.s')"),
            (setval("app.s", 10, None), "SELECT setval('app.s', 10)"),
            (call("nextval", "app.s"), "SELECT nextval('app.s')"),
            (setval("app.s", 20, Some(false)), "SELECT setval('app.s', 20, false)"),
            (call("nextval", "app.s"), "SELECT nextval('app.s')"),
            (setval("app.s", 30, Some(true)), "SELECT setval('app.s', 30, true)"),
            (call("nextval", "app.s"), "SELECT nextval('app.s')"),
            (call("nextval", "app.missing"), "SELECT nextval('app.missing')"),
            (create("nosuch.s"), "CREATE SEQUENCE nosuch.s"),
            (drop("app.missing", false), "DROP SEQUENCE app.missing"),
        ] {
            let reply = execute(&f, &token, request, grpc).await;
            same(&reply, sql.execute_sql(q, &[]), q);
        }
        // Dependencies: a column default keeps the sequence unless CASCADE.
        for fixture in [&f, &twin] {
            fixture.service.role_session("owner").unwrap()
                .execute_sql("CREATE TABLE app.t(id BIGINT PRIMARY KEY DEFAULT nextval('app.s'), v BIGINT)", &[]).unwrap();
        }
        for (request, q) in [(drop("app.s", false), "DROP SEQUENCE app.s"), (drop("app.s", true), "DROP SEQUENCE app.s CASCADE")] {
            let reply = execute(&f, &token, request, grpc).await;
            same(&reply, sql.execute_sql(q, &[]), q);
        }
        // The session search_path resolves bare names like SQL.
        assert_eq!(code(&execute(&f, &token, json!({"searchPath":{"set":true,"schemas":["app"]}}), grpc).await), "ok");
        assert_eq!(code(&execute(&f, &token, create("bare"), grpc).await), "ok");
        assert_eq!(code(&execute(&f, &token, call("nextval", "bare"), grpc).await), "ok");
        assert_eq!(f.service.role_session("owner").unwrap().execute_sql("SELECT nextval('app.bare')", &[]).unwrap().rows[0].values[0], V::Int(2));
        // CREATE SEQUENCE is transactional; nextval is not undone by ROLLBACK (PG).
        for request in [json!({"begin":{}}), create("app.tx"), call("nextval", "app.tx"), json!({"rollback":{}})] {
            assert_eq!(code(&execute(&f, &token, request, grpc).await), "ok");
        }
        assert_eq!(code(&execute(&f, &token, call("nextval", "app.tx"), grpc).await), "42P01");
        for request in [json!({"begin":{}}), call("nextval", "app.bare"), json!({"rollback":{}})] {
            assert_eq!(code(&execute(&f, &token, request, grpc).await), "ok");
        }
        assert_eq!(f.service.role_session("owner").unwrap().execute_sql("SELECT nextval('app.bare')", &[]).unwrap().rows[0].values[0], V::Int(4));
        // A failing call aborts an open block like SQL.
        for (request, expected) in [(json!({"begin":{}}), "ok"), (call("currval", "app.missing"), "42P01"), (call("nextval", "app.bare"), "25P02"), (json!({"rollback":{}}), "ok")] {
            assert_eq!(code(&execute(&f, &token, request, grpc).await), expected);
        }
        f.stop().await; twin.stop().await;
    }
}

#[tokio::test]
async fn sequence_authority_matches_sql_before_binding() {
    use giggles_core::roles::SchemaPrivilege;
    for grpc in [false, true] {
        let f = Fixture::catalog_empty().await;
        roles(&f);
        f.service.role_session("owner").unwrap().execute_sql("CREATE SEQUENCE app.owned", &[]).unwrap();
        // Application role: no DDL, and no access to a standalone sequence.
        let app = f.open_http(&f.app, false).await;
        for request in [create("app.x"), drop("app.owned", false), call("nextval", "app.owned"), call("nextval", "app.missing")] {
            assert_eq!(code(&execute(&f, &app, request, grpc).await), "42501");
        }
        let reader = database(&f, "reader").await;
        let check = |reply: Value, q: &str, role: &str, expected: &str| {
            assert_eq!(code(&reply), expected, "{q}: {reply}");
            let result = f.service.role_session(role).unwrap().execute_sql(q, &[]);
            assert_eq!(result.as_ref().err().map(giggles_server::sqlstate).unwrap_or("ok"), expected, "{q}: {result:?}");
        };
        check(execute(&f, &reader, create("r1"), grpc).await, "CREATE SEQUENCE r2", "reader", "42501");
        check(execute(&f, &reader, call("nextval", "app.owned"), grpc).await, "SELECT nextval('app.owned')", "reader", "42501");
        check(execute(&f, &reader, setval("app.owned", 5, None), grpc).await, "SELECT setval('app.owned', 5)", "reader", "42501");
        check(execute(&f, &reader, drop("app.owned", false), grpc).await, "DROP SEQUENCE app.owned", "reader", "42501");
        // Refused before binding: a missing sequence is 42501, not 42P01.
        check(execute(&f, &reader, call("nextval", "app.missing"), grpc).await, "SELECT nextval('app.missing')", "reader", "42501");
        check(execute(&f, &reader, drop("app.missing", false), grpc).await, "DROP SEQUENCE app.missing", "reader", "42501");
        f.admin.roles().grant_schema("public", "reader", &[SchemaPrivilege::Create, SchemaPrivilege::Usage]).unwrap();
        check(execute(&f, &reader, create("r1"), grpc).await, "CREATE SEQUENCE r2", "reader", "ok");
        // The creator owns the sequence, natively as with SQL.
        check(execute(&f, &reader, call("nextval", "r1"), grpc).await, "SELECT nextval('r2')", "reader", "ok");
        let other = database(&f, "other").await;
        assert_eq!(code(&execute(&f, &other, call("nextval", "r1"), grpc).await), "42501");
        assert_eq!(code(&execute(&f, &other, drop("r1", false), grpc).await), "42501");
        let other_sql = f.service.role_session("other").unwrap().execute_sql("SELECT nextval('r2')", &[]);
        assert_eq!(giggles_server::sqlstate(&other_sql.unwrap_err()), "42501");
        let reader = database(&f, "reader").await;
        check(execute(&f, &reader, drop("r1", false), grpc).await, "DROP SEQUENCE r2", "reader", "ok");
        f.stop().await;
    }
}

#[tokio::test]
async fn malformed_sequence_requests() {
    let f = Fixture::catalog_empty().await;
    let token = f.open_http(&f.app, false).await;
    for body in [
        json!({"sequence":{}}), json!({"sequence":{"nextval":{}}}), json!({"sequence":{"nextval":{"name":""}}}),
        json!({"sequence":{"nextval":{"name":"s","extra":1}}}), json!({"sequence":{"lastval":{"name":"s"}}}),
        json!({"sequence":{"setval":{"name":"s"}}}), json!({"sequence":{"setval":{"name":"s","value":"x"}}}),
        json!({"sequence":{"nextval":{"name":"s"},"currval":{"name":"s"}}}),
        json!({"catalog":{"createSequence":{}}}), json!({"catalog":{"createSequence":{"name":""}}}),
        json!({"catalog":{"createSequence":{"name":"s","cascade":true}}}), json!({"catalog":{"dropSequence":{"name":""}}}),
    ] {
        let bytes = serde_json::to_vec(&body).unwrap();
        let (status, reply) = f.http(&f.app, "POST", "/v1/session/execute", Some(&token), bytes.clone(), false).await.unwrap();
        assert_eq!(status, 400, "{body}: {}", String::from_utf8_lossy(&reply));
        if let Ok(request) = wire_json::decode_execute(&bytes) {
            assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token, request)).await.unwrap_err().code(), tonic::Code::InvalidArgument, "{body}");
        }
    }
    use giggles_server::api::proto as p;
    use p::native_sequence_call::Call as K;
    for call in [None, Some(K::Nextval(p::NativeSequenceName { name: None })), Some(K::Currval(p::NativeSequenceName { name: Some("a\0b".into()) })),
        Some(K::Setval(p::NativeSetval { name: Some("s".into()), value: None, is_called: None }))] {
        let request = p::ExecuteRequest { request: Some(p::execute_request::Request::Sequence(p::NativeSequenceCall { call })), ..Default::default() };
        assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token, request)).await.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
    for request in [p::native_catalog_request::Request::CreateSequence(p::NativeCreateSequence { name: None }),
        p::native_catalog_request::Request::DropSequence(p::NativeDropSequence { name: Some(String::new()), cascade: None })] {
        let request = p::ExecuteRequest { request: Some(p::execute_request::Request::Catalog(p::NativeCatalogRequest { request: Some(request) })), ..Default::default() };
        assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token, request)).await.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
    f.stop().await;
}
