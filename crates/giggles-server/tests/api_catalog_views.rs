//! Native CREATE/DROP VIEW over both transports, compared with SQL twins.
#![cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
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
fn code(reply: &Value) -> String {
    // Core keeps some classes (42P07 is NATIVE_ERROR_CLASS_DUPLICATE) and always carries the SQLSTATE.
    if reply["code"] == "native_error" && !reply["nativeError"]["sqlstate"].is_null() {
        reply["nativeError"]["sqlstate"].as_str().unwrap().into()
    } else { reply["code"].as_str().unwrap().into() }
}
fn sql_code(result: Result<giggles_core::relational::SqlResult, giggles_core::CoreError>) -> String {
    result.as_ref().err().map(giggles_server::sqlstate).unwrap_or("ok").into()
}
fn column(source: Option<&str>, name: &str) -> Value {
    match source { Some(s) => json!({"column":{"source":s,"name":name}}), None => json!({"column":{"name":name}}) }
}
fn single(name: &str, table: &str) -> Value {
    json!({"catalog":{"createView":{"name":name,"read":{"table":table,"projection":[
        {"expression":column(None,"id")},{"expression":column(None,"v")}]}}}})
}
fn joined(name: &str) -> Value {
    json!({"catalog":{"createView":{"name":name,"read":{"table":"app.t","join":{"table":"app.u",
        "leftAlias":"a","rightAlias":"b","left":false,"on":{"compare":{"left":column(Some("a"),"id"),
        "comparison":"NATIVE_COMPARISON_EQ","right":column(Some("b"),"t_id")}}},
        "projection":[{"expression":column(Some("a"),"id")},{"expression":column(Some("b"),"id"),"alias":"u_id"}]}}}})
}
fn drop(name: &str, cascade: bool) -> Value { json!({"catalog":{"dropView":{"name":name,"cascade":cascade}}}) }
async fn database(f: &Fixture, role: &str) -> String {
    let mut b = f.bindings.clone();
    b[0].role = giggles_server::Role::Database { name: role.into() };
    f.service.reload_bindings(b).unwrap();
    f.open_http(&f.app, false).await
}
fn setup(f: &Fixture) {
    use giggles_core::roles::NewRole;
    for (name, superuser) in [("owner", true), ("reader", false), ("other", false)] {
        f.admin.roles().create(NewRole { name: name.into(), login: true, superuser, ..Default::default() }).unwrap();
    }
    let mut s = f.service.role_session("owner").unwrap();
    for q in ["CREATE SCHEMA app", "CREATE TABLE app.t(id BIGINT PRIMARY KEY, v BIGINT)",
        "CREATE TABLE app.u(id BIGINT PRIMARY KEY, t_id BIGINT)"] { s.execute_sql(q, &[]).unwrap(); }
}
fn rows(f: &Fixture, view: &str) -> (Vec<String>, Vec<Vec<giggles_core::relational::Value>>) {
    let r = f.service.role_session("owner").unwrap().execute_sql(&format!("SELECT * FROM {view} ORDER BY 1, 2"), &[]).unwrap();
    (r.columns, r.rows.into_iter().map(|r| r.values).collect())
}

// Core runs SQL and native views on the V2 engine only (Legacy: 0A000 for both).
#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn views_match_sql_twins() {
    for grpc in [false, true] {
        let f = Fixture::catalog_empty_engine(giggles_core::Engine::V2).await;
        let twin = Fixture::catalog_empty_engine(giggles_core::Engine::V2).await;
        setup(&f); setup(&twin);
        let token = database(&f, "owner").await;
        let mut sql = twin.service.role_session("owner").unwrap();
        let cases = [
            (single("app.v1", "app.t"), "CREATE VIEW app.v1 AS SELECT id, v FROM app.t", "ok"),
            (single("app.v1", "app.t"), "CREATE VIEW app.v1 AS SELECT id, v FROM app.t", "42P07"),
            (single("app.bad", "app.missing"), "CREATE VIEW app.bad AS SELECT id, v FROM app.missing", "42P01"),
            (single("nosuch.v", "app.t"), "CREATE VIEW nosuch.v AS SELECT id, v FROM app.t", "3F000"),
            // GigglesDB SQL does not create join views yet (0A000); native matches it (PG18 accepts them).
            (joined("app.v2"), "CREATE VIEW app.v2 AS SELECT a.id, b.id AS u_id FROM app.t a JOIN app.u b ON a.id = b.t_id", "0A000"),
            (single("app.v3", "app.v1"), "CREATE VIEW app.v3 AS SELECT id, v FROM app.v1", "ok"),
            (drop("app.v1", false), "DROP VIEW app.v1", "2BP01"),
            (drop("app.missing", false), "DROP VIEW app.missing", "42P01"),
        ];
        for (request, q, expected) in cases {
            let reply = execute(&f, &token, request, grpc).await;
            assert_eq!(code(&reply), expected, "{q}: {reply}");
            assert_eq!(sql_code(sql.execute_sql(q, &[])), expected, "{q}");
            if expected == "ok" {
                let name = q.split_whitespace().nth(2).unwrap();
                assert_eq!(reply["catalogOutcome"]["table"], name, "{reply}");
                assert!(reply["catalogOutcome"]["indexId"].is_null());
            }
        }
        for fixture in [&f, &twin] {
            let mut s = fixture.service.role_session("owner").unwrap();
            for q in ["INSERT INTO app.t VALUES (1, 10), (2, 20), (3, 30)", "INSERT INTO app.u VALUES (7, 1), (8, 1), (9, 3)"] {
                s.execute_sql(q, &[]).unwrap();
            }
        }
        for view in ["app.v1", "app.v3"] {
            let (native, expected) = (rows(&f, view), rows(&twin, view));
            assert_eq!(native, expected, "{view}");
            assert!(!native.1.is_empty(), "{view}");
        }
        // The session search_path resolves bare view names like SQL.
        assert_eq!(code(&execute(&f, &token, json!({"searchPath":{"set":true,"schemas":["app"]}}), grpc).await), "ok");
        for (request, q, expected) in [(drop("v1", true), "DROP VIEW app.v1 CASCADE", "ok"), (drop("v2", false), "DROP VIEW app.v2", "42P01")] {
            let reply = execute(&f, &token, request, grpc).await;
            assert_eq!(code(&reply), expected, "{reply}");
            assert_eq!(sql_code(sql.execute_sql(q, &[])), expected, "{q}");
        }
        for view in ["app.v1", "app.v2", "app.v3"] {
            for fixture in [&f, &twin] {
                let r = fixture.service.role_session("owner").unwrap().execute_sql(&format!("SELECT * FROM {view}"), &[]);
                assert_eq!(sql_code(r), "42P01", "{view}");
            }
        }
        // A view created inside a rolled-back block disappears like SQL.
        assert_eq!(code(&execute(&f, &token, json!({"begin":{}}), grpc).await), "ok");
        assert_eq!(code(&execute(&f, &token, single("app.tx", "app.t"), grpc).await), "ok");
        assert_eq!(code(&execute(&f, &token, json!({"rollback":{}}), grpc).await), "ok");
        assert_eq!(sql_code(f.service.role_session("owner").unwrap().execute_sql("SELECT * FROM app.tx", &[])), "42P01");
        f.stop().await; twin.stop().await;
    }
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn view_authority_matches_sql_before_binding() {
    use giggles_core::roles::SchemaPrivilege;
    for grpc in [false, true] {
        let f = Fixture::catalog_empty_engine(giggles_core::Engine::V2).await;
        setup(&f);
        let app = f.open_http(&f.app, false).await;
        assert_eq!(code(&execute(&f, &app, single("app.v", "app.t"), grpc).await), "42501");
        assert_eq!(code(&execute(&f, &app, drop("app.v", false), grpc).await), "42501");
        let reader = database(&f, "reader").await;
        let check = |reply: Value, q: &str, expected: &str| {
            assert_eq!(code(&reply), expected, "{q}: {reply}");
            assert_eq!(sql_code(f.service.role_session("reader").unwrap().execute_sql(q, &[])), expected, "{q}");
        };
        // No CREATE on the schema: refused before the read target is bound (no 42P01).
        check(execute(&f, &reader, single("app.r1", "app.t"), grpc).await, "CREATE VIEW app.r1 AS SELECT id, v FROM app.t", "42501");
        check(execute(&f, &reader, single("app.r1", "app.missing"), grpc).await, "CREATE VIEW app.r1 AS SELECT id, v FROM app.missing", "42501");
        f.admin.roles().grant_schema("app", "reader", &[SchemaPrivilege::Create, SchemaPrivilege::Usage]).unwrap();
        // CREATE on the schema but no SELECT on the read table.
        check(execute(&f, &reader, single("app.no_select", "app.t"), grpc).await, "CREATE VIEW app.no_select_sql AS SELECT id, v FROM app.t", "ok");
        check(execute(&f, &reader, joined("app.no_select_join"), grpc).await, "CREATE VIEW app.no_select_join_sql AS SELECT a.id, b.id AS u_id FROM app.t a JOIN app.u b ON a.id = b.t_id", "0A000");
        f.admin.roles().grant_table("app t", "reader", &[giggles_core::roles::TablePrivilege::Select]).unwrap();
        check(execute(&f, &reader, joined("app.partial_select_join"), grpc).await, "CREATE VIEW app.partial_select_join_sql AS SELECT a.id, b.id AS u_id FROM app.t a JOIN app.u b ON a.id = b.t_id", "0A000");
        f.admin.roles().grant_table("app u", "reader", &[giggles_core::roles::TablePrivilege::Select]).unwrap();
        check(execute(&f, &reader, single("app.r1", "app.t"), grpc).await, "CREATE VIEW app.r2 AS SELECT id, v FROM app.t", "ok");
        check(execute(&f, &reader, joined("app.r3"), grpc).await, "CREATE VIEW app.r4 AS SELECT a.id, b.id AS u_id FROM app.t a JOIN app.u b ON a.id = b.t_id", "0A000");
        // Views belong to their creator: another role cannot drop them, the creator can.
        f.service.role_session("owner").unwrap().execute_sql("CREATE VIEW app.owned AS SELECT id FROM app.t", &[]).unwrap();
        check(execute(&f, &reader, drop("app.owned", false), grpc).await, "DROP VIEW app.owned", "42501");
        let other = database(&f, "other").await;
        assert_eq!(code(&execute(&f, &other, drop("app.r1", false), grpc).await), "42501");
        assert_eq!(sql_code(f.service.role_session("other").unwrap().execute_sql("DROP VIEW app.r2", &[])), "42501");
        let reader = database(&f, "reader").await;
        check(execute(&f, &reader, drop("app.r1", false), grpc).await, "DROP VIEW app.r2", "ok");
        // Never created (join views are 0A000): missing views get PostgreSQL 42P01.
        check(execute(&f, &reader, drop("app.r3", false), grpc).await, "DROP VIEW app.r4", "42P01");
        f.stop().await;
    }
}

#[tokio::test]
async fn malformed_view_requests() {
    let f = Fixture::catalog_empty().await;
    let token = f.open_http(&f.app, false).await;
    let read = json!({"table":"t","projection":[{"expression":column(None,"id")}]});
    for body in [
        json!({"createView":{"name":"","read":read}}),
        json!({"createView":{"name":"v"}}),
        json!({"createView":{"name":"v","read":{"table":"t","projection":[]}}}),
        json!({"createView":{"name":"v","read":{"table":"t"}}}),
        json!({"createView":{"name":"v","read":read,"replace":true}}),
        json!({"dropView":{"name":""}}),
        json!({"dropView":{}}),
        json!({"dropView":{"name":"v","cascade":"yes"}}),
    ] {
        let bytes = serde_json::to_vec(&json!({"catalog":body})).unwrap();
        let (status, reply) = f.http(&f.app, "POST", "/v1/session/execute", Some(&token), bytes.clone(), false).await.unwrap();
        assert_eq!(status, 400, "{body}: {}", String::from_utf8_lossy(&reply));
        // Shapes protobuf can carry must fail the same way over gRPC.
        if let Ok(request) = wire_json::decode_execute(&bytes) {
            assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token, request)).await.unwrap_err().code(), tonic::Code::InvalidArgument, "{body}");
        }
    }
    use giggles_server::api::proto as p;
    use p::native_catalog_request::Request as C;
    for request in [
        C::CreateView(p::NativeCreateView { name: Some("v".into()), read: None }),
        C::CreateView(p::NativeCreateView { name: None, read: Some(p::NativeTypedRead { table: Some("t".into()), ..Default::default() }) }),
        C::DropView(p::NativeDropView { name: None, cascade: None }),
    ] {
        let request = p::ExecuteRequest { request: Some(p::execute_request::Request::Catalog(p::NativeCatalogRequest { request: Some(request) })), ..Default::default() };
        assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token, request)).await.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
    f.stop().await;
}
