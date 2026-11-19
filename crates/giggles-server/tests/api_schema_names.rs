//! Native names resolve and authorize against the retained SQL session.
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
fn read(table: &str) -> Value { json!({"operation":{"request":{"read":{"table":table,"projection":["id"]}}}}) }
fn typed(table: &str) -> Value { json!({"typedRead":{"table":table,"projection":[
    {"expression":{"column":{"name":"id"}}}]}}) }
fn insert(table: &str) -> Value { json!({"operation":{"request":{"insert":{"table":table,
    "values":[{"column":"id","value":{"literal":{"intValue":"9"}}}]}}}}) }
fn text(source: &str) -> Value { json!({"text":{"text":source,"languageVersion":"0.18"}}) }
fn sql(source: &str) -> Value { json!({"sql":{"sql":source}}) }
async fn fixture() -> Fixture {
    let f = Fixture::new().await;
    let mut s = f.admin.sql_session();
    for q in ["CREATE SCHEMA app", "CREATE TABLE app.t(id BIGINT PRIMARY KEY)",
        "CREATE TABLE public.t(id BIGINT PRIMARY KEY)", "INSERT INTO app.t VALUES (1)",
        "INSERT INTO public.t VALUES (2)"] { s.execute(q, &[], None).unwrap(); }
    f
}
fn database(f: &Fixture, name: &str) {
    let mut bindings = f.bindings.clone();
    bindings[0].role = giggles_server::Role::Database { name: name.into() };
    f.service.reload_bindings(bindings).unwrap();
}
#[tokio::test]
async fn qualified_native_rows_writes_and_search_path_match_sql() {
    let f = fixture().await;
    let mut bindings = f.bindings.clone();
    if let giggles_server::Role::Application { tables, .. } = &mut bindings[0].role {
        *tables = ["app\0t".into(),"t".into()].into_iter().collect();
    }
    f.service.reload_bindings(bindings).unwrap();
    let token = f.open_http(&f.app, false).await;
    for grpc in [false, true] {
        for table in ["app.t", "public.t"] {
            let expected = execute(&f,&token,sql(&format!("SELECT id FROM {table}")),grpc).await;
            assert_eq!(expected["code"], "ok", "{expected}");
            for request in [read(table), text(&format!("{table} select id"))] {
                let reply = execute(&f,&token,request,grpc).await;
                assert_eq!(reply["code"], "ok", "{reply}");
                assert_eq!(expected["code"], "ok", "{expected}");
        assert_eq!(reply["operationOutcome"]["rows"], expected["sql"]["rows"]);
            }
            execute(&f,&token,json!({"begin":{}}),grpc).await;
            let sql_insert = execute(&f,&token,sql(&format!("INSERT INTO {table} VALUES (9)")),grpc).await;
            assert_eq!(sql_insert["code"], "ok", "{sql_insert}");
            execute(&f,&token,json!({"rollback":{}}),grpc).await;
            for request in [insert(table), text(&format!("insert {table} set id=9"))] {
                execute(&f,&token,json!({"begin":{}}),grpc).await;
                let reply = execute(&f,&token,request,grpc).await;
                assert_eq!(reply["code"], "ok", "{reply}");
                assert_eq!(reply["operationOutcome"]["affected"], sql_insert["sql"]["affected"]);
                let rows = execute(&f,&token,sql(&format!("SELECT id FROM {table} ORDER BY id")),grpc).await;
                assert_eq!(rows["sql"]["rows"].as_array().unwrap().len(), 2);
                execute(&f,&token,json!({"rollback":{}}),grpc).await;
            }
        }
        let reply = execute(&f,&token,typed("app.t"),grpc).await;
        let expected = execute(&f,&token,sql("SELECT id FROM app.t"),grpc).await;
        assert_eq!(reply["code"], "ok", "{reply}");
        assert_eq!(expected["code"], "ok", "{expected}");
        assert_eq!(reply["operationOutcome"]["rows"], expected["sql"]["rows"]);
        let join = json!({"typedRead":{"table":"app.t","join":{"table":"public.t",
            "leftAlias":"a","rightAlias":"b","left":false,"on":{"compare":{
                "left":{"column":{"source":"a","name":"id"}},"comparison":"NATIVE_COMPARISON_LT",
                "right":{"column":{"source":"b","name":"id"}}}}},"projection":[
            {"expression":{"column":{"source":"a","name":"id"}}},
            {"expression":{"column":{"source":"b","name":"id"}}}]}});
        let reply = execute(&f,&token,join,grpc).await;
        let expected = execute(&f,&token,sql("SELECT a.id,b.id FROM app.t a JOIN public.t b ON a.id<b.id"),grpc).await;
        assert_eq!(reply["code"], "ok", "{reply}");
        assert_eq!(expected["code"], "ok", "{expected}");
        assert_eq!(reply["operationOutcome"]["rows"], expected["sql"]["rows"]);
        let native = execute(&f,&token,read("t"),grpc).await;
        let expected = execute(&f,&token,sql("SELECT id FROM t"),grpc).await;
        assert_eq!(native["code"], "ok", "{native}");
        assert_eq!(expected["code"], "ok", "{expected}");
        assert_eq!(native["operationOutcome"]["rows"], expected["sql"]["rows"]);
        assert_eq!(execute(&f,&token,json!({"searchPath":{"set":true,"schemas":["app","public"]}}),grpc).await["code"], "ok");
        let reply = execute(&f,&token,typed("t"),grpc).await;
        let expected = execute(&f,&token,sql("SELECT id FROM app.t"),grpc).await;
        assert_eq!(reply["code"], "ok", "{reply}");
        assert_eq!(expected["code"], "ok", "{expected}");
        assert_eq!(reply["operationOutcome"]["rows"], expected["sql"]["rows"]);
        execute(&f,&token,json!({"searchPath":{"set":true,"schemas":["public"]}}),grpc).await;
    }
    f.admin.roles().create(giggles_core::roles::NewRole { name:"owner".into(), login:true,
        superuser:true, ..Default::default() }).unwrap();
    database(&f,"owner");
    let token = f.open_http(&f.app,false).await;
    for grpc in [false,true] {
        for table in ["app.", ".t", "a.b.c"] {
            let reply = execute(&f,&token,read(table),grpc).await;
            assert_eq!(reply["code"], "native_error", "{reply}");
            assert_eq!(reply["nativeError"]["kind"], "NATIVE_ERROR_CLASS_BIND", "{reply}");
            assert!(reply["nativeError"]["sqlstate"].is_null(), "{reply}");
        }
    }
    f.stop().await;
}
#[tokio::test]
async fn resolved_authority_denies_other_schemas_and_missing_targets() {
    use giggles_core::roles::NewRole;
    let f = fixture().await;
    f.admin.roles().create(NewRole { name:"reader".into(), login:true, ..NewRole::default() }).unwrap();
    f.admin.roles().grant_table("app\0t", "reader", &[giggles_core::roles::TablePrivilege::Select]).unwrap();
    f.admin.roles().grant_schema("app", "reader", &[giggles_core::roles::SchemaPrivilege::Usage]).unwrap();
    database(&f, "reader");
    let mut authority = f.service.role_session("reader").unwrap();
    assert!(authority.prepare("SELECT id FROM app.t").is_ok());
    let token = f.open_http(&f.app, false).await;
    for grpc in [false,true] {
        assert_eq!(execute(&f,&token,read("app.t"),grpc).await["code"],"ok");
        assert_eq!(execute(&f,&token,typed("app.t"),grpc).await["code"],"ok");
        assert_eq!(execute(&f,&token,typed("public.t"),grpc).await["code"],"42501");
        for table in ["public.t","app.missing","nosuch.t"] {
            let error = authority.prepare(&format!("SELECT id FROM {table}")).unwrap_err();
            assert_eq!(giggles_server::sqlstate(&error), "42501");
            assert_eq!(execute(&f,&token,sql(&format!("SELECT id FROM {table}")),grpc).await["code"],"42501");
            for request in [read(table),text(&format!("{table} select id"))] {
                assert_eq!(execute(&f,&token,request,grpc).await["code"],"42501");
            }
        }
        assert_eq!(giggles_server::sqlstate(&authority.prepare("INSERT INTO app.t VALUES (9)").unwrap_err()),"42501");
        assert_eq!(execute(&f,&token,insert("app.t"),grpc).await["code"],"42501");
    }
    let mut bindings = f.bindings.clone();
    if let giggles_server::Role::Application { tables, .. } = &mut bindings[0].role {
        *tables = ["t".into()].into_iter().collect();
    }
    f.service.reload_bindings(bindings).unwrap();
    let token = f.open_http(&f.app,false).await;
    for grpc in [false,true] {
        for table in ["t","public.t","app.t"] {
            for request in [read(table),text(&format!("{table} select id"))] {
                assert_eq!(execute(&f,&token,request,grpc).await["code"], if table == "app.t" {"42501"} else {"ok"});
            }
        }
        assert_eq!(execute(&f,&token,read("app.t"),grpc).await["code"],"42501");
    }
    f.stop().await;
}
