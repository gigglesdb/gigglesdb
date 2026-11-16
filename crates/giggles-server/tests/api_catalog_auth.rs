//! Catalog parity and explicit differences through both retained-session transports.
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
fn assert_code(reply: &Value, code: &str) {
    if reply["code"]=="native_error" && !reply["nativeError"]["sqlstate"].is_null() {
        assert_eq!(reply["nativeError"]["kind"],"NATIVE_ERROR_CLASS_SQL","{reply}");
        assert_eq!(reply["nativeError"]["sqlstate"],code,"{reply}");
    } else { assert_eq!(reply["code"],code,"{reply}"); }
}
fn create(table: &str) -> Value { json!({"catalog":{"createTable":{"table":table,
    "columns":[{"name":"id","dataType":"BigInt","notNull":true}],"primaryKey":["id"]}}}) }
fn bind(f: &Fixture, name: &str) {
    let mut b=f.bindings.clone(); b[0].role=giggles_server::Role::Database {name:name.into()};
    f.service.reload_bindings(b).unwrap();
}
fn sql(f: &Fixture, role: &str, q: &str, code: &str) {
    let r=f.service.role_session(role).unwrap().execute_sql(q,&[]);
    assert_eq!(r.as_ref().err().map(giggles_server::sqlstate).unwrap_or("ok"),code,"{q}: {r:?}");
}
fn drop(table: &str) -> Value {json!({"catalog":{"dropTable":{"table":table}}})}
#[tokio::test]
async fn catalog_creator_ownership_and_privilege_order() {
    use giggles_core::roles::{NewRole,SchemaPrivilege,TablePrivilege};
    for grpc in [false,true] {
        let f=Fixture::catalog_empty().await;
        for name in ["creator","other"] {f.admin.roles().create(NewRole {name:name.into(),login:true,..Default::default()}).unwrap();}
        for q in ["CREATE SCHEMA app","CREATE TABLE app.t(id BIGINT PRIMARY KEY)","CREATE INDEX t_v ON app.t(id)"] {f.admin.sql_session().execute(q,&[],None).unwrap();}
        f.admin.roles().grant_schema("app","creator",&[SchemaPrivilege::Usage,SchemaPrivilege::Create]).unwrap();
        f.admin.roles().grant_schema("app","other",&[SchemaPrivilege::Usage]).unwrap();
        f.admin.roles().grant_table("app\0t","other",&[TablePrivilege::Select,TablePrivilege::Insert,TablePrivilege::Update,TablePrivilege::Delete]).unwrap();
        bind(&f,"creator"); let token=f.open_http(&f.app,false).await;
        assert_code(&execute(&f,&token,create("app.c"),grpc).await,"ok");
        sql(&f,"creator","ALTER TABLE app.c ADD COLUMN via_sql TEXT","ok");
        sql(&f,"other","ALTER TABLE app.c ADD COLUMN forbidden TEXT","42501");
        assert_code(&execute(&f,&token,json!({"catalog":{"alterTable":{"table":"app.c","addColumn":{"name":"via_native","dataType":"Text"}}}}),grpc).await,"ok");
        sql(&f,"creator","CREATE TABLE app.sql_owned(id BIGINT PRIMARY KEY)","ok");
        assert_code(&execute(&f,&token,drop("app.sql_owned"),grpc).await,"ok");
        // Shared GigglesDB SQL rule; PG18 requires table ownership here.
        sql(&f,"creator","CREATE INDEX sql_i ON app.t(id)","ok");
        assert_code(&execute(&f,&token,json!({"catalog":{"createIndex":{"table":"app.t","name":"native_i","columns":["id"]}}}),grpc).await,"ok");
        bind(&f,"other"); let other=f.open_http(&f.app,false).await;
        for (request, query, code) in [
            (create("app.x"), "CREATE TABLE app.x(id BIGINT PRIMARY KEY)", "42501"),
            (drop("app.t"), "DROP TABLE app.t", "42501"),
            (drop("app.c"), "DROP TABLE app.c", "42501"),
            (json!({"catalog":{"alterTable":{"table":"app.t","addColumn":{"name":"x","dataType":"Text"}}}}), "ALTER TABLE app.t ADD COLUMN x TEXT", "42501"),
            (json!({"catalog":{"alterTable":{"table":"app.t","renameColumn":{"from":"id","to":"x"}}}}), "ALTER TABLE app.t RENAME COLUMN id TO x", "42501"),
            (json!({"catalog":{"createIndex":{"table":"app.t","name":"bad_i","columns":["id"]}}}), "CREATE INDEX bad_i ON app.t(id)", "42501"),
            (json!({"catalog":{"dropIndex":{"name":"app.t_v"}}}), "DROP INDEX app.t_v", "42501"),
            (drop("app.absent"), "DROP TABLE app.absent", "42501"),
            (json!({"catalog":{"alterTable":{"table":"app.absent","addColumn":{"name":"x","dataType":"Text"}}}}), "ALTER TABLE app.absent ADD COLUMN x TEXT", "42501"),
            (json!({"catalog":{"createIndex":{"table":"app.absent","name":"absent_i","columns":["id"]}}}), "CREATE INDEX absent_i ON app.absent(id)", "42501"),
            (json!({"catalog":{"dropIndex":{"name":"app.absent_i"}}}), "DROP INDEX app.absent_i", "42501"),
            (json!({"catalog":{"createIndex":{"table":"app.t","name":"bad_i","columns":["missing"]}}}), "CREATE INDEX bad_i ON app.t(missing)", "42501"),
            (json!({"catalog":{"alterTable":{"table":"app.t","renameColumn":{"from":"missing","to":"x"}}}}), "ALTER TABLE app.t RENAME COLUMN missing TO x", "42501"),
            (create("nosuch.x"), "CREATE TABLE nosuch.x(id BIGINT PRIMARY KEY)", "3F000"),
        ] {
            assert_code(&execute(&f,&other,request,grpc).await,code);
            sql(&f,"other",query,code);
        }
        let mut denied=f.service.role_session("other").unwrap();
        denied.execute_sql("BEGIN",&[]).unwrap();
        for (q,code) in [("DROP TABLE app.t","42501"),("SELECT * FROM app.t","25P02")] {
            assert_eq!(giggles_server::sqlstate(&denied.execute_sql(q,&[]).unwrap_err()),code);
        }
        denied.execute_sql("ROLLBACK",&[]).unwrap();
        assert_code(&execute(&f,&other,json!({"begin":{}}),grpc).await,"ok");
        assert_code(&execute(&f,&other,drop("app.t"),grpc).await,"42501");
        assert_code(&execute(&f,&other,json!({"searchPath":{}}),grpc).await,"25P02");
        assert_code(&execute(&f,&other,json!({"rollback":{}}),grpc).await,"ok");
        bind(&f,"creator"); let token=f.open_http(&f.app,false).await;
        // Distinct targets leave SQL and native successes with equivalent preconditions.
        for (request, query) in [
            (json!({"catalog":{"createIndex":{"table":"app.c","name":"c_native","columns":["id"]}}}), "CREATE INDEX c_sql ON app.c(id)"),
            (json!({"catalog":{"dropIndex":{"name":"app.c_native"}}}), "DROP INDEX app.c_sql"),
            (json!({"catalog":{"alterTable":{"table":"app.c","renameColumn":{"from":"via_native","to":"native_name"}}}}), "ALTER TABLE app.c RENAME COLUMN via_sql TO sql_name"),
            (drop("app.c"), "DROP TABLE app.sql_drop"),
        ] {
            if query == "DROP TABLE app.sql_drop" { sql(&f,"creator","CREATE TABLE app.sql_drop(id BIGINT PRIMARY KEY)","ok"); }
            assert_code(&execute(&f,&token,request,grpc).await,"ok");
            sql(&f,"creator",query,"ok");
        }
        // The token predates the revocation; authorization is refreshed on execution.
        f.admin.roles().revoke_schema("app","creator",&[SchemaPrivilege::Create]).unwrap();
        for (request, query) in [
            (create("app.revoked"), "CREATE TABLE app.revoked(id BIGINT PRIMARY KEY)"),
            (json!({"catalog":{"createIndex":{"table":"app.t","name":"revoked_i","columns":["id"]}}}), "CREATE INDEX revoked_i ON app.t(id)"),
        ] {
            assert_code(&execute(&f,&token,request,grpc).await,"42501");
            sql(&f,"creator",query,"42501");
        }
        let mut revoked=f.service.role_session("creator").unwrap();
        revoked.execute_sql("BEGIN",&[]).unwrap();
        assert_eq!(giggles_server::sqlstate(&revoked.execute_sql("CREATE TABLE app.revoked(id BIGINT PRIMARY KEY)",&[]).unwrap_err()),"42501");
        assert_eq!(giggles_server::sqlstate(&revoked.execute_sql("SELECT * FROM app.t",&[]).unwrap_err()),"25P02");
        revoked.execute_sql("ROLLBACK",&[]).unwrap();
        for (request, code) in [
            (json!({"begin":{}}),"ok"), (create("app.revoked"),"42501"),
            (json!({"searchPath":{}}),"25P02"), (json!({"rollback":{}}),"ok"),
        ] { assert_code(&execute(&f,&token,request,grpc).await,code); }
        f.service.reload_bindings(f.bindings.clone()).unwrap(); let app=f.open_http(&f.app,false).await;
        for (request, query) in [
            (create("app.denied"), "CREATE TABLE app.denied(id BIGINT PRIMARY KEY)"),
            (drop("app.t"), "DROP TABLE app.t"),
            (json!({"catalog":{"alterTable":{"table":"app.t","addColumn":{"name":"x","dataType":"Text"}}}}), "ALTER TABLE app.t ADD COLUMN x TEXT"),
            (json!({"catalog":{"createIndex":{"table":"app.t","name":"app_i","columns":["id"]}}}), "CREATE INDEX app_i ON app.t(id)"),
            (json!({"catalog":{"dropIndex":{"name":"app.t_v"}}}), "DROP INDEX app.t_v"),
        ] {
            assert_code(&execute(&f,&app,request,grpc).await,"42501");
            assert_code(&execute(&f,&app,json!({"sql":{"sql":query}}),grpc).await,"42501");
        }
        f.stop().await;
    }
}
