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
fn add(name: &str, kind: Value) -> Value {
    let mut v=json!({"table":"t","name":name}); v.as_object_mut().unwrap().extend(kind.as_object().unwrap().clone()); json!({"catalog":{"addConstraint":v}})
}
#[tokio::test]
async fn constraints_sql_twins_and_authority() {
    use giggles_core::roles::{NewRole,SchemaPrivilege};
    for grpc in [false,true] {
        let f=Fixture::catalog_empty().await; let twin=Fixture::catalog_empty().await;
        for fixture in [&f,&twin] {
            fixture.admin.roles().create(NewRole {name:"owner".into(),login:true,superuser:true,..Default::default()}).unwrap();
            fixture.admin.roles().create(NewRole {name:"reader".into(),login:true,..Default::default()}).unwrap();
            let mut b=fixture.bindings.clone(); b[0].role=giggles_server::Role::Database{name:"owner".into()}; fixture.service.reload_bindings(b).unwrap();
            for q in ["CREATE SCHEMA app","CREATE TABLE app.p(id BIGINT PRIMARY KEY)","CREATE TABLE app.t(id BIGINT PRIMARY KEY, v BIGINT)"] {fixture.admin.sql_session().execute(q,&[],None).unwrap();}
        }
        let token=f.open_http(&f.app,false).await; let mut sql=twin.service.role_session("owner").unwrap();
        assert_code(&execute(&f,&token,json!({"searchPath":{"set":true,"schemas":["app"]}}),grpc).await,"ok");
        let check=json!({"check":{"expression":{"compare":{"left":{"field":"v"},"comparison":"NATIVE_COMPARISON_GT","right":{"value":{"literal":{"intValue":"0"}}}}}}});
        for (request,q,code) in [
            (add("uq",json!({"unique":{"columns":["v"]}})),"ALTER TABLE app.t ADD CONSTRAINT uq UNIQUE(v)","ok"),
            (add("uq",json!({"unique":{"columns":["v"]}})),"ALTER TABLE app.t ADD CONSTRAINT uq UNIQUE(v)","42P07"),
            (json!({"catalog":{"addConstraint":{"table":"app.absent","unique":{"columns":["v"]}}}}),"ALTER TABLE app.absent ADD UNIQUE(v)","42P01"),
            (add("ck",check),"ALTER TABLE app.t ADD CONSTRAINT ck CHECK(v > 0)","ok"),
            (add("fk",json!({"foreignKey":{"columns":["v"],"refTable":"app.p","refColumns":["ignored"],"onDelete":"Cascade"}})),"ALTER TABLE app.t ADD CONSTRAINT fk FOREIGN KEY(v) REFERENCES app.p ON DELETE CASCADE","ok"),
            (json!({"catalog":{"dropConstraint":{"table":"t","name":"missing"}}}),"ALTER TABLE app.t DROP CONSTRAINT missing","42704"),
            (json!({"catalog":{"dropConstraint":{"table":"absent","name":"x"}}}),"ALTER TABLE absent DROP CONSTRAINT x","42P01"),
        ] {
            let reply=execute(&f,&token,request,grpc).await; assert_code(&reply,code);
            let result=sql.execute_sql(q,&[]); assert_eq!(result.as_ref().err().map(giggles_server::sqlstate).unwrap_or("ok"),code,"{q}: {result:?}");
            if code=="ok" {assert_eq!(reply["catalogOutcome"]["table"],"app.t"); assert!(reply["catalogOutcome"]["indexId"].is_null());}
        }
        for fixture in [&f,&twin] {fixture.service.role_session("owner").unwrap().execute_sql("INSERT INTO app.p VALUES(1)",&[]).unwrap();}
        // Legacy native catalog retains its populated-database planning gate; SQL DDL remains synchronous.
        let populated=execute(&f,&token,add("populated",json!({"unique":{"columns":["id"]}})),grpc).await;
        assert_eq!(populated["nativeError"]["kind"],"NATIVE_ERROR_CLASS_PLAN_REQUIRED");
        sql.execute_sql("ALTER TABLE app.t ADD CONSTRAINT populated UNIQUE(id)",&[]).unwrap(); sql.execute_sql("ALTER TABLE app.t DROP CONSTRAINT populated",&[]).unwrap();
        for (q,code) in [("INSERT INTO app.t VALUES(1,0)","23514"),("INSERT INTO app.t VALUES(1,2)","23503"),("INSERT INTO app.t VALUES(1,1)","ok"),("INSERT INTO app.t VALUES(2,1)","23505"),("DELETE FROM app.p WHERE id=1","ok")] {
            for fixture in [&f,&twin] {let result=fixture.service.role_session("owner").unwrap().execute_sql(q,&[]); assert_eq!(result.as_ref().err().map(giggles_server::sqlstate).unwrap_or("ok"),code,"{q}: {result:?}");}
        }
        for fixture in [&f,&twin] {assert!(fixture.admin.sql_session().execute("SELECT * FROM app.t",&[],None).unwrap().rows.is_empty());}
        for cascade in [true,false] {
            let request=json!({"catalog":{"dropConstraint":{"table":"app.t","name":"fk","cascade":cascade}}});
            let code=if cascade {"42601"} else {"ok"}; assert_code(&execute(&f,&token,request,grpc).await,code);
            let q=if cascade {"ALTER TABLE app.t DROP CONSTRAINT fk CASCADE"} else {"ALTER TABLE app.t DROP CONSTRAINT fk"};
            let result=sql.execute_sql(q,&[]); assert_eq!(result.as_ref().err().map(giggles_server::sqlstate).unwrap_or("ok"),code);
        }
        for fixture in [&f,&twin] {let mut session=fixture.service.role_session("owner").unwrap();
            session.execute_sql("INSERT INTO app.t VALUES(3,99)",&[]).unwrap(); session.execute_sql("DELETE FROM app.t WHERE id=3",&[]).unwrap();}
        // Wire expressions are shared, but core's native CHECK compiler is narrower than SQL.
        let rich=json!({"catalog":{"addConstraint":{"table":"app.t","name":"rich","check":{"expression":{"notExpr":{"compare":{"left":{"field":"v"},"comparison":"NATIVE_COMPARISON_GT","right":{"value":{"literal":{"intValue":"0"}}}}}}}}}});
        let reply=execute(&f,&token,rich,grpc).await; assert_eq!(reply["code"],"native_error");
        assert_eq!(reply["nativeError"]["kind"],"NATIVE_ERROR_CLASS_UNSUPPORTED"); assert!(reply["nativeError"]["sqlstate"].is_null());
        sql.execute_sql("ALTER TABLE app.t ADD CONSTRAINT rich CHECK(NOT(v > 0))",&[]).unwrap();
        f.admin.roles().grant_schema("app","reader",&[SchemaPrivilege::Usage]).unwrap();
        for application in [false,true] {
            let mut b=f.bindings.clone(); if !application {b[0].role=giggles_server::Role::Database{name:"reader".into()};} f.service.reload_bindings(b).unwrap(); let denied=f.open_http(&f.app,false).await;
            for (request,q) in [
                (json!({"catalog":{"addConstraint":{"table":"app.t","unique":{"columns":["missing"]}}}}),"ALTER TABLE app.t ADD UNIQUE(missing)"),
                (json!({"catalog":{"dropConstraint":{"table":"app.t","name":"missing"}}}),"ALTER TABLE app.t DROP CONSTRAINT missing"),
                (json!({"catalog":{"addConstraint":{"table":"app.t","foreignKey":{"columns":["missing"],"refTable":"app.missing"}}}}),"ALTER TABLE app.t ADD FOREIGN KEY(missing) REFERENCES app.missing")
            ] {
                assert_code(&execute(&f,&denied,request,grpc).await,"42501");
                let mut session=if application {f.service.session(&f.bindings[0].fingerprint).unwrap()} else {f.service.role_session("reader").unwrap()};
                assert_eq!(giggles_server::sqlstate(&session.execute_sql(q,&[]).unwrap_err()),"42501");
            }
        }
        // A child owner still needs authority over the referenced parent before binding columns.
        f.admin.roles().grant_schema("app","reader",&[SchemaPrivilege::Create]).unwrap();
        f.service.role_session("reader").unwrap().execute_sql("CREATE TABLE app.child(id BIGINT PRIMARY KEY, v BIGINT)",&[]).unwrap();
        let mut b=f.bindings.clone(); b[0].role=giggles_server::Role::Database{name:"reader".into()}; f.service.reload_bindings(b).unwrap(); let reader=f.open_http(&f.app,false).await;
        let request=json!({"catalog":{"addConstraint":{"table":"app.child","foreignKey":{"columns":["v"],"refTable":"app.p","refColumnsSet":true,"refColumns":["missing"]}}}});
        assert_code(&execute(&f,&reader,request,grpc).await,"42501");
        assert_eq!(giggles_server::sqlstate(&f.service.role_session("reader").unwrap().execute_sql("ALTER TABLE app.child ADD FOREIGN KEY(v) REFERENCES app.p(missing)",&[]).unwrap_err()),"42501");
        for (request,q) in [
            (json!({"catalog":{"addConstraint":{"table":"app.child","name":"child_uq","unique":{"columns":["v"]}}}}),"ALTER TABLE app.child ADD CONSTRAINT sql_uq UNIQUE(v)"),
            (json!({"catalog":{"dropConstraint":{"table":"app.child","name":"child_uq"}}}),"ALTER TABLE app.child DROP CONSTRAINT sql_uq")
        ] {assert_code(&execute(&f,&reader,request,grpc).await,"ok"); f.service.role_session("reader").unwrap().execute_sql(q,&[]).unwrap();}
        f.stop().await; twin.stop().await;
    }
}

#[tokio::test]
async fn malformed_constraint_transports() {
    use giggles_server::api::proto as p;
    let f=Fixture::catalog_empty().await; let token=f.open_http(&f.app,false).await;
    for (body,wire) in [
        (json!({"table":"t"}),p::NativeAddConstraint {table:Some("t".into()),..Default::default()}),
        (json!({"table":"","unique":{}}),p::NativeAddConstraint {table:Some("".into()),constraint:Some(p::native_add_constraint::Constraint::Unique(Default::default())),..Default::default()}),
        (json!({"table":"t","name":"","unique":{}}),p::NativeAddConstraint {table:Some("t".into()),name:Some("".into()),constraint:Some(p::native_add_constraint::Constraint::Unique(Default::default()))}),
        (json!({"table":"t","foreignKey":{"refTable":""}}),p::NativeAddConstraint {table:Some("t".into()),constraint:Some(p::native_add_constraint::Constraint::ForeignKey(p::NativeForeignKey {ref_table:Some("".into()),..Default::default()})),..Default::default()}),
        (json!({"table":"t","foreignKey":{"refTable":"p","onUpdate":"invalid"}}),p::NativeAddConstraint {table:Some("t".into()),constraint:Some(p::native_add_constraint::Constraint::ForeignKey(p::NativeForeignKey {ref_table:Some("p".into()),on_update:Some("invalid".into()),..Default::default()})),..Default::default()}),
    ] {
        let (status,_)=f.http(&f.app,"POST","/v1/session/execute",Some(&token),serde_json::to_vec(&json!({"catalog":{"addConstraint":body}})).unwrap(),false).await.unwrap(); assert_eq!(status,400);
        let request=p::ExecuteRequest {request:Some(p::execute_request::Request::Catalog(p::NativeCatalogRequest {request:Some(p::native_catalog_request::Request::AddConstraint(wire))})),..Default::default()};
        assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token,request)).await.unwrap_err().code(),tonic::Code::InvalidArgument);
    }
    let (status,_)=f.http(&f.app,"POST","/v1/session/execute",Some(&token),serde_json::to_vec(&json!({"catalog":{"addConstraint":{"table":"t","unique":{"unknown":1}}}})).unwrap(),false).await.unwrap(); assert_eq!(status,400);
    for (table,name) in [("","x"),("t","")] {
        let bytes=serde_json::to_vec(&json!({"catalog":{"dropConstraint":{"table":table,"name":name}}})).unwrap();
        let (status,_)=f.http(&f.app,"POST","/v1/session/execute",Some(&token),bytes.clone(),false).await.unwrap(); assert_eq!(status,400);
        let request=wire_json::decode_execute(&bytes).unwrap();
        assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token,request)).await.unwrap_err().code(),tonic::Code::InvalidArgument);
    }
    f.stop().await;
}
