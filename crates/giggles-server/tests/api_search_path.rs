//! Retained session search paths through both transports.
mod api_support;
use api_support::{Fixture, rpc};
use giggles_server::api::{json as wire, proto as p};
use serde_json::{Value, json};
async fn run(f: &Fixture, token: &str, request: Value, grpc: bool) -> Value {
    let bytes = serde_json::to_vec(&request).unwrap();
    let reply = if grpc {
        let reply = f.grpc_client(&f.app).await.execute(rpc(token, wire::decode_execute(&bytes).unwrap())).await.unwrap().into_inner();
        wire::encode_reply(&reply).unwrap()
    } else {
        let (status, reply) = f.http(&f.app,"POST","/v1/session/execute",Some(token),bytes,false).await.unwrap();
        assert_eq!(status,200); reply
    };
    serde_json::from_slice(&reply).unwrap()
}
fn path(names: &[&str]) -> Value { json!({"searchPath":{"set":true,"schemas":names}}) }
fn sql(q: &str) -> Value { json!({"sql":{"sql":q}}) }
fn display(reply: &Value) -> &str { reply["sql"]["rows"][0]["values"][0]["textValue"].as_str().unwrap() }
#[test]
fn empty_core_path_and_wire_shape() {
    assert!(giggles_core::relational::schema_search_path("", "owner").is_empty());
    for input in [json!({"searchPath":{"extra":true}}), json!({"searchPath":{"set":"true"}})] {
        assert!(wire::decode_execute(&serde_json::to_vec(&input).unwrap()).is_err());
    }
}
#[tokio::test]
async fn retained_resolution_format_and_malformed() {
    let f = Fixture::new().await;
    let mut s = f.admin.sql_session();
    for q in ["CREATE SCHEMA app", "CREATE TABLE app.t(id BIGINT PRIMARY KEY)", "CREATE TABLE public.t(id BIGINT PRIMARY KEY)", "INSERT INTO app.t VALUES (1)", "INSERT INTO public.t VALUES (2)"] { s.execute(q,&[],None).unwrap(); }
    let mut bindings = f.bindings.clone();
    if let giggles_server::Role::Application { tables, .. } = &mut bindings[0].role { *tables = ["app\0t".into(),"t".into()].into_iter().collect(); }
    f.service.reload_bindings(bindings.clone()).unwrap();
    for grpc in [false,true] {
        let token = f.open_http(&f.app,false).await;
        assert_eq!(display(&run(&f,&token,json!({"searchPath":{}}),grpc).await), "\"$user\", public");
        for (names,id) in [(vec!["app","public"],"1"),(vec!["public"],"2")] {
            let set = run(&f,&token,path(&names),grpc).await;
            assert_eq!(set["code"],"ok"); assert_eq!(set["sql"]["columns"],json!(["search_path"]));
            let expected = run(&f,&token,sql("SELECT id FROM t"),grpc).await;
            assert_eq!(expected["sql"]["rows"][0]["values"][0]["intValue"],id);
            for request in [json!({"operation":{"request":{"read":{"table":"t","projection":["id"]}}}}), json!({"text":{"text":"t select id","languageVersion":"0.18"}})] {
                let reply = run(&f,&token,request,grpc).await;
                assert_eq!(reply["operationOutcome"]["rows"],expected["sql"]["rows"],"{reply}");
            }
        }
        for names in [vec![],vec!["a\"b","Upper","$user","a_b$9","select"]] {
            let result = run(&f,&token,path(&names),grpc).await;
            let expected = if names.is_empty() { "\"\"" } else { "\"a\"\"b\", \"Upper\", \"$user\", a_b$9, select" };
            // PG18.6 quotes the keyword "select"; our authorized fallback uses identifier syntax only.
            assert_eq!(display(&result),expected);
            if names.is_empty() {
                let sql_reply = run(&f,&token,sql("SELECT id FROM t"),grpc).await;
                let native = run(&f,&token,json!({"operation":{"request":{"read":{"table":"t","projection":["id"]}}}}),grpc).await;
                assert_ne!(sql_reply["code"],"ok");
                let state = if native["code"] == "native_error" { &native["nativeError"]["sqlstate"] } else { &native["code"] };
                assert_eq!(state,&sql_reply["code"],"{native} {sql_reply}");
            }
            assert_eq!(display(&run(&f,&token,json!({"searchPath":{"set":false}}),grpc).await),expected);
        }
        assert_eq!(display(&run(&f,&token,path(&["Mixed Case","app","$user"]),grpc).await), "\"Mixed Case\", app, \"$user\"");
        run(&f,&token,path(&["public"]),grpc).await;
        for (set,schemas) in [(None,vec!["app".into()]),(Some(false),vec!["app".into()]),(Some(true),vec![String::new()]),(Some(true),vec!["a\0b".into()]),(Some(true),vec!["\u{e9}".repeat(32)]),(Some(true),vec!["a".into();65])] {
            let request = p::ExecuteRequest { request: Some(p::execute_request::Request::SearchPath(p::NativeSearchPathRequest { set, schemas: schemas.clone() })), ..Default::default() };
            if grpc {
                assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token,request)).await.unwrap_err().code(),tonic::Code::InvalidArgument);
            } else {
                let mut shape = json!({"schemas":schemas});
                if let Some(set) = set { shape["set"] = json!(set); }
                let body = serde_json::to_vec(&json!({"searchPath":shape})).unwrap();
                assert_eq!(f.http(&f.app,"POST","/v1/session/execute",Some(&token),body,false).await.unwrap().0,400);
            }
            assert_eq!(display(&run(&f,&token,json!({"searchPath":{}}),grpc).await),"public");
        }
        assert_eq!(f.http(&f.app,"POST","/v1/session/execute",Some(&token),br#"{"searchPath":{"extra":true}}"#.to_vec(),false).await.unwrap().0,400);
        assert_eq!(display(&run(&f,&token,json!({"searchPath":{}}),grpc).await),"public");
        let observed = run(&f,&token,sql("SET search_path TO app, public"),grpc).await;
        assert_eq!(observed["code"],"0A000","current SQL API SET behavior: {observed}");
        let task = f.open_http(&f.task,false).await;
        let reply = f.grpc_client(&f.task).await.execute(rpc(&task,p::ExecuteRequest { request: Some(p::execute_request::Request::SearchPath(p::NativeSearchPathRequest::default())), ..Default::default() })).await.unwrap().into_inner();
        assert_eq!(reply.code,"42501");
        let (status,bytes) = f.http(&f.task,"POST","/v1/session/execute",Some(&task),br#"{"searchPath":{}}"#.to_vec(),false).await.unwrap();
        assert_eq!(status,200); assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap()["code"],"42501");
        assert_eq!(f.http(&f.unbound,"POST","/v1/session/execute",Some(&token),b"invalid".to_vec(),false).await.unwrap().0,401);
        assert_eq!(f.grpc_client(&f.unbound).await.execute(rpc(&token,p::ExecuteRequest::default())).await.unwrap_err().code(),tonic::Code::Unauthenticated);
    }
    f.admin.roles().create(giggles_core::roles::NewRole { name:"owner".into(),login:true,superuser:true,..Default::default() }).unwrap();
    s.execute("CREATE SCHEMA owner",&[],None).unwrap(); s.execute("CREATE TABLE owner.t(id BIGINT PRIMARY KEY)",&[],None).unwrap(); s.execute("INSERT INTO owner.t VALUES (3)",&[],None).unwrap();
    bindings[0].role = giggles_server::Role::Database { name:"owner".into() }; f.service.reload_bindings(bindings.clone()).unwrap();
    let mut role = f.service.role_session("owner").unwrap();
    let expected = role.execute_sql("SELECT id FROM t",&[]).unwrap();
    assert_eq!(expected.rows[0].values[0],giggles_core::relational::Value::Int(3));
    for grpc in [false,true] {
        let token = f.open_http(&f.app,false).await;
        let default = run(&f,&token,json!({"operation":{"request":{"read":{"table":"t","projection":["id"]}}}}),grpc).await;
        assert_eq!(default["operationOutcome"]["rows"][0]["values"][0]["intValue"],"3","{default}");
        run(&f,&token,path(&["$user","public"]),grpc).await;
        let reply = run(&f,&token,json!({"operation":{"request":{"read":{"table":"t","projection":["id"]}}}}),grpc).await;
        assert_eq!(reply["operationOutcome"]["rows"][0]["values"][0]["intValue"],"3","{reply}");
        run(&f,&token,path(&[]),grpc).await;
        let empty = run(&f,&token,json!({"operation":{"request":{"read":{"table":"t","projection":["id"]}}}}),grpc).await;
        assert_eq!(empty["nativeError"]["sqlstate"],"42P01","{empty}");
    }
    for grpc in [false,true] {
        let token = f.open_http(&f.app,false).await;
        f.service.reload_bindings(Vec::new()).unwrap();
        if grpc {
            assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token,p::ExecuteRequest { request: Some(p::execute_request::Request::SearchPath(p::NativeSearchPathRequest { set:Some(true),schemas:vec!["app".into()] })),..Default::default() })).await.unwrap_err().code(),tonic::Code::Unauthenticated);
        } else {
            assert_eq!(f.http(&f.app,"POST","/v1/session/execute",Some(&token),serde_json::to_vec(&path(&["app"])).unwrap(),false).await.unwrap().0,401);
        }
        f.service.reload_bindings(bindings.clone()).unwrap();
    }
    f.stop().await;
}

#[tokio::test]
async fn transactional_path_native_and_sql_controls() {
    let f = Fixture::new().await;
    let mut setup = f.admin.sql_session();
    for q in ["CREATE SCHEMA app", "CREATE TABLE app.t(id BIGINT PRIMARY KEY)",
        "CREATE TABLE public.t(id BIGINT PRIMARY KEY)", "INSERT INTO app.t VALUES (1)",
        "INSERT INTO public.t VALUES (2)"] { setup.execute(q,&[],None).unwrap(); }
    let mut bindings = f.bindings.clone();
    if let giggles_server::Role::Application { tables, .. } = &mut bindings[0].role {
        *tables = ["app\0t".into(),"t".into()].into_iter().collect();
    }
    f.service.reload_bindings(bindings).unwrap();
    for grpc in [false,true] {
        for sql_controls in [false,true] {
            let token = f.open_http(&f.app,false).await;
            let control = |name: &str| if sql_controls { sql(name) }
                else { json!({name.to_ascii_lowercase():{}}) };
            for (finish,duplicate,expected,id) in [("ROLLBACK",false,"\"$user\", public","2"),
                ("COMMIT",false,"app","1"),("COMMIT",true,"\"$user\", public","2")] {
                run(&f,&token,path(&["$user","public"]),grpc).await;
                assert_eq!(run(&f,&token,control("BEGIN"),grpc).await["code"],"ok");
                assert_eq!(display(&run(&f,&token,path(&["app"]),grpc).await),"app");
                let inside = run(&f,&token,sql("SELECT id FROM t"),grpc).await;
                assert_eq!(inside["sql"]["rows"][0]["values"][0]["intValue"],"1");
                if duplicate {
                    let error = run(&f,&token,sql("INSERT INTO app.t VALUES (1)"),grpc).await;
                    assert_eq!(error["code"],"23505","{error}");
                }
                let mut reply = run(&f,&token,control(finish),grpc).await;
                if duplicate {
                    assert_eq!(reply["code"],"25P02","{reply}");
                    assert_eq!(reply["transactionState"],"TRANSACTION_STATE_ABORTED","{reply}");
                    let refused = run(&f,&token,json!({"operation":{"request":{"read":{"table":"t","projection":["id"]}}}}),grpc).await;
                    assert_eq!(refused["code"],"25P02","{refused}");
                    let refused_path = run(&f,&token,json!({"searchPath":{}}),grpc).await;
                    assert_eq!(refused_path["code"],"25P02","{refused_path}");
                    reply = run(&f,&token,control("ROLLBACK"),grpc).await;
                }
                assert_eq!(reply["code"],"ok","{reply}");
                assert_eq!(reply["transactionState"],"TRANSACTION_STATE_IDLE","{reply}");
                assert_eq!(display(&run(&f,&token,json!({"searchPath":{}}),grpc).await),expected);
                let read = run(&f,&token,json!({"operation":{"request":{"read":{"table":"t","projection":["id"]}}}}),grpc).await;
                assert_eq!(read["operationOutcome"]["rows"][0]["values"][0]["intValue"],id,"{read}");
            }
            assert_eq!(display(&run(&f,&token,path(&["app"]),grpc).await),"app");
            assert_eq!(display(&run(&f,&token,json!({"searchPath":{}}),grpc).await),"app");
            run(&f,&token,path(&[]),grpc).await;
            let absent = run(&f,&token,sql("SELECT id FROM t"),grpc).await;
            run(&f,&token,control("BEGIN"),grpc).await;
            run(&f,&token,path(&["app"]),grpc).await;
            run(&f,&token,control("ROLLBACK"),grpc).await;
            let empty = run(&f,&token,sql("SELECT id FROM t"),grpc).await;
            assert_eq!(display(&run(&f,&token,json!({"searchPath":{}}),grpc).await),"\"\"");
            assert_ne!(empty["code"],"ok","{empty}");
            assert_eq!(empty["code"],absent["code"],"{empty}");
        }
    }
    f.stop().await;
}
