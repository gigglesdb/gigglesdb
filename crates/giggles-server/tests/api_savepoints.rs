//! Native savepoint parity with a second Application SQL session, over both wires.
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
fn sql(q: &str) -> Value { json!({"sql":{"sql":q}}) }
fn state(reply: &Value) -> &str {
    if reply["code"] == "native_error" { reply["nativeError"]["sqlstate"].as_str().unwrap() }
    else { reply["code"].as_str().unwrap() }
}
fn check(reply: &Value, code: &str, transaction: &str) {
    assert_eq!(state(reply),code,"{reply}");
    let expected = match transaction { "I" => "IN_TRANSACTION", "A" => "ABORTED", "D" => "IDLE", _ => panic!("bad state") };
    assert_eq!(reply["transactionState"],format!("TRANSACTION_STATE_{expected}"),"{reply}");
}
fn requests(command: &str) -> (Value, Value) {
    let (kind,name) = command.split_once(' ').unwrap_or((command,""));
    let quoted = format!("\"{}\"",name.replace('"',"\"\""));
    match kind {
        "i" => (json!({"operation":{"request":{"insert":{"table":"app","values":[
            {"column":"id","value":{"literal":{"intValue":name}}}]}}}}),sql(&format!("INSERT INTO app(id) VALUES ({name})"))),
        "s" => (json!({"savepoint":{"name":name}}),sql(&format!("SAVEPOINT {quoted}"))),
        "t" => (json!({"rollbackTo":{"name":name}}),sql(&format!("ROLLBACK TO {quoted}"))),
        "r" => (json!({"release":{"name":name}}),sql(&format!("RELEASE {quoted}"))),
        _ => (json!({kind:{}}),sql(&kind.to_ascii_uppercase())),
    }
}
#[tokio::test]
async fn native_sql_savepoint_matrix() {
    // Verified with postgres:18.6; suffixes give SQLSTATE and transaction state.
    let long = "é".repeat(32);
    let utf8 = format!("begin;s {long}x;i 1;t {long}y;commit");
    let cases = [
        ("nesting","begin;i 1;s a;i 2;s b;i 3;t a;i 4;r a;commit",vec!["1","4"]),
        ("aborted recovery","begin;i 1;s a;i 1/23505/A;i 2/25P02/A;s b/25P02/A;r a/25P02/A;t a;i 2;commit",vec!["1","2"]),
        ("outside","s a/25P01/D;t a/25P01/D;r a/25P01/D",vec![]),
        ("unknown rollback","begin;t absent/3B001/A;rollback",vec![]),
        ("unknown release","begin;r absent/3B001/A;rollback",vec![]),
        ("shadow","begin;s a;i 1;s a;i 2;t a;r a;t a;i 3;commit",vec!["3"]),
        ("case sensitive","begin;s A;i 1;s a;i 2;t A;i 3;commit",vec!["3"]),
        ("UTF8 truncation",utf8.as_str(),vec![]),
    ];
    for grpc in [false,true] {
        let f = Fixture::new().await;
        for (label,script,ids) in &cases {
            let tokens = [f.open_http(&f.app,false).await,f.open_http(&f.app,false).await];
            let mut outcomes = Vec::new();
            for (oracle,token) in tokens.iter().enumerate() {
                f.admin.sql_session().execute("DELETE FROM app",&[],None).unwrap();
                for entry in script.split(';') {
                    let parts: Vec<_> = entry.split('/').collect();
                    let command = parts[0];
                    let (native,sql) = requests(command);
                    let reply = run(&f,token,if oracle == 0 {native} else {sql},grpc).await;
                    let idle = matches!(command,"commit"|"rollback");
                    check(&reply,parts.get(1).copied().unwrap_or("ok"),parts.get(2).copied().unwrap_or(if idle {"D"} else {"I"}));
                }
                let request = if oracle == 0 { json!({"operation":{"request":{"read":{
                    "table":"app","projection":["id"],"order":[{"field":"id","descending":false}]}}}}) }
                    else { sql("SELECT id FROM app ORDER BY id") };
                let reply = run(&f,token,request,grpc).await;
                check(&reply,"ok","D");
                let rows = if oracle == 0 { &reply["operationOutcome"]["rows"] } else { &reply["sql"]["rows"] };
                let actual: Vec<_> = rows.as_array().unwrap().iter().map(|r| r["values"][0]["intValue"].as_str().unwrap()).collect();
                assert_eq!(&actual,ids,"{label}, grpc={grpc}, oracle={oracle}");
                outcomes.push(rows.as_array().unwrap().iter().map(|r| r["values"].clone()).collect::<Vec<_>>());
            }
            assert_eq!(outcomes[0],outcomes[1],"{label}, grpc={grpc}");
        }
        f.stop().await;
    }
}
fn path() -> Value { json!({"searchPath":{"set":true,"schemas":["app"]}}) }
fn display(reply: &Value) -> &str { reply["sql"]["rows"][0]["values"][0]["textValue"].as_str().unwrap() }
#[tokio::test]
async fn savepoint_search_path_and_sql_stack_difference() {
    for grpc in [false,true] {
        let f = Fixture::new().await;
        for finish in ["rollbackTo","rollback","commit"] {
            for oracle in [false,true] {
                let token = f.open_http(&f.app,false).await;
                for command in ["begin","s p"] {
                    let (native,sql) = requests(command);
                    check(&run(&f,&token,if oracle {sql} else {native},grpc).await,"ok","I");
                }
                check(&run(&f,&token,path(),grpc).await,"ok","I");
                let control = if finish == "rollbackTo" {"t p"} else {"r p"};
                let (native,sql) = requests(control);
                check(&run(&f,&token,if oracle {sql} else {native},grpc).await,"ok","I");
                if finish != "rollbackTo" {
                    let (native,sql) = requests(finish);
                    check(&run(&f,&token,if oracle {sql} else {native},grpc).await,"ok","D");
                }
                let reply = run(&f,&token,json!({"searchPath":{}}),grpc).await;
                check(&reply,"ok",if finish == "rollbackTo" {"I"} else {"D"});
                // SQL savepoints do not maintain the native API path stack. Assert the
                // observed difference: native rollbackTo restores default; SQL retains app.
                let expected = if finish == "commit" || (finish == "rollbackTo" && oracle) {"app"} else {"\"$user\", public"};
                assert_eq!(display(&reply),expected,"oracle={oracle}, {finish}, grpc={grpc}");
                if finish == "rollbackTo" {
                    let (native,sql) = requests("rollback");
                    check(&run(&f,&token,if oracle {sql} else {native},grpc).await,"ok","D");
                    assert_eq!(display(&run(&f,&token,json!({"searchPath":{}}),grpc).await),"\"$user\", public");
                }
            }
        }
        f.stop().await;
    }
}
#[tokio::test]
async fn malformed_and_authentication_before_decoding() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app,false).await;
    check(&run(&f,&token,json!({"begin":{}}),false).await,"ok","I");
    for kind in ["savepoint","rollbackTo","release"] {
        for name in [None,Some(""),Some("a\0b")] {
            let mut shape = json!({});
            if let Some(name) = name { shape["name"] = json!(name); }
            let body = serde_json::to_vec(&json!({kind:shape})).unwrap();
            assert_eq!(f.http(&f.app,"POST","/v1/session/execute",Some(&token),body.clone(),false).await.unwrap().0,400);
            assert_eq!(f.http(&f.unbound,"POST","/v1/session/execute",Some(&token),body,false).await.unwrap().0,401);
            let value = p::NativeSavepointRequest { name:name.map(str::to_owned) };
            let request = p::ExecuteRequest { request:Some(match kind {
                "savepoint" => p::execute_request::Request::Savepoint(value),
                "rollbackTo" => p::execute_request::Request::RollbackTo(value),
                _ => p::execute_request::Request::Release(value),
            }),..Default::default() };
            assert_eq!(f.grpc_client(&f.app).await.execute(rpc(&token,request.clone())).await.unwrap_err().code(),tonic::Code::InvalidArgument);
            assert_eq!(f.grpc_client(&f.unbound).await.execute(rpc(&token,request)).await.unwrap_err().code(),tonic::Code::Unauthenticated);
        }
        let valid = serde_json::to_vec(&json!({kind:{"name":"a"}})).unwrap();
        assert_eq!(f.http(&f.unbound,"POST","/v1/session/execute",Some(&token),valid.clone(),false).await.unwrap().0,401);
        assert_eq!(f.grpc_client(&f.unbound).await.execute(rpc(&token,wire::decode_execute(&valid).unwrap())).await.unwrap_err().code(),tonic::Code::Unauthenticated);
        let unknown = serde_json::to_vec(&json!({kind:{"name":"a","unknown":1}})).unwrap();
        // Protobuf has no unknown JSON field representation; the JSON codec rejects it.
        assert!(wire::decode_execute(&unknown).is_err());
        assert_eq!(f.http(&f.app,"POST","/v1/session/execute",Some(&token),unknown,false).await.unwrap().0,400);
    }
    assert_eq!(f.http(&f.unbound,"POST","/v1/session/execute",Some(&token),b"invalid JSON".to_vec(),false).await.unwrap().0,401);
    check(&run(&f,&token,json!({"savepoint":{"name":"still_active"}}),true).await,"ok","I");
    check(&run(&f,&token,json!({"rollback":{}}),true).await,"ok","D");
    f.stop().await;
}
