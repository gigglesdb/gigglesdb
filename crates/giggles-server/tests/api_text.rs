//! Text lowering shares the authenticated native execution path on both transports.
mod api_support;
use api_support::{Fixture, rpc};
use giggles_server::api::{json as wire_json, proto};
use serde_json::{Value, json};

async fn execute(f: &Fixture, token: &str, request: Value, grpc: bool) -> Value {
    if grpc {
        let wire = wire_json::decode_execute(&serde_json::to_vec(&request).unwrap()).unwrap();
        let reply = f.grpc_client(&f.app).await.execute(rpc(token, wire)).await.unwrap().into_inner();
        serde_json::from_slice(&wire_json::encode_reply(&reply).unwrap()).unwrap()
    } else {
        let (status, bytes) = f.http(&f.app, "POST", "/v1/session/execute", Some(token),
            serde_json::to_vec(&request).unwrap(), false).await.unwrap();
        assert_eq!(status, 200, "{request}: {}", String::from_utf8_lossy(&bytes));
        serde_json::from_slice(&bytes).unwrap()
    }
}
fn text(source: &str) -> Value {
    json!({"text":{"text":source,"languageVersion":"0.16"}})
}
fn int(v: i64) -> Value { json!({"literal":{"intValue":v.to_string()}}) }
fn id() -> Value {
    json!({"compare":{"left":{"field":"id"},"comparison":"NATIVE_COMPARISON_EQ","right":{"value":int(1)}}})
}
fn insert() -> Value {
    json!({"insert":{"table":"app","values":[{"column":"id","value":int(2)}]}})
}
fn delete() -> Value { json!({"delete":{"table":"app","filter":id()}}) }
fn operation(request: Value) -> Value { json!({"operation":{"request":request}}) }

#[tokio::test]
async fn text_matches_structured_crud_blocks_and_retained_sessions() {
    for grpc in [false, true] {
        let f = Fixture::new().await;
        f.admin.sql_session().execute("INSERT INTO app (id,body) VALUES (1,'seed')", &[], None).unwrap();
        let token = f.open_http(&f.app, false).await;
        let cases = [
            (text("insert app set id=2"), operation(insert())),
            (text("app where id=1 select id,body"), operation(json!({"read":{"table":"app","filter":id(),"projection":["id","body"]}}))),
            (text("update app where id=1 set body=\"changed\""), operation(json!({"update":{"table":"app","filter":id(),"values":[{"column":"body","value":{"literal":{"textValue":"changed"}}}],"intent":{"one":{}}}}))),
            (text("delete app where id=1"), operation(delete())),
            (text("transaction { insert app set id=2; delete app where id=1 }"), json!({"transaction":{"transaction":{"readOnly":false,"steps":[{"execute":insert()},{"execute":delete()}]}}})),
        ];
        for (source, ast) in cases {
            let mut replies = Vec::new();
            for request in [source, ast] {
                assert_eq!(execute(&f, &token, json!({"begin":{}}), grpc).await["code"], "ok");
                let reply = execute(&f, &token, request, grpc).await;
                assert_eq!(reply["code"], "ok", "{reply}");
                replies.push(reply);
                execute(&f, &token, json!({"rollback":{}}), grpc).await;
            }
            assert_eq!(replies[0], replies[1]);
        }
        execute(&f, &token, json!({"begin":{}}), grpc).await;
        // Omitted version follows CLI 0.1; named bindings use the same value wire type.
        let reply = execute(&f, &token, json!({"text":{"text":"insert app set id=$id","bindings":[{"name":"id","value":{"intValue":"2"}}]}}), grpc).await;
        assert_eq!(reply["code"], "ok", "{reply}");
        assert_eq!(reply["transactionState"], "TRANSACTION_STATE_IN_TRANSACTION");
        assert_eq!(f.admin.sql_session().execute("SELECT id FROM app", &[], None).unwrap().rows.len(), 1);
        let reply = execute(&f, &token, json!({"commit":{}}), !grpc).await;
        assert_eq!(reply["transactionState"], "TRANSACTION_STATE_IDLE");
        assert_eq!(f.admin.sql_session().execute("SELECT id FROM app", &[], None).unwrap().rows.len(), 2);
        f.stop().await;
    }
}

#[tokio::test]
async fn text_authorizes_every_target_before_binding_or_staging() {
    for grpc in [false, true] {
        let f = Fixture::new().await;
        let token = f.open_http(&f.app, false).await;
        // Neither the table nor its column exists: table scope must win over binding.
        let source = text("missing select absent");
        let ast = operation(json!({"read":{"table":"missing","projection":["absent"]}}));
        let reply = execute(&f, &token, source, grpc).await;
        assert_eq!(reply, execute(&f, &token, ast, grpc).await);
        assert_eq!(reply["code"], "42501");
        execute(&f, &token, json!({"begin":{}}), grpc).await;
        let reply = execute(&f, &token, text("transaction { insert app set id=2; missing select absent }"), grpc).await;
        assert_eq!(reply["code"], "42501");
        assert_eq!(reply["transactionState"], "TRANSACTION_STATE_ABORTED");
        execute(&f, &token, json!({"rollback":{}}), grpc).await;
        assert!(f.admin.sql_session().execute("SELECT id FROM app", &[], None).unwrap().rows.is_empty());
        f.stop().await;
    }
}

#[tokio::test]
async fn text_syntax_catalog_version_and_binding_rejections_are_invalid_arguments() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    execute(&f, &token, json!({"begin":{}}), false).await;
    let mut unknown = text("app");
    unknown["text"]["languageVersion"] = json!("999");
    for request in [text("insert app set id="), text("create table t (id int64,primary key(id))"), text("show tables"), unknown,
        json!({"text":{"text":"app where id=$id","bindings":[{"name":"id","value":{"intValue":"1"}},{"name":"id","value":{"intValue":"2"}}]}})] {
        let bytes = serde_json::to_vec(&request).unwrap();
        let (status, body) = f.http(&f.app, "POST", "/v1/session/execute", Some(&token), bytes.clone(), false).await.unwrap();
        assert_eq!(status, 400, "{request}");
        assert_eq!(serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"], "INVALID_ARGUMENT");
        let wire = wire_json::decode_execute(&bytes).unwrap();
        let error = f.grpc_client(&f.app).await.execute(rpc(&token, wire)).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    let reply = execute(&f, &token, text("app select id"), true).await;
    assert_eq!(reply["code"], "ok");
    assert_eq!(reply["transactionState"], "TRANSACTION_STATE_IN_TRANSACTION");
    execute(&f, &token, json!({"rollback":{}}), false).await;
    // Unknown JSON keys and duplicate members retain the strict request profile.
    for bytes in [br#"{"text":{"text":"app","unknown":true}}"#.as_slice(), br#"{"text":{"text":"app","text":"other"}}"#] {
        assert!(wire_json::decode_execute(bytes).is_err());
    }
    // Field 12 is additive; old protobuf field numbers remain unchanged.
    use prost::Message;
    let wire = proto::ExecuteRequest { request: Some(proto::execute_request::Request::Text(proto::NativeTextRequest {
        text: "app select id".into(), bindings: vec![], language_version: None,
    })), budget_ms: None }.encode_to_vec();
    assert_eq!(wire[0], (12 << 3) | 2);
    f.stop().await;
}

// Native authority must track GigglesDB SQL, including its present PG gaps.
#[tokio::test]
async fn database_native_grants_match_sql_on_http_and_grpc() {
    use giggles_core::roles::{NewRole, TablePrivilege::{Select, Insert, Update, Delete}};
    use giggles_server::{Role, sqlstate};
    for grpc in [false, true] {
        let f = Fixture::new().await;
        let roles = f.admin.roles();
        roles.create(NewRole { name: "bound".into(), login: true, ..NewRole::default() }).unwrap();
        let mut bindings = f.bindings.clone();
        bindings[0].role = Role::Database { name: "bound".into() };
        f.service.reload_bindings(bindings).unwrap();
        let token = f.open_http(&f.app, false).await;
        let mut sql = f.service.role_session("bound").unwrap();
        let denied = sqlstate(&sql.prepare("INSERT INTO app(id) VALUES (2)").unwrap_err());
        assert_eq!(denied, "42501");
        for request in [text("insert app set id=2"), operation(insert())] {
            assert_eq!(execute(&f, &token, request, grpc).await["code"], denied);
        }
        for control in ["begin", "commit", "begin", "rollback"] {
            assert_eq!(execute(&f, &token, json!({(control):{}}), grpc).await["code"], "ok");
        }
        roles.grant_table("app", "bound", &[Select]).unwrap();
        assert_eq!(execute(&f, &token, operation(insert()), grpc).await["code"],
            sqlstate(&sql.prepare("INSERT INTO app(id) VALUES (2)").unwrap_err()));
        let read = operation(json!({"read":{"table":"app","projection":["id"]}}));
        assert_eq!(execute(&f, &token, text("app select id"), grpc).await,
            execute(&f, &token, read, grpc).await);
        assert_eq!(execute(&f, &token, text("app select id"), grpc).await["code"], "ok");
        roles.grant_table("app", "bound", &[Insert]).unwrap();
        assert_eq!(execute(&f, &token, operation(insert()), grpc).await["code"], "ok");
        roles.revoke_table("app", "bound", &[Insert]).unwrap();
        assert_eq!(execute(&f, &token, text("insert app set id=3"), grpc).await["code"], denied);
        roles.grant_table("app", "bound", &[Insert]).unwrap();
        for block in [
            text("transaction { insert app set id=2; other select absent }"),
            text("transaction { let first=(insert app set id=3); let later=(other select absent) }"),
        ] {
            assert_eq!(execute(&f, &token, json!({"begin":{}}), grpc).await["code"], "ok");
            let reply = execute(&f, &token, block, grpc).await;
            assert_eq!(reply["code"], denied);
            assert_eq!(reply["transactionState"], "TRANSACTION_STATE_ABORTED");
            execute(&f, &token, json!({"rollback":{}}), grpc).await;
            assert_eq!(f.admin.sql_session().execute("SELECT id FROM app", &[], None).unwrap().rows.len(), 1);
        }
        for table in ["other", "missing"] {
            let error = sql.prepare(&format!("SELECT absent FROM {table}")).unwrap_err();
            assert_eq!(execute(&f, &token, text(&format!("{table} select absent")), grpc).await["code"], sqlstate(&error));
        }
        // Each case has only its command grant: WHERE, RETURNING and conflict
        // updates intentionally follow SQL's current policy, not PG's policy.
        roles.revoke_table("app", "bound", &[Select, Insert]).unwrap();
        for (privilege, source, statement) in [
            (Select, "app select id", "SELECT id FROM app"),
            (Select, "app select id+1 as n", "SELECT id+1 AS n FROM app"),
            (Insert, "insert app set id=3", "INSERT INTO app(id) VALUES (3)"),
            (Insert, "insert into app(id) values(3+0)", "INSERT INTO app(id) VALUES (3+0)"),
            (Insert, "insert many into app(id) values(3),(4) at most 2", "INSERT INTO app(id) VALUES (3),(4)"),
            (Insert, "insert app set id=3 return id", "INSERT INTO app(id) VALUES (3) RETURNING id"),
            (Insert, "insert into app(id) values(2) on conflict(id) do nothing", "INSERT INTO app(id) VALUES (2) ON CONFLICT(id) DO NOTHING"),
            (Insert, "insert into app(id,body) values(2,\"upsert\") on conflict(id) do update set body=incoming.body return id", "INSERT INTO app(id,body) VALUES (2,'upsert') ON CONFLICT(id) DO UPDATE SET body=excluded.body RETURNING id"),
            (Update, "update app where id=2 set body=\"changed\"", "UPDATE app SET body='changed' WHERE id=2"),
            (Update, "update app where id=2 set body=body return id", "UPDATE app SET body=body WHERE id=2 RETURNING id"),
            (Delete, "delete app where id=2 return id", "DELETE FROM app WHERE id=2 RETURNING id"),
        ] {
            let expected = sql.prepare(statement).map(|_| ()).unwrap_err();
            assert_eq!(execute(&f, &token, text(source), grpc).await["code"], sqlstate(&expected), "{source}");
            roles.grant_table("app", "bound", &[privilege]).unwrap();
            let prepared = sql.prepare(statement).unwrap();
            assert!(prepared.authorization_tables().unwrap().iter().all(|t| t == "app"));
            assert_eq!(execute(&f, &token, json!({"begin":{}}), grpc).await["code"], "ok");
            let reply = execute(&f, &token, text(source), grpc).await;
            assert_eq!(reply["code"], "ok", "{source}: {reply}");
            execute(&f, &token, json!({"rollback":{}}), grpc).await;
            roles.revoke_table("app", "bound", &[privilege]).unwrap();
        }
        roles.set_table_owner("app", "bound").unwrap();
        assert_eq!(execute(&f, &token, text("app select id"), grpc).await["code"], "ok");
        f.stop().await;
    }
}
