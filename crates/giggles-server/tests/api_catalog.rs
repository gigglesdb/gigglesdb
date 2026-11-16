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
fn alter(action: Value) -> Value {
    let mut v = json!({"table":"app.t"});
    v.as_object_mut().unwrap().extend(action.as_object().unwrap().clone());
    json!({"catalog":{"alterTable":v}})
}
fn bind(f: &Fixture, name: &str) {
    let mut bindings = f.bindings.clone();
    bindings[0].role = giggles_server::Role::Database { name:name.into() };
    f.service.reload_bindings(bindings).unwrap();
}
fn setup(f: &Fixture) {
    f.admin.sql_session().execute("CREATE SCHEMA app", &[], None).unwrap();
    for (name, superuser) in [("owner",true),("reader",false)] {
        f.admin.roles().create(giggles_core::roles::NewRole { name:name.into(), login:true,
            superuser, ..Default::default() }).unwrap();
    }
    bind(f,"owner");
}
fn same_catalog(a: &Fixture, b: &Fixture) {
    for table in ["app.t","app.renamed"] {
        let source=format!("SELECT * FROM {table}");
        let left=a.admin.sql_session().execute(&source,&[],None);
        let right=b.admin.sql_session().execute(&source,&[],None);
        match (left,right) {
            (Ok(l),Ok(r))=>{assert_eq!(l.columns,r.columns);assert_eq!(l.rows,r.rows);},
            (Err(l),Err(r))=>assert_eq!(giggles_server::sqlstate(&l),giggles_server::sqlstate(&r)),
            other=>panic!("{source}: {other:?}"),
        }
    }
    assert_eq!(a.admin.catalog().unwrap().1, b.admin.catalog().unwrap().1);
    assert_eq!(a.admin.secondary_index_catalog().unwrap().1, b.admin.secondary_index_catalog().unwrap().1);
}
#[tokio::test]
async fn catalog_actions_and_sqlstate_matrix() {
    for grpc in [false,true] {
        let f = Fixture::catalog_empty().await;
        let oracle = Fixture::catalog_empty().await;
        setup(&f); setup(&oracle);
        let token = f.open_http(&f.app,false).await;
        let mut sql = oracle.service.role_session("owner").unwrap();
        let cases = vec![
            (create("app.t"), "CREATE TABLE app.t(id BIGINT PRIMARY KEY)", "ok"),
            (create("app.t"), "CREATE TABLE app.t(id BIGINT PRIMARY KEY)", "42P07"),
            (alter(json!({"addColumn":{"name":"body","dataType":"Text"}})), "ALTER TABLE app.t ADD COLUMN body TEXT", "ok"),
            (alter(json!({"addColumn":{"name":"body","dataType":"Text"}})), "ALTER TABLE app.t ADD COLUMN body TEXT", "42701"),
            (alter(json!({"renameColumn":{"from":"absent","to":"x"}})), "ALTER TABLE app.t RENAME COLUMN absent TO x", "42703"),
            (alter(json!({"renameColumn":{"from":"body","to":"label"}})), "ALTER TABLE app.t RENAME COLUMN body TO label", "ok"),
            (json!({"catalog":{"createIndex":{"table":"app.t","name":"t_label","columns":["label"]}}}), "CREATE INDEX t_label ON app.t(label)", "ok"),
            (json!({"catalog":{"dropIndex":{"name":"app.t_label"}}}), "DROP INDEX app.t_label", "ok"),
            (json!({"catalog":{"dropIndex":{"name":"app.absent"}}}), "DROP INDEX app.absent", "42704"),
            (alter(json!({"dropColumn":{"name":"label"}})), "ALTER TABLE app.t DROP COLUMN label", "ok"),
            (alter(json!({"renameTable":{"to":"renamed"}})), "ALTER TABLE app.t RENAME TO renamed", "ok"),
            (json!({"catalog":{"dropTable":{"table":"app.renamed"}}}), "DROP TABLE app.renamed", "ok"),
            (json!({"catalog":{"dropTable":{"table":"app.absent"}}}), "DROP TABLE app.absent", "42P01"),
            (create("absent.t"), "CREATE TABLE absent.t(id BIGINT PRIMARY KEY)", "3F000"),
        ];
        for (request, source, code) in cases {
            if source == "DROP TABLE app.renamed" {
                for fixture in [&f,&oracle] {
                    fixture.admin.sql_session().execute("CREATE TABLE app.dep(id BIGINT PRIMARY KEY, parent BIGINT REFERENCES app.renamed(id))",&[],None).unwrap();
                }
                let reply=execute(&f,&token,request.clone(),grpc).await;
                assert_code(&reply,"2BP01");
                let error=sql.prepare(source).and_then(|q|sql.execute_prepared(&q,&[])).unwrap_err();
                assert_eq!(giggles_server::sqlstate(&error),"2BP01");
                for fixture in [&f,&oracle] { fixture.admin.sql_session().execute("DROP TABLE app.dep",&[],None).unwrap(); }
            }
            let reply = execute(&f,&token,request,grpc).await;
            assert_code(&reply,code);
            let result = sql.prepare(source).and_then(|q| sql.execute_prepared(&q,&[]));
            assert_eq!(result.as_ref().err().map(giggles_server::sqlstate).unwrap_or("ok"),code,"{source}: {result:?}");
            if code=="ok" {
                assert!(reply["catalogOutcome"]["columns"].is_array(),"{reply}");
                if source.starts_with("CREATE TABLE") {assert_eq!(reply["catalogOutcome"]["primaryKey"],json!(["id"]));}
                if source.starts_with("CREATE INDEX") {assert!(reply["catalogOutcome"]["indexId"].is_string() || reply["catalogOutcome"]["indexId"].is_number(),"{reply}");}
            }
            same_catalog(&f,&oracle);
        }
        f.stop().await; oracle.stop().await;
    }
}
#[tokio::test]
async fn catalog_transactions_search_path_and_authority() {
    for grpc in [false,true] {
        let f = Fixture::catalog_empty().await; setup(&f);
        let token = f.open_http(&f.app,false).await;
        for (request, code) in [
            (json!({"searchPath":{"set":true,"schemas":["app","public"]}}),"ok"),
            (json!({"begin":{}}),"ok"), (create("t"),"ok"),
            (json!({"rollback":{}}),"ok"),
        ] {
            let reply = execute(&f,&token,request,grpc).await;
            assert_code(&reply,code);
            if !reply["catalogOutcome"].is_null() { assert_eq!(reply["catalogOutcome"]["table"],"app.t"); }
        }
        assert_eq!(giggles_server::sqlstate(&f.admin.sql_session().execute("SELECT * FROM app.t",&[],None).unwrap_err()),"42P01");
        for request in [json!({"begin":{}}),create("t"),json!({"commit":{}})] {
            let reply=execute(&f,&token,request,grpc).await; assert_eq!(reply["code"],"ok","{reply}");
        }
        assert!(f.admin.sql_session().execute("SELECT * FROM app.t",&[],None).is_ok());
        for (request,code) in [(json!({"begin":{}}),"ok"),(create("t"),"42P07"),
            (create("blocked"),"25P02"),(json!({"rollback":{}}),"ok"),
            (json!({"begin":{}}),"ok"),(json!({"savepoint":{"name":"s"}}),"ok"),
            (create("undone"),"ok"),(json!({"rollbackTo":{"name":"s"}}),"ok"),
            (create("kept"),"ok"),(json!({"commit":{}}),"ok")] {
            let reply=execute(&f,&token,request,grpc).await; assert_code(&reply,code);
        }
        let before=f.admin.catalog().unwrap();
        assert!(f.admin.sql_session().execute("SELECT * FROM app.kept",&[],None).is_ok());
        assert_eq!(giggles_server::sqlstate(&f.admin.sql_session().execute("SELECT * FROM app.undone",&[],None).unwrap_err()),"42P01");
        for reader in [false,true] {
            if reader { bind(&f,"reader"); } else { f.service.reload_bindings(f.bindings.clone()).unwrap(); }
            let token=f.open_http(&f.app,false).await;
            assert_eq!(execute(&f,&token,create("denied"),grpc).await["code"],"42501");
            assert_eq!(f.admin.catalog().unwrap(),before);
        }
        f.stop().await;
    }
}
#[tokio::test]
async fn catalog_explicit_native_sql_differences() {
    for grpc in [false,true] {
        for populated in [false,true] {
            let f=if populated { Fixture::new().await } else { Fixture::catalog_empty().await };
            setup(&f); let token=f.open_http(&f.app,false).await;
            let mut request=create("app.t");
            if !populated { request["catalog"]["createTable"]["primaryKey"]=json!([]); }
            let reply=execute(&f,&token,request,grpc).await;
            assert_eq!(reply["code"],"native_error","{reply}");
            assert_eq!(reply["nativeError"]["kind"],if populated {"NATIVE_ERROR_CLASS_PLAN_REQUIRED"} else {"NATIVE_ERROR_CLASS_BIND"},"{reply}");
            let mut sql=f.service.role_session("owner").unwrap();
            let source=if populated {"CREATE TABLE app.t(id BIGINT PRIMARY KEY)"} else {"CREATE TABLE app.t(id BIGINT)"};
            let q=sql.prepare(source).unwrap(); sql.execute_prepared(&q,&[]).unwrap();
            assert!(f.admin.sql_session().execute("SELECT * FROM app.t",&[],None).is_ok());
            if !populated {
                let reply=execute(&f,&token,json!({"catalog":{"dropTable":{"table":"app.t"}}}),grpc).await;
                assert_eq!(reply["code"],"ok","{reply}");
                assert_eq!(reply["catalogOutcome"]["columns"],json!([{"name":"id","dataType":"BigInt","notNull":false}]));
                assert_eq!(reply["catalogOutcome"]["primaryKey"],json!([]));
            }
            f.stop().await;
        }
    }
}
#[tokio::test]
async fn catalog_malformed_transport_arguments() {
    use giggles_server::api::proto as p;
    use p::{execute_request::Request as R,native_catalog_request::Request as C,native_alter_table::Action as A};
    let f=Fixture::catalog_empty().await; setup(&f);
    let token=f.open_http(&f.app,false).await;
    let mut cases=vec![(json!({}),p::ExecuteRequest::default()),
        (json!({"catalog":{}}),p::ExecuteRequest {request:Some(R::Catalog(p::NativeCatalogRequest::default())),..Default::default()})];
    let mut requests=vec![
        (json!({"createTable":{}}),C::CreateTable(p::NativeCreateTable::default())),
        (json!({"createIndex":{}}),C::CreateIndex(p::NativeCreateIndex::default())),
        (json!({"createIndex":{"table":"t"}}),C::CreateIndex(p::NativeCreateIndex {table:Some("t".into()),..Default::default()})),
        (json!({"dropTable":{}}),C::DropTable(p::NativeDropTable::default())),
        (json!({"dropIndex":{}}),C::DropIndex(p::NativeDropIndex::default())),
        (json!({"alterTable":{"table":"t"}}),C::AlterTable(p::NativeAlterTable {table:Some("t".into()),action:None})),
    ];
    for (key,action) in [
        (json!({"renameColumn":{"to":"x"}}),A::RenameColumn(p::NativeRenameColumn {from:None,to:Some("x".into())})),
        (json!({"renameColumn":{"from":"x"}}),A::RenameColumn(p::NativeRenameColumn {from:Some("x".into()),to:None})),
        (json!({"renameTable":{}}),A::RenameTable(p::NativeRenameTable::default())),
        (json!({"dropColumn":{}}),A::DropColumn(p::NativeDropColumn::default())),
    ] {
        let mut body=json!({"table":"t"}); body.as_object_mut().unwrap().extend(key.as_object().unwrap().clone());
        requests.push((json!({"alterTable":body}),C::AlterTable(p::NativeAlterTable {table:Some("t".into()),action:Some(action)})));
    }
    requests.push((json!({"alterTable":{"renameTable":{"to":"x"}}}),C::AlterTable(p::NativeAlterTable {table:None,action:Some(A::RenameTable(p::NativeRenameTable {to:Some("x".into())}))})));
    for (name,data_type) in [(None,Some("BigInt")),(Some("id"),None),(Some("id"),Some("bogus"))] {
        let mut column=json!({});
        if let Some(v)=name {column["name"]=json!(v);}
        if let Some(v)=data_type {column["dataType"]=json!(v);}
        requests.push((json!({"createTable":{"table":"t","columns":[column]}}),C::CreateTable(p::NativeCreateTable {table:Some("t".into()),columns:vec![p::NativeCatalogColumn {name:name.map(str::to_owned),data_type:data_type.map(str::to_owned),not_null:None,..Default::default()}],..Default::default()})));
    }
    requests.push((json!({"createIndex":{"name":"i"}}),C::CreateIndex(p::NativeCreateIndex {name:Some("i".into()),..Default::default()})));
    requests.push((json!({"createTable":{"columns":[{"name":"id","dataType":"BigInt"}],"primaryKey":["id"]}}),C::CreateTable(p::NativeCreateTable {table:None,columns:vec![p::NativeCatalogColumn {name:Some("id".into()),data_type:Some("BigInt".into()),not_null:None,..Default::default()}],primary_key:vec!["id".into()]})));
    for (name,data_type) in [(None,Some("BigInt")),(Some("id"),None),(Some("id"),Some("bogus"))] {
        let mut column=json!({});
        if let Some(v)=name {column["name"]=json!(v);}
        if let Some(v)=data_type {column["dataType"]=json!(v);}
        requests.push((json!({"alterTable":{"table":"t","addColumn":column}}),C::AlterTable(p::NativeAlterTable {table:Some("t".into()),action:Some(A::AddColumn(p::NativeCatalogColumn {name:name.map(str::to_owned),data_type:data_type.map(str::to_owned),not_null:None,..Default::default()}))})));
    }
    let col=p::NativeCatalogColumn {name:Some("id".into()),data_type:Some("BigInt".into()),not_null:None,..Default::default()};
    requests.extend([
        (json!({"createTable":{"table":"t","columns":vec![json!({"name":"id","dataType":"BigInt"});1601]}}),C::CreateTable(p::NativeCreateTable {table:Some("t".into()),columns:vec![col;1601],..Default::default()})),
        (json!({"createTable":{"table":"t","primaryKey":vec!["id";1601]}}),C::CreateTable(p::NativeCreateTable {table:Some("t".into()),primary_key:vec!["id".into();1601],..Default::default()})),
        (json!({"createIndex":{"table":"t","name":"i","columns":vec!["id";1601]}}),C::CreateIndex(p::NativeCreateIndex {table:Some("t".into()),name:Some("i".into()),columns:vec!["id".into();1601]})),
    ]);
    cases.extend(requests.into_iter().map(|(v,c)|(json!({"catalog":v}),p::ExecuteRequest {request:Some(R::Catalog(p::NativeCatalogRequest {request:Some(c)})),..Default::default()})));
    let before=f.admin.catalog().unwrap();
    for (body,request) in cases {
        let (status,bytes)=f.http(&f.app,"POST","/v1/session/execute",Some(&token),serde_json::to_vec(&body).unwrap(),false).await.unwrap();
        assert_eq!(status,400,"{body}: {}",String::from_utf8_lossy(&bytes));
        assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap()["error"]["code"],"INVALID_ARGUMENT");
        let error=f.grpc_client(&f.app).await.execute(rpc(&token,request)).await.unwrap_err();
        assert_eq!(error.code(),tonic::Code::InvalidArgument,"{body}: {error}");
        assert_eq!(f.admin.catalog().unwrap(),before);
    }
    for body in [json!({"catalog":{"dropTable":{"table":"t","unknown":true}}}),json!({"catalog":{"unknown":{}}}),json!({"catalog":{"alterTable":{"table":"t","renameTable":{"to":"u","unknown":true}}}})] {
        let bytes=serde_json::to_vec(&body).unwrap(); assert!(wire_json::decode_execute(&bytes).is_err());
        let (status,bytes)=f.http(&f.app,"POST","/v1/session/execute",Some(&token),bytes,false).await.unwrap();
        assert_eq!(status,400);
        assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap()["error"]["code"],"INVALID_ARGUMENT");
    }
    f.stop().await;
}
