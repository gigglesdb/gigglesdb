pub(super) mod api_support;
use api_support::Fixture;
#[cfg(feature = "engine-rocksdb")]
use api_support::sql;
use giggles_core::roles::NewRole;
use giggles_server::{
    api::{ApiLimits, ApiRuntime, json::decode_sql_result, net::serve_role_bound, proto},
    auth::RoleApiConfig,
    wire::{client::{Client, ClientTls, Credential}, listen::serve_role_wire_bound},
};
use serde_json::json;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::watch};

pub(super) async fn fixture() -> (Fixture, giggles_server::Running, Client, u32) {
    let engine = if cfg!(feature = "engine-rocksdb") {
        giggles_core::Engine::V2
    } else {
        giggles_core::Engine::Legacy
    };
    let mut f = Fixture::catalog_empty_engine(engine).await;
    f.admin
        .roles()
        .create(NewRole {
            name: "oid_owner".into(),
            superuser: true,
            login: true,
            ..Default::default()
        })
        .unwrap();
    let wire_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config: RoleApiConfig = serde_json::from_value(json!({
        "http":"127.0.0.1:0", "wire":wire_listener.local_addr().unwrap().to_string(), "tls":"required",
        "certificate": f.path.join("server.pem"), "private_key": f.path.join("server.key.TEST-ONLY"),
        "client_ca": f.path.join("ca.pem"),
        "certificates": { (giggles_server::tls::fingerprint(f.app.cert.der())): "oid_owner" }
    })).unwrap();
    let settings = Arc::new(config.build().unwrap());
    let runtime =
        ApiRuntime::new_role(f.service.clone(), ApiLimits::default(), settings.clone()).unwrap();
    let (_, gate) = watch::channel(true);
    let http = serve_role_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        runtime.clone(),
        settings.clone(),
        gate.clone(),
        false,
    )
    .await
    .unwrap();
    let grpc = serve_role_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        runtime.clone(),
        settings.clone(),
        gate.clone(),
        true,
    )
    .await
    .unwrap();
    std::mem::replace(&mut f.http, http)
        .shutdown()
        .await
        .unwrap();
    std::mem::replace(&mut f.grpc, grpc)
        .shutdown()
        .await
        .unwrap();
    f.runtime.shutdown().await;
    f.runtime = runtime;
    let wire = serve_role_wire_bound(
        wire_listener,
        f.runtime.clone(),
        settings,
        gate,
    )
    .await
    .unwrap();
    let tls = ClientTls {
        ca: f.ca.cert.pem().into_bytes(),
        cert: Some(f.app.cert.pem().into_bytes()),
        key: Some(f.app.key.serialize_pem().into_bytes()),
        server_name: "localhost".into(),
    };
    let (client, ready) = Client::connect(
        wire.address,
        Some(tls),
        proto::OpenSessionRequest {
            role: Some("oid_owner".into()),
            database: Some("postgres".into()),
            ..Default::default()
        },
        Credential::None,
    )
    .await
    .unwrap();
    assert_eq!(client.protocol_minor(), 1);
    (f, wire, client, ready.max_shapes)
}

pub(super) fn setup(f: &Fixture, query: &str) {
    f.service
        .role_session("oid_owner")
        .unwrap()
        .execute_sql(query, &[])
        .unwrap_or_else(|error| panic!("{query}: {error:?}"));
}

pub(super) async fn http_result(f: &Fixture, query: &str) -> proto::SqlResult {
    let (status, bytes) = f
        .http(
            &f.app,
            "POST",
            "/v1/sessions",
            None,
            serde_json::to_vec(&json!({"role":"oid_owner", "database":"postgres"})).unwrap(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&bytes));
    let opened: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let token = opened["sessionId"].as_str().unwrap().to_owned();
    let value = f
        .exec_http(&f.app, &token, json!({"sql":{"sql":query}}), false)
        .await;
    assert_eq!(value["code"], "ok", "{query}: {value}");
    let types = value["sql"]["column_types"]
        .as_array()
        .expect("numeric OID array");
    assert!(
        types
            .iter()
            .all(|v| v.as_u64().is_some_and(|n| u32::try_from(n).is_ok()))
    );
    let http = decode_sql_result(&serde_json::to_vec(&value["sql"]).unwrap()).unwrap();
    f.http(&f.app, "DELETE", "/v1/session", Some(&token), vec![], false)
        .await
        .unwrap();
    http
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn wire_types_equal_http_types() {
    let (f, wire, mut client, _) = fixture().await;
    setup(
        &f,
        "CREATE TABLE oid_types (b boolean, s smallint, i integer, l bigint, t text, v varchar(10), c char(3), u uuid, j jsonb, ia integer[], la bigint[], ta text[], ua uuid[], by bytea, n numeric(10,2), f real, d double precision, dt date, tm time, tz timetz, ts timestamp, tstz timestamptz, iv interval)",
    );
    setup(&f, "CREATE TABLE typed_dml (id integer)");
    setup(&f, "CREATE TABLE typed_returning (id integer)");
    for query in [
        "SELECT * FROM oid_types",
        "SELECT count(*), NULL, 1+1, now(), pg_typeof(1) FROM oid_types",
        "CREATE TABLE IF NOT EXISTS typed_ddl (id integer)",
        "INSERT INTO typed_dml VALUES (1)",
        "UPDATE typed_dml SET id = 2",
        "DELETE FROM typed_dml",
        "INSERT INTO typed_returning VALUES (1) RETURNING id",
    ] {
        let http = http_result(&f, query).await;
        for _ in 0..2 {
            let reply = client.execute(sql(query)).await.unwrap();
            assert_eq!(reply.code, "ok", "{query}: {reply:?}");
            let result = reply.sql.unwrap();
            assert_eq!(result.columns, http.columns, "{query}");
            assert_eq!(result.column_types, http.column_types, "{query}");
        }
    }
    client.goodbye().await.unwrap();
    wire.shutdown().await.unwrap();
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn wire_empty_result_typed() {
    let (f, wire, mut client, _) = fixture().await;
    setup(&f, "CREATE TABLE typed_empty (id integer)");
    let result = client
        .execute(sql("SELECT id FROM typed_empty WHERE false"))
        .await
        .unwrap()
        .sql
        .unwrap();
    assert_eq!(result.column_types, [23]);
    assert!(result.rows.is_empty());
    client.goodbye().await.unwrap();
    wire.shutdown().await.unwrap();
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn shape_cap_with_types() {
    use giggles_server::wire::messages::WireReply;
    use prost::Message;

    let (f, wire, mut client, cap) = fixture().await;
    assert_eq!(cap, 256);
    for i in 0..=cap {
        // Equal names with different types consume separate slots.
        let cast = if i % 2 == 0 { "integer" } else { "bigint" };
        let query = format!("SELECT 1::{cast} AS col_{}", i / 2);
        client.send_execute(sql(&query)).await.unwrap();
        let (_, body) = client.recv_raw_frame().await.unwrap();
        let envelope = WireReply::decode(body.as_slice()).unwrap();
        let shape = &envelope.shapes[0];
        assert_eq!(shape.shape_id, (i < cap).then_some(i));
        assert_eq!(shape.columns, [format!("col_{}", i / 2)]);
        assert_eq!(shape.type_oids, [if i % 2 == 0 { 23 } else { 20 }]);
        let result = envelope.reply.unwrap().sql.unwrap();
        assert!(result.columns.is_empty());
        assert!(result.column_types.is_empty());
    }
    client.goodbye().await.unwrap();
    wire.shutdown().await.unwrap();
    f.stop().await;
}
