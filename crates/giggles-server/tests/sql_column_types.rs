//! SQL metadata agrees on HTTP, gRPC and PostgreSQL Describe.
#![cfg(feature = "engine-rocksdb")]
mod api_support;
#[path = "wire_support/mod.rs"]
#[allow(dead_code)]
mod wire_support;

use api_support::{Fixture, rpc, sql};
use giggles_core::roles::NewRole;
use giggles_server::{
    api::{ApiLimits, ApiRuntime, json::decode_sql_result, net::serve_role_bound, proto},
    auth::{PostgresqlConfig, RoleApiConfig, TlsMode, serve_postgresql},
};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::watch,
};
use wire_support::{cstr, parse, pg_read, pg_send, pg_sync};

async fn fixture() -> Fixture {
    let mut f = Fixture::catalog_empty_engine(giggles_core::Engine::V2).await;
    f.admin
        .roles()
        .create(NewRole {
            name: "oid_owner".into(),
            superuser: true,
            login: true,
            ..Default::default()
        })
        .unwrap();
    let config: RoleApiConfig = serde_json::from_value(json!({
        "http":"127.0.0.1:0", "tls":"required",
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
        settings,
        gate,
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
    f
}

/// Inspect the raw HTTP array before decoding it, then compare to protobuf.
async fn api_results(f: &Fixture, query: &str) -> proto::SqlResult {
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
    let mut client = f.grpc_client(&f.app).await;
    let session = client
        .open_session(proto::OpenSessionRequest {
            role: Some("oid_owner".into()),
            database: Some("postgres".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let reply = client
        .execute(rpc(&session.session_id, sql(query)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.code, "ok", "{query}: {reply:?}");
    let grpc = reply.sql.unwrap();
    assert_eq!(http.column_types, grpc.column_types, "{query}");
    assert_eq!(http.columns, grpc.columns, "{query}");
    assert!(http.column_types.is_empty() || http.column_types.len() == http.columns.len());
    f.http(&f.app, "DELETE", "/v1/session", Some(&token), vec![], false)
        .await
        .unwrap();
    client
        .close_session(rpc(&session.session_id, proto::CloseSessionRequest {}))
        .await
        .unwrap();
    http
}

fn setup(f: &Fixture, query: &str) {
    f.service
        .role_session("oid_owner")
        .unwrap()
        .execute_sql(query, &[])
        .unwrap_or_else(|error| panic!("{query}: {error:?}"));
}

#[tokio::test]
async fn oids_equal_pgwire_row_description() {
    let f = fixture().await;
    setup(
        &f,
        "CREATE TABLE oid_types (b boolean, s smallint, i integer, l bigint, t text, v varchar(10), c char(3), u uuid, j jsonb, ia integer[], la bigint[], ta text[], ua uuid[], by bytea, n numeric(10,2), f real, d double precision, dt date, tm time, tz timetz, ts timestamp, tstz timestamptz, iv interval)",
    );
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: BTreeMap::new(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 8,
    };
    let pg = serve_postgresql(
        config.listen,
        f.service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let mut stream = TcpStream::connect(pg.address).await.unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    for value in ["user", "oid_owner", "database", "postgres", ""] {
        cstr(value, &mut startup);
    }
    stream.write_u32((startup.len() + 4) as u32).await.unwrap();
    stream.write_all(&startup).await.unwrap();
    loop {
        let (tag, body) = pg_read(&mut stream).await;
        assert_ne!(tag, b'E', "{}", String::from_utf8_lossy(&body));
        if tag == b'Z' {
            break;
        }
    }
    for (query, expected) in [
        (
            "SELECT * FROM oid_types",
            vec![
                16, 21, 23, 20, 25, 1043, 1042, 2950, 3802, 1007, 1016, 1009, 2951, 17, 1700, 700,
                701, 1082, 1083, 1266, 1114, 1184, 1186,
            ],
        ),
        (
            "SELECT count(*), NULL, 1+1, now(), pg_typeof(1) FROM oid_types",
            vec![20, 25, 23, 1184, 2206],
        ),
    ] {
        pg_send(&mut stream, b'P', &parse("", query)).await;
        pg_send(&mut stream, b'D', &[b'S', 0]).await;
        let frames = pg_sync(&mut stream).await;
        assert!(!frames.iter().any(|(tag, _)| *tag == b'E'), "{frames:?}");
        let body = &frames
            .iter()
            .find(|(tag, _)| *tag == b'T')
            .expect("RowDescription")
            .1;
        let count = u16::from_be_bytes(body[..2].try_into().unwrap()) as usize;
        let mut rest = &body[2..];
        let mut oids = vec![];
        for _ in 0..count {
            let name_end = rest.iter().position(|b| *b == 0).unwrap();
            rest = &rest[name_end + 1..];
            oids.push(u32::from_be_bytes(rest[6..10].try_into().unwrap()));
            rest = &rest[18..];
        }
        let api = api_results(&f, query).await;
        for (index, (pg_oid, api_oid)) in oids.iter().zip(&api.column_types).enumerate() {
            assert_eq!(
                pg_oid, api_oid,
                "column {}: pgwire OID {pg_oid}, API OID {api_oid}",
                api.columns[index]
            );
        }
        assert_eq!(oids, api.column_types, "{query}");
        assert_eq!(oids, expected, "{query}");
    }
    pg_send(&mut stream, b'X', &[]).await;
    drop(stream);
    pg.shutdown().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn empty_result_is_typed_http_and_grpc() {
    let f = fixture().await;
    setup(&f, "CREATE TABLE typed_empty (id integer)");
    let result = api_results(&f, "SELECT id FROM typed_empty WHERE false").await;
    assert_eq!(result.column_types, vec![23]);
    assert!(result.rows.is_empty());
    f.stop().await;
}

#[tokio::test]
async fn ddl_and_dml_without_returning_have_no_types() {
    let f = fixture().await;
    // Each protocol executes against its own table so CREATE is repeatable.
    setup(&f, "CREATE TABLE typed_dml (id integer)");
    for query in [
        "CREATE TABLE IF NOT EXISTS typed_ddl (id integer)",
        "INSERT INTO typed_dml VALUES (1)",
        "UPDATE typed_dml SET id = 2",
        "DELETE FROM typed_dml",
    ] {
        let result = api_results(&f, query).await;
        assert!(result.columns.is_empty(), "{query}");
        assert!(result.column_types.is_empty(), "{query}");
    }
    f.stop().await;
}

#[tokio::test]
async fn insert_returning_is_typed() {
    let f = fixture().await;
    setup(&f, "CREATE TABLE typed_returning (id integer)");
    let result = api_results(&f, "INSERT INTO typed_returning VALUES (1) RETURNING id").await;
    assert_eq!(result.column_types, vec![23]);
    assert_eq!(result.rows.len(), 1);
    f.stop().await;
}

#[test]
fn openapi_column_types_schema() {
    let api: serde_json::Value =
        serde_json::from_str(include_str!("../docs/http-api.openapi.json")).unwrap();
    let schema = &api["components"]["schemas"]["SqlResult"]["properties"]["column_types"];
    assert_eq!(schema["type"], "array");
    assert_eq!(schema["items"]["type"], "integer");
    assert_eq!(schema["items"]["format"], "uint32");
    assert_eq!(
        schema["description"],
        "PostgreSQL type OID per column, parallel to `columns`, as the PostgreSQL listener's RowDescription reports it. Empty when `columns` is empty."
    );
}
