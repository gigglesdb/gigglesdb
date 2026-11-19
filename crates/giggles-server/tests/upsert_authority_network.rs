//! E939: UPSERT authority is checked before target/excluded name resolution.
mod api_support;

#[cfg(feature = "legacy-task")]
use giggles_core::{CoreError, Error};
#[cfg(feature = "legacy-task")]
use std::sync::Arc;

#[cfg(feature = "legacy-task")]
#[tokio::test]
async fn upsert_authority_precedes_ambiguity_on_native_and_non_sql_surfaces() {
    let f = api_support::Fixture::new().await;
    f.admin
        .sql_session()
        .execute("CREATE TABLE t (id INT PRIMARY KEY, n INT)", &[], None)
        .unwrap();
    let statements = [
        "INSERT INTO t VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET n = n + 1",
        "INSERT INTO t VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET n = t.n + 1",
    ];
    // The application is restricted to app; the task has no SQL authority.
    for binding in [&f.bindings[0], &f.bindings[2]] {
        let mut session = f.service.session(&binding.fingerprint).unwrap();
        for sql in statements {
            let result = session.execute_sql(sql, &[]);
            assert!(
                matches!(result, Err(CoreError::Policy(Error::Scope))),
                "native {:?}, {sql}: {result:?}",
                binding.role,
            );
            assert_eq!(
                giggles_server::sqlstate(result.as_ref().unwrap_err()),
                "42501",
                "direct execute_sql {:?}, {sql}: {result:?}",
                binding.role,
            );
            assert_eq!(
                session.handle(giggles_server::Request::Ping).code,
                "ok",
                "direct execute_sql after {sql}"
            );
            let response = session.handle(giggles_server::Request::Sql {
                sql: sql.into(),
                params: vec![],
            });
            assert_eq!(response.code, "42501", "native {sql}: {response:?}");
            assert!(response.sql.is_none(), "native {sql}: {response:?}");
            assert_eq!(session.handle(giggles_server::Request::Ping).code, "ok");
        }
    }
    let config = giggles_server::tls::server(
        vec![f.server.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(f.server.key.serialize_der()).into(),
        vec![f.ca.cert.der().clone()],
    )
    .unwrap();
    let native = giggles_server::serve(
        "127.0.0.1:0".parse().unwrap(),
        f.service.clone(),
        config,
        16,
    )
    .await
    .unwrap();
    for (cert, task) in [(&f.app, false), (&f.task, true)] {
        let mut config = (*f.client_config(cert, false)).clone();
        config.alpn_protocols.clear();
        let mut client =
            giggles_server::Client::connect(native.address, "localhost", Arc::new(config))
                .await
                .unwrap();
        for sql in statements {
            let response = client
                .call(&giggles_server::Request::Sql {
                    sql: sql.into(),
                    params: vec![],
                })
                .await
                .unwrap();
            assert_eq!(
                response.code, "42501",
                "native task={task}, {sql}: {response:?}"
            );
            assert!(response.sql.is_none(), "{response:?}");
            let usable = if task {
                giggles_server::Request::Status {
                    operation: giggles_core::OperationId(1),
                }
            } else {
                giggles_server::Request::Sql {
                    sql: "SELECT id FROM app".into(),
                    params: vec![],
                }
            };
            assert_eq!(
                client.call(&usable).await.unwrap().code,
                if task { "absent" } else { "ok" }
            );
        }
    }
    native.shutdown().await.unwrap();
    for cert in [&f.app, &f.task] {
        for h2 in [false, true] {
            let token = f.open_http(cert, h2).await;
            for sql in statements {
                let response = f
                    .exec_http(cert, &token, serde_json::json!({"sql":{"sql":sql}}), h2)
                    .await;
                assert_eq!(response["code"], "42501", "HTTP h2={h2}, {sql}: {response}");
                assert!(response.get("sql").is_none(), "{response}");
                let ping = f
                    .exec_http(cert, &token, serde_json::json!({"ping":{}}), h2)
                    .await;
                assert_eq!(ping["code"], "ok", "HTTP h2={h2}, after {sql}: {ping}");
            }
        }
    }
    let path = f.path.clone();
    f.stop().await;
    std::fs::remove_dir_all(path).unwrap();
}
