use bytes::BytesMut;
use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, error::Error, sync::Arc, time::SystemTime};
use tokio_postgres::{
    NoTls,
    types::{Format, IsNull, ToSql, Type, to_sql_checked},
};

#[derive(Debug)]
struct TextParameter(&'static str);

impl ToSql for TextParameter {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::TEXT | Type::BOOL)
    }

    fn encode_format(&self, _: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

#[tokio::test]
async fn extended_union_resolves_unknowns_with_correlated_array_predicate() {
    let path = std::env::temp_dir().join(format!(
        "needs-query-network-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..NewRole::default()
        })
        .unwrap();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
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
    let server = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(server.address.ip())
        .port(server.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);

    client
        .batch_execute(
            "CREATE TABLE variations (id int PRIMARY KEY, version int);
         CREATE TABLE history (variation_id int PRIMARY KEY, authors uuid[]);
         INSERT INTO variations VALUES (1, 2), (2, 3);
         INSERT INTO history VALUES (1, '{00000000-0000-4000-8000-000000000001}'), (2, '{}');",
        )
        .await
        .unwrap();

    // Push this ordinary query through the catalog overflow routing path.
    let predicates = " AND (1 = 1)".repeat(65);
    let sql = format!(
        "WITH waiting AS (
             SELECT 0 AS id, 'initial'::text AS kind, NULL::int AS days, NULL::text AS note
             UNION ALL
             SELECT v.id, 'stale', NULL, NULL FROM variations v
             WHERE v.version > 1 AND EXISTS (
                 SELECT 1 FROM history h
                 WHERE h.variation_id = v.id AND $1::uuid = ANY(h.authors)
             ) AND $2{predicates}
         )
         SELECT * FROM waiting ORDER BY id"
    );
    let statement = client
        .prepare_typed(&sql, &[Type::TEXT, Type::BOOL])
        .await
        .unwrap();
    assert_eq!(
        statement
            .columns()
            .iter()
            .map(|column| column.type_().clone())
            .collect::<Vec<_>>(),
        [Type::INT4, Type::TEXT, Type::INT4, Type::TEXT]
    );
    for (enabled, expected) in [
        (
            "true",
            vec![
                (0, "initial".to_owned(), None, None),
                (1, "stale".to_owned(), None, None),
            ],
        ),
        ("false", vec![(0, "initial".to_owned(), None, None)]),
    ] {
        let rows: Vec<(i32, String, Option<i32>, Option<String>)> = client
            .query(
                &statement,
                &[
                    &TextParameter("00000000-0000-4000-8000-000000000001"),
                    &TextParameter(enabled),
                ],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
            .collect();
        assert_eq!(rows, expected);
    }

    drop(client);
    connection.await.unwrap().unwrap();
    server.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
