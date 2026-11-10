#![cfg(feature = "engine-rocksdb")]

use bytes::BytesMut;
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, error::Error, sync::Arc};
use tokio_postgres::{
    NoTls, SimpleQueryMessage,
    types::{FromSql, IsNull, ToSql, Type, to_sql_checked},
};

#[derive(Debug, PartialEq)]
struct Uuid([u8; 16]);

impl ToSql for Uuid {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::UUID
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for Uuid {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(raw.try_into()?))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::UUID
    }
}

#[tokio::test]
async fn uuid_extended_protocol_types_and_values() {
    let path = std::env::temp_dir().join(format!("v2-uuid-{}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..NewRole::default()
        })
        .unwrap();
    let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: BTreeMap::new(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 4,
    };
    let pg = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(pg.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    // Npgsql caches built-in types when connecting, before application DDL.
    let types = client
        .query(
            "SELECT oid FROM pg_catalog.pg_type WHERE typname = 'uuid'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(types.len(), 1, "uuid must exist before any UUID column");
    assert_eq!(types[0].get::<_, u32>(0), 2950);
    client
        .batch_execute("CREATE TABLE uuid_values (id integer PRIMARY KEY, v uuid)")
        .await
        .unwrap();
    let insert = client
        .prepare("INSERT INTO uuid_values (id, v) VALUES ($1, $2)")
        .await
        .unwrap();
    assert_eq!(insert.params(), &[Type::INT4, Type::UUID]);
    let select = client
        .prepare("SELECT v FROM uuid_values WHERE id = $1")
        .await
        .unwrap();
    assert_eq!(*select.columns()[0].type_(), Type::UUID);
    let value = Uuid([
        0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56,
        0x78,
    ]);
    client.execute(&insert, &[&1i32, &value]).await.unwrap();
    client
        .execute(&insert, &[&2i32, &None::<Uuid>])
        .await
        .unwrap();
    let row = client.query_one(&select, &[&1i32]).await.unwrap();
    assert_eq!(row.get::<_, Uuid>(0), value);
    let row = client.query_one(&select, &[&2i32]).await.unwrap();
    assert_eq!(row.get::<_, Option<Uuid>>(0), None);
    let rows = client
        .simple_query("SELECT v FROM uuid_values WHERE id = 1")
        .await
        .unwrap();
    let row = rows
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("expected row");
    assert_eq!(row.get(0), Some("12345678-1234-5678-1234-567812345678"));
    drop(client);
    connection.await.unwrap().unwrap();
    pg.shutdown().await.unwrap();
    drop(service);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
