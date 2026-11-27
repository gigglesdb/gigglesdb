use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::{Client, NoTls, error::SqlState};

async fn expect_error(
    client: &Client,
    sql: &str,
    state: SqlState,
    message: &str,
    detail: Option<&str>,
) {
    let error = client.batch_execute(sql).await.expect_err(sql);
    let error = error.as_db_error().expect(sql);
    assert_eq!(error.code(), &state, "{sql}");
    assert_eq!(error.message(), message, "{sql}");
    if let Some(detail) = detail {
        assert_eq!(error.detail(), Some(detail), "{sql}");
    }
}

async fn exercise(engine: Engine) {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("fk-type-pairs-{}-{nonce}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..Default::default()
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
    let running = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);

    client.batch_execute(
        "CREATE TABLE bookings(day date UNIQUE);
         CREATE TABLE slots(id int PRIMARY KEY, at timestamp REFERENCES bookings(day) ON UPDATE CASCADE);
         INSERT INTO bookings VALUES ('2020-01-01');
         INSERT INTO slots VALUES (1,'2020-01-01');
         CREATE TABLE departures(at timestamp UNIQUE);
         CREATE TABLE tickets(id int PRIMARY KEY, day date);
         ALTER TABLE tickets ADD CONSTRAINT tickets_departure_fkey FOREIGN KEY (day)
             REFERENCES departures(at) ON UPDATE CASCADE;
         INSERT INTO departures VALUES ('2020-01-01'),('2020-01-05 12:00');
         INSERT INTO tickets VALUES (1,'2020-01-01');
         CREATE TABLE countries(code text PRIMARY KEY);
         CREATE TABLE addresses(id int PRIMARY KEY, country char(2)
             REFERENCES countries ON DELETE RESTRICT ON UPDATE CASCADE);
         INSERT INTO countries VALUES ('a'),('a '),('b');
         INSERT INTO addresses VALUES (1,'a'),(2,'b');",
    ).await.unwrap();

    expect_error(
        &client,
        "INSERT INTO slots VALUES (2,'2020-01-01 12:00')",
        SqlState::FOREIGN_KEY_VIOLATION,
        "insert or update on table \"slots\" violates foreign key constraint \"slots_at_fkey\"",
        Some("Key (at)=(2020-01-01 12:00:00) is not present in table \"bookings\"."),
    )
    .await;
    client
        .batch_execute("UPDATE bookings SET day='2020-01-02'")
        .await
        .unwrap();
    let at: String = client
        .query_one("SELECT at::text FROM slots", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(at, "2020-01-02 00:00:00");

    for (sql, detail) in [
        (
            "INSERT INTO tickets VALUES (2,'2020-01-05')",
            "Key (day)=(2020-01-05) is not present in table \"departures\".",
        ),
        (
            "UPDATE departures SET at='2020-01-02 06:00' WHERE at='2020-01-01'",
            "Key (day)=(2020-01-02) is not present in table \"departures\".",
        ),
    ] {
        expect_error(
            &client, sql, SqlState::FOREIGN_KEY_VIOLATION,
            "insert or update on table \"tickets\" violates foreign key constraint \"tickets_departure_fkey\"",
            Some(detail),
        ).await;
    }
    let day: String = client
        .query_one("SELECT day::text FROM tickets", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(day, "2020-01-01");

    expect_error(
        &client, "DELETE FROM countries WHERE code='a'", SqlState::RESTRICT_VIOLATION,
        "update or delete on table \"countries\" violates RESTRICT setting of foreign key constraint \"addresses_country_fkey\" on table \"addresses\"",
        Some("Key (code)=(a) is referenced from table \"addresses\"."),
    ).await;
    assert_eq!(
        client
            .execute("DELETE FROM countries WHERE code='a '", &[])
            .await
            .unwrap(),
        1
    );
    expect_error(
        &client, "UPDATE countries SET code='c ' WHERE code='b'", SqlState::FOREIGN_KEY_VIOLATION,
        "insert or update on table \"addresses\" violates foreign key constraint \"addresses_country_fkey\"",
        Some("Key (country)=(c ) is not present in table \"countries\"."),
    ).await;
    expect_error(
        &client,
        "UPDATE countries SET code='abc' WHERE code='b'",
        SqlState::STRING_DATA_RIGHT_TRUNCATION,
        "value too long for type character(2)",
        None,
    )
    .await;
    let country: String = client
        .query_one("SELECT country::text FROM addresses WHERE id=2", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(country, "b");

    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn temporal_and_char_foreign_keys_legacy() {
    exercise(Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn temporal_and_char_foreign_keys_v2() {
    exercise(Engine::V2).await;
}
