use bytes::{BufMut, BytesMut};
use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    error::Error,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{
    Client, NoTls, SimpleQueryMessage,
    types::{IsNull, ToSql, Type, to_sql_checked},
};

#[derive(Debug)]
struct PgDate(i32);

impl ToSql for PgDate {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_i32(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::DATE
    }

    to_sql_checked!();
}

async fn rows(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|column| row.get(column).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn temporal_casts_over_pgwire() {
    let path = std::env::temp_dir().join(format!(
        "temporal-cast-network-{}-{}",
        std::process::id(),
        UNIX_EPOCH.elapsed().unwrap().as_nanos()
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
    let listener = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(listener.address.ip())
        .port(listener.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);

    client.batch_execute(
        "SET TIME ZONE 'UTC'; CREATE TABLE events (id int PRIMARY KEY, day date, clock_time time, ts timestamp, happened_at timestamptz)"
    ).await.unwrap();
    let insert = client
        .prepare_typed(
            "INSERT INTO events (id, day, clock_time, ts, happened_at) VALUES \
         ($3, $1::timestamp::date, $2::timestamptz::time, $1::timestamp, $2::timestamptz)",
            &[Type::TIMESTAMP, Type::TIMESTAMPTZ, Type::INT4],
        )
        .await
        .unwrap();
    let timestamp = UNIX_EPOCH + Duration::from_secs(1_715_076_000); // 2024-05-07 10:00
    for (id, seconds) in [
        (1_i32, 1_715_131_800), // 2024-05-08 01:30 UTC
        (2, 1_715_211_000),     // 2024-05-08 23:30 UTC
    ] {
        let happened_at = UNIX_EPOCH + Duration::from_secs(seconds);
        assert_eq!(
            client
                .execute(&insert, &[&timestamp, &happened_at, &id])
                .await
                .unwrap(),
            1
        );
    }
    assert_eq!(
        rows(&client, "SELECT day, clock_time FROM events ORDER BY id").await,
        [
            vec![Some("2024-05-07".into()), Some("01:30:00".into())],
            vec![Some("2024-05-07".into()), Some("23:30:00".into())],
        ],
    );
    assert_eq!(
        client
            .execute("UPDATE events SET day = happened_at::date", &[])
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        rows(&client, "SELECT day FROM events ORDER BY id").await,
        [
            vec![Some("2024-05-08".into())],
            vec![Some("2024-05-08".into())]
        ],
    );
    assert_eq!(
        rows(
            &client,
            "SELECT happened_at::date, count(*) FROM events GROUP BY 1"
        )
        .await,
        [vec![Some("2024-05-08".into()), Some("2".into())]],
    );

    let prepared = client
        .prepare_typed("SELECT $1::date::timestamp", &[Type::DATE])
        .await
        .unwrap();
    assert_eq!(prepared.params(), [Type::DATE]);
    assert_eq!(*prepared.columns()[0].type_(), Type::TIMESTAMP);
    let row = client.query_one(&prepared, &[&PgDate(0)]).await.unwrap();
    assert_eq!(
        row.get::<_, SystemTime>(0),
        UNIX_EPOCH + Duration::from_secs(946_684_800)
    );
    let truncated = client
        .prepare("SELECT date_trunc('week', ts::date) FROM events ORDER BY id")
        .await
        .unwrap();
    assert_eq!(*truncated.columns()[0].type_(), Type::TIMESTAMPTZ);
    assert_eq!(
        rows(
            &client,
            "SELECT date_trunc('week', ts::date) FROM events ORDER BY id"
        )
        .await,
        [
            vec![Some("2024-05-06 00:00:00+00".into())],
            vec![Some("2024-05-06 00:00:00+00".into())]
        ],
    );

    let error = client
        .prepare_typed("SELECT $1::date::time", &[Type::DATE])
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "42846");
    let error = client
        .simple_query("SELECT day::interval FROM events WHERE id=99")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "42846");
    client
        .batch_execute("SET TIME ZONE 'America/New_York'")
        .await
        .unwrap();
    assert_eq!(
        rows(
            &client,
            "SELECT happened_at::date, happened_at::timetz FROM events ORDER BY id"
        )
        .await,
        [
            vec![Some("2024-05-07".into()), Some("21:30:00-04".into())],
            vec![Some("2024-05-08".into()), Some("19:30:00-04".into())],
        ],
    );

    let prepared = client
        .prepare("SELECT date_trunc($1,date '2024-05-08')::text")
        .await
        .unwrap();
    assert_eq!(prepared.params(), [Type::TEXT]);
    let truncated = client.query_one(&prepared, &[&"weeks"]).await.unwrap();
    assert_eq!(truncated.get::<_, &str>(0), "2024-05-06 00:00:00-04");
    assert_eq!(
        rows(
            &client,
            "SELECT date_trunc('years', date '2024-05-08'), \
                    date_trunc('seconds'::varchar, date '2024-05-08'), \
                    date '4714-11-24 BC'::timestamptz, \
                    '4714-11-24 00:00:00+00 BC'::timestamptz::time, \
                    '4714-11-24 00:00:00+00 BC'::timestamptz::timetz",
        )
        .await,
        [vec![
            Some("2024-01-01 00:00:00-05".into()),
            Some("2024-05-08 00:00:00-04".into()),
            Some("4714-11-24 00:00:00-04:56:02 BC".into()),
            Some("19:03:58".into()),
            Some("19:03:58-04:56:02".into()),
        ]],
    );
    for (sql, state) in [
        ("SELECT date_trunc('julian', date '2024-03-10')", "22023"),
        ("SELECT date_trunc('timezone', date '2024-03-10')", "0A000"),
        (
            "SELECT '294276-12-31 23:59:59.999999'::timestamp::timestamptz",
            "22008",
        ),
        (
            "SELECT '4714-11-24 00:00:00+00 BC'::timestamptz::timestamp",
            "22008",
        ),
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), state, "{sql}");
    }
    client
        .batch_execute("SET TIME ZONE 'Asia/Kolkata'")
        .await
        .unwrap();
    assert_eq!(
        rows(
            &client,
            "SELECT '294276-12-31 23:59:59.999999+00'::timestamptz::date, \
                    '294276-12-31 23:59:59.999999+00'::timestamptz::time, \
                    '294276-12-31 23:59:59.999999+00'::timestamptz::timetz",
        )
        .await,
        [vec![
            Some("294277-01-01".into()),
            Some("05:29:59.999999".into()),
            Some("05:29:59.999999+05:30".into()),
        ]],
    );
    let error = client
        .simple_query("SELECT '294276-12-31 23:59:59.999999+00'::timestamptz::timestamp")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "22008");

    drop(client);
    connection.await.unwrap().unwrap();
    listener.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
