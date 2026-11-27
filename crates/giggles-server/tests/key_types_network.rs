use super::session_settings_network::Fixture;
use giggles_core::Engine;
use std::time::{Duration, UNIX_EPOCH};
use tokio_postgres::types::{IsNull, ToSql, Type};

#[derive(Debug)]
struct DateParam(i32);

impl ToSql for DateParam {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync>> {
        out.extend_from_slice(&self.0.to_be_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::DATE
    }

    tokio_postgres::types::to_sql_checked!();
}

async fn exercise(engine: Engine) {
    let fixture = Fixture::with_engine(engine).await;
    let client = fixture.client().await;
    for sql in [
        "CREATE TABLE countries(code char(2) PRIMARY KEY, label text)",
        "CREATE TABLE daily_facts(country char(2) REFERENCES countries, day date, count int, PRIMARY KEY(country, day))",
        "CREATE TABLE events(at timestamptz PRIMARY KEY, title text)",
    ] {
        client.batch_execute(sql).await.unwrap();
    }

    let country = client
        .prepare("INSERT INTO countries VALUES ($1, $2)")
        .await
        .unwrap();
    assert_eq!(country.params(), &[Type::BPCHAR, Type::TEXT]);
    client
        .execute(&country, &[&"US", &"United States"])
        .await
        .unwrap();
    let error = client
        .execute(&country, &[&"US ", &"duplicate"])
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23505");
    assert_eq!(
        error.message(),
        "duplicate key value violates unique constraint \"countries_pkey\""
    );

    let upsert = client
        .prepare(
            "INSERT INTO daily_facts VALUES ($1, $2, $3) \
         ON CONFLICT (country, day) DO UPDATE SET count = excluded.count RETURNING count",
        )
        .await
        .unwrap();
    let first_day = DateParam(8766); // 2024-01-01, days since PostgreSQL's 2000-01-01 epoch.
    let second_day = DateParam(8767);
    assert_eq!(upsert.params(), &[Type::BPCHAR, Type::DATE, Type::INT4]);
    for (day, value) in [(&first_day, 2i32), (&second_day, 3), (&first_day, 5)] {
        let row = client
            .query_one(&upsert, &[&"US ", day, &value])
            .await
            .unwrap();
        assert_eq!(row.get::<_, i32>(0), value);
    }
    let lookup = client
        .prepare("SELECT count FROM daily_facts WHERE country = $1 AND day = $2")
        .await
        .unwrap();
    assert_eq!(
        client
            .query_one(&lookup, &[&"US ", &first_day])
            .await
            .unwrap()
            .get::<_, i32>(0),
        5
    );
    let range = client
        .prepare("SELECT count FROM daily_facts WHERE country = $1 AND day >= $2 ORDER BY day DESC")
        .await
        .unwrap();
    let rows = client.query(&range, &[&"US", &first_day]).await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, i32>(0))
            .collect::<Vec<_>>(),
        [3, 5]
    );

    let instant = UNIX_EPOCH + Duration::from_secs(1_704_110_400);
    let event = client
        .prepare("INSERT INTO events VALUES ($1, $2)")
        .await
        .unwrap();
    assert_eq!(event.params(), &[Type::TIMESTAMPTZ, Type::TEXT]);
    client
        .execute(&event, &[&instant, &"launch"])
        .await
        .unwrap();
    let event_lookup = client
        .prepare("SELECT title FROM events WHERE at = $1")
        .await
        .unwrap();
    assert_eq!(
        client
            .query_one(&event_lookup, &[&instant])
            .await
            .unwrap()
            .get::<_, &str>(0),
        "launch"
    );
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn legacy_extended_key_types() {
    exercise(Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn v2_extended_key_types() {
    exercise(Engine::V2).await;
}
