//! Opt-in end-to-end gateway session worked example.
use super::common::{ServeHarness, ServeResult, file_sink_policy};
use nunya::state::model::Mode;
struct Abort<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Abort<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
#[tokio::test]
#[ignore = "requires NUNYA_TEST_SPANNER_PGADAPTER pointing at harness database"]
async fn session_worked_example() -> ServeResult<()> {
    let endpoint = std::env::var("NUNYA_TEST_SPANNER_PGADAPTER")?;
    let url = format!("postgresql://nunya:emulator-only@{endpoint}/pgdb?sslmode=disable");
    let mut harness = ServeHarness::new(
        &file_sink_policy(),
        Some(&url),
        Some("spanner-pg"),
        None,
        "",
    )?;
    harness.start().await?;
    let (session, secret, principal_secret) = harness
        .acquire_session("managed-agent", "postgres", Mode::ReadWrite)
        .await?;
    // Neither the management principal nor an old standing-style login is a DB identity.
    for (name, password) in [
        ("managed-agent", principal_secret.secret.as_bytes()),
        ("agent", b"standing-test".as_slice()),
    ] {
        let mut rejected = tokio_postgres::Config::new();
        rejected
            .host("127.0.0.1")
            .port(harness.pg_port())
            .dbname("pgdb")
            .user(name)
            .password(password);
        assert!(
            rejected.connect(tokio_postgres::NoTls).await.is_err(),
            "non-session database login must be refused"
        );
    }
    let (client, driver) = harness.pg_connect(&session, &secret, "pgdb").await?;
    let _driver = Abort(driver);
    let rows = client
        .simple_query("SELECT email, phone FROM customers WHERE id = 1")
        .await?;
    let row = rows
        .iter()
        .find_map(|r| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = r {
                Some(r)
            } else {
                None
            }
        })
        .ok_or("protected read returned no row")?;
    let token = row.get(0).ok_or("protected read returned no email")?;
    assert!(row.get(1) == Some("[hidden]"), "phone is masked");
    assert!(
        token != "alice@example.test" && token.starts_with("tok_"),
        "email is tokenized"
    );
    let marker = format!(
        "serve-worked-example-{}",
        harness.dir().file_name().unwrap().to_string_lossy()
    );
    let insert = format!(
        "INSERT INTO outbox(to_address, template) VALUES ('{}', '{marker}')",
        token.replace('\'', "''")
    );
    assert!(
        client.simple_query(&insert).await.is_ok(),
        "token transfers across email domain"
    );
    let refusal = client
        .simple_query("SELECT lower(email) FROM customers")
        .await;
    assert!(refusal.is_err(), "blind expression is refused");
    let (direct, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await?;
    let _direct_driver = Abort(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let stored = direct.query_one("SELECT to_address = 'alice@example.test' FROM outbox WHERE template = $1 ORDER BY id DESC LIMIT 1", &[&marker]).await;
    direct
        .execute("DELETE FROM outbox WHERE template = $1", &[&marker])
        .await?;
    assert!(
        stored?.try_get::<_, bool>(0)?,
        "upstream stores original value"
    );
    Ok(())
}
