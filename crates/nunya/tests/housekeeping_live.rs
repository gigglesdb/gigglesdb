//! Opt-in PostgreSQL prepared-statement housekeeping through the gateway.
use super::common::{ServeHarness, ServeResult};
use nunya::{state::model::Mode, token::SessionId};
use std::time::Duration;
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires NUNYA_TEST_PG and NUNYA_TEST_PG_ADMIN"]
async fn postgres_prepared_statement_housekeeping_after_rollback() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_PG")?;
    let admin_url = std::env::var("NUNYA_TEST_PG_ADMIN")?;
    let schema = format!("housekeeping_{}", &SessionId::random().to_hex()[..12]);
    let (admin, connection) = tokio_postgres::connect(&admin_url, NoTls).await?;
    let admin_driver = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let result = async {
        admin
            .batch_execute(&format!(
                "CREATE TABLE {schema}.items(id int PRIMARY KEY);
             INSERT INTO {schema}.items VALUES (1), (2);
             GRANT USAGE ON SCHEMA {schema} TO PUBLIC;
             GRANT SELECT ON {schema}.items TO PUBLIC;"
            ))
            .await?;
        let policy = format!(
            r#"
version = 2
default = 'tokenize'
[[column]]
table = '{schema}.items'
name = 'id'
show = true
"#
        );
        let mut harness = ServeHarness::new(&policy, Some(&url), None, None, "")?;
        let config_path = harness.dir().join("nunya.toml");
        let mut config: toml::Table = toml::from_str(&std::fs::read_to_string(&config_path)?)?;
        config.insert("schemas".into(), vec![schema.clone()].into());
        config.insert("search_path".into(), vec![schema.clone()].into());
        std::fs::write(config_path, toml::to_string(&config)?)?;
        harness.start().await?;
        let (access, secret, _) = harness
            .acquire_session("housekeeping", "postgres", Mode::ReadOnly)
            .await?;
        let (mut client, driver) = harness.pg_connect(&access, &secret, "app").await?;
        let outcome = tokio::time::timeout(Duration::from_secs(15), async {
            let first = client.prepare("SELECT id FROM items WHERE id = 1").await?;
            let second = client.prepare("SELECT id FROM items WHERE id = 2").await?;
            let tx = client.transaction().await?;
            if tx.query_one(&first, &[]).await?.try_get::<_, i32>(0)? != 1
                || tx.query_one(&second, &[]).await?.try_get::<_, i32>(0)? != 2
            {
                return Err("unexpected rows from prepared statements before rollback".into());
            }
            tx.rollback().await?;
            client.batch_execute("DEALLOCATE ALL").await?;
            let again = client.prepare("SELECT id FROM items WHERE id = 1").await?;
            if client.query_one(&again, &[]).await?.try_get::<_, i32>(0)? != 1 {
                return Err("unexpected row after DEALLOCATE ALL".into());
            }

            client.batch_execute("DISCARD ALL").await?;
            client.batch_execute("RESET ALL").await?;
            let after_reset = client.prepare("SELECT id FROM items WHERE id = 2").await?;
            if client
                .query_one(&after_reset, &[])
                .await?
                .try_get::<_, i32>(0)?
                != 2
            {
                return Err("unexpected row after DISCARD ALL and RESET ALL".into());
            }
            Ok(())
        })
        .await;
        drop(client);
        driver.abort();
        outcome?
    }
    .await;
    let cleanup = admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    admin_driver.abort();
    result.and(cleanup.map_err(Into::into))
}
