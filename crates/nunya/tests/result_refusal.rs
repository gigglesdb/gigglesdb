//! Opt-in binary extended-protocol result refusal against live PostgreSQL.
use super::common::{ServeHarness, ServeResult};
use futures::TryStreamExt;
use nunya::{state::model::Mode, token::SessionId};
use tokio_postgres::NoTls;

const PROTECTED: &str = "private-live-result-fixture";

#[tokio::test]
#[ignore = "requires NUNYA_TEST_PG and NUNYA_TEST_PG_ADMIN"]
async fn postgres_binary_result_refusal_preserves_connection() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_PG")?;
    let admin_url = std::env::var("NUNYA_TEST_PG_ADMIN")?;
    let schema = format!("result_refusal_{}", &SessionId::random().to_hex()[..12]);
    let (admin, connection) = tokio_postgres::connect(&admin_url, NoTls).await?;
    let admin_driver = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let result = async {
        // PostgreSQL accepts both documents, but the second exceeds the gateway's
        // nesting limit during binary JSONB row rewriting.
        let depth = nunya::json::JsonLimits::default().max_depth + 1;
        admin
            .batch_execute(&format!(
                r#"
            CREATE TABLE {schema}.items(id int PRIMARY KEY, payload jsonb);
            INSERT INTO {schema}.items VALUES
                (1, '{{"secret":"{PROTECTED}"}}'),
                (2, (repeat('[', {depth}) || '"{PROTECTED}"' || repeat(']', {depth}))::jsonb);
            GRANT USAGE ON SCHEMA {schema} TO PUBLIC;
            GRANT SELECT ON {schema}.items TO PUBLIC;
        "#
            ))
            .await?;
        let policy = format!(
            r#"
version = 2
default = 'tokenize'
[domains.private]
tokenize = 'off'
[[column]]
table = '{schema}.items'
name = 'id'
show = true
[[json_path]]
table = '{schema}.items'
name = 'payload'
path = '$.secret'
blind = 'private'
type = 'text'
"#
        );
        let mut harness = ServeHarness::new(&policy, Some(&url), None, None, "")?;
        let config_path = harness.dir().join("nunya.toml");
        let mut config: toml::Table = toml::from_str(&std::fs::read_to_string(&config_path)?)?;
        config.insert("schemas".into(), vec![schema.clone()].into());
        config.insert("search_path".into(), vec![schema.clone()].into());
        config.insert(
            "readlog".into(),
            toml::Value::Table(toml::Table::from_iter([
                ("sink".into(), "file".into()),
                ("path".into(), "read.log".into()),
            ])),
        );
        std::fs::write(config_path, toml::to_string(&config)?)?;
        harness.start().await?;
        let (access, secret, _) = harness
            .acquire_session("result-refusal", "postgres", Mode::ReadOnly)
            .await?;
        let (client, driver) = harness.pg_connect(&access, &secret, "app").await?;
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let statement = client
                .prepare("SELECT id, payload FROM items WHERE id <= $1 ORDER BY id")
                .await?;
            let stream = client.query_raw(&statement, [&2_i32]).await?;
            tokio::pin!(stream);
            let first = stream
                .try_next()
                .await?
                .ok_or("expected a row before the refusal")?;
            if first.try_get::<_, i32>(0)? != 1 {
                return Err("unexpected row before the refusal".into());
            }
            let error = stream
                .try_next()
                .await
                .err()
                .ok_or("expected a result-time refusal")?;
            let db = error
                .as_db_error()
                .ok_or("expected a PostgreSQL ErrorResponse")?;
            if db.code().code() != "NY002"
                || db.message() != nunya::error::NyCode::BlindOperation.message()
                || format!("{db:?}").contains(PROTECTED)
            {
                return Err("unexpected result-time ErrorResponse".into());
            }
            // Reuse both the same connection and the same prepared statement.
            if client
                .query_one(&statement, &[&1_i32])
                .await?
                .try_get::<_, i32>(0)?
                != 1
            {
                return Err("unexpected result on the recovered connection".into());
            }
            let audit = std::fs::read_to_string(harness.dir().join("read.log"))?;
            let records: Vec<serde_json::Value> = audit
                .lines()
                .map(serde_json::from_str)
                .collect::<Result<_, _>>()?;
            let refused: Vec<_> = records.iter().filter(|r| r["outcome"] == "NY002").collect();
            if refused.len() != 1
                || refused[0]["rows"] != 1
                || !refused[0]["tokens"]
                    .as_array()
                    .is_some_and(|tokens| tokens.len() == 1)
                || audit.contains(PROTECTED)
            {
                return Err("unexpected read log for the refused result".into());
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
