//! Opt-in grouping and ordering checks through the gateway against PostgreSQL.
use super::common::{ServeHarness, ServeResult};
use nunya::{state::model::Mode, token::SessionId};
use tokio_postgres::{NoTls, SimpleQueryMessage};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires NUNYA_TEST_JSON_PG and NUNYA_TEST_JSON_PG_ADMIN"]
async fn postgres_groups_and_orders_only_declared_show_json_leaves() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_JSON_PG")?;
    let admin_url = std::env::var("NUNYA_TEST_JSON_PG_ADMIN")?;
    let schema = format!("json_group_{}", &SessionId::random().to_hex()[..12]);
    let (admin, connection) = tokio_postgres::connect(&admin_url, NoTls).await?;
    let driver = tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let result = async {
        admin.batch_execute(&format!(r#"
            CREATE TABLE {schema}.customers(id int PRIMARY KEY, preferences jsonb);
            INSERT INTO {schema}.customers VALUES
                (1, '{{"locale":"en-US","blind":"private-blind","masked":"private-masked"}}'),
                (2, '{{"locale":"en-US","blind":"different-blind","masked":"different-masked"}}'),
                (3, '{{"locale":"fr-FR"}}'),
                (4, '{{"locale":42}}'),
                (5, '{{"locale":null}}'),
                (6, '{{}}');
            GRANT USAGE ON SCHEMA {schema} TO PUBLIC;
            GRANT SELECT ON {schema}.customers TO PUBLIC;
        "#)).await?;
        let policy = format!(r#"
version = 2
default = 'tokenize'
[domains.private]
tokenize = 'off'
[[json_path]]
table = '{schema}.customers'
name = 'preferences'
path = '$.locale'
show = true
type = 'string'
[[json_path]]
table = '{schema}.customers'
name = 'preferences'
path = '$.blind'
blind = 'private'
type = 'text'
[[json_path]]
table = '{schema}.customers'
name = 'preferences'
path = '$.masked'
masked = true
"#);
        let mut h = ServeHarness::new(&policy, Some(&url), None, None, "")?;
        let path = h.dir().join("nunya.toml");
        let mut config: toml::Table = toml::from_str(&std::fs::read_to_string(&path)?)?;
        config.insert("schemas".into(), vec![schema.clone()].into());
        config.insert("search_path".into(), vec![schema.clone()].into());
        config.insert(
            "readlog".into(),
            toml::Value::Table(toml::Table::from_iter([
                ("sink".into(), "file".into()),
                ("path".into(), "read.log".into()),
            ])),
        );
        std::fs::write(path, toml::to_string(&config)?)?;
        h.start().await?;
        let (access, secret, _) = h.acquire_session("json-groups", "postgres", Mode::ReadOnly).await?;
        let (client, client_driver) = h.pg_connect(&access, &secret, "app").await?;
        let queries = [
            ("SELECT preferences->>'locale', count(*) FROM customers GROUP BY preferences->>'locale' ORDER BY preferences->>'locale'", ["en-US", "fr-FR"]),
            ("SELECT (preferences->>'locale'), count(*) FROM customers GROUP BY 1 ORDER BY 1", ["en-US", "fr-FR"]),
            ("SELECT preferences->>'locale' AS locale, count(*) FROM customers GROUP BY preferences->>'locale' ORDER BY locale", ["en-US", "fr-FR"]),
            ("SELECT preferences->'locale', count(*) FROM customers GROUP BY preferences->'locale' ORDER BY preferences->'locale'", [r#""en-US""#, r#""fr-FR""#]),
        ];
        for (sql, [first, second]) in queries {
            let messages = client.simple_query(sql).await?;
            let rows: Vec<_> = messages.iter().filter_map(|message| match message {
                SimpleQueryMessage::Row(row) => Some((row.get(0), row.get(1))),
                _ => None,
            }).collect();
            if rows != [(Some(first), Some("2")), (Some(second), Some("1")), (None, Some("3"))] {
                return Err("unexpected groups or counts".into());
            }
        }

        for (path, reason) in [("blind", "blind"), ("masked", "masked"), ("unlisted", "unlisted")] {
            let extract = format!("preferences->>'{path}'");
            let cases = [
                format!("SELECT {extract}, count(*) FROM customers GROUP BY {extract} ORDER BY {extract}"),
                format!("SELECT {extract}, count(*) FROM customers GROUP BY 1 ORDER BY 1"),
                format!("SELECT {extract} AS locale FROM customers ORDER BY locale"),
                format!("SELECT DISTINCT {extract} FROM customers"),
            ];
            for sql in cases {
                let Err(error) = client.simple_query(&sql).await else {
                    return Err("protected grouping was accepted".into());
                };
                let Some(db) = error.as_db_error() else { return Err(error.into()) };
                if db.code().code() != "NY005" || !db.message().ends_with(reason) {
                    return Err(format!("unexpected JSON grouping refusal: {db}").into());
                }
            }
        }
        let Err(error) = client.prepare("SELECT preferences->>'blind' FROM customers ORDER BY 1").await else {
            return Err("extended protected ordering was accepted".into());
        };
        if !error.as_db_error().is_some_and(|db| db.code().code() == "NY005" && db.message().ends_with("blind")) {
            return Err("unexpected extended JSON ordering refusal".into());
        }
        client.simple_query("SELECT 1").await?;
        drop(client);
        client_driver.abort();
        Ok(())
    }.await;
    let cleanup = admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    driver.abort();
    result.and(cleanup.map_err(Into::into))
}
