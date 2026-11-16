use mysql_async::prelude::Queryable;
use skedaddle::{
    capability::{CapabilityItem, Criticality, ItemStatus},
    definition::{ObjectKind, ObjectSelector},
    mysql::{
        client::MysqlConfig,
        preflight::{Options, classify, collect},
    },
};

// A separate runtime lets Drop restore globals even while the test runtime unwinds.
struct Restore {
    config: MysqlConfig,
    sql: Vec<String>,
}
impl Drop for Restore {
    fn drop(&mut self) {
        let config = self.config.clone();
        let sql = std::mem::take(&mut self.sql);
        let restored = std::thread::spawn(move || -> bool {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                let Ok(mut client) = config.connect().await else {
                    eprintln!("MYSQL RESTORE CONNECT FAILED (details redacted)");
                    return false;
                };
                for statement in sql {
                    if let Err(error) = client.conn.query_drop(&statement).await {
                        match error {
                            mysql_async::Error::Server(e) => eprintln!("MYSQL RESTORE QUERY FAILED: code={}, state={}, SQL={statement}", e.code, e.state),
                            _ => eprintln!("MYSQL RESTORE QUERY FAILED: transport error (details redacted), SQL={statement}"),
                        }
                        return false;
                    }
                }
                let result = client.conn.disconnect().await.is_ok();
                if !result {
                    eprintln!("MYSQL RESTORE DISCONNECT FAILED (details redacted)");
                }
                result
            })
        })
        .join()
        .unwrap_or(false);
        if !restored {
            eprintln!("MYSQL RESTORE FAILED");
            assert!(std::thread::panicking(), "restore settings/tables failed");
        }
    }
}
fn table(name: &str) -> ObjectSelector {
    ObjectSelector {
        schema: "type_zoo".into(),
        name: name.into(),
        kind: ObjectKind::Table,
        exclude_columns: vec![],
    }
}
fn blocking(items: &[CapabilityItem]) -> Vec<&str> {
    items
        .iter()
        .filter(|i| {
            i.criticality == Criticality::Critical
                && !matches!(
                    i.status,
                    ItemStatus::Supported | ItemStatus::SupportedWithTransformation
                )
        })
        .map(|i| i.id.as_str())
        .collect()
}
#[tokio::test]
async fn live_preflight_settings_and_tables() -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; use harness mysql80/mysql84 cargo-test lane"
        );
        return Ok(());
    };
    let config: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut client = config.connect().await?;
    let options = Options {
        estimated_copy_seconds: 10,
        planned_catchup_seconds: 20,
        max_decompressed_bytes: config.max_decompressed_bytes,
        selected_tables: vec![table("zoo")],
    };
    assert!(blocking(&classify(&collect(&mut client, &options).await, &options)).is_empty());
    for (variable, setting, check) in [
        ("binlog_row_image", "'MINIMAL'", "mysql_binlog_row_image"),
        (
            "binlog_row_value_options",
            "'PARTIAL_JSON'",
            "mysql_binlog_row_value_options",
        ),
        (
            "binlog_transaction_compression",
            "ON",
            "mysql_binlog_compression",
        ),
    ] {
        let mut restore = Restore {
            config: config.clone(),
            sql: Vec::new(),
        };
        for name in if variable == "binlog_transaction_compression" {
            vec![variable, "max_binlog_cache_size"]
        } else {
            vec![variable]
        } {
            let original: String = client
                .conn
                .query_first(format!("SELECT CAST(@@global.{name} AS CHAR)"))
                .await?
                .unwrap();
            let original = match name {
                "max_binlog_cache_size" => original.parse::<u64>()?.to_string(),
                "binlog_transaction_compression" => match original.as_str() {
                    "0" | "OFF" => "OFF".into(),
                    "1" | "ON" => "ON".into(),
                    _ => return Err("unknown compression setting".into()),
                },
                _ => mysql_async::Value::Bytes(original.into_bytes()).as_sql(false),
            };
            restore.sql.push(format!("SET GLOBAL {name}={original}"));
        }
        if variable == "binlog_transaction_compression" {
            client
                .conn
                .query_drop(format!(
                    "SET GLOBAL max_binlog_cache_size={}",
                    config.max_decompressed_bytes * 2
                ))
                .await?;
        }
        client
            .conn
            .query_drop(format!("SET GLOBAL {variable}={setting}"))
            .await?;
        assert_eq!(
            blocking(&classify(&collect(&mut client, &options).await, &options)),
            [check]
        );
        drop(restore);
        assert!(blocking(&classify(&collect(&mut client, &options).await, &options)).is_empty());
    }
    for (suffix, ddl, check) in [
        (
            "myisam",
            "(id BIGINT PRIMARY KEY) ENGINE=MyISAM",
            "mysql_table_engine",
        ),
        ("no_pk", "(id BIGINT) ENGINE=InnoDB", "mysql_primary_key"),
    ] {
        let name = format!("m6_{}_{}", std::process::id(), suffix);
        let restore = Restore {
            config: config.clone(),
            sql: vec![format!("DROP TABLE IF EXISTS type_zoo.`{name}`")],
        };
        client
            .conn
            .query_drop(format!("CREATE TABLE type_zoo.`{name}` {ddl}"))
            .await?;
        let options = Options {
            selected_tables: vec![table(&name)],
            ..options.clone()
        };
        assert_eq!(
            blocking(&classify(&collect(&mut client, &options).await, &options)),
            [check]
        );
        drop(restore);
    }
    client.conn.disconnect().await?;
    Ok(())
}
