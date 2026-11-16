//! Per-test databases on the harness's shared, credential-free emulator instance.
use reqwest::{Client, Method};
use serde_json::{Value, json};
use skedaddle::spanner::client::Error;
use std::time::Duration;

pub struct Database {
    config: Value,
    url: String,
}

impl Database {
    pub async fn create(mut config: Value, dialect: &str) -> Result<Self, Error> {
        let endpoint = config["endpoint"].as_str().ok_or("missing REST endpoint")?;
        let parsed = reqwest::Url::parse(endpoint)?;
        if parsed.scheme() != "http" || parsed.host_str() != Some("127.0.0.1") {
            return Err("test databases require the loopback emulator".into());
        }
        let instance = format!(
            "projects/{}/instances/{}",
            config["project"].as_str().ok_or("missing project")?,
            config["instance"].as_str().ok_or("missing instance")?
        );
        // Spanner database IDs are at most 30 characters.
        let id = format!("t1-{}", &uuid::Uuid::new_v4().simple().to_string()[..26]);
        let database = format!("{instance}/databases/{id}");
        let url = format!("{endpoint}/v1/{database}");
        let parent = format!("{endpoint}/v1/{instance}/databases");
        let google = match dialect {
            "GOOGLE_STANDARD_SQL" => true,
            "POSTGRESQL" => false,
            _ => return Err("unknown test database dialect".into()),
        };
        let statement = if google {
            format!("CREATE DATABASE `{id}`")
        } else {
            format!("CREATE DATABASE \"{id}\"")
        };
        config["databases"][dialect]["database"] = json!(database);
        // Arm cleanup before provisioning so failed DDL and panics also drop it.
        let isolated = Self { config, url };
        let client = Client::builder().timeout(Duration::from_secs(15)).build()?;
        isolated
            .operation(
                &client,
                Method::POST,
                &parent,
                json!({"createStatement": statement, "databaseDialect": dialect}),
            )
            .await?;
        // Keep this schema identical to harness/spanner-setup.py::schema.
        let statements = if google {
            vec![
                "CREATE TABLE parents (parent_id INT64 NOT NULL, payload STRING(MAX), changed_at TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY(parent_id)",
                "CREATE TABLE children (parent_id INT64 NOT NULL, child_id INT64 NOT NULL, payload STRING(MAX)) PRIMARY KEY(parent_id, child_id), INTERLEAVE IN PARENT parents ON DELETE CASCADE",
                "CREATE CHANGE STREAM changes FOR ALL OPTIONS (value_capture_type='NEW_ROW_AND_OLD_VALUES', retention_period='1d')",
            ]
        } else {
            vec![
                "CREATE TABLE parents (parent_id bigint NOT NULL PRIMARY KEY, payload text, changed_at spanner.commit_timestamp)",
                "CREATE TABLE children (parent_id bigint NOT NULL, child_id bigint NOT NULL, payload text, PRIMARY KEY(parent_id, child_id)) INTERLEAVE IN PARENT parents ON DELETE CASCADE",
                "CREATE CHANGE STREAM changes FOR ALL WITH (value_capture_type='NEW_ROW_AND_OLD_VALUES', retention_period='1d')",
            ]
        };
        isolated
            .operation(
                &client,
                Method::PATCH,
                &format!("{}/ddl", isolated.url),
                json!({"statements": statements}),
            )
            .await?;
        Ok(isolated)
    }

    pub fn config(&self) -> &Value {
        &self.config
    }

    async fn operation(
        &self,
        client: &Client,
        method: Method,
        url: &str,
        body: Value,
    ) -> Result<(), Error> {
        let mut operation: Value = client
            .request(method, url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let endpoint = self.config["endpoint"].as_str().unwrap();
        tokio::time::timeout(Duration::from_secs(90), async {
            while operation["done"] != true {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let name = operation["name"].as_str().ok_or("missing operation name")?;
                operation = client
                    .get(format!("{endpoint}/v1/{name}"))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
            }
            if let Some(error) = operation.get("error") {
                return Err(format!("test database operation failed: {error}").into());
            }
            Ok::<(), Error>(())
        })
        .await??;
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let url = self.url.clone();
        // A separate runtime makes best-effort cleanup complete even while a
        // current-thread tokio test unwinds or returns an error.
        let cleanup = std::thread::spawn(move || -> Result<(), Error> {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    Client::builder()
                        .timeout(Duration::from_secs(5))
                        .build()?
                        .delete(url)
                        .send()
                        .await?
                        .error_for_status()?;
                    Ok(())
                })
        });
        if !matches!(cleanup.join(), Ok(Ok(()))) {
            eprintln!("SPANNER TEST DATABASE CLEANUP FAILED: {}", self.url);
        }
    }
}
