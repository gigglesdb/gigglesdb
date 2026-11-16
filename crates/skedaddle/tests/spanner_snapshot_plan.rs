//! Real, two-dialect exact-T partition and serialized-token qualification.
#[path = "support/spanner.rs"]
mod spanner_support;

use serde_json::Value;
use skedaddle::{
    definition::{SecretRef, SpannerDialect, SpannerEndpoint},
    spanner::{
        client::{Client, Error},
        snapshot::{Plan, TableName},
    },
};
use std::collections::{BTreeMap, BTreeSet};

async fn rows(plan: &Plan, client: &Client, id: &str) -> Result<BTreeSet<(i64, String)>, Error> {
    let mut rows = plan.execute(client, id).await?;
    let mut result = BTreeSet::new();
    while let Some(row) = rows.next().await {
        let row = row?;
        assert!(
            result.insert((row.get(0)?, row.get(1)?)),
            "duplicate partition row"
        );
    }
    Ok(result)
}
async fn qualify(name: &str, dialect: SpannerDialect) -> Result<(), Error> {
    let Ok(path) = std::env::var("SKEDADDLE_SPANNER_CONFIG") else {
        eprintln!(
            "SPANNER EMULATOR SKIPPED ({name}): SKEDADDLE_SPANNER_CONFIG unset; run harness/run.ps1 -Phase spanner-emulator -SpannerCargoTest '-p skedaddle --test spanner_snapshot_plan -- --nocapture'"
        );
        return Ok(());
    };
    let config: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let isolated = spanner_support::Database::create(config, name).await?;
    let config = isolated.config();
    let endpoint = SpannerEndpoint {
        project: config["project"].as_str().unwrap().into(),
        instance: config["instance"].as_str().unwrap().into(),
        database: config["databases"][name]["database"]
            .as_str()
            .unwrap()
            .rsplit('/')
            .next()
            .unwrap()
            .into(),
        dialect,
        change_stream: config["stream"].as_str().unwrap().into(),
        credentials: SecretRef::parse("env:UNRESOLVED_SPANNER_TEST")?,
        emulator: true,
    };
    let client = Client::connect(&endpoint, config["grpc_endpoint"].as_str()).await?;
    client
        .write(
            &[
                "INSERT INTO parents (parent_id,payload) VALUES (1,'before-1'),(2,'before-2')"
                    .into(),
                "INSERT INTO children (parent_id,child_id,payload) VALUES (1,7,'before-child')"
                    .into(),
            ],
            "s6a-seed",
        )
        .await?;
    let schema = if dialect == SpannerDialect::GoogleSql {
        ""
    } else {
        "public"
    };
    let selected = ["parents", "children"].map(|name| TableName {
        schema: schema.into(),
        name: name.into(),
    });
    let plan = Plan::create(&client, &selected).await?;
    assert_eq!(plan.tables().len(), 2);
    assert!(!plan.partitions().is_empty());
    for table in plan.tables() {
        let keys = table
            .columns
            .iter()
            .filter_map(|c| c.key_ordinal.map(|i| (i, c.name.as_str())))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(keys[&1], "parent_id");
        assert!(table.columns.iter().all(|c| !c.spanner_type.is_empty()));
        if table.name.name == "children" {
            assert_eq!(keys[&2], "child_id");
        }
    }
    let saved = plan.encode()?;
    let after = client
        .write(
            &[
                "UPDATE parents SET payload='after' WHERE parent_id=1".into(),
                "DELETE FROM parents WHERE parent_id=2".into(),
                "INSERT INTO parents (parent_id,payload) VALUES (3,'after-3')".into(),
            ],
            "s6a-after-t",
        )
        .await?;
    assert!(after > plan.binding().timestamp());
    let mut expected_partition_rows = BTreeMap::new();
    let mut actual = BTreeMap::<_, BTreeSet<_>>::new();
    for p in plan.partitions() {
        let mut stream = plan.execute(&client, p.id()).await?;
        let mut partition_rows = BTreeSet::new();
        while let Some(row) = stream.next().await {
            let row = row?;
            let value: (i64, String) = if p.table().name == "parents" {
                (row.get(0)?, row.get(1)?)
            } else {
                assert_eq!(row.get::<i64>(1)?, 7);
                (row.get(0)?, row.get(2)?)
            };
            assert!(partition_rows.insert(value.clone()));
            assert!(
                actual
                    .entry(p.table().name.clone())
                    .or_default()
                    .insert(value)
            );
        }
        expected_partition_rows.insert(p.id().to_owned(), partition_rows);
    }
    assert_eq!(
        actual["parents"],
        BTreeSet::from([(1, "before-1".into()), (2, "before-2".into())])
    );
    assert_eq!(
        actual["children"],
        BTreeSet::from([(1, "before-child".into())])
    );
    let fresh = Client::connect(&endpoint, config["grpc_endpoint"].as_str()).await?;
    let restored = Plan::restore(&fresh, &saved, plan.binding()).await?;
    assert_eq!(restored.encode()?, saved);
    // Re-execute every parents partition using only restored SDK tokens and a fresh client.
    for p in restored
        .partitions()
        .iter()
        .filter(|p| p.table().name == "parents")
    {
        assert_eq!(
            rows(&restored, &fresh, p.id()).await?,
            expected_partition_rows[p.id()]
        );
    }
    println!(
        "{name}: exact-T insert/update/delete isolation, metadata, {} saved partitions and fresh-client replay passed",
        plan.partitions().len()
    );
    Ok(())
}
#[tokio::test]
async fn google_sql_snapshot_plan() -> Result<(), Error> {
    qualify("GOOGLE_STANDARD_SQL", SpannerDialect::GoogleSql).await
}
#[tokio::test]
async fn postgresql_snapshot_plan() -> Result<(), Error> {
    qualify("POSTGRESQL", SpannerDialect::Postgresql).await
}
