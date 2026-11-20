#[path = "support/root.rs"]
mod test_root;

#[path = "support/spanner.rs"]
mod spanner_support;

use google_cloud_spanner::value::Timestamp;
use serde_json::Value;
use skedaddle::{
    definition::{SecretRef, SpannerDialect, SpannerEndpoint},
    spanner::{
        client::Client,
        records::{ModType, Record, decode_row},
    },
};
use std::io::Write;
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
fn text(ts: Timestamp) -> String {
    serde_json::to_value(ts).unwrap().as_str().unwrap().into()
}

#[tokio::test]
async fn both_dialects_stream() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Ok(path) = std::env::var("SKEDADDLE_SPANNER_CONFIG") else {
        writeln!(
            std::io::stderr(),
            "SPANNER EMULATOR SKIPPED: SKEDADDLE_SPANNER_CONFIG is unset; run harness/run.ps1 -Phase spanner-emulator -SpannerCargoTest '-p skedaddle --test spanner_stream -- --nocapture'"
        )?;
        return Ok(());
    };
    let config: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    for (name, dialect) in [
        ("GOOGLE_STANDARD_SQL", SpannerDialect::GoogleSql),
        ("POSTGRESQL", SpannerDialect::Postgresql),
    ] {
        let isolated = spanner_support::Database::create(config.clone(), name).await?;
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
        let start = client
            .write(
                &[
                    "INSERT INTO parents (parent_id,payload) VALUES (101,'before'),(102,'second')"
                        .into(),
                    "INSERT INTO children (parent_id,child_id,payload) VALUES (101,7,'child')"
                        .into(),
                ],
                "s3a-insert",
            )
            .await?;
        client
            .write(
                &["UPDATE parents SET payload='after' WHERE parent_id=101".into()],
                "s3a-update",
            )
            .await?;
        client
            .write(
                &["DELETE FROM parents WHERE parent_id=102".into()],
                "s3a-delete",
            )
            .await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)? + Duration::from_secs(1);
        let end = Timestamp::new(now.as_secs() as i64, now.subsec_nanos() as i32)?;
        let mut queue = VecDeque::from([(None, start)]);
        let mut seen = HashSet::new();
        let (mut raw, mut data, mut heartbeats, mut children) = (Vec::new(), Vec::new(), 0, 0);
        while let Some((token, from)) = queue.pop_front() {
            assert!(seen.len() < 32, "unbounded emulator partition fanout");
            let rows = client
                .change_stream(token.as_deref(), &text(from), &text(end), 100)
                .await?;
            for row in &rows {
                for record in decode_row(row)? {
                    match record {
                        Record::Data(r) => data.push(r),
                        Record::Heartbeat(_) => heartbeats += 1,
                        Record::Children(r) => {
                            children += 1;
                            let from = if r.start_timestamp < start {
                                start
                            } else {
                                r.start_timestamp
                            };
                            for child in r.child_partitions {
                                if from <= end && seen.insert(child.token.clone()) {
                                    queue.push_back((Some(child.token), from));
                                }
                            }
                        }
                    }
                }
            }
            raw.extend(rows);
        }
        if std::env::var_os("SKEDADDLE_TEST_ROOT").is_some() {
            let root = crate::test_root::scratch_root().display().to_string();
            std::fs::create_dir_all(&root)?;
            std::fs::write(
                format!("{root}/spanner-{name}.json"),
                serde_json::to_vec_pretty(&raw)?,
            )?;
        }
        assert!(
            children > 0 && heartbeats > 0,
            "{name}: missing lineage or heartbeats"
        );
        assert_eq!(data.len(), 4, "{name}: {data:?}");
        let mut groups = BTreeMap::<_, Vec<_>>::new();
        for record in &data {
            assert!(["parents", "children"].contains(&record.table_name.as_str()));
            assert!(!record.record_sequence.is_empty());
            assert_eq!(record.number_of_partitions_in_transaction, 1); // Emulator limitation.
            assert_eq!(record.transaction_tag, ""); // Emulator discards the supplied tag.
            assert!(!record.is_system_transaction);
            groups
                .entry(&record.server_transaction_id)
                .or_default()
                .push(record);
        }
        assert_eq!(groups.len(), 3);
        assert!(groups.values().any(|records| records.len() == 2));
        for (id, records) in groups {
            assert!(!id.is_empty());
            assert_eq!(
                records
                    .iter()
                    .filter(|r| r.is_last_record_in_transaction_in_partition)
                    .count(),
                1
            );
            assert_eq!(
                records
                    .iter()
                    .map(|r| &r.record_sequence)
                    .collect::<HashSet<_>>()
                    .len(),
                records.len()
            );
            for r in &records {
                assert_eq!(records.len() as u64, r.number_of_records_in_transaction);
                assert_eq!(r.commit_timestamp, records[0].commit_timestamp);
            }
        }
        let child = data.iter().find(|r| r.table_name == "children").unwrap();
        assert_eq!(child.mod_type, ModType::Insert);
        assert_eq!(child.mods[0].keys["child_id"], "7");
        assert_eq!(child.mods[0].new_values["payload"], "child");
        let insert = data
            .iter()
            .find(|r| r.mod_type == ModType::Insert && r.table_name == "parents")
            .unwrap();
        assert_eq!(insert.mods.len(), 2);
        assert_eq!(
            insert
                .mods
                .iter()
                .map(|m| (&m.keys["parent_id"], &m.new_values["payload"]))
                .collect::<Vec<_>>(),
            vec![
                (&serde_json::json!("101"), &serde_json::json!("before")),
                (&serde_json::json!("102"), &serde_json::json!("second"))
            ]
        );
        let update = &data
            .iter()
            .find(|r| r.mod_type == ModType::Update)
            .unwrap()
            .mods[0];
        assert_eq!(update.keys["parent_id"], "101");
        assert_eq!(update.new_values["payload"], "after");
        assert_eq!(update.old_values["payload"], "before");
        let delete = &data
            .iter()
            .find(|r| r.mod_type == ModType::Delete)
            .unwrap()
            .mods[0];
        assert_eq!(delete.keys["parent_id"], "102");
        assert_eq!(delete.old_values["payload"], "second");
        println!(
            "{name}: {} data records, {heartbeats} heartbeats, {children} lineage records",
            data.len()
        );
    }
    Ok(())
}
