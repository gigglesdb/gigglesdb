use serde_json::Value;
use skedaddle::{
    definition::{SecretRef, SpannerDialect, SpannerEndpoint},
    spanner::{
        client::Client,
        frontier::{DuplicateFilter, Frontier, SpannerPosition},
        stream::{Config, Event, PartitionReader, timestamp},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    sync::Arc,
    time::Duration,
};

#[tokio::test]
async fn both_dialects_reader() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Ok(path) = std::env::var("SKEDADDLE_SPANNER_CONFIG") else {
        writeln!(
            std::io::stderr(),
            "SPANNER EMULATOR SKIPPED: SKEDADDLE_SPANNER_CONFIG is unset; use harness/run.ps1 -Phase spanner-emulator -SpannerCargoTest '-p skedaddle --test spanner_reader -- --nocapture'"
        )?;
        return Ok(());
    };
    let config: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    for (name, dialect) in [
        ("GOOGLE_STANDARD_SQL", SpannerDialect::GoogleSql),
        ("POSTGRESQL", SpannerDialect::Postgresql),
    ] {
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
        let client = Arc::new(Client::connect(&endpoint, config["grpc_endpoint"].as_str()).await?);
        let snapshot = timestamp(
            client
                .write(
                    &["INSERT INTO parents (parent_id,payload) VALUES (900,'snapshot')".into()],
                    "s3b-snapshot",
                )
                .await?,
        );
        let make = |frontier, identities| {
            PartitionReader::new(
                client.clone(),
                snapshot,
                frontier,
                DuplicateFilter::restore(10000, 1024, identities).unwrap(),
                Config {
                    concurrency: 2,
                    channel_capacity: 1,
                    query_window: Duration::from_secs(1),
                    ..Config::default()
                },
            )
        };
        let mut reader = make(Frontier::new(snapshot), Vec::new())?;
        let writer_client = client.clone();
        let mut writer = Box::pin(async move {
            let mut commits = Vec::new();
            for i in 1000..1080 {
                commits.push(timestamp(writer_client.write(&[format!("INSERT INTO parents (parent_id,payload) VALUES ({i},'reader')"),
                    format!("INSERT INTO children (parent_id,child_id,payload) VALUES ({i},1,'reader')")], "s3b-write").await?));
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok::<_, skedaddle::spanner::client::Error>(commits)
        });
        let (mut seen, mut groups, mut identities) =
            (BTreeSet::new(), BTreeMap::<_, usize>::new(), Vec::new());
        let (mut restarted, mut drops, mut heartbeats, mut ticks) = (false, 0, 0, 0);
        let mut timer = tokio::time::interval(Duration::from_millis(1));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        let mut commits: Option<Vec<i128>> = None;
        loop {
            if tokio::time::Instant::now() >= deadline {
                assert_eq!(seen.len(), 160, "{name}: reader timed out");
                assert_eq!(groups.len(), 80);
                assert!(groups.values().all(|n| *n == 2));
                let commits = commits.as_ref().expect("writer did not finish");
                assert!(commits.iter().all(|ts| groups.contains_key(ts)));
                assert!(reader.frontier().resolved_ts()?.unwrap() > *commits.last().unwrap());
                assert!(restarted && drops > 0 && heartbeats > 0 && ticks > 0);
                let splits = reader
                    .frontier()
                    .records()
                    .values()
                    .filter(|p| {
                        !p.token.is_empty()
                            && reader
                                .frontier()
                                .records()
                                .values()
                                .filter(|c| c.parents.contains(&p.token))
                                .count()
                                > 1
                    })
                    .count();
                assert!(splits > 0, "no split observed");
                println!(
                    "{name}: 160 records, {splits} split parents; NO MERGE OBSERVED within 5 minutes; restart/inclusive start/resolved heartbeat checks passed"
                );
                break;
            }
            tokio::select! {
                biased;
                _ = timer.tick() => { ticks += 1; continue; }
                result = &mut writer, if commits.is_none() => { commits = Some(result?); }
                result = reader.next() => match result? {
                    Event::Data(record,_) => {
                        assert!(timestamp(record.commit_timestamp) > snapshot);
                        assert!(record.mods.iter().all(|m| m.keys["parent_id"].as_str().unwrap().parse::<u64>().unwrap() >= 1000), "inclusive start leaked snapshot");
                        let identity = (record.server_transaction_id.as_bytes().to_vec(),record.record_sequence.as_bytes().to_vec());
                        assert!(seen.insert(identity.clone()), "duplicate handed downstream");
                        identities.push(identity);
                        *groups.entry(timestamp(record.commit_timestamp)).or_default() += 1;
                    }
                    Event::Heartbeat(..) => heartbeats += 1,
                    Event::Children(..) | Event::PartitionEnd(_) => {}
                }
            }
            if !restarted && seen.len() == 7 {
                let bytes = SpannerPosition::Frontier(reader.frontier().clone()).encode();
                let SpannerPosition::Frontier(frontier) = SpannerPosition::decode(&bytes)? else {
                    panic!()
                };
                drop(reader);
                reader = make(frontier, identities.clone())?;
                restarted = true;
            }
            let frontier = reader.frontier();
            let splits = frontier
                .records()
                .values()
                .filter(|p| {
                    !p.token.is_empty()
                        && frontier
                            .records()
                            .values()
                            .filter(|c| c.parents.contains(&p.token))
                            .count()
                            > 1
                })
                .count();
            let merges = frontier
                .records()
                .values()
                .filter(|p| p.parents.len() > 1)
                .count();
            drops = drops.max(reader.duplicate_drops());
            if let Some(commits) = &commits
                && seen.len() == 160
                && frontier.resolved_ts()?.unwrap() > *commits.last().unwrap()
                && splits > 0
                && merges > 0
            {
                assert_eq!(groups.len(), 80);
                assert!(groups.values().all(|n| *n == 2));
                assert!(commits.iter().all(|ts| groups.contains_key(ts)));
                assert!(restarted && drops > 0 && heartbeats > 0 && ticks > 0);
                println!(
                    "{name}: {} records, {splits} split parents, {merges} merge children, {heartbeats} heartbeats, {drops} duplicate drops, {ticks} cancellation ticks",
                    seen.len()
                );
                break;
            }
        }
    }
    Ok(())
}
