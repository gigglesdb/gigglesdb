//! Real emulator -> Capture -> local manifests -> journal -> inclusive restart.
#[path = "support/root.rs"]
mod test_root;

#[path = "support/spanner.rs"]
mod spanner_support;

use serde_json::Value;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::{BufferWriter, WriterCheckpoint, WriterConfig},
    },
    coordinator::{
        Materialized, Transition,
        journal::{Entry, Journal, local_store},
    },
    definition::{SecretRef, SpannerDialect, SpannerEndpoint},
    engine::{Capture, decode_buffered_position},
    event::{AssembledTxn, Event, EventKind, Position, PositionOrder, Redelivery, TxnId},
    spanner::{
        capture::{SpannerCapture, buffered_identities},
        client::{Client, Error},
        frontier::{DuplicateFilter, Frontier, SpannerOrder, SpannerPosition},
        stream::{Config, PartitionReader, timestamp},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::Path,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

async fn writer(root: &Path, order: Arc<SpannerOrder>) -> Result<BufferWriter, Error> {
    for sub in ["buffer", "scratch"] {
        std::fs::create_dir_all(root.join(sub))?;
    }
    let (tx, _rx) = mpsc::channel();
    Ok(BufferWriter::open_from_checkpoint(
        Arc::new(ObjectBufferStore::local(
            root.join("buffer"),
            Duration::from_secs(3),
        )?),
        WriterConfig {
            prefix: "s5c".into(),
            migration_id: "capture".into(),
            stream: "changes".into(),
            epoch: 1,
            segment_bytes: 65536,
            segment_transactions: 1,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 1048576,
        },
        WriterCheckpoint::default(),
        order,
        tx,
        root.join("scratch"),
    )
    .await?)
}
async fn buffered(
    root: &Path,
    order: Arc<SpannerOrder>,
    through: u64,
) -> Result<Vec<AssembledTxn>, Error> {
    let mut reader = BufferReader::new(
        Arc::new(ObjectBufferStore::local(
            root.join("buffer"),
            Duration::from_secs(3),
        )?),
        ReaderConfig {
            prefix: "s5c".into(),
            migration_id: "capture".into(),
            stream: "changes".into(),
            limits: Default::default(),
            required_next_mseq: through,
        },
        ReaderCheckpoint::default(),
        order,
    )?;
    let mut out = Vec::new();
    while let Some(txn) = reader.next_txn().await? {
        out.push(txn);
    }
    assert_eq!(reader.checkpoint().next_mseq, through);
    Ok(out)
}
async fn put(
    journal: &Journal,
    state: &mut Materialized,
    transition: Transition,
) -> Result<(), Error> {
    let entry = Entry::new(state, transition, "s5c")?;
    journal.put(&entry).await?;
    state.apply(&entry)?;
    Ok(())
}
fn split_count(frontier: &Frontier) -> usize {
    frontier
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
        .count()
}
async fn qualify(name: &str, dialect: SpannerDialect) -> Result<(), Error> {
    let Ok(path) = std::env::var("SKEDADDLE_SPANNER_CONFIG") else {
        writeln!(
            std::io::stderr(),
            "SPANNER EMULATOR SKIPPED ({name}): SKEDADDLE_SPANNER_CONFIG is unset; run harness/run.ps1 -Phase spanner-emulator -SpannerCargoTest '--test spanner_capture'"
        )?;
        return Ok(());
    };
    assert_eq!(
        std::env::var("SKEDADDLE_SPANNER_SHORT_PARTITIONS").as_deref(),
        Ok("1"),
        "capture qualification requires short partitions"
    );
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
    let client = Arc::new(Client::connect(&endpoint, config["grpc_endpoint"].as_str()).await?);
    let snapshot = timestamp(
        client
            .write(
                &["INSERT INTO parents (parent_id,payload) VALUES (900,'snapshot')".into()],
                "s5c-snapshot",
            )
            .await?,
    );
    let order = Arc::new(SpannerOrder::new(
        &endpoint.project,
        &endpoint.instance,
        &endpoint.database,
        &endpoint.change_stream,
        snapshot,
    )?);
    let make_reader = |frontier, identities| {
        PartitionReader::new(
            client.clone(),
            snapshot,
            frontier,
            DuplicateFilter::restore(10000, 1024, identities).unwrap(),
            Config {
                concurrency: 8,
                channel_capacity: 1,
                query_window: Duration::from_secs(1),
                ..Default::default()
            },
        )
    };
    let root =
        crate::test_root::scratch_root().join(format!("s5c-{name}-{}", uuid::Uuid::new_v4()));
    crate::test_root::assert_under_work_root(&root);
    std::fs::create_dir_all(root.join("journal"))?;
    let journal = Journal::new(
        Arc::new(local_store(root.join("journal"))?),
        "s5c",
        "capture",
        Duration::from_secs(3),
    )?;
    let mut state = Materialized::default();
    put(
        &journal,
        &mut state,
        Transition::AcquireOwnership { epoch: 1 },
    )
    .await?;
    let mut capture = SpannerCapture::new(
        &endpoint,
        snapshot,
        make_reader(Frontier::new(snapshot), Vec::new())?,
        writer(&root, order.clone()).await?,
        10000,
    )?;
    let source = capture.source().clone();
    let high = Position::new(
        order.domain(),
        SpannerPosition::Txn {
            commit_ts: snapshot,
            server_transaction_id: b"snapshot".to_vec(),
        }
        .encode(),
    );
    let id = TxnId(b"snapshot-chunk".to_vec());
    let chunk = AssembledTxn::new(
        source.clone(),
        id.clone(),
        high.clone(),
        None,
        vec![Event {
            source: source.clone(),
            txn: id,
            ordinal_in_txn: 0,
            position: high.clone(),
            schema_version: 0,
            kind: EventKind::ChunkRows,
            table: Some("parents".into()),
            key: BTreeMap::new(),
            before: None,
            after: Some(BTreeMap::new()),
            unchanged_cols: Default::default(),
        }],
    )?;
    let reference = capture.publish(high.clone(), chunk.clone()).await?;
    assert_eq!(
        capture.publish(high, chunk.clone()).await?,
        reference,
        "snapshot redelivery must reuse its durable manifest"
    );
    assert_eq!(capture.writer_checkpoint()?.unwrap().next_mseq, 1);
    let writer_client = client.clone();
    let mut writes = Box::pin(async move {
        let mut expected = BTreeMap::new();
        for n in 0..24 {
            let a = 1000 + 2 * n;
            let ts = timestamp(writer_client.write(&[
                format!("INSERT INTO parents (parent_id,payload) VALUES ({a},'capture-{n}'),({},'capture-{n}')", a+1),
                format!("INSERT INTO children (parent_id,child_id,payload) VALUES ({a},1,'capture-{n}')"),
            ], "s5c-write").await?);
            assert!(expected.insert(ts, n).is_none());
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        Ok::<_, Error>(expected)
    });
    let (mut expected, mut delivered) = (None, Vec::<AssembledTxn>::new());
    let (mut steps, mut restart_step, mut progress, mut cancellations) = (0, None, 0, 0);
    let mut last = Position::new(
        order.domain(),
        SpannerPosition::Frontier(Frontier::new(snapshot)).encode(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{name}: timed out: {} txns, {steps} steps, restart {restart_step:?}",
            delivered.len()
        );
        let batch = tokio::select! {
            result = &mut writes, if expected.is_none() => { expected = Some(result?); continue; }
            result = tokio::time::timeout(Duration::from_millis(25), capture.step()) => match result {
                Ok(result) => result?, Err(_) => { cancellations += 1; continue; }
            }
        };
        steps += 1;
        let durable = batch
            .durable_position
            .as_ref()
            .expect("every step is durable");
        assert!(
            !last.compare(durable, order.as_ref())?.is_gt(),
            "durable frontier regressed"
        );
        let SpannerPosition::Frontier(frontier) = SpannerPosition::decode(durable.bytes())? else {
            panic!("expected frontier")
        };
        let resolved = frontier.resolved_ts()?.unwrap();
        let checkpoint = capture.writer_checkpoint()?.unwrap();
        assert!(checkpoint.redelivery.iter().all(|r| r.manifest.is_some()));
        let marker = capture.buffered_marker(&batch)?;
        let binding: Value = serde_json::from_str(&marker)?;
        assert_eq!(binding["version"], 1);
        assert_eq!(binding["writer"]["version"], checkpoint.version);
        assert_eq!(binding["writer"]["next_mseq"], checkpoint.next_mseq);
        assert_eq!(
            binding["writer"]["prev_digest"],
            serde_json::json!(checkpoint.prev_digest)
        );
        assert_eq!(binding["writer"]["last_epoch"], checkpoint.last_epoch);
        assert_eq!(
            decode_buffered_position(
                binding["frontier"].as_str().unwrap(),
                &source,
                order.domain()
            )?,
            *durable
        );
        put(
            &journal,
            &mut state,
            Transition::Progress {
                stream: "buffered".into(),
                marker,
                position: binding["frontier"].as_str().unwrap().into(),
            },
        )
        .await?;
        if let Some(txn) = batch.transaction {
            assert!(
                txn.commit_position
                    .compare(durable, order.as_ref())?
                    .is_le(),
                "D-A1 commit exceeds durable frontier"
            );
            assert!(
                delivered.iter().all(|old| old.txn != txn.txn),
                "duplicate delivery"
            );
            if let Some(old) = delivered.last() {
                assert!(
                    old.commit_position
                        .compare(&txn.commit_position, order.as_ref())?
                        .is_lt()
                );
            }
            assert_eq!(
                txn.events.len(),
                3,
                "multi-row/multi-table transaction is incomplete"
            );
            assert!(txn.events.iter().all(|e| e.kind == EventKind::Insert));
            delivered.push(txn);
        } else if durable != &last && capture.buffered_records() == 0 {
            let SpannerPosition::Frontier(previous) = SpannerPosition::decode(last.bytes())? else {
                panic!()
            };
            // Exclude lineage/state-only steps: unchanged partitions advanced by heartbeat.
            if previous.records().len() == frontier.records().len()
                && previous.records().iter().all(|(t, p)| {
                    frontier
                        .records()
                        .get(t)
                        .is_some_and(|next| p.state == next.state && p.parents == next.parents)
                })
            {
                progress += 1;
            }
        }
        last = durable.clone();
        if restart_step.is_none() && delivered.len() >= 3 && capture.buffered_records() > 0 {
            // This stop deliberately discards read-ahead records not handed to the writer.
            let read_ahead = capture.buffered_records();
            drop(capture);
            let recovered = journal.load(false).await?;
            assert_eq!(recovered.progress["buffered"], state.progress["buffered"]);
            let saved: Value = serde_json::from_str(&recovered.progress["buffered"].0)?;
            let saved_position = decode_buffered_position(
                saved["frontier"].as_str().unwrap(),
                &source,
                order.domain(),
            )?;
            assert_eq!(saved_position, last);
            let SpannerPosition::Frontier(frontier) =
                SpannerPosition::decode(saved_position.bytes())?
            else {
                panic!()
            };
            let persisted = buffered(&root, order.clone(), checkpoint.next_mseq).await?;
            assert_eq!(persisted[0], chunk);
            assert_eq!(&persisted[1..], delivered.as_slice());
            let identities = persisted[1..]
                .iter()
                .map(buffered_identities)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            let mut reopened = writer(&root, order.clone()).await?;
            assert_eq!(reopened.checkpoint()?, checkpoint);
            assert_eq!(
                reopened
                    .append(delivered.last().unwrap().clone(), Instant::now())
                    .await?,
                Redelivery::Noop
            );
            assert_eq!(reopened.checkpoint()?, checkpoint);
            capture = SpannerCapture::new(
                &endpoint,
                snapshot,
                make_reader(frontier, identities)?,
                reopened,
                10000,
            )?;
            restart_step = Some(steps);
            println!(
                "{name}: restarted after step {steps}, discarded {read_ahead} unreleased read-ahead records"
            );
        }
        if let Some(expected) = &expected
            && delivered.len() == expected.len()
            && restart_step.is_some()
            && progress > 0
            && resolved > *expected.last_key_value().unwrap().0
            && split_count(&frontier) > 0
        {
            let mut actual = BTreeSet::new();
            for txn in &delivered {
                let SpannerPosition::Txn { commit_ts, .. } =
                    SpannerPosition::decode(txn.commit_position.bytes())?
                else {
                    panic!()
                };
                let n = expected
                    .get(&commit_ts)
                    .expect("unwritten or snapshot transaction");
                let a = 1000 + 2 * n;
                let mut rows = BTreeSet::new();
                for event in &txn.events {
                    let decode = |name: &str| {
                        skedaddle::mapping::Value::from_canonical_bytes(
                            event.after.as_ref().unwrap()[name].bytes.as_ref().unwrap(),
                        )
                        .unwrap()
                    };
                    let skedaddle::mapping::Value::Text(payload) = decode("payload") else {
                        panic!()
                    };
                    assert_eq!(payload.bytes, format!("capture-{n}").as_bytes());
                    let skedaddle::mapping::Value::Int8(parent) = decode("parent_id") else {
                        panic!()
                    };
                    if event.table.as_deref() == Some("children") {
                        assert_eq!(decode("child_id"), skedaddle::mapping::Value::Int8(1));
                    }
                    assert!(rows.insert((event.table.clone().unwrap(), parent)));
                }
                assert_eq!(
                    rows,
                    BTreeSet::from([
                        ("parents".into(), a),
                        ("parents".into(), a + 1),
                        ("children".into(), a)
                    ])
                );
                assert!(actual.insert(commit_ts));
            }
            assert_eq!(actual, expected.keys().copied().collect());
            assert!(cancellations > 0, "pending steps were never cancelled");
            let checkpoint = capture.writer_checkpoint()?.unwrap();
            let persisted = buffered(&root, order.clone(), checkpoint.next_mseq).await?;
            assert_eq!(persisted[0], chunk);
            assert_eq!(&persisted[1..], delivered.as_slice());
            println!(
                "{name}: {} complete transactions, {} rows, {steps} steps, {} split parents, {progress} progress-only advances, {cancellations} cancelled pending steps; publish/marker/restart/dedup/order passed",
                delivered.len(),
                delivered.len() * 3,
                split_count(&frontier)
            );
            break;
        }
    }
    drop(capture);
    drop(journal);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
#[tokio::test]
async fn google_sql_capture_restart() -> Result<(), Error> {
    qualify("GOOGLE_STANDARD_SQL", SpannerDialect::GoogleSql).await
}
#[tokio::test]
async fn postgresql_capture_restart() -> Result<(), Error> {
    qualify("POSTGRESQL", SpannerDialect::Postgresql).await
}
