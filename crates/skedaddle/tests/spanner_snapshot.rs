//! Live snapshot cancellation, durable redelivery, and exact-T2 reconciliation.
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
    engine::Capture,
    event::{AssembledTxn, EventKind, Redelivery, Row},
    mapping::{EncodedBytes, Value as Scalar},
    spanner::{
        client::{Client, Error},
        frontier::{Frontier, SpannerOrder, SpannerPosition},
        snapshot::{
            Plan, TableName,
            copy::{Copy, Limits, PROGRESS_STREAM, Progress, ProgressSink},
        },
        stream::{Config, timestamp},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};
use tokio::sync::Notify;

async fn writer(root: &Path, order: Arc<SpannerOrder>) -> Result<BufferWriter, Error> {
    for sub in ["buffer", "scratch", "stage", "journal"] {
        std::fs::create_dir_all(root.join(sub))?;
    }
    let (tx, _rx) = mpsc::channel();
    Ok(BufferWriter::open_from_checkpoint(
        Arc::new(ObjectBufferStore::local(
            root.join("buffer"),
            Duration::from_secs(3),
        )?),
        WriterConfig {
            prefix: "s6c".into(),
            migration_id: "snapshot".into(),
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
            prefix: "s6c".into(),
            migration_id: "snapshot".into(),
            stream: "changes".into(),
            limits: Default::default(),
            required_next_mseq: through,
        },
        ReaderCheckpoint::default(),
        order,
    )?;
    let mut result = Vec::new();
    while let Some(txn) = reader.next_txn().await? {
        result.push(txn);
    }
    assert_eq!(reader.checkpoint().next_mseq, through);
    Ok(result)
}
struct Sink {
    journal: Journal,
    state: Materialized,
    // 1: stop after a durable batch marker; 2: manifest durable, marker absent.
    fault: u8,
    reached: Arc<Notify>,
}
impl Sink {
    async fn open(root: &Path, fault: u8) -> Result<Self, Error> {
        let journal = Journal::new(
            Arc::new(local_store(root.join("journal"))?),
            "s6c",
            "snapshot",
            Duration::from_secs(3),
        )?;
        let state = journal.load(false).await?;
        let mut sink = Self {
            journal,
            state,
            fault,
            reached: Arc::new(Notify::new()),
        };
        if sink.state.epoch == 0 {
            sink.put(Transition::AcquireOwnership { epoch: 1 }).await?;
        }
        Ok(sink)
    }
    async fn put(&mut self, transition: Transition) -> Result<(), Error> {
        let entry = Entry::new(&self.state, transition, "s6c")?;
        self.journal.put(&entry).await?;
        self.state.apply(&entry)?;
        Ok(())
    }
    fn saved(&self) -> Option<&str> {
        self.state
            .progress
            .get(PROGRESS_STREAM)
            .map(|p| p.0.as_str())
    }
}
impl ProgressSink for Sink {
    async fn save(&mut self, p: &Progress) -> Result<(), Error> {
        let stop = p.completed_partitions() > 0 && p.next_batch() == u64::from(self.fault);
        if !(stop && self.fault == 2) {
            self.put(Transition::Progress {
                stream: PROGRESS_STREAM.into(),
                marker: p.encode()?,
                position: format!("{}:{}", p.completed_partitions(), p.next_batch()),
            })
            .await?;
        }
        if stop && self.fault != 0 {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}
type Image = BTreeMap<(String, i64, i64), BTreeMap<String, Option<Vec<u8>>>>;
fn integer(row: &Row, name: &str) -> i64 {
    let Scalar::Int8(n) = Scalar::from_canonical_bytes(row[name].bytes.as_ref().unwrap()).unwrap()
    else {
        panic!("integer key")
    };
    n
}
fn replay(txns: &[AssembledTxn], snapshot: i128) -> Image {
    let mut image = Image::new();
    let mut ids = BTreeSet::new();
    let mut streaming = false;
    for txn in txns {
        assert!(
            ids.insert(txn.txn.0.clone()),
            "duplicate transaction identity"
        );
        let chunk = txn.events[0].kind == EventKind::ChunkRows;
        let SpannerPosition::Txn { commit_ts, .. } =
            SpannerPosition::decode(txn.commit_position.bytes()).unwrap()
        else {
            panic!("transaction position")
        };
        if chunk {
            assert!(!streaming);
            assert_eq!(commit_ts, snapshot);
        } else {
            streaming = true;
            assert!(commit_ts > snapshot, "transaction at/before T");
        }
        for e in &txn.events {
            let table = e.table.as_ref().unwrap().clone();
            let key = (
                table.clone(),
                integer(&e.key, "parent_id"),
                if table == "children" {
                    integer(&e.key, "child_id")
                } else {
                    0
                },
            );
            match e.kind {
                EventKind::ChunkRows | EventKind::Insert => {
                    assert!(
                        image
                            .insert(
                                key,
                                e.after
                                    .as_ref()
                                    .unwrap()
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.bytes.clone()))
                                    .collect()
                            )
                            .is_none(),
                        "duplicate snapshot/insert row"
                    );
                }
                EventKind::Update => {
                    let row = image.get_mut(&key).expect("update missing row");
                    for (k, v) in e.after.as_ref().unwrap() {
                        row.insert(k.clone(), v.bytes.clone());
                    }
                }
                EventKind::Delete => {
                    assert!(image.remove(&key).is_some(), "delete missing row");
                }
                _ => panic!("unexpected event"),
            }
        }
    }
    image
}
async fn source_image(plan: &Plan, client: &Client) -> Result<Image, Error> {
    let mut image = Image::new();
    for p in plan.partitions() {
        let mut rows = plan.execute(client, p.id()).await?;
        while let Some(row) = rows.next().await {
            let row = row?;
            let parent: i64 = row.get(0)?;
            let child = if p.table().name == "children" {
                row.get(1)?
            } else {
                0
            };
            let payload: String = row.get(if child == 0 { 1 } else { 2 })?;
            let mut values = BTreeMap::from([
                (
                    "parent_id".into(),
                    Some(Scalar::Int8(parent).canonical_bytes()?),
                ),
                (
                    "payload".into(),
                    Some(
                        Scalar::Text(EncodedBytes {
                            bytes: payload.into_bytes(),
                            encoding: "UTF8".into(),
                        })
                        .canonical_bytes()?,
                    ),
                ),
            ]);
            if child == 0 {
                values.insert("changed_at".into(), None);
            } else {
                values.insert(
                    "child_id".into(),
                    Some(Scalar::Int8(child).canonical_bytes()?),
                );
            }
            assert!(
                image
                    .insert((p.table().name.clone(), parent, child), values)
                    .is_none()
            );
        }
    }
    Ok(image)
}
async fn qualify(name: &str, dialect: SpannerDialect) -> Result<(), Error> {
    let Ok(path) = std::env::var("SKEDADDLE_SPANNER_CONFIG") else {
        eprintln!(
            "SPANNER EMULATOR SKIPPED ({name}): SKEDADDLE_SPANNER_CONFIG unset; run harness/run.ps1 -Phase spanner-emulator -SpannerCargoTest '-p skedaddle --test spanner_snapshot -- --nocapture'"
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
    let client = Arc::new(Client::connect(&endpoint, config["grpc_endpoint"].as_str()).await?);
    client
        .write(
            &[
                format!(
                    "INSERT INTO parents (parent_id,payload) VALUES {}",
                    (10..22)
                        .map(|n| format!("({n},'seed-{n}')"))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                format!(
                    "INSERT INTO children (parent_id,child_id,payload) VALUES {}",
                    (10..22)
                        .map(|n| format!("({n},1,'seed-{n}')"))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            ],
            "s6c-seed",
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
    let t = timestamp(plan.binding().timestamp());
    let order = Arc::new(SpannerOrder::new(
        &endpoint.project,
        &endpoint.instance,
        &endpoint.database,
        &endpoint.change_stream,
        t,
    )?);
    let root = std::path::PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT").ok_or("SKEDADDLE_TEST_ROOT required")?,
    )
    .join(format!("s6c-{name}-{}", uuid::Uuid::new_v4()));
    assert!(root.starts_with("J:/gigglesdb-work"));
    let first_writer = writer(&root, order.clone()).await?;
    let staging = Arc::new(ObjectBufferStore::local(
        root.join("stage"),
        Duration::from_secs(3),
    )?);
    let limits = Limits {
        concurrency: 1,
        rows: 1,
        bytes: 8192,
    };
    let stream_config = Config {
        concurrency: 8,
        channel_capacity: 1,
        query_window: Duration::from_secs(1),
        ..Default::default()
    };
    let mut copy = Copy::open(client.clone(), plan.clone(), staging.clone(), limits, None).await?;
    let mut capture = copy.handoff(first_writer, stream_config.clone(), 10000)?;
    let mut sink = Sink::open(&root, 1).await?;
    let reached = sink.reached.clone();
    let mut running = Box::pin(copy.run(&mut capture, &mut sink));
    tokio::select! {
        result = &mut running => panic!("copy ended before durable mid-partition cancellation: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(90), reached.notified()) => { result?; }
    }
    // The copy task is live, suspended mid-partition at the durable sink.
    // Mutations commit while the exact-T snapshot is incomplete.
    let mut expected = BTreeSet::new();
    for n in 0..6 {
        let commit = client
            .write(
                &[
                    format!("INSERT INTO parents (parent_id,payload) VALUES ({},'insert-{n}')", 100 + n),
                    format!("INSERT INTO children (parent_id,child_id,payload) VALUES ({},1,'insert-{n}')", 100 + n),
                    format!("UPDATE parents SET payload='updated-{n}' WHERE parent_id={}", 10 + n),
                    format!("UPDATE children SET payload='updated-{n}' WHERE parent_id={}", 10 + n),
                    format!("DELETE FROM children WHERE parent_id={}", 16 + n),
                    format!("DELETE FROM parents WHERE parent_id={}", 16 + n),
                ],
                "s6c-concurrent",
            )
            .await?;
        assert!(expected.insert(timestamp(commit)));
    }
    drop(running); // cancel the task future; discard all in-memory copy progress
    let mut executions = copy.partition_executions();
    let checkpoint = capture.writer_checkpoint()?.unwrap();
    let first = buffered(&root, order.clone(), checkpoint.next_mseq).await?;
    let saved = sink.saved().unwrap().to_owned();
    let progress = Progress::decode(&saved, &plan, limits)?;
    assert!(progress.completed_partitions() > 0 && progress.next_batch() == 1);
    let interrupted = progress.completed_partitions();
    drop((copy, capture, sink));
    // T2 is fixed by this strong read; all source reads below use its saved tokens.
    let at_t2 = Plan::create(&client, &selected).await?;
    let t2 = timestamp(at_t2.binding().timestamp());
    assert!(t2 > *expected.last().unwrap() && *expected.first().unwrap() > t);
    let mut sink = Sink::open(&root, 2).await?;
    assert_eq!(sink.saved(), Some(saved.as_str()));
    let mut copy = Copy::open(
        client.clone(),
        plan.clone(),
        staging.clone(),
        limits,
        sink.saved(),
    )
    .await?;
    let mut capture = copy.handoff(
        writer(&root, order.clone()).await?,
        stream_config.clone(),
        10000,
    )?;
    let reached = sink.reached.clone();
    let mut running = Box::pin(copy.run(&mut capture, &mut sink));
    tokio::select! {
        result = &mut running => panic!("copy ended before manifest-before-marker fault: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(90), reached.notified()) => { result?; }
    }
    drop(running);
    for (total, n) in executions.iter_mut().zip(copy.partition_executions()) {
        *total += n;
    }
    assert!(
        copy.partition_executions()[..=interrupted]
            .iter()
            .all(|n| *n == 0)
    );
    let crashed = capture.writer_checkpoint()?.unwrap();
    assert_eq!(crashed.next_mseq, checkpoint.next_mseq + 1);
    assert_eq!(
        sink.saved(),
        Some(saved.as_str()),
        "crash must leave journal behind durable manifest"
    );
    let before_resume = buffered(&root, order.clone(), crashed.next_mseq).await?;
    assert_eq!(&before_resume[..first.len()], first.as_slice());
    drop((copy, capture, sink));
    let mut sink = Sink::open(&root, 0).await?;
    let mut reopened = writer(&root, order.clone()).await?;
    assert_eq!(reopened.checkpoint()?, crashed);
    let (redelivery, manifest) = reopened
        .append_chunk_rows(before_resume.last().unwrap().clone(), Instant::now())
        .await?;
    assert_eq!(redelivery, Redelivery::Noop);
    assert_eq!(manifest.mseq + 1, crashed.next_mseq);
    assert_eq!(
        reopened.checkpoint()?,
        crashed,
        "redelivery must reuse manifest"
    );
    let mut copy = Copy::open(client.clone(), plan.clone(), staging, limits, sink.saved()).await?;
    let mut capture = copy.handoff(reopened, stream_config, 10000)?;
    tokio::time::timeout(Duration::from_secs(90), copy.run(&mut capture, &mut sink)).await??;
    for (total, n) in executions.iter_mut().zip(copy.partition_executions()) {
        *total += n;
    }
    assert_eq!(
        executions,
        vec![1; plan.partitions().len()],
        "every partition executed exactly once across cancellations"
    );
    assert!(
        copy.partition_executions()[..=interrupted]
            .iter()
            .all(|n| *n == 0)
    );
    assert_eq!(
        copy.progress().completed_partitions(),
        plan.partitions().len()
    );
    let snapshot_checkpoint = capture.writer_checkpoint()?.unwrap();
    let chunks = buffered(&root, order.clone(), snapshot_checkpoint.next_mseq).await?;
    assert_eq!(&chunks[..before_resume.len()], before_resume.as_slice());
    assert_eq!(
        chunks.iter().map(|t| t.events.len()).sum::<usize>(),
        24,
        "no snapshot row lost/duplicated"
    );
    assert_eq!(replay(&chunks, t).len(), 24);
    // Handoff reader was constructed with Frontier::new(T); consume only after copy.
    let initial = Frontier::new(t);
    assert_eq!(initial.resolved_ts()?, Some(t));
    let mut delivered = BTreeSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let batch = tokio::time::timeout_at(deadline, capture.step()).await??;
        if let Some(txn) = batch.transaction {
            let SpannerPosition::Txn { commit_ts, .. } =
                SpannerPosition::decode(txn.commit_position.bytes())?
            else {
                panic!()
            };
            assert!(commit_ts > t && commit_ts <= t2);
            assert!(delivered.insert(commit_ts), "duplicate stream transaction");
            assert_eq!(txn.events.len(), 6, "multi-table change incomplete");
        }
        let durable = batch.durable_position.as_ref().unwrap();
        let SpannerPosition::Frontier(frontier) = SpannerPosition::decode(durable.bytes())? else {
            panic!()
        };
        if frontier.resolved_ts()?.is_some_and(|ts| ts >= t2) {
            break;
        }
    }
    assert_eq!(
        delivered, expected,
        "no missing or extra post-T transaction"
    );
    let final_checkpoint = capture.writer_checkpoint()?.unwrap();
    let all = buffered(&root, order.clone(), final_checkpoint.next_mseq).await?;
    assert_eq!(&all[..chunks.len()], chunks.as_slice());
    assert_eq!(all.len(), chunks.len() + expected.len());
    for pair in all.windows(2) {
        assert!(
            pair[0]
                .commit_position
                .compare(&pair[1].commit_position, order.as_ref())?
                .is_lt()
        );
    }
    assert_eq!(
        replay(&all, t),
        source_image(&at_t2, &client).await?,
        "verified snapshot + changes differs from exact-T2 source"
    );
    println!(
        "{name}: {} partitions executed once; 24 snapshot rows; {} unique six-event transactions; both cancellation windows, Noop manifest reuse and exact-T2 image equality passed",
        executions.len(),
        expected.len()
    );
    drop((copy, capture, sink));
    std::fs::remove_dir_all(root)?;
    Ok(())
}
#[tokio::test]
async fn google_sql_snapshot_restart_handoff() -> Result<(), Error> {
    qualify("GOOGLE_STANDARD_SQL", SpannerDialect::GoogleSql).await
}
#[tokio::test]
async fn postgresql_snapshot_restart_handoff() -> Result<(), Error> {
    qualify("POSTGRESQL", SpannerDialect::Postgresql).await
}
