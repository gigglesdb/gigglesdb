use super::*;
use crate::{
    buffer::{
        backends::{BufferStore, ObjectBufferStore},
        format::{
            Limits, ManifestEntry, SegmentEncoder, SegmentHeader, SegmentRecord, segment_key,
        },
        reader::ReaderConfig,
    },
    event::{EventError, Position, PositionOrder, SourceIdentity},
};
use std::{
    cmp::Ordering,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "p6b"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
fn position(n: u64) -> Position {
    Position::new("p6b", n.to_be_bytes())
}
fn txn(n: u64) -> AssembledTxn {
    AssembledTxn::new(
        SourceIdentity {
            engine: "postgres".into(),
            system_id: "s".into(),
            lineage: "1".into(),
            db_name: "d".into(),
            stream: "s".into(),
        },
        TxnId(vec![n as u8]),
        position(n),
        Some(1_700_000_000_000_000),
        vec![],
    )
    .unwrap()
}
async fn reader() -> (BufferReader, Arc<dyn BufferStore>, ReaderConfig) {
    let root = crate::test_root::scratch_root().join(format!("p6b-{}", uuid::Uuid::new_v4()));
    crate::test_root::assert_under_work_root(&root);
    std::fs::create_dir_all(&root).unwrap();
    let store: Arc<dyn BufferStore> =
        Arc::new(ObjectBufferStore::local(root, Duration::from_secs(30)).unwrap());
    let c = ReaderConfig {
        prefix: "buffer".into(),
        migration_id: "migration".into(),
        stream: "forward".into(),
        limits: Limits::default(),
        required_next_mseq: 2,
    };
    let mut prev = [0; 32];
    for mseq in 0..2 {
        let start = 1 + mseq * 2;
        let mut encoder = SegmentEncoder::new(
            Vec::new(),
            SegmentHeader {
                migration_id: c.migration_id.clone(),
                stream: c.stream.clone(),
                epoch: 1,
                first_pos: position(start),
                last_pos: position(start + 1),
                record_count: 2,
            },
            c.limits,
        )
        .unwrap();
        for n in start..start + 2 {
            encoder
                .write_record(&SegmentRecord::Transaction(txn(n)))
                .unwrap();
        }
        let (bytes, summary) = encoder.finish().unwrap();
        let entry = ManifestEntry {
            mseq,
            epoch: 1,
            prev_digest: prev,
            segment_key: segment_key(
                &c.prefix,
                &c.migration_id,
                &c.stream,
                &position(start),
                &position(start + 1),
                1,
            ),
            segment_digest: summary.digest,
            first_pos: position(start),
            last_pos: position(start + 1),
            txn_count: 2,
            bytes: summary.bytes,
        };
        store
            .put_create_only(&entry.segment_key, &bytes)
            .await
            .unwrap();
        store
            .put_create_only(&c.manifest_key(mseq), &entry.canonical_json().unwrap())
            .await
            .unwrap();
        prev = entry.digest().unwrap();
    }
    (
        BufferReader::new(
            store.clone(),
            c.clone(),
            ReaderCheckpoint::default(),
            Arc::new(Order),
        )
        .unwrap(),
        store,
        c,
    )
}
fn plan(t: &AssembledTxn, mseq: u64) -> super::super::Result<TransactionPlan> {
    super::super::plan_transaction(
        t,
        &[],
        &super::super::PlanContext {
            positions: SourcePositions::native_pg("p6b"),
            stream: "forward".into(),
            mseq,
            origin: "origin".into(),
            synthetic_commit_time: None,
        },
        |_, _| unreachable!(),
    )
}
// Only acknowledgment seams are simulated. Files, segments, digests, manifests
// and the reader are real; real-PG tests exercise the same Destination path.
struct Probe {
    trace: Arc<Mutex<Vec<String>>>,
    fail: Option<u64>,
}
#[async_trait]
impl Destination for Probe {
    async fn apply(&mut self, p: &TransactionPlan) -> Result<CommitDecision> {
        self.trace.lock().unwrap().push(format!(
            "apply:{}:{}",
            p.progress.mseq,
            u64::from_be_bytes(p.progress.position.bytes().try_into().unwrap())
        ));
        if self.fail
            == Some(u64::from_be_bytes(
                p.progress.position.bytes().try_into().unwrap(),
            ))
        {
            return Err(Error::Database);
        }
        self.trace.lock().unwrap().push(format!(
            "commit:{}",
            u64::from_be_bytes(p.progress.position.bytes().try_into().unwrap())
        ));
        Ok(CommitDecision::Applied)
    }
    async fn retain_after_checkpoint(&mut self, cp: &ReaderCheckpoint) -> Result<()> {
        self.trace
            .lock()
            .unwrap()
            .push(format!("retain:{}", cp.next_mseq));
        Ok(())
    }
}
#[tokio::test]
async fn ordered_records_use_verified_mseq_and_commit_before_checkpoint() {
    let (reader, _, _) = reader().await;
    let mut runner = ReaderLoop::new(reader, "forward".into());
    let trace = Arc::new(Mutex::new(vec![]));
    let mut destination = Probe {
        trace: trace.clone(),
        fail: None,
    };
    for _ in 0..4 {
        let trace = trace.clone();
        assert!(
            runner
                .step(&mut destination, plan, |cp| async move {
                    trace.lock().unwrap().push(format!("save:{}", cp.next_mseq));
                    Ok(())
                })
                .await
                .unwrap()
        );
    }
    assert!(
        !runner
            .step(&mut destination, plan, |_| async {
                panic!("idle saves nothing")
            })
            .await
            .unwrap()
    );
    assert_eq!(
        *trace.lock().unwrap(),
        [
            "apply:0:1",
            "commit:1",
            "save:0",
            "retain:0",
            "apply:0:2",
            "commit:2",
            "save:1",
            "retain:1",
            "apply:1:3",
            "commit:3",
            "save:1",
            "retain:1",
            "apply:1:4",
            "commit:4",
            "save:2",
            "retain:2"
        ]
    );
}
#[tokio::test]
async fn failed_apply_cannot_save_or_consume_past_fault() {
    let (reader, _, _) = reader().await;
    let mut runner = ReaderLoop::new(reader, "forward".into());
    let trace = Arc::new(Mutex::new(vec![]));
    let mut destination = Probe {
        trace: trace.clone(),
        fail: Some(1),
    };
    assert!(
        runner
            .step(&mut destination, plan, |_| async {
                panic!("uncommitted checkpoint")
            })
            .await
            .is_err()
    );
    assert!(matches!(
        runner
            .step(&mut destination, plan, |_| async { Ok(()) })
            .await,
        Err(Error::Blocked)
    ));
    assert_eq!(*trace.lock().unwrap(), ["apply:0:1"]);
}
#[tokio::test]
async fn final_record_failure_cannot_save_advanced_segment_checkpoint() {
    let (reader, _, _) = reader().await;
    let mut runner = ReaderLoop::new(reader, "forward".into());
    let trace = Arc::new(Mutex::new(vec![]));
    let mut destination = Probe {
        trace: trace.clone(),
        fail: Some(2),
    };
    let saved = Arc::new(Mutex::new(Vec::new()));
    for _ in 0..2 {
        let saved = saved.clone();
        let _ = runner
            .step(&mut destination, plan, |cp| async move {
                saved.lock().unwrap().push(cp.next_mseq);
                Ok(())
            })
            .await;
    }
    assert_eq!(*saved.lock().unwrap(), [0]);
    assert_eq!(
        *trace.lock().unwrap(),
        ["apply:0:1", "commit:1", "retain:0", "apply:0:2"]
    );
}
#[tokio::test]
async fn failed_checkpoint_keeps_receipts_and_restart_replays_segment() {
    let (reader, store, c) = reader().await;
    let mut runner = ReaderLoop::new(reader, "forward".into());
    let trace = Arc::new(Mutex::new(vec![]));
    let mut destination = Probe {
        trace: trace.clone(),
        fail: None,
    };
    assert!(matches!(
        runner
            .step(&mut destination, plan, |_| async { Err(Error::Checkpoint) })
            .await,
        Err(Error::Checkpoint)
    ));
    assert_eq!(*trace.lock().unwrap(), ["apply:0:1", "commit:1"]);
    let mut restart =
        BufferReader::new(store, c, ReaderCheckpoint::default(), Arc::new(Order)).unwrap();
    assert_eq!(restart.next_txn().await.unwrap().unwrap().txn, txn(1).txn);
}
#[tokio::test]
async fn cancellation_blocks_the_consumed_loop() {
    let (reader, _, _) = reader().await;
    let mut runner = ReaderLoop::new(reader, "forward".into());
    let mut destination = Probe {
        trace: Arc::new(Mutex::new(vec![])),
        fail: None,
    };
    let result = tokio::time::timeout(
        Duration::from_millis(50),
        runner.step(&mut destination, plan, |_| std::future::pending()),
    )
    .await;
    assert!(result.is_err());
    assert!(matches!(
        runner
            .step(&mut destination, plan, |_| async { Ok(()) })
            .await,
        Err(Error::Blocked)
    ));
}

#[tokio::test]
async fn restart_prunes_only_its_already_durable_checkpoint_before_apply() {
    let (mut first, store, c) = reader().await;
    first.next_txn().await.unwrap();
    first.next_txn().await.unwrap();
    let durable = first.checkpoint().clone();
    assert_eq!(durable.next_mseq, 1);
    let restarted = BufferReader::new(store, c, durable, Arc::new(Order)).unwrap();
    let mut runner = ReaderLoop::new(restarted, "forward".into());
    let trace = Arc::new(Mutex::new(vec![]));
    let mut destination = Probe {
        trace: trace.clone(),
        fail: None,
    };
    runner
        .step(&mut destination, plan, |_| async { Ok(()) })
        .await
        .unwrap();
    assert_eq!(
        *trace.lock().unwrap(),
        ["retain:1", "apply:1:3", "commit:3", "retain:1"]
    );
}

// Capture only at an injected pre-apply failure. No simulated database outcome:
// the gated pg_apply campaign below qualifies actual Executor commits.
struct CaptureFault(Arc<Mutex<Option<TransactionPlan>>>);
#[async_trait]
impl Destination for CaptureFault {
    async fn apply(&mut self, p: &TransactionPlan) -> Result<CommitDecision> {
        *self.0.lock().unwrap() = Some(p.clone());
        Err(Error::Database)
    }
    async fn retain_after_checkpoint(&mut self, _: &ReaderCheckpoint) -> Result<()> {
        Ok(())
    }
}

async fn equal_h_reader(records: Vec<Vec<SegmentRecord>>) -> (Arc<dyn BufferStore>, ReaderConfig) {
    let root = crate::test_root::scratch_root().join(format!("equal-h-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let store: Arc<dyn BufferStore> =
        Arc::new(ObjectBufferStore::local(root, Duration::from_secs(30)).unwrap());
    let config = ReaderConfig {
        prefix: "buffer".into(),
        migration_id: "migration".into(),
        stream: "forward".into(),
        limits: Limits::default(),
        required_next_mseq: records.len() as u64,
    };
    let mut previous = [0; 32];
    let mut previous_position = None;
    for (mseq, records) in records.into_iter().enumerate() {
        let first = records.first().unwrap().position().clone();
        let last = records.last().unwrap().position().clone();
        let mut encoder = SegmentEncoder::new(
            Vec::new(),
            SegmentHeader {
                migration_id: config.migration_id.clone(),
                stream: config.stream.clone(),
                epoch: 1,
                first_pos: first.clone(),
                last_pos: last.clone(),
                record_count: records.len() as u64,
            },
            config.limits,
        )
        .unwrap();
        for record in records {
            encoder.write_record(&record).unwrap();
        }
        let (bytes, summary) = encoder.finish().unwrap();
        let entry = ManifestEntry {
            mseq: mseq as u64,
            epoch: 1,
            prev_digest: previous,
            segment_key: crate::buffer::format::boundary_segment_key(
                segment_key(
                    &config.prefix,
                    &config.migration_id,
                    &config.stream,
                    &first,
                    &last,
                    1,
                ),
                &first,
                previous_position.as_ref(),
            ),
            segment_digest: summary.digest,
            first_pos: first,
            last_pos: last.clone(),
            txn_count: summary.record_count,
            bytes: summary.bytes,
        };
        store
            .put_create_only(&entry.segment_key, &bytes)
            .await
            .unwrap();
        store
            .put_create_only(
                &config.manifest_key(entry.mseq),
                &entry.canonical_json().unwrap(),
            )
            .await
            .unwrap();
        previous = entry.digest().unwrap();
        previous_position = Some(last);
    }
    (store, config)
}

#[tokio::test]
async fn equal_h_real_reader_binds_manifest_kind_including_empty_nonfinal_chunk() {
    let h = txn(20);
    let mut rows = txn(20);
    rows.txn = TxnId(vec![21]);
    rows = AssembledTxn::new(
        rows.source,
        rows.txn,
        rows.commit_position,
        rows.commit_time,
        vec![],
    )
    .unwrap();
    // A nonfinal empty SKCR cannot be classified from its events. The verified
    // boundary still proves its envelope kind and the next-manifest relation.
    let (store, config) = equal_h_reader(vec![
        vec![SegmentRecord::Transaction(h)],
        vec![
            SegmentRecord::ChunkRows(rows),
            SegmentRecord::Transaction(txn(22)),
        ],
    ])
    .await;
    let mut reader = BufferReader::new(
        store.clone(),
        config.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    let captured = Arc::new(Mutex::new(None));
    let mut fault = CaptureFault(captured.clone());
    let mut runner = ReaderLoop::new(reader, "forward".into());
    assert!(
        runner
            .step(&mut fault, plan, |_| async { panic!("apply failed") })
            .await
            .is_err()
    );
    let h_plan = captured.lock().unwrap().take().unwrap();
    reader = BufferReader::new(
        store.clone(),
        config.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    reader.next_txn().await.unwrap();
    let checkpoint = reader.checkpoint().clone();
    let restarted = BufferReader::new(store, config, checkpoint, Arc::new(Order)).unwrap();
    let mut runner = ReaderLoop::new(restarted, "forward".into());
    assert!(
        runner
            .step(&mut fault, plan, |_| async { panic!("apply failed") })
            .await
            .is_err()
    );
    let rows_plan = captured.lock().unwrap().take().unwrap();
    assert_eq!(
        h_plan.progress.evidence.as_ref().unwrap().kind,
        super::super::RecordKind::Transaction
    );
    assert_eq!(
        rows_plan.progress.evidence.as_ref().unwrap().kind,
        super::super::RecordKind::ChunkRows
    );
    assert_eq!(
        resolve_commit(
            &SourcePositions::native_pg("p6b"),
            &rows_plan.progress,
            Some(&h_plan.progress),
            20,
            None
        ),
        Ok(CommitDecision::NotApplied)
    );
    assert_eq!(
        resolve_commit(
            &SourcePositions::native_pg("p6b"),
            &h_plan.progress,
            Some(&rows_plan.progress),
            20,
            Some(&h_plan.progress)
        ),
        Ok(CommitDecision::Applied)
    );
}

#[tokio::test]
async fn equal_h_real_reader_rejects_second_chunk_and_lineage_conflicts() {
    for different_lineage in [false, true] {
        let h = txn(20);
        let rows = txn(20);
        let mut conflict = txn(20);
        if different_lineage {
            conflict.source.lineage.push('x');
            conflict = AssembledTxn::new(
                conflict.source,
                conflict.txn,
                conflict.commit_position,
                conflict.commit_time,
                conflict.events,
            )
            .unwrap();
        }
        let records = if different_lineage {
            vec![
                vec![SegmentRecord::Transaction(h)],
                vec![SegmentRecord::ChunkRows(conflict)],
            ]
        } else {
            vec![
                vec![SegmentRecord::Transaction(h)],
                vec![SegmentRecord::ChunkRows(rows)],
                vec![SegmentRecord::ChunkRows(conflict)],
            ]
        };
        let (store, config) = equal_h_reader(records).await;
        let mut reader =
            BufferReader::new(store, config, ReaderCheckpoint::default(), Arc::new(Order)).unwrap();
        reader.next_txn().await.unwrap();
        if !different_lineage {
            reader.next_txn().await.unwrap();
        }
        assert!(matches!(
            reader.next_txn().await,
            Err(crate::buffer::reader::Error::RecoveryRequired(_))
        ));
    }
}

#[test]
fn native_pg_progress_and_receipt_position_bytes_match_v2_golden() {
    let txn = txn(0x0123_4567_89ab_cdef);
    let plan = plan(&txn, 7).unwrap();
    let golden = [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef];
    assert_eq!(plan.progress.position, Position::new("p6b", golden));
    assert_eq!(
        plan.statements.last().unwrap().parameters[3].bytes(),
        Some(golden.as_slice())
    );
    assert_eq!(receipt_position(&plan.progress), golden);
    assert_eq!(
        plan.statements[1].parameters[0].bytes(),
        Some(b"1234567/89ABCDEF".as_slice())
    );
}

#[test]
fn opaque_hash_lookup_verifies_full_domain_and_bytes() {
    let position = Position::new("decimal", b"1000");
    assert!(verify_hash_match(&position, &position).is_ok());
    for collision in [
        Position::new("decimal", b"1001"),
        Position::new("other", b"1000"),
    ] {
        assert!(matches!(
            verify_hash_match(&position, &collision),
            Err(Error::Apply(ApplyError::PositionHashCollision))
        ));
    }
    use sha2::{Digest, Sha256};
    assert_eq!(
        position_hash(&position).as_slice(),
        Sha256::digest(b"decimal\x001000").as_slice()
    );
}
