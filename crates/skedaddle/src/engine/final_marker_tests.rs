use super::*;
use crate::{
    buffer::{
        backends::ObjectBufferStore,
        format,
        reader::{BufferReader, ReaderConfig},
        writer::{BufferWriter, WriterConfig},
    },
    coordinator::{
        epoch::Coordinator,
        journal::{self, Journal},
        local::Local,
    },
    engine::Engine,
    event::{Event, EventError, OpaqueValue, Position, PositionOrder, Row, TxnId},
    pg::chunk::{ChunkId, WatermarkKind},
};
use std::{cmp::Ordering, path::PathBuf, time::Instant};

struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "pg:1:1:slot"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "postgres".into(),
        system_id: "1".into(),
        lineage: "1".into(),
        db_name: "db".into(),
        stream: "slot".into(),
    }
}
fn marker() -> Marker {
    Marker {
        cutover: "cutover".into(),
        epoch: 1,
        lsn: 15,
    }
}
fn key() -> Key {
    Key {
        migration: "migration".into(),
        cutover: "cutover".into(),
        epoch: 1,
        coordinator: "owner".into(),
    }
}
fn transaction() -> AssembledTxn {
    let position = Position::new(Order.domain(), 20u64.to_be_bytes());
    let event = Event { source: source(), txn: TxnId(vec![1]), ordinal_in_txn: 0, position: position.clone(), schema_version: 0, kind: EventKind::Watermark, table: None, key: Row::new(), before: None,
        after: Some(Row::from([("control".into(), OpaqueValue { type_id: "skedaddle.watermark.v1".into(), bytes: Some(serde_json::to_vec(&serde_json::json!({"message_lsn":15,"content":marker().payload().as_bytes()})).unwrap()) })])), unchanged_cols: Default::default() };
    AssembledTxn::new(source(), TxnId(vec![1]), position, Some(1), vec![event]).unwrap()
}
struct Fixture {
    engine: Engine,
    journal: Journal,
    writer: BufferWriter,
    store: Arc<ObjectBufferStore>,
    root: PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let root =
            crate::test_root::scratch_root().join(format!("final-marker-{}", uuid::Uuid::new_v4()));
        for name in ["journal", "buffer", "scratch"] {
            std::fs::create_dir_all(root.join(name)).unwrap();
        }
        let journal = Journal::new(
            Arc::new(journal::local_store(root.join("journal")).unwrap()),
            "engine",
            "migration",
            Duration::from_secs(5),
        )
        .unwrap();
        let mut coordinator = Coordinator::observer(
            journal.clone(),
            Local::create(root.join("local")).unwrap(),
            "owner",
        )
        .unwrap();
        coordinator.acquire().await.unwrap();
        let engine = Engine::new(coordinator, journal.clone());
        engine
            .runtime
            .6
            .markers
            .begin(
                "migration",
                source(),
                Order.domain().into(),
                &engine.runtime.snapshot(),
            )
            .unwrap();
        let store = Arc::new(
            ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(5)).unwrap(),
        );
        let (feedback, _) = std::sync::mpsc::channel();
        let writer = BufferWriter::open_with_scratch(
            store.clone(),
            WriterConfig {
                prefix: "engine".into(),
                migration_id: "migration".into(),
                stream: "wal".into(),
                epoch: 1,
                segment_bytes: 1000000,
                segment_transactions: 100,
                segment_age: Duration::from_secs(1),
                max_transaction_bytes: 1000000,
            },
            Arc::new(Order),
            feedback,
            root.join("scratch"),
        )
        .await
        .unwrap();
        Self {
            engine,
            journal,
            writer,
            store,
            root,
        }
    }
    fn progress(&self) -> FinalMarkerProgress {
        let key = Key {
            coordinator: self.engine.runtime.snapshot().owner,
            ..key()
        };
        self.engine
            .runtime
            .final_marker_progress(&key, &source())
            .unwrap()
    }
    fn batch() -> Captured {
        Captured {
            transaction: Some(transaction()),
            durable_position: Some(Position::new(Order.domain(), 21u64.to_be_bytes())),
            ..Default::default()
        }
    }
    async fn seal(&mut self) {
        self.writer
            .append(transaction(), Instant::now())
            .await
            .unwrap();
        self.writer.seal().await.unwrap();
    }
    async fn buffered(&self) {
        self.engine
            .runtime
            .capture_final_marker(&Self::batch(), Some(&self.writer.checkpoint().unwrap()))
            .await
            .unwrap();
    }
    async fn replay_record(&self) -> (AssembledTxn, ReaderCheckpoint) {
        let mut reader = BufferReader::new(
            self.store.clone(),
            ReaderConfig {
                prefix: "engine".into(),
                migration_id: "migration".into(),
                stream: "wal".into(),
                limits: format::Limits::default(),
                required_next_mseq: 1,
            },
            ReaderCheckpoint::default(),
            Arc::new(Order),
        )
        .unwrap();
        let txn = reader.next_txn().await.unwrap().unwrap();
        (txn, reader.checkpoint().clone())
    }
}

#[test]
fn chunk_final_other_and_unknown_are_distinct() {
    let chunk = Watermark {
        wm: WatermarkKind::High,
        chunk: ChunkId {
            table: "table".into(),
            plan: "a".repeat(64),
            ordinal: 0,
        },
        attempt: 1,
    };
    let bytes = serde_json::to_vec(&chunk).unwrap();
    assert_eq!(
        decode_marker_payload(&bytes, 15).unwrap(),
        CapturedControl::Chunk(chunk)
    );
    assert_eq!(
        decode_marker_payload(marker().payload().as_bytes(), 15).unwrap(),
        CapturedControl::Final(marker())
    );
    for bytes in [
        b"{}".as_slice(),
        b"{\"final\":\"cutover\",\"epoch\":1,\"unknown\":true}",
        b"garbage",
    ] {
        assert_eq!(
            decode_marker_payload(bytes, 15),
            Err(MarkerProgressError::UnknownPayload)
        );
    }
    assert_eq!(
        decode_marker_payload(&vec![b' '; MAX_PAYLOAD + 1], 15),
        Err(MarkerProgressError::UnknownPayload)
    );
    let mut event = transaction().events.remove(0);
    event.kind = EventKind::DdlBoundary;
    assert_eq!(
        classify_captured_control(&event).unwrap(),
        CapturedControl::Other(EventKind::DdlBoundary)
    );
}

#[tokio::test]
async fn actual_manifest_durability_and_exact_reader_boundary_gate_receipts() {
    let mut fixture = Fixture::new().await;
    let progress = fixture.progress();
    fixture
        .writer
        .append(transaction(), Instant::now())
        .await
        .unwrap();
    assert!(
        fixture
            .engine
            .runtime
            .capture_final_marker(
                &Fixture::batch(),
                Some(&fixture.writer.checkpoint().unwrap())
            )
            .await
            .is_err()
    );
    assert!(progress.receipt(&marker(), false).unwrap().is_none());
    fixture
        .engine
        .runtime
        .marker("buffered", "manifest-durable".into(), "999999".into())
        .await
        .unwrap();
    assert!(progress.receipt(&marker(), false).unwrap().is_none());
    fixture.writer.seal().await.unwrap();
    fixture
        .engine
        .runtime
        .2
        .persistence
        .lock()
        .unwrap()
        .last_flush = Instant::now();
    fixture
        .engine
        .runtime
        .progress_marker("buffered", "manifest-durable".into(), "1000000".into())
        .await
        .unwrap();
    fixture.buffered().await;
    assert_eq!(
        fixture.engine.runtime.snapshot().progress["buffered"].1,
        "1000000"
    );
    let buffered = progress.receipt(&marker(), false).unwrap().unwrap();
    assert_eq!(buffered.marker().lsn, 15);
    assert_eq!(buffered.commit_lsn, 20);
    assert!(progress.receipt(&marker(), true).unwrap().is_none());
    let (txn, checkpoint) = fixture.replay_record().await;
    let mut wrong = checkpoint.clone();
    wrong.next_mseq += 1;
    assert!(
        fixture
            .engine
            .runtime
            .applied_final_marker(&txn, 0, &wrong)
            .await
            .is_err()
    );
    assert!(progress.receipt(&marker(), true).unwrap().is_none());
    fixture
        .engine
        .runtime
        .applied_final_marker(&txn, 0, &checkpoint)
        .await
        .unwrap();
    let applied = progress.receipt(&marker(), true).unwrap().unwrap();
    assert_eq!(applied.buffered(), buffered);
    let replay = fixture.journal.load(false).await.unwrap();
    assert!(replay.progress.contains_key(APPLIED));
    assert!(
        !replay.progress.contains_key("applied"),
        "exact applied attestation precedes ordinary cursor movement"
    );
}

#[tokio::test]
async fn durable_buffer_without_receipt_is_repaired_by_exact_replay_and_survives_restart() {
    let mut fixture = Fixture::new().await;
    fixture.seal().await;
    assert!(
        !fixture
            .journal
            .load(false)
            .await
            .unwrap()
            .progress
            .contains_key(BUFFERED)
    );
    let (txn, checkpoint) = fixture.replay_record().await;
    fixture
        .engine
        .runtime
        .applied_final_marker(&txn, 0, &checkpoint)
        .await
        .unwrap();
    let old = fixture.progress();
    let expected = old.receipt(&marker(), true).unwrap().unwrap();
    fixture.engine.runtime.6.markers.finish();
    assert_eq!(
        old.receipt(&marker(), true),
        Err(MarkerProgressError::RunExited)
    );
    let mut coordinator = Coordinator::observer(
        fixture.journal.clone(),
        Local::create(fixture.root.join("restart-local")).unwrap(),
        "owner",
    )
    .unwrap();
    assert_eq!(coordinator.acquire().await.unwrap(), 2);
    let restored = Engine::new(coordinator, fixture.journal.clone());
    restored
        .runtime
        .6
        .markers
        .begin(
            "migration",
            source(),
            Order.domain().into(),
            &restored.runtime.snapshot(),
        )
        .unwrap();
    let restored_key = Key {
        epoch: 2,
        coordinator: restored.runtime.snapshot().owner,
        ..key()
    };
    let resumed = restored
        .runtime
        .final_marker_progress(&restored_key, &source())
        .unwrap();
    assert_eq!(resumed.receipt(&marker(), true).unwrap(), Some(expected));
    assert_eq!(
        old.receipt(&marker(), true),
        Err(MarkerProgressError::RunExited)
    );
}

#[tokio::test]
async fn gaps_identity_epoch_and_generation_revoke_without_blocking_fresh_runs() {
    let mut fixture = Fixture::new().await;
    fixture.seal().await;
    fixture.buffered().await;
    let old = fixture.progress();
    for wrong in [
        Marker {
            cutover: "other".into(),
            ..marker()
        },
        Marker {
            lsn: 16,
            ..marker()
        },
    ] {
        assert_eq!(
            old.receipt(&wrong, false),
            Err(MarkerProgressError::IdentityMismatch)
        );
    }
    assert_eq!(
        old.receipt(
            &Marker {
                epoch: 2,
                ..marker()
            },
            false
        ),
        Err(MarkerProgressError::EpochChanged)
    );
    fixture.engine.runtime.invalidate_final_marker_capture_gap();
    assert_eq!(
        old.receipt(&marker(), false),
        Err(MarkerProgressError::CaptureGap)
    );
    fixture
        .engine
        .runtime
        .6
        .markers
        .begin(
            "migration",
            source(),
            Order.domain().into(),
            &fixture.engine.runtime.snapshot(),
        )
        .unwrap();
    assert_eq!(
        old.receipt(&marker(), false),
        Err(MarkerProgressError::Rebound)
    );
    let fresh = fixture.progress();
    assert!(fresh.receipt(&marker(), false).unwrap().is_some());
    let mut state = fixture.engine.runtime.snapshot();
    state.epoch = 2;
    *fixture.engine.runtime.1.write().unwrap() = state.clone();
    assert_eq!(
        fresh.receipt(&marker(), false),
        Err(MarkerProgressError::EpochChanged)
    );
    fixture
        .engine
        .runtime
        .6
        .markers
        .begin("migration", source(), Order.domain().into(), &state)
        .unwrap();
    let next = fixture
        .engine
        .runtime
        .final_marker_progress(
            &Key {
                epoch: 2,
                coordinator: fixture.engine.runtime.snapshot().owner,
                ..key()
            },
            &source(),
        )
        .unwrap();
    assert!(next.receipt(&marker(), false).unwrap().is_some());
    assert_eq!(
        next.receipt(
            &Marker {
                epoch: 2,
                ..marker()
            },
            false
        ),
        Err(MarkerProgressError::IdentityMismatch)
    );
}

#[tokio::test]
async fn bounded_wait_has_typed_timeout_and_wakes_on_gap_or_epoch_without_markers() {
    let fixture = Fixture::new().await;
    let progress = fixture.progress();
    assert_eq!(
        progress
            .wait(&marker(), false, Duration::from_millis(5))
            .await,
        Err(MarkerProgressError::Timeout)
    );
    let gap = async {
        tokio::task::yield_now().await;
        fixture.engine.runtime.invalidate_final_marker_capture_gap();
    };
    let marker = marker();
    let (wait, _) = tokio::join!(progress.wait(&marker, true, Duration::from_secs(1)), gap);
    assert_eq!(wait, Err(MarkerProgressError::CaptureGap));
    fixture
        .engine
        .runtime
        .6
        .markers
        .begin(
            "migration",
            source(),
            Order.domain().into(),
            &fixture.engine.runtime.snapshot(),
        )
        .unwrap();
    let progress = fixture.progress();
    let epoch = async {
        tokio::task::yield_now().await;
        let journal = fixture.journal.clone();
        let mut next = Coordinator::observer(
            journal,
            Local::create(fixture.root.join("next-owner")).unwrap(),
            "next-owner",
        )
        .unwrap();
        next.acquire().await.unwrap();
    };
    let (wait, _) = tokio::join!(progress.wait(&marker, true, Duration::from_secs(1)), epoch);
    assert_eq!(wait, Err(MarkerProgressError::EpochChanged));
    assert_eq!(
        progress.query(&marker, true).await,
        Err(MarkerProgressError::EpochChanged)
    );
}

#[tokio::test]
async fn same_marker_cannot_substitute_another_transaction_or_manifest() {
    let mut fixture = Fixture::new().await;
    fixture.seal().await;
    fixture.buffered().await;
    let receipt = fixture
        .progress()
        .receipt(&marker(), false)
        .unwrap()
        .unwrap();
    let before = fixture.journal.load(false).await.unwrap().applied_seq;
    for altered in [
        FinalMarkerReceipt {
            txn: vec![2],
            ..receipt.clone()
        },
        FinalMarkerReceipt {
            txn_digest: [9; 32],
            ..receipt.clone()
        },
        FinalMarkerReceipt {
            manifest_digest: [9; 32],
            ..receipt.clone()
        },
    ] {
        assert!(
            fixture
                .engine
                .runtime
                .persist_final_receipt(altered)
                .await
                .is_err()
        );
    }
    assert_eq!(
        fixture.journal.load(false).await.unwrap().applied_seq,
        before
    );
}

#[tokio::test]
async fn dropping_actual_run_guard_revokes_and_wakes_pending_wait() {
    let fixture = Fixture::new().await;
    let progress = fixture.progress();
    let guard = RunGuard(fixture.engine.runtime.6.markers.clone());
    let exit = async {
        tokio::task::yield_now().await;
        drop(guard);
    };
    let marker = marker();
    let (wait, _) = tokio::join!(progress.wait(&marker, true, Duration::from_secs(1)), exit);
    assert_eq!(wait, Err(MarkerProgressError::RunExited));
}
