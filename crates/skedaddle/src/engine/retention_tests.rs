//! Retention tests use actual local journal/buffer objects and authenticated
//! capture records. Direct queue calls supply trusted receipt facts to isolate
//! retention ordering; real destination receipt qualification is tests/engine.rs.
use super::*;
use crate::buffer::{
    backends::BufferStore,
    gc::RetentionPolicy,
    reader::{BufferReader, ReaderCheckpoint},
    writer::{BufferWriter, WriterConfig},
};

async fn buffered() -> (
    Fixture,
    crate::buffer::writer::WriterCheckpoint,
    Vec<(crate::event::Position, ReaderCheckpoint)>,
) {
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(16);
    for lsn in [20, 30, 40] {
        marker(
            &tx,
            Watermark {
                wm: WatermarkKind::Low,
                chunk: plan().ranges[0].id.clone(),
                attempt: 1,
            },
            lsn,
        )
        .await;
    }
    let mut capture = f.capture(rx, 10).await;
    for _ in 0..9 {
        capture.step().await.unwrap();
    }
    let writer = capture.writer_checkpoint().unwrap().unwrap();
    let mut reader = BufferReader::new(
        f.store.clone(),
        f.config.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    let mut facts = Vec::new();
    while let Some(txn) = reader.next_txn().await.unwrap() {
        facts.push((txn.commit_position, reader.checkpoint().clone()));
    }
    f.engine
        .enable_retention(
            f.store.clone(),
            f.config.clone(),
            Arc::new(Order),
            RetentionPolicy {
                retained_for_ms: 0,
                orphan_grace_ms: 0,
            },
            Duration::from_nanos(1),
        )
        .await
        .unwrap();
    (f, writer, facts)
}
#[tokio::test]
async fn retention_requires_apply_and_both_durable_checkpoints_and_shrinks_bytes() {
    let (f, writer, facts) = buffered().await;
    let runtime = f.engine.runtime();
    let before = f
        .store
        .list_prefix("engine/", None, 1000)
        .await
        .unwrap()
        .objects
        .iter()
        .map(|o| o.bytes)
        .sum::<u64>();
    runtime.queue_writer(Some(writer)).await;
    runtime.retention_tick().await.unwrap();
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_some()
    );
    // A complete first segment with a prefix equal to its last record is retained.
    runtime
        .queue_applied(facts[0].0.clone(), facts[0].1.clone())
        .await;
    runtime.retention_tick().await.unwrap();
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_some()
    );
    runtime
        .queue_applied(facts[1].0.clone(), facts[1].1.clone())
        .await;
    runtime.retention_tick().await.unwrap();
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        f.store
            .get(&f.config.manifest_key(1))
            .await
            .unwrap()
            .is_some()
    );
    let status = runtime.retention_status().await.unwrap();
    assert_eq!(status.deleted_objects, 2);
    assert!(status.deleted_bytes > 0);
    let after = f
        .store
        .list_prefix("engine/", None, 1000)
        .await
        .unwrap()
        .objects
        .iter()
        .map(|o| o.bytes)
        .sum::<u64>();
    assert!(
        after < before,
        "actual retained local buffer bytes did not shrink"
    );
    assert!(
        runtime
            .state()
            .await
            .progress
            .keys()
            .any(|k| k.starts_with("buffer-gc:"))
    );
}
#[tokio::test]
async fn retention_apply_before_writer_checkpoint_is_retained_then_retry_succeeds() {
    let (mut f, writer, facts) = buffered().await;
    let runtime = f.engine.runtime();
    runtime
        .queue_applied(facts[1].0.clone(), facts[1].1.clone())
        .await;
    runtime.retention_tick().await.unwrap();
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_some()
    );
    runtime
        .lifecycle(Event::Interrupt {
            to: State::Paused,
            reason: "pause after durable apply before writer checkpoint".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::Paused
    );
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_some()
    );
    // Reconstruct a new owner from the real durable journal, as after a crash.
    let mut owner = Coordinator::observer(
        f.journal.clone(),
        Local::create(f.root.join("apply-checkpoint-restart")).unwrap(),
        "restart",
    )
    .unwrap();
    owner.acquire().await.unwrap();
    f.engine = Engine::new(owner, f.journal.clone());
    f.engine.resume().await.unwrap();
    f.engine
        .enable_retention(
            f.store.clone(),
            f.config.clone(),
            Arc::new(Order),
            RetentionPolicy {
                retained_for_ms: 0,
                orphan_grace_ms: 0,
            },
            Duration::from_nanos(1),
        )
        .await
        .unwrap();
    let runtime = f.engine.runtime();
    runtime.queue_writer(Some(writer)).await;
    runtime.retention_tick().await.unwrap();
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn retention_operational_journal_failure_blocks_only_gc_then_retries() {
    let (f, writer, facts) = buffered().await;
    let runtime = f.engine.runtime();
    runtime.queue_writer(Some(writer)).await;
    runtime
        .queue_applied(facts[1].0.clone(), facts[1].1.clone())
        .await;
    // A real local-store failure publishing immutable GC checkpoint payloads;
    // the authoritative journal entries remain readable and intact.
    let obstruction = f.root.join("journal").join("gc-checkpoints");
    std::fs::write(&obstruction, b"injected journal checkpoint access failure").unwrap();
    let result = runtime.retention_tick().await;
    let migration_progress = runtime
        .marker(
            "retention-outage-migration-progress",
            "continued".into(),
            "1".into(),
        )
        .await;
    std::fs::remove_file(&obstruction).unwrap();
    migration_progress.unwrap();
    result.unwrap();
    assert_eq!(runtime.state().await.lifecycle.state, State::Copying);
    assert!(
        runtime
            .retention_status()
            .await
            .unwrap()
            .blocked_reason
            .is_some()
    );
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_some()
    );
    runtime.retention_tick().await.unwrap();
    assert!(
        runtime
            .retention_status()
            .await
            .unwrap()
            .blocked_reason
            .is_none()
    );
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn retention_stale_epoch_requires_recovery_without_deleting() {
    let (f, writer, facts) = buffered().await;
    let runtime = f.engine.runtime();
    runtime.queue_writer(Some(writer)).await;
    runtime
        .queue_applied(facts[1].0.clone(), facts[1].1.clone())
        .await;
    let mut replacement = Coordinator::observer(
        f.journal.clone(),
        Local::create(f.root.join("replacement-local")).unwrap(),
        "replacement",
    )
    .unwrap();
    replacement.acquire().await.unwrap();
    let before = f.journal.load(false).await.unwrap().applied_seq;
    assert!(matches!(
        runtime.retention_tick().await,
        Err(coordinator::Error::RecoveryRequired(_))
    ));
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        runtime.state().await.lifecycle.state,
        State::RecoveryRequired
    );
    assert!(runtime.retention_status().await.unwrap().recovery_required);
    assert_eq!(f.journal.load(false).await.unwrap().applied_seq, before);
}
#[tokio::test]
async fn retention_restart_finishes_segment_deleted_before_manifest() {
    let (f, writer, facts) = buffered().await;
    // First persist both restart checkpoints without allowing retirement.
    f.engine
        .enable_retention(
            f.store.clone(),
            f.config.clone(),
            Arc::new(Order),
            RetentionPolicy {
                retained_for_ms: 60_000,
                orphan_grace_ms: 0,
            },
            Duration::from_nanos(1),
        )
        .await
        .unwrap();
    let runtime = f.engine.runtime();
    runtime.queue_writer(Some(writer)).await;
    runtime
        .queue_applied(facts[1].0.clone(), facts[1].1.clone())
        .await;
    runtime.retention_tick().await.unwrap();
    let bytes = f
        .store
        .get(&f.config.manifest_key(0))
        .await
        .unwrap()
        .unwrap();
    let entry = format::ManifestEntry::from_canonical_json(&bytes).unwrap();
    crate::buffer::reader::verify_segment(f.store.as_ref(), &f.config, &entry, None, &Order)
        .await
        .unwrap();
    runtime
        .marker(
            "engine-retention-proof:wal",
            String::from_utf8(bytes).unwrap(),
            "0".into(),
        )
        .await
        .unwrap();
    // Inject process loss at the actual local object-delete boundary.
    f.store.delete(&entry.segment_key).await.unwrap();
    f.engine
        .enable_retention(
            f.store.clone(),
            f.config.clone(),
            Arc::new(Order),
            RetentionPolicy {
                retained_for_ms: 0,
                orphan_grace_ms: 0,
            },
            Duration::from_nanos(1),
        )
        .await
        .unwrap();
    runtime.retention_tick().await.unwrap();
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn retention_preserves_a_partially_applied_multi_transaction_segment() {
    let (f, _, _) = buffered().await;
    let mut old = BufferReader::new(
        f.store.clone(),
        f.config.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    let mut txns = Vec::new();
    while let Some(txn) = old.next_txn().await.unwrap() {
        txns.push(txn);
    }
    let mut config = f.config.clone();
    config.prefix = "partial".into();
    let (feedback, _) = std::sync::mpsc::channel();
    std::fs::create_dir_all(f.root.join("partial-scratch")).unwrap();
    let mut writer = BufferWriter::open_with_scratch(
        f.store.clone(),
        WriterConfig {
            prefix: config.prefix.clone(),
            migration_id: config.migration_id.clone(),
            stream: config.stream.clone(),
            epoch: 1,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(60),
            max_transaction_bytes: 1_000_000,
        },
        Arc::new(Order),
        feedback,
        f.root.join("partial-scratch"),
    )
    .await
    .unwrap();
    for txn in txns {
        writer.append(txn, std::time::Instant::now()).await.unwrap();
    }
    writer.seal().await.unwrap();
    f.engine
        .enable_retention(
            f.store.clone(),
            config.clone(),
            Arc::new(Order),
            RetentionPolicy {
                retained_for_ms: 0,
                orphan_grace_ms: 0,
            },
            Duration::from_nanos(1),
        )
        .await
        .unwrap();
    let runtime = f.engine.runtime();
    runtime
        .queue_writer(Some(writer.checkpoint().unwrap()))
        .await;
    let mut reader = BufferReader::new(
        f.store.clone(),
        config.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    let first = reader.next_txn().await.unwrap().unwrap();
    assert_eq!(reader.checkpoint().next_mseq, 0);
    runtime
        .queue_applied(first.commit_position, reader.checkpoint().clone())
        .await;
    runtime.retention_tick().await.unwrap();
    assert!(
        f.store
            .get(&config.manifest_key(0))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(runtime.retention_status().await.unwrap().deleted_objects, 0);
}

#[tokio::test]
async fn retention_restart_does_not_regress_a_partial_applied_prefix() {
    let (f, writer, facts) = buffered().await;
    f.engine
        .enable_retention(
            f.store.clone(),
            f.config.clone(),
            Arc::new(Order),
            RetentionPolicy {
                retained_for_ms: 60_000,
                orphan_grace_ms: 0,
            },
            Duration::from_nanos(1),
        )
        .await
        .unwrap();
    let runtime = f.engine.runtime();
    runtime.queue_writer(Some(writer)).await;
    // A receipt inside the next segment can advance apply without advancing
    // the last complete reader checkpoint. Reconstructed conservative progress
    // must never overwrite that newer durable receipt prefix.
    runtime
        .queue_applied(facts[1].0.clone(), facts[0].1.clone())
        .await;
    runtime.retention_tick().await.unwrap();
    f.engine
        .enable_retention(
            f.store.clone(),
            f.config.clone(),
            Arc::new(Order),
            RetentionPolicy {
                retained_for_ms: 60_000,
                orphan_grace_ms: 0,
            },
            Duration::from_nanos(1),
        )
        .await
        .unwrap();
    runtime.retention_tick().await.unwrap();
    assert!(!runtime.retention_status().await.unwrap().recovery_required);
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_some()
    );
}
#[cfg(windows)]
#[tokio::test]
async fn retention_setup_storage_failure_is_visible_and_retried() {
    use std::os::windows::fs::OpenOptionsExt;
    let (f, writer, facts) = buffered().await;
    let runtime = f.engine.runtime();
    runtime.queue_writer(Some(writer)).await;
    runtime
        .queue_applied(facts[0].0.clone(), facts[0].1.clone())
        .await;
    runtime.retention_tick().await.unwrap();
    let state = f.journal.load(false).await.unwrap();
    let checkpoint = state
        .progress
        .iter()
        .find(|(key, _)| key.starts_with("buffer-gc:") && key.contains("\"reader\""))
        .unwrap()
        .1
        .1
        .clone();
    let path = f.root.join("journal").join(checkpoint);
    // Exclusive OS access is fault injection alongside the unchanged real
    // LocalFileSystem journal; the journal object remains present and intact.
    let held = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(path)
        .unwrap();
    f.engine
        .enable_retention(
            f.store.clone(),
            f.config.clone(),
            Arc::new(Order),
            RetentionPolicy {
                retained_for_ms: 0,
                orphan_grace_ms: 0,
            },
            Duration::from_nanos(1),
        )
        .await
        .unwrap();
    assert!(
        f.engine
            .runtime()
            .retention_status()
            .await
            .unwrap()
            .blocked_reason
            .is_some()
    );
    assert_eq!(
        f.engine.runtime().state().await.lifecycle.state,
        State::Copying
    );
    drop(held);
    f.engine.runtime().retention_tick().await.unwrap();
    assert!(
        f.engine
            .runtime()
            .retention_status()
            .await
            .unwrap()
            .blocked_reason
            .is_none()
    );
}
#[cfg(windows)]
#[tokio::test]
async fn retention_delayed_setup_cannot_adopt_replacement_epoch() {
    use std::os::windows::fs::OpenOptionsExt;
    let (f, writer, facts) = buffered().await;
    let runtime = f.engine.runtime();
    runtime.queue_writer(Some(writer)).await;
    runtime
        .queue_applied(facts[0].0.clone(), facts[0].1.clone())
        .await;
    runtime.retention_tick().await.unwrap();
    let state = f.journal.load(false).await.unwrap();
    let checkpoint = state
        .progress
        .iter()
        .find(|(key, _)| key.starts_with("buffer-gc:") && key.contains("\"reader\""))
        .unwrap()
        .1
        .1
        .clone();
    let held = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(f.root.join("journal").join(checkpoint))
        .unwrap();
    f.engine
        .enable_retention(
            f.store.clone(),
            f.config.clone(),
            Arc::new(Order),
            RetentionPolicy {
                retained_for_ms: 0,
                orphan_grace_ms: 0,
            },
            Duration::from_nanos(1),
        )
        .await
        .unwrap();
    drop(held);
    let mut replacement = Coordinator::observer(
        f.journal.clone(),
        Local::create(f.root.join("setup-replacement-local")).unwrap(),
        "setup-replacement",
    )
    .unwrap();
    replacement.acquire().await.unwrap();
    assert!(f.engine.runtime().retention_tick().await.is_err());
    assert_eq!(
        f.engine.runtime().state().await.lifecycle.state,
        State::RecoveryRequired
    );
}

#[cfg(windows)]
fn lock_journal_head(f: &Fixture) -> std::fs::File {
    use std::os::windows::fs::OpenOptionsExt;
    let seq = f.engine.runtime.snapshot().applied_seq;
    std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(
            f.root
                .join("journal")
                .join(f.journal.entry_key(seq).as_ref()),
        )
        .unwrap()
}
fn assert_journal_pause(error: coordinator::Error) {
    let coordinator::Error::Io(error) = error else {
        panic!("not typed journal pause: {error}")
    };
    assert_eq!(
        error.get_ref().unwrap().downcast_ref::<EnginePauseReason>(),
        Some(&EnginePauseReason::CoordinatorJournalUnavailable)
    );
}
#[cfg(windows)]
#[tokio::test]
async fn retention_revoked_journal_failure_pauses_then_fresh_epoch_resume_collects() {
    let (f, writer, facts) = buffered().await;
    let runtime = f.engine.runtime();
    runtime.queue_writer(Some(writer)).await;
    runtime
        .queue_applied(facts[1].0.clone(), facts[1].1.clone())
        .await;
    let before_epoch = runtime.state().await.epoch;
    let held = lock_journal_head(&f);
    assert_journal_pause(runtime.retention_tick().await.unwrap_err());
    assert_eq!(
        runtime.pause_reason(),
        Some(EnginePauseReason::CoordinatorJournalUnavailable)
    );
    assert!(!runtime.0.lock().await.is_owner());
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_some()
    );
    drop(held);
    let reached = f
        .engine
        .finish_journal_pause(StopAt::UntilPaused)
        .await
        .unwrap();
    assert_eq!(reached.state, State::Paused);
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::Paused
    );
    let pause_epoch = runtime.state().await.epoch;
    assert!(pause_epoch > before_epoch);
    f.engine.resume().await.unwrap();
    assert!(runtime.state().await.epoch > pause_epoch);
    assert_eq!(runtime.pause_reason(), None);
    runtime.retention_tick().await.unwrap();
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::Copying
    );
}
#[cfg(windows)]
#[tokio::test]
async fn retention_outage_returns_typed_pause_without_durable_write_then_resumes() {
    let (f, writer, facts) = buffered().await;
    let runtime = f.engine.runtime();
    runtime.queue_writer(Some(writer)).await;
    runtime
        .queue_applied(facts[1].0.clone(), facts[1].1.clone())
        .await;
    let seq = runtime.state().await.applied_seq;
    let held = lock_journal_head(&f);
    assert_journal_pause(runtime.retention_tick().await.unwrap_err());
    assert_journal_pause(
        f.engine
            .finish_journal_pause(StopAt::UntilPaused)
            .await
            .unwrap_err(),
    );
    assert_eq!(runtime.state().await.lifecycle.state, State::Paused);
    drop(held);
    let durable = f.journal.load(false).await.unwrap();
    assert_eq!(durable.lifecycle.state, State::Copying);
    assert_eq!(durable.applied_seq, seq);
    f.engine.resume().await.unwrap();
    runtime.retention_tick().await.unwrap();
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_none()
    );
}
#[cfg(windows)]
#[tokio::test]
async fn retention_outage_never_reacquires_replacement_authority() {
    let (f, _, _) = buffered().await;
    let held = lock_journal_head(&f);
    assert_journal_pause(f.engine.runtime.retention_tick().await.unwrap_err());
    drop(held);
    let mut replacement = Coordinator::observer(
        f.journal.clone(),
        Local::create(f.root.join("outage-replacement")).unwrap(),
        "replacement",
    )
    .unwrap();
    replacement.acquire().await.unwrap();
    let seq = replacement.state().applied_seq;
    assert!(matches!(
        f.engine.finish_journal_pause(StopAt::UntilPaused).await,
        Err(coordinator::Error::RecoveryRequired(_))
    ));
    assert_eq!(
        f.engine.runtime.state().await.lifecycle.state,
        State::RecoveryRequired
    );
    assert_eq!(f.journal.load(false).await.unwrap().applied_seq, seq);
}
