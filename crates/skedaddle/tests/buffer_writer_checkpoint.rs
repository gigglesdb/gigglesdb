use async_trait::async_trait;
use skedaddle::{
    buffer::{
        backends::{self, BackendReport, BufferStore, ListPage, ObjectBufferStore, PutOutcome},
        format::{ManifestEntry, manifest_key},
        writer::{BufferWriter, BufferedProgress, Error, WriterCheckpoint, WriterConfig},
    },
    event::{AssembledTxn, EventError, Position, PositionOrder, Redelivery, SourceIdentity, TxnId},
};
use std::{
    cmp::Ordering,
    fs::File,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "checkpoint-test"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
fn pos(n: u64) -> Position {
    Position::new("checkpoint-test", n.to_be_bytes().to_vec())
}
fn txn(n: u64) -> AssembledTxn {
    AssembledTxn::new(
        SourceIdentity {
            engine: "test".into(),
            system_id: "s".into(),
            lineage: "l".into(),
            db_name: "d".into(),
            slot: "s".into(),
        },
        TxnId(n.to_be_bytes().to_vec()),
        pos(n),
        Some(7),
        vec![],
    )
    .unwrap()
}
fn root() -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT")
            .expect("set SKEDADDLE_TEST_ROOT under J:/gigglesdb-work"),
    )
    .join(format!("p3f-writer-{}", uuid::Uuid::new_v4()));
    assert!(path.starts_with("J:/gigglesdb-work"));
    std::fs::create_dir_all(&path).unwrap();
    path
}
fn local() -> Arc<dyn BufferStore> {
    local_at(root())
}
fn local_at(path: PathBuf) -> Arc<dyn BufferStore> {
    Arc::new(ObjectBufferStore::local(path, Duration::from_secs(30)).unwrap())
}
fn config() -> WriterConfig {
    WriterConfig {
        prefix: "checkpoint".into(),
        migration_id: "migration".into(),
        stream: "stream".into(),
        epoch: 3,
        segment_bytes: 64 * 1024,
        segment_transactions: 1,
        segment_age: Duration::from_secs(1),
        max_transaction_bytes: 1024 * 1024,
    }
}
async fn resume(
    store: Arc<dyn BufferStore>,
    config: WriterConfig,
    checkpoint: WriterCheckpoint,
) -> Result<(BufferWriter, mpsc::Receiver<BufferedProgress>), Error> {
    let (tx, rx) = mpsc::channel();
    let writer =
        BufferWriter::open_from_checkpoint(store, config, checkpoint, Arc::new(Order), tx, root())
            .await?;
    Ok((writer, rx))
}
async fn open(store: Arc<dyn BufferStore>, config: WriterConfig) -> BufferWriter {
    resume(store, config, WriterCheckpoint::default())
        .await
        .unwrap()
        .0
}
async fn append(writer: &mut BufferWriter, n: u64) {
    assert_eq!(
        writer.append(txn(n), Instant::now()).await.unwrap(),
        Redelivery::New
    );
}
async fn retire(store: &dyn BufferStore, c: &WriterConfig, through: u64) {
    for seq in 0..through {
        let key = manifest_key(&c.prefix, &c.migration_id, &c.stream, seq);
        let entry =
            ManifestEntry::from_canonical_json(&store.get(&key).await.unwrap().unwrap()).unwrap();
        // Model acknowledged GC intents: the tests have already retained the
        // restart checkpoint and are exercising writer recovery, not GC policy.
        store.delete(&entry.segment_key).await.unwrap();
        store.delete(&key).await.unwrap();
        assert!(store.get(&key).await.unwrap().is_none());
        assert!(store.get(&entry.segment_key).await.unwrap().is_none());
    }
}

// Observes real local I/O unchanged. The optional process exit injects only the
// crash seam after an actual manifest put has returned durably from the backend.
struct Observed {
    inner: Arc<dyn BufferStore>,
    gets: Mutex<Vec<String>>,
    lists: Mutex<u64>,
    exit_after_manifest: bool,
}
impl Observed {
    fn new(inner: Arc<dyn BufferStore>) -> Self {
        Self {
            inner,
            gets: Mutex::new(vec![]),
            lists: Mutex::new(0),
            exit_after_manifest: false,
        }
    }
}
#[async_trait]
impl BufferStore for Observed {
    fn report(&self) -> BackendReport {
        self.inner.report()
    }
    async fn put_create_only(&self, key: &str, content: &[u8]) -> backends::Result<PutOutcome> {
        let outcome = self.inner.put_create_only(key, content).await?;
        if self.exit_after_manifest && key.contains("/manifest/") {
            std::process::exit(86);
        }
        Ok(outcome)
    }
    async fn put_file_create_only(&self, key: &str, file: File) -> backends::Result<PutOutcome> {
        self.inner.put_file_create_only(key, file).await
    }
    async fn get(&self, key: &str) -> backends::Result<Option<Vec<u8>>> {
        self.gets.lock().unwrap().push(key.into());
        self.inner.get(key).await
    }
    async fn get_into(
        &self,
        key: &str,
        output: &mut (dyn Write + Send),
    ) -> backends::Result<Option<backends::ObjectDigest>> {
        self.gets.lock().unwrap().push(key.into());
        self.inner.get_into(key, output).await
    }
    async fn list_prefix(
        &self,
        prefix: &str,
        token: Option<&str>,
        limit: usize,
    ) -> backends::Result<ListPage> {
        *self.lists.lock().unwrap() += 1;
        self.inner.list_prefix(prefix, token, limit).await
    }
    async fn delete(&self, key: &str) -> backends::Result<()> {
        self.inner.delete(key).await
    }
}

#[tokio::test]
async fn resume_after_gc_from_checkpoint() {
    let store = local();
    let c = config();
    let mut writer = open(store.clone(), c.clone()).await;
    for n in 1..=3 {
        append(&mut writer, n).await;
    }
    writer.retain_redelivery_from(pos(2)).unwrap();
    let checkpoint = writer.checkpoint().unwrap();
    assert_eq!(checkpoint.next_mseq, 3);
    assert_eq!(checkpoint.last_epoch, 3);
    assert_eq!(checkpoint.last_position, Some(pos(3)));
    assert_eq!(checkpoint.redelivery_from, Some(pos(2)));
    assert_eq!(checkpoint.redelivery.len(), 2);
    for n in 4..=5 {
        append(&mut writer, n).await;
    }
    drop(writer);
    retire(store.as_ref(), &c, checkpoint.next_mseq).await;
    let (mut writer, progress) = resume(store.clone(), c.clone(), checkpoint).await.unwrap();
    assert_eq!(writer.next_manifest_sequence(), 5);
    assert_eq!(progress.try_recv().unwrap().position, pos(5));
    assert_eq!(
        writer.append(txn(2), Instant::now()).await.unwrap(),
        Redelivery::Noop
    );
    assert_eq!(
        writer.append(txn(5), Instant::now()).await.unwrap(),
        Redelivery::Noop
    );
    let previous = writer.checkpoint().unwrap().prev_digest;
    append(&mut writer, 6).await;
    let last = ManifestEntry::from_canonical_json(
        &store
            .get(&manifest_key(&c.prefix, &c.migration_id, &c.stream, 5))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(last.prev_digest, previous);
    assert_eq!(last.first_pos, pos(6));
}

#[tokio::test]
async fn resume_counts_no_list() {
    let store = local();
    let c = config();
    let mut writer = open(store.clone(), c.clone()).await;
    for n in 1..=32 {
        append(&mut writer, n).await;
    }
    writer.retain_redelivery_from(pos(31)).unwrap();
    let checkpoint = writer.checkpoint().unwrap();
    for n in 33..=36 {
        append(&mut writer, n).await;
    }
    drop(writer);
    retire(store.as_ref(), &c, 32).await;
    let observed = Arc::new(Observed::new(store));
    let (writer, _) = resume(observed.clone(), c.clone(), checkpoint)
        .await
        .unwrap();
    let gets = observed.gets.lock().unwrap().clone();
    let expected: Vec<_> = (32..=36)
        .flat_map(|seq| {
            let manifest = manifest_key(&c.prefix, &c.migration_id, &c.stream, seq);
            if seq == 36 {
                vec![manifest]
            } else {
                vec![
                    manifest,
                    skedaddle::buffer::format::segment_key(
                        &c.prefix,
                        &c.migration_id,
                        &c.stream,
                        &pos(seq + 1),
                        &pos(seq + 1),
                        c.epoch,
                    ),
                ]
            }
        })
        .collect();
    assert_eq!(gets, expected); // Four manifests + four segments + one idle tail.
    assert_eq!(*observed.lists.lock().unwrap(), 0);
    assert_eq!(writer.checkpoint().unwrap().redelivery.len(), 6);
}

#[tokio::test]
async fn redelivery_below_coverage_is_recovery_required() {
    let store = local();
    let c = config();
    let mut writer = open(store.clone(), c.clone()).await;
    for n in 1..=3 {
        append(&mut writer, n).await;
    }
    writer.retain_redelivery_from(pos(2)).unwrap();
    let checkpoint = writer.checkpoint().unwrap();
    drop(writer);
    retire(store.as_ref(), &c, 3).await;
    let (mut writer, _) = resume(store, c, checkpoint).await.unwrap();
    assert_eq!(
        writer.append(txn(2), Instant::now()).await.unwrap(),
        Redelivery::Noop
    );
    let error = writer.append(txn(1), Instant::now()).await.unwrap_err();
    assert!(matches!(error, Error::RecoveryRequired(_)));
    assert!(error.to_string().contains("recovery-required"));
    assert!(writer.is_blocked());
    assert!(matches!(
        writer.append(txn(4), Instant::now()).await,
        Err(Error::Blocked)
    ));
}

#[tokio::test]
async fn retained_redelivery_conflict_is_detected_after_gc() {
    let store = local();
    let c = config();
    let mut writer = open(store.clone(), c.clone()).await;
    append(&mut writer, 1).await;
    let checkpoint = writer.checkpoint().unwrap();
    drop(writer);
    retire(store.as_ref(), &c, 1).await;
    let (mut writer, _) = resume(store, c, checkpoint).await.unwrap();
    let original = txn(1);
    let conflicting = AssembledTxn::new(
        original.source,
        original.txn,
        original.commit_position,
        Some(8),
        vec![],
    )
    .unwrap();
    assert!(matches!(
        writer.append(conflicting, Instant::now()).await,
        Err(Error::Event(EventError::RedeliveryConflict { .. }))
    ));
    assert_eq!(writer.next_manifest_sequence(), 1);
}

#[tokio::test]
async fn stale_epoch_rejected_after_checkpoint_resume() {
    let store = local();
    let c = config();
    let mut writer = open(store.clone(), c.clone()).await;
    append(&mut writer, 1).await;
    let checkpoint = writer.checkpoint().unwrap();
    drop(writer);
    retire(store.as_ref(), &c, 1).await;
    let mut stale = c.clone();
    stale.epoch = 2;
    assert!(matches!(
        resume(store.clone(), stale, checkpoint.clone()).await,
        Err(Error::SteppedDown {
            writer_epoch: 2,
            manifest_epoch: 3
        })
    ));
    let (mut stale, _) = resume(store.clone(), c.clone(), checkpoint.clone())
        .await
        .unwrap();
    let mut newer = c.clone();
    newer.epoch = 4;
    let (mut winner, _) = resume(store.clone(), newer, checkpoint.clone())
        .await
        .unwrap();
    append(&mut winner, 2).await;
    assert!(matches!(
        resume(store, c, checkpoint).await,
        Err(Error::SteppedDown {
            writer_epoch: 3,
            manifest_epoch: 4
        })
    ));
    assert!(matches!(
        stale.append(txn(3), Instant::now()).await,
        Err(Error::SteppedDown {
            writer_epoch: 3,
            manifest_epoch: 4
        })
    ));
    assert!(stale.is_blocked());
}

#[tokio::test]
async fn crash_between_manifest_put_and_checkpoint_persist_resumes_correctly() {
    const CHILD: &str = "SKEDADDLE_WRITER_CHECKPOINT_CRASH_ROOT";
    if let Some(path) = std::env::var_os(CHILD) {
        let mut observed = Observed::new(local_at(PathBuf::from(path)));
        observed.exit_after_manifest = true;
        let mut writer = open(Arc::new(observed), config()).await;
        append(&mut writer, 2).await;
        panic!("manifest put did not terminate the child");
    }
    let path = root();
    let store = local_at(path.clone());
    let c = config();
    let mut writer = open(store.clone(), c.clone()).await;
    append(&mut writer, 1).await;
    // This checkpoint remains with the coordinator while its writer child
    // crashes. No post-crash checkpoint state can cross the process boundary.
    let persisted = writer.checkpoint().unwrap();
    drop(writer);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "crash_between_manifest_put_and_checkpoint_persist_resumes_correctly",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(86));
    let (mut writer, progress) = resume(store, c, persisted).await.unwrap();
    assert_eq!(writer.next_manifest_sequence(), 2);
    assert_eq!(progress.try_recv().unwrap().position, pos(2));
    assert_eq!(
        writer.append(txn(2), Instant::now()).await.unwrap(),
        Redelivery::Noop
    );
    append(&mut writer, 3).await;
    assert_eq!(writer.next_manifest_sequence(), 3);
}

#[tokio::test]
async fn checkpoint_excludes_pending_transactions_and_compaction_is_monotonic() {
    let store = local();
    let mut c = config();
    c.segment_transactions = 2;
    let mut writer = open(store.clone(), c.clone()).await;
    append(&mut writer, 1).await;
    assert_eq!(writer.checkpoint().unwrap(), WriterCheckpoint::default());
    assert!(writer.retain_redelivery_from(pos(1)).is_err());
    append(&mut writer, 2).await;
    append(&mut writer, 3).await;
    writer.retain_redelivery_from(pos(2)).unwrap();
    assert!(writer.retain_redelivery_from(pos(1)).is_err());
    assert!(writer.retain_redelivery_from(pos(3)).is_err());
    let checkpoint = writer.checkpoint().unwrap();
    assert_eq!(checkpoint.next_mseq, 1);
    assert_eq!(checkpoint.redelivery.len(), 1);
    assert_eq!(checkpoint.redelivery[0].position, pos(2));
    drop(writer); // The pending third transaction was never durable.
    let (mut writer, _) = resume(store, c, checkpoint).await.unwrap();
    append(&mut writer, 3).await;
    assert_eq!(writer.pending_transactions(), 1);
}

#[tokio::test]
async fn malformed_checkpoints_and_unproven_old_positions_fail_closed() {
    let store = local();
    let c = config();
    let mut writer = open(store.clone(), c.clone()).await;
    append(&mut writer, 1).await;
    let checkpoint = writer.checkpoint().unwrap();
    drop(writer);
    let mut invalid = checkpoint.clone();
    invalid.next_mseq = 0;
    assert!(matches!(
        resume(store.clone(), c.clone(), invalid).await,
        Err(Error::Invalid(_))
    ));
    let mut invalid = checkpoint.clone();
    invalid.last_position = None;
    assert!(matches!(
        resume(store.clone(), c.clone(), invalid).await,
        Err(Error::Invalid(_))
    ));
    let mut invalid = checkpoint.clone();
    invalid.redelivery.push(invalid.redelivery[0].clone());
    assert!(matches!(
        resume(store.clone(), c.clone(), invalid).await,
        Err(Error::Invalid(_))
    ));
    let mut invalid = checkpoint.clone();
    invalid.redelivery_from = Some(pos(2));
    assert!(matches!(
        resume(store.clone(), c.clone(), invalid).await,
        Err(Error::Invalid(_))
    ));
    let mut incomplete = checkpoint.clone();
    incomplete.redelivery.clear();
    incomplete.redelivery_from = Some(pos(1));
    let (mut writer, _) = resume(store.clone(), c.clone(), incomplete).await.unwrap();
    assert!(matches!(
        writer.append(txn(1), Instant::now()).await,
        Err(Error::RecoveryRequired(_))
    ));
    let mut wrong_digest = checkpoint;
    wrong_digest.prev_digest[0] ^= 1;
    let mut writer = open(store.clone(), c.clone()).await;
    append(&mut writer, 2).await;
    drop(writer);
    assert!(matches!(
        resume(store, c, wrong_digest).await,
        Err(Error::Invalid("manifest chain"))
    ));
}

#[tokio::test]
async fn each_zero_seal_threshold_is_rejected_independently() {
    for threshold in ["bytes", "transactions", "age"] {
        let mut c = config();
        match threshold {
            "bytes" => c.segment_bytes = 0,
            "transactions" => c.segment_transactions = 0,
            "age" => c.segment_age = Duration::ZERO,
            _ => unreachable!(),
        }
        assert!(
            matches!(
                resume(local(), c, WriterCheckpoint::default()).await,
                Err(Error::Invalid("zero seal threshold"))
            ),
            "accepted zero {threshold} threshold"
        );
    }
}

#[tokio::test]
async fn checkpoint_without_a_recovery_suffix_publishes_the_exact_durable_progress() {
    let store = local();
    let c = config();
    let mut writer = open(store.clone(), c.clone()).await;
    for n in 1..=3 {
        append(&mut writer, n).await;
    }
    let checkpoint = writer.checkpoint().unwrap();
    let expected = BufferedProgress {
        position: pos(3),
        mseq: 2,
        epoch: c.epoch,
        manifest_digest: checkpoint.prev_digest,
    };
    drop(writer);
    let (writer, progress) = resume(store, c, checkpoint.clone()).await.unwrap();
    assert_eq!(progress.try_recv().unwrap(), expected);
    assert!(progress.try_recv().is_err());
    assert_eq!(writer.next_manifest_sequence(), checkpoint.next_mseq);
}

#[tokio::test]
async fn chunk_rows_checkpoint_retains_kind_and_original_manifest_evidence() {
    let store = local();
    let c = config();
    let rows = txn(1); // Empty snapshot batches still require a typed envelope.
    let mut writer = open(store.clone(), c.clone()).await;
    let (_, reference) = writer
        .append_chunk_rows(rows.clone(), Instant::now())
        .await
        .unwrap();
    let checkpoint = writer.checkpoint().unwrap();
    assert!(checkpoint.redelivery[0].chunk_rows);
    assert_eq!(checkpoint.redelivery[0].manifest.as_ref(), Some(&reference));
    // Recover both a checkpoint with no suffix and one with a durable suffix.
    append(&mut writer, 2).await;
    drop(writer);
    let (mut recovered, _) = resume(store.clone(), c.clone(), checkpoint.clone())
        .await
        .unwrap();
    assert_eq!(
        recovered
            .append_chunk_rows(rows.clone(), Instant::now())
            .await
            .unwrap(),
        (Redelivery::Noop, reference.clone())
    );
    assert!(matches!(
        recovered.append(rows.clone(), Instant::now()).await,
        Err(Error::Event(EventError::RedeliveryConflict { .. }))
    ));
    let latest = recovered.checkpoint().unwrap();
    drop(recovered);
    let (mut recovered, _) = resume(store.clone(), c.clone(), latest).await.unwrap();
    assert_eq!(
        recovered
            .append_chunk_rows(rows, Instant::now())
            .await
            .unwrap(),
        (Redelivery::Noop, reference)
    );
    let mut invalid = checkpoint;
    invalid.redelivery[0].manifest = None;
    assert!(matches!(
        resume(store, c, invalid).await,
        Err(Error::Invalid("writer checkpoint redelivery manifest"))
    ));
}

#[tokio::test]
async fn checkpoint_rejects_obsolete_missing_or_inconsistent_record_boundary() {
    let store = local();
    let c = config();
    let mut writer = open(store, c.clone()).await;
    append(&mut writer, 5).await;
    let checkpoint = writer.checkpoint().unwrap();
    for mode in 0..3 {
        let mut invalid = checkpoint.clone();
        match mode {
            0 => invalid.version = 1,
            1 => invalid.last_record = None,
            _ => invalid.last_record.as_mut().unwrap().source.lineage = "foreign".into(),
        }
        assert!(matches!(
            resume(local(), c.clone(), invalid).await,
            Err(Error::Invalid(_))
        ));
    }
}

#[tokio::test]
async fn process_exit_after_h_recovers_and_admits_its_single_chunk_rows() {
    const CHILD: &str = "SKEDADDLE_EQUAL_H_EXIT_CHILD";
    const PATH: &str = "SKEDADDLE_EQUAL_H_EXIT_ROOT";
    if std::env::var_os(CHILD).is_some() {
        let store = local_at(PathBuf::from(std::env::var_os(PATH).unwrap()));
        let mut observed = Observed::new(store);
        observed.exit_after_manifest = true;
        let mut writer = open(Arc::new(observed), config()).await;
        append(&mut writer, 5).await;
        panic!("child must exit after publishing H");
    }
    let path = root();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "process_exit_after_h_recovers_and_admits_its_single_chunk_rows",
            "--exact",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env(PATH, &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(86));
    let store = local_at(path);
    let mut writer = open(store, config()).await;
    assert_eq!(writer.checkpoint().unwrap().last_position, Some(pos(5)));
    let chunk = AssembledTxn::new(txn(5).source, TxnId(vec![99]), pos(5), Some(7), vec![]).unwrap();
    let (_, reference) = writer
        .append_chunk_rows(chunk.clone(), Instant::now())
        .await
        .unwrap();
    assert_eq!(reference.mseq, 1);
    let mut checkpoint = writer.checkpoint().unwrap();
    checkpoint.redelivery.clear();
    drop(writer);
    // A consumed slot survives even complete retirement of identity history.
    let (mut writer, _) = resume(local(), config(), checkpoint).await.unwrap();
    assert!(
        writer
            .append_chunk_rows(chunk, Instant::now())
            .await
            .is_err()
    );
    assert!(writer.is_blocked());
}

#[tokio::test]
async fn redelivery_export_is_stable_across_permuted_checkpoint_resume() {
    let store = local();
    let c = config();
    let mut writer = open(store.clone(), c.clone()).await;
    append(&mut writer, 1).await;
    append(&mut writer, 2).await;
    let canonical = writer.checkpoint().unwrap();
    assert_eq!(canonical.redelivery.len(), 2);
    drop(writer);
    let mut legacy = canonical.clone();
    legacy.redelivery.reverse();
    for _ in 0..8 {
        let (writer, _) = resume(store.clone(), c.clone(), legacy.clone())
            .await
            .unwrap();
        assert_eq!(writer.checkpoint().unwrap(), canonical);
    }
}
