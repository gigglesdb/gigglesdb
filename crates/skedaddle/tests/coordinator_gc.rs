//! Real local-journal/local-buffer qualification of coordinator GC authority.
#[path = "support/root.rs"]
mod test_root;

use std::{
    cmp::Ordering,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use skedaddle::{
    buffer::{
        backends::{BufferStore, ObjectBufferStore},
        format::Limits,
        gc::{AppliedSegment, GcIntent, GcJournal, GcReason, RetentionGc, RetentionPolicy},
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig, VerifiedManifest},
        writer::{BufferWriter, WriterCheckpoint, WriterConfig},
    },
    coordinator::{
        epoch::Coordinator,
        gc_authority::CoordinatorGcJournal,
        journal::{self, Journal},
        local::Local,
    },
    event::{AssembledTxn, EventError, Position, PositionOrder, SourceIdentity, TxnId},
};
use std::sync::mpsc;
use tokio::sync::Mutex;

#[test]
fn unsupported_coordinator_format_blocks_gc_with_recovery_required() {
    use skedaddle::{
        buffer::reader,
        coordinator::{Error, gc_authority::coordinator_gc_error},
    };
    for found in [None, Some(1), Some(3)] {
        assert!(matches!(
            coordinator_gc_error(Error::UnsupportedFormatVersion {
                format: "journal-entry",
                found,
                expected: 2,
            }),
            reader::Error::RecoveryRequired(_)
        ));
    }
}

struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "coordinator-gc-test"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
fn pos(n: u64) -> Position {
    Position::new(Order.domain(), n.to_be_bytes().to_vec())
}
fn txn(n: u64) -> AssembledTxn {
    AssembledTxn::new(
        SourceIdentity {
            engine: "test".into(),
            system_id: "system".into(),
            lineage: "lineage".into(),
            db_name: "db".into(),
            stream: "slot".into(),
        },
        TxnId(n.to_be_bytes().to_vec()),
        pos(n),
        None,
        vec![],
    )
    .unwrap()
}
fn config() -> ReaderConfig {
    ReaderConfig {
        prefix: "r2".into(),
        migration_id: "migration".into(),
        stream: "stream".into(),
        limits: Limits::default(),
        required_next_mseq: 0,
    }
}
fn fresh_root() -> PathBuf {
    let root = crate::test_root::scratch_root();
    crate::test_root::assert_under_work_root(&root);
    let root = root.join(format!("coordinator-gc-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(root.join("buffer")).unwrap();
    root
}
async fn owner(root: &std::path::Path) -> Arc<Mutex<Coordinator>> {
    let objects = root.join("journal");
    std::fs::create_dir_all(&objects).unwrap();
    let journal = Journal::new(
        Arc::new(journal::local_store(objects).unwrap()),
        "r2",
        "migration",
        Duration::from_secs(5),
    )
    .unwrap();
    let local = Local::create(root.join(format!("local-{}", uuid::Uuid::new_v4()))).unwrap();
    let mut c = Coordinator::observer(journal, local, "gc-owner").unwrap();
    c.acquire().await.unwrap();
    Arc::new(Mutex::new(c))
}
async fn authority(root: &std::path::Path, c: Arc<Mutex<Coordinator>>) -> CoordinatorGcJournal {
    CoordinatorGcJournal::new(
        c,
        Arc::new(ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(5)).unwrap()),
        config(),
        Arc::new(Order),
    )
    .await
    .unwrap()
}
struct Fixture {
    root: PathBuf,
    store: Arc<dyn BufferStore>,
    c: Arc<Mutex<Coordinator>>,
    authority: CoordinatorGcJournal,
    verified: VerifiedManifest,
    reader: ReaderCheckpoint,
    writer: WriterCheckpoint,
}
impl Fixture {
    async fn new() -> Self {
        let root = fresh_root();
        let store: Arc<dyn BufferStore> = Arc::new(
            ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(5)).unwrap(),
        );
        let (tx, _rx) = mpsc::channel();
        let mut writer = BufferWriter::open(
            store.clone(),
            WriterConfig {
                prefix: "r2".into(),
                migration_id: "migration".into(),
                stream: "stream".into(),
                epoch: 1,
                segment_bytes: 1024 * 1024,
                segment_transactions: 2,
                segment_age: Duration::from_secs(60),
                max_transaction_bytes: 1024 * 1024,
            },
            Arc::new(Order),
            tx,
        )
        .await
        .unwrap();
        writer.append(txn(10), Instant::now()).await.unwrap();
        writer.append(txn(20), Instant::now()).await.unwrap();
        writer.append(txn(30), Instant::now()).await.unwrap();
        writer.seal().await.unwrap();
        let writer = writer.checkpoint().unwrap();
        let mut reader = BufferReader::new(
            store.clone(),
            config(),
            ReaderCheckpoint::default(),
            Arc::new(Order),
        )
        .unwrap();
        assert_eq!(reader.next_txn().await.unwrap().unwrap(), txn(10));
        let verified = reader.last_verified().unwrap().clone();
        // A proof exists after the first read, but its two-record segment is
        // still only partially consumed. This is F2's previously missing cut.
        assert_eq!(verified.entry().txn_count, 2);
        assert_eq!(reader.checkpoint().next_mseq, 0);
        assert_eq!(reader.next_txn().await.unwrap().unwrap(), txn(20));
        assert_eq!(reader.next_txn().await.unwrap().unwrap(), txn(30));
        let reader = reader.checkpoint().clone();
        let c = owner(&root).await;
        let authority = authority(&root, c.clone()).await;
        Self {
            root,
            store,
            c,
            authority,
            verified,
            reader,
            writer,
        }
    }
    fn candidate(&self) -> AppliedSegment {
        AppliedSegment::new(self.verified.clone(), 1, 1)
    }
    fn gc(&self) -> RetentionGc {
        RetentionGc::new(
            self.store.clone(),
            Arc::new(Order),
            RetentionPolicy {
                retained_for_ms: 0,
                orphan_grace_ms: 0,
            },
        )
    }
    fn intent(&self) -> GcIntent {
        let entry = self.verified.entry();
        GcIntent {
            key: entry.segment_key.clone(),
            content_digest: entry.segment_digest,
            bytes: entry.bytes,
            reason: GcReason::AppliedAndVerified {
                mseq: entry.mseq,
                manifest_digest: self.verified.digest(),
                applied_at_ms: 1,
                verified_at_ms: 1,
            },
        }
    }
    async fn checkpoints(&self) {
        self.authority
            .persist_reader_checkpoint(&self.reader)
            .await
            .unwrap();
        self.authority
            .persist_writer_checkpoint(&self.writer)
            .await
            .unwrap();
    }
    async fn eligible(&self) {
        self.authority
            .persist_applied_through(&pos(30))
            .await
            .unwrap();
        self.checkpoints().await;
    }
    async fn refused(&self) {
        let before = self.c.lock().await.state().applied_seq;
        assert!(
            self.gc()
                .collect_applied(&self.candidate(), 2, &self.authority)
                .await
                .is_err()
        );
        assert_eq!(
            self.c.lock().await.state().applied_seq,
            before,
            "refusal must not journal an intent"
        );
        assert!(
            self.store
                .get(&self.verified.entry().segment_key)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            self.store
                .get(&self.verified.manifest_key())
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test]
async fn unapplied_segment_is_refused() {
    let f = Fixture::new().await;
    f.checkpoints().await;
    f.refused().await;
}
#[tokio::test]
async fn partially_applied_multirecord_segment_is_refused() {
    let f = Fixture::new().await;
    f.checkpoints().await;
    f.authority.persist_applied_through(&pos(10)).await.unwrap();
    f.refused().await;
    // Equality also cannot prove a contiguous prefix beyond the last record.
    f.authority.persist_applied_through(&pos(20)).await.unwrap();
    f.refused().await;
}
#[tokio::test]
async fn reader_checkpoint_that_is_not_durable_is_refused() {
    let f = Fixture::new().await;
    assert!(f.reader.next_mseq > f.verified.entry().mseq);
    f.authority.persist_applied_through(&pos(30)).await.unwrap();
    f.authority
        .persist_writer_checkpoint(&f.writer)
        .await
        .unwrap();
    f.authority
        .persist_reader_checkpoint(&ReaderCheckpoint::default())
        .await
        .unwrap();
    f.refused().await;
    f.authority
        .persist_reader_checkpoint(&f.reader)
        .await
        .unwrap();
    assert_eq!(
        f.gc()
            .collect_applied(&f.candidate(), 2, &f.authority)
            .await
            .unwrap()
            .deleted_objects,
        2
    );
}
#[tokio::test]
async fn writer_checkpoint_that_is_not_durable_is_refused() {
    let f = Fixture::new().await;
    assert!(f.writer.next_mseq > f.verified.entry().mseq);
    f.authority.persist_applied_through(&pos(30)).await.unwrap();
    f.authority
        .persist_reader_checkpoint(&f.reader)
        .await
        .unwrap();
    f.authority
        .persist_writer_checkpoint(&WriterCheckpoint::default())
        .await
        .unwrap();
    f.refused().await;
    f.authority
        .persist_writer_checkpoint(&f.writer)
        .await
        .unwrap();
    assert_eq!(
        f.gc()
            .collect_applied(&f.candidate(), 2, &f.authority)
            .await
            .unwrap()
            .deleted_objects,
        2
    );
}
#[tokio::test]
async fn stale_epoch_is_refused_even_for_previously_journaled_intent() {
    let f = Fixture::new().await;
    f.eligible().await;
    f.authority.record(&f.intent()).await.unwrap();
    let _new_owner = owner(&f.root).await;
    assert!(matches!(
        f.authority.record(&f.intent()).await.unwrap_err(),
        skedaddle::buffer::reader::Error::RecoveryRequired(_)
    ));
    assert!(f.authority.persist_applied_through(&pos(40)).await.is_err());
    f.refused().await;
}
#[tokio::test]
async fn fully_eligible_segment_is_journaled_then_deleted() {
    let f = Fixture::new().await;
    f.eligible().await;
    assert_eq!(
        f.authority.reader_checkpoint().await.unwrap(),
        Some(f.reader.clone())
    );
    assert_eq!(
        f.authority.writer_checkpoint().await.unwrap(),
        Some(f.writer.clone())
    );
    let before = f.c.lock().await.state().applied_seq;
    f.authority.record(&f.intent()).await.unwrap();
    assert_eq!(f.c.lock().await.state().applied_seq, before + 1);
    assert!(
        f.store.get(&f.intent().key).await.unwrap().is_some(),
        "journaling must precede deletion"
    );
    f.authority.record(&f.intent()).await.unwrap();
    assert_eq!(
        f.c.lock().await.state().applied_seq,
        before + 1,
        "idempotent append"
    );
    assert_eq!(
        f.gc()
            .collect_applied(&f.candidate(), 2, &f.authority)
            .await
            .unwrap()
            .deleted_objects,
        2
    );
    assert!(f.store.get(&f.intent().key).await.unwrap().is_none());
    assert!(
        f.store
            .get(&f.verified.manifest_key())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(f.c.lock().await.state().applied_seq, before + 2);
    // A retry after both object deletions must consult durable intent history.
    f.authority.record(&f.intent()).await.unwrap();
    assert_eq!(f.c.lock().await.state().applied_seq, before + 2);
    let mut conflicting = f.intent();
    conflicting.bytes += 1;
    assert!(matches!(
        f.authority.record(&conflicting).await.unwrap_err(),
        skedaddle::buffer::reader::Error::RecoveryRequired(_)
    ));
}
#[tokio::test]
async fn checkpoint_and_apply_regressions_are_refused() {
    let f = Fixture::new().await;
    f.eligible().await;
    let mut c = f.c.lock().await;
    let reserved = c
        .state()
        .progress
        .keys()
        .find(|key| key.starts_with("buffer-gc:"))
        .unwrap()
        .clone();
    assert!(
        c.commit(skedaddle::coordinator::Transition::Progress {
            stream: reserved,
            marker: "forged".into(),
            position: "unpersisted".into(),
        })
        .await
        .is_err()
    );
    drop(c);
    assert!(
        f.authority
            .persist_reader_checkpoint(&ReaderCheckpoint::default())
            .await
            .is_err()
    );
    assert!(
        f.authority
            .persist_writer_checkpoint(&WriterCheckpoint::default())
            .await
            .is_err()
    );
    assert!(f.authority.persist_applied_through(&pos(10)).await.is_err());
    for version in [1, 2] {
        let mut old_version = f.writer.clone();
        old_version.version = version;
        assert!(
            f.authority
                .persist_writer_checkpoint(&old_version)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn crash_between_journaling_and_deletion_resumes_idempotently() {
    if let Some(root) = std::env::var_os("SKEDADDLE_GC_CRASH_CHILD") {
        let root = PathBuf::from(root);
        let c = owner(&root).await;
        let a = authority(&root, c.clone()).await;
        let store: Arc<dyn BufferStore> = Arc::new(
            ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(5)).unwrap(),
        );
        let mut reader = BufferReader::new(
            store,
            config(),
            ReaderCheckpoint::default(),
            Arc::new(Order),
        )
        .unwrap();
        reader.next_txn().await.unwrap().unwrap();
        let verified = reader.last_verified().unwrap();
        let entry = verified.entry();
        let intent = GcIntent {
            key: entry.segment_key.clone(),
            content_digest: entry.segment_digest,
            bytes: entry.bytes,
            reason: GcReason::AppliedAndVerified {
                mseq: entry.mseq,
                manifest_digest: verified.digest(),
                applied_at_ms: 1,
                verified_at_ms: 1,
            },
        };
        a.record(&intent).await.unwrap();
        // A real process exit leaves an acknowledged intent and both objects.
        std::process::exit(86);
    }
    let mut f = Fixture::new().await;
    f.eligible().await;
    f.c.lock().await.checkpoint().await.unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "crash_between_journaling_and_deletion_resumes_idempotently",
            "--nocapture",
        ])
        .env("SKEDADDLE_GC_CRASH_CHILD", &f.root)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(86));
    assert!(f.store.get(&f.intent().key).await.unwrap().is_some());
    f.c = owner(&f.root).await;
    f.authority = authority(&f.root, f.c.clone()).await;
    assert_eq!(
        f.authority.writer_checkpoint().await.unwrap(),
        Some(f.writer.clone())
    );
    assert_eq!(
        f.authority.reader_checkpoint().await.unwrap(),
        Some(f.reader.clone())
    );
    let before = f.c.lock().await.state().applied_seq;
    f.authority.record(&f.intent()).await.unwrap();
    assert_eq!(f.c.lock().await.state().applied_seq, before);
    assert_eq!(
        f.gc()
            .collect_applied(&f.candidate(), 2, &f.authority)
            .await
            .unwrap()
            .deleted_objects,
        2
    );
    assert_eq!(f.c.lock().await.state().applied_seq, before + 1);
}

// Fault injection surrounds the same real local buffer/journal path exercised
// above. Only manifest access by the authority fails; GC itself reads normally.
struct ManifestReadFailure(Arc<dyn BufferStore>);
#[async_trait::async_trait]
impl BufferStore for ManifestReadFailure {
    fn report(&self) -> skedaddle::buffer::backends::BackendReport {
        self.0.report()
    }
    async fn put_create_only(
        &self,
        key: &str,
        content: &[u8],
    ) -> skedaddle::buffer::backends::Result<skedaddle::buffer::backends::PutOutcome> {
        self.0.put_create_only(key, content).await
    }
    async fn get(&self, _key: &str) -> skedaddle::buffer::backends::Result<Option<Vec<u8>>> {
        Err(skedaddle::buffer::backends::Error::LocalIo)
    }
    async fn list_prefix(
        &self,
        prefix: &str,
        token: Option<&str>,
        limit: usize,
    ) -> skedaddle::buffer::backends::Result<skedaddle::buffer::backends::ListPage> {
        self.0.list_prefix(prefix, token, limit).await
    }
    async fn delete(&self, key: &str) -> skedaddle::buffer::backends::Result<()> {
        self.0.delete(key).await
    }
}
#[tokio::test]
async fn manifest_read_failure_is_operational_through_real_retention_path() {
    let f = Fixture::new().await;
    f.eligible().await;
    let authority = CoordinatorGcJournal::new(
        f.c.clone(),
        Arc::new(ManifestReadFailure(f.store.clone())),
        config(),
        Arc::new(Order),
    )
    .await
    .unwrap();
    let before = f.c.lock().await.state().applied_seq;
    let error = f
        .gc()
        .collect_applied(&f.candidate(), 2, &authority)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        skedaddle::buffer::reader::Error::Store(skedaddle::buffer::backends::Error::LocalIo)
    ));
    assert_eq!(f.c.lock().await.state().applied_seq, before);
    assert!(f.store.get(&f.intent().key).await.unwrap().is_some());
    // A successful exact retry follows the real durable journal-before-delete path.
    assert_eq!(
        f.gc()
            .collect_applied(&f.candidate(), 2, &f.authority)
            .await
            .unwrap()
            .deleted_objects,
        2
    );
}
#[tokio::test]
async fn insufficient_proof_is_fatal_through_real_retention_path() {
    let f = Fixture::new().await;
    assert!(matches!(
        f.gc()
            .collect_applied(&f.candidate(), 2, &f.authority)
            .await
            .unwrap_err(),
        skedaddle::buffer::reader::Error::RecoveryRequired(_)
    ));
    assert!(f.store.get(&f.intent().key).await.unwrap().is_some());
}
