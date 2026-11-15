use async_trait::async_trait;
use skedaddle::{
    buffer::{
        backends::{BufferStore, ObjectBufferStore},
        format::{
            Limits, ManifestEntry, SegmentDecoder, SegmentEncoder, SegmentHeader, SegmentRecord,
            segment_key,
        },
        gc::{AppliedSegment, GcIntent, GcJournal, RetentionGc, RetentionPolicy},
        reader::{
            BufferReader, Error, ManifestRange, OrphanFence, ReaderCheckpoint, ReaderConfig,
            list_orphan_segments,
        },
    },
    event::{
        AssembledTxn, ContentDigest, EventError, Position, PositionOrder, SourceIdentity, TxnId,
    },
};
use std::{
    cmp::Ordering,
    fs::{File, OpenOptions},
    io::{Cursor, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "gc-test"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
fn pos(n: u64) -> Position {
    Position::new("gc-test", n.to_be_bytes().to_vec())
}
fn root() -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT")
            .expect("set SKEDADDLE_TEST_ROOT under J:/gigglesdb-work"),
    )
    .join(format!("p3-gc-{}", uuid::Uuid::new_v4()));
    assert!(path.starts_with("J:/gigglesdb-work"));
    std::fs::create_dir_all(&path).unwrap();
    path
}
fn local() -> Arc<dyn BufferStore> {
    Arc::new(ObjectBufferStore::local(root(), Duration::from_secs(30)).unwrap())
}
fn config() -> ReaderConfig {
    ReaderConfig {
        prefix: format!("gc/{}", uuid::Uuid::new_v4()),
        migration_id: "m".into(),
        stream: "s".into(),
        limits: Limits::default(),
        required_next_mseq: 0,
    }
}
fn encoded(c: &ReaderConfig, n: u64) -> (Vec<u8>, ManifestEntry) {
    let txn = AssembledTxn::new(
        SourceIdentity {
            engine: "test".into(),
            system_id: "s".into(),
            lineage: "l".into(),
            db_name: "d".into(),
            stream: "s".into(),
        },
        TxnId(n.to_be_bytes().to_vec()),
        pos(n),
        Some(1),
        vec![],
    )
    .unwrap();
    let mut encoder = SegmentEncoder::new(
        Vec::new(),
        SegmentHeader {
            migration_id: c.migration_id.clone(),
            stream: c.stream.clone(),
            epoch: 1,
            first_pos: pos(n),
            last_pos: pos(n),
            record_count: 1,
        },
        c.limits,
    )
    .unwrap();
    encoder
        .write_record(&SegmentRecord::Transaction(txn))
        .unwrap();
    let (bytes, summary) = encoder.finish().unwrap();
    (
        bytes,
        ManifestEntry {
            mseq: n - 1,
            epoch: 1,
            prev_digest: [0; 32],
            segment_key: segment_key(&c.prefix, &c.migration_id, &c.stream, &pos(n), &pos(n), 1),
            segment_digest: summary.digest,
            first_pos: pos(n),
            last_pos: pos(n),
            txn_count: 1,
            bytes: summary.bytes,
        },
    )
}
async fn publish(
    store: &dyn BufferStore,
    c: &ReaderConfig,
    n: u64,
    prev: ContentDigest,
) -> ManifestEntry {
    let (bytes, mut entry) = encoded(c, n);
    entry.prev_digest = prev;
    store
        .put_create_only(&entry.segment_key, &bytes)
        .await
        .unwrap();
    store
        .put_create_only(&c.manifest_key(n - 1), &entry.canonical_json().unwrap())
        .await
        .unwrap();
    entry
}
// Real durable journal implementation for tests. It verifies that every object
// still exists when its intent is appended and fsync'd, before GC may delete it.
struct FileJournal {
    file: Mutex<File>,
    intents: Mutex<Vec<GcIntent>>,
    store: Arc<dyn BufferStore>,
    fail_at: Option<usize>,
}
impl FileJournal {
    fn new(store: Arc<dyn BufferStore>, fail_at: Option<usize>) -> Self {
        Self {
            file: Mutex::new(
                OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(root().join("gc-journal.jsonl"))
                    .unwrap(),
            ),
            intents: Mutex::new(vec![]),
            store,
            fail_at,
        }
    }
}
#[async_trait]
impl GcJournal for FileJournal {
    async fn record(&self, intent: &GcIntent) -> skedaddle::buffer::reader::Result<()> {
        assert!(
            self.store.get(&intent.key).await?.is_some(),
            "delete preceded journal"
        );
        let mut intents = self.intents.lock().unwrap();
        if self.fail_at == Some(intents.len()) {
            return Err(Error::Invalid("injected journal failure"));
        }
        let mut file = self.file.lock().unwrap();
        serde_json::to_writer(&mut *file, intent).unwrap();
        file.write_all(b"\n").unwrap();
        file.sync_all().unwrap();
        intents.push(intent.clone());
        Ok(())
    }
}
async fn total_bytes(store: &dyn BufferStore, prefix: &str) -> u64 {
    let mut total = 0;
    let mut token = None;
    loop {
        let page = store
            .list_prefix(prefix, token.as_deref(), 1000)
            .await
            .unwrap();
        total += page.objects.iter().map(|o| o.bytes).sum::<u64>();
        token = page.next;
        if token.is_none() {
            return total;
        }
    }
}
async fn soak(store: Arc<dyn BufferStore>, iterations: u64) {
    let c = config();
    let gc = RetentionGc::new(
        store.clone(),
        Arc::new(Order),
        RetentionPolicy {
            retained_for_ms: 3,
            orphan_grace_ms: 5,
        },
    );
    let journal = FileJournal::new(store.clone(), None);
    // This corrupt, unmanifested object must survive every cycle, irrespective
    // of age. There is intentionally no way to manufacture a VerifiedManifest.
    let unverified = format!("{}unverified.seg", c.segment_prefix());
    store
        .put_create_only(&unverified, b"bad segment")
        .await
        .unwrap();
    let mut reader = BufferReader::new(
        store.clone(),
        c.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    let mut queue = std::collections::VecDeque::new();
    let mut prev = [0; 32];
    let mut peak = 0;
    for n in 1..=iterations {
        let entry = publish(store.as_ref(), &c, n, prev).await;
        prev = entry.digest().unwrap();
        assert!(reader.next_txn().await.unwrap().is_some());
        let now = n;
        let applied = AppliedSegment::new(reader.last_verified().unwrap().clone(), now, now);
        assert_eq!(
            gc.collect_applied(&applied, now, &journal)
                .await
                .unwrap()
                .deleted_objects,
            0
        );
        queue.push_back((now, applied));
        while queue.front().is_some_and(|(at, _)| now - *at >= 3) {
            let (_, candidate) = queue.pop_front().unwrap();
            let outcome = gc.collect_applied(&candidate, now, &journal).await.unwrap();
            assert_eq!(outcome.deleted_objects, 2);
        }
        // Actual backend bytes (including retained manifest JSON), not a model.
        let bytes = total_bytes(store.as_ref(), &c.prefix).await;
        peak = peak.max(bytes);
        assert!(bytes < 8_000, "space grows at iteration {n}: {bytes}");
        if n % 100 == 0 {
            assert_eq!(
                store.get(&unverified).await.unwrap().as_deref(),
                Some(b"bad segment".as_slice())
            );
        }
    }
    assert!(reader.next_txn().await.unwrap().is_none());
    assert_eq!(
        journal.intents.lock().unwrap().len() as u64,
        2 * (iterations - 3)
    );
    eprintln!(
        "GC long loop iterations={iterations}, peak_live_backend_bytes={peak}, unverified object preserved"
    );
}
#[tokio::test]
async fn gc_never_deletes_unverified_and_keeps_space_bounded() {
    soak(local(), 256).await;
}

#[tokio::test]
async fn retention_requires_complete_apply_verification_and_journal_before_each_delete() {
    retention_journal_matrix(local()).await;
}

async fn retention_journal_matrix(store: Arc<dyn BufferStore>) {
    let c = config();
    let entry = publish(store.as_ref(), &c, 1, [0; 32]).await;
    let mut reader = BufferReader::new(
        store.clone(),
        c.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    reader.next_txn().await.unwrap();
    let candidate = AppliedSegment::new(reader.last_verified().unwrap().clone(), 10, 20);
    let gc = RetentionGc::new(
        store.clone(),
        Arc::new(Order),
        RetentionPolicy {
            retained_for_ms: 10,
            orphan_grace_ms: 10,
        },
    );
    let reject = FileJournal::new(store.clone(), Some(0));
    for now in [0, 19, 20, 29] {
        assert_eq!(
            gc.collect_applied(&candidate, now, &reject)
                .await
                .unwrap()
                .deleted_objects,
            0
        );
    }
    assert!(gc.collect_applied(&candidate, 30, &reject).await.is_err());
    assert!(store.get(&entry.segment_key).await.unwrap().is_some());
    assert!(store.get(&c.manifest_key(0)).await.unwrap().is_some());
    let partial = FileJournal::new(store.clone(), Some(1));
    assert!(gc.collect_applied(&candidate, 30, &partial).await.is_err());
    assert!(store.get(&entry.segment_key).await.unwrap().is_none());
    assert!(store.get(&c.manifest_key(0)).await.unwrap().is_some());
    let journal = FileJournal::new(store.clone(), None);
    assert_eq!(
        gc.collect_applied(&candidate, 30, &journal)
            .await
            .unwrap()
            .deleted_objects,
        1
    );
    assert_eq!(
        gc.collect_applied(&candidate, 30, &journal)
            .await
            .unwrap()
            .deleted_objects,
        0
    );
    assert_eq!(partial.intents.lock().unwrap().len(), 1);
    assert_eq!(journal.intents.lock().unwrap().len(), 1);
    // Typed journal payloads round trip, including their exact reason/digests.
    for intent in partial
        .intents
        .lock()
        .unwrap()
        .iter()
        .chain(journal.intents.lock().unwrap().iter())
    {
        assert_eq!(
            &serde_json::from_slice::<GcIntent>(&serde_json::to_vec(intent).unwrap()).unwrap(),
            intent
        );
    }
}

async fn orphan_gc(store: Arc<dyn BufferStore>) {
    let c = config();
    let entry = publish(store.as_ref(), &c, 1, [0; 32]).await;
    let range = ManifestRange {
        start: ReaderCheckpoint::default(),
        end_mseq: 1,
        end_digest: entry.digest().unwrap(),
    };
    let (bytes, orphan) = encoded(&c, 2);
    store
        .put_create_only(&orphan.segment_key, &bytes)
        .await
        .unwrap();
    let bad = format!("{}bad.seg", c.segment_prefix());
    store.put_create_only(&bad, b"unverified").await.unwrap();
    let page = list_orphan_segments(
        store.as_ref(),
        &c,
        &range,
        OrphanFence { before_epoch: 2 },
        None,
        100,
        100,
    )
    .await
    .unwrap();
    assert_eq!(page.orphans.len(), 1);
    assert_eq!(page.unverified, vec![bad.clone()]);
    let gc = RetentionGc::new(
        store.clone(),
        Arc::new(Order),
        RetentionPolicy {
            retained_for_ms: 0,
            orphan_grace_ms: 10,
        },
    );
    let journal = FileJournal::new(store.clone(), None);
    for now in [0, 99, 100, 109] {
        assert_eq!(
            gc.collect_orphan(&page.orphans[0], now, &journal)
                .await
                .unwrap()
                .deleted_objects,
            0
        );
    }
    assert_eq!(
        gc.collect_orphan(&page.orphans[0], 110, &journal)
            .await
            .unwrap()
            .deleted_objects,
        1
    );
    assert_eq!(
        gc.collect_orphan(&page.orphans[0], 110, &journal)
            .await
            .unwrap()
            .deleted_objects,
        0
    );
    assert!(store.get(&entry.segment_key).await.unwrap().is_some());
    assert!(store.get(&c.manifest_key(0)).await.unwrap().is_some());
    assert!(store.get(&bad).await.unwrap().is_some());
    let mut broken_range = range;
    broken_range.end_mseq = 2;
    assert!(matches!(
        list_orphan_segments(
            store.as_ref(),
            &c,
            &broken_range,
            OrphanFence { before_epoch: 2 },
            None,
            100,
            100
        )
        .await,
        Err(Error::RecoveryRequired(_))
    ));
}
#[tokio::test]
async fn orphan_gc_requires_fence_grace_and_verified_content() {
    orphan_gc(local()).await;
}

#[tokio::test]
async fn gc_rechecks_current_content_before_journaling() {
    retention_corruption(local()).await;
}

async fn retention_corruption(store: Arc<dyn BufferStore>) {
    let c = config();
    let entry = publish(store.as_ref(), &c, 1, [0; 32]).await;
    let mut reader = BufferReader::new(
        store.clone(),
        c.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    reader.next_txn().await.unwrap();
    let candidate = AppliedSegment::new(reader.last_verified().unwrap().clone(), 0, 0);
    // Actual backend corruption replacement by the test exercises stale proofs.
    store.delete(&entry.segment_key).await.unwrap();
    store
        .put_create_only(&entry.segment_key, b"broken")
        .await
        .unwrap();
    let gc = RetentionGc::new(
        store.clone(),
        Arc::new(Order),
        RetentionPolicy {
            retained_for_ms: 0,
            orphan_grace_ms: 0,
        },
    );
    let journal = FileJournal::new(store.clone(), None);
    assert!(matches!(
        gc.collect_applied(&candidate, 1, &journal).await,
        Err(Error::RecoveryRequired(_))
    ));
    assert!(journal.intents.lock().unwrap().is_empty());
    assert_eq!(
        store.get(&entry.segment_key).await.unwrap().as_deref(),
        Some(b"broken".as_slice())
    );
}

#[tokio::test]
async fn zero_retention_collects_at_exact_boundary_and_counts_backend_bytes() {
    retention_boundary(local()).await;
}

async fn retention_boundary(store: Arc<dyn BufferStore>) {
    let c = config();
    publish(store.as_ref(), &c, 1, [0; 32]).await;
    let bytes_before = total_bytes(store.as_ref(), &c.prefix).await;
    let mut reader = BufferReader::new(
        store.clone(),
        c.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    reader.next_txn().await.unwrap();
    let candidate = AppliedSegment::new(reader.last_verified().unwrap().clone(), 10, 10);
    let gc = RetentionGc::new(
        store.clone(),
        Arc::new(Order),
        RetentionPolicy {
            retained_for_ms: 0,
            orphan_grace_ms: 0,
        },
    );
    let journal = FileJournal::new(store.clone(), None);
    let outcome = gc.collect_applied(&candidate, 10, &journal).await.unwrap();
    assert_eq!(outcome.deleted_objects, 2);
    assert_eq!(outcome.deleted_bytes, bytes_before);
    assert_eq!(total_bytes(store.as_ref(), &c.prefix).await, 0);
    assert_eq!(journal.intents.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn zero_orphan_grace_collects_at_observation_time() {
    orphan_boundary(local()).await;
}

async fn orphan_boundary(store: Arc<dyn BufferStore>) {
    let c = config();
    let (bytes, entry) = encoded(&c, 1);
    store
        .put_create_only(&entry.segment_key, &bytes)
        .await
        .unwrap();
    let page = list_orphan_segments(
        store.as_ref(),
        &c,
        &ManifestRange {
            start: ReaderCheckpoint::default(),
            end_mseq: 0,
            end_digest: [0; 32],
        },
        OrphanFence { before_epoch: 2 },
        None,
        100,
        10,
    )
    .await
    .unwrap();
    assert_eq!(page.orphans.len(), 1);
    let gc = RetentionGc::new(
        store.clone(),
        Arc::new(Order),
        RetentionPolicy {
            retained_for_ms: 0,
            orphan_grace_ms: 0,
        },
    );
    let journal = FileJournal::new(store.clone(), None);
    let outcome = gc
        .collect_orphan(&page.orphans[0], 10, &journal)
        .await
        .unwrap();
    assert_eq!(outcome.deleted_objects, 1);
    assert_eq!(outcome.deleted_bytes, bytes.len() as u64);
    assert!(store.get(&entry.segment_key).await.unwrap().is_none());
    assert_eq!(journal.intents.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn orphan_gc_rejects_valid_same_size_replacement_before_journaling() {
    orphan_replacement(local()).await;
}

async fn orphan_replacement(store: Arc<dyn BufferStore>) {
    let c = config();
    let (bytes, entry) = encoded(&c, 1);
    store
        .put_create_only(&entry.segment_key, &bytes)
        .await
        .unwrap();
    let page = list_orphan_segments(
        store.as_ref(),
        &c,
        &ManifestRange {
            start: ReaderCheckpoint::default(),
            end_mseq: 0,
            end_digest: [0; 32],
        },
        OrphanFence { before_epoch: 2 },
        None,
        100,
        0,
    )
    .await
    .unwrap();
    assert_eq!(page.orphans.len(), 1);

    // A fresh, internally valid segment has the same identity and byte length,
    // but different transaction content. Length alone cannot validate the proof.
    let mut decoder = SegmentDecoder::new(Cursor::new(bytes), c.limits).unwrap();
    let header = decoder.header().clone();
    let SegmentRecord::Transaction(txn) = decoder.next_record().unwrap().unwrap() else {
        panic!("expected transaction");
    };
    decoder.finish().unwrap();
    let changed = AssembledTxn::new(
        txn.source,
        txn.txn,
        txn.commit_position,
        Some(2),
        txn.events,
    )
    .unwrap();
    let mut encoder = SegmentEncoder::new(Vec::new(), header, c.limits).unwrap();
    encoder
        .write_record(&SegmentRecord::Transaction(changed))
        .unwrap();
    let (changed, summary) = encoder.finish().unwrap();
    assert_eq!(changed.len() as u64, page.orphans[0].bytes());
    assert_ne!(summary.digest, entry.segment_digest);
    store.delete(&entry.segment_key).await.unwrap();
    store
        .put_create_only(&entry.segment_key, &changed)
        .await
        .unwrap();

    let gc = RetentionGc::new(
        store.clone(),
        Arc::new(Order),
        RetentionPolicy {
            retained_for_ms: 0,
            orphan_grace_ms: 0,
        },
    );
    let journal = FileJournal::new(store.clone(), None);
    assert!(matches!(
        gc.collect_orphan(&page.orphans[0], 0, &journal).await,
        Err(Error::RecoveryRequired("orphan changed"))
    ));
    assert!(journal.intents.lock().unwrap().is_empty());
    assert_eq!(store.get(&entry.segment_key).await.unwrap(), Some(changed));
}

async fn gc_suite(store: Arc<dyn BufferStore>) {
    orphan_gc(store.clone()).await;
    soak(store.clone(), 256).await;
    retention_journal_matrix(store.clone()).await;
    retention_corruption(store.clone()).await;
    retention_boundary(store.clone()).await;
    orphan_boundary(store.clone()).await;
    orphan_replacement(store.clone()).await;
}

#[tokio::test]
async fn local_gc_matrix() {
    gc_suite(local()).await;
}

fn remote_store(backend: &str, endpoint_override: Option<&str>) -> Arc<dyn BufferStore> {
    use skedaddle::buffer::backends::{Encryption, TransportSecurity};
    let cfg: serde_json::Value =
        serde_json::from_slice(
            &std::fs::read(std::env::var_os("SKEDADDLE_TEST_CONFIG").expect(
                "run harness/run.ps1 p3: remote buffer tests require harness configuration",
            ))
            .unwrap(),
        )
        .unwrap();
    let endpoint = endpoint_override.unwrap_or_else(|| cfg[backend].as_str().unwrap());
    let timeout = Duration::from_secs(300);
    let retry = object_store::RetryConfig {
        max_retries: 0,
        ..Default::default()
    };
    Arc::new(match backend {
        "minio" => ObjectBufferStore::s3(
            object_store::aws::AmazonS3Builder::new()
                .with_bucket_name("p2-bucket")
                .with_region("us-east-1")
                .with_access_key_id("skedaddle")
                .with_secret_access_key("skedaddle-secret")
                .with_endpoint(endpoint)
                .with_retry(retry),
            Encryption::UnencryptedLocalTest,
            TransportSecurity::DisableForLocalTest,
            timeout,
        )
        .unwrap(),
        "gcs" => {
            let auth = object_store::gcp::GoogleCloudStorageBuilder::new()
                .with_bucket_name("p2-bucket")
                .with_service_account_key(r#"{"private_key":"","private_key_id":"","client_email":"","disable_oauth":true}"#)
                .with_skip_signature(true).with_base_url(endpoint)
                .with_client_options(object_store::ClientOptions::new().with_allow_http(true))
                .with_retry(retry).build().unwrap();
            ObjectBufferStore::gcs(
                "p2-bucket",
                endpoint,
                auth,
                Encryption::GcsDefault,
                TransportSecurity::DisableForLocalTest,
                timeout,
            )
            .unwrap()
        }
        _ => panic!("unknown remote backend {backend}"),
    })
}

#[tokio::test]
#[ignore = "requires MinIO/fake-gcs via harness/run.ps1 p3; plain cargo test explicitly skips infrastructure"]
async fn minio_gc_matrix() {
    gc_suite(remote_store("minio", None)).await;
}

#[tokio::test]
#[ignore = "requires MinIO/fake-gcs via harness/run.ps1 p3; plain cargo test explicitly skips infrastructure"]
async fn gcs_gc_matrix() {
    gc_suite(remote_store("gcs", None)).await;
}

struct OperationalFailureJournal;
#[async_trait]
impl GcJournal for OperationalFailureJournal {
    async fn record(&self, _: &GcIntent) -> skedaddle::buffer::reader::Result<()> {
        Err(Error::Store(
            skedaddle::buffer::backends::Error::UnknownPutOutcome,
        ))
    }
}
#[tokio::test]
async fn ambiguous_intent_append_preserves_operational_error_and_objects() {
    let store = local();
    let c = config();
    let entry = publish(store.as_ref(), &c, 1, [0; 32]).await;
    let mut reader = BufferReader::new(
        store.clone(),
        c.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    reader.next_txn().await.unwrap().unwrap();
    let candidate = AppliedSegment::new(reader.last_verified().unwrap().clone(), 0, 0);
    let gc = RetentionGc::new(
        store.clone(),
        Arc::new(Order),
        RetentionPolicy {
            retained_for_ms: 0,
            orphan_grace_ms: 0,
        },
    );
    assert!(matches!(
        gc.collect_applied(&candidate, 1, &OperationalFailureJournal)
            .await
            .unwrap_err(),
        Error::Store(skedaddle::buffer::backends::Error::UnknownPutOutcome)
    ));
    assert!(store.get(&entry.segment_key).await.unwrap().is_some());
    assert!(store.get(&c.manifest_key(0)).await.unwrap().is_some());
    assert_eq!(
        gc.collect_applied(&candidate, 1, &FileJournal::new(store.clone(), None))
            .await
            .unwrap()
            .deleted_objects,
        2
    );
}
