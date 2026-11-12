#[cfg(feature = "test-crash")]
use skedaddle::buffer::writer::CrashPoint;
use skedaddle::{
    buffer::{
        backends::{BufferStore, ObjectBufferStore},
        format::{ManifestEntry, SegmentDecoder, manifest_key, segment_key},
        writer::{BufferWriter, Error, TransactionAssembler, WriterConfig},
    },
    event::{
        AssembledTxn, Event, EventError, EventKind, OpaqueValue, Position, PositionOrder,
        Redelivery, Row, SourceIdentity, TxnId,
    },
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cmp::Ordering,
    io::{Cursor, Read, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
        mpsc,
    },
    time::{Duration, Instant},
};

// Global, generation-tagged measurement includes Tokio and blocking worker
// allocations. Headers distinguish allocations made before a measurement, so
// freeing a pre-existing allocation cannot undercount live measured bytes.
struct MeasuredAllocator;
#[repr(C)]
struct AllocationHeader {
    generation: u64,
}
static ACCOUNT_LOCK: AtomicBool = AtomicBool::new(false);
struct AccountGuard;
impl AccountGuard {
    fn acquire() -> Self {
        while ACCOUNT_LOCK
            .compare_exchange_weak(
                false,
                true,
                AtomicOrdering::Acquire,
                AtomicOrdering::Relaxed,
            )
            .is_err()
        {
            std::hint::spin_loop();
        }
        Self
    }
}
impl Drop for AccountGuard {
    fn drop(&mut self) {
        ACCOUNT_LOCK.store(false, AtomicOrdering::Release);
    }
}
static ACTIVE: AtomicU64 = AtomicU64::new(0);
static GENERATION: AtomicU64 = AtomicU64::new(1);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
// Measure in a singleton test process: libtest can allocate parallel test
// threads and Tokio runtimes before those tests acquire TEST_LOCK. The child
// still measures every allocation on every product/runtime worker thread.
fn run_allocation_test_isolated(name: &str, ignored: bool) -> bool {
    const MARKER: &str = "SKEDADDLE_ALLOCATION_TEST_CHILD";
    if std::env::var(MARKER).as_deref() == Ok(name) {
        return false;
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child.args([name, "--exact", "--nocapture", "--test-threads=1"]);
    if ignored {
        child.arg("--ignored");
    }
    let status = child
        .env(MARKER, name)
        .status()
        .expect("start isolated allocation test");
    assert!(
        status.success(),
        "isolated allocation test {name} failed: {status}"
    );
    true
}

fn measured_layout(layout: Layout) -> (Layout, usize) {
    Layout::new::<AllocationHeader>().extend(layout).unwrap()
}
fn begin_measurement() {
    let _guard = AccountGuard::acquire();
    assert_eq!(ACTIVE.load(AtomicOrdering::SeqCst), 0);
    LIVE.store(0, AtomicOrdering::SeqCst);
    PEAK.store(0, AtomicOrdering::SeqCst);
    ACTIVE.store(
        GENERATION.fetch_add(1, AtomicOrdering::SeqCst),
        AtomicOrdering::SeqCst,
    );
}
fn end_measurement() -> usize {
    let _guard = AccountGuard::acquire();
    ACTIVE.store(0, AtomicOrdering::SeqCst);
    PEAK.load(AtomicOrdering::SeqCst)
}
// SAFETY: each System allocation includes an aligned header and the original
// requested payload. Deallocation reconstructs the same layout and base pointer.
// Header access and atomic accounting allocate nothing. Realloc adjusts live
// requested capacity only after System reports success.
unsafe impl GlobalAlloc for MeasuredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let (backing, offset) = measured_layout(layout);
        let base = unsafe { System.alloc(backing) };
        if base.is_null() {
            return base;
        }
        let _guard = AccountGuard::acquire();
        let generation = ACTIVE.load(AtomicOrdering::SeqCst);
        unsafe {
            base.cast::<AllocationHeader>()
                .write(AllocationHeader { generation });
        }
        if generation != 0 {
            let live = LIVE.fetch_add(layout.size(), AtomicOrdering::SeqCst) + layout.size();
            PEAK.fetch_max(live, AtomicOrdering::SeqCst);
        }
        unsafe { base.add(offset) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let (old_layout, offset) = measured_layout(layout);
        let new_payload = Layout::from_size_align(size, layout.align()).unwrap();
        let (new_layout, new_offset) = measured_layout(new_payload);
        assert_eq!(offset, new_offset);
        let base = unsafe { ptr.sub(offset) };
        let old_generation = unsafe { (*base.cast::<AllocationHeader>()).generation };
        let result = unsafe { System.realloc(base, old_layout, new_layout.size()) };
        if result.is_null() {
            return result;
        }
        let _guard = AccountGuard::acquire();
        let generation = ACTIVE.load(AtomicOrdering::SeqCst);
        unsafe {
            result
                .cast::<AllocationHeader>()
                .write(AllocationHeader { generation });
        }
        if generation != 0 {
            if old_generation == generation {
                LIVE.fetch_sub(layout.size(), AtomicOrdering::SeqCst);
            }
            let live = LIVE.fetch_add(size, AtomicOrdering::SeqCst) + size;
            PEAK.fetch_max(live, AtomicOrdering::SeqCst);
        }
        unsafe { result.add(offset) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let (backing, offset) = measured_layout(layout);
        let base = unsafe { ptr.sub(offset) };
        let generation = unsafe { (*base.cast::<AllocationHeader>()).generation };
        let guard = AccountGuard::acquire();
        if generation != 0 && generation == ACTIVE.load(AtomicOrdering::SeqCst) {
            LIVE.fetch_sub(layout.size(), AtomicOrdering::SeqCst);
        }
        drop(guard);
        unsafe {
            System.dealloc(base, backing);
        }
    }
}
#[global_allocator]
static ALLOCATOR: MeasuredAllocator = MeasuredAllocator;

fn scratch(name: &str) -> PathBuf {
    let root = PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT")
            .expect("set SKEDADDLE_TEST_ROOT under J:/gigglesdb-work"),
    );
    assert!(root.starts_with("J:/gigglesdb-work"));
    let path = root.join(format!("p3-writer-{name}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path).unwrap();
    path
}
fn local() -> Arc<dyn BufferStore> {
    Arc::new(ObjectBufferStore::local(scratch("local"), Duration::from_secs(300)).unwrap())
}
fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "pg".into(),
        system_id: "s".into(),
        lineage: "t1".into(),
        db_name: "db".into(),
        slot: "slot".into(),
    }
}
fn pos(n: u64) -> Position {
    Position::new("pg:test", n.to_be_bytes().to_vec())
}
struct PgOrder;
impl PositionOrder for PgOrder {
    fn domain(&self) -> &str {
        "pg:test"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
        let a: [u8; 8] = a
            .try_into()
            .map_err(|_| EventError::InvalidEncoding("LSN"))?;
        let b: [u8; 8] = b
            .try_into()
            .map_err(|_| EventError::InvalidEncoding("LSN"))?;
        Ok(u64::from_be_bytes(a).cmp(&u64::from_be_bytes(b)))
    }
}
fn event(id: u64, ordinal: u64, size: usize) -> Event {
    Event {
        source: source(),
        txn: TxnId(id.to_be_bytes().to_vec()),
        ordinal_in_txn: ordinal,
        position: pos(id),
        schema_version: 1,
        kind: EventKind::Insert,
        table: Some("public.t".into()),
        key: Row::new(),
        before: None,
        after: Some(Row::from([(
            "v".into(),
            OpaqueValue {
                type_id: "bytes".into(),
                bytes: Some(vec![b'x'; size]),
            },
        )])),
        unchanged_cols: Default::default(),
    }
}
fn txn(id: u64, size: usize) -> AssembledTxn {
    AssembledTxn::new(
        source(),
        TxnId(id.to_be_bytes().to_vec()),
        pos(id),
        Some(42),
        vec![event(id, 0, size)],
    )
    .unwrap()
}
fn config() -> WriterConfig {
    WriterConfig {
        prefix: format!("p3-writer/{}", uuid::Uuid::new_v4()),
        migration_id: "migration".into(),
        stream: "stream".into(),
        epoch: 1,
        segment_bytes: 1024 * 1024,
        segment_transactions: 100,
        segment_age: Duration::from_secs(1),
        max_transaction_bytes: 64 * 1024 * 1024,
    }
}
async fn open(
    store: Arc<dyn BufferStore>,
    config: WriterConfig,
) -> (
    BufferWriter,
    mpsc::Receiver<skedaddle::buffer::writer::BufferedProgress>,
) {
    let (tx, rx) = mpsc::channel();
    (
        BufferWriter::open(store, config, Arc::new(PgOrder), tx)
            .await
            .unwrap(),
        rx,
    )
}
async fn manifest(store: &dyn BufferStore, c: &WriterConfig, seq: u64) -> Option<ManifestEntry> {
    store
        .get(&manifest_key(&c.prefix, &c.migration_id, &c.stream, seq))
        .await
        .unwrap()
        .map(|b| ManifestEntry::from_canonical_json(&b).unwrap())
}
async fn records(store: &dyn BufferStore, m: &ManifestEntry) -> Vec<AssembledTxn> {
    let bytes = store.get(&m.segment_key).await.unwrap().unwrap();
    let mut decoder = SegmentDecoder::new(
        Cursor::new(bytes),
        skedaddle::buffer::format::Limits {
            max_record_bytes: 64 * 1024 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    let mut txns = vec![];
    while let Some(record) = decoder.next_record().unwrap() {
        let skedaddle::buffer::format::SegmentRecord::Transaction(txn) = record else {
            panic!("expected transaction")
        };
        txns.push(txn);
    }
    let (_, summary) = decoder.finish().unwrap();
    assert_eq!(summary.digest, m.segment_digest);
    assert_eq!(summary.bytes, m.bytes);
    txns
}

#[cfg(feature = "test-crash")]
async fn ack_matrix(store: Arc<dyn BufferStore>) {
    for point in [
        CrashPoint::BeforeSegmentPut,
        CrashPoint::AfterSegmentPut,
        CrashPoint::BeforeManifestPut,
        CrashPoint::AfterManifestPut,
    ] {
        let c = config();
        let (mut writer, rx) = open(store.clone(), c.clone()).await;
        writer.append(txn(10, 10), Instant::now()).await.unwrap();
        assert!(rx.try_recv().is_err());
        writer.inject_crash(point);
        assert!(matches!(writer.seal().await, Err(Error::InjectedCrash(p)) if p == point));
        assert!(rx.try_recv().is_err());
        assert!(writer.is_blocked());
        let m = manifest(store.as_ref(), &c, 0).await;
        assert_eq!(m.is_some(), point == CrashPoint::AfterManifestPut);
        let key = segment_key(
            &c.prefix,
            &c.migration_id,
            &c.stream,
            &pos(10),
            &pos(10),
            c.epoch,
        );
        assert_eq!(
            store.get(&key).await.unwrap().is_some(),
            point != CrashPoint::BeforeSegmentPut
        );
        drop(writer); // Discard all process state at the seam, keep real storage.
        let (mut restarted, recovered) = open(store.clone(), c.clone()).await;
        if m.is_some() {
            let progress = recovered.try_recv().unwrap();
            assert_eq!(progress.position, pos(10));
            assert_eq!(
                restarted.append(txn(10, 10), Instant::now()).await.unwrap(),
                Redelivery::Noop
            );
            assert!(!restarted.seal().await.unwrap());
            assert!(recovered.try_recv().is_err());
        } else {
            assert!(recovered.try_recv().is_err());
            restarted.append(txn(10, 10), Instant::now()).await.unwrap();
            restarted.seal().await.unwrap();
            assert_eq!(recovered.try_recv().unwrap().position, pos(10));
        }
        let m = manifest(store.as_ref(), &c, 0).await.unwrap();
        assert_eq!(records(store.as_ref(), &m).await, vec![txn(10, 10)]);
        assert!(manifest(store.as_ref(), &c, 1).await.is_none());
    }
}
#[tokio::test]
#[cfg_attr(
    not(feature = "test-crash"),
    ignore = "crash seams require cargo test --features test-crash --test buffer_writer (also run by harness/run.ps1 p3)"
)]
async fn ack_only_after_manifest_durable() {
    let _test_lock = TEST_LOCK.lock().await;
    #[cfg(feature = "test-crash")]
    ack_matrix(local()).await;
    #[cfg(not(feature = "test-crash"))]
    panic!("enable test-crash; production builds contain no crash seam");
}

async fn identical(store: Arc<dyn BufferStore>) {
    let c = config();
    let (mut w, rx) = open(store.clone(), c.clone()).await;
    assert!(!w.is_blocked());
    let now = Instant::now();
    assert_eq!(w.append(txn(1, 10), now).await.unwrap(), Redelivery::New);
    assert_eq!(w.append(txn(1, 10), now).await.unwrap(), Redelivery::Noop);
    assert_eq!(w.pending_transactions(), 1);
    assert!(rx.try_recv().is_err());
    w.seal().await.unwrap();
    assert!(!w.is_blocked());
    assert_eq!(rx.try_recv().unwrap().position, pos(1));
    assert_eq!(w.append(txn(1, 10), now).await.unwrap(), Redelivery::Noop);
    assert!(!w.seal().await.unwrap());
    assert!(rx.try_recv().is_err());
    drop(w);
    let (mut w, rx) = open(store.clone(), c.clone()).await;
    assert!(!w.is_blocked());
    assert_eq!(rx.try_recv().unwrap().position, pos(1));
    assert_eq!(w.append(txn(1, 10), now).await.unwrap(), Redelivery::Noop);
    assert!(!w.seal().await.unwrap());
    assert_eq!(
        records(
            store.as_ref(),
            &manifest(store.as_ref(), &c, 0).await.unwrap()
        )
        .await
        .len(),
        1
    );
    assert!(manifest(store.as_ref(), &c, 1).await.is_none());
}
#[tokio::test]
async fn redelivery_identical_is_noop() {
    let _test_lock = TEST_LOCK.lock().await;
    identical(local()).await;
}

#[tokio::test]
async fn recovery_enforces_the_configured_transaction_limit_before_ack() {
    let _test_lock = TEST_LOCK.lock().await;
    recovery_transaction_limit(local()).await;
}

async fn recovery_transaction_limit(store: Arc<dyn BufferStore>) {
    let c = config();
    let (mut writer, progress) = open(store.clone(), c.clone()).await;
    writer.append(txn(1, 1024), Instant::now()).await.unwrap();
    writer.seal().await.unwrap();
    assert_eq!(progress.try_recv().unwrap().position, pos(1));
    drop(writer);

    let (writer, progress) = open(store.clone(), c.clone()).await;
    assert_eq!(progress.try_recv().unwrap().position, pos(1));
    drop(writer);

    let mut tighter = c;
    tighter.max_transaction_bytes = 128;
    let (progress, received) = mpsc::channel();
    let result = BufferWriter::open(store.clone(), tighter, Arc::new(PgOrder), progress).await;
    assert!(matches!(result, Err(Error::Format(_))));
    assert!(received.try_recv().is_err());
}

#[tokio::test]
async fn recovery_rejects_each_authenticated_manifest_header_and_summary_mismatch() {
    let _test_lock = TEST_LOCK.lock().await;
    recovery_mismatches(local()).await;
}

async fn recovery_mismatches(store: Arc<dyn BufferStore>) {
    use skedaddle::buffer::format::{Limits, SegmentEncoder, SegmentHeader, SegmentRecord};
    for mismatch in [
        "none",
        "migration",
        "stream",
        "epoch",
        "first",
        "last",
        "count",
        "digest",
        "bytes",
    ] {
        let mut c = config();
        c.epoch = 3;
        let mut header = SegmentHeader {
            migration_id: c.migration_id.clone(),
            stream: c.stream.clone(),
            epoch: 1,
            first_pos: pos(1),
            last_pos: pos(1),
            record_count: 1,
        };
        if mismatch == "migration" {
            header.migration_id = "other-migration".into();
        }
        if mismatch == "stream" {
            header.stream = "other-stream".into();
        }
        let mut encoder = SegmentEncoder::new(Vec::new(), header, Limits::default()).unwrap();
        encoder
            .write_record(&SegmentRecord::Transaction(txn(1, 11)))
            .unwrap();
        let (bytes, summary) = encoder.finish().unwrap();
        let mut entry = ManifestEntry {
            mseq: 0,
            epoch: 1,
            prev_digest: [0; 32],
            segment_key: String::new(),
            segment_digest: summary.digest,
            first_pos: pos(1),
            last_pos: pos(1),
            txn_count: 1,
            bytes: summary.bytes,
        };
        match mismatch {
            "epoch" => entry.epoch = 2,
            "first" => entry.first_pos = pos(0),
            "last" => entry.last_pos = pos(2),
            "count" => entry.txn_count = 2,
            "digest" => entry.segment_digest[0] ^= 1,
            "bytes" => entry.bytes += 1,
            _ => {}
        }
        entry.segment_key = segment_key(
            &c.prefix,
            &c.migration_id,
            &c.stream,
            &entry.first_pos,
            &entry.last_pos,
            entry.epoch,
        );
        store
            .put_create_only(&entry.segment_key, &bytes)
            .await
            .unwrap();
        store
            .put_create_only(
                &manifest_key(&c.prefix, &c.migration_id, &c.stream, 0),
                &entry.canonical_json().unwrap(),
            )
            .await
            .unwrap();
        let (progress, received) = mpsc::channel();
        let recovered = BufferWriter::open(store.clone(), c, Arc::new(PgOrder), progress).await;
        if mismatch == "none" {
            let writer = recovered.unwrap();
            let progress = received.try_recv().unwrap();
            assert_eq!(progress.position, pos(1));
            assert_eq!(progress.manifest_digest, entry.digest().unwrap());
            assert_eq!(writer.next_manifest_sequence(), 1);
        } else {
            let expected = if matches!(mismatch, "digest" | "bytes") {
                "manifest/segment digest or size"
            } else {
                "manifest/header mismatch"
            };
            assert!(
                matches!(recovered, Err(Error::Invalid(reason)) if reason == expected),
                "accepted or misclassified {mismatch} mismatch"
            );
            assert!(received.try_recv().is_err(), "acknowledged {mismatch}");
        }
    }
}

async fn conflicting(store: Arc<dyn BufferStore>) {
    let c = config();
    let (mut w, rx) = open(store.clone(), c.clone()).await;
    w.append(txn(1, 10), Instant::now()).await.unwrap();
    w.seal().await.unwrap();
    rx.try_recv().unwrap();
    assert!(matches!(
        w.append(txn(1, 11), Instant::now()).await,
        Err(Error::Event(EventError::RedeliveryConflict { .. }))
    ));
    assert!(rx.try_recv().is_err());
    drop(w);
    let (mut w, rx) = open(store.clone(), c.clone()).await;
    rx.try_recv().unwrap();
    assert!(matches!(
        w.append(txn(1, 11), Instant::now()).await,
        Err(Error::Event(EventError::RedeliveryConflict { .. }))
    ));
    assert!(rx.try_recv().is_err());
    assert!(manifest(store.as_ref(), &c, 1).await.is_none());
}
#[tokio::test]
async fn redelivery_conflicting_is_error() {
    let _test_lock = TEST_LOCK.lock().await;
    conflicting(local()).await;
}

async fn oversized(store: Arc<dyn BufferStore>) {
    let mut c = config();
    c.max_transaction_bytes = 1024;
    let (mut w, rx) = open(store.clone(), c.clone()).await;
    w.append(txn(1, 10), Instant::now()).await.unwrap();
    assert!(matches!(
        w.append(txn(2, 4096), Instant::now()).await,
        Err(Error::OversizedTransaction { maximum: 1024, .. })
    ));
    assert_eq!(w.pending_transactions(), 1);
    assert!(w.is_blocked());
    assert!(matches!(w.checkpoint(), Err(Error::Blocked)));
    assert!(matches!(
        w.retain_redelivery_from(pos(1)),
        Err(Error::Blocked)
    ));
    assert!(matches!(w.seal().await, Err(Error::Blocked)));
    assert!(rx.try_recv().is_err());
    assert!(manifest(store.as_ref(), &c, 0).await.is_none());
    let mut a = TransactionAssembler::new(
        source(),
        TxnId(2_u64.to_be_bytes().to_vec()),
        pos(2),
        None,
        256,
        1024,
        scratch("oversized"),
    )
    .unwrap();
    a.push(event(2, 0, 100)).unwrap();
    let path = a.spilled_path().unwrap().to_owned();
    assert!(matches!(
        a.push(event(2, 1, 4096)),
        Err(Error::OversizedTransaction { .. })
    ));
    assert!(matches!(a.finish(), Err(Error::Blocked)));
    assert!(!path.exists());
}
#[tokio::test]
async fn oversized_txn_blocks_not_split() {
    let _test_lock = TEST_LOCK.lock().await;
    oversized(local()).await;
}

#[tokio::test]
async fn writer_and_assembler_reject_transaction_limits_outside_the_public_range() {
    let _test_lock = TEST_LOCK.lock().await;
    for maximum in [0, 1_000_000_001] {
        let expected = "transaction maximum must be in 1..=1 GB";
        assert!(matches!(
            TransactionAssembler::new(
                source(),
                TxnId(vec![1]),
                pos(1),
                Some(42),
                1,
                maximum,
                scratch("invalid-limit"),
            ),
            Err(Error::Invalid(reason)) if reason == expected
        ));
        let mut c = config();
        c.max_transaction_bytes = maximum;
        let (progress, received) = mpsc::channel();
        assert!(matches!(
            BufferWriter::open(local(), c, Arc::new(PgOrder), progress).await,
            Err(Error::Invalid(reason)) if reason == expected
        ));
        assert!(received.try_recv().is_err());
    }
}

#[tokio::test]
async fn transaction_limit_is_inclusive_for_owned_and_streaming_admission() {
    let _test_lock = TEST_LOCK.lock().await;
    for accepted in [true, false] {
        let expected = AssembledTxn::new(
            source(),
            TxnId(1_u64.to_be_bytes().to_vec()),
            pos(1),
            Some(42),
            vec![event(1, 0, 11), event(1, 1, 12)],
        )
        .unwrap();
        let length = expected.canonical_bytes().unwrap().len() as u64;
        let mut assembler = TransactionAssembler::new(
            source(),
            expected.txn.clone(),
            pos(1),
            Some(42),
            8192,
            1_000_000_000,
            scratch("streaming-limit"),
        )
        .unwrap();
        for item in expected.events.clone() {
            assert_eq!(assembler.push(item).unwrap(), Redelivery::New);
        }
        let prepared = assembler.finish().unwrap();
        assert_eq!(prepared.canonical_len(), length);
        assert_eq!(prepared.event_count(), 2);
        let mut c = config();
        c.segment_transactions = 1;
        c.max_transaction_bytes = if accepted { length } else { length - 1 };
        let (mut writer, progress) = open(local(), c.clone()).await;
        let result = writer.append_streaming(prepared, Instant::now()).await;
        if accepted {
            assert_eq!(result.unwrap(), Redelivery::New);
            assert_eq!(progress.try_recv().unwrap().position, pos(1));
            assert_eq!(writer.pending_transactions(), 0);
            let (mut owned, progress) = open(local(), c).await;
            assert_eq!(
                owned.append(expected, Instant::now()).await.unwrap(),
                Redelivery::New
            );
            assert_eq!(progress.try_recv().unwrap().position, pos(1));
            assert!(!owned.is_blocked());
        } else {
            assert!(matches!(
                result,
                Err(Error::OversizedTransaction { bytes, maximum })
                    if bytes == length && maximum == length - 1
            ));
            assert!(writer.is_blocked());
            assert_eq!(writer.pending_transactions(), 0);
            assert!(matches!(writer.checkpoint(), Err(Error::Blocked)));
            assert!(progress.try_recv().is_err());
        }
    }
}

async fn stale(store: Arc<dyn BufferStore>) {
    let old = config();
    let mut new = old.clone();
    new.epoch = 2;
    let (mut stale, stale_rx) = open(store.clone(), old.clone()).await;
    let (mut active, active_rx) = open(store.clone(), new.clone()).await;
    active.append(txn(1, 10), Instant::now()).await.unwrap();
    active.seal().await.unwrap();
    active_rx.try_recv().unwrap();
    stale.append(txn(1, 10), Instant::now()).await.unwrap();
    assert!(matches!(
        stale.seal().await,
        Err(Error::SteppedDown {
            writer_epoch: 1,
            manifest_epoch: 2
        })
    ));
    assert!(stale.is_blocked());
    assert!(stale_rx.try_recv().is_err());
    assert!(matches!(stale.seal().await, Err(Error::Blocked)));
    let orphan = segment_key(
        &old.prefix,
        &old.migration_id,
        &old.stream,
        &pos(1),
        &pos(1),
        1,
    );
    assert!(store.get(&orphan).await.unwrap().is_some());
    let m = manifest(store.as_ref(), &old, 0).await.unwrap();
    assert_eq!(m.epoch, 2);
    assert_ne!(m.segment_key, orphan);
    active.append(txn(2, 10), Instant::now()).await.unwrap();
    active.seal().await.unwrap();
    let second = manifest(store.as_ref(), &new, 1).await.unwrap();
    assert_eq!(second.prev_digest, m.digest().unwrap());
}
#[tokio::test]
async fn stale_writer_manifest_rejected() {
    let _test_lock = TEST_LOCK.lock().await;
    stale(local()).await;
}

async fn sealing(store: Arc<dyn BufferStore>) {
    let now = Instant::now();
    for trigger in ["size", "count", "age"] {
        let mut c = config();
        if trigger == "size" {
            c.segment_bytes = 1;
        }
        if trigger == "count" {
            c.segment_transactions = 2;
        }
        let (mut w, rx) = open(store.clone(), c.clone()).await;
        w.append(txn(1, 10), now).await.unwrap();
        if trigger == "count" {
            assert!(rx.try_recv().is_err());
            w.append(txn(2, 10), now).await.unwrap();
        }
        if trigger == "age" {
            assert!(rx.try_recv().is_err());
            assert!(
                !w.seal_if_due(now + Duration::from_millis(999))
                    .await
                    .unwrap()
            );
            assert!(w.seal_if_due(now + Duration::from_secs(1)).await.unwrap());
        }
        let expected = if trigger == "count" { 2 } else { 1 };
        assert_eq!(rx.try_recv().unwrap().position, pos(expected));
        let m = manifest(store.as_ref(), &c, 0).await.unwrap();
        assert_eq!(m.txn_count, expected);
        assert_eq!(records(store.as_ref(), &m).await.len(), expected as usize);
    }
}
#[tokio::test]
async fn seals_by_size_count_and_idle_age() {
    let _test_lock = TEST_LOCK.lock().await;
    sealing(local()).await;
}

#[tokio::test]
async fn size_sealing_uses_the_actual_encoded_segment_bytes() {
    let _test_lock = TEST_LOCK.lock().await;
    use skedaddle::buffer::format::{Limits, SegmentEncoder, SegmentHeader, SegmentRecord};
    for extra in [0, 1] {
        let store = local();
        let mut c = config();
        let header = SegmentHeader {
            migration_id: c.migration_id.clone(),
            stream: c.stream.clone(),
            epoch: c.epoch,
            first_pos: pos(1),
            last_pos: pos(1),
            record_count: 1,
        };
        let mut encoder = SegmentEncoder::new(Vec::new(), header, Limits::default()).unwrap();
        encoder
            .write_record(&SegmentRecord::Transaction(txn(1, 11)))
            .unwrap();
        let (_, summary) = encoder.finish().unwrap();
        c.segment_bytes = summary.bytes + extra;
        let (mut writer, progress) = open(store.clone(), c.clone()).await;
        let now = Instant::now();
        writer.append(txn(1, 11), now).await.unwrap();
        if extra == 0 {
            assert_eq!(progress.try_recv().unwrap().position, pos(1));
            assert_eq!(writer.pending_transactions(), 0);
            assert_eq!(
                manifest(store.as_ref(), &c, 0).await.unwrap().bytes,
                summary.bytes
            );
        } else {
            assert!(progress.try_recv().is_err());
            assert_eq!(writer.pending_transactions(), 1);
            writer.append(txn(2, 11), now).await.unwrap();
            assert_eq!(progress.try_recv().unwrap().position, pos(2));
            assert_eq!(writer.pending_transactions(), 0);
            assert_eq!(manifest(store.as_ref(), &c, 0).await.unwrap().txn_count, 2);
        }
    }
}

// Independent decoder for the v1 segment / v2 transaction fixture. It uses
// no product encoder/decoder, checks every field and payload byte, and hashes
// both envelopes independently with fixed stack buffers.
struct Independent<R> {
    input: R,
    segment: sha2::Sha256,
    transaction: Option<sha2::Sha256>,
}
impl<R: Read> Independent<R> {
    fn bytes<const N: usize>(&mut self) -> [u8; N] {
        use sha2::Digest;
        let mut bytes = [0; N];
        self.input.read_exact(&mut bytes).unwrap();
        self.segment.update(bytes);
        if let Some(hash) = &mut self.transaction {
            hash.update(bytes);
        }
        bytes
    }
    fn expect(&mut self, expected: &[u8]) {
        let mut offset = 0;
        while offset < expected.len() {
            let byte = self.bytes::<1>();
            assert_eq!(byte[0], expected[offset]);
            offset += 1;
        }
    }
    fn field(&mut self, expected: &[u8]) {
        assert_eq!(u32::from_be_bytes(self.bytes()), expected.len() as u32);
        self.expect(expected);
    }
    fn source(&mut self) {
        for field in [b"pg".as_slice(), b"s", b"t1", b"db", b"slot"] {
            self.field(field);
        }
    }
    fn position(&mut self) {
        self.field(b"pg:test");
        self.field(&5_u64.to_be_bytes());
    }
}
fn independently_decode(file: std::fs::File, manifest: &ManifestEntry, expected: [u8; 32]) {
    use sha2::Digest;
    let mut r = Independent {
        input: file,
        segment: sha2::Sha256::new(),
        transaction: None,
    };
    r.expect(b"SKEDSEG\0");
    r.expect(&1_u16.to_be_bytes());
    let header_len = u32::from_be_bytes(r.bytes());
    assert_eq!(header_len, 85);
    r.field(b"migration");
    r.field(b"stream");
    r.expect(&1_u64.to_be_bytes());
    r.position();
    r.position();
    r.expect(&1_u64.to_be_bytes());
    let length = u64::from_be_bytes(r.bytes());
    assert!(length >= 300_000_000);
    r.transaction = Some(sha2::Sha256::new());
    r.expect(b"SKEV\0\x02\x02");
    r.source();
    r.field(&5_u64.to_be_bytes());
    r.position();
    r.expect(&[1]);
    r.expect(&42_i64.to_be_bytes());
    r.expect(&[0]); // v2 transaction source origin is absent in this fixture.
    r.expect(&4800_u32.to_be_bytes());
    for ordinal in 0..4800_u64 {
        r.source();
        r.field(&5_u64.to_be_bytes());
        r.expect(&ordinal.to_be_bytes());
        r.position();
        r.expect(&1_u64.to_be_bytes());
        r.expect(&[0, 1]);
        r.field(b"public.t");
        r.expect(&0_u32.to_be_bytes());
        r.expect(&[0, 1]);
        r.expect(&1_u32.to_be_bytes());
        r.field(b"v");
        r.field(b"bytes");
        r.expect(&[1]);
        r.expect(&65536_u32.to_be_bytes());
        for _ in 0..4 {
            assert!(r.bytes::<16384>().iter().all(|b| *b == b'x'));
        }
        r.expect(&0_u32.to_be_bytes());
    }
    let digest: [u8; 32] = r.transaction.take().unwrap().finalize().into();
    assert_eq!(digest, expected);
    assert_eq!(r.bytes::<32>(), digest);
    r.expect(&0_u64.to_be_bytes());
    r.expect(b"SKEDEND\0");
    r.expect(&1_u64.to_be_bytes());
    let segment: [u8; 32] = r.segment.finalize().into();
    let mut trailer = [0; 32];
    r.input.read_exact(&mut trailer).unwrap();
    assert_eq!(segment, trailer);
    assert_eq!(segment, manifest.segment_digest);
    assert_eq!(r.input.read(&mut [0]).unwrap(), 0);
    assert_eq!(manifest.bytes, 14 + header_len as u64 + 8 + length + 56);
}
async fn large(store: Arc<dyn BufferStore>) {
    const BUDGET: usize = 256 * 1024;
    const PAYLOAD: usize = 64 * 1024;
    const CONSTANT: usize = 32 * 1024;
    let root = scratch("large");
    let mut c = config();
    c.max_transaction_bytes = 1_000_000_000;
    begin_measurement();
    let mut assembler = TransactionAssembler::new(
        source(),
        TxnId(5_u64.to_be_bytes().to_vec()),
        pos(5),
        Some(42),
        BUDGET,
        1_000_000_000,
        &root,
    )
    .unwrap();
    for ordinal in 0..4800 {
        assembler.push(event(5, ordinal, PAYLOAD)).unwrap();
    }
    assert!(assembler.peak_spool_bytes() <= BUDGET);
    let size = assembler.canonical_bytes();
    assert!(size >= 300_000_000);
    let raw_path = assembler.spilled_path().unwrap().to_owned();
    let txn = assembler.finish().unwrap();
    assert!(!raw_path.exists());
    assert_eq!(txn.event_count(), 4800);
    assert_eq!(txn.canonical_len(), size);
    let expected = txn.digest();
    let (tx, rx) = mpsc::channel();
    let mut writer =
        BufferWriter::open_with_scratch(store.clone(), c.clone(), Arc::new(PgOrder), tx, &root)
            .await
            .unwrap();
    writer.append_streaming(txn, Instant::now()).await.unwrap();
    assert_eq!(rx.try_recv().unwrap().position, pos(5));
    assert_eq!(writer.pending_transactions(), 0);
    drop(writer);
    let peak = end_measurement();
    eprintln!(
        "large_txn_bounded_memory: canonical={size}, budget={BUDGET}, event={PAYLOAD}, constant={CONSTANT}, global_peak_live={peak}"
    );
    assert!(
        peak <= BUDGET + PAYLOAD + CONSTANT,
        "global peak {peak} exceeds {}",
        BUDGET + PAYLOAD + CONSTANT
    );
    let m = manifest(store.as_ref(), &c, 0).await.unwrap();
    assert_eq!(m.txn_count, 1);
    // Download uses a real file and bounded backend reads. Independently decode
    // the published bytes rather than the assembler's private canonical file.
    let path = root.join("independent.seg");
    let mut output = std::fs::File::create(&path).unwrap();
    let object = store
        .get_into(&m.segment_key, &mut output)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(object.bytes, m.bytes);
    output.flush().unwrap();
    drop(output);
    independently_decode(std::fs::File::open(&path).unwrap(), &m, expected);
    // Reuse/truncate this large verification fixture before recovery, keeping
    // the qualification's simultaneous data below 1 GB.
    drop(std::fs::File::create(&path).unwrap());
    // Exercise file-backed recovery under the same bound, with 4800 events and
    // only one transaction history entry; no event digest index may be rebuilt.
    begin_measurement();
    let (tx, recovered) = mpsc::channel();
    let mut writer =
        BufferWriter::open_with_scratch(store.clone(), c.clone(), Arc::new(PgOrder), tx, &root)
            .await
            .unwrap();
    assert_eq!(recovered.try_recv().unwrap().position, pos(5));
    assert_eq!(writer.next_manifest_sequence(), 1);
    assert!(!writer.seal().await.unwrap());
    let recovery_peak = end_measurement();
    eprintln!("large_txn_bounded_memory: recovery_global_peak_live={recovery_peak}");
    assert!(
        recovery_peak <= BUDGET + PAYLOAD + CONSTANT,
        "recovery peak {recovery_peak}"
    );
    assert!(manifest(store.as_ref(), &c, 1).await.is_none());
    drop(writer);
    store.delete(&m.segment_key).await.unwrap();
    store
        .delete(&manifest_key(&c.prefix, &c.migration_id, &c.stream, 0))
        .await
        .unwrap();
}
#[tokio::test]
async fn large_txn_bounded_memory() {
    if run_allocation_test_isolated("large_txn_bounded_memory", false) {
        return;
    }
    let _test_lock = TEST_LOCK.lock().await;
    large(local()).await;
}

#[test]
fn assembler_finish_preserves_io_errors_from_truncated_scratch_frames() {
    let _test_lock = TEST_LOCK.blocking_lock();
    for retained_bytes in [0, 8] {
        let mut assembler = TransactionAssembler::new(
            source(),
            TxnId(1_u64.to_be_bytes().to_vec()),
            pos(1),
            Some(42),
            1,
            1024 * 1024,
            scratch("truncated-frame"),
        )
        .unwrap();
        assembler.push(event(1, 0, 11)).unwrap();
        let path = assembler.spilled_path().unwrap().to_owned();
        // Real filesystem fault: truncate either the frame prefix or its event
        // body, without replacing the assembler's retained file capability.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(retained_bytes)
            .unwrap();
        let Err(Error::Event(error)) = assembler.finish() else {
            panic!("truncated scratch frame did not return an event error");
        };
        let expected: EventError = std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into();
        assert_eq!(
            std::mem::discriminant(&error),
            std::mem::discriminant(&expected)
        );
        assert!(!path.exists());
    }
}

#[test]
fn assembler_transaction_limit_is_inclusive_and_oversize_rejection_is_sticky() {
    let _test_lock = TEST_LOCK.blocking_lock();
    let first = event(1, 0, 11);
    let second = event(1, 1, 12);
    let first_len = txn(1, 11).canonical_bytes().unwrap().len() as u64;
    let second_len = AssembledTxn::new(
        source(),
        first.txn.clone(),
        pos(1),
        Some(42),
        vec![first.clone(), second.clone()],
    )
    .unwrap()
    .canonical_bytes()
    .unwrap()
    .len() as u64;
    for delta in [0, 1] {
        let limit = first_len - delta;
        let mut assembler = TransactionAssembler::new(
            source(),
            first.txn.clone(),
            pos(1),
            Some(42),
            1,
            limit,
            scratch("assembler-limit"),
        )
        .unwrap();
        let (rejected, expected_bytes) = if delta == 0 {
            assert_eq!(assembler.push(first.clone()).unwrap(), Redelivery::New);
            assert_eq!(assembler.canonical_bytes(), first_len);
            (assembler.push(second.clone()), second_len)
        } else {
            (assembler.push(first.clone()), first_len)
        };
        assert!(matches!(
            rejected,
            Err(Error::OversizedTransaction { bytes, maximum })
                if bytes == expected_bytes && maximum == limit
        ));
        assert!(matches!(assembler.push(first.clone()), Err(Error::Blocked)));
        assert!(matches!(assembler.finish(), Err(Error::Blocked)));
    }
}

#[test]
fn assembler_accounting_and_spill_boundaries_match_canonical_bytes() {
    let _test_lock = TEST_LOCK.blocking_lock();
    let first = event(1, 0, 11);
    let second = event(1, 1, 12);
    let canonical = |events: Vec<Event>| {
        AssembledTxn::new(
            source(),
            TxnId(1_u64.to_be_bytes().to_vec()),
            pos(1),
            Some(42),
            events,
        )
        .unwrap()
        .canonical_bytes()
        .unwrap()
    };
    let empty_len = canonical(vec![]).len() as u64;
    let first_len = canonical(vec![first.clone()]).len() as u64;
    let expected = canonical(vec![first.clone(), second.clone()]);
    let mut encoded_event = Vec::new();
    first.write_canonical(&mut encoded_event).unwrap();
    let frame_bytes = 8 + encoded_event.len();
    for budget in [frame_bytes, frame_bytes - 1] {
        let root = scratch("accounting-boundary");
        let mut assembler = TransactionAssembler::new(
            source(),
            first.txn.clone(),
            pos(1),
            Some(42),
            budget,
            1_000_000_000,
            &root,
        )
        .unwrap();
        assert_eq!(assembler.canonical_bytes(), empty_len);
        assert_eq!(assembler.peak_spool_bytes(), 0);
        assert!(assembler.spilled_path().is_none());
        for wrong_source in [true, false] {
            let mut wrong = first.clone();
            if wrong_source {
                wrong.source.system_id = "other-source".into();
            } else {
                wrong.txn = TxnId(vec![255]);
            }
            assert!(matches!(
                assembler.push(wrong),
                Err(Error::Event(EventError::InvalidTransaction(
                    "assembler membership"
                )))
            ));
            assert_eq!(assembler.canonical_bytes(), empty_len);
        }
        assert_eq!(assembler.push(first.clone()).unwrap(), Redelivery::New);
        assert_eq!(assembler.canonical_bytes(), first_len);
        if budget == frame_bytes {
            assert!(assembler.spilled_path().is_none());
            assert!(assembler.peak_spool_bytes() >= frame_bytes);
        } else {
            assert!(assembler.spilled_path().is_some());
        }
        assert!(assembler.peak_spool_bytes() <= budget);
        assert_eq!(assembler.push(second.clone()).unwrap(), Redelivery::New);
        assert!(assembler.spilled_path().is_some());
        assert_eq!(assembler.canonical_bytes(), expected.len() as u64);
        assert!(assembler.peak_spool_bytes() <= budget);
        let mut prepared = assembler.finish().unwrap();
        assert_eq!(prepared.event_count(), 2);
        assert_eq!(prepared.canonical_len(), expected.len() as u64);
        let mut actual = Vec::new();
        prepared
            .canonical_reader()
            .unwrap()
            .read_to_end(&mut actual)
            .unwrap();
        assert_eq!(actual, expected);
        drop(prepared);
        assert_eq!(std::fs::read_dir(root).unwrap().count(), 0);
    }
}

#[test]
fn assembler_redelivery_and_scratch_cleanup() {
    let _test_lock = TEST_LOCK.blocking_lock();
    let root = scratch("assembler");
    let mut a = TransactionAssembler::new(
        source(),
        TxnId(1_u64.to_be_bytes().to_vec()),
        pos(1),
        Some(42),
        256,
        1024 * 1024,
        &root,
    )
    .unwrap();
    assert_eq!(a.push(event(1, 0, 1024)).unwrap(), Redelivery::New);
    let path = a.spilled_path().unwrap().to_owned();
    assert!(path.exists());
    assert_eq!(a.push(event(1, 0, 1024)).unwrap(), Redelivery::Noop);
    assert!(matches!(
        a.push(event(1, 0, 1025)),
        Err(Error::Event(EventError::RedeliveryConflict { .. }))
    ));
    let mut prepared = a.finish().unwrap();
    let mut bytes = Vec::new();
    prepared
        .canonical_reader()
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    assert_eq!(
        AssembledTxn::from_canonical_bytes(&bytes).unwrap(),
        txn(1, 1024)
    );
    assert!(!path.exists());
    drop(prepared);
    let mut abandoned = TransactionAssembler::new(
        source(),
        TxnId(1_u64.to_be_bytes().to_vec()),
        pos(1),
        None,
        1,
        1024 * 1024,
        &root,
    )
    .unwrap();
    abandoned.push(event(1, 0, 1024)).unwrap();
    let path = abandoned.spilled_path().unwrap().to_owned();
    drop(abandoned);
    assert!(!path.exists());
}

#[test]
fn assembler_startup_preserves_active_owners_and_reports_unknown_files() {
    let _test_lock = TEST_LOCK.blocking_lock();
    let root = scratch("assembler-startup");
    let abandoned = root.join(format!("txn-{}.skedaddle-scratch", uuid::Uuid::new_v4()));
    let unknown = root.join("keep-user-data");
    std::fs::write(&abandoned, b"abandoned").unwrap();
    std::fs::write(&unknown, b"unknown").unwrap();
    let mut active = TransactionAssembler::new(
        source(),
        TxnId(vec![1]),
        pos(1),
        None,
        1,
        1024 * 1024,
        &root,
    )
    .unwrap();
    assert_eq!(active.startup_reclamation().removed, vec![abandoned]);
    assert_eq!(active.startup_reclamation().retained, vec![unknown.clone()]);
    // Matching membership is required for the actual spilled event.
    let mut item = event(1, 0, 1024);
    item.txn = TxnId(vec![1]);
    active.push(item).unwrap();
    let active_path = active.spilled_path().unwrap().to_owned();
    let next = TransactionAssembler::new(
        source(),
        TxnId(vec![2]),
        pos(2),
        None,
        1,
        1024 * 1024,
        &root,
    )
    .unwrap();
    assert!(next.startup_reclamation().removed.is_empty());
    assert!(next.startup_reclamation().retained.contains(&active_path));
    assert!(active_path.exists());
    assert_eq!(std::fs::read(unknown).unwrap(), b"unknown");
}

#[tokio::test]
async fn malformed_positions_and_broken_chain_never_ack() {
    let _test_lock = TEST_LOCK.lock().await;
    let store = local();
    let mut c = config();
    c.segment_transactions = 1;
    let (mut w, rx) = open(store.clone(), c.clone()).await;
    w.append(txn(2, 10), Instant::now()).await.unwrap();
    rx.try_recv().unwrap();
    assert!(matches!(
        w.append(txn(1, 10), Instant::now()).await,
        Err(Error::Invalid(_))
    ));
    assert!(w.is_blocked());
    assert!(rx.try_recv().is_err());
    let m = manifest(store.as_ref(), &c, 0).await.unwrap();
    // Product backend deletion is exercised to model durable data loss.
    store.delete(&m.segment_key).await.unwrap();
    let (tx, rx) = mpsc::channel();
    assert!(matches!(
        BufferWriter::open(store, c, Arc::new(PgOrder), tx).await,
        Err(Error::Invalid("manifest segment missing"))
    ));
    assert!(rx.try_recv().is_err());
}

// Fault injection wraps real durable storage; it never emulates stored bytes.
// The normal ack matrix above exercises the exact same path without faults.
struct InterruptedManifest {
    inner: Arc<dyn BufferStore>,
    mode: std::sync::atomic::AtomicU8,
    upload_calls: AtomicUsize,
    manifest_calls: AtomicUsize,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl BufferStore for InterruptedManifest {
    fn report(&self) -> skedaddle::buffer::backends::BackendReport {
        self.inner.report()
    }
    async fn put_create_only(
        &self,
        key: &str,
        content: &[u8],
    ) -> skedaddle::buffer::backends::Result<skedaddle::buffer::backends::PutOutcome> {
        use std::sync::atomic::Ordering::SeqCst;
        self.manifest_calls.fetch_add(1, SeqCst);
        let mode = if key.contains("/manifest/") {
            self.mode.swap(0, SeqCst)
        } else {
            0
        };
        if mode == 1 {
            return Err(skedaddle::buffer::backends::Error::Store);
        }
        let outcome = self.inner.put_create_only(key, content).await?;
        if mode == 2 {
            return Err(skedaddle::buffer::backends::Error::UnknownPutOutcome);
        }
        if mode == 3 {
            self.reached.notify_one();
            self.release.notified().await;
        }
        Ok(outcome)
    }
    async fn put_file_create_only(
        &self,
        key: &str,
        file: std::fs::File,
    ) -> skedaddle::buffer::backends::Result<skedaddle::buffer::backends::PutOutcome> {
        self.upload_calls.fetch_add(1, AtomicOrdering::SeqCst);
        if self
            .mode
            .compare_exchange(4, 0, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
            .is_ok()
        {
            self.reached.notify_one();
            self.release.notified().await;
        }
        self.inner.put_file_create_only(key, file).await
    }
    async fn get_into(
        &self,
        key: &str,
        output: &mut (dyn Write + Send),
    ) -> skedaddle::buffer::backends::Result<Option<skedaddle::buffer::backends::ObjectDigest>>
    {
        self.inner.get_into(key, output).await
    }
    async fn get(&self, key: &str) -> skedaddle::buffer::backends::Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn list_prefix(
        &self,
        prefix: &str,
        token: Option<&str>,
        limit: usize,
    ) -> skedaddle::buffer::backends::Result<skedaddle::buffer::backends::ListPage> {
        self.inner.list_prefix(prefix, token, limit).await
    }
    async fn delete(&self, key: &str) -> skedaddle::buffer::backends::Result<()> {
        self.inner.delete(key).await
    }
}
async fn retry_matrix(store: Arc<dyn BufferStore>) {
    use std::sync::atomic::AtomicU8;
    for mode in [1, 2, 3, 4] {
        let mut c = config();
        c.segment_transactions = 1;
        let fault = Arc::new(InterruptedManifest {
            inner: store.clone(),
            mode: AtomicU8::new(mode),
            upload_calls: AtomicUsize::new(0),
            manifest_calls: AtomicUsize::new(0),
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let (mut w, rx) = open(fault.clone(), c.clone()).await;
        if mode == 3 || mode == 4 {
            tokio::select! {
                result = w.append(txn(1, 10), Instant::now()) => panic!("expected cancellation, got {result:?}"),
                _ = fault.reached.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(30)) => panic!("manifest seam timed out"),
            }
        } else {
            assert!(matches!(
                w.append(txn(1, 10), Instant::now()).await,
                Err(Error::Store(_))
            ));
        }
        assert!(rx.try_recv().is_err());
        assert_eq!(w.pending_transactions(), 1);
        assert!(matches!(w.checkpoint(), Err(Error::Blocked)));
        assert!(matches!(
            w.retain_redelivery_from(pos(1)),
            Err(Error::Blocked)
        ));
        assert_eq!(
            manifest(store.as_ref(), &c, 0).await.is_some(),
            mode != 1 && mode != 4
        );
        if mode == 3 || mode == 4 {
            fault.release.notify_one();
        }
        // A later input must finish the frozen first publication before it can
        // change any content attempted at mseq zero.
        w.append(txn(2, 10), Instant::now()).await.unwrap();
        assert_eq!(rx.try_recv().unwrap().position, pos(1));
        assert_eq!(rx.try_recv().unwrap().position, pos(2));
        let first = manifest(store.as_ref(), &c, 0).await.unwrap();
        let second = manifest(store.as_ref(), &c, 1).await.unwrap();
        assert_eq!(records(store.as_ref(), &first).await, vec![txn(1, 10)]);
        assert_eq!(records(store.as_ref(), &second).await, vec![txn(2, 10)]);
        assert_eq!(second.prev_digest, first.digest().unwrap());
        if mode == 4 {
            assert_eq!(fault.upload_calls.load(AtomicOrdering::SeqCst), 2);
        }
    }
}
#[tokio::test]
async fn interrupted_publication_freezes_batch_until_retry() {
    let _test_lock = TEST_LOCK.lock().await;
    retry_matrix(local()).await;
}

#[tokio::test]
async fn repeatedly_cancelled_seal_resumes_the_published_manifest_ack() {
    let _test_lock = TEST_LOCK.lock().await;
    let store = local();
    let c = config();
    let fault = Arc::new(InterruptedManifest {
        inner: store.clone(),
        mode: std::sync::atomic::AtomicU8::new(3),
        upload_calls: AtomicUsize::new(0),
        manifest_calls: AtomicUsize::new(0),
        reached: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let (mut writer, progress) = open(fault.clone(), c.clone()).await;
    writer.append(txn(1, 10), Instant::now()).await.unwrap();
    tokio::select! {
        result = writer.seal() => panic!("expected delayed acknowledgement: {result:?}"),
        _ = fault.reached.notified() => {},
        _ = tokio::time::sleep(Duration::from_secs(30)) => panic!("publication seam timed out"),
    }
    // Real publication has finished; only its acknowledgement is held back.
    assert!(manifest(store.as_ref(), &c, 0).await.is_some());
    for _ in 0..5 {
        assert!(
            tokio::time::timeout(Duration::from_millis(10), writer.seal())
                .await
                .is_err()
        );
        assert!(writer.checkpoint().is_err());
        assert_eq!(writer.next_manifest_sequence(), 0);
        assert!(progress.try_recv().is_err());
    }
    fault.release.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(30), writer.seal())
            .await
            .unwrap()
            .unwrap()
    );
    let checkpoint = writer.checkpoint().unwrap();
    assert_eq!(checkpoint.next_mseq, 1);
    assert_eq!(checkpoint.last_position, Some(pos(1)));
    assert_eq!(progress.try_recv().unwrap().mseq, 0);
    assert!(progress.try_recv().is_err());
    assert!(!writer.seal().await.unwrap());
    assert_eq!(
        writer.append(txn(1, 10), Instant::now()).await.unwrap(),
        Redelivery::Noop
    );
    assert_eq!(writer.checkpoint().unwrap(), checkpoint);
    assert!(progress.try_recv().is_err());
    assert_eq!(fault.upload_calls.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(fault.manifest_calls.load(AtomicOrdering::SeqCst), 1);
    assert!(manifest(store.as_ref(), &c, 1).await.is_none());
}

// Every backend enters the same suite; local named cases remain focused selectors.
async fn writer_suite(store: Arc<dyn BufferStore>) {
    #[cfg(feature = "test-crash")]
    ack_matrix(store.clone()).await;
    #[cfg(not(feature = "test-crash"))]
    eprintln!("SKIP writer crash seams: rerun with --features test-crash");
    identical(store.clone()).await;
    conflicting(store.clone()).await;
    oversized(store.clone()).await;
    stale(store.clone()).await;
    sealing(store.clone()).await;
    retry_matrix(store.clone()).await;
    recovery_transaction_limit(store.clone()).await;
    recovery_mismatches(store.clone()).await;
    recovery_envelopes(store.clone()).await;
    checkpoint_single_slot(store.clone()).await;
    checkpoint_pending_pair(store.clone()).await;
    #[cfg(feature = "test-crash")]
    checkpoint_crash(store.clone()).await;
    large(store).await;
}

#[tokio::test]
async fn local_writer_matrix() {
    if run_allocation_test_isolated("local_writer_matrix", false) {
        return;
    }
    let _test_lock = TEST_LOCK.lock().await;
    writer_suite(local()).await;
}

#[cfg(feature = "test-crash")]
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
async fn minio_writer_matrix() {
    if run_allocation_test_isolated("minio_writer_matrix", true) {
        return;
    }
    let _test_lock = TEST_LOCK.lock().await;
    #[cfg(not(feature = "test-crash"))]
    panic!("run harness/run.ps1 p3 with test-crash");
    #[cfg(feature = "test-crash")]
    writer_suite(remote_store("minio", None)).await;
}

#[tokio::test]
#[ignore = "requires MinIO/fake-gcs via harness/run.ps1 p3; plain cargo test explicitly skips infrastructure"]
async fn gcs_writer_matrix() {
    if run_allocation_test_isolated("gcs_writer_matrix", true) {
        return;
    }
    let _test_lock = TEST_LOCK.lock().await;
    #[cfg(not(feature = "test-crash"))]
    panic!("run harness/run.ps1 p3 with test-crash");
    #[cfg(feature = "test-crash")]
    writer_suite(remote_store("gcs", None)).await;
}

#[tokio::test]
async fn streaming_recovery_validates_chunkrows_and_corrupt_envelopes() {
    let _test_lock = TEST_LOCK.lock().await;
    recovery_envelopes(local()).await;
}

async fn recovery_envelopes(store: Arc<dyn BufferStore>) {
    use skedaddle::buffer::format::{Limits, SegmentEncoder, SegmentHeader, SegmentRecord};
    for chunk in [false, true] {
        let mut expected = txn(1, 127);
        if chunk {
            let mut item = event(1, 0, 127);
            item.kind = EventKind::ChunkRows;
            expected = AssembledTxn::new(source(), item.txn.clone(), pos(1), Some(42), vec![item])
                .unwrap();
        }
        let record = if chunk {
            SegmentRecord::ChunkRows(expected.clone())
        } else {
            SegmentRecord::Transaction(expected.clone())
        };
        let header = SegmentHeader {
            migration_id: "migration".into(),
            stream: "stream".into(),
            epoch: 1,
            first_pos: pos(1),
            last_pos: pos(1),
            record_count: 1,
        };
        let mut encoder = SegmentEncoder::new(Vec::new(), header, Limits::default()).unwrap();
        encoder.write_record(&record).unwrap();
        let (original, summary) = encoder.finish().unwrap();
        let record_start =
            14 + u32::from_be_bytes(original[10..14].try_into().unwrap()) as usize + 8;
        for corruption in [
            "none",
            "record-digest",
            "segment-digest",
            "truncated",
            "trailing",
        ] {
            let c = config();
            let mut bytes = original.clone();
            match corruption {
                "record-digest" => {
                    bytes[record_start + 15] ^= 1;
                }
                "segment-digest" => {
                    let last = bytes.len() - 1;
                    bytes[last] ^= 1;
                }
                "truncated" => {
                    bytes.truncate(bytes.len() - 1);
                }
                "trailing" => {
                    bytes.push(0);
                }
                _ => {}
            }
            let key = segment_key(&c.prefix, &c.migration_id, &c.stream, &pos(1), &pos(1), 1);
            let entry = ManifestEntry {
                mseq: 0,
                epoch: 1,
                prev_digest: [0; 32],
                segment_key: key.clone(),
                segment_digest: summary.digest,
                first_pos: pos(1),
                last_pos: pos(1),
                txn_count: 1,
                bytes: summary.bytes,
            };
            store.put_create_only(&key, &bytes).await.unwrap();
            store
                .put_create_only(
                    &manifest_key(&c.prefix, &c.migration_id, &c.stream, 0),
                    &entry.canonical_json().unwrap(),
                )
                .await
                .unwrap();
            let (tx, rx) = mpsc::channel();
            let recovered = BufferWriter::open_with_scratch(
                store.clone(),
                c,
                Arc::new(PgOrder),
                tx,
                scratch("recover-envelopes"),
            )
            .await;
            if corruption == "none" {
                let mut writer = recovered.unwrap();
                assert_eq!(rx.try_recv().unwrap().position, pos(1));
                let outcome = if chunk {
                    writer
                        .append_chunk_rows(expected.clone(), Instant::now())
                        .await
                        .unwrap()
                        .0
                } else {
                    writer
                        .append(expected.clone(), Instant::now())
                        .await
                        .unwrap()
                };
                assert_eq!(outcome, Redelivery::Noop);
                assert!(!writer.seal().await.unwrap());
            } else {
                assert!(recovered.is_err(), "accepted {corruption}, chunk={chunk}");
                assert!(rx.try_recv().is_err());
            }
        }
    }
}

#[test]
fn allocation_probe_counts_other_threads_without_subtracting_prior_allocations() {
    if run_allocation_test_isolated(
        "allocation_probe_counts_other_threads_without_subtracting_prior_allocations",
        false,
    ) {
        return;
    }
    let _test_lock = TEST_LOCK.blocking_lock();
    let prior = vec![0_u8; 64 * 1024];
    begin_measurement();
    let retained = vec![1_u8; 8192];
    let worker = std::thread::spawn(move || {
        drop(prior);
        vec![2_u8; 32768]
    })
    .join()
    .unwrap();
    std::hint::black_box((&retained, &worker));
    let peak = end_measurement();
    assert!(
        peak >= 8192 + 32768,
        "missed cross-thread live bytes: {peak}"
    );
    assert!(peak < 64 * 1024, "counted prior allocation: {peak}");
}

#[test]
fn streaming_record_hook_validates_and_poison_on_corruption() {
    let _test_lock = TEST_LOCK.blocking_lock();
    use skedaddle::buffer::format::{
        FormatError, Limits, SegmentEncoder, SegmentHeader, SegmentRecord,
    };
    for chunk in [false, true] {
        let mut item = event(1, 0, 127);
        if chunk {
            item.kind = EventKind::ChunkRows;
        }
        let expected =
            AssembledTxn::new(source(), item.txn.clone(), pos(1), Some(42), vec![item]).unwrap();
        let record = if chunk {
            SegmentRecord::ChunkRows(expected.clone())
        } else {
            SegmentRecord::Transaction(expected.clone())
        };
        let header = SegmentHeader {
            migration_id: "migration".into(),
            stream: "stream".into(),
            epoch: 1,
            first_pos: pos(1),
            last_pos: pos(1),
            record_count: 1,
        };
        let mut encoder = SegmentEncoder::new(Vec::new(), header, Limits::default()).unwrap();
        encoder.write_record(&record).unwrap();
        let (bytes, _) = encoder.finish().unwrap();
        let mut decoder =
            SegmentDecoder::new(Cursor::new(bytes.clone()), Limits::default()).unwrap();
        let mut txn = decoder
            .next_transaction_into(Cursor::new(Vec::new()))
            .unwrap()
            .unwrap();
        assert_eq!(txn.digest(), expected.digest);
        let mut canonical = Vec::new();
        txn.canonical_reader()
            .unwrap()
            .read_to_end(&mut canonical)
            .unwrap();
        assert_eq!(canonical, expected.canonical_bytes().unwrap());
        assert!(
            decoder
                .next_transaction_into(Cursor::new(Vec::new()))
                .unwrap()
                .is_none()
        );
        decoder.finish().unwrap();
        // Both inner/outer envelope corruption and trailer corruption poison
        // the new hook. Subsequent calls cannot resume from provisional output.
        for index in [
            bytes.len() - 1,
            14 + u32::from_be_bytes(bytes[10..14].try_into().unwrap()) as usize + 8 + 15,
        ] {
            let mut corrupt = bytes.clone();
            corrupt[index] ^= 1;
            let mut decoder = SegmentDecoder::new(Cursor::new(corrupt), Limits::default()).unwrap();
            let first = decoder.next_transaction_into(Cursor::new(Vec::new()));
            if first.is_ok() {
                assert!(
                    decoder
                        .next_transaction_into(Cursor::new(Vec::new()))
                        .is_err()
                );
            }
            assert!(matches!(
                decoder.next_transaction_into(Cursor::new(Vec::new())),
                Err(FormatError::Poisoned)
            ));
        }
    }
}

fn chunk_txn(id: u64, size: usize) -> AssembledTxn {
    let mut item = event(id, 0, size);
    item.kind = EventKind::ChunkRows;
    AssembledTxn::new(source(), item.txn.clone(), pos(id), Some(42), vec![item]).unwrap()
}

#[tokio::test]
async fn typed_chunk_rows_roundtrip_redelivery_and_ordinary_append() {
    let _test_lock = TEST_LOCK.lock().await;
    use skedaddle::buffer::format::{Limits, SegmentEncoder, SegmentHeader, SegmentRecord};
    let store = local();
    let c = config();
    let (mut writer, rx) = open(store.clone(), c.clone()).await;
    assert_eq!(
        writer.append(txn(1, 10), Instant::now()).await.unwrap(),
        Redelivery::New
    );
    assert!(rx.try_recv().is_err());
    let expected = chunk_txn(2, 1024);
    let (outcome, reference) = writer
        .append_chunk_rows(expected.clone(), Instant::now())
        .await
        .unwrap();
    assert_eq!(outcome, Redelivery::New);
    let progress = rx.try_recv().unwrap();
    assert_eq!(reference.mseq, progress.mseq);
    assert_eq!(reference.manifest_digest, progress.manifest_digest);
    let m = manifest(store.as_ref(), &c, reference.mseq).await.unwrap();
    assert_eq!(reference.manifest_digest, m.digest().unwrap());
    let bytes = store.get(&m.segment_key).await.unwrap().unwrap();
    let mut decoder = SegmentDecoder::new(Cursor::new(&bytes), Limits::default()).unwrap();
    let expected_records = [
        SegmentRecord::Transaction(txn(1, 10)),
        SegmentRecord::ChunkRows(expected.clone()),
    ];
    let mut encoder = SegmentEncoder::new(
        Vec::new(),
        SegmentHeader {
            migration_id: c.migration_id.clone(),
            stream: c.stream.clone(),
            epoch: c.epoch,
            first_pos: pos(1),
            last_pos: pos(2),
            record_count: 2,
        },
        Limits::default(),
    )
    .unwrap();
    for record in &expected_records {
        assert_eq!(decoder.next_record().unwrap().as_ref(), Some(record));
        encoder.write_record(record).unwrap();
    }
    assert!(decoder.next_record().unwrap().is_none());
    assert_eq!(decoder.finish().unwrap().1.digest, m.segment_digest);
    assert_eq!(
        encoder.finish().unwrap().0,
        bytes,
        "streaming encoding matches the existing record codec"
    );
    assert_eq!(
        writer
            .append_chunk_rows(expected.clone(), Instant::now())
            .await
            .unwrap(),
        (Redelivery::Noop, reference.clone())
    );
    assert!(rx.try_recv().is_err());
    assert!(matches!(
        writer
            .append_chunk_rows(chunk_txn(2, 1025), Instant::now())
            .await,
        Err(Error::Event(EventError::RedeliveryConflict { .. }))
    ));
    assert!(matches!(
        writer.append(expected.clone(), Instant::now()).await,
        Err(Error::Event(EventError::RedeliveryConflict { .. }))
    ));
    drop(writer);
    let (mut writer, recovered) = open(store.clone(), c.clone()).await;
    assert_eq!(
        recovered.try_recv().unwrap().manifest_digest,
        reference.manifest_digest
    );
    assert_eq!(
        writer
            .append_chunk_rows(expected, Instant::now())
            .await
            .unwrap(),
        (Redelivery::Noop, reference)
    );
    assert_eq!(
        writer.append(txn(3, 10), Instant::now()).await.unwrap(),
        Redelivery::New
    );
    assert!(recovered.try_recv().is_err());
    assert_eq!(
        writer
            .append_chunk_rows(chunk_txn(2, 1024), Instant::now())
            .await
            .unwrap()
            .0,
        Redelivery::Noop
    );
    assert_eq!(
        writer.pending_transactions(),
        1,
        "redelivery must not publish unrelated pending input"
    );
    assert!(manifest(store.as_ref(), &c, 1).await.is_none());
    writer.seal().await.unwrap();
    assert_eq!(
        records(
            store.as_ref(),
            &manifest(store.as_ref(), &c, 1).await.unwrap()
        )
        .await,
        vec![txn(3, 10)]
    );
}

#[tokio::test]
async fn typed_chunk_rows_retry_and_cancellation_keep_durable_evidence() {
    let _test_lock = TEST_LOCK.lock().await;
    use std::sync::atomic::AtomicU8;
    for mode in [1, 2, 3, 4] {
        let store = local();
        let c = config();
        let fault = Arc::new(InterruptedManifest {
            inner: store.clone(),
            mode: AtomicU8::new(mode),
            upload_calls: AtomicUsize::new(0),
            manifest_calls: AtomicUsize::new(0),
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let (mut writer, rx) = open(fault.clone(), c.clone()).await;
        if mode >= 3 {
            tokio::select! {
                result = writer.append_chunk_rows(chunk_txn(1, 10), Instant::now()) => panic!("expected cancellation: {result:?}"),
                _ = fault.reached.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(30)) => panic!("publication seam timed out"),
            }
        } else {
            assert!(matches!(
                writer
                    .append_chunk_rows(chunk_txn(1, 10), Instant::now())
                    .await,
                Err(Error::Store(_))
            ));
        }
        assert!(rx.try_recv().is_err());
        assert!(writer.checkpoint().is_err());
        assert_eq!(writer.pending_transactions(), 1);
        if mode == 3 || mode == 4 {
            fault.release.notify_one();
        }
        let (outcome, reference) = writer
            .append_chunk_rows(chunk_txn(1, 10), Instant::now())
            .await
            .unwrap();
        assert_eq!(outcome, Redelivery::Noop);
        assert_eq!(
            reference.manifest_digest,
            rx.try_recv().unwrap().manifest_digest
        );
        assert_eq!(
            reference.manifest_digest,
            manifest(store.as_ref(), &c, 0)
                .await
                .unwrap()
                .digest()
                .unwrap()
        );
        assert!(!writer.seal().await.unwrap());
        assert!(manifest(store.as_ref(), &c, 1).await.is_none());
        assert_eq!(fault.upload_calls.load(AtomicOrdering::SeqCst), 1);
    }
}

#[tokio::test]
#[cfg_attr(
    not(feature = "test-crash"),
    ignore = "crash seams require cargo test --features test-crash --test buffer_writer"
)]
async fn typed_chunk_rows_crash_never_returns_false_durable_evidence() {
    #[cfg(feature = "test-crash")]
    {
        let _test_lock = TEST_LOCK.lock().await;
        for point in [
            CrashPoint::BeforeSegmentPut,
            CrashPoint::AfterSegmentPut,
            CrashPoint::BeforeManifestPut,
            CrashPoint::AfterManifestPut,
        ] {
            let store = local();
            let c = config();
            let (mut writer, rx) = open(store.clone(), c.clone()).await;
            writer.inject_crash(point);
            assert!(
                matches!(writer.append_chunk_rows(chunk_txn(1, 10), Instant::now()).await, Err(Error::InjectedCrash(p)) if p == point)
            );
            assert!(rx.try_recv().is_err());
            assert!(writer.checkpoint().is_err());
            let durable = point == CrashPoint::AfterManifestPut;
            assert_eq!(manifest(store.as_ref(), &c, 0).await.is_some(), durable);
            let key = segment_key(
                &c.prefix,
                &c.migration_id,
                &c.stream,
                &pos(1),
                &pos(1),
                c.epoch,
            );
            assert_eq!(
                store.get(&key).await.unwrap().is_some(),
                point != CrashPoint::BeforeSegmentPut
            );
            drop(writer);
            let (mut writer, recovered) = open(store.clone(), c.clone()).await;
            assert_eq!(recovered.try_recv().is_ok(), durable);
            let (outcome, reference) = writer
                .append_chunk_rows(chunk_txn(1, 10), Instant::now())
                .await
                .unwrap();
            assert_eq!(
                outcome,
                if durable {
                    Redelivery::Noop
                } else {
                    Redelivery::New
                }
            );
            assert_eq!(reference.mseq, 0);
            assert_eq!(
                reference.manifest_digest,
                manifest(store.as_ref(), &c, 0)
                    .await
                    .unwrap()
                    .digest()
                    .unwrap()
            );
            assert!(manifest(store.as_ref(), &c, 1).await.is_none());
        }
    }
}

#[tokio::test]
async fn typed_chunk_rows_manifest_is_authenticated_by_chunk_done() {
    chunk_rows_manifest_authentication(false).await;
}

#[tokio::test]
async fn chunk_done_rejects_tampered_equal_h_segment_key_with_valid_object_and_digest() {
    chunk_rows_manifest_authentication(true).await;
}

async fn chunk_rows_manifest_authentication(tampered_key: bool) {
    let _test_lock = TEST_LOCK.lock().await;
    use sha2::{Digest, Sha256};
    use skedaddle::{
        buffer::reader::ReaderConfig,
        coordinator::{
            Transition,
            chunk::{ChunkPlan, ChunkRowsRef},
            journal::{Entry, Journal, local_store},
        },
        mapping::{PgType, Value},
        pg::chunk::{Direction, KeyColumn, QualifiedName, TableKey},
    };
    let plan = ChunkPlan::new(
        TableKey {
            table: "public.t".into(),
            relation: QualifiedName {
                schema: "public".into(),
                name: "t".into(),
            },
            columns: vec![KeyColumn {
                name: "k".into(),
                pg_type: PgType::Int4,
                sql_type: QualifiedName {
                    schema: "pg_catalog".into(),
                    name: "int4".into(),
                },
                collation: None,
                direction: Direction::Asc,
            }],
        },
        100,
        vec![vec![Value::Int4(10)]],
    )
    .unwrap();
    let chunk = &plan.ranges[0];
    let mut identity = Sha256::new();
    identity.update(b"skedaddle-chunk-record-v1\0");
    identity.update(serde_json::to_vec(&chunk.id).unwrap());
    identity.update(1_u64.to_be_bytes());
    identity.update(100_u64.to_be_bytes());
    let id = TxnId(identity.finalize().to_vec());
    let mut item = event(100, 0, 10);
    item.kind = EventKind::ChunkRows;
    item.txn = id.clone();
    let rows = AssembledTxn::new(source(), id, pos(100), Some(42), vec![item]).unwrap();
    let store = local();
    let c = config();
    let (mut writer, _) = open(store.clone(), c.clone()).await;
    if tampered_key {
        writer.append(txn(100, 1), Instant::now()).await.unwrap();
        writer.seal().await.unwrap();
    }
    let (_, mut reference) = writer
        .append_chunk_rows(rows, Instant::now())
        .await
        .unwrap();
    if tampered_key {
        let mut entry = manifest(store.as_ref(), &c, reference.mseq).await.unwrap();
        let bytes = store.get(&entry.segment_key).await.unwrap().unwrap();
        assert!(entry.segment_key.ends_with(".chunk-rows"));
        // The bytes and reference digest remain valid; only the derived key
        // contract is violated. Store the object too, so GET cannot hide it.
        entry.segment_key.push_str("-tampered");
        entry.mseq += 1;
        entry.prev_digest = reference.manifest_digest;
        store
            .put_create_only(&entry.segment_key, &bytes)
            .await
            .unwrap();
        store
            .put_create_only(
                &manifest_key(&c.prefix, &c.migration_id, &c.stream, entry.mseq),
                &entry.canonical_json().unwrap(),
            )
            .await
            .unwrap();
        reference.mseq = entry.mseq;
        reference.manifest_digest = entry.digest().unwrap();
    }
    for (index, attempt, high, bad_digest, succeeds) in [
        (0, 1, 100, false, true),
        (1, 1, 100, false, false),
        (0, 2, 100, false, false),
        (0, 1, 101, false, false),
        (0, 1, 100, true, false),
    ] {
        let journal = Journal::new(
            Arc::new(local_store(scratch("chunk-done")).unwrap()),
            "writer-test",
            "migration",
            Duration::from_secs(30),
        )
        .unwrap()
        .with_chunk_buffer(
            store.clone(),
            ReaderConfig {
                prefix: c.prefix.clone(),
                migration_id: c.migration_id.clone(),
                stream: c.stream.clone(),
                limits: Default::default(),
                required_next_mseq: 0,
            },
            Arc::new(PgOrder),
        );
        let saved_plan = journal.publish_chunk_plan(&plan).await.unwrap();
        let range = plan.ranges[index].clone();
        for transition in [
            Transition::AcquireOwnership { epoch: 1 },
            Transition::ChunkPlanned { plan: saved_plan },
            Transition::ChunkStarted {
                chunk: range.clone(),
                attempt,
            },
        ] {
            let entry = Entry::new(
                &journal.load(false).await.unwrap(),
                transition,
                "writer-test",
            )
            .unwrap();
            journal.put(&entry).await.unwrap();
        }
        let mut digest = reference.manifest_digest;
        if bad_digest {
            digest[0] ^= 1;
        }
        let entry = Entry::new(
            &journal.load(false).await.unwrap(),
            Transition::ChunkDone {
                chunk: range,
                attempt,
                high_lsn: high,
                chunk_rows: ChunkRowsRef {
                    mseq: reference.mseq,
                    manifest_digest: digest,
                },
            },
            "writer-test",
        )
        .unwrap();
        let result = journal.put(&entry).await;
        assert_eq!(
            result.is_ok(),
            succeeds && !tampered_key,
            "chunk={index}, attempt={attempt}, H={high}, digest={bad_digest}: {result:?}"
        );
        if tampered_key {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("ChunkRows manifest identity/digest")
            );
        } else if succeeds {
            journal.load(false).await.unwrap();
        }
    }
}

fn ordered_record(id: u64, position: u64, chunk_rows: bool, foreign: bool) -> AssembledTxn {
    let mut item = event(id, 0, 8);
    item.position = pos(position);
    if chunk_rows {
        item.kind = EventKind::ChunkRows;
    }
    if foreign {
        item.source.lineage = "other".into();
    }
    AssembledTxn::new(
        item.source.clone(),
        item.txn.clone(),
        pos(position),
        Some(42),
        vec![item],
    )
    .unwrap()
}

#[tokio::test]
async fn equal_h_admission_rejects_every_other_tie_and_decrease_and_fences() {
    let _test_lock = TEST_LOCK.lock().await;
    // (accepted prefix, rejected record): distinct identities avoid redelivery no-ops.
    let cases = [
        (vec![(5, false, false)], (5, false, false)),
        (vec![(5, false, false), (5, true, false)], (5, true, false)),
        (vec![(5, true, false)], (5, true, false)),
        (vec![(5, false, false), (6, false, false)], (5, true, false)),
        (vec![(5, false, false)], (4, true, false)),
        (vec![(5, false, false)], (4, false, false)),
        (vec![(5, false, false)], (5, true, true)),
        (vec![(5, false, false), (5, true, false)], (5, false, false)),
    ];
    for seal in [false, true] {
        for (prefix, rejected) in &cases {
            let (mut writer, _) = open(local(), config()).await;
            for (index, &(position, chunk_rows, foreign)) in prefix.iter().enumerate() {
                let txn = ordered_record(index as u64 + 10, position, chunk_rows, foreign);
                if chunk_rows {
                    writer.append_chunk_rows(txn, Instant::now()).await.unwrap();
                } else {
                    writer.append(txn, Instant::now()).await.unwrap();
                }
                if seal {
                    writer.seal().await.unwrap();
                }
            }
            let (position, chunk_rows, foreign) = *rejected;
            let rejected_txn = ordered_record(99, position, chunk_rows, foreign);
            let result = if chunk_rows {
                writer
                    .append_chunk_rows(rejected_txn, Instant::now())
                    .await
                    .map(|(outcome, _)| outcome)
            } else {
                writer.append(rejected_txn, Instant::now()).await
            };
            assert!(
                matches!(result, Err(Error::Invalid(_))),
                "{prefix:?} -> {rejected:?}: {result:?}"
            );
            assert!(writer.is_blocked());
            assert!(matches!(
                writer.append(txn(100, 1), Instant::now()).await,
                Err(Error::Blocked)
            ));
        }
    }
}

#[tokio::test]
async fn equal_h_checkpoint_and_genesis_recovery_preserve_the_single_slot() {
    let _test_lock = TEST_LOCK.lock().await;
    checkpoint_single_slot(local()).await;
}

async fn checkpoint_single_slot(store: Arc<dyn BufferStore>) {
    use skedaddle::buffer::writer::WriterCheckpoint;
    for checkpoint_mode in 0..4 {
        let c = config();
        let (mut writer, _) = open(store.clone(), c.clone()).await;
        writer
            .append(ordered_record(10, 5, false, false), Instant::now())
            .await
            .unwrap();
        writer.seal().await.unwrap();
        writer.retain_redelivery_from(pos(5)).unwrap();
        let mut at_h = writer.checkpoint().unwrap();
        assert!(!at_h.last_record.as_ref().unwrap().chunk_rows);
        // Boundary state does not depend on retained transaction identities.
        at_h.redelivery.clear();
        let chunk = ordered_record(11, 5, true, false);
        let mut reference = None;
        if checkpoint_mode >= 2 {
            reference = Some(
                writer
                    .append_chunk_rows(chunk.clone(), Instant::now())
                    .await
                    .unwrap()
                    .1,
            );
        }
        let checkpoint = match checkpoint_mode {
            0 => WriterCheckpoint::default(),
            1 | 2 => at_h,
            _ => {
                let mut checkpoint = writer.checkpoint().unwrap();
                assert!(checkpoint.last_record.as_ref().unwrap().chunk_rows);
                checkpoint.redelivery.clear();
                checkpoint
            }
        };
        drop(writer);
        let (tx, _) = mpsc::channel();
        let mut writer = BufferWriter::open_from_checkpoint(
            store.clone(),
            c.clone(),
            checkpoint,
            Arc::new(PgOrder),
            tx,
            scratch("equal-h-resume"),
        )
        .await
        .unwrap();
        if checkpoint_mode < 2 {
            reference = Some(
                writer
                    .append_chunk_rows(chunk.clone(), Instant::now())
                    .await
                    .unwrap()
                    .1,
            );
        }
        let reference = reference.unwrap();
        assert_eq!(reference.mseq, 1);
        let h = manifest(store.as_ref(), &c, 0).await.unwrap();
        let chunk_manifest = manifest(store.as_ref(), &c, 1).await.unwrap();
        assert_ne!(h.segment_key, chunk_manifest.segment_key);
        assert_eq!(writer.checkpoint().unwrap().last_position, Some(pos(5)));
        assert!(writer.checkpoint().unwrap().last_record.unwrap().chunk_rows);
        if checkpoint_mode != 3 {
            assert_eq!(
                writer
                    .append_chunk_rows(chunk, Instant::now())
                    .await
                    .unwrap(),
                (Redelivery::Noop, reference)
            );
        }
        assert!(
            writer
                .append_chunk_rows(ordered_record(12, 5, true, false), Instant::now())
                .await
                .is_err()
        );
        assert!(writer.is_blocked());
    }
}

#[cfg(feature = "test-crash")]
#[tokio::test]
async fn equal_h_chunk_crash_recovers_between_h_and_its_chunk_rows() {
    let _test_lock = TEST_LOCK.lock().await;
    checkpoint_crash(local()).await;
}

#[cfg(feature = "test-crash")]
async fn checkpoint_crash(store: Arc<dyn BufferStore>) {
    for point in [
        CrashPoint::BeforeSegmentPut,
        CrashPoint::AfterSegmentPut,
        CrashPoint::BeforeManifestPut,
        CrashPoint::AfterManifestPut,
    ] {
        let c = config();
        let (mut writer, _) = open(store.clone(), c.clone()).await;
        writer
            .append(ordered_record(10, 5, false, false), Instant::now())
            .await
            .unwrap();
        writer.seal().await.unwrap();
        let checkpoint = writer.checkpoint().unwrap();
        writer.inject_crash(point);
        let chunk = ordered_record(11, 5, true, false);
        assert!(
            matches!(writer.append_chunk_rows(chunk.clone(), Instant::now()).await, Err(Error::InjectedCrash(p)) if p == point)
        );
        drop(writer);
        let (tx, _) = mpsc::channel();
        let mut writer = BufferWriter::open_from_checkpoint(
            store.clone(),
            c,
            checkpoint,
            Arc::new(PgOrder),
            tx,
            scratch("equal-h-crash"),
        )
        .await
        .unwrap();
        let (outcome, reference) = writer
            .append_chunk_rows(chunk, Instant::now())
            .await
            .unwrap();
        assert_eq!(reference.mseq, 1);
        assert_eq!(
            outcome,
            if point == CrashPoint::AfterManifestPut {
                Redelivery::Noop
            } else {
                Redelivery::New
            }
        );
        assert!(
            writer
                .append_chunk_rows(ordered_record(12, 5, true, false), Instant::now())
                .await
                .is_err()
        );
        assert!(writer.is_blocked());
    }
}

#[tokio::test]
async fn equal_h_pending_pair_recovers_from_one_manifest() {
    let _test_lock = TEST_LOCK.lock().await;
    checkpoint_pending_pair(local()).await;
}

async fn checkpoint_pending_pair(store: Arc<dyn BufferStore>) {
    let c = config();
    let (mut writer, _) = open(store.clone(), c.clone()).await;
    writer
        .append(ordered_record(10, 5, false, false), Instant::now())
        .await
        .unwrap();
    assert_eq!(writer.checkpoint().unwrap().last_position, None);
    let chunk = ordered_record(11, 5, true, false);
    let (_, reference) = writer
        .append_chunk_rows(chunk.clone(), Instant::now())
        .await
        .unwrap();
    assert_eq!(reference.mseq, 0);
    assert_eq!(manifest(store.as_ref(), &c, 0).await.unwrap().txn_count, 2);
    drop(writer);
    let (mut writer, _) = open(store, c).await;
    assert!(writer.checkpoint().unwrap().last_record.unwrap().chunk_rows);
    assert_eq!(
        writer
            .append_chunk_rows(chunk, Instant::now())
            .await
            .unwrap(),
        (Redelivery::Noop, reference)
    );
    assert!(
        writer
            .append_chunk_rows(ordered_record(12, 5, true, false), Instant::now())
            .await
            .is_err()
    );
    assert!(writer.is_blocked());
}
