use async_trait::async_trait;
use skedaddle::{
    buffer::{
        backends::{self, BackendReport, BufferStore, ListPage, ObjectBufferStore, PutOutcome},
        format::{
            Limits, ManifestEntry, SegmentEncoder, SegmentHeader, SegmentRecord, segment_key,
        },
        reader::{
            Backpressure, BufferBounds, BufferReader, Error, ManifestRange, OrphanFence,
            ReaderCheckpoint, ReaderConfig, list_orphan_segments,
        },
        writer::{BufferWriter, WriterConfig},
    },
    event::{
        AssembledTxn, ContentDigest, EventError, Position, PositionOrder, SourceIdentity, TxnId,
    },
};
use std::{
    cmp::Ordering,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        mpsc,
    },
    time::{Duration, Instant},
};

struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "reader-test"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
fn pos(n: u64) -> Position {
    Position::new("reader-test", n.to_be_bytes().to_vec())
}
fn txn(n: u64) -> AssembledTxn {
    AssembledTxn::new(
        SourceIdentity {
            engine: "test".into(),
            system_id: "s".into(),
            lineage: "l".into(),
            db_name: "d".into(),
            stream: "s".into(),
        },
        TxnId(n.to_be_bytes().to_vec()),
        pos(n),
        Some(7),
        vec![],
    )
    .unwrap()
}
fn config() -> ReaderConfig {
    ReaderConfig {
        prefix: format!("reader/{}", uuid::Uuid::new_v4()),
        migration_id: "m".into(),
        stream: "s".into(),
        limits: Limits::default(),
        required_next_mseq: 0,
    }
}
fn root() -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT")
            .expect("set SKEDADDLE_TEST_ROOT under J:/gigglesdb-work"),
    )
    .join(format!("p3-reader-{}", uuid::Uuid::new_v4()));
    assert!(path.starts_with("J:/gigglesdb-work"));
    std::fs::create_dir_all(&path).unwrap();
    path
}
fn local() -> Arc<dyn BufferStore> {
    Arc::new(ObjectBufferStore::local(root(), Duration::from_secs(30)).unwrap())
}

#[tokio::test]
async fn long_mysql_gtid_positions_round_trip_equal_h_and_resume() {
    use skedaddle::{
        event::{Event, EventKind, Row},
        mysql::{MysqlOrder, MysqlPosition},
    };

    let order = Arc::new(MysqlOrder::gtid("long-buffer-test").unwrap());
    let executed = (0..256)
        .map(|n| format!("{n:08x}-0000-0000-0000-000000000000:1-10"))
        .collect::<Vec<_>>()
        .join(",");
    let native = MysqlPosition::Gtid {
        set: executed.parse().unwrap(),
        server_uuid: "00000000-0000-0000-0000-000000000000".into(),
        file: "mysql-bin.000001".into(),
        offset: 1234,
    };
    let position = Position::new(order.domain(), native.encode().unwrap());
    assert!(position.bytes().len() > 8 * 1024);
    assert_eq!(MysqlPosition::decode(position.bytes()).unwrap(), native);
    let record = |id: u8, chunk_rows: bool| {
        let mut source = txn(1).source;
        source.engine = "mysql".into();
        let txn = TxnId(vec![id]);
        let event = Event {
            source: source.clone(),
            txn: txn.clone(),
            ordinal_in_txn: 0,
            position: position.clone(),
            schema_version: 1,
            kind: if chunk_rows {
                EventKind::ChunkRows
            } else {
                EventKind::Insert
            },
            table: Some("db.t".into()),
            key: Row::new(),
            before: None,
            after: Some(Row::new()),
            unchanged_cols: Default::default(),
        };
        AssembledTxn::new(source, txn, position.clone(), Some(7), vec![event]).unwrap()
    };
    let transaction = record(1, false);
    let chunk = record(2, true);
    // Exercise both one segment and the distinct equal-H suffix segment.
    for seal_at_h in [false, true] {
        let store = local();
        let mut c = config();
        let writer_config = WriterConfig {
            prefix: c.prefix.clone(),
            migration_id: c.migration_id.clone(),
            stream: c.stream.clone(),
            epoch: 1,
            segment_bytes: 1024 * 1024,
            segment_transactions: 100,
            segment_age: Duration::from_secs(60),
            max_transaction_bytes: 1024 * 1024,
        };
        let (tx, _) = mpsc::channel();
        let mut writer = BufferWriter::open_with_scratch(
            store.clone(),
            writer_config.clone(),
            order.clone(),
            tx,
            root(),
        )
        .await
        .unwrap();
        writer
            .append(transaction.clone(), Instant::now())
            .await
            .unwrap();
        if seal_at_h {
            writer.seal().await.unwrap();
        }
        let at_h = writer.checkpoint().unwrap();
        writer
            .append_chunk_rows(chunk.clone(), Instant::now())
            .await
            .unwrap();
        c.required_next_mseq = if seal_at_h { 2 } else { 1 };
        assert_eq!(writer.next_manifest_sequence(), c.required_next_mseq);
        let durable = writer.checkpoint().unwrap();
        drop(writer);
        // Streaming writer recovery checks full endpoints and envelope kinds.
        let (tx, _) = mpsc::channel();
        let recovered = BufferWriter::open_from_checkpoint(
            store.clone(),
            writer_config,
            at_h,
            order.clone(),
            tx,
            root(),
        )
        .await
        .unwrap();
        assert_eq!(recovered.checkpoint().unwrap(), durable);
        drop(recovered);

        let mut reader = BufferReader::new(
            store.clone(),
            c.clone(),
            ReaderCheckpoint::default(),
            order.clone(),
        )
        .unwrap();
        assert_eq!(reader.next_txn().await.unwrap(), Some(transaction.clone()));
        let checkpoint = reader.checkpoint().clone();
        assert_eq!(checkpoint.next_mseq, u64::from(seal_at_h));
        if seal_at_h {
            reader =
                BufferReader::new(store.clone(), c.clone(), checkpoint, order.clone()).unwrap();
        }
        assert_eq!(reader.next_txn().await.unwrap(), Some(chunk.clone()));
        assert!(reader.next_txn().await.unwrap().is_none());
        assert_eq!(reader.checkpoint().last_position.as_ref(), Some(&position));
        assert!(reader.checkpoint().last_record.as_ref().unwrap().chunk_rows);

        // Real local backend pagination: bounded filenames, including suffix.
        let mut token = None;
        let mut keys = vec![];
        loop {
            let page = store
                .list_prefix(&c.segment_prefix(), token.as_deref(), 1)
                .await
                .unwrap();
            keys.extend(page.objects.into_iter().map(|o| o.key));
            token = page.next;
            if token.is_none() {
                break;
            }
        }
        assert_eq!(keys.len() as u64, c.required_next_mseq);
        assert!(keys.iter().all(|key| key.len() < 300));
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        if seal_at_h {
            assert_eq!(keys[1], format!("{}.chunk-rows", keys[0]));
        }
    }
}

#[tokio::test]
async fn reader_and_writer_reject_old_segment_key_scheme_before_object_lookup() {
    use skedaddle::buffer::format::FormatError;
    let store = local();
    let c = config();
    let (_, mut entry) = encoded(&c, 1, 1, 1);
    entry.segment_key = format!("{}x64.01-x64.01-1.seg", c.segment_prefix());
    store
        .put_create_only(&c.manifest_key(0), &entry.canonical_json().unwrap())
        .await
        .unwrap();
    let mut reader = BufferReader::new(
        store.clone(),
        c.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    assert!(matches!(
        reader.next_txn().await,
        Err(Error::Format(FormatError::UnsupportedFormatVersion {
            format: "segment-key",
            found: None,
            expected: 2,
        }))
    ));
    let writer_config = WriterConfig {
        prefix: c.prefix,
        migration_id: c.migration_id,
        stream: c.stream,
        epoch: 1,
        segment_bytes: 1024,
        segment_transactions: 100,
        segment_age: Duration::from_secs(1),
        max_transaction_bytes: 1024,
    };
    let (tx, _) = mpsc::channel();
    assert!(matches!(
        BufferWriter::open_with_scratch(store, writer_config, Arc::new(Order), tx, root(),).await,
        Err(skedaddle::buffer::writer::Error::Format(
            FormatError::UnsupportedFormatVersion {
                format: "segment-key",
                found: None,
                expected: 2,
            }
        ))
    ));
}
fn encoded(c: &ReaderConfig, start: u64, count: u64, epoch: u64) -> (Vec<u8>, ManifestEntry) {
    let mut encoder = SegmentEncoder::new(
        Vec::new(),
        SegmentHeader {
            migration_id: c.migration_id.clone(),
            stream: c.stream.clone(),
            epoch,
            first_pos: pos(start),
            last_pos: pos(start + count - 1),
            record_count: count,
        },
        c.limits,
    )
    .unwrap();
    for n in start..start + count {
        encoder
            .write_record(&SegmentRecord::Transaction(txn(n)))
            .unwrap();
    }
    let (bytes, summary) = encoder.finish().unwrap();
    (
        bytes,
        ManifestEntry {
            mseq: 0,
            epoch,
            prev_digest: [0; 32],
            segment_key: segment_key(
                &c.prefix,
                &c.migration_id,
                &c.stream,
                &pos(start),
                &pos(start + count - 1),
                epoch,
            ),
            segment_digest: summary.digest,
            first_pos: pos(start),
            last_pos: pos(start + count - 1),
            txn_count: count,
            bytes: summary.bytes,
        },
    )
}
async fn publish(
    store: &dyn BufferStore,
    c: &ReaderConfig,
    mut entry: ManifestEntry,
    bytes: &[u8],
    mseq: u64,
    prev: ContentDigest,
) -> ContentDigest {
    entry.mseq = mseq;
    entry.prev_digest = prev;
    store
        .put_create_only(&entry.segment_key, bytes)
        .await
        .unwrap();
    store
        .put_create_only(&c.manifest_key(mseq), &entry.canonical_json().unwrap())
        .await
        .unwrap();
    entry.digest().unwrap()
}

// Instrumentation delegates every operation to a real backend; it supplies no
// synthetic object responses and changes no store semantics.
struct Counted {
    inner: Arc<dyn BufferStore>,
    gets: AtomicU64,
    lists: AtomicU64,
    keys: std::sync::Mutex<Vec<String>>,
}
#[async_trait]
impl BufferStore for Counted {
    fn report(&self) -> BackendReport {
        self.inner.report()
    }
    async fn put_create_only(&self, k: &str, b: &[u8]) -> backends::Result<PutOutcome> {
        self.inner.put_create_only(k, b).await
    }
    async fn get(&self, k: &str) -> backends::Result<Option<Vec<u8>>> {
        self.gets.fetch_add(1, AtomicOrdering::Relaxed);
        self.keys.lock().unwrap().push(k.to_owned());
        self.inner.get(k).await
    }
    async fn list_prefix(&self, p: &str, t: Option<&str>, l: usize) -> backends::Result<ListPage> {
        self.lists.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.list_prefix(p, t, l).await
    }
    async fn delete(&self, k: &str) -> backends::Result<()> {
        self.inner.delete(k).await
    }
}
async fn orphan_matrix(store: Arc<dyn BufferStore>) {
    // Include a literal manifest path in the prefix to test layout derivation.
    let mut c = config();
    c.prefix.push_str("/manifest/nested");
    let (bytes, entry) = encoded(&c, 1, 2, 1);
    let digest = publish(store.as_ref(), &c, entry, &bytes, 0, [0; 32]).await;
    let (orphan, entry) = encoded(&c, 3, 1, 1);
    store
        .put_create_only(&entry.segment_key, &orphan)
        .await
        .unwrap();
    let orphan_key = entry.segment_key;
    let (active, entry) = encoded(&c, 4, 1, 2);
    store
        .put_create_only(&entry.segment_key, &active)
        .await
        .unwrap();
    let corrupt_key = format!("{}broken.seg", c.segment_prefix());
    store
        .put_create_only(&corrupt_key, b"corrupt")
        .await
        .unwrap();
    let counted = Arc::new(Counted {
        inner: store.clone(),
        gets: AtomicU64::new(0),
        lists: AtomicU64::new(0),
        keys: std::sync::Mutex::new(Vec::new()),
    });
    let mut reader = BufferReader::new(
        counted.clone(),
        c.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    assert_eq!(reader.next_txn().await.unwrap(), Some(txn(1)));
    assert_eq!(reader.checkpoint().next_mseq, 0); // Partial segment replay is safe.
    assert_eq!(reader.next_txn().await.unwrap(), Some(txn(2)));
    assert_eq!(reader.checkpoint().next_mseq, 1);
    assert!(reader.next_txn().await.unwrap().is_none());
    assert_eq!(counted.lists.load(AtomicOrdering::Relaxed), 0);
    let range = ManifestRange {
        start: ReaderCheckpoint::default(),
        end_mseq: 1,
        end_digest: digest,
    };
    let mut token = None;
    let mut found = vec![];
    let mut unverified = vec![];
    loop {
        let page = list_orphan_segments(
            store.as_ref(),
            &c,
            &range,
            OrphanFence { before_epoch: 2 },
            token.as_deref(),
            1,
            100,
        )
        .await
        .unwrap();
        found.extend(page.orphans.into_iter().map(|o| o.key().to_owned()));
        unverified.extend(page.unverified);
        token = page.next;
        if token.is_none() {
            break;
        }
    }
    assert_eq!(found, vec![orphan_key]);
    assert_eq!(unverified, vec![corrupt_key]);
}
#[tokio::test]
async fn orphan_segments_ignored_and_listed() {
    orphan_matrix(local()).await;
}

#[tokio::test]
async fn broken_chain_and_segment_are_recovery_required_before_yield() {
    reader_corruption(local()).await;
}

async fn reader_corruption(store: Arc<dyn BufferStore>) {
    for mode in 0..7 {
        let mut c = config();
        let (mut bytes, mut entry) = encoded(&c, 1, 2, 1);
        match mode {
            0 => entry.prev_digest = [1; 32],
            1 => {
                let n = bytes.len();
                bytes[n - 1] ^= 1;
            }
            2 => entry.segment_digest = [1; 32],
            3 => entry.mseq = 1,
            4 => {
                c.required_next_mseq = 1;
            }
            5 => entry.segment_key.push_str("wrong"),
            _ => {} // Valid manifest key but missing referenced segment.
        }
        if mode != 4 {
            if mode != 5 && mode != 6 {
                store
                    .put_create_only(&entry.segment_key, &bytes)
                    .await
                    .unwrap();
            }
            store
                .put_create_only(&c.manifest_key(0), &entry.canonical_json().unwrap())
                .await
                .unwrap();
        }
        let mut reader = BufferReader::new(
            store.clone(),
            c,
            ReaderCheckpoint::default(),
            Arc::new(Order),
        )
        .unwrap();
        assert!(
            matches!(reader.next_txn().await, Err(Error::RecoveryRequired(_))),
            "mode {mode}"
        );
        assert!(matches!(
            reader.next_txn().await,
            Err(Error::RecoveryRequired(_))
        ));
        assert_eq!(reader.checkpoint().next_mseq, 0);
    }
}
#[tokio::test]
async fn durable_progress_distinguishes_idle_tail_from_missing_manifest() {
    reader_tail(local()).await;
}

async fn reader_tail(store: Arc<dyn BufferStore>) {
    let mut reader = BufferReader::new(
        store,
        config(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    assert!(reader.next_txn().await.unwrap().is_none());
    reader.require_through(1);
    assert!(matches!(
        reader.next_txn().await,
        Err(Error::RecoveryRequired(_))
    ));
}
#[tokio::test]
async fn checkpoint_chain_anchor_and_position_order_are_checked() {
    reader_anchor(local()).await;
}

async fn reader_anchor(store: Arc<dyn BufferStore>) {
    for mode in 0..3 {
        let c = config();
        let (bytes, mut entry) = encoded(&c, 1, 1, 1);
        let checkpoint = ReaderCheckpoint {
            next_mseq: 10,
            prev_digest: [7; 32],
            last_epoch: 2,
            last_position: Some(pos(2)),
            version: skedaddle::buffer::format::CHECKPOINT_VERSION,
            last_record: Some(skedaddle::buffer::format::RecordBoundary {
                source: txn(1).source,
                chunk_rows: false,
            }),
        };
        entry.mseq = 10;
        entry.prev_digest = checkpoint.prev_digest;
        entry.epoch = 2;
        if mode == 0 {
            entry.prev_digest = [8; 32];
        }
        if mode == 1 {
            entry.epoch = 1;
        }
        if mode == 2 {
            entry.epoch = 1;
        } // Separate below uses valid header/epoch but reversed position.
        if mode == 2 {
            let checkpoint = ReaderCheckpoint {
                last_epoch: 1,
                ..checkpoint
            };
            entry.segment_key =
                segment_key(&c.prefix, &c.migration_id, &c.stream, &pos(1), &pos(1), 1);
            store
                .put_create_only(&entry.segment_key, &bytes)
                .await
                .unwrap();
            store
                .put_create_only(&c.manifest_key(10), &entry.canonical_json().unwrap())
                .await
                .unwrap();
            let mut reader =
                BufferReader::new(store.clone(), c, checkpoint, Arc::new(Order)).unwrap();
            assert!(matches!(
                reader.next_txn().await,
                Err(Error::RecoveryRequired(_))
            ));
        } else {
            store
                .put_create_only(&c.manifest_key(10), &entry.canonical_json().unwrap())
                .await
                .unwrap();
            let mut reader =
                BufferReader::new(store.clone(), c, checkpoint, Arc::new(Order)).unwrap();
            assert!(matches!(
                reader.next_txn().await,
                Err(Error::RecoveryRequired(_))
            ));
        }
    }
}

#[tokio::test]
async fn checkpoint_epoch_and_segment_layout_reject_otherwise_valid_objects() {
    reader_layout(local()).await;
}

async fn reader_layout(store: Arc<dyn BufferStore>) {
    for regressed_epoch in [true, false] {
        let c = config();
        let (bytes, mut entry) = encoded(&c, 1, 1, 1);
        let checkpoint = ReaderCheckpoint {
            next_mseq: 10,
            prev_digest: [7; 32],
            last_epoch: if regressed_epoch { 2 } else { 1 },
            last_position: Some(pos(0)),
            version: skedaddle::buffer::format::CHECKPOINT_VERSION,
            last_record: Some(skedaddle::buffer::format::RecordBoundary {
                source: txn(1).source,
                chunk_rows: false,
            }),
        };
        if !regressed_epoch {
            // The valid segment is deliberately stored outside its derived
            // layout. A successful GET and matching header cannot authorize it.
            entry.segment_key = format!("unexpected/{}", entry.segment_key);
        }
        publish(
            store.as_ref(),
            &c,
            entry,
            &bytes,
            checkpoint.next_mseq,
            checkpoint.prev_digest,
        )
        .await;
        let mut reader =
            BufferReader::new(store.clone(), c, checkpoint.clone(), Arc::new(Order)).unwrap();
        assert!(matches!(
            reader.next_txn().await,
            Err(Error::RecoveryRequired(
                "manifest chain or segment identity"
            ))
        ));
        assert!(reader.last_verified().is_none());
        assert_eq!(reader.checkpoint(), &checkpoint);
        assert!(matches!(
            reader.next_txn().await,
            Err(Error::RecoveryRequired("reader is blocked"))
        ));
    }
}

#[tokio::test]
async fn resumed_multirecord_segment_commits_all_checkpoint_fields_at_its_boundary() {
    reader_checkpoint_boundary(local()).await;
}

async fn reader_checkpoint_boundary(store: Arc<dyn BufferStore>) {
    let c = config();
    let checkpoint = ReaderCheckpoint {
        next_mseq: 7,
        prev_digest: [9; 32],
        last_epoch: 2,
        last_position: Some(pos(40)),
        version: skedaddle::buffer::format::CHECKPOINT_VERSION,
        last_record: Some(skedaddle::buffer::format::RecordBoundary {
            source: txn(1).source,
            chunk_rows: false,
        }),
    };
    let (bytes, mut entry) = encoded(&c, 41, 2, 3);
    entry.mseq = 7;
    entry.prev_digest = checkpoint.prev_digest;
    let digest = publish(
        store.as_ref(),
        &c,
        entry.clone(),
        &bytes,
        7,
        checkpoint.prev_digest,
    )
    .await;
    let mut reader = BufferReader::new(
        store.clone(),
        c.clone(),
        checkpoint.clone(),
        Arc::new(Order),
    )
    .unwrap();
    assert!(reader.last_verified().is_none());
    assert_eq!(reader.next_txn().await.unwrap(), Some(txn(41)));
    assert_eq!(reader.checkpoint(), &checkpoint);
    let verified = reader.last_verified().unwrap();
    assert_eq!(verified.entry(), &entry);
    assert_eq!(verified.digest(), digest);
    assert_eq!(verified.manifest_key(), c.manifest_key(7));
    // A restart from a partially consumed segment must replay its first record.
    let mut restarted = BufferReader::new(
        store.clone(),
        c,
        reader.checkpoint().clone(),
        Arc::new(Order),
    )
    .unwrap();
    assert_eq!(restarted.next_txn().await.unwrap(), Some(txn(41)));
    assert_eq!(restarted.next_txn().await.unwrap(), Some(txn(42)));
    assert_eq!(reader.next_txn().await.unwrap(), Some(txn(42)));
    let expected = ReaderCheckpoint {
        next_mseq: 8,
        prev_digest: digest,
        last_epoch: 3,
        last_position: Some(pos(42)),
        version: skedaddle::buffer::format::CHECKPOINT_VERSION,
        last_record: Some(skedaddle::buffer::format::RecordBoundary {
            source: txn(1).source,
            chunk_rows: false,
        }),
    };
    assert_eq!(reader.checkpoint(), &expected);
    assert_eq!(restarted.checkpoint(), &expected);
    reader.require_through(8);
    reader.require_through(3);
    assert!(reader.next_txn().await.unwrap().is_none());
    reader.require_through(9);
    reader.require_through(8);
    assert!(matches!(
        reader.next_txn().await,
        Err(Error::RecoveryRequired("missing durable manifest"))
    ));
    assert_eq!(reader.checkpoint(), &expected);
}

#[tokio::test]
async fn every_manifest_header_field_is_verified_before_any_record_is_yielded() {
    reader_headers(local()).await;
}

async fn reader_headers(store: Arc<dyn BufferStore>) {
    for mode in 0..7 {
        let mut c = config();
        let (bytes, mut entry) = encoded(&c, 1, 2, 1);
        match mode {
            0 => c.migration_id = "another-migration".into(),
            1 => c.stream = "another-stream".into(),
            2 => entry.epoch = 2,
            3 => entry.first_pos = pos(0),
            4 => entry.last_pos = pos(3),
            5 => entry.txn_count = 3,
            6 => entry.bytes += 1,
            _ => unreachable!(),
        }
        entry.segment_key = segment_key(
            &c.prefix,
            &c.migration_id,
            &c.stream,
            &entry.first_pos,
            &entry.last_pos,
            entry.epoch,
        );
        publish(store.as_ref(), &c, entry, &bytes, 0, [0; 32]).await;
        let mut reader = BufferReader::new(
            store.clone(),
            c,
            ReaderCheckpoint::default(),
            Arc::new(Order),
        )
        .unwrap();
        let reason = if mode == 6 {
            "manifest/segment digest or size"
        } else {
            "manifest/header mismatch"
        };
        assert!(
            matches!(reader.next_txn().await, Err(Error::RecoveryRequired(s)) if s == reason),
            "mode {mode}"
        );
        assert!(reader.last_verified().is_none());
        assert_eq!(reader.checkpoint(), &ReaderCheckpoint::default());
        assert!(matches!(
            reader.next_txn().await,
            Err(Error::RecoveryRequired("reader is blocked"))
        ));
    }
}

#[tokio::test]
async fn invalid_record_order_and_checkpoint_domain_block_before_yield() {
    reader_order(local()).await;
}

async fn reader_order(store: Arc<dyn BufferStore>) {
    for positions in [vec![2, 1], vec![1, 1]] {
        let c = config();
        let mut encoder = SegmentEncoder::new(
            Vec::new(),
            SegmentHeader {
                migration_id: c.migration_id.clone(),
                stream: c.stream.clone(),
                epoch: 1,
                first_pos: pos(positions[0]),
                last_pos: pos(positions[1]),
                record_count: 2,
            },
            c.limits,
        )
        .unwrap();
        for n in &positions {
            encoder
                .write_record(&SegmentRecord::Transaction(txn(*n)))
                .unwrap();
        }
        let (bytes, summary) = encoder.finish().unwrap();
        let (_, mut entry) = encoded(&c, 1, 2, 1);
        entry.first_pos = pos(positions[0]);
        entry.last_pos = pos(positions[1]);
        entry.segment_digest = summary.digest;
        entry.bytes = summary.bytes;
        entry.segment_key = segment_key(
            &c.prefix,
            &c.migration_id,
            &c.stream,
            &entry.first_pos,
            &entry.last_pos,
            1,
        );
        publish(store.as_ref(), &c, entry, &bytes, 0, [0; 32]).await;
        let mut reader = BufferReader::new(
            store.clone(),
            c,
            ReaderCheckpoint::default(),
            Arc::new(Order),
        )
        .unwrap();
        assert!(matches!(
            reader.next_txn().await,
            Err(Error::RecoveryRequired("positions not strictly increasing"))
        ));
        assert!(reader.last_verified().is_none());
        assert_eq!(reader.checkpoint(), &ReaderCheckpoint::default());
    }
    let c = config();
    let (bytes, entry) = encoded(&c, 1, 1, 1);
    let checkpoint = ReaderCheckpoint {
        next_mseq: 1,
        last_position: Some(Position::new("foreign", vec![0])),
        last_record: Some(skedaddle::buffer::format::RecordBoundary {
            source: txn(1).source,
            chunk_rows: false,
        }),
        ..ReaderCheckpoint::default()
    };
    publish(store.as_ref(), &c, entry, &bytes, 1, checkpoint.prev_digest).await;
    let mut reader =
        BufferReader::new(store.clone(), c, checkpoint.clone(), Arc::new(Order)).unwrap();
    assert!(matches!(
        reader.next_txn().await,
        Err(Error::RecoveryRequired("position order domain"))
    ));
    assert_eq!(reader.checkpoint(), &checkpoint);
}

#[test]
fn reader_error_messages_preserve_the_failure_reason() {
    assert_eq!(
        Error::RecoveryRequired("gap").to_string(),
        "buffer recovery-required: gap"
    );
    assert_eq!(
        Error::Invalid("identity").to_string(),
        "invalid buffer reader configuration: identity"
    );
    assert_eq!(
        Error::from(backends::Error::Store).to_string(),
        backends::Error::Store.to_string()
    );
}

#[tokio::test]
async fn orphan_scan_reports_foreign_layout_and_corrupt_objects_without_gc_authority() {
    orphan_validation(local()).await;
}

async fn orphan_validation(store: Arc<dyn BufferStore>) {
    for mode in 0..5 {
        let c = config();
        let mut header_config = c.clone();
        if mode == 1 {
            header_config.migration_id = "foreign-migration".into();
        }
        if mode == 2 {
            header_config.stream = "foreign-stream".into();
        }
        let (mut bytes, _) = encoded(&header_config, 1, 2, 1);
        let key = if mode == 3 {
            format!("{}wrong-layout.seg", c.segment_prefix())
        } else {
            segment_key(&c.prefix, &c.migration_id, &c.stream, &pos(1), &pos(2), 1)
        };
        if mode == 4 {
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
        }
        store.put_create_only(&key, &bytes).await.unwrap();
        let range = ManifestRange {
            start: ReaderCheckpoint::default(),
            end_mseq: 0,
            end_digest: [0; 32],
        };
        let page = list_orphan_segments(
            store.as_ref(),
            &c,
            &range,
            OrphanFence { before_epoch: 2 },
            None,
            10,
            100,
        )
        .await
        .unwrap();
        assert!(page.next.is_none());
        if mode == 0 {
            assert!(page.unverified.is_empty());
            assert_eq!(page.orphans.len(), 1);
            assert_eq!(page.orphans[0].key(), key);
            assert_eq!(page.orphans[0].bytes(), bytes.len() as u64);
        } else {
            assert!(page.orphans.is_empty(), "mode {mode}");
            assert_eq!(page.unverified, vec![key], "mode {mode}");
        }
    }
}

#[test]
fn backpressure_releases_requests_that_exactly_fit_and_tracks_remaining_age() {
    let now = Instant::now();
    for (requested_bytes, requested_txns) in [(10, 1), (4, 2)] {
        let capacity = Backpressure::new(BufferBounds {
            max_buffered_bytes: 10,
            max_unapplied_txns: 2,
            max_oldest_unapplied_age: Duration::from_secs(20),
        })
        .unwrap();
        let signal = capacity.subscribe();
        let id = capacity.try_admit(6, 1, now).unwrap();
        let h = capacity.refresh(now + Duration::from_secs(3));
        assert_eq!((h.bytes, h.transactions), (4, 1));
        assert_eq!(h.oldest_unapplied_age, Duration::from_secs(3));
        assert_eq!(h.age, Duration::from_secs(17));
        assert!(!h.paused);
        assert!(
            capacity
                .try_admit(requested_bytes, requested_txns, now)
                .is_err()
        );
        assert!(signal.borrow().paused);
        capacity.mark_applied(id, now).unwrap();
        let h = capacity.refresh(now + Duration::from_secs(30));
        assert_eq!((h.bytes, h.transactions), (4, 2));
        assert_eq!(h.oldest_unapplied_age, Duration::ZERO);
        assert_eq!(h.age, Duration::from_secs(20));
        assert_eq!(h.paused, requested_bytes == 10);
        capacity.mark_deleted(id, now).unwrap();
        assert!(!signal.borrow().paused);
        assert!(!capacity.refresh(now).paused);
        capacity
            .try_admit(requested_bytes, requested_txns, now)
            .unwrap();
    }
}

#[test]
fn invalid_zero_admissions_do_not_pause_later_valid_capture() {
    let now = Instant::now();
    for (bytes, txns) in [(0, 0), (0, 1), (1, 0), (0, 3), (11, 0)] {
        let capacity = Backpressure::new(BufferBounds {
            max_buffered_bytes: 10,
            max_unapplied_txns: 2,
            max_oldest_unapplied_age: Duration::from_secs(20),
        })
        .unwrap();
        assert!(capacity.try_admit(bytes, txns, now).is_err());
        let h = capacity.refresh(now);
        assert_eq!((h.bytes, h.transactions), (10, 2));
        assert!(!h.paused, "invalid request ({bytes}, {txns})");
        let id = capacity.try_admit(1, 1, now).unwrap();
        capacity.mark_applied(id, now).unwrap();
        capacity.mark_deleted(id, now).unwrap();
    }
}

#[tokio::test]
async fn backpressure_pauses_capture_not_drops() {
    capacity_matrix(local()).await;
}

async fn capacity_matrix(store: Arc<dyn BufferStore>) {
    let c = config();
    let (_, example) = encoded(&c, 1, 1, 1);
    let charge = example.bytes + example.canonical_json().unwrap().len() as u64;
    let capacity = Backpressure::new(BufferBounds {
        max_buffered_bytes: charge * 2,
        max_unapplied_txns: 2,
        max_oldest_unapplied_age: Duration::from_secs(5),
    })
    .unwrap();
    let source_signal = capacity.subscribe();
    let writer_signal = capacity.subscribe();
    let (progress, _) = mpsc::channel();
    let mut writer = BufferWriter::open(
        store.clone(),
        WriterConfig {
            prefix: c.prefix.clone(),
            migration_id: c.migration_id.clone(),
            stream: c.stream.clone(),
            epoch: 1,
            segment_bytes: 1024,
            segment_transactions: 1,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 4096,
        },
        Arc::new(Order),
        progress,
    )
    .await
    .unwrap();
    let now = Instant::now();
    let first = capacity.try_admit(charge, 1, now).unwrap();
    writer.append(txn(1), now).await.unwrap();
    let second = capacity.try_admit(charge, 1, now).unwrap();
    writer.append(txn(2), now).await.unwrap();
    assert!(source_signal.borrow().paused && writer_signal.borrow().paused);
    let retained = txn(3);
    assert!(capacity.try_admit(charge, 1, now).is_err());
    assert_eq!(writer.next_manifest_sequence(), 2);
    assert!(capacity.mark_deleted(first, now).is_err());
    let mut reader = BufferReader::new(
        store.clone(),
        c.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    let mut applied = vec![reader.next_txn().await.unwrap().unwrap()];
    capacity.mark_applied(first, now).unwrap();
    assert!(source_signal.borrow().paused); // bytes still retained
    struct Journal(Arc<dyn BufferStore>);
    #[async_trait]
    impl skedaddle::buffer::gc::GcJournal for Journal {
        async fn record(
            &self,
            intent: &skedaddle::buffer::gc::GcIntent,
        ) -> skedaddle::buffer::reader::Result<()> {
            self.0
                .put_create_only(
                    &format!("gc-journal/{}", uuid::Uuid::new_v4()),
                    &serde_json::to_vec(intent).unwrap(),
                )
                .await?;
            Ok(())
        }
    }
    let gc = skedaddle::buffer::gc::RetentionGc::new(
        store.clone(),
        Arc::new(Order),
        skedaddle::buffer::gc::RetentionPolicy {
            retained_for_ms: 0,
            orphan_grace_ms: 0,
        },
    );
    let receipt =
        skedaddle::buffer::gc::AppliedSegment::new(reader.last_verified().unwrap().clone(), 0, 0);
    gc.collect_applied(&receipt, 0, &Journal(store.clone()))
        .await
        .unwrap();
    capacity.mark_deleted(first, now).unwrap();
    assert!(!writer_signal.borrow().paused);
    capacity.try_admit(charge, 1, now).unwrap();
    writer.append(retained, now).await.unwrap();
    capacity.mark_applied(second, now).unwrap();
    for _ in 2..=3 {
        applied.push(reader.next_txn().await.unwrap().unwrap());
    }
    assert_eq!(applied, vec![txn(1), txn(2), txn(3)]);
    assert!(reader.next_txn().await.unwrap().is_none());
    let partial_capacity = Backpressure::new(BufferBounds {
        max_buffered_bytes: 7,
        max_unapplied_txns: 10,
        max_oldest_unapplied_age: Duration::from_secs(20),
    })
    .unwrap();
    let signal = partial_capacity.subscribe();
    let id = partial_capacity.try_admit(5, 1, now).unwrap();
    assert_eq!(partial_capacity.try_admit(3, 1, now).unwrap_err().bytes, 2);
    assert!(signal.borrow().paused); // Requested txn cannot fit in residual bytes.
    partial_capacity.mark_applied(id, now).unwrap();
    partial_capacity.mark_deleted(id, now).unwrap();
    assert!(!signal.borrow().paused);
    // Each independent bound is inclusive, and elapsed idle time updates signals.
    for (bytes, txns, age) in [(5, 9, 20), (50, 1, 20), (50, 9, 5)] {
        let p = Backpressure::new(BufferBounds {
            max_buffered_bytes: bytes,
            max_unapplied_txns: txns,
            max_oldest_unapplied_age: Duration::from_secs(age),
        })
        .unwrap();
        let id = p.try_admit(5, 1, now).unwrap();
        assert!(p.refresh(now + Duration::from_secs(5)).paused);
        assert!(p.try_admit(1, 1, now + Duration::from_secs(5)).is_err());
        p.mark_applied(id, now + Duration::from_secs(5)).unwrap();
        p.mark_deleted(id, now + Duration::from_secs(5)).unwrap();
        assert!(!p.refresh(now + Duration::from_secs(5)).paused);
    }
}

async fn resume(
    store: Arc<dyn BufferStore>,
    local_root: Option<PathBuf>,
    count: u64,
    backend: Option<&str>,
) {
    assert!(count >= 4);
    let mut uploads = tokio::task::JoinSet::new();
    let mut c = config();
    c.required_next_mseq = count;
    let mut prev = [0; 32];
    let mut checkpoint = ReaderCheckpoint::default();
    let mut bytes_written = 0u64;
    if let Some(path) = &local_root {
        std::fs::create_dir_all(path.join(c.segment_prefix())).unwrap();
        std::fs::create_dir_all(path.join(c.manifest_key(0)).parent().unwrap()).unwrap();
    }
    for n in 0..count {
        let (segment, mut entry) = encoded(&c, n + 1, 1, 1);
        entry.mseq = n;
        entry.prev_digest = prev;
        let manifest = entry.canonical_json().unwrap();
        bytes_written += manifest.len() as u64;
        if let Some(path) = &local_root {
            // The trusted apply checkpoint permits historical segments to have
            // been reclaimed. Retain all 100k manifests and only the four live
            // suffix segments, reducing fixture data and detecting old GETs.
            // Bulk loading is real FS I/O; resume uses production BufferStore.
            if n >= count - 4 {
                std::fs::write(path.join(&entry.segment_key), &segment).unwrap();
                bytes_written += segment.len() as u64;
            }
            std::fs::write(path.join(c.manifest_key(n)), &manifest).unwrap();
        } else {
            // Historical segments are absent on remotes too: reading from
            // genesis cannot accidentally satisfy the suffix proof.
            let segment = (n >= count - 4).then_some(segment);
            bytes_written += segment.as_ref().map_or(0, |s| s.len() as u64);
            let store = store.clone();
            let key = c.manifest_key(n);
            let segment_key = entry.segment_key.clone();
            uploads.spawn(async move {
                if let Some(segment) = segment {
                    store.put_create_only(&segment_key, &segment).await.unwrap();
                }
                store.put_create_only(&key, &manifest).await.unwrap();
            });
            if uploads.len() >= 32 {
                uploads.join_next().await.unwrap().unwrap();
            }
        }
        prev = entry.digest().unwrap();
        if n + 1 == count - 4 {
            checkpoint = ReaderCheckpoint {
                next_mseq: n + 1,
                prev_digest: prev,
                last_epoch: 1,
                last_position: Some(pos(n + 1)),
                version: skedaddle::buffer::format::CHECKPOINT_VERSION,
                last_record: Some(skedaddle::buffer::format::RecordBoundary {
                    source: txn(1).source,
                    chunk_rows: false,
                }),
            };
        }
    }
    while let Some(result) = uploads.join_next().await {
        result.unwrap();
    }
    assert!(bytes_written < 250_000_000);
    let probe = if let Some(backend) = backend {
        let cfg: serde_json::Value = serde_json::from_slice(
            &std::fs::read(std::env::var_os("SKEDADDLE_TEST_CONFIG").unwrap()).unwrap(),
        )
        .unwrap();
        Some(RequestProxy::start(cfg[backend].as_str().unwrap(), false, false).await)
    } else {
        None
    };
    let observed = probe.as_ref().map_or_else(
        || store.clone(),
        |p| remote_store(backend.unwrap(), Some(&p.endpoint)),
    );
    let counted = Arc::new(Counted {
        inner: observed,
        gets: AtomicU64::new(0),
        lists: AtomicU64::new(0),
        keys: std::sync::Mutex::new(Vec::new()),
    });
    let mut reader =
        BufferReader::new(counted.clone(), c.clone(), checkpoint, Arc::new(Order)).unwrap();
    for n in count - 3..=count {
        assert_eq!(reader.next_txn().await.unwrap(), Some(txn(n)));
    }
    assert!(reader.next_txn().await.unwrap().is_none());
    assert_eq!(counted.lists.load(AtomicOrdering::Relaxed), 0);
    assert_eq!(counted.gets.load(AtomicOrdering::Relaxed), 9);
    let mut expected_keys = Vec::new();
    for n in count - 4..count {
        expected_keys.push(c.manifest_key(n));
        expected_keys.push(encoded(&c, n + 1, 1, 1).1.segment_key);
    }
    expected_keys.push(c.manifest_key(count));
    assert_eq!(*counted.keys.lock().unwrap(), expected_keys);
    if let Some(probe) = &probe {
        let requests = probe.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            9,
            "backend must issue only the nine suffix GET requests: {requests:?}"
        );
        assert!(
            requests.iter().all(|(method, target)| method == "GET"
                && !target.contains("list-type=")
                && !target.contains("prefix=")),
            "unexpected backend listing: {requests:?}"
        );
        eprintln!("remote suffix HTTP requests: {requests:?}");
    }
    eprintln!("resume manifests={count}, fixture_bytes={bytes_written}, LIST=0, GET=9");
    drop(counted);
    drop(probe);
    // Delete only this proof's unique keys. Bounded concurrency keeps 100k
    // objects below the harness cap and leaves no accumulated remote dataset.
    if local_root.is_none() {
        for n in 0..count {
            let store = store.clone();
            let key = c.manifest_key(n);
            let segment_key = (n >= count - 4).then(|| encoded(&c, n + 1, 1, 1).1.segment_key);
            uploads.spawn(async move {
                store.delete(&key).await.unwrap();
                if let Some(key) = segment_key {
                    store.delete(&key).await.unwrap();
                }
            });
            if uploads.len() >= 32 {
                uploads.join_next().await.unwrap().unwrap();
            }
        }
        while let Some(result) = uploads.join_next().await {
            result.unwrap();
        }
    }
}
#[tokio::test]
async fn resume_without_full_listing() {
    let path = root();
    let store = Arc::new(ObjectBufferStore::local(&path, Duration::from_secs(30)).unwrap());
    resume(store, Some(path), 100_000, None).await;
}
async fn reader_suite(store: Arc<dyn BufferStore>) {
    orphan_matrix(store.clone()).await;
    reader_corruption(store.clone()).await;
    reader_tail(store.clone()).await;
    reader_anchor(store.clone()).await;
    reader_layout(store.clone()).await;
    reader_checkpoint_boundary(store.clone()).await;
    reader_headers(store.clone()).await;
    reader_order(store.clone()).await;
    orphan_validation(store.clone()).await;
    capacity_matrix(store.clone()).await;
    reader_equal_h_checkpoint(store.clone()).await;
    reader_equal_h_order(store).await;
}

#[tokio::test]
async fn local_reader_matrix() {
    reader_suite(local()).await;
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
async fn minio_reader_matrix() {
    reader_suite(remote_store("minio", None)).await;
}

#[tokio::test]
#[ignore = "requires MinIO/fake-gcs via harness/run.ps1 p3; plain cargo test explicitly skips infrastructure"]
async fn gcs_reader_matrix() {
    reader_suite(remote_store("gcs", None)).await;
}

fn ordered_record(id: u64, position: u64, chunk_rows: bool, foreign: bool) -> SegmentRecord {
    let mut source = txn(1).source;
    if foreign {
        source.lineage = "other".into();
    }
    let txn = AssembledTxn::new(
        source,
        TxnId(id.to_be_bytes().to_vec()),
        pos(position),
        Some(7),
        vec![],
    )
    .unwrap();
    if chunk_rows {
        SegmentRecord::ChunkRows(txn)
    } else {
        SegmentRecord::Transaction(txn)
    }
}

async fn publish_records(
    store: &dyn BufferStore,
    c: &ReaderConfig,
    records: &[SegmentRecord],
    mseq: u64,
    prev: ContentDigest,
    previous: Option<&Position>,
) -> ContentDigest {
    let first = records.first().unwrap().position();
    let last = records.last().unwrap().position();
    let epoch = mseq + 1;
    let mut encoder = SegmentEncoder::new(
        Vec::new(),
        SegmentHeader {
            migration_id: c.migration_id.clone(),
            stream: c.stream.clone(),
            epoch,
            first_pos: first.clone(),
            last_pos: last.clone(),
            record_count: records.len() as u64,
        },
        c.limits,
    )
    .unwrap();
    for record in records {
        encoder.write_record(record).unwrap();
    }
    let (bytes, summary) = encoder.finish().unwrap();
    let mut key = segment_key(&c.prefix, &c.migration_id, &c.stream, first, last, epoch);
    if previous == Some(first) {
        key.push_str(".chunk-rows");
    }
    publish(
        store,
        c,
        ManifestEntry {
            mseq,
            epoch,
            prev_digest: prev,
            segment_key: key,
            segment_digest: summary.digest,
            first_pos: first.clone(),
            last_pos: last.clone(),
            txn_count: records.len() as u64,
            bytes: summary.bytes,
        },
        &bytes,
        mseq,
        prev,
    )
    .await
}

#[tokio::test]
async fn equal_h_pair_is_valid_within_segments_and_across_resumed_checkpoints() {
    reader_equal_h_checkpoint(local()).await;
}

async fn reader_equal_h_checkpoint(store: Arc<dyn BufferStore>) {
    for split in [false, true] {
        let c = config();
        let records = [
            ordered_record(10, 5, false, false),
            ordered_record(11, 5, true, false),
        ];
        let end_digest = if split {
            let prev = publish_records(store.as_ref(), &c, &records[..1], 0, [0; 32], None).await;
            publish_records(store.as_ref(), &c, &records[1..], 1, prev, Some(&pos(5))).await
        } else {
            publish_records(store.as_ref(), &c, &records, 0, [0; 32], None).await
        };
        let mut reader = BufferReader::new(
            store.clone(),
            c.clone(),
            ReaderCheckpoint::default(),
            Arc::new(Order),
        )
        .unwrap();
        assert!(reader.next_txn().await.unwrap().is_some());
        if split {
            assert!(!reader.checkpoint().last_record.as_ref().unwrap().chunk_rows);
            reader = BufferReader::new(
                store.clone(),
                c.clone(),
                reader.checkpoint().clone(),
                Arc::new(Order),
            )
            .unwrap();
        } else {
            assert_eq!(reader.checkpoint(), &ReaderCheckpoint::default());
        }
        assert!(reader.next_txn().await.unwrap().is_some());
        let checkpoint = reader.checkpoint().clone();
        assert_eq!(checkpoint.last_position, Some(pos(5)));
        assert!(checkpoint.last_record.as_ref().unwrap().chunk_rows);
        assert_eq!(
            checkpoint.version,
            skedaddle::buffer::format::CHECKPOINT_VERSION
        );
        // The orphan scan also traverses the equal-position manifest boundary.
        list_orphan_segments(
            store.as_ref(),
            &c,
            &ManifestRange {
                start: ReaderCheckpoint::default(),
                end_mseq: checkpoint.next_mseq,
                end_digest,
            },
            OrphanFence { before_epoch: 10 },
            None,
            10,
            10_000,
        )
        .await
        .unwrap();
        publish_records(
            store.as_ref(),
            &c,
            &[ordered_record(12, 5, true, false)],
            checkpoint.next_mseq,
            checkpoint.prev_digest,
            Some(&pos(5)),
        )
        .await;
        let mut resumed =
            BufferReader::new(store.clone(), c, checkpoint.clone(), Arc::new(Order)).unwrap();
        assert!(matches!(
            resumed.next_txn().await,
            Err(Error::RecoveryRequired("positions not strictly increasing"))
        ));
        assert_eq!(resumed.checkpoint(), &checkpoint);
    }
}

#[tokio::test]
async fn equal_h_order_rejects_all_other_ties_decreases_and_foreign_lineages() {
    reader_equal_h_order(local()).await;
}

async fn reader_equal_h_order(store: Arc<dyn BufferStore>) {
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
    for split in [false, true] {
        for (prefix, rejected) in &cases {
            let c = config();
            let records: Vec<_> = prefix
                .iter()
                .chain(std::iter::once(rejected))
                .enumerate()
                .map(|(id, &(position, chunk_rows, foreign))| {
                    ordered_record(id as u64 + 10, position, chunk_rows, foreign)
                })
                .collect();
            let mut reader = BufferReader::new(
                store.clone(),
                c.clone(),
                ReaderCheckpoint::default(),
                Arc::new(Order),
            )
            .unwrap();
            let writer_config = WriterConfig {
                prefix: c.prefix.clone(),
                migration_id: c.migration_id.clone(),
                stream: c.stream.clone(),
                epoch: 10,
                segment_bytes: 65536,
                segment_transactions: 10,
                segment_age: Duration::from_secs(1),
                max_transaction_bytes: 1024 * 1024,
            };
            let mut writer_checkpoint = skedaddle::buffer::writer::WriterCheckpoint::default();
            if split {
                let mut prev = [0; 32];
                for (index, record) in records[..records.len() - 1].iter().enumerate() {
                    prev = publish_records(
                        store.as_ref(),
                        &c,
                        std::slice::from_ref(record),
                        index as u64,
                        prev,
                        index
                            .checked_sub(1)
                            .map(|previous| records[previous].position()),
                    )
                    .await;
                    assert!(reader.next_txn().await.unwrap().is_some());
                    reader = BufferReader::new(
                        store.clone(),
                        c.clone(),
                        reader.checkpoint().clone(),
                        Arc::new(Order),
                    )
                    .unwrap();
                }
                let (tx, _) = mpsc::channel();
                let writer =
                    BufferWriter::open(store.clone(), writer_config.clone(), Arc::new(Order), tx)
                        .await
                        .unwrap();
                writer_checkpoint = writer.checkpoint().unwrap();
                drop(writer);
                let index = records.len() - 1;
                publish_records(
                    store.as_ref(),
                    &c,
                    &records[index..],
                    index as u64,
                    prev,
                    Some(records[index - 1].position()),
                )
                .await;
            } else {
                publish_records(store.as_ref(), &c, &records, 0, [0; 32], None).await;
            }
            let checkpoint = reader.checkpoint().clone();
            assert!(
                matches!(
                    reader.next_txn().await,
                    Err(Error::RecoveryRequired("positions not strictly increasing"))
                ),
                "{prefix:?} -> {rejected:?}"
            );
            assert_eq!(reader.checkpoint(), &checkpoint);
            assert!(matches!(
                reader.next_txn().await,
                Err(Error::RecoveryRequired("reader is blocked"))
            ));
            // Both writer replay modes must reject exactly the same malformed suffix.
            let (tx, _) = mpsc::channel();
            assert!(
                BufferWriter::open(store.clone(), writer_config.clone(), Arc::new(Order), tx)
                    .await
                    .is_err()
            );
            let (tx, _) = mpsc::channel();
            assert!(
                BufferWriter::open_from_checkpoint(
                    store.clone(),
                    writer_config,
                    writer_checkpoint,
                    Arc::new(Order),
                    tx,
                    root()
                )
                .await
                .is_err()
            );
        }
    }
}

#[test]
fn reader_rejects_obsolete_or_incomplete_checkpoint_boundaries() {
    for version in [1, 2] {
        let obsolete = ReaderCheckpoint {
            version,
            ..ReaderCheckpoint::default()
        };
        assert!(matches!(
            BufferReader::new(local(), config(), obsolete, Arc::new(Order)),
            Err(Error::Format(skedaddle::buffer::format::FormatError::UnsupportedFormatVersion {
                format: "reader-checkpoint", found: Some(found), expected: 3,
            })) if found == version
        ));
    }
    for with_position in [false, true] {
        let checkpoint = ReaderCheckpoint {
            next_mseq: 1,
            last_position: with_position.then(|| pos(5)),
            ..ReaderCheckpoint::default()
        };
        assert!(BufferReader::new(local(), config(), checkpoint, Arc::new(Order)).is_err());
    }
}

#[test]
fn pg_reader_marker_rejects_v2_and_round_trips_v3() {
    use skedaddle::engine::{decode_reader_checkpoint, encode_reader_checkpoint};
    // Persisted predecessor marker: retain it as explicit rejection evidence.
    let v2 = concat!(
        "{\"version\":2,\"next_mseq\":0,\"prev_digest\":[",
        "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
        "],\"last_epoch\":0,\"position\":null,\"source\":null}"
    );
    assert!(matches!(
        decode_reader_checkpoint(v2),
        Err(skedaddle::coordinator::Error::RecoveryRequired(_))
    ));
    let checkpoint = ReaderCheckpoint::default();
    assert_eq!(checkpoint.version, 3);
    let current = encode_reader_checkpoint(&checkpoint).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&current).unwrap()["version"],
        3
    );
    assert_eq!(decode_reader_checkpoint(&current).unwrap(), checkpoint);
}

// This forwards real emulator traffic. It fabricates no object contents.
// Connection-close framing keeps the fault injector independent of a server crate.
struct RequestProxy {
    endpoint: String,
    requests: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for RequestProxy {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl RequestProxy {
    async fn start(upstream: &str, delay_write: bool, reject_reads: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let upstream = upstream.trim_end_matches('/').to_owned();
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let server = tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap();
            let mut pending = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let client = client.clone();
                        let upstream = upstream.clone();
                        let recorded = recorded.clone();
                        pending.spawn(async move {
                            use tokio::io::{AsyncReadExt, AsyncWriteExt};
                            let mut input = Vec::new();
                            let header_end = loop {
                                let mut buf = [0u8; 4096];
                                let n = socket.read(&mut buf).await.unwrap();
                                if n == 0 { return; }
                                input.extend_from_slice(&buf[..n]);
                                assert!(input.len() < 1024 * 1024, "proxy is for small fault/GET requests only");
                                if let Some(n) = input.windows(4).position(|w| w == b"\r\n\r\n") { break n + 4; }
                            };
                            let header = std::str::from_utf8(&input[..header_end]).unwrap().to_owned();
                            let mut lines = header.split("\r\n");
                            let mut first = lines.next().unwrap().split_whitespace();
                            let method = first.next().unwrap().to_owned();
                            let target = first.next().unwrap().to_owned();
                            recorded.lock().unwrap().push((method.clone(), target.clone()));
                            let mut request = client.request(reqwest::Method::from_bytes(method.as_bytes()).unwrap(), format!("{upstream}{target}"));
                            let mut length = 0;
                            for line in lines.filter(|l| !l.is_empty()) {
                                let (name, value) = line.split_once(':').unwrap();
                                let value = value.trim();
                                if name.eq_ignore_ascii_case("content-length") { length = value.parse::<usize>().unwrap(); }
                                assert!(!name.eq_ignore_ascii_case("transfer-encoding"), "unexpected chunked request");
                                if !name.eq_ignore_ascii_case("connection") { request = request.header(name, value); }
                            }
                            assert!(length < 1024 * 1024);
                            while input.len() - header_end < length {
                                let mut buf = [0u8; 4096];
                                let n = socket.read(&mut buf).await.unwrap();
                                assert_ne!(n, 0);
                                input.extend_from_slice(&buf[..n]);
                            }
                            let (status, headers, body) = if reject_reads && method == "GET" {
                                (503, String::new(), Vec::new())
                            } else {
                                let response = request.body(input[header_end..header_end + length].to_vec()).send().await.unwrap();
                                let status = response.status().as_u16();
                                let mut headers = String::new();
                                for (name, value) in response.headers() {
                                    if !matches!(name.as_str(), "connection" | "content-length" | "transfer-encoding") {
                                        headers.push_str(&format!("{}: {}\r\n", name, value.to_str().unwrap()));
                                    }
                                }
                                let body = response.bytes().await.unwrap().to_vec();
                                // Delay only after the real upstream has accepted the write.
                                if delay_write && matches!(method.as_str(), "POST" | "PUT") && (200..300).contains(&status) {
                                    tokio::time::sleep(Duration::from_secs(3)).await;
                                }
                                (status, headers, body)
                            };
                            let head = format!("HTTP/1.1 {status} Proxy\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                            if socket.write_all(head.as_bytes()).await.is_ok() { let _ = socket.write_all(&body).await; }
                        });
                    }
                    result = pending.join_next(), if !pending.is_empty() => { result.unwrap().unwrap(); }
                }
            }
        });
        Self {
            endpoint,
            requests,
            server,
        }
    }
}

#[tokio::test]
#[ignore = "100k real remote manifests require harness/run.ps1 p3; plain cargo test explicitly skips infrastructure"]
async fn minio_resume_without_full_listing_100k() {
    resume(remote_store("minio", None), None, 100_000, Some("minio")).await;
}

#[tokio::test]
#[ignore = "100k real remote manifests require harness/run.ps1 p3; plain cargo test explicitly skips infrastructure"]
async fn gcs_resume_without_full_listing_100k() {
    resume(remote_store("gcs", None), None, 100_000, Some("gcs")).await;
}
