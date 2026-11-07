use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Cursor, Read, Write},
    panic::{AssertUnwindSafe, catch_unwind},
};

use sha2::{Digest, Sha256};
use skedaddle::{
    buffer::format::{
        END_MAGIC, FormatError, Limits, ManifestEntry, SegmentDecoder, SegmentEncoder,
        SegmentHeader, SegmentRecord, manifest_key, segment_key,
    },
    event::{AssembledTxn, Event, EventKind, OpaqueValue, Position, Row, SourceIdentity, TxnId},
};

// Track live allocations only on the large-stream test's thread. System does
// the real allocation; this instrumentation never substitutes storage behavior.
struct MeasuredAllocator;
#[derive(Clone, Copy, Default)]
struct Allocations {
    enabled: bool,
    current: usize,
    peak: usize,
}
thread_local! { static ALLOCATIONS: Cell<Allocations> = const { Cell::new(Allocations { enabled: false, current: 0, peak: 0 }) }; }
fn account(old: usize, new: usize) {
    let _ = ALLOCATIONS.try_with(|cell| {
        let mut value = cell.get();
        if value.enabled {
            value.current = value.current.saturating_sub(old).saturating_add(new);
            value.peak = value.peak.max(value.current);
            cell.set(value);
        }
    });
}
// SAFETY: each method forwards the unchanged allocation/layout contract to
// System; accounting never accesses allocation contents or allocates memory.
unsafe impl GlobalAlloc for MeasuredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            account(0, layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        account(layout.size(), 0);
        unsafe {
            System.dealloc(ptr, layout);
        }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, size) };
        if !result.is_null() {
            account(layout.size(), size);
        }
        result
    }
}
#[global_allocator]
static ALLOCATOR: MeasuredAllocator = MeasuredAllocator;

fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "pg".into(),
        system_id: "s".into(),
        lineage: "l".into(),
        db_name: "db".into(),
        slot: "slot".into(),
    }
}
fn pos() -> Position {
    Position::new("d", vec![9])
}
fn transaction() -> SegmentRecord {
    SegmentRecord::Transaction(
        AssembledTxn::new(source(), TxnId(vec![1]), pos(), None, vec![]).unwrap(),
    )
}
fn chunk(payload: usize) -> SegmentRecord {
    let event = Event {
        source: source(),
        txn: TxnId(vec![2]),
        ordinal_in_txn: 0,
        position: pos(),
        schema_version: 1,
        kind: EventKind::ChunkRows,
        table: Some("public.t".into()),
        key: Row::new(),
        before: None,
        after: Some(Row::from([(
            "v".into(),
            OpaqueValue {
                type_id: "pg/bytea".into(),
                bytes: Some(vec![42; payload]),
            },
        )])),
        unchanged_cols: Default::default(),
    };
    SegmentRecord::ChunkRows(
        AssembledTxn::new(source(), TxnId(vec![2]), pos(), None, vec![event]).unwrap(),
    )
}

fn reseal(txn: AssembledTxn) -> AssembledTxn {
    AssembledTxn::new(
        txn.source,
        txn.txn,
        txn.commit_position,
        txn.commit_time,
        txn.events,
    )
    .unwrap()
}
fn header(count: u64) -> SegmentHeader {
    SegmentHeader {
        migration_id: "m".into(),
        stream: "s".into(),
        epoch: 7,
        first_pos: pos(),
        last_pos: pos(),
        record_count: count,
    }
}
fn encode(records: &[SegmentRecord]) -> Vec<u8> {
    let mut encoder =
        SegmentEncoder::new(Vec::new(), header(records.len() as u64), Limits::default()).unwrap();
    for record in records {
        encoder.write_record(record).unwrap();
    }
    encoder.finish().unwrap().0
}
fn decode(bytes: &[u8]) -> Result<Vec<SegmentRecord>, FormatError> {
    let mut decoder = SegmentDecoder::new(
        bytes,
        Limits {
            max_record_bytes: 1024 * 1024,
            ..Limits::default()
        },
    )?;
    let mut records = Vec::new();
    while let Some(record) = decoder.next_record()? {
        records.push(record);
    }
    decoder.finish()?;
    Ok(records)
}
fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
fn record_start(bytes: &[u8]) -> usize {
    14 + u32::from_be_bytes(bytes[10..14].try_into().unwrap()) as usize
}
fn resign_segment(bytes: &mut [u8]) {
    let end = bytes.len() - 32;
    let digest = Sha256::digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&digest);
}

// Independently constructed using Python struct.pack/hashlib, including the
// P3a transaction encoding, record digest, header and complete segment trailer.
const GOLDEN: &str = concat!(
    "534b45445345470000010000002e000000016d00000001730000000000000007",
    "00000001640000000109000000016400000001090000000000000001",
    "0000000000000059534b45560001020000000270670000000173000000016c",
    "00000002646200000004736c6f740000000101000000016400000001090000000000",
    "dba702fe63fa8822942316ddf436a2bd27dc86473bd74bbe3f3001152fe0b780",
    "0000000000000000534b4544454e44000000000000000001",
    "4d6fa58b20fa624e7cb98feb2028ff1d260d56adb7651d680797b1919b05d920",
);

#[test]
fn golden_bytes_are_stable_and_reuse_p3a_record() {
    let bytes = encode(&[transaction()]);
    assert_eq!(bytes, unhex(GOLDEN));
    let SegmentRecord::Transaction(txn) = transaction() else {
        unreachable!()
    };
    let canonical = txn.canonical_bytes().unwrap();
    let start = record_start(&bytes) + 8;
    assert_eq!(&bytes[start..start + canonical.len()], canonical);
    assert_eq!(decode(&bytes).unwrap(), vec![transaction()]);
    // Independent Python struct.pack/hashlib golden for an empty ChunkRows
    // batch, including its embedded unchanged P3a envelope and outer checksum.
    let SegmentRecord::Transaction(txn) = transaction() else {
        unreachable!()
    };
    let chunk = SegmentRecord::ChunkRows(txn);
    assert_eq!(
        chunk.canonical_bytes().unwrap(),
        unhex(concat!(
            "534b43520001534b45560001020000000270670000000173000000016c000000026462",
            "00000004736c6f740000000101000000016400000001090000000000",
            "dba702fe63fa8822942316ddf436a2bd27dc86473bd74bbe3f3001152fe0b780",
            "d799104d0d304d1f1a840f0de781a58b2bd0029bb9c1a291c65ab543ae4b9cd6"
        ))
    );
}

#[test]
fn transactions_chunks_and_empty_segments_round_trip() {
    for records in [
        vec![],
        vec![transaction()],
        vec![transaction(), chunk(8192), transaction()],
    ] {
        let bytes = encode(&records);
        let mut decoder = SegmentDecoder::new(Cursor::new(&bytes), Limits::default()).unwrap();
        assert_eq!(decoder.header(), &header(records.len() as u64));
        for expected in &records {
            assert_eq!(decoder.next_record().unwrap().as_ref(), Some(expected));
        }
        assert!(decoder.next_record().unwrap().is_none());
        assert!(decoder.next_record().unwrap().is_none());
        let (_, summary) = decoder.finish().unwrap();
        assert_eq!(summary.bytes, bytes.len() as u64);
        assert_eq!(summary.digest.as_slice(), &bytes[bytes.len() - 32..]);
        assert_eq!(decode(&bytes).unwrap(), records);
    }
    // finish must drain and validate records even when the caller read none.
    let bytes = encode(&[chunk(0), transaction()]);
    assert_eq!(
        SegmentDecoder::new(bytes.as_slice(), Limits::default())
            .unwrap()
            .finish()
            .unwrap()
            .1
            .record_count,
        2
    );
}

#[test]
fn chunk_rows_preserve_atomic_multirow_and_empty_batches() {
    let SegmentRecord::ChunkRows(mut batch) = chunk(32) else {
        unreachable!()
    };
    let mut second = batch.events[0].clone();
    second.ordinal_in_txn = 1;
    second.key.insert(
        "id".into(),
        OpaqueValue {
            type_id: "pg/int4".into(),
            bytes: Some(b"2".to_vec()),
        },
    );
    batch.events.push(second);
    let record = SegmentRecord::ChunkRows(reseal(batch));
    let bytes = encode(std::slice::from_ref(&record));
    assert_eq!(decode(&bytes).unwrap(), vec![record.clone()]);
    assert_eq!(
        SegmentDecoder::new(bytes.as_slice(), Limits::default())
            .unwrap()
            .finish()
            .unwrap()
            .1
            .record_count,
        1
    );
    let SegmentRecord::ChunkRows(mut batch) = record else {
        unreachable!()
    };
    batch.events.clear();
    let empty = SegmentRecord::ChunkRows(reseal(batch));
    assert_eq!(
        decode(&encode(std::slice::from_ref(&empty))).unwrap(),
        vec![empty]
    );
    for field in ["position", "table", "schema"] {
        let SegmentRecord::ChunkRows(mut batch) = chunk(0) else {
            unreachable!()
        };
        let mut second = batch.events[0].clone();
        second.ordinal_in_txn = 1;
        match field {
            "position" => second.position = Position::new("d", vec![10]),
            "table" => second.table = Some("public.other".into()),
            _ => second.schema_version += 1,
        }
        batch.events.push(second);
        assert!(
            SegmentRecord::ChunkRows(reseal(batch))
                .canonical_bytes()
                .is_err()
        );
    }
}

#[test]
fn rejects_unknown_version_and_wrong_magic() {
    let mut bytes = encode(&[transaction()]);
    bytes[9] = 2;
    assert!(matches!(
        decode(&bytes),
        Err(FormatError::UnsupportedVersion(2))
    ));
    bytes[9] = 1;
    bytes[0] ^= 1;
    assert!(matches!(decode(&bytes), Err(FormatError::WrongMagic)));
    let mut bytes = encode(&[transaction()]);
    let end_magic = bytes.len() - 48;
    bytes[end_magic] ^= 1;
    assert!(matches!(decode(&bytes), Err(FormatError::WrongMagic)));
}

#[test]
fn rejects_every_truncation_including_record_and_trailer() {
    let bytes = encode(&[transaction(), chunk(64)]);
    for end in 0..bytes.len() {
        assert!(decode(&bytes[..end]).is_err(), "accepted truncation {end}");
    }
    let end = record_start(&bytes) + 8 + 20;
    assert!(matches!(
        decode(&bytes[..end]),
        Err(FormatError::Truncated("record"))
    ));
}

#[test]
fn rejects_record_digest_mismatch() {
    let mut bytes = encode(&[transaction()]);
    let start = record_start(&bytes);
    bytes[start + 8 + 12] ^= 1;
    resign_segment(&mut bytes);
    assert!(matches!(
        decode(&bytes),
        Err(FormatError::RecordDigestMismatch)
    ));
}

#[test]
fn rejects_trailer_digest_and_authenticated_header_tampering() {
    let mut bytes = encode(&[transaction()]);
    let end = bytes.len() - 1;
    bytes[end] ^= 1;
    assert!(matches!(
        decode(&bytes),
        Err(FormatError::TrailerDigestMismatch)
    ));
    let mut bytes = encode(&[transaction()]);
    bytes[18] = b'n'; // migration id, still a valid header
    assert!(matches!(
        decode(&bytes),
        Err(FormatError::TrailerDigestMismatch)
    ));
}

#[test]
fn rejects_header_trailer_and_actual_count_mismatches() {
    for count in [0, 2, u64::MAX] {
        let mut bytes = encode(&[transaction()]);
        let start = record_start(&bytes);
        bytes[start - 8..start].copy_from_slice(&count.to_be_bytes());
        resign_segment(&mut bytes);
        assert!(matches!(
            decode(&bytes),
            Err(FormatError::CountMismatch { .. })
        ));
    }
    let mut bytes = encode(&[transaction()]);
    let end = bytes.len() - 32;
    bytes[end - 8..end].copy_from_slice(&2u64.to_be_bytes());
    resign_segment(&mut bytes);
    assert!(matches!(
        decode(&bytes),
        Err(FormatError::CountMismatch { .. })
    ));
}

#[test]
fn rejects_trailing_garbage_and_concatenated_segments() {
    let bytes = encode(&[transaction()]);
    for suffix in [vec![0], vec![0xff; 8], bytes.clone()] {
        let mut mutated = bytes.clone();
        mutated.extend(suffix);
        assert!(matches!(
            decode(&mutated),
            Err(FormatError::TrailingGarbage)
        ));
    }
}

#[test]
fn max_length_guard_runs_before_payload_read_or_allocation() {
    struct PrefixOnly {
        bytes: Cursor<Vec<u8>>,
        forbidden_reads: usize,
    }
    impl Read for PrefixOnly {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if self.bytes.position() == self.bytes.get_ref().len() as u64 {
                self.forbidden_reads += 1;
                panic!("decoder tried to read the oversized payload");
            }
            self.bytes.read(out)
        }
    }
    let limits = Limits {
        max_header_bytes: 1024,
        max_record_bytes: 128,
    };
    for length in [129, u32::MAX as u64, u64::MAX] {
        let bytes = encode(&[transaction()]);
        let start = record_start(&bytes);
        let mut prefix = bytes[..start].to_vec();
        prefix.extend(length.to_be_bytes());
        let mut reader = PrefixOnly {
            bytes: Cursor::new(prefix),
            forbidden_reads: 0,
        };
        let mut decoder = SegmentDecoder::new(&mut reader, limits).unwrap();
        let before = decoder.peak_buffer_bytes();
        ALLOCATIONS.with(|cell| {
            cell.set(Allocations {
                enabled: true,
                ..Default::default()
            })
        });
        let result = decoder.next_record();
        let peak = ALLOCATIONS.with(|cell| {
            let peak = cell.get().peak;
            cell.set(Allocations::default());
            peak
        });
        assert!(
            matches!(result, Err(FormatError::LengthLimit { field: "record", length: n, maximum: 128 }) if n == length)
        );
        assert_eq!(peak, 0, "oversized frame allocated memory");
        assert_eq!(decoder.peak_buffer_bytes(), before);
        assert_eq!(reader.forbidden_reads, 0);
    }
    let mut bytes = encode(&[]);
    bytes[10..14].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        SegmentDecoder::new(&bytes[..14], limits),
        Err(FormatError::LengthLimit {
            field: "header",
            ..
        })
    ));
    let exact = encode(&[transaction()]);
    let length = u64::from_be_bytes(
        exact[record_start(&exact)..record_start(&exact) + 8]
            .try_into()
            .unwrap(),
    );
    assert!(
        SegmentDecoder::new(
            exact.as_slice(),
            Limits {
                max_record_bytes: length,
                ..limits
            }
        )
        .unwrap()
        .finish()
        .is_ok()
    );
}

#[test]
fn encoder_enforces_bounds_counts_and_record_kinds() {
    let mut encoder = SegmentEncoder::new(
        Vec::new(),
        header(1),
        Limits {
            max_record_bytes: 8,
            ..Limits::default()
        },
    )
    .unwrap();
    assert!(matches!(
        encoder.write_record(&transaction()),
        Err(FormatError::LengthLimit { .. })
    ));
    assert!(matches!(encoder.finish(), Err(FormatError::Poisoned)));
    assert!(matches!(
        SegmentEncoder::new(Vec::new(), header(1), Limits::default())
            .unwrap()
            .finish(),
        Err(FormatError::CountMismatch {
            expected: 1,
            actual: 0
        })
    ));
    let mut encoder = SegmentEncoder::new(Vec::new(), header(0), Limits::default()).unwrap();
    assert!(matches!(
        encoder.write_record(&transaction()),
        Err(FormatError::CountMismatch {
            expected: 0,
            actual: 1
        })
    ));
    let SegmentRecord::ChunkRows(mut txn) = chunk(0) else {
        unreachable!()
    };
    txn.events[0].kind = EventKind::Insert;
    let mut encoder = SegmentEncoder::new(Vec::new(), header(1), Limits::default()).unwrap();
    assert!(matches!(
        encoder.write_record(&SegmentRecord::ChunkRows(reseal(txn))),
        Err(FormatError::Invalid(_))
    ));
    assert!(matches!(
        SegmentEncoder::new(
            Vec::new(),
            header(1),
            Limits {
                max_header_bytes: 1,
                ..Limits::default()
            }
        ),
        Err(FormatError::LengthLimit {
            field: "header",
            ..
        })
    ));
}

#[test]
fn rejects_invalid_envelopes_even_when_segment_digest_is_valid() {
    let bytes = encode(&[chunk(0)]);
    let start = record_start(&bytes) + 8;
    let length = u64::from_be_bytes(bytes[start - 8..start].try_into().unwrap()) as usize;
    for offset in [0, 5, 6, 16] {
        let mut mutated = bytes.clone();
        mutated[start + offset] ^= 255;
        let digest = Sha256::digest(&mutated[start..start + length - 32]);
        mutated[start + length - 32..start + length].copy_from_slice(&digest);
        resign_segment(&mut mutated);
        assert!(decode(&mutated).is_err());
    }
    let SegmentRecord::ChunkRows(mut txn) = chunk(0) else {
        unreachable!()
    };
    txn.events[0].kind = EventKind::Insert;
    let mut mutated = bytes;
    let mut invalid = b"SKCR\0\x01".to_vec();
    invalid.extend(reseal(txn).canonical_bytes().unwrap());
    invalid.extend(Sha256::digest(&invalid).as_slice());
    mutated[start..start + length].copy_from_slice(&invalid);
    resign_segment(&mut mutated);
    assert!(matches!(
        decode(&mutated),
        Err(FormatError::Invalid("expected ChunkRows event"))
    ));
}

#[test]
fn rejects_mismatched_endpoint_and_domain_without_global_position_order() {
    let mut bad = header(1);
    bad.last_pos = Position::new("other", vec![9]);
    assert!(SegmentEncoder::new(Vec::new(), bad, Limits::default()).is_err());
    let mut bad = header(1);
    bad.first_pos = Position::new("d", vec![10]);
    bad.last_pos = bad.first_pos.clone();
    let mut encoder = SegmentEncoder::new(Vec::new(), bad, Limits::default()).unwrap();
    assert!(matches!(
        encoder.write_record(&transaction()),
        Err(FormatError::Invalid("segment endpoint position"))
    ));
    let mut bytes = encode(&[transaction()]);
    // first and last position domains remain equal but differ from the record.
    bytes[36] = b'e';
    bytes[46] = b'e';
    resign_segment(&mut bytes);
    assert!(decode(&bytes).is_err());
}

#[test]
fn random_mutations_and_truncations_never_panic_ten_thousand_iterations() {
    let original = encode(&[transaction(), chunk(64), transaction()]);
    let mut seed = 0x534b_4544_5345_4731u64;
    let mut random = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for iteration in 0..10_000 {
        let mut bytes = original.clone();
        let flips = (random() % 8 + 1) as usize;
        for _ in 0..flips {
            let index = random() as usize % bytes.len();
            bytes[index] ^= 1 << (random() % 8);
        }
        if iteration % 2 == 0 {
            bytes.truncate(random() as usize % (bytes.len() + 1));
        }
        if iteration % 4 == 0 && bytes.len() >= 32 {
            resign_segment(&mut bytes);
        }
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ = decode(&bytes);
            }))
            .is_ok(),
            "panic at iteration {iteration}"
        );
    }
}

struct Fragmented<T>(T);
impl<T: Read> Read for Fragmented<T> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let len = out.len().min(3);
        self.0.read(&mut out[..len])
    }
}
impl<T: Write> Write for Fragmented<T> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write(&bytes[..bytes.len().min(3)])
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

#[test]
fn handles_short_reads_writes_io_errors_and_poisoned_streams() {
    let mut encoder =
        SegmentEncoder::new(Fragmented(Vec::new()), header(1), Limits::default()).unwrap();
    encoder.write_record(&transaction()).unwrap();
    let bytes = encoder.finish().unwrap().0.0;
    assert_eq!(
        SegmentDecoder::new(Fragmented(bytes.as_slice()), Limits::default())
            .unwrap()
            .finish()
            .unwrap()
            .1
            .record_count,
        1
    );
    struct Broken;
    impl Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("fault injection"))
        }
    }
    impl Write for Broken {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("fault injection"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("fault injection"))
        }
    }
    assert!(matches!(
        SegmentDecoder::new(Broken, Limits::default()),
        Err(FormatError::Io(_))
    ));
    assert!(matches!(
        SegmentEncoder::new(Broken, header(1), Limits::default()),
        Err(FormatError::Io(_))
    ));
    let bytes = encode(&[transaction()]);
    let mut decoder = SegmentDecoder::new(&bytes[..bytes.len() - 1], Limits::default()).unwrap();
    assert!(decoder.next_record().unwrap().is_some());
    assert!(decoder.next_record().is_err());
    assert!(matches!(decoder.next_record(), Err(FormatError::Poisoned)));
    assert!(matches!(decoder.finish(), Err(FormatError::Poisoned)));
}

#[test]
fn streams_more_than_64_mib_with_small_measured_peak_memory() {
    let root = std::env::var_os("SKEDADDLE_TEST_ROOT")
        .expect("SKEDADDLE_TEST_ROOT must point under the J: task folder");
    let root = std::path::PathBuf::from(root).join("buffer-format");
    fs::create_dir_all(&root).unwrap();
    let path = root.join(format!("large-{}.seg", std::process::id()));
    ALLOCATIONS.with(|cell| {
        cell.set(Allocations {
            enabled: true,
            ..Default::default()
        })
    });
    let record = chunk(16 * 1024);
    const COUNT: u64 = 4200;
    let limits = Limits {
        max_record_bytes: 32 * 1024,
        ..Limits::default()
    };
    let mut encoder = SegmentEncoder::new(
        BufWriter::new(File::create(&path).unwrap()),
        header(COUNT),
        limits,
    )
    .unwrap();
    for _ in 0..COUNT {
        encoder.write_record(&record).unwrap();
    }
    let (writer, encoded) = encoder.finish().unwrap();
    drop(writer);
    drop(record);
    let mut decoder =
        SegmentDecoder::new(BufReader::new(File::open(&path).unwrap()), limits).unwrap();
    let mut count = 0;
    while let Some(record) = decoder.next_record().unwrap() {
        assert!(matches!(record, SegmentRecord::ChunkRows(_)));
        count += 1;
    }
    let (_, decoded) = decoder.finish().unwrap();
    let peak = ALLOCATIONS.with(|cell| {
        let peak = cell.get().peak;
        cell.set(Allocations::default());
        peak
    });
    assert_eq!(count, COUNT);
    assert_eq!(encoded.bytes, fs::metadata(&path).unwrap().len());
    assert!(encoded.bytes > 64 * 1024 * 1024);
    assert_eq!(encoded.digest, decoded.digest);
    assert_eq!(encoded.bytes, decoded.bytes);
    assert!(encoded.peak_buffer_bytes < 128 * 1024);
    assert!(decoded.peak_buffer_bytes <= 32 * 1024);
    assert!(peak < 1024 * 1024, "peak live allocation: {peak}");
    println!(
        "segment_bytes={} encoder_peak_buffer={} decoder_peak_buffer={} peak_live_allocations={peak}",
        encoded.bytes, encoded.peak_buffer_bytes, decoded.peak_buffer_bytes
    );
    // Retain evidence; common-rules.md forbids agent deletion.
}

fn manifest() -> ManifestEntry {
    ManifestEntry {
        mseq: 1,
        epoch: 7,
        prev_digest: [0; 32],
        segment_key: "p/buffer/m/s/seg/x64.09-x64.09-7.seg".into(),
        segment_digest: unhex("4d6fa58b20fa624e7cb98feb2028ff1d260d56adb7651d680797b1919b05d920")
            .try_into()
            .unwrap(),
        first_pos: pos(),
        last_pos: pos(),
        txn_count: 1,
        bytes: 213,
    }
}

#[test]
fn manifest_has_canonical_json_and_independent_self_digest() {
    let entry = manifest();
    // Independently calculated with Python json.dumps(sort_keys=True,
    // separators=(',', ':')) + hashlib.sha256, excluding the digest field.
    assert_eq!(
        entry.digest().unwrap().as_slice(),
        unhex("87b8cc9a79171c99e5a3fb33d3d0565e733b9ebcdd40b1b96fb70bab6e790c31")
    );
    let bytes = entry.canonical_json().unwrap();
    let golden = concat!(
        "{\"bytes\":213,\"digest\":\"87b8cc9a79171c99e5a3fb33d3d0565e733b9ebcdd40b1b96fb70bab6e790c31\",\"epoch\":7,",
        "\"first_pos\":{\"bytes\":\"09\",\"order_domain\":\"d\"},\"last_pos\":{\"bytes\":\"09\",\"order_domain\":\"d\"},\"mseq\":1,",
        "\"prev_digest\":\"0000000000000000000000000000000000000000000000000000000000000000\",",
        "\"segment_digest\":\"4d6fa58b20fa624e7cb98feb2028ff1d260d56adb7651d680797b1919b05d920\",",
        "\"segment_key\":\"p/buffer/m/s/seg/x64.09-x64.09-7.seg\",\"txn_count\":1}"
    );
    assert_eq!(bytes, golden.as_bytes());
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        value["digest"],
        "87b8cc9a79171c99e5a3fb33d3d0565e733b9ebcdd40b1b96fb70bab6e790c31"
    );
    assert_eq!(ManifestEntry::from_canonical_json(&bytes).unwrap(), entry);
    let pretty = serde_json::to_vec_pretty(&value).unwrap();
    assert!(matches!(
        ManifestEntry::from_canonical_json(&pretty),
        Err(FormatError::Invalid("noncanonical manifest JSON"))
    ));
}

#[test]
fn manifest_rejects_tampering_unknown_duplicate_and_invalid_digest_fields() {
    let entry = manifest();
    let original = entry.canonical_json().unwrap();
    let value: serde_json::Value = serde_json::from_slice(&original).unwrap();
    for field in [
        "mseq",
        "epoch",
        "segment_key",
        "segment_digest",
        "first_pos",
        "last_pos",
        "txn_count",
        "bytes",
        "prev_digest",
        "digest",
    ] {
        let mut changed = value.clone();
        changed[field] = if changed[field].is_number() {
            999.into()
        } else {
            "bad".into()
        };
        assert!(
            ManifestEntry::from_canonical_json(&serde_json::to_vec(&changed).unwrap()).is_err(),
            "accepted changed {field}"
        );
    }
    let mut changed = value;
    changed["epoch"] = 8.into();
    assert!(matches!(
        ManifestEntry::from_canonical_json(&serde_json::to_vec(&changed).unwrap()),
        Err(FormatError::ManifestDigestMismatch)
    ));
    changed["surprise"] = true.into();
    assert!(matches!(
        ManifestEntry::from_canonical_json(&serde_json::to_vec(&changed).unwrap()),
        Err(FormatError::Json(_))
    ));
    let mut duplicate = br#"{"mseq":1,"#.to_vec();
    duplicate.extend(&original[1..]);
    assert!(matches!(
        ManifestEntry::from_canonical_json(&duplicate),
        Err(FormatError::Json(_))
    ));
}

#[test]
fn key_layout_padding_order_and_components_are_stable() {
    assert_eq!(
        manifest_key("p/", "m", "s", 1),
        "p/buffer/m/s/manifest/00000000000000000001.json"
    );
    assert_eq!(
        segment_key("p", "m", "s", &pos(), &pos(), 7),
        "p/buffer/m/s/seg/x64.09-x64.09-7.seg"
    );
    assert_eq!(
        manifest_key("", "m", "s", 0),
        "buffer/m/s/manifest/00000000000000000000.json"
    );
    let sequences = [0, 1, 9, 10, 99, 100, 999_999, u64::MAX];
    let keys: Vec<_> = sequences
        .iter()
        .map(|seq| manifest_key("p", "m", "s", *seq))
        .collect();
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(
        manifest_key("p", "../a", "/s%", 1),
        "p/buffer/%2E%2E%2Fa/%2Fs%25/manifest/00000000000000000001.json"
    );
    assert_ne!(
        manifest_key("p", "a/b", "s", 1),
        manifest_key("p", "a%2Fb", "s", 1)
    );
    assert_ne!(
        manifest_key("p", "", "s", 1),
        manifest_key("p", "~", "s", 1)
    );
    let other_domain = Position::new("other", vec![9]);
    assert_ne!(
        segment_key("p", "m", "s", &pos(), &pos(), 7),
        segment_key("p", "m", "s", &other_domain, &other_domain, 7)
    );
    let mut next = manifest();
    next.mseq = 2;
    next.prev_digest = manifest().digest().unwrap();
    assert_eq!(
        ManifestEntry::from_canonical_json(&next.canonical_json().unwrap()).unwrap(),
        next
    );
}

#[test]
fn zero_record_length_is_only_a_trailer_sentinel() {
    let mut bytes = encode(&[transaction()]);
    let start = record_start(&bytes);
    bytes[start..start + 8].fill(0);
    resign_segment(&mut bytes);
    assert!(decode(&bytes).is_err());
    let bytes = encode(&[]);
    assert_eq!(
        &bytes[record_start(&bytes) + 8..record_start(&bytes) + 16],
        END_MAGIC
    );
}
