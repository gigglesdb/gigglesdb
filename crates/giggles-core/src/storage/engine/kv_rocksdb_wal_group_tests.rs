use super::{tests::TestDir, *};
use std::fs;

const CACHE_BYTES: usize = 8 << 20;
const WAL_BLOCK: usize = 32 * 1024;
const WAL_HEADER: usize = 7;
const HOLE_MARKER: &[u8] = b"gd-0084-group-three-block";

fn open(path: &Path) -> RocksKv {
    let was_wal_only = WAL_ONLY_SHUTDOWN.replace(true);
    let result = RocksKv::open(path, CACHE_BYTES);
    WAL_ONLY_SHUTDOWN.set(was_wal_only);
    result.unwrap()
}

pub(super) fn initialize_marked(path: &Path) {
    if !path.join("CURRENT").exists() {
        drop(open(path));
    }
    meta::write_marker(path).unwrap();
    mark_commit_groups(path).unwrap();
}

fn group(db: &RocksKv, number: u64, value: Vec<u8>) {
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Rows, number.to_be_bytes().to_vec(), value);
    batch.put_commit_group(number);
    db.append_commit(batch).unwrap();
}

fn small_group(db: &RocksKv, number: u64) {
    group(db, number, vec![number as u8]);
}

fn commit_group(db: &RocksKv) -> Option<u64> {
    db.get(Keyspace::Meta, COMMIT_GROUP_KEY)
        .unwrap()
        .map(|value| u64::from_be_bytes(value.try_into().unwrap()))
}

fn assert_rows(db: &RocksKv, present: std::ops::RangeInclusive<u64>, absent: &[u64]) {
    for row in present {
        assert_eq!(
            db.get(Keyspace::Rows, &row.to_be_bytes()).unwrap(),
            Some(vec![row as u8]),
            "row from group {row}"
        );
    }
    for row in absent {
        assert_eq!(db.get(Keyspace::Rows, &row.to_be_bytes()).unwrap(), None);
    }
}

fn current_wal(path: &Path) -> PathBuf {
    path.read_dir()
        .unwrap()
        .map(|file| file.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .max()
        .unwrap()
}

fn wal_len(path: &Path) -> usize {
    path.metadata().unwrap().len().try_into().unwrap()
}

fn zeroable_group_three(db: &RocksKv, wal: &Path) {
    // End a padding record at the block boundary, then fill the next block
    // with exactly one physical record. A zero block is skipped without
    // invoking RocksDB's own corrupt-record recovery.
    let rows = db.db.cf_handle(Keyspace::Rows.name()).unwrap();
    let remaining = WAL_BLOCK - wal_len(wal) % WAL_BLOCK;
    let mut sizing = rocksdb::WriteBatch::default();
    sizing.put_cf(rows, b"padding", vec![0; remaining]);
    let overhead = sizing.size_in_bytes() - remaining + WAL_HEADER;
    let mut padding = WriteBatch::default();
    padding.put(
        Keyspace::Rows,
        b"padding".to_vec(),
        vec![0; remaining - overhead],
    );
    db.write(padding, Durability::NoSync).unwrap();
    let start = wal_len(wal);
    assert_eq!(start % WAL_BLOCK, 0);

    let meta = db.db.cf_handle(Keyspace::Meta.name()).unwrap();
    let mut sizing = rocksdb::WriteBatch::default();
    sizing.put_cf(rows, 3u64.to_be_bytes(), vec![0; WAL_BLOCK]);
    sizing.put_cf(meta, COMMIT_GROUP_KEY, 3u64.to_be_bytes());
    let overhead = sizing.size_in_bytes() - WAL_BLOCK + WAL_HEADER;
    let mut payload = vec![0; WAL_BLOCK - overhead];
    payload[..HOLE_MARKER.len()].copy_from_slice(HOLE_MARKER);
    group(db, 3, payload);
    assert_eq!(wal_len(wal), start + WAL_BLOCK);
}

fn copy_with_interior_hole(copy: &Path, flushed_seed: bool, flush_later_cf: bool) -> u64 {
    let source = TestDir::new();
    initialize_marked(&source.0);
    let db = open(&source.0);
    small_group(&db, 1);
    if flushed_seed {
        db.flush_one_memtable(Keyspace::Meta).unwrap();
    }
    small_group(&db, 2);
    db.sync_commits().unwrap();
    let wal = current_wal(&source.0);
    zeroable_group_three(&db, &wal);
    small_group(&db, 4);
    small_group(&db, 5);
    let mut final_sync = WriteBatch::default();
    final_sync.put(Keyspace::Index, b"later".to_vec(), b"synced".to_vec());
    db.write(final_sync, Durability::Sync).unwrap();
    if flush_later_cf {
        db.flush_one_memtable(Keyspace::Index).unwrap();
    }
    assert_eq!(db.has_sst(Keyspace::Meta), flushed_seed);
    assert_eq!(db.has_sst(Keyspace::Index), flush_later_cf);
    drop(db);

    copy_store_files(&source.0, copy);
    let wal = copy.join(wal.file_name().unwrap());
    let mut bytes = fs::read(&wal).unwrap();
    let mut markers = bytes
        .windows(HOLE_MARKER.len())
        .enumerate()
        .filter_map(|(offset, window)| (window == HOLE_MARKER).then_some(offset));
    let marker = markers.next().unwrap();
    assert!(markers.next().is_none());
    let start = marker / WAL_BLOCK * WAL_BLOCK;
    let header = &bytes[start..start + WAL_HEADER];
    assert_eq!(header[6], 1, "the hole must be one full physical record");
    assert_eq!(
        usize::from(u16::from_le_bytes([header[4], header[5]])),
        WAL_BLOCK - WAL_HEADER
    );
    bytes[start..start + WAL_BLOCK].fill(0);
    fs::write(&wal, bytes).unwrap();
    wal_number(wal.file_name().unwrap().to_str().unwrap()).unwrap()
}

fn copy_store_files(source: &Path, copy: &Path) {
    for file in source.read_dir().unwrap() {
        let file = file.unwrap();
        if file.file_type().unwrap().is_file() {
            fs::copy(file.path(), copy.join(file.file_name())).unwrap();
        }
    }
}

fn assert_gap(db: &RocksKv, wal: u64, expected: u64, found: u64) {
    let stop = db.wal_stop.expect("replay should have stopped");
    assert_eq!(
        (stop.reason, stop.wal, stop.expected, stop.found),
        (1, wal, expected, found)
    );
}

#[test]
fn no_meta_group_keeps_replay_unchanged() {
    let dir = TestDir::new();
    let db = open(&dir.0);
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Rows,
        COMMIT_GROUP_KEY.to_vec(),
        b"unrelated".to_vec(),
    );
    batch.put(Keyspace::Rows, b"remove".to_vec(), b"old".to_vec());
    db.write(batch, Durability::NoSync).unwrap();
    let mut batch = WriteBatch::default();
    batch.delete(Keyspace::Rows, b"remove".to_vec());
    db.write(batch, Durability::Sync).unwrap();
    drop(db);

    let db = open(&dir.0);
    assert_eq!(commit_group(&db), None);
    assert_eq!(db.wal_stop, None);
    assert_eq!(
        db.get(Keyspace::Rows, COMMIT_GROUP_KEY).unwrap(),
        Some(b"unrelated".to_vec()),
    );
    assert_eq!(db.get(Keyspace::Rows, b"remove").unwrap(), None);
}

#[test]
fn chain_crosses_reopens_and_partial_memtable_flushes() {
    let dir = TestDir::new();
    initialize_marked(&dir.0);
    let mut db = open(&dir.0);
    for first in [1, 4, 7] {
        small_group(&db, first);
        small_group(&db, first + 1);
        db.flush_one_memtable(Keyspace::Meta).unwrap();
        small_group(&db, first + 2);
        db.sync_commits().unwrap();
        drop(db);

        db = open(&dir.0);
        assert_eq!(db.wal_stop, None);
        assert_eq!(commit_group(&db), Some(first + 2));
        assert_rows(&db, 1..=first + 2, &[]);
    }
}

#[test]
fn interior_hole_stops_at_next_group_with_seeded_meta() {
    let copy = TestDir::new();
    let wal = copy_with_interior_hole(&copy.0, true, false);
    let db = open(&copy.0);
    assert_gap(&db, wal, 3, 4);
    assert_rows(&db, 1..=2, &[3, 4, 5]);
    assert_eq!(commit_group(&db), Some(2));
    assert_eq!(db.get(Keyspace::Index, b"later").unwrap(), None);
}

#[test]
fn first_generation_hole_stops_and_recovery_retires_the_wal() {
    let copy = TestDir::new();
    let wal = copy_with_interior_hole(&copy.0, false, false);
    let db = open(&copy.0);
    assert_gap(&db, wal, 3, 4);
    assert_rows(&db, 1..=2, &[3, 4, 5]);
    assert_eq!(commit_group(&db), Some(2));
    assert!(db.has_sst(Keyspace::Meta));

    small_group(&db, 3);
    small_group(&db, 4);
    db.sync_commits().unwrap();
    drop(db);
    let db = open(&copy.0);
    assert_eq!(db.wal_stop, None);
    assert_eq!(commit_group(&db), Some(4));
    assert_rows(&db, 1..=4, &[5]);
    assert_eq!(db.get(Keyspace::Index, b"later").unwrap(), None);
}

#[test]
fn wal_start_gap_uses_sst_seed() {
    let dir = TestDir::new();
    initialize_marked(&dir.0);
    let db = open(&dir.0);
    small_group(&db, 1);
    db.sync_commits().unwrap();
    db.flush_memtables().unwrap();
    small_group(&db, 3);
    db.sync_commits().unwrap();
    let wal = current_wal(&dir.0);
    let wal = wal_number(wal.file_name().unwrap().to_str().unwrap()).unwrap();
    drop(db);

    #[cfg(any(target_os = "linux", windows))]
    let db = RocksKv::open_recovery(
        DirectoryIdentity::read(&dir.0)
            .unwrap()
            .pin(&dir.0)
            .unwrap(),
        CACHE_BYTES,
    )
    .unwrap();
    #[cfg(not(any(target_os = "linux", windows)))]
    let db = open(&dir.0);
    assert_gap(&db, wal, 2, 3);
    assert_rows(&db, 1..=1, &[2, 3]);
    assert_eq!(commit_group(&db), Some(1));
}

fn data_error(path: &Path) -> String {
    let Err(error) = RocksKv::open(path, CACHE_BYTES) else {
        panic!("unsafe recovery opened successfully");
    };
    assert_eq!(
        FenceCause::for_kv(FenceSite::Open, &error).class,
        FenceClass::Data
    );
    error.to_string()
}

#[test]
fn live_sst_beyond_stopping_wal_refuses_recovery() {
    let copy = TestDir::new();
    let wal = copy_with_interior_hole(&copy.0, false, true);
    let detail = format!("beyond the commit group gap at WAL {wal} (expected 3, found 4)");
    assert!(data_error(&copy.0).contains(&detail));
    assert!(
        data_error(&copy.0).contains(&detail),
        "the refused open must preserve the WAL"
    );
}

#[test]
fn repeated_and_backwards_groups_above_seed_refuse_recovery() {
    for unexpected in [2, 1] {
        let dir = TestDir::new();
        initialize_marked(&dir.0);
        let db = open(&dir.0);
        small_group(&db, 1);
        small_group(&db, 2);
        small_group(&db, unexpected);
        db.sync_commits().unwrap();
        drop(db);

        let detail = format!("expected 3, found {unexpected}");
        assert!(data_error(&dir.0).contains(&detail));
        assert!(data_error(&dir.0).contains(&detail));
    }
}

fn file_contents(path: &Path) -> BTreeMap<std::ffi::OsString, Vec<u8>> {
    path.read_dir()
        .unwrap()
        .map(|file| file.unwrap())
        .filter(|file| file.file_name() != WRITER_LOCK_FILE && file.file_type().unwrap().is_file())
        .map(|file| (file.file_name(), fs::read(file.path()).unwrap()))
        .collect()
}

#[test]
fn seed_open_failure_has_writable_open_class_and_leaves_files_intact() {
    let dir = TestDir::new();
    initialize_marked(&dir.0);
    drop(open(&dir.0));
    fs::write(dir.0.join("CURRENT"), b"").unwrap();
    let before = file_contents(&dir.0);

    let Err(seed_error) = RocksKv::open(&dir.0, CACHE_BYTES) else {
        panic!("invalid CURRENT was accepted");
    };
    assert_eq!(file_contents(&dir.0), before);

    let options = RocksKv::open_options(CACHE_BYTES, DBRecoveryMode::PointInTime).unwrap();
    let Err(native_error) = DB::open_cf_with_opts(
        &options,
        native_path(&dir.0),
        Keyspace::ALL.map(|ks| (ks.name(), options.clone())),
    ) else {
        panic!("native open accepted invalid CURRENT");
    };
    let native_error = map_error_at(native_error, FenceSite::Open);
    assert_eq!(
        FenceCause::for_kv(FenceSite::Open, &seed_error).class,
        FenceCause::for_kv(FenceSite::Open, &native_error).class,
    );
}

#[test]
fn seed_open_allows_a_partially_initialized_store() {
    let dir = TestDir::new();
    let mut options = Options::default();
    options.create_if_missing(true);
    let raw = DB::open(&options, native_path(&dir.0)).unwrap();
    raw.put(b"existing", b"value").unwrap();
    drop(raw);

    initialize_marked(&dir.0);
    let db = open(&dir.0);
    assert_eq!(db.db.get(b"existing").unwrap(), Some(b"value".to_vec()));
    assert_eq!(commit_group(&db), None);
    assert_eq!(db.wal_stop, None);
}

#[test]
fn unmarked_local_and_replicated_open_refuse_commit_group_records() {
    for malformed in [false, true] {
        let dir = TestDir::new();
        let db = open(&dir.0);
        let mut batch = WriteBatch::default();
        batch.put(
            Keyspace::Meta,
            COMMIT_GROUP_KEY.to_vec(),
            if malformed { vec![0] } else { 1u64.to_be_bytes().to_vec() },
        );
        db.write(batch, Durability::Sync).unwrap();
        drop(db);

        assert!(data_error(&dir.0).contains("requires a marked local store"));
        let Err(error) = RocksKv::open_replicated(&dir.0, CACHE_BYTES) else {
            panic!("replicated open accepted a commit group");
        };
        assert_eq!(FenceCause::for_kv(FenceSite::Open, &error).class, FenceClass::Data);
    }
}

#[test]
fn marked_format_is_persistent_and_replicated_open_refuses_it() {
    let dir = TestDir::new();
    initialize_marked(&dir.0);
    mark_commit_groups(&dir.0).unwrap();
    assert_eq!(meta::classify_dir(&dir.0).unwrap(), meta::DirClass::V2);
    assert!(open(&dir.0).pipelined_sync());
    let before = file_contents(&dir.0);
    assert!(matches!(RocksKv::open_replicated(&dir.0, CACHE_BYTES), Err(KvError::Corrupt(_))));
    assert_eq!(file_contents(&dir.0), before);
}

fn assert_pin_refused(path: &Path) {
    let Err(error) = RocksKv::open(path, CACHE_BYTES) else {
        panic!("opened marked store without a usable directory pin");
    };
    assert!(matches!(error, KvError::OpenRefused(_)), "{error}");
    let failure = crate::storage::v2::OpenFailure::from(error);
    assert_eq!(failure.cause.class, FenceClass::NotRecoverable);
}

#[cfg(windows)]
#[test]
fn marked_store_without_a_directory_pin_is_not_recoverable() {
    use std::os::windows::fs::OpenOptionsExt;

    let dir = TestDir::new();
    initialize_marked(&dir.0);
    let _deny_pin = OpenOptions::new()
        .access_mode(1) // FILE_LIST_DIRECTORY
        .share_mode(0)
        .custom_flags(0x0200_0000) // FILE_FLAG_BACKUP_SEMANTICS
        .open(&dir.0)
        .unwrap();
    assert_pin_refused(&dir.0);
}

#[cfg(target_os = "linux")]
#[test]
fn marked_store_on_9p_is_not_recoverable() {
    let Some(parent) = std::env::var_os("GIGGLES_TEST_9P_DIR") else {
        return; // Set to a writable directory on a 9p mount.
    };
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = PathBuf::from(parent).join(format!("kv-9p-{:032x}", u128::from_be_bytes(nonce)));
    fs::create_dir(&path).unwrap();
    let dir = TestDir(path);
    let source = TestDir::new();
    initialize_marked(&source.0);
    copy_store_files(&source.0, &dir.0);
    let pin = DirectoryIdentity::read(&dir.0).unwrap().pin(&dir.0).unwrap();
    let c_path = CString::new(pin.path().as_os_str().as_encoded_bytes()).unwrap();
    // SAFETY: the NUL-terminated path is live through the statfs check.
    assert_eq!(unsafe { giggles_rocksdb_check_directory_pin(c_path.as_ptr()) }, libc::EOPNOTSUPP);
    assert_pin_refused(&dir.0);
}

#[cfg(not(any(target_os = "linux", windows)))]
#[test]
fn marked_store_on_unsupported_target_is_not_recoverable() {
    let dir = TestDir::new();
    drop(open(&dir.0));
    meta::write_commit_groups_marker(&dir.0).unwrap();
    assert_pin_refused(&dir.0);
}

#[test]
fn recovery_seed_ignores_bad_checksum_after_the_chain_stop() {
    let copy = TestDir::new();
    let wal = copy_with_interior_hole(&copy.0, true, false);
    let path = current_wal(&copy.0);
    let mut bytes = fs::read(&path).unwrap();
    let block = bytes.len().next_multiple_of(WAL_BLOCK);
    bytes.resize(block + WAL_BLOCK, 0);
    bytes[block + 4..block + 6].copy_from_slice(&((WAL_BLOCK - WAL_HEADER) as u16).to_le_bytes());
    bytes[block + 6] = 1; // Full record with an invalid checksum.
    fs::write(&path, bytes).unwrap();

    let options = RocksKv::open_options(CACHE_BYTES, DBRecoveryMode::TolerateCorruptedTailRecords).unwrap();
    let families = DB::list_cf(&options, native_path(&copy.0)).unwrap();
    assert!(RocksKv::open_read_only(&copy.0, &options, &families).is_err());

    let pin = DirectoryIdentity::read(&copy.0).unwrap().pin(&copy.0).unwrap();
    let db = RocksKv::open_recovery(pin, CACHE_BYTES).unwrap();
    assert_gap(&db, wal, 3, 4);
    assert_rows(&db, 1..=2, &[3, 4, 5]);
}

#[test]
fn recovery_seed_ignores_checksum_valid_malformed_count_after_the_chain_stop() {
    let copy = TestDir::new();
    let wal = copy_with_interior_hole(&copy.0, true, false);
    let path = current_wal(&copy.0);
    let mut bytes = fs::read(&path).unwrap();
    bytes.resize(bytes.len().next_multiple_of(WAL_BLOCK), 0);

    let mut batch = rocksdb::WriteBatch::default();
    batch.put(b"unaltered key", b"one operation");
    let mut payload = batch.data().to_vec();
    payload[..8].copy_from_slice(&100u64.to_le_bytes()); // Sequence number.
    payload[8..12].copy_from_slice(&2u32.to_le_bytes()); // Invalid count.
    let mut record = vec![1]; // Full physical record type, covered by the checksum.
    record.extend_from_slice(&payload);
    // RocksDB's log format uses masked CRC32C over the type byte and payload.
    let mut crc = !0u32;
    for byte in &record {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    let masked = (!crc).rotate_right(15).wrapping_add(0xa282_ead8);
    bytes.extend_from_slice(&masked.to_le_bytes());
    bytes.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(&record);
    fs::write(&path, bytes).unwrap();

    let options = RocksKv::open_options(CACHE_BYTES, DBRecoveryMode::SkipAnyCorruptedRecord).unwrap();
    let families = DB::list_cf(&options, native_path(&copy.0)).unwrap();
    let Err(error) = RocksKv::open_read_only(&copy.0, &options, &families) else {
        panic!("the unrestricted scan accepted a malformed count");
    };
    assert!(error.to_string().contains("wrong count"), "{error}");

    let db = open(&copy.0);
    assert_gap(&db, wal, 3, 4);
    assert_rows(&db, 1..=2, &[3, 4, 5]);
    assert_eq!(commit_group(&db), Some(2));
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn recovery_seed_skips_an_unopenable_wal_after_the_chain_stop() {
    let copy = TestDir::new();
    let wal = copy_with_interior_hole(&copy.0, true, false);
    let later_wal = copy.0.join(format!("{:06}.log", wal + 1000));
    #[cfg(target_os = "linux")]
    std::os::unix::fs::symlink("missing-wal-target", &later_wal).unwrap();
    #[cfg(windows)]
    fs::write(&later_wal, b"unreadable WAL").unwrap();
    #[cfg(windows)]
    let _deny_read = {
        use std::os::windows::fs::OpenOptionsExt;
        OpenOptions::new().read(true).share_mode(4).open(&later_wal).unwrap()
    };
    assert!(File::open(&later_wal).is_err());

    let options = RocksKv::open_options(CACHE_BYTES, DBRecoveryMode::SkipAnyCorruptedRecord).unwrap();
    let families = DB::list_cf(&options, native_path(&copy.0)).unwrap();
    let Err(error) = RocksKv::open_read_only(&copy.0, &options, &families) else {
        panic!("the unrestricted scan accepted an unopenable WAL");
    };
    assert!(error.to_string().contains(&format!("{:06}.log", wal + 1000)), "{error}");

    let db = open(&copy.0);
    assert_gap(&db, wal, 3, 4);
    assert_rows(&db, 1..=2, &[3, 4, 5]);
    assert_eq!(commit_group(&db), Some(2));
}

#[test]
fn validation_refusal_preserves_files_after_a_full_replay_buffer() {
    const WRITE_BUFFER: usize = 64 << 10;
    let dir = TestDir::new();
    initialize_marked(&dir.0);
    let db = open(&dir.0);
    group(&db, 1, vec![1; 2 * WRITE_BUFFER]);
    group(&db, 2, vec![2; 2 * WRITE_BUFFER]);
    small_group(&db, 2);
    db.sync_commits().unwrap();
    drop(db);
    let before = file_contents(&dir.0);
    assert!(wal_len(&current_wal(&dir.0)) > 4 * WRITE_BUFFER);

    let (path, lock) = RocksKv::lock_local(&dir.0).unwrap();
    let mut options = RocksKv::open_options(CACHE_BYTES, DBRecoveryMode::PointInTime).unwrap();
    options.set_write_buffer_size(WRITE_BUFFER);
    let Err(error) = RocksKv::open_with_options(&path, lock.writer_lock, lock.directory_pin, options, true) else {
        panic!("unsafe recovery opened successfully");
    };
    assert!(error.to_string().contains("expected 3, found 2"));
    assert_eq!(FenceCause::for_kv(FenceSite::Open, &error).class, FenceClass::Data);
    assert_eq!(file_contents(&dir.0), before);
}

#[cfg(windows)]
#[test]
fn unreadable_current_is_not_more_recoverable_in_the_seed() {
    use std::os::windows::fs::OpenOptionsExt;
    let dir = TestDir::new();
    initialize_marked(&dir.0);
    let before = file_contents(&dir.0);
    let current = OpenOptions::new().read(true).share_mode(0)
        .open(dir.0.join("CURRENT")).unwrap();
    assert!(File::open(dir.0.join("CURRENT")).is_err());
    let Err(seed_error) = RocksKv::open(&dir.0, CACHE_BYTES) else {
        panic!("seed accepted an unreadable CURRENT");
    };
    drop(current);
    assert_eq!(file_contents(&dir.0), before);

    let _current = OpenOptions::new().read(true).share_mode(0)
        .open(dir.0.join("CURRENT")).unwrap();
    let options = RocksKv::open_options(CACHE_BYTES, DBRecoveryMode::PointInTime).unwrap();
    let Err(native_error) = DB::open_cf_with_opts(
        &options,
        native_path(&dir.0),
        Keyspace::ALL.map(|ks| (ks.name(), options.clone())),
    ) else {
        panic!("native open accepted an unreadable CURRENT");
    };
    let native_error = map_error_at(native_error, FenceSite::Open);
    use crate::storage::v2::OpenFailure;
    assert_eq!(OpenFailure::from(seed_error).cause.class, FenceClass::NotRecoverable);
    assert_eq!(OpenFailure::from(native_error).cause.class, FenceClass::NotRecoverable);
}

#[test]
#[ignore = "measures repeated opens with a 250 MiB pending WAL"]
fn measure_unmarked_pending_wal_open() {
    const PAYLOAD_BYTES: usize = 250 << 20;
    const VALUE_BYTES: usize = 4 << 10;
    const ROWS_PER_BATCH: usize = 16;
    const SAMPLES: usize = 12;

    let source = TestDir::new();
    let (path, lock) = RocksKv::lock_local(&source.0).unwrap();
    WAL_ONLY_SHUTDOWN.set(true);
    let mut options = RocksKv::open_options(CACHE_BYTES, DBRecoveryMode::PointInTime).unwrap();
    WAL_ONLY_SHUTDOWN.set(false);
    options.set_write_buffer_size(512 << 20);
    let db = RocksKv::open_with_options(&path, lock.writer_lock, lock.directory_pin, options, false).unwrap();
    meta::write_marker(&source.0).unwrap();
    let mut value = vec![0; VALUE_BYTES];
    getrandom::fill(&mut value).unwrap();
    for first in (0..PAYLOAD_BYTES / VALUE_BYTES).step_by(ROWS_PER_BATCH) {
        let mut batch = WriteBatch::default();
        for row in first..first + ROWS_PER_BATCH {
            batch.put(Keyspace::Rows, (row as u64).to_be_bytes().to_vec(), value.clone());
        }
        db.write(batch, Durability::NoSync).unwrap();
    }
    db.flush_barrier().unwrap();
    assert!(!db.has_sst(Keyspace::Rows));
    drop(db);
    let wal_bytes = wal_len(&current_wal(&source.0));
    assert!(wal_bytes >= PAYLOAD_BYTES);
    assert!(!source.0.read_dir().unwrap().any(|entry| entry.unwrap().path().extension().is_some_and(|ext| ext == "sst")));
    eprintln!("wal_bytes={wal_bytes}");

    for round in 0..=SAMPLES {
        for baseline in [round % 2 == 0, round % 2 != 0] {
            let copy = TestDir::new();
            copy_store_files(&source.0, &copy.0);
            MEASURE_WITHOUT_REFUSE_FILTER.set(baseline);
            let start = Instant::now();
            let db = RocksKv::open(&copy.0, CACHE_BYTES).unwrap();
            let ms = start.elapsed().as_secs_f64() * 1000.0;
            MEASURE_WITHOUT_REFUSE_FILTER.set(false);
            assert_eq!(db.get(Keyspace::Rows, &0u64.to_be_bytes()).unwrap().as_deref(), Some(value.as_slice()));
            if round > 0 {
                let mode = if baseline { "baseline" } else { "refuse" };
                eprintln!("sample={round} mode={mode} ms={ms:.3}");
            }
        }
    }
}
