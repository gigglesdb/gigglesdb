use super::super::{kv_mem::MemKv, mvcc};
use super::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "meta-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn put(&self, name: &str, bytes: &[u8]) {
        fs::write(self.0.join(name), bytes).unwrap();
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn base() -> MemKv {
    let kv = MemKv::new();
    let mut b = WriteBatch::default();
    for (k, v) in [
        (b"family".as_slice(), 2u32.to_be_bytes().to_vec()),
        (b"subversion", 1u32.to_be_bytes().to_vec()),
        (b"domain", vec![3; 16]),
        (b"engine", b"rocksdb".to_vec()),
    ] {
        b.put(Keyspace::Meta, k.to_vec(), v);
    }
    put_features(&mut b, 0).unwrap();
    kv.write(b, Durability::Sync).unwrap();
    kv
}
fn put(kv: &MemKv, k: &[u8], v: &[u8]) {
    let mut b = WriteBatch::default();
    b.put(Keyspace::Meta, k.to_vec(), v.to_vec());
    kv.write(b, Durability::Sync).unwrap();
}
fn role(kv: &MemKv, id: u64, ts: u64, live: bool) {
    let mut key = vec![7];
    key.extend_from_slice(&id.to_be_bytes());
    key.extend_from_slice(&(!ts).to_be_bytes());
    let mut b = WriteBatch::default();
    b.put(
        Keyspace::Catalog,
        key,
        if live {
            mvcc::encode_live(b"role")
        } else {
            mvcc::encode_tombstone()
        },
    );
    kv.write(b, Durability::Sync).unwrap();
}
#[test]
fn directory_matrix() {
    let d = TestDir::new();
    assert_eq!(classify_dir(&d.0).unwrap(), DirClass::Empty);
    d.put("giggles.lock", b"stale writer identity");
    assert_eq!(classify_dir(&d.0).unwrap(), DirClass::Empty);
    d.put(TMP, b"partial");
    assert_eq!(classify_dir(&d.0).unwrap(), DirClass::Empty);
    d.put("data.redb", b"");
    assert_eq!(classify_dir(&d.0).unwrap(), DirClass::Legacy);
    fs::remove_file(d.0.join(TMP)).unwrap();
    assert_eq!(classify_dir(&d.0).unwrap(), DirClass::Legacy);
    d.put("legacy-original.redb", b"");
    assert_eq!(classify_dir(&d.0).unwrap(), DirClass::Legacy);
    fs::remove_file(d.0.join("legacy-original.redb")).unwrap();
    d.put(MARKER, V2);
    assert!(matches!(classify_dir(&d.0), Err(FormatError::Corrupt(_))));
    fs::remove_file(d.0.join("data.redb")).unwrap();
    assert!(matches!(classify_dir(&d.0), Err(FormatError::Corrupt(_))));
    d.put("CURRENT", b"");
    assert_eq!(classify_dir(&d.0).unwrap(), DirClass::V2);
    d.put("unrecognized", b"");
    assert_eq!(classify_dir(&d.0).unwrap(), DirClass::V2);
    fs::remove_file(d.0.join("unrecognized")).unwrap();
    for bytes in [
        b"giggles-v1\n".as_slice(),
        b"giggles-v3\n".as_slice(),
        b"giggles-v999999999999999999999999999999999999\n",
    ] {
        d.put(MARKER, bytes);
        assert!(matches!(
            classify_dir(&d.0),
            Err(FormatError::Unsupported(_))
        ));
    }
    for bytes in [
        b"giggles-v2".as_slice(),
        b"giggles-v0\n",
        b"giggles-v02\n",
        b"giggles-v03\n",
        b"giggles-v\n",
        &[b'x'; 65],
    ] {
        d.put(MARKER, bytes);
        assert!(matches!(classify_dir(&d.0), Err(FormatError::Corrupt(_))));
    }
    fs::remove_file(d.0.join(MARKER)).unwrap();
    assert!(matches!(
        classify_dir(&d.0),
        Err(FormatError::Incomplete(_))
    ));
    let orphan = TestDir::new();
    orphan.put("MANIFEST-000001", b"");
    assert!(matches!(
        classify_dir(&orphan.0),
        Err(FormatError::Incomplete(_))
    ));
    orphan.put("unrecognized", b"");
    assert!(matches!(
        classify_dir(&orphan.0),
        Err(FormatError::Incomplete(_))
    ));
    let unknown = TestDir::new();
    unknown.put("unrecognized", b"");
    assert!(matches!(
        classify_dir(&unknown.0),
        Err(FormatError::Corrupt(_))
    ));
    assert_eq!(
        classify_dir(&unknown.0.join("absent")).unwrap(),
        DirClass::Empty
    );
}
#[test]
fn real_legacy_directory_has_no_rocks_files() {
    let d = TestDir::new();
    let path = d.0.join("legacy");
    let storage = crate::storage::Admin::create(&path, crate::storage::Limits::default()).unwrap();
    drop(storage);
    let mut files: Vec<_> = fs::read_dir(&path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    files.sort();
    println!("legacy storage files: {files:?}");
    assert!(!files.iter().any(|name| rocks_file(name)));
    assert_eq!(classify_dir(&path).unwrap(), DirClass::Legacy);
}
#[test]
fn feature_positions_and_validation() {
    assert_eq!(
        FEATURE_TABLE.iter().fold(0, |bits, (bit, _)| bits | bit),
        0x1FFFF | KEY_DEPENDENCY_IDENTITY | RANGES | WIDE_ONCE_RECEIPTS | TASK_CATALOG | PROTECTED_RECORDS | WRITE_FENCE | USER_COLLATIONS | COLUMN_COLLATIONS | BUILTIN_EXTENSIONS | ROLE_MEMBERSHIP | WIDE_ROWS | UUID_ARRAY_COLUMNS | WIDE_CATALOG
    );
    assert_eq!(FEATURE_TABLE.len(), 30);
    assert_eq!(FEATURE_TABLE[28], (UUID_ARRAY_COLUMNS, "uuid_array_columns"));
    assert_eq!(UUID_ARRAY_COLUMNS, 1 << 32);
    assert!(check_features(UUID_ARRAY_COLUMNS).is_ok());
    assert!(matches!(
        check_features_allowed(UUID_ARRAY_COLUMNS, (1 << 29) - 1),
        Err(FormatError::Unsupported(_)),
    ));
    assert_eq!(FEATURE_TABLE[29], (WIDE_CATALOG, "wide_catalog"));
    assert_eq!(WIDE_CATALOG, 1 << 31);
    assert!(check_features_allowed(WIDE_CATALOG, u64::from(u32::MAX >> 1)).is_err());
    check_features(WIDE_CATALOG).unwrap();
    assert_eq!(ROLE_MEMBERSHIP, 1 << 27);
    assert_eq!(FEATURE_TABLE[26], (ROLE_MEMBERSHIP, "role_membership"));
    assert_eq!(FEATURE_TABLE[27], (WIDE_ROWS, "wide_rows"));
    assert_eq!(WIDE_ROWS, 1 << 28);
    assert!(check_features_allowed(WIDE_ROWS, (1 << 26) - 1).is_err());
    assert_eq!(FEATURE_TABLE[25],(BUILTIN_EXTENSIONS,"builtin_extensions"));
    assert_eq!(BUILTIN_EXTENSIONS,1 << 25);
    assert_eq!(FEATURE_TABLE[23],(USER_COLLATIONS,"user_collations"));
    assert_eq!(FEATURE_TABLE[24],(COLUMN_COLLATIONS,"column_collations"));
    assert_eq!(USER_COLLATIONS, 1 << 23);
    assert_eq!(COLUMN_COLLATIONS, 1 << 24);
    assert_eq!(FEATURE_TABLE[21], (PROTECTED_RECORDS, "protected_records"));
    assert_eq!(PROTECTED_RECORDS, 1 << 21);
    assert_eq!(FEATURE_TABLE[22], (WRITE_FENCE, "write_fence"));
    assert_eq!(WRITE_FENCE, 1 << 22);
    assert_eq!(TASK_CATALOG, 1 << 20);
    for (i, (bit, _)) in FEATURE_TABLE[..14].iter().enumerate() {
        assert_eq!(*bit, 1u64 << i);
    }
    assert_eq!(FEATURE_TABLE[14].0, SQL_VIEWS);
    assert_eq!(FEATURE_TABLE[16].0, REPLICATED);
    assert_eq!(FEATURE_TABLE[17].0, KEY_DEPENDENCY_IDENTITY);
    assert_eq!(FEATURE_TABLE[18], (RANGES, "ranges"));
    assert_eq!(FEATURE_TABLE[19], (1 << 19, "wide_once_receipts"));
    assert_eq!(FEATURE_TABLE[20], (TASK_CATALOG, "task_catalog"));
    assert_eq!(FEATURE_TABLE[10].0, TEXT_BACKED_COLUMN_TYPES);
    assert_eq!(
        FEATURE_TABLE[11].0,
        super::super::super::roles::engine::FEATURE
    );
    assert_eq!(FEATURE_TABLE[10].0 | FEATURE_TABLE[11].0, 0xC00);
    assert_eq!(
        FEATURE_TABLE[9].0,
        super::super::super::feature_envelope::ROLE_BOOTSTRAP
    );
    assert_eq!(
        FEATURE_TABLE[6..10]
            .iter()
            .fold(0, |bits, (bit, _)| bits | bit),
        super::super::super::feature_envelope::NEW_BITS
            & !(KEY_DEPENDENCY_IDENTITY
                | UUID_ARRAY_COLUMNS
                | super::super::super::feature_envelope::USER_COLLATIONS
                | super::super::super::feature_envelope::COLUMN_COLLATIONS
                | WIDE_CATALOG)
    );
    let kv = base();
    assert_eq!(validate_meta(&kv).unwrap().features, 0);
    put(&kv, b"features", &TASK_CATALOG.to_be_bytes());
    assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
    put(&kv, b"features", &(TASK_CATALOG | 2048).to_be_bytes());
    assert!(validate_meta(&kv).is_ok());
    put(&kv, b"features", &PROTECTED_RECORDS.to_be_bytes());
    assert_eq!(validate_meta(&kv).unwrap().features, PROTECTED_RECORDS);
    assert!(check_features(PROTECTED_RECORDS).is_ok());
    for i in (26..64).filter(|i| !matches!(*i, 27 | 28 | 31 | 32)) {
        put(&kv, b"features", &(1u64 << i).to_be_bytes());
        assert!(matches!(
            validate_meta(&kv),
            Err(FormatError::Unsupported(_))
        ));
    }
    put(&kv, b"features", &256u64.to_be_bytes());
    assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
    put(&kv, b"features", &320u64.to_be_bytes());
    assert!(validate_meta(&kv).is_ok());
}
#[test]
fn metadata_allowlist_and_lengths() {
    for key in [
        b"family".as_slice(),
        b"subversion",
        b"features",
        b"domain",
        b"engine",
    ] {
        let kv = base();
        let mut b = WriteBatch::default();
        b.delete(Keyspace::Meta, key.to_vec());
        kv.write(b, Durability::Sync).unwrap();
        assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
        let kv = base();
        put(&kv, key, b"x");
        assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
    }
    for (k, v) in [
        (b"family".as_slice(), 3u32),
        (b"subversion", CURRENT_SUBVERSION + 1),
    ] {
        let kv = base();
        put(&kv, k, &v.to_be_bytes());
        assert!(matches!(
            validate_meta(&kv),
            Err(FormatError::Unsupported(_))
        ));
    }
    let kv = base();
    put(&kv, b"future", b"");
    assert!(matches!(
        validate_meta(&kv),
        Err(FormatError::Unsupported(_))
    ));
    let kv = base();
    for key in [b"last_commit_ts".as_slice(), b"gc_watermark"] {
        put(&kv, key, &42u64.to_be_bytes());
    }
    let info = validate_meta(&kv).unwrap();
    assert_eq!(info.last_commit_ts, Some(42));
    assert_eq!(info.gc_watermark, Some(42));
    for key in [b"last_commit_ts".as_slice(), b"gc_watermark"] {
        let kv = base();
        put(&kv, key, b"x");
        assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
    }
    let kv = base();
    put(&kv, b"engine", b"notrock");
    assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
    let kv = base();
    put(&kv, b"subversion", &0u32.to_be_bytes());
    assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
    for subversion in 1..=CURRENT_SUBVERSION {
        put(&kv, b"subversion", &subversion.to_be_bytes());
        assert_eq!(validate_meta(&kv).unwrap().subversion, subversion);
    }
}
#[test]
fn latest_role_count() {
    let kv = base();
    put(&kv, b"features", &512u64.to_be_bytes());
    assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
    role(&kv, 1, 1, true);
    assert!(validate_meta(&kv).is_ok());
    role(&kv, 1, 2, true);
    assert!(validate_meta(&kv).is_ok());
    role(&kv, 2, 1, true);
    assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
    role(&kv, 2, 2, false);
    assert!(validate_meta(&kv).is_ok());
    put(&kv, b"features", &0u64.to_be_bytes());
    assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
    role(&kv, 1, 3, false);
    assert!(validate_meta(&kv).is_ok());
    role(&kv, 1, 4, true);
    assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
}
#[test]
fn features_share_caller_atomic_batch() {
    for fail in [false, true] {
        let kv = base();
        let mut b = WriteBatch::default();
        b.put(Keyspace::Rows, b"content".to_vec(), b"value".to_vec());
        put_features(&mut b, 1).unwrap();
        assert_eq!(b.len(), 2);
        if fail {
            kv.fail_next_write(KvError::Io("injected".into()));
        }
        assert_eq!(kv.write(b, Durability::Sync).is_err(), fail);
        let reopened = kv.crash();
        assert_eq!(
            reopened.get(Keyspace::Rows, b"content").unwrap().is_some(),
            !fail
        );
        assert_eq!(validate_meta(&reopened).unwrap().features, u64::from(!fail));
    }
    let mut b = WriteBatch::default();
    put_features(&mut b, 5).unwrap();
    assert_eq!(
        b.into_ops(),
        vec![super::super::kv::Op::Put {
            ks: Keyspace::Meta,
            key: b"features".to_vec(),
            value: 5u64.to_be_bytes().to_vec()
        }]
    );
}
#[test]
fn invalid_features_leave_batch_unchanged() {
    for (bits, expected) in [
        (1 << 26, FormatError::Unsupported("feature bits".into())),
        (
            (1 << 26) | 0xC00,
            FormatError::Unsupported("feature bits".into()),
        ),
        (256, corrupt("sequence counters require catalog_ext")),
    ] {
        let mut empty = WriteBatch::default();
        assert_eq!(put_features(&mut empty, bits).unwrap_err(), expected);
        assert!(empty.is_empty());

        let mut populated = WriteBatch::default();
        populated.put(Keyspace::Rows, b"content".to_vec(), b"value".to_vec());
        put_features(&mut populated, 5).unwrap();
        assert_eq!(put_features(&mut populated, bits).unwrap_err(), expected);
        assert_eq!(
            populated.into_ops(),
            vec![
                super::super::kv::Op::Put {
                    ks: Keyspace::Rows,
                    key: b"content".to_vec(),
                    value: b"value".to_vec(),
                },
                super::super::kv::Op::Put {
                    ks: Keyspace::Meta,
                    key: b"features".to_vec(),
                    value: 5u64.to_be_bytes().to_vec(),
                },
            ]
        );
    }
}
#[cfg(feature = "engine-rocksdb")]
fn open(path: &Path) -> Result<Arc<dyn KvBackend>, KvError> {
    Ok(Arc::new(super::super::kv_rocksdb::RocksKv::open(
        path,
        1024 * 1024,
    )?))
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn rocks_creation_crash_witnesses_and_reopen() {
    for stage in [
        Stage::Open,
        Stage::MetaBatch,
        Stage::TmpWrite,
        Stage::Rename,
    ] {
        let d = TestDir::new();
        let result = create_inner(&d.0, [3; 16], 0, open, |at| {
            if at == stage {
                Err(FormatError::Incomplete("crash hook".into()))
            } else {
                Ok(())
            }
        });
        assert!(result.is_err());
        if stage == Stage::Rename {
            assert_eq!(classify_dir(&d.0).unwrap(), DirClass::V2);
            let kv = open(&d.0).unwrap();
            assert!(validate_meta(kv.as_ref()).is_ok());
        } else {
            assert!(matches!(
                classify_dir(&d.0),
                Err(FormatError::Incomplete(_))
            ));
            assert!(create(&d.0, [3; 16], open).is_err());
        }
    }
    let d = TestDir::new();
    let kv = create(&d.0, [3; 16], open).unwrap();
    assert!(validate_meta(kv.as_ref()).is_ok());
    drop(kv);
    assert_eq!(classify_dir(&d.0).unwrap(), DirClass::V2);
    let kv = open(&d.0).unwrap();
    assert_eq!(validate_meta(kv.as_ref()).unwrap().domain, [3; 16]);
    drop(kv);
    assert!(create(&d.0, [3; 16], open).is_err());
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn rocks_features_and_content_reopen_together() {
    for apply in [false, true] {
        let d = TestDir::new();
        let kv = create(&d.0, [3; 16], open).unwrap();
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Rows, b"content".to_vec(), b"value".to_vec());
        put_features(&mut batch, 1).unwrap();
        if apply {
            kv.write(batch, Durability::Sync).unwrap();
        }
        drop(kv);
        let kv = open(&d.0).unwrap();
        assert_eq!(kv.get(Keyspace::Rows, b"content").unwrap().is_some(), apply);
        assert_eq!(
            validate_meta(kv.as_ref()).unwrap().features,
            u64::from(apply)
        );
    }
}

#[test]
fn old_binary_rejects_each_new_feature_and_their_combination() {
    let kv = base();
    let columns = TEXT_BACKED_COLUMN_TYPES;
    let roles = super::super::super::roles::engine::FEATURE;
    let allowed = FEATURE_TABLE.iter().fold(0, |bits, (bit, _)| bits | bit);
    for features in [columns, roles, columns | roles] {
        put(&kv, b"features", &features.to_be_bytes());
        let bits = validate_meta(&kv).unwrap().features;
        assert_eq!(bits, features);
        check_features_allowed(bits, allowed).unwrap();
        for omitted in [columns, roles, columns | roles] {
            assert_eq!(
                check_features_allowed(bits, allowed & !omitted),
                if bits & omitted == 0 {
                    Ok(())
                } else {
                    Err(FormatError::Unsupported("feature bits".into()))
                },
            );
        }
    }
}

#[test]
fn old_binary_rejects_wide_once_receipts_bit_and_bit_requires_native_outcomes() {
    let kv = base();
    let bits = NATIVE_OUTCOMES | WIDE_ONCE_RECEIPTS;
    put(&kv, b"features", &bits.to_be_bytes());
    assert_eq!(validate_meta(&kv).unwrap().features, bits);
    let allowed = FEATURE_TABLE.iter().fold(0, |bits, (bit, _)| bits | bit);
    check_features(bits).unwrap();
    check_features_allowed(bits, allowed).unwrap();
    // The binary before bit 19 refuses the database rather than misreading receipts.
    assert_eq!(
        check_features_allowed(bits, allowed & !WIDE_ONCE_RECEIPTS),
        Err(FormatError::Unsupported("feature bits".into()))
    );
    assert!(matches!(check_features(WIDE_ONCE_RECEIPTS), Err(FormatError::Corrupt(_))));
}

#[test]
fn old_binary_guard_rejects_schema_type_bit_before_catalog_decode() {
    let kv = base();
    put(&kv, b"features", &SCHEMAS_AND_USER_TYPES.to_be_bytes());
    let bits = validate_meta(&kv).unwrap().features;
    assert_eq!(bits, 0x1000);
    assert_eq!(
        check_features_allowed(bits, 0x0FFF),
        Err(FormatError::Unsupported("feature bits".into()))
    );
    check_features_allowed(bits, 0x1FFF).unwrap();
}

#[test]
fn raft_applied_requires_replicated_feature_and_exact_length() {
    let kv = MemKv::new();
    write_initial_meta(&kv, [3; 16], REPLICATED).unwrap();
    put(&kv, b"raft_applied", &[0; 16]);
    assert_eq!(validate_meta(&kv).unwrap().features, REPLICATED);
    put(&kv, b"features", &0u64.to_be_bytes());
    assert_eq!(
        validate_meta(&kv).unwrap_err(),
        FormatError::Unsupported("raft_applied without replicated feature".into())
    );
    put(&kv, b"features", &REPLICATED.to_be_bytes());
    for len in [15, 17] {
        put(&kv, b"raft_applied", &vec![0; len]);
        assert_eq!(
            validate_meta(&kv).unwrap_err(),
            corrupt("wrong raft_applied length")
        );
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn replicated_creation_is_v2() {
    let d = TestDir::new();
    let kv = create_with_features(&d.0, [3; 16], REPLICATED, open).unwrap();
    assert_eq!(validate_meta(kv.as_ref()).unwrap().features, REPLICATED);
    drop(kv);
    assert_eq!(classify_dir(&d.0).unwrap(), DirClass::V2);
}

#[test]
fn key_dependency_feature_requires_catalog_ext_and_is_monotone() {
    assert!(check_features(64 | KEY_DEPENDENCY_IDENTITY).is_ok());
    assert!(matches!(check_features(KEY_DEPENDENCY_IDENTITY), Err(FormatError::Corrupt(_))));
    for unknown in [1 << 26, 1 << 40] {
        assert!(matches!(check_features(unknown | KEY_DEPENDENCY_IDENTITY), Err(FormatError::Unsupported(_))));
    }
    let kv = base();
    put(&kv, b"features", &(64 | KEY_DEPENDENCY_IDENTITY).to_be_bytes());
    assert_eq!(open_checks(&kv).unwrap().features, 64 | KEY_DEPENDENCY_IDENTITY);
}

#[test]
fn ranges_feature_requires_replicated() {
    assert_eq!(check_features(RANGES), Err(corrupt("ranges requires replicated")));
    assert_eq!(check_features(RANGES | 64), Err(corrupt("ranges requires replicated")));
    check_features(RANGES | REPLICATED).unwrap();
    let kv = base();
    put(&kv, b"features", &RANGES.to_be_bytes());
    assert_eq!(validate_meta(&kv).unwrap_err(), corrupt("ranges requires replicated"));
    put(&kv, b"features", &(RANGES | REPLICATED).to_be_bytes());
    assert_eq!(validate_meta(&kv).unwrap().features, RANGES | REPLICATED);
}

use crate::storage::range_api::{Epoch, RangeDescriptor, RangeId, SYSTEM_RANGE};
const RANGE_SUBS: [RangeLocalKey; 10] = [
    RangeLocalKey::Applied,
    RangeLocalKey::LastCommitTs,
    RangeLocalKey::Descriptor,
    RangeLocalKey::Features,
    RangeLocalKey::OracleHw,
    RangeLocalKey::Install,
    RangeLocalKey::Prepared(0x0102_0304_0506_0708),
    RangeLocalKey::Home(0),
    RangeLocalKey::Marker(u64::MAX),
    RangeLocalKey::Child(5),
];
fn raw_range_key(id: u64, sub: &[u8]) -> Vec<u8> {
    let mut key = b"range/".to_vec();
    key.extend_from_slice(&id.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(sub);
    key
}
fn descriptor(id: u64) -> Vec<u8> {
    RangeDescriptor {
        id: RangeId(id),
        epoch: Epoch(1),
        start: Vec::new(),
        end: None,
        replicas: Vec::new(),
    }
    .encode()
    .unwrap()
}

#[test]
fn range_local_keys_round_trip() {
    let mut prepared = b"prepared/".to_vec();
    prepared.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
    assert_eq!(
        range_local_key(RangeId(2), &RANGE_SUBS[6]),
        raw_range_key(2, &prepared)
    );
    assert_eq!(range_local_prefix(RangeId(2)), raw_range_key(2, b""));
    let mut child = b"child/".to_vec();
    child.extend_from_slice(&5u64.to_be_bytes());
    assert_eq!(
        range_local_key(RangeId(2), &RANGE_SUBS[9]),
        raw_range_key(2, &child)
    );
    assert_eq!(
        range_local_key(RangeId(2), &RangeLocalKey::LastCommitTs),
        raw_range_key(2, b"last_commit_ts")
    );
    for id in [RangeId(1), RangeId(2), RangeId(u64::MAX)] {
        for sub in RANGE_SUBS {
            let key = range_local_key(id, &sub);
            assert!(key.starts_with(&range_local_prefix(id)));
            assert_eq!(parse_range_local_key(&key), Some((id, sub)));
        }
    }
}

#[test]
fn malformed_range_local_keys_do_not_parse() {
    let mut applied_trailing = raw_range_key(2, b"applied");
    applied_trailing.push(0);
    for key in [
        raw_range_key(0, b"applied"),
        b"range/\0\0\0\x01".to_vec(),
        b"range/\0\0\0\0\0\0\0\x01applied".to_vec(),
        b"range/".to_vec(),
        raw_range_key(2, b""),
        raw_range_key(2, b"future"),
        raw_range_key(2, b"Applied"),
        applied_trailing,
        raw_range_key(2, b"prepared/\0\0\0\0\0\0\x01"),
        raw_range_key(2, b"prepared/\0\0\0\0\0\0\0\x01\0"),
        raw_range_key(2, b"home/"),
        raw_range_key(2, b"marker"),
        raw_range_key(2, b"child/\0\0\0\0\0\0\x05"),
        raw_range_key(2, b"child"),
        b"ranges/\0\0\0\0\0\0\0\x01/applied".to_vec(),
    ] {
        assert_eq!(parse_range_local_key(&key), None, "{key:?}");
    }
}

fn ranged() -> MemKv {
    let kv = base();
    put(&kv, b"features", &(RANGES | REPLICATED).to_be_bytes());
    put(&kv, &raw_range_key(1, b"applied"), &[0; 16]);
    put(&kv, &raw_range_key(1, b"last_commit_ts"), &7u64.to_be_bytes());
    put(&kv, &raw_range_key(1, b"descriptor"), &descriptor(1));
    put(&kv, &raw_range_key(1, b"features"), &REPLICATED.to_be_bytes());
    put(&kv, &raw_range_key(1, b"oracle_hw"), &9u64.to_be_bytes());
    put(&kv, &raw_range_key(1, b"gc_watermark"), &3u64.to_be_bytes());
    put(&kv, &raw_range_key(1, b"install"), b"opaque");
    for sub in &RANGE_SUBS[6..] {
        let value: &[u8] = match sub {
            RangeLocalKey::Prepared(_) => &[
                1, // v1; home and timestamp, zero writes and locks
                0, 0, 0, 0, 0, 0, 0, 2,
                0, 0, 0, 0, 0, 0, 0, 1,
                0, 0, 0, 0, 0, 0, 0, 0,
            ],
            RangeLocalKey::Marker(_) => &[1; 8],
            RangeLocalKey::Child(child) => &descriptor(*child),
            _ => b"any",
        };
        put(&kv, &range_local_key(RangeId(2), sub), value);
    }
    put(&kv, &raw_range_key(2, b"descriptor"), &descriptor(2));
    put(&kv, &raw_range_key(2, b"features"), &(RANGES | REPLICATED).to_be_bytes());
    kv
}

#[test]
fn validate_meta_checks_range_local_values() {
    assert_eq!(validate_meta(&ranged()).unwrap().features, RANGES | REPLICATED);
    let len = || corrupt("wrong range metadata length");
    let unknown = || FormatError::Unsupported("unknown metadata key".into());
    let marker = range_local_key(RangeId(2), &RangeLocalKey::Marker(3));
    let child = range_local_key(RangeId(2), &RangeLocalKey::Child(5));
    let cases: Vec<(Vec<u8>, Vec<u8>, FormatError)> = vec![
        (raw_range_key(1, b"applied"), vec![0; 15], corrupt("wrong range applied length")),
        (raw_range_key(1, b"applied"), vec![0; 17], corrupt("wrong range applied length")),
        (raw_range_key(1, b"last_commit_ts"), vec![0; 7], len()),
        (raw_range_key(2, b"features"), vec![0; 9], len()),
        (raw_range_key(1, b"oracle_hw"), vec![0; 7], len()),
        (marker.clone(), vec![0; 7], len()),
        (marker, vec![0; 9], len()),
        (
            raw_range_key(2, b"oracle_hw"),
            9u64.to_be_bytes().to_vec(),
            corrupt("oracle_hw outside the system range"),
        ),
        (
            raw_range_key(2, b"descriptor"),
            descriptor(3),
            corrupt("range descriptor id mismatch"),
        ),
        (
            raw_range_key(2, b"features"),
            (REPLICATED | 64).to_be_bytes().to_vec(),
            corrupt("range features exceed node features"),
        ),
        (
            raw_range_key(2, b"features"),
            RANGES.to_be_bytes().to_vec(),
            corrupt("ranges requires replicated"),
        ),
        (
            raw_range_key(2, b"features"),
            (1u64 << 26).to_be_bytes().to_vec(),
            FormatError::Unsupported("feature bits".into()),
        ),
        (child.clone(), descriptor(6), corrupt("range child id mismatch")),
        (
            range_local_key(RangeId(2), &RangeLocalKey::Child(2)),
            descriptor(2),
            corrupt("range child id mismatch"),
        ),
        (
            range_local_key(RangeId(2), &RangeLocalKey::Child(0)),
            descriptor(0),
            corrupt("range child id mismatch"),
        ),
        (raw_range_key(2, b"future"), Vec::new(), unknown()),
        (raw_range_key(0, b"applied"), vec![0; 16], unknown()),
    ];
    for (key, value, expected) in cases {
        let kv = ranged();
        put(&kv, &key, &value);
        assert_eq!(validate_meta(&kv).unwrap_err(), expected, "{key:?}");
    }
    for bad in [Vec::new(), vec![2], descriptor(1)[..20].to_vec()] {
        let kv = ranged();
        put(&kv, &raw_range_key(1, b"descriptor"), &bad);
        assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
        let kv = ranged();
        put(&kv, &child, &bad);
        assert!(matches!(validate_meta(&kv), Err(FormatError::Corrupt(_))));
    }
    assert_eq!(SYSTEM_RANGE, RangeId(1));
}

#[test]
fn range_keys_are_unknown_without_ranges_feature() {
    let key = range_local_key(RangeId(1), &RangeLocalKey::Applied);
    let unknown = FormatError::Unsupported("unknown metadata key".into());
    for features in [Some(0), Some(REPLICATED), Some(64 | KEY_DEPENDENCY_IDENTITY), None] {
        let kv = base();
        match features {
            Some(bits) => put(&kv, b"features", &bits.to_be_bytes()),
            None => {
                let mut b = WriteBatch::default();
                b.delete(Keyspace::Meta, b"features".to_vec());
                kv.write(b, Durability::Sync).unwrap();
            }
        }
        assert!(features.is_none() || validate_meta(&kv).is_ok());
        put(&kv, &key, &[0; 16]);
        assert_eq!(validate_meta(&kv).unwrap_err(), unknown);
    }
    let kv = base();
    put(&kv, b"features", &[0; 7]);
    put(&kv, &key, &[0; 16]);
    assert_eq!(validate_meta(&kv).unwrap_err(), unknown);
}

#[test]
fn write_fence_bit_key_agreement_and_decode_at_open() {
    use super::super::write_fence::{FenceState, WriteFenceRecord, holder_digest};
    let kv = base();
    assert!(open_checks(&kv).unwrap().write_fence.is_none());
    put(&kv, b"features", &WRITE_FENCE.to_be_bytes());
    assert!(matches!(open_checks(&kv), Err(FormatError::Corrupt(_))));
    let record = WriteFenceRecord {
        state: FenceState::Fenced,
        epoch: 1,
        holder_digest: holder_digest(b"holder"),
        changed_at: 42,
        label: "test".into(),
    };
    put(&kv, b"write_fence", &record.encode());
    assert_eq!(open_checks(&kv).unwrap().write_fence, Some(record));
    put(&kv, b"features", &0u64.to_be_bytes());
    assert!(matches!(open_checks(&kv), Err(FormatError::Corrupt(_))));
    put(&kv, b"features", &WRITE_FENCE.to_be_bytes());
    put(&kv, b"write_fence", &[1]);
    assert!(matches!(open_checks(&kv), Err(FormatError::Corrupt(_))));
}

#[test]
fn write_fence_old_binary_refuses_feature() {
    let allowed = FEATURE_TABLE.iter().fold(0, |bits, (bit, _)| bits | bit);
    assert_eq!(
        check_features_allowed(WRITE_FENCE, allowed & !WRITE_FENCE),
        Err(FormatError::Unsupported("feature bits".into()))
    );
}
