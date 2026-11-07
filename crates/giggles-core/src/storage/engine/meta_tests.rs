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
        1023
    );
    for (i, (bit, _)) in FEATURE_TABLE.iter().enumerate() {
        assert_eq!(*bit, 1u64 << i);
    }
    assert_eq!(
        FEATURE_TABLE[9].0,
        super::super::super::feature_envelope::ROLE_BOOTSTRAP
    );
    assert_eq!(
        FEATURE_TABLE[6..]
            .iter()
            .fold(0, |bits, (bit, _)| bits | bit),
        super::super::super::feature_envelope::NEW_BITS
    );
    let kv = base();
    assert_eq!(validate_meta(&kv).unwrap().features, 0);
    for i in 10..64 {
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
        (1 << 10, FormatError::Unsupported("feature bits".into())),
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
        let result = create_inner(&d.0, [3; 16], open, |at| {
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
