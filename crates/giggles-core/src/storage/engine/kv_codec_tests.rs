use super::{Keyspace, KvError, WriteBatch};

fn corrupt(bytes: &[u8]) {
    assert!(matches!(
        WriteBatch::decode(bytes),
        Err(KvError::Corrupt(_))
    ));
}

fn checksum(payload: &[u8]) -> Vec<u8> {
    let mut bytes = payload.to_vec();
    bytes.extend_from_slice(&super::super::crc32c::crc32c(payload).to_be_bytes());
    bytes
}

fn sample() -> WriteBatch {
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Meta, b"k".to_vec(), b"v".to_vec());
    batch
}

#[test]
fn empty_and_all_operations_round_trip() {
    let empty = WriteBatch::default();
    assert_eq!(WriteBatch::decode(&empty.encode()).unwrap(), empty);
    let mut batch = WriteBatch::default();
    for ks in Keyspace::ALL {
        batch.put(ks, vec![], vec![]);
        batch.put(ks, vec![0, 255], vec![255, 0, 42]);
        batch.delete(ks, vec![]);
        batch.delete(ks, b"key".to_vec());
        batch.delete_range(ks, vec![], vec![]);
        batch.delete_range(ks, b"a".to_vec(), b"z".to_vec());
    }
    let decoded = WriteBatch::decode(&batch.encode()).unwrap();
    assert_eq!(decoded, batch);
    assert_eq!(decoded.ops(), batch.clone().into_ops());
}

#[test]
fn deterministic_big_endian_wire_layout() {
    let bytes = sample().encode();
    // v1, one Put in Meta, one-byte key and value, followed by CRC32C.
    let payload = [1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, b'k', 0, 0, 0, 1, b'v'];
    assert_eq!(bytes, checksum(&payload));
    assert_eq!(bytes, sample().encode());
    assert_eq!(bytes, WriteBatch::decode(&bytes).unwrap().encode());
    for (index, ks) in Keyspace::ALL.into_iter().enumerate() {
        let mut batch = WriteBatch::default();
        batch.delete(ks, vec![]);
        batch.delete_range(ks, vec![], vec![]);
        assert_eq!(
            batch.encode(),
            checksum(&[
                1,
                0,
                0,
                0,
                2,
                2,
                index as u8,
                0,
                0,
                0,
                0,
                3,
                index as u8,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ])
        );
    }
}

#[test]
fn rejects_every_truncation_and_single_byte_flip() {
    let bytes = sample().encode();
    for len in 0..bytes.len() {
        corrupt(&bytes[..len]);
    }
    for index in 0..bytes.len() {
        for mask in 1..=u8::MAX {
            let mut flipped = bytes.clone();
            flipped[index] ^= mask;
            corrupt(&flipped);
        }
    }
    let mut trailing = bytes;
    trailing.push(0);
    corrupt(&trailing);
}

#[test]
fn rejects_unknown_versions_even_with_valid_checksum() {
    for version in [0, 2, 255] {
        let bytes = checksum(&[version, 0, 0, 0, 0]);
        let Err(KvError::Corrupt(message)) = WriteBatch::decode(&bytes) else {
            panic!("expected corrupt version");
        };
        assert!(message.contains("unknown batch version"));
    }
}

#[test]
fn rejects_malformed_payloads_with_valid_checksums() {
    // Resealing isolates structural validation from checksum rejection.
    let bytes = sample().encode();
    let payload = &bytes[..bytes.len() - 4];
    for (index, value) in [(5, 0), (5, 4), (5, 255), (6, 7), (6, 255)] {
        let mut invalid = payload.to_vec();
        invalid[index] = value;
        corrupt(&checksum(&invalid));
    }
    for count in [0u32, 2, u32::MAX] {
        let mut invalid = payload.to_vec();
        invalid[1..5].copy_from_slice(&count.to_be_bytes());
        corrupt(&checksum(&invalid));
    }
    for index in [7, 12] {
        let mut invalid = payload.to_vec();
        invalid[index..index + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        corrupt(&checksum(&invalid));
    }
    // Truncate each payload position and replace CRC to exercise the parser.
    for len in 1..payload.len() {
        corrupt(&checksum(&payload[..len]));
    }
    let mut trailing = payload.to_vec();
    trailing.push(0);
    corrupt(&checksum(&trailing));
}

#[test]
fn fence_classification_and_precedence() {
    use super::{FenceCause, FenceClass as Class, FenceSite as Site, FenceState, RocksStatus};

    for (site, code, subcode, expected) in [
        (Site::WalWrite, 5, 0, Class::Recoverable),
        (Site::WalWrite, 5, 4, Class::Recoverable), // NoSpace
        (Site::WalFlush, 5, 0, Class::Recoverable),
        (Site::BackgroundError, 5, 4, Class::NotRecoverable),
        (Site::Write, 5, 0, Class::NotRecoverable),
        (Site::FlushBarrier, 5, 0, Class::NotRecoverable),
        (Site::Write, 4, 0, Class::NotRecoverable),
        (Site::Read, 5, 0, Class::Recoverable),
        (Site::Open, 5, 0, Class::Recoverable),
    ] {
        let status = RocksStatus { code, subcode, severity: None };
        let error = KvError::RocksIo { message: "native error".into(), status, site };
        let cause = FenceCause::for_kv(site, &error);
        assert_eq!(cause.status, Some(status));
        assert_eq!(cause.class, expected, "{site:?} {code} {subcode}");
        assert_eq!(error.to_string(), "KV I/O: native error");
    }

    for (site, error, expected) in [
        (Site::CatalogReload, crate::CoreError::Storage("catalog read".into()), Class::NotRecoverable),
        (Site::CatalogReload, crate::CoreError::Corrupt("catalog decode".into()), Class::Data),
        (Site::NativeOnceLedger, crate::CoreError::Storage("ledger read".into()), Class::Data),
        (Site::Read, crate::CoreError::Storage("legacy read".into()), Class::NotRecoverable),
        (Site::LegacyPersist, crate::CoreError::Storage("legacy persist".into()), Class::NotRecoverable),
        (Site::Open, crate::CoreError::Storage("open error".into()), Class::NotRecoverable),
    ] {
        assert_eq!(FenceCause::for_core(site, &error).class, expected);
    }
    let disconnected = KvError::Io("RocksDB snapshot worker disconnected".into());
    for site in [Site::Read, Site::CatalogReload, Site::Open] {
        assert_eq!(
            FenceCause::for_kv(site, &disconnected).class,
            Class::NotRecoverable
        );
    }
    assert_eq!(
        FenceCause::for_kv(Site::NativeOnceLedger, &disconnected).class,
        Class::Data
    );

    let state = FenceState::default();
    let first = FenceCause::new(Site::WalWrite, Class::Recoverable, &"first");
    state.record(first.clone());
    let later = FenceCause::new(Site::WalFlush, Class::Recoverable, &"later");
    state.record(later.clone());
    assert_eq!(state.cause(), Some(first.clone()));
    let bad = FenceCause::for_kv(Site::Read, &KvError::Corrupt("bad bytes".into()));
    state.record(bad.clone());
    state.record(later);
    assert_eq!(state.cause(), Some(bad.clone()));
    let newer = FenceCause {
        generation: 1,
        ..first
    };
    state.record(newer.clone());
    state.record(bad);
    state.record_if_empty(FenceCause::new(
        Site::Other,
        Class::NotRecoverable,
        &"fallback",
    ));
    assert_eq!(state.cause(), Some(newer));
}

#[test]
fn failed_write_fences_and_preserves_cause() {
    use super::{Durability, FenceClass, FenceSite, Kv, RocksStatus};
    use super::super::kv_mem::MemKv;
    use std::sync::Arc;
    let backend = Arc::new(MemKv::new());
    let kv = Kv::new(backend.clone());
    let error = KvError::RocksIo {
        message: "injected first write error".into(),
        status: RocksStatus {
            code: RocksStatus::IO_ERROR,
            subcode: 0,
            severity: None,
        },
        site: FenceSite::Read,
    };
    backend.fail_next_write(error.clone());
    assert_eq!(kv.write(sample(), Durability::Sync), Err(error));
    let cause = kv.causes.cause().unwrap();
    assert!(cause.error_text.contains("injected first write error"));
    assert_eq!(cause.class, FenceClass::NotRecoverable);
    assert_eq!(cause.site, FenceSite::Write);
    assert_eq!(kv.flush_barrier(), Err(KvError::Fenced));
    assert_eq!(kv.get(Keyspace::Meta, b"k"), Err(KvError::Fenced));
}

#[test]
fn corrupt_read_reports_data_without_fencing() {
    use super::{Direction, FenceClass, FenceSite, Kv, KvBackend, KvIter, KvRead, KvPair, Durability};
    use std::{ops::Bound, sync::Arc};

    struct CorruptReader;
    impl KvRead for CorruptReader {
        fn get(&self, _: Keyspace, _: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
            Err(KvError::Corrupt("bad block".into()))
        }
        fn range(&self, _: Keyspace, _: Bound<&[u8]>, _: Bound<&[u8]>, _: Direction)
            -> Result<KvIter<'_>, KvError>
        {
            Ok(KvIter::new(std::iter::once(Err::<KvPair, _>(KvError::Corrupt("bad scan".into())))))
        }
    }
    impl KvBackend for CorruptReader {
        fn write(&self, _: WriteBatch, _: Durability) -> Result<(), KvError> { Ok(()) }
        fn flush_barrier(&self) -> Result<(), KvError> { Ok(()) }
        fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> { Ok(Arc::new(Self)) }
        fn engine_name(&self) -> &'static str { "corrupt-test" }
    }

    let live = Kv::new(Arc::new(CorruptReader));
    assert!(matches!(live.get(Keyspace::Meta, b"key"), Err(KvError::Corrupt(_))));
    let cause = live.causes.cause().unwrap();
    assert_eq!((cause.site, cause.class), (FenceSite::Read, FenceClass::Data));
    assert!(!live.is_fenced());
    live.flush_barrier().unwrap();

    let kv = Kv::new(Arc::new(CorruptReader));
    let snapshot = kv.snapshot().unwrap();
    let mut scan = snapshot.range(Keyspace::Meta, Bound::Unbounded, Bound::Unbounded, Direction::Forward).unwrap();
    assert!(kv.causes.cause().is_none());
    assert!(matches!(scan.next(), Some(Err(KvError::Corrupt(_)))));
    assert_eq!(kv.causes.cause().unwrap().class, FenceClass::Data);
    assert!(!kv.is_fenced());
}
