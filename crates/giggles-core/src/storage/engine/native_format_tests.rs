use super::super::{
    kv::{KvBackend, KvError},
    meta, mvcc,
};
use super::guard_tests::{fixture, request};
use super::*;
use crate::{
    operations::once::{OnceDescription, OnceSchema},
    storage,
};

fn ledger(result: Option<&[u8]>) -> Vec<u8> {
    let mut out = 1u16.to_be_bytes().to_vec();
    for n in [110u64, 1] {
        out.extend(n.to_be_bytes());
    }
    out.extend(99u128.to_be_bytes()); // A historical domain must remain legal.
    out.extend(1u64.to_be_bytes());
    out.push(42);
    out.extend([1; 32]);
    out.extend([2; 32]);
    out.extend(120u64.to_be_bytes());
    out.push(u8::from(result.is_some()));
    if let Some(bytes) = result {
        out.extend((bytes.len() as u64).to_be_bytes());
        out.extend(bytes);
    }
    for n in [10u64, 100, 110] {
        out.extend(n.to_be_bytes());
    }
    out
}
fn native() -> CommitRequest {
    let mut req = request(meta::NATIVE_OUTCOMES);
    req.minimum_subversion = meta::NATIVE_SUBVERSION;
    req.writes.insert(
        (Keyspace::Kernel, vec![0]),
        Some(
            storage::encode(&storage::Metadata {
                format: 4,
                domain: 0,
                schema: 0,
                last_time: 0,
                next_approval: 0,
                origin: Some(77),
            })
            .unwrap(),
        ),
    );
    req.writes
        .insert((Keyspace::Kernel, vec![7]), Some(ledger(None)));
    req.writes
        .insert((Keyspace::Rows, vec![0; 8]), Some(vec![42]));
    req
}
#[test]
fn reader_matrix_and_receipt_free_creation() {
    let (mem, _) = fixture();
    for version in [1u32, 2, 3] {
        let mut batch = WriteBatch::default();
        batch.put(
            Keyspace::Meta,
            b"subversion".to_vec(),
            version.to_be_bytes().to_vec(),
        );
        mem.write(batch, Durability::Sync).unwrap();
        assert_eq!(meta::validate_meta(mem.as_ref()).is_ok(), version <= 2);
        assert_eq!(meta::check_subversion(version, 1).is_ok(), version == 1);
    }
    let path =
        crate::test_artifact_root().join(format!("native-meta-create-{}", std::process::id()));
    let created = meta::create(&path, [0; 16], |_| {
        Ok(Arc::new(super::super::kv_mem::MemKv::new()))
    })
    .unwrap();
    assert_eq!(meta::validate_meta(created.as_ref()).unwrap().subversion, 1);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn native_metadata_is_atomic_and_monotone() {
    for fail in [false, true] {
        let (mem, seq) = fixture();
        if fail {
            mem.fail_next_write(KvError::Io("before sync".into()));
        }
        let result = seq.submit(native());
        assert_eq!(result.is_ok(), !fail);
        let durable = mem.crash();
        let info = meta::open_checks(&durable).unwrap();
        assert_eq!(
            (info.subversion, info.features),
            if fail { (1, 0) } else { (2, 2) }
        );
        assert_eq!(
            mvcc::latest_version(&durable, Keyspace::Rows, &[0; 8])
                .unwrap()
                .is_some(),
            !fail
        );
        if !fail {
            seq.submit(request(0)).unwrap();
            let info = meta::open_checks(mem.as_ref()).unwrap();
            assert_eq!((info.subversion, info.features), (2, 2));
        } else {
            assert_eq!(seq.visible_ts(), Ts(0));
        }
    }
    let (mem, seq) = fixture();
    let results = seq.process_group_for_test(vec![native(), request(0)], &ManualClock::new(10), &mut RecentLog::new(1024));
    assert!(results.into_iter().all(|r| r.is_ok()));
    assert_eq!(meta::open_checks(mem.as_ref()).unwrap().subversion, 2);
}
#[test]
fn native_commit_guard_rejects_each_incomplete_or_corrupt_batch() {
    for case in 0..9 {
        let (mem, seq) = fixture();
        let mut req = native();
        match case {
            0 => req.features_or = 0,
            1 => req.minimum_subversion = 0,
            2 => {
                req.writes.remove(&(Keyspace::Kernel, vec![0]));
            }
            3 => {
                req.writes.remove(&(Keyspace::Kernel, vec![7]));
            }
            4 => {
                req.writes
                    .insert((Keyspace::Kernel, vec![7]), Some(vec![0, 2]));
            }
            5 => {
                req.writes
                    .insert((Keyspace::Kernel, vec![7, 1]), Some(ledger(None)));
            }
            6 => {
                req.writes.insert((Keyspace::Kernel, vec![7]), None);
            }
            7 => {
                req.writes
                    .insert((Keyspace::Kernel, vec![7]), Some(ledger(Some(b"bad"))));
            }
            _ => req.minimum_subversion = 3,
        }
        assert_eq!(seq.submit(req), Err(TxnError::Misuse), "case {case}");
        assert_eq!(seq.visible_ts(), Ts(0));
        assert_eq!(meta::open_checks(&mem.crash()).unwrap().features, 0);
    }
}
#[test]
fn open_gate_checks_native_bytes_metadata_and_monotone_scalar_flags() {
    let payload = crate::operations::once::outcome::encode(
        &OnceDescription {
            catalog_epoch: 1,
            output: OnceSchema::Operation(vec![]),
        },
        &Err(crate::operations::Error::NotFound),
    )
    .unwrap();
    for case in 0..9 {
        let (mem, _) = fixture();
        let mut req = native();
        req.writes
            .insert((Keyspace::Kernel, vec![7]), Some(ledger(Some(&payload))));
        let mut batch = WriteBatch::default();
        let mut bits = 2u64;
        let mut version = 2u32;
        match case {
            0 => bits = 0,
            1 => version = 1,
            2 => {
                req.writes.remove(&(Keyspace::Kernel, vec![0]));
            }
            3 => {
                req.writes
                    .insert((Keyspace::Kernel, vec![7]), Some(ledger(Some(b"bad"))));
            }
            4 => {
                req.writes
                    .insert((Keyspace::Kernel, vec![7, 1]), Some(ledger(None)));
            }
            5 => {
                req.writes.insert((Keyspace::Kernel, vec![0]), Some(vec![]));
            }
            6 => {
                req.writes.insert((Keyspace::Kernel, vec![7]), None);
            }
            7 => {
                bits = 2 | 1 | 16; // flags may outlive compacted payloads
                req.writes.insert((Keyspace::Kernel, vec![7]), Some(ledger(None)));
            }
            _ => {}
        }
        for ((space, key), value) in req.writes {
            batch.put(
                space,
                super::super::codec::user_key_seek_at(&key, Ts(1)),
                value.map_or_else(mvcc::encode_tombstone, |v| mvcc::encode_live(&v)),
            );
        }
        batch.put(
            Keyspace::Meta,
            b"subversion".to_vec(),
            version.to_be_bytes().to_vec(),
        );
        meta::put_features(&mut batch, bits).unwrap();
        mem.write(batch, Durability::Sync).unwrap();
        assert_eq!(
            meta::open_checks(mem.as_ref()).is_ok(),
            case >= 7,
            "case {case}"
        );
    }
}

#[test]
fn receipt_descriptions_require_scalar_and_temporal_format_guards() {
    use crate::relational::{ColumnDef, DataType};
    use super::super::{kernel::{Kernel, KernelImage}, locks::{LockConfig, LockManager, TxnId}, txn::{Isolation, Transaction}};
    for (data_type, format, bits) in [(DataType::Numeric, 6, 3), (DataType::Date, 16, 18)] {
        let payload = crate::operations::once::outcome::encode(&OnceDescription {
            catalog_epoch: 1, output: OnceSchema::Operation(vec![ColumnDef {
                id: 1, name: "x".into(), data_type, not_null: false,
            }]),
        }, &Err(crate::operations::Error::NotFound)).unwrap();
        for (family, flags, accepted) in [(4, bits, false), (format, 2, false), (format, bits, true)] {
            let (_, seq) = fixture();
            let mut req = native(); req.features_or = flags;
            let key = (Keyspace::Kernel, vec![0]);
            let mut metadata: storage::Metadata = storage::decode(req.writes[&key].as_ref().unwrap()).unwrap();
            metadata.format = family;
            req.writes.insert(key, Some(storage::encode(&metadata).unwrap()));
            req.writes.insert((Keyspace::Kernel, vec![7]), Some(ledger(Some(&payload))));
            assert_eq!(seq.submit(req).is_ok(), accepted);
        }
        let (mem, seq) = fixture();
        let locks = Arc::new(LockManager::new(LockConfig::default()));
        let mut txn = Transaction::begin(&seq, &locks, TxnId(1), Isolation::ReadCommitted);
        let before = KernelImage(storage::empty(0));
        let mut after = before.clone();
        after.0.native_outcomes = Some(storage::native_once::NativeOutcomes::from_bytes(&ledger(Some(&payload))).unwrap());
        {
            let mut guard = Kernel::lock(&mut txn, seq.kv(), None, &|| false).unwrap();
            Kernel::stage(&mut guard, &before, &after).unwrap();
        }
        txn.commit().unwrap();
        let info = meta::open_checks(mem.as_ref()).unwrap();
        assert_eq!((info.features, info.subversion), (bits, 2));
    }
}

#[test]
fn replicated_native_content_is_rejected_at_commit_and_open() {
    let (mem, seq) = fixture();
    let mut req = native();
    req.features_or |= meta::REPLICATED;
    assert_eq!(seq.submit(req), Err(TxnError::Misuse));
    assert_eq!(seq.visible_ts(), Ts(0));
    assert_eq!(meta::open_checks(mem.as_ref()).unwrap().features, 0);
    seq.submit(native()).unwrap();
    let mut batch = WriteBatch::default();
    meta::put_features(&mut batch, meta::NATIVE_OUTCOMES | meta::REPLICATED).unwrap();
    mem.write(batch, Durability::Sync).unwrap();
    assert!(matches!(meta::open_checks(mem.as_ref()),
        Err(meta::FormatError::Unsupported(message))
            if message == "native outcomes on a replicated database"));
}
