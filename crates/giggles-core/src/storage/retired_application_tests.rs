use super::*;

// Written by the pre-removal writer at 429cbd2c.
const IMAGE: &str = "0000000000000003000000000000000100000000000000002d0000000b000000000000000000000000000000110000000000000000000000000000000000000000000000000000000000000000010500000000000000340000000100000000000000000000000000000000000000000000000100000000000000010000000000000000000000000000000000000000000000010900000000000000140000000100000000000000000000000000000000";
// Namespace-9 ledger written by the pre-removal writer at 429cbd2c (format 11).
const LEDGER: &str = "0000000100000000000000000000000000000000";

fn unhex(hex: &str) -> Vec<u8> {
    hex.as_bytes().as_chunks::<2>().0.iter().map(|pair| {
        u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()
    }).collect()
}
fn captured() -> BTreeMap<Vec<u8>, Vec<u8>> {
    decode(&unhex(IMAGE)).unwrap()
}
fn refused<T>(result: Result<T, CoreError>) {
    match result {
        Err(CoreError::EngineUnsupported { engine, feature }) => {
            assert_eq!(engine, "legacy records");
            assert_eq!(feature, RETIRED_APPLICATION_LEDGER);
            assert!(feature.contains("application") && feature.contains("429cbd2c"));
        }
        Err(error) => panic!("unexpected error: {error:?}"),
        Ok(_) => panic!("retired application image accepted"),
    }
}
#[test]
fn legacy_application_image_is_refused_by_name() {
    refused(from_records(captured()));
}
#[test]
fn every_application_family_version_is_refused() {
    for base in 11..=14 {
        for wrap in [0, 12, 26, 38] {
            for ledger in [unhex(LEDGER), vec![255]] {
                let mut r = records(&empty(17)).unwrap();
                let mut m: Metadata = decode(&r[&vec![0]]).unwrap();
                m.format = base + wrap;
                r.insert(vec![0], encode(&m).unwrap());
                r.insert(vec![9], ledger);
                refused(from_records(r));
            }
        }
    }
}
#[test]
fn format_53_bit_3_is_refused_by_name() {
    let mut r = records(&empty(17)).unwrap();
    let mut m: Metadata = decode(&r[&vec![0]]).unwrap();
    m.format = 53;
    r.insert(vec![0], encode(&m).unwrap());
    // Canonical format-53 bits: application (3) plus catalog_ext (6), a NEW_BITS member.
    r.insert(vec![10], (8u64 | 64).to_be_bytes().to_vec());
    r.insert(vec![9], unhex(LEDGER));
    refused(from_records(r));
    // Bit 3 alone was never a canonical format-53 record; it stays Corrupt.
    let mut alone = records(&empty(17)).unwrap();
    alone.insert(vec![0], encode(&m).unwrap());
    alone.insert(vec![10], 8u64.to_be_bytes().to_vec());
    alone.insert(vec![9], unhex(LEDGER));
    assert!(matches!(from_records(alone), Err(CoreError::Corrupt(_))));
}
#[test]
fn replica_image_from_bytes_refuses_application_image() {
    refused(replica::Image::from_bytes(unhex(IMAGE)));
}
#[test]
fn namespace_9_under_non_application_family_is_corrupt() {
    let mut r = records(&empty(17)).unwrap();
    r.insert(vec![9], unhex(LEDGER));
    assert!(matches!(from_records(r), Err(CoreError::Corrupt(_))));
}
#[test]
fn v2_meta_refuses_retired_applications_bit() {
    use engine::meta::FormatError;
    for bits in [8, 8 | 1] {
        assert!(matches!(engine::meta::check_features(bits),
            Err(FormatError::Unsupported(message)) if message.contains("applications")));
    }
    assert!(engine::meta::check_features(1).is_ok());
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn kernel_load_refuses_application_family() {
    use engine::{
        codec,
        commit::{ManualClock, Sequencer, SequencerConfig},
        hlc::Ts,
        kernel::Kernel,
        kv::{Durability, Keyspace, Kv, WriteBatch},
        kv_mem::MemKv,
        locks::{LockConfig, LockManager, TxnId},
        mvcc,
        txn::{Isolation, Transaction},
    };
    let kv = Arc::new(Kv::new(Arc::new(MemKv::new())));
    let seq = Sequencer::open(
        kv.clone(),
        Arc::new(ManualClock::new(10)),
        SequencerConfig {
            group_max: 0,
            group_wait: std::time::Duration::ZERO,
            ..Default::default()
        },
    ).unwrap();
    let locks = Arc::new(LockManager::new(LockConfig::default()));
    let mut batch = WriteBatch::default();
    for (key, value) in captured() {
        // SQL is reconstructed by Kernel::load, not stored in Keyspace::Kernel.
        if key == [5] {
            continue;
        }
        batch.put(Keyspace::Kernel, codec::user_key_seek_at(&key, Ts(0)), mvcc::encode_live(&value));
    }
    kv.write(batch, Durability::Sync).unwrap();
    let mut txn = Transaction::begin(&seq, &locks, TxnId(1), Isolation::ReadCommitted);
    refused(Kernel::load(&mut txn));
}
#[test]
fn legacy_store_on_disk_with_application_ledger_is_refused() {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!("retired-applications-{:032x}", u128::from_be_bytes(nonce)));
    fs::create_dir(&path).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(path.clone());
    let db = Database::create(path.join("data.redb")).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(RECORDS).unwrap();
        for (key, value) in captured() {
            table.insert(key.as_slice(), value.as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
    drop(db);
    let error = match Admin::open(&path, Limits::default()) {
        Err(error) => error,
        Ok(_) => panic!("retired on-disk store accepted"),
    };
    // Debug escapes the quotes around the application family name.
    let named = format!("{RETIRED_APPLICATION_LEDGER:?}");
    assert!(format!("{error:?}").contains(named.trim_matches('"')), "{error:?}");
    refused::<()>(Err(error));
}
