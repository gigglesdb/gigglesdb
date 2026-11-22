use super::*;
use crate::storage::engine::{codec, kv::{Durability, WriteBatch}, meta, mvcc};
use super::super::tests::{clock, terminal_payload};

fn fixture() -> (std::path::PathBuf, Admin) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!("once-v2-{:032x}", u128::from_be_bytes(nonce)));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    (path, admin)
}
fn db(admin: &Admin) -> crate::storage::EngineLease {
    admin.test_v2()
}
fn key(admin: &Admin, n: usize) -> NativeOnceKey {
    NativeOnceKey { database_domain: u128::from_be_bytes(db(admin).meta.domain), identity: n.to_be_bytes().to_vec() }
}
fn admit(admin: &Admin, key: &NativeOnceKey) -> NativeResult<NativeOnceAdmission> {
    admin.admit_native_once_engine(key.clone(), [1; 32])
}
fn owner(admin: &Admin, key: &NativeOnceKey) -> NativeOnceOwner {
    match admit(admin, key).unwrap() { NativeOnceAdmission::Owner(o) => o, _ => panic!("owner") }
}
fn seed(admin: &Admin, ledger: &NativeOutcomes) {
    let mut txn = db(admin).begin(Isolation::RepeatableRead);
    let metadata = Metadata { format: 4, domain: 0, schema: 0, last_time: 0, next_approval: 0, origin: Some(77) };
    txn.put(Keyspace::Kernel, vec![0], encode(&metadata).unwrap()).unwrap();
    txn.put(Keyspace::Kernel, vec![7], ledger.to_bytes().unwrap()).unwrap();
    txn.require_features(meta::NATIVE_OUTCOMES);
    txn.require_subversion(meta::NATIVE_SUBVERSION);
    txn.commit().unwrap();
}
fn ledger(admin: &Admin, count: usize, old: bool) -> NativeOutcomes {
    let mut ledger = NativeOutcomes::default();
    for n in 0..count {
        let mut k = key(admin, n);
        if old { k.database_domain ^= 1; }
        ledger.insert_outcome(k, [1; 32], [2; 32], terminal_payload(), 10, clock(100, 110).unwrap()).unwrap();
    }
    ledger
}
fn cleanup(path: std::path::PathBuf, admin: Admin) { drop(admin); fs::remove_dir_all(path).unwrap(); }

#[test]
fn fresh_owner_domain_and_drop() {
    let (path, admin) = fixture();
    let k = key(&admin, 0);
    assert_eq!(admin.lookup_native_once_engine(&k).unwrap(), NativeOnceLookup::Absent);
    let o = owner(&admin, &k);
    assert!(matches!(admit(&admin, &k), Err(NativeOnceError::InProgress)));
    assert!(matches!(admin.admit_native_once_engine(k.clone(), [3; 32]), Err(NativeOnceError::IdempotencyConflict)));
    assert_eq!(admin.lookup_native_once_engine(&k).unwrap(), NativeOnceLookup::InProgress);
    let mut wrong = k.clone(); wrong.database_domain ^= 1;
    assert!(matches!(admit(&admin, &wrong), Err(NativeOnceError::DomainMismatch)));
    assert_eq!(admin.lookup_native_once_engine(&wrong), Err(NativeOnceError::DomainMismatch));
    drop(o);
    assert_eq!(admin.lookup_native_once_engine(&k).unwrap(), NativeOnceLookup::Absent);
    cleanup(path, admin);
}
#[test]
fn replay_tombstone_and_fresh_snapshot() {
    let (path, admin) = fixture();
    let k = key(&admin, 0);
    let mut old = db(&admin).begin(Isolation::RepeatableRead);
    assert_eq!(old.get(Keyspace::Kernel, &[7]).unwrap(), None);
    let mut ledger = ledger(&admin, 1, false);
    let expected = ledger.retained(&k).unwrap().clone();
    seed(&admin, &ledger);
    assert_eq!(old.get(Keyspace::Kernel, &[7]).unwrap(), None);
    assert!(matches!(admit(&admin, &k).unwrap(), NativeOnceAdmission::Replay(r) if r == expected));
    assert_eq!(admin.lookup_native_once_engine(&k).unwrap(), NativeOnceLookup::Retained(expected.clone()));
    assert!(matches!(admin.admit_native_once_engine(k.clone(), [3; 32]), Err(NativeOnceError::IdempotencyConflict)));
    assert_eq!(expected.result, Some(terminal_payload()));
    ledger.compact(clock(121, 121).unwrap()); seed(&admin, &ledger);
    assert_eq!(admin.lookup_native_once_engine(&k).unwrap(), NativeOnceLookup::Expired { retry_until_seconds: 120 });
    assert!(matches!(admit(&admin, &k).unwrap(), NativeOnceAdmission::Replay(r) if r == *ledger.retained(&k).unwrap()));
    drop(old); cleanup(path, admin);
}
#[test]
fn historical_domains_and_concurrent_last_slot() {
    for historical in [true, false] {
        let (path, admin) = fixture();
        seed(&admin, &ledger(&admin, MAX_NATIVE_RECORDS - 1, historical));
        if historical {
            let mut wrong = key(&admin, 0); wrong.database_domain ^= 1;
            assert!(matches!(admit(&admin, &wrong), Err(NativeOnceError::DomainMismatch)));
            assert_eq!(admin.lookup_native_once_engine(&wrong), Err(NativeOnceError::DomainMismatch));
            let o = owner(&admin, &key(&admin, MAX_NATIVE_RECORDS));
            assert!(matches!(admit(&admin, &key(&admin, MAX_NATIVE_RECORDS + 1)), Err(NativeOnceError::RetentionUnavailable)));
            drop(o);
        } else {
            let barrier = std::sync::Barrier::new(3);
            std::thread::scope(|scope| {
                let handles: Vec<_> = (MAX_NATIVE_RECORDS..MAX_NATIVE_RECORDS + 2).map(|n| {
                    let barrier = &barrier; let admin = &admin;
                    scope.spawn(move || { barrier.wait(); admit(admin, &key(admin, n)) })
                }).collect();
                barrier.wait();
                let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
                assert_eq!(results.iter().filter(|r| matches!(r, Ok(NativeOnceAdmission::Owner(_)))).count(), 1);
                assert_eq!(results.iter().filter(|r| matches!(r, Err(NativeOnceError::RetentionUnavailable))).count(), 1);
            });
        }
        cleanup(path, admin);
    }
}
#[test]
fn orphan_ledger_fences() {
    let (path, admin) = fixture();
    seed(&admin, &ledger(&admin, 1, false));
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Kernel, codec::user_key_seek_at(&[0], db(&admin).sequencer.visible_ts()), mvcc::encode_tombstone());
    db(&admin).kv.write(batch, Durability::Sync).unwrap();
    let key = key(&admin, 0);
    assert_eq!(admin.lookup_native_once_engine(&key), Err(NativeOnceError::Core(CoreError::Corrupt("native ledger without kernel metadata".into()))));
    assert!(matches!(admit(&admin, &key), Err(NativeOnceError::Core(CoreError::Fenced))));
    cleanup(path, admin);
}

#[test]
fn t14_kernel_gc_preserves_mixed_ledger_and_corrupt_reopen_fails() {
    use crate::storage::engine::gc::{Gc, GcConfig};
    let (path, admin) = fixture();
    let mut receipts = ledger(&admin, 2, false);
    let entry = receipts.entries.get_mut(&key(&admin, 1)).unwrap();
    entry.retention_seconds = 90;
    entry.record.retry_until_seconds = 200;
    let live = receipts.retained(&key(&admin, 1)).unwrap().clone();
    seed(&admin, &receipts);
    let original_ms = db(&admin).sequencer.visible_ts().0 >> 16;
    let mut pinned = db(&admin).begin(Isolation::RepeatableRead);
    let original = pinned.get(Keyspace::Kernel, &[7]).unwrap();
    assert_eq!(receipts.compact(clock(121, 125).unwrap()), 1);
    seed(&admin, &receipts);
    for _ in 0..100 {
        if db(&admin).sequencer.visible_ts().0 >> 16 > original_ms { break; }
        seed(&admin, &receipts);
    }
    assert!(db(&admin).sequencer.visible_ts().0 >> 16 > original_ms);
    let gc = Gc::open(db(&admin).kv.clone(), db(&admin).sequencer.clone(), GcConfig { retention: Duration::ZERO }).unwrap();
    gc.advance().unwrap();
    while !gc.step(Keyspace::Kernel, 100).unwrap().wrapped {}
    assert_eq!(pinned.get(Keyspace::Kernel, &[7]).unwrap(), original);
    drop(pinned);
    gc.advance().unwrap();
    let mut deleted = 0;
    loop {
        let stats = gc.step(Keyspace::Kernel, 100).unwrap();
        deleted += stats.deleted;
        if stats.wrapped { break; }
    }
    assert!(deleted > 0, "GC must delete superseded kernel versions");
    drop(gc); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(admin.lookup_native_once_engine(&key(&admin, 0)).unwrap(),
        NativeOnceLookup::Expired { retry_until_seconds: 120 });
    assert_eq!(admin.lookup_native_once_engine(&key(&admin, 1)).unwrap(), NativeOnceLookup::Retained(live));
    let mut txn = db(&admin).begin(Isolation::RepeatableRead);
    let mut bytes = txn.get(Keyspace::Kernel, &[7]).unwrap().unwrap();
    drop(txn);
    bytes.truncate(2); // Preserve the supported version, corrupt the ledger body.
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Kernel, codec::user_key_seek_at(&[7], db(&admin).sequencer.visible_ts()),
        mvcc::encode_live(&bytes));
    db(&admin).kv.write(batch, Durability::Sync).unwrap();
    drop(admin);
    assert!(matches!(Admin::open(&path, Limits::default()), Err(CoreError::Corrupt(_))));
    fs::remove_dir_all(path).unwrap();
}
