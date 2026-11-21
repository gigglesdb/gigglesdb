//! GD-0061 R3-601: the PROTECTED_RECORDS commit gate, the bounded open check
//! and `Kernel::load` skipping the protected Kernel namespaces.
use super::*;
use crate::{
    CoreError,
    storage::engine::{
        commit::{ManualClock, Sequencer, SequencerConfig},
        hlc::Ts,
        kernel::{Kernel, KernelImage},
        kv::{Durability, Kv, KvBackend, KvError, KvIter, WriteBatch},
        kv_mem::MemKv,
        locks::{LockConfig, LockManager, TxnId},
        meta::{self, PROTECTED_RECORDS, REPLICATED},
        txn::{Isolation, Transaction, TxnError},
    },
    task::records::{self as rec, TaskObjectId},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

struct Env {
    mem: Arc<MemKv>,
    kv: Arc<Kv>,
    seq: Arc<Sequencer>,
    locks: Arc<LockManager>,
}
impl Env {
    fn new(features: u64) -> Self {
        let mem = Arc::new(MemKv::new());
        meta::write_initial_meta(mem.as_ref(), [7; 16], features).unwrap();
        let kv = Arc::new(Kv::new(mem.clone()));
        let seq = Sequencer::open(
            kv.clone(),
            Arc::new(ManualClock::new(10)),
            SequencerConfig {
                group_max: 0,
                group_wait: Duration::ZERO,
                ..Default::default()
            },
        )
        .unwrap();
        Self {
            mem,
            kv,
            seq,
            locks: Arc::new(LockManager::new(LockConfig::default())),
        }
    }
    fn txn(&self, id: u64) -> Transaction {
        Transaction::begin(&self.seq, &self.locks, TxnId(id), Isolation::ReadCommitted)
    }
    /// Commit one Kernel write, optionally declaring PROTECTED_RECORDS.
    fn commit(&self, key: Vec<u8>, value: Option<Vec<u8>>, bit: bool) -> Result<Ts, TxnError> {
        let mut txn = self.txn(1);
        match value {
            Some(value) => txn.put(Keyspace::Kernel, key, value)?,
            None => txn.delete(Keyspace::Kernel, key)?,
        }
        if bit {
            txn.require_features(PROTECTED_RECORDS);
        }
        txn.commit()
    }
    fn features(&self) -> u64 {
        meta::validate_meta(self.mem.as_ref()).unwrap().features
    }
}

const TASK: TaskObjectId = TaskObjectId(42);

fn head() -> (Vec<u8>, Vec<u8>) {
    (
        rec::mutation_head_key(TASK),
        rec::MutationHead { next_seq: 1, ..Default::default() }.encode(),
    )
}

/// Write a raw versioned Kernel entry, bypassing the sequencer.
fn raw(mem: &MemKv, key: &[u8], value: &[u8]) {
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Kernel,
        codec::user_key_seek_at(key, Ts(5)),
        mvcc::encode_live(value),
    );
    mem.write(batch, Durability::Sync).unwrap();
}

fn base_kv(features: u64) -> MemKv {
    let mem = MemKv::new();
    meta::write_initial_meta(&mem, [7; 16], features).unwrap();
    mem
}

#[test]
fn kernel_write_without_bit_is_misuse() {
    let env = Env::new(0);
    let (key, value) = head();
    assert!(matches!(env.commit(key.clone(), Some(value), false), Err(TxnError::Misuse)));
    // A tombstone needs the bit too: the namespace itself is gated.
    assert!(matches!(env.commit(key, None, false), Err(TxnError::Misuse)));
    assert_eq!(env.features(), 0);
    // Legacy Kernel namespaces keep their ungated path. (Namespace 7 is the
    // native outcomes ledger, which V2ONCE gates on its own feature bit.)
    env.commit(vec![9], Some(vec![1, 2, 3]), false).unwrap();
}

#[test]
fn kernel_write_with_bit_commits_and_sets_features() {
    let env = Env::new(0);
    let (key, value) = head();
    env.commit(key.clone(), Some(value.clone()), true).unwrap();
    assert_eq!(env.features(), PROTECTED_RECORDS);
    let mut txn = env.txn(2);
    assert_eq!(txn.get(Keyspace::Kernel, &key).unwrap(), Some(value));
    drop(txn);
    // Once the bit is set, later protected writes need no new declaration.
    let debit = rec::DebitRecord { rows_read: 1, ..Default::default() };
    env.commit(rec::debit_key(TASK, 1, 0), Some(debit.encode()), false)
        .unwrap();
    env.commit(key, None, false).unwrap();
    meta::open_checks(env.mem.as_ref()).unwrap();
}

#[test]
fn malformed_key_or_value_rejected_at_commit() {
    let env = Env::new(0);
    let (key, value) = head();
    let mut trailing = value.clone();
    trailing.push(0);
    let mut version = value.clone();
    version[0] = 2;
    let mut short_key = key.clone();
    short_key.pop();
    let mut long_key = key.clone();
    long_key.push(0);
    let bad = [
        (short_key.clone(), Some(value.clone())),
        (long_key, Some(value.clone())),
        (vec![0x26, 1], Some(rec::LedgerHeader::default().encode())),
        (key.clone(), Some(trailing)),
        (key.clone(), Some(version)),
        (key.clone(), Some(Vec::new())),
        // A value of another namespace's shape.
        (key.clone(), Some(rec::DebitRecord::default().encode())),
        (short_key, None),
    ];
    for (key, value) in bad {
        assert!(
            matches!(env.commit(key.clone(), value, true), Err(TxnError::Misuse)),
            "{key:02x?}"
        );
    }
    assert_eq!(env.features(), 0);
    // The whole transaction is refused: a valid record beside a bad one is not written.
    let mut txn = env.txn(3);
    txn.put(Keyspace::Kernel, key.clone(), value).unwrap();
    txn.put(Keyspace::Kernel, rec::receipt_key(TASK, 1), vec![1]).unwrap();
    txn.require_features(PROTECTED_RECORDS);
    assert!(matches!(txn.commit(), Err(TxnError::Misuse)));
    assert_eq!(env.txn(4).get(Keyspace::Kernel, &key).unwrap(), None);
    // A well-formed delete commits.
    env.commit(key, None, true).unwrap();
    assert_eq!(env.features(), PROTECTED_RECORDS);
}

#[test]
fn replicated_plus_protected_rejected() {
    let env = Env::new(REPLICATED);
    let (key, value) = head();
    assert!(matches!(env.commit(key.clone(), Some(value.clone()), true), Err(TxnError::Misuse)));
    assert!(matches!(env.commit(key.clone(), Some(value.clone()), false), Err(TxnError::Misuse)));
    assert_eq!(env.features(), REPLICATED);
    assert!(matches!(
        required_bits_for_kernel_write(&key, Some(&value), REPLICATED | PROTECTED_RECORDS),
        Err(FormatError::Unsupported(_))
    ));
    // Open refuses the combination even with no record present.
    let kv = base_kv(REPLICATED | PROTECTED_RECORDS);
    assert!(matches!(meta::open_checks(&kv), Err(FormatError::Unsupported(_))));
    let kv = base_kv(REPLICATED);
    meta::open_checks(&kv).unwrap();
}

#[test]
fn open_rejects_records_without_bit() {
    let (key, value) = head();
    let kv = base_kv(0);
    raw(&kv, &key, &value);
    assert_eq!(
        meta::open_checks(&kv).unwrap_err(),
        FormatError::Corrupt("content requires unset format feature".into())
    );
    // Even a tombstone is content in the protected namespaces.
    let kv = base_kv(0);
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Kernel,
        codec::user_key_seek_at(&key, Ts(5)),
        mvcc::encode_tombstone(),
    );
    kv.write(batch, Durability::Sync).unwrap();
    assert!(matches!(meta::open_checks(&kv), Err(FormatError::Corrupt(_))));
    // With the bit: valid records open; bad keys or versions do not.
    let kv = base_kv(PROTECTED_RECORDS);
    raw(&kv, &key, &value);
    meta::open_checks(&kv).unwrap();
    let mut wrong_version = value.clone();
    wrong_version[0] = 9;
    for (key, value) in [
        (key.clone(), wrong_version),
        (vec![0x20, 0, 0], value.clone()),
        (vec![0x26, 7], value),
    ] {
        let kv = base_kv(PROTECTED_RECORDS);
        raw(&kv, &key, &value);
        assert!(matches!(meta::open_checks(&kv), Err(FormatError::Corrupt(_))), "{key:02x?}");
    }
}

#[test]
fn open_rejects_unknown_namespace_gaps() {
    for key in [vec![0x0C], vec![0x10, 1], vec![0x1F; 9], vec![0x2C], vec![0x80, 0], vec![0xFE; 3]] {
        for features in [0, PROTECTED_RECORDS] {
            let kv = base_kv(features);
            raw(&kv, &key, &[1]);
            assert_eq!(
                meta::open_checks(&kv).unwrap_err(),
                FormatError::Corrupt("unknown kernel namespace".into()),
                "{key:02x?}"
            );
        }
    }
    // Legacy namespaces and the mutation stamp are outside the gap ranges.
    // (Namespace 7, the native outcomes ledger, needs NATIVE_OUTCOMES; use 9.)
    let kv = base_kv(0);
    raw(&kv, &[0x09], &[1]);
    raw(&kv, &[0xFF], &[]);
    meta::open_checks(&kv).unwrap();
}

/// Counts every raw entry any range over the wrapped store yields.
struct CountingKv<'a> {
    inner: &'a MemKv,
    read: AtomicUsize,
}
impl KvRead for CountingKv<'_> {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.inner.get(ks, key)
    }
    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        let iter = self.inner.range(ks, start, end, dir)?;
        let count = (ks == Keyspace::Kernel).then_some(&self.read);
        Ok(KvIter::new(iter.inspect(move |_| {
            if let Some(count) = count {
                count.fetch_add(1, Ordering::Relaxed);
            }
        })))
    }
}

#[test]
fn open_check_is_bounded() {
    let kv = base_kv(PROTECTED_RECORDS);
    let mut batch = WriteBatch::default();
    let debit = rec::DebitRecord::default().encode();
    for read_seq in 0..10_000u64 {
        batch.put(
            Keyspace::Kernel,
            codec::user_key_seek_at(&rec::debit_key(TASK, read_seq, 0), Ts(5)),
            mvcc::encode_live(&debit),
        );
    }
    // A malformed record past the scan bound is left for read time.
    batch.put(
        Keyspace::Kernel,
        codec::user_key_seek_at(&[0x2B, 0xFF], Ts(5)),
        mvcc::encode_live(&[9]),
    );
    kv.write(batch, Durability::Sync).unwrap();
    let counting = CountingKv { inner: &kv, read: AtomicUsize::new(0) };
    meta::open_checks(&counting).unwrap();
    let read = counting.read.load(Ordering::Relaxed);
    assert!(
        (PROTECTED_OPEN_SCAN_LIMIT..=PROTECTED_OPEN_SCAN_LIMIT + 2).contains(&read),
        "read {read} kernel entries"
    );
    // The same bad record inside the bound is found.
    let kv = base_kv(PROTECTED_RECORDS);
    raw(&kv, &[0x20, 0xFF], &[9]);
    assert!(meta::open_checks(&kv).is_err());
}

#[test]
fn kernel_load_ignores_protected_namespaces() {
    let env = Env::new(0);
    let deadline = Some(Instant::now() + Duration::from_secs(30));
    let mut txn = env.txn(1);
    {
        let mut guard = Kernel::lock(&mut txn, &env.kv, deadline, &|| false).unwrap();
        let image = KernelImage(crate::storage::empty(77));
        Kernel::stage(&mut guard, &KernelImage(crate::storage::empty(0)), &image).unwrap();
    }
    txn.commit().unwrap();
    let before = crate::storage::records(&Kernel::load(&mut env.txn(2)).unwrap().0).unwrap();
    let mut txn = env.txn(3);
    let (key, value) = head();
    txn.put(Keyspace::Kernel, key, value).unwrap();
    txn.put(Keyspace::Kernel, rec::LEDGER_HEADER_KEY.to_vec(), rec::LedgerHeader::default().encode())
        .unwrap();
    txn.put(
        Keyspace::Kernel,
        rec::debit_key(TaskObjectId(u64::MAX), u64::MAX, u32::MAX),
        rec::DebitRecord::default().encode(),
    )
    .unwrap();
    txn.require_features(PROTECTED_RECORDS);
    txn.commit().unwrap();
    let after = crate::storage::records(&Kernel::load(&mut env.txn(4)).unwrap().0).unwrap();
    assert_eq!(after, before);
    // Keys outside the protected range are refused at commit (native content
    // check) and, if present on disk anyway, still validated by load.
    let mut txn = env.txn(5);
    txn.put(Keyspace::Kernel, vec![0x2C], vec![0]).unwrap();
    assert!(matches!(txn.commit(), Err(TxnError::Misuse)));
    raw(&env.mem, &[0x2C], &[0]);
    assert!(matches!(Kernel::load(&mut env.txn(6)), Err(CoreError::Corrupt(_))));
}
