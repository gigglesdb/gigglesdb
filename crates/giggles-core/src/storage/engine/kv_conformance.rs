//! Reusable backend checks. Each check obtains a fresh backend from the factory.
use super::{kv::*, kv_mem::MemKv};
use std::{
    ops::Bound::{self, Excluded, Included, Unbounded},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

const KS: Keyspace = Keyspace::Rows;
type Pair = (Vec<u8>, Vec<u8>);

fn put(backend: &dyn KvBackend, ks: Keyspace, keys: &[&[u8]]) {
    let mut batch = WriteBatch::default();
    for key in keys {
        batch.put(ks, key.to_vec(), key.to_vec());
    }
    backend.write(batch, Durability::Sync).unwrap();
}
fn scan(
    read: &dyn KvRead,
    ks: Keyspace,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    dir: Direction,
) -> Vec<Pair> {
    read.range(ks, start, end, dir)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
fn keys(read: &dyn KvRead, ks: Keyspace) -> Vec<Vec<u8>> {
    scan(read, ks, Unbounded, Unbounded, Direction::Forward)
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

pub(crate) fn run_all(make: &dyn Fn() -> Arc<dyn KvBackend>) {
    isolation(make);
    ordering_and_bounds(make);
    ordered_batch(make);
    exact_delete_range(make);
    snapshots(make);
    large_prefix(make);
    seek(make);
}

fn isolation(make: &dyn Fn() -> Arc<dyn KvBackend>) {
    let db = make();
    let mut batch = WriteBatch::default();
    for ks in Keyspace::ALL {
        batch.put(ks, b"same".to_vec(), ks.name().as_bytes().to_vec());
    }
    db.write(batch, Durability::Sync).unwrap();
    for ks in Keyspace::ALL {
        assert_eq!(
            db.get(ks, b"same").unwrap(),
            Some(ks.name().as_bytes().to_vec())
        );
        for dir in [Direction::Forward, Direction::Reverse] {
            assert_eq!(
                scan(db.as_ref(), ks, Unbounded, Unbounded, dir),
                vec![(b"same".to_vec(), ks.name().as_bytes().to_vec())]
            );
        }
    }
    let mut batch = WriteBatch::default();
    batch.delete_range(KS, Vec::new(), vec![0xff]);
    db.write(batch, Durability::Sync).unwrap();
    for ks in Keyspace::ALL {
        assert_eq!(db.get(ks, b"same").unwrap().is_some(), ks != KS);
    }
}

fn ordering_and_bounds(make: &dyn Fn() -> Arc<dyn KvBackend>) {
    let db = make();
    let all = [
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 255],
        vec![1],
        vec![127],
        vec![255],
        vec![255, 0],
    ];
    let input: Vec<_> = all.iter().rev().map(Vec::as_slice).collect();
    put(db.as_ref(), KS, &input);
    // Exercise every bound type at every present and absent endpoint, including
    // equal-excluded and reversed bounds (which must never panic in a backend).
    let mut endpoints = all.to_vec();
    endpoints.extend([vec![0, 1], vec![2], vec![255, 255]]);
    let mut bounds = vec![Unbounded];
    for endpoint in &endpoints {
        bounds.push(Included(endpoint.as_slice()));
        bounds.push(Excluded(endpoint.as_slice()));
    }
    for &start in &bounds {
        for &end in &bounds {
            let expected: Vec<_> = all
                .iter()
                .filter(|key| {
                    let lower = match start {
                        Included(v) => key.as_slice() >= v,
                        Excluded(v) => key.as_slice() > v,
                        Unbounded => true,
                    };
                    let upper = match end {
                        Included(v) => key.as_slice() <= v,
                        Excluded(v) => key.as_slice() < v,
                        Unbounded => true,
                    };
                    lower && upper
                })
                .map(|key| (key.clone(), key.clone()))
                .collect();
            for dir in [Direction::Forward, Direction::Reverse] {
                let mut wanted = expected.clone();
                if dir == Direction::Reverse {
                    wanted.reverse();
                }
                assert_eq!(
                    scan(db.as_ref(), KS, start, end, dir),
                    wanted,
                    "{start:?} {end:?} {dir:?}"
                );
            }
        }
    }
    let empty = make();
    assert!(keys(empty.as_ref(), KS).is_empty());
    for dir in [Direction::Forward, Direction::Reverse] {
        let mut one = db.range(KS, Included(b""), Included(b""), dir).unwrap();
        assert_eq!(one.next().unwrap().unwrap(), (vec![], vec![]));
        assert!(one.next().is_none());
        assert!(one.next().is_none());
    }
}

fn ordered_batch(make: &dyn Fn() -> Arc<dyn KvBackend>) {
    let db = make();
    let mut batch = WriteBatch::default();
    batch.put(KS, b"a".to_vec(), b"first".to_vec());
    batch.delete(KS, b"a".to_vec());
    batch.put(KS, b"b".to_vec(), b"second".to_vec());
    batch.put(KS, b"a".to_vec(), b"third".to_vec());
    batch.put(KS, b"z".to_vec(), b"removed".to_vec());
    batch.delete(KS, b"z".to_vec());
    batch.delete_range(KS, b"a".to_vec(), b"c".to_vec());
    batch.put(KS, b"b".to_vec(), b"last".to_vec());
    db.write(batch, Durability::Sync).unwrap();
    assert_eq!(db.get(KS, b"z").unwrap(), None);
    assert_eq!(
        scan(db.as_ref(), KS, Unbounded, Unbounded, Direction::Forward),
        vec![(b"b".to_vec(), b"last".to_vec())]
    );
}

fn exact_delete_range(make: &dyn Fn() -> Arc<dyn KvBackend>) {
    let db = make();
    put(db.as_ref(), KS, &[b"a", b"b", b"ba", b"c", b"ca", b"d"]);
    let mut batch = WriteBatch::default();
    batch.delete_range(KS, b"b".to_vec(), b"c".to_vec());
    batch.delete_range(KS, b"a".to_vec(), b"a".to_vec());
    batch.delete_range(KS, b"z".to_vec(), b"a".to_vec());
    db.write(batch, Durability::Sync).unwrap();
    assert_eq!(
        keys(db.as_ref(), KS),
        [b"a".to_vec(), b"c".to_vec(), b"ca".to_vec(), b"d".to_vec()]
    );
}

fn snapshots(make: &dyn Fn() -> Arc<dyn KvBackend>) {
    let db = make();
    put(db.as_ref(), KS, &[b"a", b"b", b"c"]);
    let old = db.snapshot().unwrap();
    let mut batch = WriteBatch::default();
    batch.delete(KS, b"a".to_vec());
    batch.delete_range(KS, b"b".to_vec(), b"d".to_vec());
    batch.put(KS, b"b".to_vec(), b"new".to_vec());
    db.write(batch, Durability::NoSync).unwrap();
    assert_eq!(db.get(KS, b"a").unwrap(), None);
    assert_eq!(db.get(KS, b"b").unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get(KS, b"c").unwrap(), None);
    assert_eq!(keys(db.as_ref(), KS), [b"b".to_vec()]);
    assert_eq!(old.get(KS, b"a").unwrap(), Some(b"a".to_vec()));
    assert_eq!(old.get(KS, b"b").unwrap(), Some(b"b".to_vec()));
    assert_eq!(old.get(KS, b"c").unwrap(), Some(b"c".to_vec()));
    assert_eq!(
        keys(old.as_ref(), KS),
        [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );
    assert_eq!(
        scan(
            old.as_ref(),
            KS,
            Included(b"a"),
            Included(b"c"),
            Direction::Reverse
        ),
        vec![
            (b"c".to_vec(), b"c".to_vec()),
            (b"b".to_vec(), b"b".to_vec()),
            (b"a".to_vec(), b"a".to_vec())
        ]
    );
    drop(old);
    let new = db.snapshot().unwrap();
    assert_eq!(new.get(KS, b"a").unwrap(), None);
    assert_eq!(new.get(KS, b"b").unwrap(), Some(b"new".to_vec()));
    assert_eq!(keys(new.as_ref(), KS), [b"b".to_vec()]);
    drop(new);
    put(db.as_ref(), KS, &[b"a"]);
    assert_eq!(
        keys(db.snapshot().unwrap().as_ref(), KS),
        [b"a".to_vec(), b"b".to_vec()]
    );
}

fn large_prefix(make: &dyn Fn() -> Arc<dyn KvBackend>) {
    let db = make();
    let mut batch = WriteBatch::default();
    for i in 0..100_000u32 {
        batch.put(KS, i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
    }
    db.write(batch, Durability::Sync).unwrap();
    // Correctness smoke only, not a memory benchmark or proof of laziness. The
    // KvRead contract independently requires backends to iterate lazily.
    let first: Vec<_> = db
        .range(KS, Unbounded, Unbounded, Direction::Forward)
        .unwrap()
        .take(3)
        .collect::<Result<_, _>>()
        .unwrap();
    let expected: Vec<_> = (0..3u32)
        .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
        .collect();
    assert_eq!(first, expected);
}

fn seek(make: &dyn Fn() -> Arc<dyn KvBackend>) {
    let db = make();
    assert_eq!(db.seek_ge(KS, b"").unwrap(), None);
    put(db.as_ref(), KS, &[b"b", b"d", b"f"]);
    for (query, expected) in [
        (b"".as_slice(), Some(b"b".as_slice())),
        (b"b", Some(b"b")),
        (b"c", Some(b"d")),
        (b"f", Some(b"f")),
        (b"g", None),
    ] {
        assert_eq!(
            db.seek_ge(KS, query).unwrap(),
            expected.map(|key| (key.to_vec(), key.to_vec()))
        );
    }
    assert_eq!(db.seek_ge(Keyspace::Meta, b"").unwrap(), None);
}

fn mem() -> Arc<dyn KvBackend> {
    Arc::new(MemKv::new())
}
#[test]
fn mem_a_keyspace_isolation() {
    isolation(&mem);
}
#[test]
fn mem_b_ordering_and_bounds() {
    ordering_and_bounds(&mem);
}
#[test]
fn mem_c_ordered_batch() {
    ordered_batch(&mem);
}
#[test]
fn mem_d_exact_delete_range() {
    exact_delete_range(&mem);
}
#[test]
fn mem_e_snapshots() {
    snapshots(&mem);
}
#[test]
fn mem_f_large_prefix() {
    large_prefix(&mem);
}
#[test]
fn mem_g_seek_ge() {
    seek(&mem);
}

fn single(key: &[u8]) -> WriteBatch {
    let mut batch = WriteBatch::default();
    batch.put(KS, key.to_vec(), key.to_vec());
    batch
}

#[test]
fn mem_crash_barriers_and_atomic_batches() {
    for flush in [false, true] {
        let db = MemKv::new();
        db.write(single(b"pending"), Durability::NoSync).unwrap();
        assert_eq!(db.get(KS, b"pending").unwrap(), Some(b"pending".to_vec()));
        assert!(keys(&db.crash(), KS).is_empty());
        if flush {
            db.flush_barrier().unwrap();
        } else {
            db.write(WriteBatch::default(), Durability::Sync).unwrap();
        }
        let mut trailing = single(b"trailing");
        trailing.delete(KS, b"pending".to_vec());
        db.write(trailing, Durability::NoSync).unwrap();
        let recovered = db.crash();
        assert_eq!(keys(&recovered, KS), [b"pending".to_vec()]);
        assert_eq!(keys(&db, KS), [b"trailing".to_vec()]);
    }
    let db = MemKv::new();
    db.write(single(b"old"), Durability::Sync).unwrap();
    let mut batch = single(b"new1");
    batch.put(KS, b"new2".to_vec(), b"new2".to_vec());
    batch.delete(KS, b"old".to_vec());
    db.fail_next_write(KvError::Io("before".into()));
    assert_eq!(
        db.write(batch, Durability::Sync),
        Err(KvError::Io("before".into()))
    );
    assert_eq!(keys(&db, KS), [b"old".to_vec()]);
    assert_eq!(keys(&db.crash(), KS), [b"old".to_vec()]);
    // Injection is consumed by precisely one write.
    db.write(single(b"ok"), Durability::Sync).unwrap();
    assert_eq!(keys(&db.crash(), KS), [b"ok".to_vec(), b"old".to_vec()]);
}

#[test]
fn mem_before_apply_error_fences_all_wrapper_entry_points() {
    for error in errors() {
        let backend = Arc::new(MemKv::new());
        backend.write(single(b"old"), Durability::Sync).unwrap();
        backend.fail_next_write(error.clone());
        let kv = Kv::new(backend.clone());
        assert_eq!(kv.write(single(b"new"), Durability::Sync), Err(error));
        assert!(kv.is_fenced());
        assert_eq!(kv.get(KS, b"old"), Err(KvError::Fenced));
        assert!(matches!(
            kv.range(KS, Unbounded, Unbounded, Direction::Forward),
            Err(KvError::Fenced)
        ));
        assert_eq!(kv.seek_ge(KS, b""), Err(KvError::Fenced));
        assert!(matches!(kv.snapshot(), Err(KvError::Fenced)));
        assert_eq!(
            kv.write(single(b"later"), Durability::Sync),
            Err(KvError::Fenced)
        );
        assert_eq!(kv.flush_barrier(), Err(KvError::Fenced));
        assert_eq!(keys(backend.as_ref(), KS), [b"old".to_vec()]);
        assert_eq!(keys(&backend.crash(), KS), [b"old".to_vec()]);
    }
}

#[test]
fn mem_ambiguous_outcome_is_durable_and_fences_wrapper() {
    let backend = Arc::new(MemKv::new());
    backend
        .write(single(b"earlier"), Durability::NoSync)
        .unwrap();
    backend.fail_next_write_after_apply();
    let kv = Kv::new(backend.clone());
    let mut batch = single(b"first");
    batch.put(KS, b"second".to_vec(), b"second".to_vec());
    assert!(matches!(
        kv.write(batch, Durability::NoSync),
        Err(KvError::Io(_))
    ));
    assert!(kv.is_fenced());
    assert_eq!(
        keys(&backend.crash(), KS),
        [b"earlier".to_vec(), b"first".to_vec(), b"second".to_vec()]
    );
    assert_eq!(kv.get(KS, b"first"), Err(KvError::Fenced));
}

#[test]
fn mem_iterator_retains_view_and_wrapper_delegates_successfully() {
    let backend = Arc::new(MemKv::new());
    let kv = Kv::new(backend.clone());
    kv.write(single(b"a"), Durability::NoSync).unwrap();
    assert_eq!(kv.get(KS, b"a").unwrap(), Some(b"a".to_vec()));
    assert_eq!(
        kv.seek_ge(KS, b"").unwrap(),
        Some((b"a".to_vec(), b"a".to_vec()))
    );
    let snapshot = kv.snapshot().unwrap();
    let mut iter = kv
        .range(KS, Unbounded, Unbounded, Direction::Forward)
        .unwrap();
    let mut batch = single(b"b");
    batch.delete(KS, b"a".to_vec());
    kv.write(batch, Durability::NoSync).unwrap();
    assert_eq!(
        iter.next().unwrap().unwrap(),
        (b"a".to_vec(), b"a".to_vec())
    );
    assert!(iter.next().is_none());
    assert_eq!(keys(snapshot.as_ref(), KS), [b"a".to_vec()]);
    kv.flush_barrier().unwrap();
    assert_eq!(keys(&backend.crash(), KS), [b"b".to_vec()]);
    assert!(!kv.is_fenced());
}

/// Count every backend entry point, including seek, so post-fence checks prove
/// that the wrapper rejects operations before touching the backend.
struct FaultBackend {
    calls: AtomicUsize,
    error: KvError,
    fail_reads: bool,
}
impl FaultBackend {
    fn touch(&self) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}
impl KvRead for FaultBackend {
    fn get(&self, _: Keyspace, _: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.touch();
        if self.fail_reads {
            Err(self.error.clone())
        } else {
            Ok(None)
        }
    }
    fn range(
        &self,
        _: Keyspace,
        _: Bound<&[u8]>,
        _: Bound<&[u8]>,
        _: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        self.touch();
        if self.fail_reads {
            Err(self.error.clone())
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }
    fn seek_ge(&self, _: Keyspace, _: &[u8]) -> Result<Option<Pair>, KvError> {
        self.touch();
        if self.fail_reads {
            Err(self.error.clone())
        } else {
            Ok(None)
        }
    }
}
impl KvBackend for FaultBackend {
    fn write(&self, _: WriteBatch, _: Durability) -> Result<(), KvError> {
        self.touch();
        Err(self.error.clone())
    }
    fn flush_barrier(&self) -> Result<(), KvError> {
        self.touch();
        Err(self.error.clone())
    }
    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        self.touch();
        if self.fail_reads {
            Err(self.error.clone())
        } else {
            Ok(Arc::new(MemKv::new()))
        }
    }
    fn engine_name(&self) -> &'static str {
        self.touch();
        "fault"
    }
}
fn errors() -> [KvError; 4] {
    [
        KvError::Io("io".into()),
        KvError::Corrupt("bad".into()),
        KvError::Fenced,
        KvError::Closed,
    ]
}
fn assert_fenced(kv: &Kv, backend: &FaultBackend) {
    let before = backend.calls.load(Ordering::Relaxed);
    assert!(kv.is_fenced());
    assert_eq!(kv.get(KS, b"x"), Err(KvError::Fenced));
    assert!(matches!(
        kv.range(KS, Unbounded, Unbounded, Direction::Forward),
        Err(KvError::Fenced)
    ));
    assert!(matches!(kv.snapshot(), Err(KvError::Fenced)));
    assert_eq!(kv.seek_ge(KS, b"x"), Err(KvError::Fenced));
    assert_eq!(
        kv.write(single(b"x"), Durability::Sync),
        Err(KvError::Fenced)
    );
    assert_eq!(kv.flush_barrier(), Err(KvError::Fenced));
    assert_eq!(backend.calls.load(Ordering::Relaxed), before);
}
#[test]
fn wrapper_write_and_flush_errors_fence_every_entry_point() {
    for error in errors() {
        for flush in [false, true] {
            let backend = Arc::new(FaultBackend {
                calls: AtomicUsize::new(0),
                error: error.clone(),
                fail_reads: false,
            });
            let kv = Kv::new(backend.clone());
            assert!(!kv.is_fenced());
            let result = if flush {
                kv.flush_barrier()
            } else {
                kv.write(single(b"x"), Durability::Sync)
            };
            assert_eq!(result, Err(error.clone()));
            assert_eq!(backend.calls.load(Ordering::Relaxed), 1);
            assert_fenced(&kv, &backend);
        }
    }
}

#[test]
fn wrapper_read_errors_do_not_fence() {
    for error in errors() {
        let backend = Arc::new(FaultBackend {
            calls: AtomicUsize::new(0),
            error: error.clone(),
            fail_reads: true,
        });
        let kv = Kv::new(backend.clone());
        assert_eq!(kv.get(KS, b"x"), Err(error.clone()));
        assert!(!kv.is_fenced());
        assert!(
            matches!(kv.range(KS, Unbounded, Unbounded, Direction::Forward), Err(e) if e == error)
        );
        assert!(!kv.is_fenced());
        assert!(matches!(kv.snapshot(), Err(e) if e == error));
        assert!(!kv.is_fenced());
        assert_eq!(kv.seek_ge(KS, b"x"), Err(error));
        assert!(!kv.is_fenced());
        assert_eq!(backend.calls.load(Ordering::Relaxed), 4);
    }
}

#[test]
fn batch_accounting_names_and_error_mapping() {
    let mut batch = WriteBatch::default();
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
    assert_eq!(batch.byte_size(), 0);
    batch.put(KS, vec![1, 2], vec![3, 4, 5]);
    batch.delete(KS, vec![6]);
    batch.delete_range(KS, vec![7, 8], vec![9, 10, 11]);
    assert_eq!(batch.len(), 3);
    assert!(!batch.is_empty());
    assert_eq!(batch.byte_size(), 11);
    assert!(matches!(
        batch.into_ops().as_slice(),
        [Op::Put { .. }, Op::Delete { .. }, Op::DeleteRange { .. }]
    ));
    assert_eq!(
        Keyspace::ALL.map(|ks| ks.name()),
        ["meta", "catalog", "rows", "index", "kernel", "seq", "gc"]
    );
    assert_eq!(
        crate::CoreError::from(KvError::Io("io".into())),
        crate::CoreError::Storage("io".into())
    );
    assert_eq!(
        crate::CoreError::from(KvError::Corrupt("bad".into())),
        crate::CoreError::Corrupt("bad".into())
    );
    assert_eq!(
        crate::CoreError::from(KvError::Closed),
        crate::CoreError::Fenced
    );
    assert_eq!(
        crate::CoreError::from(KvError::Fenced),
        crate::CoreError::Fenced
    );
    for error in errors() {
        assert!(!error.to_string().is_empty());
    }
    assert_eq!(MemKv::new().engine_name(), "mem");
}
