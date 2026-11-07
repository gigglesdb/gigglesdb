//! Serializable read-set validation. Preprocessing costs O(r log r + p log p).
//! Sorted starts and prefix-maximum ends answer each written-key membership
//! query in O(log r + log p), including nested and unbounded intervals.
use super::{
    codec,
    commit::RecentLog,
    hlc::Ts,
    kv::{Direction, Keyspace, KvRead},
    mvcc,
    txn::{ReadSet, SerializationReason, TxnError},
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

#[derive(Clone, Debug)]
pub(crate) struct ValidationConfig {
    pub(crate) scan_budget: usize,
}
impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            scan_budget: 1_000_000,
        }
    }
}
struct Interval {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    prefix_end: Bound<Vec<u8>>,
}
#[derive(Default)]
struct Space {
    points: BTreeSet<Vec<u8>>,
    ranges: Vec<Interval>,
}
pub(crate) struct Validator {
    spaces: BTreeMap<Keyspace, Space>,
    config: ValidationConfig,
    #[cfg(test)]
    fallbacks: std::sync::atomic::AtomicUsize,
}
fn start_cmp(a: &Bound<Vec<u8>>, b: &Bound<Vec<u8>>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(a), Bound::Excluded(b)) => a.cmp(b).then(Ordering::Less),
        (Bound::Excluded(a), Bound::Included(b)) => a.cmp(b).then(Ordering::Greater),
        (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => {
            a.cmp(b)
        }
    }
}
fn end_cmp(a: &Bound<Vec<u8>>, b: &Bound<Vec<u8>>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Greater,
        (_, Bound::Unbounded) => Ordering::Less,
        (Bound::Included(a), Bound::Excluded(b)) => a.cmp(b).then(Ordering::Greater),
        (Bound::Excluded(a), Bound::Included(b)) => a.cmp(b).then(Ordering::Less),
        (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => {
            a.cmp(b)
        }
    }
}
fn empty(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    match (start, end) {
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
            a >= b
        }
        _ => false,
    }
}
fn starts_before(start: &Bound<Vec<u8>>, key: &[u8]) -> bool {
    match start {
        Bound::Unbounded => true,
        Bound::Included(a) => a.as_slice() <= key,
        Bound::Excluded(a) => a.as_slice() < key,
    }
}
fn ends_after(end: &Bound<Vec<u8>>, key: &[u8]) -> bool {
    match end {
        Bound::Unbounded => true,
        Bound::Included(a) => a.as_slice() >= key,
        Bound::Excluded(a) => a.as_slice() > key,
    }
}
fn conflict() -> TxnError {
    TxnError::Serialization(SerializationReason::ReadWriteConflict)
}
impl Validator {
    pub(crate) fn new(reads: ReadSet, config: ValidationConfig) -> Self {
        let mut spaces = BTreeMap::<Keyspace, Space>::new();
        for (ks, key) in reads.points {
            spaces.entry(ks).or_default().points.insert(key);
        }
        for range in reads.ranges {
            if !empty(&range.start, &range.end) {
                spaces.entry(range.ks).or_default().ranges.push(Interval {
                    start: range.start,
                    end: range.end.clone(),
                    prefix_end: range.end,
                });
            }
        }
        for space in spaces.values_mut() {
            space.ranges.sort_by(|a, b| start_cmp(&a.start, &b.start));
            let mut max: Option<Bound<Vec<u8>>> = None;
            for interval in &mut space.ranges {
                if max
                    .as_ref()
                    .is_none_or(|end| end_cmp(end, &interval.end) == Ordering::Less)
                {
                    max = Some(interval.end.clone());
                }
                interval.prefix_end = max.as_ref().unwrap().clone();
            }
        }
        Self {
            spaces,
            config,
            #[cfg(test)]
            fallbacks: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    fn matches(&self, ks: Keyspace, key: &[u8]) -> bool {
        let Some(space) = self.spaces.get(&ks) else {
            return false;
        };
        if space.points.contains(key) {
            return true;
        }
        let n = space
            .ranges
            .partition_point(|interval| starts_before(&interval.start, key));
        n > 0 && ends_after(&space.ranges[n - 1].prefix_end, key)
    }
    pub(crate) fn validate(
        &self,
        log: &RecentLog,
        kv: &dyn KvRead,
        snapshot: Ts,
    ) -> Result<(), TxnError> {
        let mut intersects = false;
        let complete = log
            .writes_after(snapshot, |_, ks, key| intersects |= self.matches(ks, key))
            .is_ok();
        if intersects {
            return Err(conflict());
        }
        if complete {
            return Ok(());
        }
        #[cfg(test)]
        self.fallbacks
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Overflow must not hide accepted but unpublished earlier group writes.
        // All retained entries are beyond horizon; inspect these before durable KV.
        log.writes_after(snapshot.max(log.horizon()), |_, ks, key| {
            intersects |= self.matches(ks, key)
        })
        .expect("horizon covers retained history");
        if intersects {
            return Err(conflict());
        }
        let mut examined = 0usize;
        for (ks, space) in &self.spaces {
            for key in &space.points {
                // Conservatively charge one unit even for a missing point.
                self.charge(&mut examined)?;
                if mvcc::latest_version(kv, *ks, key)?.is_some_and(|(ts, _)| ts > snapshot) {
                    return Err(conflict());
                }
            }
            // Scan every raw version, including tombstones. Never stop at the
            // first user key or use the snapshot-visible MVCC iterator here.
            // Overlapping recorded ranges can examine an entry more than once;
            // each physical examination consumes the same shared total budget.
            for range in &space.ranges {
                let start = match &range.start {
                    Bound::Unbounded => Bound::Unbounded,
                    Bound::Included(k) => Bound::Included(codec::user_key_seek_at(k, Ts(u64::MAX))),
                    Bound::Excluded(k) => Bound::Included(codec::user_key_upper_bound(k)),
                };
                let end = match &range.end {
                    Bound::Unbounded => Bound::Unbounded,
                    Bound::Included(k) => Bound::Excluded(codec::user_key_upper_bound(k)),
                    Bound::Excluded(k) => Bound::Excluded(codec::user_key_seek_at(k, Ts(u64::MAX))),
                };
                for item in kv
                    .range(
                        *ks,
                        start.as_ref().map(Vec::as_slice),
                        end.as_ref().map(Vec::as_slice),
                        Direction::Forward,
                    )
                    .map_err(TxnError::Kv)?
                {
                    let (key, value) = item.map_err(TxnError::Kv)?;
                    self.charge(&mut examined)?;
                    let (_, ts) = codec::split_versioned(&key).map_err(TxnError::Codec)?;
                    mvcc::decode(&value).map_err(TxnError::Kv)?;
                    if ts > snapshot {
                        return Err(conflict());
                    }
                }
            }
        }
        Ok(())
    }
    fn charge(&self, examined: &mut usize) -> Result<(), TxnError> {
        if *examined >= self.config.scan_budget {
            return Err(TxnError::Serialization(
                SerializationReason::ValidationBudget,
            ));
        }
        *examined += 1;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn fallback_count(&self) -> usize {
        self.fallbacks.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        kv::{Durability, KvBackend, KvError, KvIter, WriteBatch},
        kv_mem::MemKv,
        txn::ReadRange,
    };
    use super::*;
    use std::sync::Arc;
    fn key(n: u64) -> Vec<u8> {
        n.to_be_bytes().to_vec()
    }
    fn overflow() -> RecentLog {
        let mut log = RecentLog::new(0);
        log.append(Ts(10), Arc::from([(Keyspace::Rows, key(99))]));
        log.trim(Some(Ts(0)));
        log
    }
    fn range(start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> ReadRange {
        ReadRange {
            ks: Keyspace::Rows,
            start,
            end,
        }
    }
    fn validator(reads: ReadSet, budget: usize) -> Validator {
        Validator::new(
            reads,
            ValidationConfig {
                scan_budget: budget,
            },
        )
    }
    #[test]
    fn interval_index_matches_linear_all_bounds_and_seeded_nested_sets() {
        let mut bounds = vec![Bound::Unbounded];
        for n in 0..=8 {
            bounds.push(Bound::Included(key(n)));
            bounds.push(Bound::Excluded(key(n)));
        }
        let mut seed = 0xa110ca7eu64;
        for round in 0..400 {
            let mut reads = ReadSet::default();
            if round % 3 == 0 {
                reads.points.insert((Keyspace::Rows, key(round % 10)));
            }
            for _ in 0..(round % 30 + 1) {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let a = seed as usize % bounds.len();
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let b = seed as usize % bounds.len();
                reads
                    .ranges
                    .push(range(bounds[a].clone(), bounds[b].clone()));
            }
            let indexed = Validator::new(reads.clone(), ValidationConfig::default());
            for ks in [Keyspace::Rows, Keyspace::Index] {
                for n in 0..=10 {
                    let k = key(n);
                    let expected = reads.points.contains(&(ks, k.clone()))
                        || reads.ranges.iter().any(|r| {
                            r.ks == ks
                                && !empty(&r.start, &r.end)
                                && starts_before(&r.start, &k)
                                && ends_after(&r.end, &k)
                        });
                    assert_eq!(
                        indexed.matches(ks, &k),
                        expected,
                        "round={round}, n={n}, ks={ks:?}"
                    );
                }
            }
        }
        // Exhaustive singleton/empty edge pairs independent of seeded coverage.
        for a in &bounds {
            for b in &bounds {
                let reads = ReadSet {
                    points: BTreeSet::new(),
                    ranges: vec![range(a.clone(), b.clone())],
                };
                let indexed = Validator::new(reads, ValidationConfig::default());
                for n in 0..=10 {
                    let k = key(n);
                    assert_eq!(
                        indexed.matches(Keyspace::Rows, &k),
                        !empty(a, b) && starts_before(a, &k) && ends_after(b, &k)
                    );
                }
            }
        }
    }
    #[test]
    fn overflow_checks_retained_unpublished_before_durable_scan() {
        let mem = MemKv::new();
        let reads = ReadSet {
            points: BTreeSet::from([(Keyspace::Rows, key(1))]),
            ranges: vec![],
        };
        let v = validator(reads, 0);
        let mut log = overflow();
        log.append(Ts(20), Arc::from([(Keyspace::Rows, key(1))]));
        assert_eq!(v.validate(&log, &mem, Ts(0)), Err(conflict()));
        assert_eq!(v.fallback_count(), 1);
    }
    #[test]
    fn fallback_raw_versions_tombstones_and_shared_budget() {
        let mem = MemKv::new();
        let mut batch = WriteBatch::default();
        for n in 1..=3 {
            for ts in 0..=4 {
                batch.put(
                    Keyspace::Rows,
                    codec::user_key_seek_at(&key(n), Ts(ts)),
                    if ts == 4 {
                        mvcc::encode_tombstone()
                    } else {
                        mvcc::encode_live(&[n as u8])
                    },
                );
            }
        }
        mem.write(batch, Durability::Sync).unwrap();
        let reads = ReadSet {
            points: BTreeSet::new(),
            ranges: vec![range(Bound::Included(key(1)), Bound::Included(key(3)))],
        };
        assert_eq!(
            validator(reads.clone(), 100).validate(&overflow(), &mem, Ts(3)),
            Err(conflict())
        );
        // Snapshot 5 is behind log horizon10, but all durable versions are old:
        // all fifteen entries, including same-key old versions, consume budget.
        assert_eq!(
            validator(reads.clone(), 14).validate(&overflow(), &mem, Ts(5)),
            Err(TxnError::Serialization(
                SerializationReason::ValidationBudget
            ))
        );
        assert_eq!(
            validator(reads.clone(), 15).validate(&overflow(), &mem, Ts(5)),
            Ok(())
        );
        let mut with_point = reads;
        with_point.points.insert((Keyspace::Rows, key(99)));
        assert_eq!(
            validator(with_point.clone(), 15).validate(&overflow(), &mem, Ts(5)),
            Err(TxnError::Serialization(
                SerializationReason::ValidationBudget
            ))
        );
        assert_eq!(
            validator(with_point, 16).validate(&overflow(), &mem, Ts(5)),
            Ok(())
        );
        let points = ReadSet {
            points: BTreeSet::from([(Keyspace::Rows, key(1))]),
            ranges: vec![],
        };
        assert_eq!(
            validator(points, 1).validate(&overflow(), &mem, Ts(3)),
            Err(conflict())
        );
    }
    #[test]
    fn fallback_range_bounds_cover_zero_and_max_timestamps() {
        let mem = MemKv::new();
        let mut batch = WriteBatch::default();
        for (n, ts) in [(1, 0), (2, u64::MAX), (3, 0)] {
            batch.put(
                Keyspace::Rows,
                codec::user_key_seek_at(&key(n), Ts(ts)),
                mvcc::encode_tombstone(),
            );
        }
        mem.write(batch, Durability::Sync).unwrap();
        for (a, b, expected) in [
            (Bound::Included(key(1)), Bound::Excluded(key(2)), false),
            (Bound::Excluded(key(1)), Bound::Included(key(2)), true),
            (Bound::Included(key(2)), Bound::Included(key(2)), true),
            (Bound::Excluded(key(2)), Bound::Unbounded, false),
            (Bound::Included(key(3)), Bound::Included(key(1)), false),
        ] {
            let v = validator(
                ReadSet {
                    points: BTreeSet::new(),
                    ranges: vec![range(a, b)],
                },
                100,
            );
            assert_eq!(
                v.validate(&overflow(), &mem, Ts(0)),
                if expected { Err(conflict()) } else { Ok(()) }
            );
        }
    }
    struct Broken {
        error: KvError,
        late: bool,
    }
    impl KvRead for Broken {
        fn get(&self, _: Keyspace, _: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
            Err(self.error.clone())
        }
        fn range(
            &self,
            _: Keyspace,
            _: Bound<&[u8]>,
            _: Bound<&[u8]>,
            _: Direction,
        ) -> Result<KvIter<'_>, KvError> {
            if self.late {
                Ok(Box::new(std::iter::once(Err(self.error.clone()))))
            } else {
                Err(self.error.clone())
            }
        }
    }
    #[test]
    fn fallback_preserves_backend_error_identity_and_codec_errors() {
        for error in [
            KvError::Io("read failed".into()),
            KvError::Corrupt("bad page".into()),
            KvError::Fenced,
            KvError::Closed,
        ] {
            for late in [false, true] {
                for point in [false, true] {
                    let reads = if point {
                        ReadSet {
                            points: BTreeSet::from([(Keyspace::Rows, key(1))]),
                            ranges: vec![],
                        }
                    } else {
                        ReadSet {
                            points: BTreeSet::new(),
                            ranges: vec![range(Bound::Unbounded, Bound::Unbounded)],
                        }
                    };
                    assert_eq!(
                        validator(reads, 100).validate(
                            &overflow(),
                            &Broken {
                                error: error.clone(),
                                late
                            },
                            Ts(0)
                        ),
                        Err(TxnError::Kv(error.clone()))
                    );
                }
            }
        }
        let mem = MemKv::new();
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Rows, vec![0], mvcc::encode_live(&[]));
        mem.write(batch, Durability::Sync).unwrap();
        let reads = ReadSet {
            points: BTreeSet::new(),
            ranges: vec![range(Bound::Unbounded, Bound::Unbounded)],
        };
        assert!(matches!(
            validator(reads, 100).validate(&overflow(), &mem, Ts(0)),
            Err(TxnError::Codec(_))
        ));
    }
}
