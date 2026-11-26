//! Snapshot visibility over user keys with descending timestamp suffixes.
//!
//! A stored key is `user || !ts` (eight bytes, big-endian, inverted), so the
//! physical order equals user-key order only where no stored user key in the
//! scanned region is a proper prefix of another one (GD-0084 Q40). For a prefix
//! `P` and an extension `P || x`, every `P || x || ..` sorts before `P`'s versions
//! whenever `x` starts below `!ts` (real timestamps make `!ts` start `FE 5x`; row
//! and index components start with a type tag `00..07` or `FF`), and a split of
//! `P`'s versions needs `x` inside the suffix range (synthetic timestamps only).
//!
//! Exact regardless of layout: point lookups (`version_at`, since 12f93a505),
//! GC (`gc.rs`) and the local SSI fallback (`ssi_validate.rs`, since bbc9db174),
//! and the bounds of `VersionedRange` that name a key to include or start
//! after: a bounded start (`Included` or `Excluded`) never yields a proper
//! prefix of the start key, an `Excluded` start never yields the start key and
//! never skips its extensions, and an `Included` end never yields an extension
//! of the end key. (The one corner left needs both an all-zero extension and a
//! synthetic timestamp next to `Ts::MAX`.) Still layout-dependent, so exact only
//! over prefix-free regions: `VersionedRange` forward and reverse order, the
//! omission by either end bound of proper prefixes of the end key (`P || !ts`
//! sorts above `P || y || ..` for real timestamps), the `TxnScan` overlay merge
//! built on it, and reverse paging: an `Excluded(last)` end omits proper
//! prefixes of `last`, and the routed pager's reverse resume (`user || !MAX`,
//! `range/engine/read.rs`) skips the extensions of the key a page ends on,
//! which in reverse follow that key's versions. Forward resume is exact. An
//! example that the exact bounds fix: `ALTER TABLE t DROP CONSTRAINT t_pkey;
//! ALTER TABLE t ADD PRIMARY KEY (a)` on a table keyed `(a, b)`, then
//! `WHERE a = 5` in the same transaction. The point range `[tid||5, tid||5]`
//! used to admit the old key `tid||5||1`, still visible in the base and deleted
//! only in the overlay outside the overlay's exact range. Keyspaces:
//! - Rows `tid || pk` and Index `iid || tuple [|| pk]`: prefix-free at any one
//!   snapshot, because every component (`relational::append_key_iter`,
//!   `secondary_index_keys::append_component`) is self-delimiting and one table
//!   or index has one arity at a time. A DDL rekey (ADD/DROP PRIMARY KEY) writes
//!   the new arity under the same id and deletes the old keys in the same
//!   transaction, so prefix-related pairs exist only with one side hidden (its
//!   tombstone stays physically next to the live side until GC, which the
//!   routed reverse pager above can trip over), or, inside that DDL
//!   transaction, between the base and the overlay.
//! - Kernel, task records (`task/records.rs`) and most Catalog kinds use fixed
//!   widths per kind byte, so they are prefix-free.
//! - Catalog is not prefix-free: `name_key("s")` (public `s`) is a proper prefix
//!   of `name_key("s\0t")` (`s.t`), and the role membership key without a
//!   grantor is a proper prefix of the same edge's named-grantor key; both sides
//!   are live. Catalog readers therefore build keyed maps from full-region scans
//!   and reapply their own writes (`catalog::index_predicate_image`,
//!   `roles_engine::load_txn`), or sort (`v2_export::export_inner`).
//!
//! A prefix-free encoding is planned separately (GD-0084 Q40).
use std::ops::Bound;

use super::{
    codec::{self, CodecError},
    hlc::Ts,
    kv::{Direction, Keyspace, KvError, KvIter, KvPair, KvRead},
    txn::TxnError,
};

type Version = (Ts, Option<Vec<u8>>);

pub(crate) fn encode_live(payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(payload.len() + 1);
    value.push(0);
    value.extend_from_slice(payload);
    value
}

pub(crate) fn encode_tombstone() -> Vec<u8> {
    vec![1]
}

pub(crate) fn decode(value: &[u8]) -> Result<Option<&[u8]>, KvError> {
    match value.split_first() {
        Some((0, payload)) => Ok(Some(payload)),
        Some((1, [])) => Ok(None),
        _ => Err(KvError::Corrupt("invalid MVCC value envelope".into())),
    }
}

fn validate_key(ks: Keyspace, key: &[u8]) -> Result<(), TxnError> {
    // Generic MVCC keys need one user byte. Rows and indexes additionally
    // carry an eight-byte table/index ID before their remaining key bytes.
    let minimum = if matches!(ks, Keyspace::Rows | Keyspace::Index) {
        8
    } else {
        1
    };
    if key.len() < minimum {
        return Err(CodecError::Corrupt("short MVCC user key").into());
    }
    Ok(())
}

pub(crate) fn read_at(
    kv: &dyn KvRead,
    ks: Keyspace,
    user_key: &[u8],
    snapshot: Ts,
) -> Result<Option<Vec<u8>>, TxnError> {
    Ok(version_at(kv, ks, user_key, snapshot)?.and_then(|(_, value)| value))
}

pub(crate) fn latest_version(
    kv: &dyn KvRead,
    ks: Keyspace,
    user_key: &[u8],
) -> Result<Option<Version>, TxnError> {
    version_at(kv, ks, user_key, Ts(u64::MAX))
}

pub(crate) fn latest_committed_at_most(
    kv: &dyn KvRead,
    ks: Keyspace,
    user_key: &[u8],
    bound: Ts,
) -> Result<Option<Version>, TxnError> {
    version_at(kv, ks, user_key, bound)
}

/// Return the newest version of exactly `user_key` with timestamp <= `snapshot`.
/// User keys need not be prefix-free; skip interfering keys within the bounds.
fn version_at(
    kv: &dyn KvRead,
    ks: Keyspace,
    user_key: &[u8],
    snapshot: Ts,
) -> Result<Option<Version>, TxnError> {
    validate_key(ks, user_key)?;
    // Keep the same inclusive seek and exclusive upper bound as the range
    // codec. Typical row and index keys fit both bounds on the stack.
    let key_len = user_key.len();
    let seek_len = key_len + 8;
    let bounds_len = 2 * seek_len + 1;
    let mut inline = [0; 128];
    let mut heap;
    let bounds = if bounds_len <= inline.len() {
        &mut inline[..bounds_len]
    } else {
        heap = vec![0; bounds_len];
        &mut heap[..]
    };
    let (seek, upper) = bounds.split_at_mut(seek_len);
    seek[..key_len].copy_from_slice(user_key);
    seek[key_len..].copy_from_slice(&snapshot.inverted_be());
    upper[..key_len].copy_from_slice(user_key);
    upper[key_len..seek_len].fill(0xff);

    // Keep routed reads within the original bounds (GD-0075 P8-0). The common
    // exact match takes one borrowed visit with no heap allocation for bounds.
    let mut retry = None;
    loop {
        let mut next = None;
        let mut version: Result<Option<Version>, TxnError> = Ok(None);
        kv.visit_first_in(
            ks,
            retry.as_deref().unwrap_or(seek),
            upper,
            &mut |key, value| {
                version = (|| {
                    let (found, ts) = codec::split_versioned(key)?;
                    validate_key(ks, found)?;
                    if found != user_key {
                        next = if key.len() > seek_len {
                            // U||w precedes this longer key. An exact version still
                            // ahead must have a suffix > w; skip all keys sharing w.
                            // Extensions shorter than 8 bytes put timestamp bytes in w,
                            // so each of their versions may cost one re-seek (bounded by
                            // the GC'd version count), on this rare path only.
                            let w = u64::from_be_bytes(key[key_len..seek_len].try_into().unwrap());
                            w.checked_add(1).map(|suffix| {
                                let mut start = user_key.to_vec();
                                start.extend_from_slice(&suffix.to_be_bytes());
                                start
                            })
                        } else {
                            // A shorter user's version fell inside U's bounds.
                            // Advance just past that physical key, not its extensions.
                            let mut start = key.to_vec();
                            start.push(0);
                            Some(start)
                        };
                        if let Some(start) = &next {
                            debug_assert!(start.as_slice() > key);
                        }
                        return Ok(None);
                    }
                    Ok(Some((ts, decode(value)?.map(<[u8]>::to_vec))))
                })();
            },
        )?;
        let version = version?;
        match next {
            Some(start) => retry = Some(start),
            None => return Ok(version),
        }
    }
}

/// The newest visible version of each user key in a range, in physical order.
///
/// GD-0084 Q40 (see the module docs): both start bounds and an `Included` end
/// are exact for any layout. A bounded start never yields a proper prefix of
/// the start key, an `Excluded` start skips only the start key itself, and an
/// `Included` end never yields an extension of the end key. Emission order, and
/// whether an end bound yields proper prefixes of the end key (it omits them for
/// real timestamps), are exact only over prefix-free regions. Callers that scan
/// a region where prefix-related keys can both be visible (Catalog) must not
/// depend on emission order or on end bounds that cut between related keys.
pub(crate) struct VersionedRange<'a> {
    iter: KvIter<'a>,
    ks: Keyspace,
    dir: Direction,
    snapshot: Ts,
    // Forward needs only the last emitted/hidden user key. Reverse retains one
    // raw lookahead and one candidate for the current key, never the whole scan.
    previous: Option<Vec<u8>>,
    pending: Option<(Vec<u8>, Vec<u8>)>,
    // The bounded start key, and whether it is excluded. Its proper prefixes
    // sort below it but their versions sit physically inside the scanned
    // interval; an excluded start is scanned from its own versions so that its
    // extensions are not skipped (GD-0084 Q40).
    start: Option<(Vec<u8>, bool)>,
    // An `Included` end key. Its extensions sort above it but their versions
    // sit physically below `end || [0xff; 8] || [0x00]` (GD-0084 Q40).
    end: Option<Vec<u8>>,
    done: bool,
}

impl<'a> VersionedRange<'a> {
    pub(crate) fn new(
        kv: &'a dyn KvRead,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
        snapshot: Ts,
    ) -> Result<Self, TxnError> {
        for bound in [start, end] {
            if let Bound::Included(key) | Bound::Excluded(key) = bound {
                validate_key(ks, key)?;
            }
        }
        let empty = match (start, end) {
            (Bound::Included(a), Bound::Included(b)) => a > b,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
                a >= b
            }
            _ => false,
        };
        // [k,... and (k,... both start at k||!MAX; ...,k] ends after k||!0,
        // while ...,k) ends before k||!MAX. The extra byte in upper_bound
        // includes timestamp zero without any arithmetic overflow at either
        // timestamp extreme. Where user keys are prefix-related these intervals
        // also hold proper prefixes of a start key and extensions of an
        // `Included` end key; `before_start` and `above_end` drop those, and
        // `before_start` drops an excluded start key itself (GD-0084 Q40).
        let lower = match start {
            // `key || [0xff; 8] || [0x00]` for an excluded start would also pass
            // over the stored extensions of `key`, which sort after it.
            Bound::Included(key) | Bound::Excluded(key) => {
                Bound::Included(codec::user_key_seek_at(key, Ts(u64::MAX)))
            }
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match end {
            Bound::Included(key) => Bound::Excluded(codec::user_key_upper_bound(key)),
            Bound::Excluded(key) => Bound::Excluded(codec::user_key_seek_at(key, Ts(u64::MAX))),
            Bound::Unbounded => Bound::Unbounded,
        };
        let iter = if empty {
            KvIter::new(std::iter::empty())
        } else {
            kv.range(
                ks,
                lower.as_ref().map(Vec::as_slice),
                upper.as_ref().map(Vec::as_slice),
                dir,
            )?
        };
        // Reuse the encoded bounds' allocations: each starts with its key.
        let start = match (start, lower) {
            (Bound::Included(key) | Bound::Excluded(key), Bound::Included(mut lower)) => {
                lower.truncate(key.len());
                Some((lower, matches!(start, Bound::Excluded(_))))
            }
            _ => None,
        };
        let end = match (end, upper) {
            (Bound::Included(key), Bound::Excluded(mut upper)) => {
                upper.truncate(key.len());
                Some(upper)
            }
            _ => None,
        };
        Ok(Self {
            iter,
            ks,
            dir,
            snapshot,
            previous: None,
            pending: None,
            start,
            end,
            done: empty,
        })
    }

    /// A proper prefix of a bounded start key sorts below it, yet its versions
    /// (`prefix || !ts`, with `!ts` above any key byte that extends the prefix
    /// toward the start key) fall inside `[start || ..)`. Skip it, and an
    /// excluded start key itself, so the start bound is exact. Longer keys cost
    /// one length comparison; same-length keys one more slice comparison only
    /// for an excluded start.
    fn before_start(start: Option<&(Vec<u8>, bool)>, user: &[u8]) -> bool {
        start.is_some_and(|(start, excluded)| {
            (user.len() < start.len() || (*excluded && user.len() == start.len()))
                && start.starts_with(user)
        })
    }

    /// The mirror of `before_start` for an `Included` end: an extension
    /// `end || x` sorts above `end`, yet its versions fall below the physical
    /// upper bound `end || [0xff; 8] || [0x00]` whenever `x` starts below
    /// `[0xff; 8]`. Skip it, so the `Included` end bound is exact (for example
    /// the old composite keys still visible in an ADD PRIMARY KEY transaction
    /// that narrowed the key). Shorter or same-length keys cost one length
    /// comparison.
    fn above_end(end: Option<&[u8]>, user: &[u8]) -> bool {
        end.is_some_and(|end| user.len() > end.len() && user.starts_with(end))
    }

    fn next_forward(&mut self) -> Result<Option<KvPair>, TxnError> {
        for item in self.iter.by_ref() {
            let (key, value) = item?;
            let (user, ts) = codec::split_versioned(&key)?;
            validate_key(self.ks, user)?;
            if self.previous.as_deref() == Some(user) || ts > self.snapshot {
                continue;
            }
            let previous = self.previous.get_or_insert_with(Vec::new);
            previous.clear();
            previous.extend_from_slice(user);
            if Self::before_start(self.start.as_ref(), user)
                || Self::above_end(self.end.as_deref(), user)
            {
                continue;
            }
            if let Some(payload) = decode(&value)? {
                return Ok(Some((user.to_vec(), payload.to_vec())));
            }
        }
        Ok(None)
    }

    fn next_reverse(&mut self) -> Result<Option<KvPair>, TxnError> {
        loop {
            let first = match self.pending.take() {
                Some(item) => Some(item),
                None => self.iter.next().transpose()?,
            };
            let Some((key, value)) = first else {
                return Ok(None);
            };
            let (user, ts) = codec::split_versioned(&key)?;
            validate_key(self.ks, user)?;
            let user = user.to_vec();
            let mut selected = (ts <= self.snapshot).then_some(value);
            // Reverse visits oldest first. Replace the candidate until this
            // key ends so a newer tombstone also suppresses older live data.
            for item in self.iter.by_ref() {
                let (key, value) = item?;
                let (next_user, ts) = codec::split_versioned(&key)?;
                validate_key(self.ks, next_user)?;
                if next_user != user {
                    self.pending = Some((key, value));
                    break;
                }
                if ts <= self.snapshot {
                    selected = Some(value);
                }
            }
            if let Some(value) = selected
                && !Self::before_start(self.start.as_ref(), &user)
                && !Self::above_end(self.end.as_deref(), &user)
                && let Some(payload) = decode(&value)?
            {
                return Ok(Some((user, payload.to_vec())));
            }
        }
    }
}

impl Iterator for VersionedRange<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>), TxnError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result = match self.dir {
            Direction::Forward => self.next_forward(),
            Direction::Reverse => self.next_reverse(),
        };
        match result {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::storage::engine::{
        kv::{Durability, KvBackend, WriteBatch},
        kv_mem::MemKv,
    };

    pub(crate) fn point_versions(kv: &dyn KvBackend) {
        let mut batch = WriteBatch::default();
        for ts in 1..=300u64 {
            batch.put(
                Keyspace::Catalog,
                codec::user_key_seek_at(b"shared-a", Ts(ts * 2)),
                if ts == 200 {
                    encode_tombstone()
                } else {
                    encode_live(&ts.to_be_bytes())
                },
            );
        }
        batch.put(
            Keyspace::Catalog,
            codec::user_key_seek_at(b"shared-b", Ts(2)),
            encode_live(b"adjacent"),
        );
        batch.put(
            Keyspace::Catalog,
            codec::user_key_seek_at(&[b'z'; 256], Ts(0)),
            encode_live(b"long key at timestamp zero"),
        );
        kv.write(batch, Durability::Sync).unwrap();
        check_point_versions(kv);
        check_point_versions(kv.snapshot().unwrap().as_ref());
    }

    pub(crate) fn check_point_versions(kv: &dyn KvRead) {
        let ks = Keyspace::Catalog;
        assert_eq!(version_at(kv, ks, b"shared-a", Ts(0)).unwrap(), None);
        assert_eq!(
            version_at(kv, ks, b"shared-a", Ts(201)).unwrap(),
            Some((Ts(200), Some(100u64.to_be_bytes().to_vec())))
        );
        assert_eq!(
            version_at(kv, ks, b"shared-a", Ts(401)).unwrap(),
            Some((Ts(400), None))
        );
        assert_eq!(read_at(kv, ks, b"shared-a", Ts(401)).unwrap(), None);
        assert_eq!(
            latest_version(kv, ks, b"shared-a").unwrap(),
            Some((Ts(600), Some(300u64.to_be_bytes().to_vec())))
        );
        assert_eq!(
            read_at(kv, ks, b"shared-b", Ts(601)).unwrap(),
            Some(b"adjacent".to_vec())
        );
        assert_eq!(version_at(kv, ks, b"shared-", Ts(601)).unwrap(), None);
        let prefix = b"shared-";
        let snapshot = Ts(u64::MAX);
        let (neighbor, _) = kv
            .first_in(
                ks,
                &codec::user_key_seek_at(prefix, snapshot),
                &codec::user_key_upper_bound(prefix),
            )
            .unwrap()
            .expect("longer key in the absent prefix's bounds");
        assert_eq!(codec::split_versioned(&neighbor).unwrap().0, b"shared-a");
        assert_eq!(version_at(kv, ks, prefix, snapshot).unwrap(), None);
        assert_eq!(version_at(kv, ks, b"shared-aa", Ts(601)).unwrap(), None);
        assert_eq!(
            version_at(kv, ks, &[b'z'; 256], Ts(0)).unwrap(),
            Some((Ts(0), Some(b"long key at timestamp zero".to_vec())))
        );
    }

    #[test]
    fn point_versions_mem() {
        point_versions(&MemKv::new());
    }

    pub(crate) fn prefixed_point_versions(kv: &dyn KvBackend) {
        let ks = Keyspace::Rows;
        let short = 7u64.to_be_bytes();
        let longer_neighbor = [&short[..], &[0x42]].concat();

        let shorter_neighbor = 8u64.to_be_bytes();
        let long = [&shorter_neighbor[..], &[0x42]].concat();
        // The shorter neighbor's inverted timestamp begins with the long
        // key's final byte and sorts before the long key's visible version.
        let interfering_ts = Ts(!0x4200_0000_0000_0001);

        let mut batch = WriteBatch::default();
        for (key, ts, value) in [
            (short.as_slice(), Ts(1), b"short".as_slice()),
            (&longer_neighbor, Ts(2), b"extension"),
            (&shorter_neighbor, interfering_ts, b"newer neighbor"),
            (&shorter_neighbor, Ts(1), b"older neighbor"),
            (&long, Ts(2), b"long"),
        ] {
            batch.put(ks, codec::user_key_seek_at(key, ts), encode_live(value));
        }
        kv.write(batch, Durability::Sync).unwrap();

        assert_eq!(
            (
                latest_version(kv, ks, &short).unwrap(),
                read_at(kv, ks, &long, Ts(u64::MAX - 1)).unwrap(),
            ),
            (
                Some((Ts(1), Some(b"short".to_vec()))),
                Some(b"long".to_vec())
            )
        );
    }

    #[test]
    fn prefixed_point_versions_mem() {
        prefixed_point_versions(&MemKv::new());
    }

    pub(crate) fn prefixed_point_versions_tombstone_and_snapshot(kv: &dyn KvBackend) {
        let ks = Keyspace::Rows;
        let deleted = 10u64.to_be_bytes();
        let future = 11u64.to_be_bytes();
        let tombstone_ts = Ts(30);
        let snapshot = Ts(0xc000_0000_0000_0000);
        let mut batch = WriteBatch::default();
        for user in [&deleted, &future] {
            let longer = [&user[..], &[0x42]].concat();
            batch.put(
                ks,
                codec::user_key_seek_at(&longer, Ts(1)),
                encode_live(b"neighbor"),
            );
        }
        batch.put(
            ks,
            codec::user_key_seek_at(&deleted, Ts(10)),
            encode_live(b"old live"),
        );
        batch.put(
            ks,
            codec::user_key_seek_at(&deleted, tombstone_ts),
            encode_tombstone(),
        );
        batch.put(
            ks,
            codec::user_key_seek_at(&future, Ts(u64::MAX)),
            encode_live(b"future"),
        );
        kv.write(batch, Durability::Sync).unwrap();

        // Both probes must actually encounter a longer key first.
        for (user, bound) in [(&deleted, Ts(u64::MAX)), (&future, snapshot)] {
            let (physical, _) = kv
                .first_in(
                    ks,
                    &codec::user_key_seek_at(user, bound),
                    &codec::user_key_upper_bound(user),
                )
                .unwrap()
                .unwrap();
            assert_eq!(
                codec::split_versioned(&physical).unwrap().0,
                [&user[..], &[0x42]].concat()
            );
        }
        assert_eq!(
            latest_version(kv, ks, &deleted).unwrap(),
            Some((tombstone_ts, None))
        );
        for bound in [tombstone_ts, Ts(u64::MAX)] {
            assert_eq!(read_at(kv, ks, &deleted, bound).unwrap(), None);
        }
        assert_eq!(
            latest_committed_at_most(kv, ks, &future, snapshot).unwrap(),
            None
        );
        assert_eq!(read_at(kv, ks, &future, snapshot).unwrap(), None);
        // Adding an older version must make it visible past the same neighbor.
        let mut batch = WriteBatch::default();
        batch.put(
            ks,
            codec::user_key_seek_at(&future, Ts(10)),
            encode_live(b"visible"),
        );
        kv.write(batch, Durability::Sync).unwrap();
        assert_eq!(
            latest_committed_at_most(kv, ks, &future, snapshot).unwrap(),
            Some((Ts(10), Some(b"visible".to_vec())))
        );
        assert_eq!(
            read_at(kv, ks, &future, snapshot).unwrap(),
            Some(b"visible".to_vec())
        );
    }

    #[test]
    fn prefixed_point_versions_tombstone_and_snapshot_mem() {
        prefixed_point_versions_tombstone_and_snapshot(&MemKv::new());
    }

    #[test]
    fn prefixed_point_versions_skip_shared_suffix_and_max() {
        let kv = MemKv::new();
        let ks = Keyspace::Rows;
        let user = 9u64.to_be_bytes();
        let mut batch = WriteBatch::default();
        // Two longer keys share w; a third requires another iteration. Its
        // successor is ff*8, which must still find the exact timestamp-zero key.
        for (w, tail) in [
            (0x4242_4242_4242_4242u64, 1),
            (0x4242_4242_4242_4242, 2),
            (u64::MAX - 1, 1),
        ] {
            let longer = [&user[..], &w.to_be_bytes(), &[tail]].concat();
            batch.put(
                ks,
                codec::user_key_seek_at(&longer, Ts(1)),
                encode_live(b"longer"),
            );
        }
        // ff*8 extensions lie beyond the exclusive upper bound, including
        // when there is no exact version left to return.
        let max_extension = [&user[..], &[0xff; 8]].concat();
        batch.put(
            ks,
            codec::user_key_seek_at(&max_extension, Ts(u64::MAX)),
            encode_live(b"outside"),
        );
        let exact = codec::user_key_seek_at(&user, Ts(0));
        batch.put(ks, exact.clone(), encode_live(b"zero"));
        kv.write(batch, Durability::Sync).unwrap();
        assert_eq!(
            latest_version(&kv, ks, &user).unwrap(),
            Some((Ts(0), Some(b"zero".to_vec())))
        );

        let mut batch = WriteBatch::default();
        batch.delete(ks, exact);
        kv.write(batch, Durability::Sync).unwrap();
        assert_eq!(latest_version(&kv, ks, &user).unwrap(), None);
    }

    #[test]
    fn forward_scan_reuses_previous_across_versions_and_tombstones() {
        let kv = MemKv::new();
        let mut batch = WriteBatch::default();
        for (key, versions) in [
            (
                b"a".as_slice(),
                [
                    (30, Some(b"future-a".as_slice())),
                    (20, Some(b"a20")),
                    (10, Some(b"a10")),
                ],
            ),
            (
                b"b".as_slice(),
                [(30, None), (20, None), (10, Some(b"b10"))],
            ),
            (
                b"c".as_slice(),
                [
                    (30, Some(b"future-c".as_slice())),
                    (20, Some(b"c20")),
                    (10, Some(b"c10")),
                ],
            ),
            (
                b"d".as_slice(),
                [
                    (50, Some(b"d50".as_slice())),
                    (40, None),
                    (30, Some(b"d30")),
                ],
            ),
            (
                b"e".as_slice(),
                [
                    (30, None),
                    (20, Some(b"e20".as_slice())),
                    (10, Some(b"e10")),
                ],
            ),
        ] {
            for (ts, payload) in versions {
                batch.put(
                    Keyspace::Catalog,
                    codec::user_key_seek_at(key, Ts(ts)),
                    payload.map_or_else(encode_tombstone, encode_live),
                );
            }
        }
        kv.write(batch, Durability::Sync).unwrap();
        // Empty user keys are invalid even in generic MVCC keyspaces.
        assert!(validate_key(Keyspace::Catalog, b"").is_err());
        let mut scan = VersionedRange::new(
            &kv,
            Keyspace::Catalog,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
            Ts(25),
        )
        .unwrap();
        assert_eq!(
            scan.by_ref().collect::<Result<Vec<_>, _>>().unwrap(),
            vec![
                (b"a".to_vec(), b"a20".to_vec()),
                (b"c".to_vec(), b"c20".to_vec()),
                (b"e".to_vec(), b"e20".to_vec()),
            ]
        );
        assert!(scan.next().is_none());
    }
}


#[cfg(test)]
mod q40_exact_key_boundary {
    use super::*;
    use crate::storage::engine::{
        kv::{Durability, KvBackend, WriteBatch},
        kv_mem::MemKv,
    };

    fn neighbors() -> (MemKv, Vec<u8>, Vec<u8>) {
        let kv = MemKv::new();
        let user = 7u64.to_be_bytes().to_vec();
        let neighbor = [&user[..], &[0x42]].concat();
        let mut batch = WriteBatch::default();
        for (key, value) in [(&user, b"short".as_slice()), (&neighbor, b"long".as_slice())] {
            batch.put(Keyspace::Rows, codec::user_key_seek_at(key, Ts(1)), encode_live(value));
        }
        kv.write(batch, Durability::Sync).unwrap();
        (kv, user, neighbor)
    }

    /// The bounded start's proper prefix `user` sorts below `neighbor`, but its
    /// versions lie physically above `neighbor`'s. Neither direction may emit it.
    #[test]
    fn start_bound_must_not_return_a_shorter_prefix() {
        let (kv, _, neighbor) = neighbors();
        for dir in [Direction::Forward, Direction::Reverse] {
            for start in [Bound::Included(neighbor.as_slice()), Bound::Excluded(neighbor.as_slice())] {
                let actual = VersionedRange::new(
                    &kv, Keyspace::Rows, start, Bound::Unbounded, dir, Ts(2),
                ).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
                let expected = match start {
                    Bound::Included(_) => vec![(neighbor.clone(), b"long".to_vec())],
                    _ => vec![],
                };
                assert_eq!(actual, expected, "{dir:?} {start:?}");
            }
        }
    }

    /// The `Included` end's extension `neighbor` sorts above `user`, but its
    /// versions lie physically below `user || [0xff; 8] || [0x00]`. Neither
    /// direction may emit it, whatever the start bound.
    #[test]
    fn included_end_must_not_return_an_extension() {
        let (kv, user, _) = neighbors();
        for dir in [Direction::Forward, Direction::Reverse] {
            for start in [Bound::Unbounded, Bound::Included(user.as_slice())] {
                let actual = VersionedRange::new(
                    &kv, Keyspace::Rows, start, Bound::Included(&user), dir, Ts(2),
                ).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
                assert_eq!(actual, vec![(user.clone(), b"short".to_vec())], "{dir:?} {start:?}");
            }
        }
    }

    /// An `Excluded` start skips the start key itself but not its extensions,
    /// which sort above it (a pager resuming after `user` must not lose them).
    #[test]
    fn excluded_start_must_return_extensions_but_not_the_start() {
        let (kv, user, neighbor) = neighbors();
        for dir in [Direction::Forward, Direction::Reverse] {
            for end in [Bound::Unbounded, Bound::Included(neighbor.as_slice())] {
                let actual = VersionedRange::new(
                    &kv, Keyspace::Rows, Bound::Excluded(&user), end, dir, Ts(2),
                ).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
                assert_eq!(actual, vec![(neighbor.clone(), b"long".to_vec())], "{dir:?} {end:?}");
            }
        }
    }

    #[test]
    fn included_end_must_not_return_a_longer_neighbor() {
        let (kv, user, _) = neighbors();
        let actual = VersionedRange::new(
            &kv, Keyspace::Rows, Bound::Included(&user), Bound::Included(&user),
            Direction::Forward, Ts(2),
        ).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(actual, vec![(user, b"short".to_vec())]);
    }

    #[test]
    fn excluded_start_must_not_skip_a_longer_neighbor() {
        let (kv, user, neighbor) = neighbors();
        let actual = VersionedRange::new(
            &kv, Keyspace::Rows, Bound::Excluded(&user), Bound::Unbounded,
            Direction::Forward, Ts(2),
        ).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(actual, vec![(neighbor, b"long".to_vec())]);
    }

    #[test]
    #[ignore = "GD-0084 Q40: range order needs prefix-free user keys (open design decision)"]
    fn reverse_must_visit_longer_neighbor_before_its_prefix() {
        let (kv, user, neighbor) = neighbors();
        let actual = VersionedRange::new(
            &kv, Keyspace::Rows, Bound::Unbounded, Bound::Unbounded,
            Direction::Reverse, Ts(2),
        ).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(actual, vec![(neighbor, b"long".to_vec()), (user, b"short".to_vec())]);
    }

    #[test]
    #[ignore = "GD-0084 Q40: range order needs prefix-free user keys (open design decision)"]
    fn forward_must_visit_prefix_before_longer_neighbor() {
        let (kv, user, neighbor) = neighbors();
        let actual = VersionedRange::new(
            &kv, Keyspace::Rows, Bound::Unbounded, Bound::Unbounded,
            Direction::Forward, Ts(2),
        ).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(actual, vec![(user, b"short".to_vec()), (neighbor, b"long".to_vec())]);
    }

    #[test]
    #[ignore = "GD-0084 Q40: range order needs prefix-free user keys (open design decision)"]
    fn forward_must_not_emit_old_live_version_after_split_tombstone() {
        let kv = MemKv::new();
        let user = 7u64.to_be_bytes().to_vec();
        let neighbor = [&user[..], &[0x80]].concat();
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Rows, codec::user_key_seek_at(&user, Ts(1)), encode_live(b"old"));
        batch.put(Keyspace::Rows, codec::user_key_seek_at(&user, Ts(0xc000_0000_0000_0000)), encode_tombstone());
        batch.put(Keyspace::Rows, codec::user_key_seek_at(&neighbor, Ts(2)), encode_live(b"neighbor"));
        kv.write(batch, Durability::Sync).unwrap();
        let actual = VersionedRange::new(
            &kv, Keyspace::Rows, Bound::Unbounded, Bound::Unbounded,
            Direction::Forward, Ts(0xe000_0000_0000_0000),
        ).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(actual, vec![(neighbor, b"neighbor".to_vec())]);
    }

    #[test]
    #[ignore = "GD-0084 Q40: range order needs prefix-free user keys (open design decision)"]
    fn reverse_must_not_emit_old_live_version_before_split_tombstone() {
        let kv = MemKv::new();
        let user = 7u64.to_be_bytes().to_vec();
        let neighbor = [&user[..], &[0x80]].concat();
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Rows, codec::user_key_seek_at(&user, Ts(1)), encode_live(b"old"));
        batch.put(Keyspace::Rows, codec::user_key_seek_at(&user, Ts(0xc000_0000_0000_0000)), encode_tombstone());
        batch.put(Keyspace::Rows, codec::user_key_seek_at(&neighbor, Ts(2)), encode_live(b"neighbor"));
        kv.write(batch, Durability::Sync).unwrap();
        let actual = VersionedRange::new(
            &kv, Keyspace::Rows, Bound::Unbounded, Bound::Unbounded,
            Direction::Reverse, Ts(0xe000_0000_0000_0000),
        ).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(actual, vec![(neighbor, b"neighbor".to_vec())]);
    }
}
