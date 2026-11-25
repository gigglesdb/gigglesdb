//! Snapshot visibility over prefix-free user keys and descending timestamp suffixes.
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

    // Bounded to this key's versions: a routed read (GD-0075 P8-0) waits
    // only on prepared writes of this key.
    let mut version = Ok(None);
    kv.visit_first_in(ks, seek, upper, &mut |key, value| {
        version = (|| {
            let (found, ts) = codec::split_versioned(key)?;
            validate_key(ks, found)?;
            if found != user_key {
                return Ok(None);
            }
            Ok(Some((ts, decode(value)?.map(<[u8]>::to_vec))))
        })();
    })?;
    version
}

pub(crate) struct VersionedRange<'a> {
    iter: KvIter<'a>,
    ks: Keyspace,
    dir: Direction,
    snapshot: Ts,
    // Forward needs only the last emitted/hidden user key. Reverse retains one
    // raw lookahead and one candidate for the current key, never the whole scan.
    previous: Option<Vec<u8>>,
    pending: Option<(Vec<u8>, Vec<u8>)>,
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
        // [k,... starts at k||!MAX; (k,... starts after all k versions.
        // ...,k] ends after k||!0, while ...,k) ends before k||!MAX.
        // The extra byte in upper_bound includes timestamp zero without any
        // arithmetic overflow at either timestamp extreme. Prefix-free user
        // keys make each such interval exactly one complete version group.
        let lower = match start {
            Bound::Included(key) => Bound::Included(codec::user_key_seek_at(key, Ts(u64::MAX))),
            Bound::Excluded(key) => Bound::Included(codec::user_key_upper_bound(key)),
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
        Ok(Self {
            iter,
            ks,
            dir,
            snapshot,
            previous: None,
            pending: None,
            done: empty,
        })
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
