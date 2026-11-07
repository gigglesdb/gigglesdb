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

fn validate_key(key: &[u8]) -> Result<(), TxnError> {
    if key.len() < 8 {
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

fn version_at(
    kv: &dyn KvRead,
    ks: Keyspace,
    user_key: &[u8],
    snapshot: Ts,
) -> Result<Option<Version>, TxnError> {
    validate_key(user_key)?;
    let seek = codec::user_key_seek_at(user_key, snapshot);
    let Some((key, value)) = kv.seek_ge(ks, &seek)? else {
        return Ok(None);
    };
    let (found, ts) = codec::split_versioned(&key)?;
    if found != user_key {
        return Ok(None);
    }
    Ok(Some((ts, decode(&value)?.map(<[u8]>::to_vec))))
}

pub(crate) struct VersionedRange<'a> {
    iter: KvIter<'a>,
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
                validate_key(key)?;
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
            Box::new(std::iter::empty()) as KvIter<'a>
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
            if self.previous.as_deref() == Some(user) || ts > self.snapshot {
                continue;
            }
            self.previous = Some(user.to_vec());
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
            let user = user.to_vec();
            let mut selected = (ts <= self.snapshot).then_some(value);
            // Reverse visits oldest first. Replace the candidate until this
            // key ends so a newer tombstone also suppresses older live data.
            for item in self.iter.by_ref() {
                let (key, value) = item?;
                let (next_user, ts) = codec::split_versioned(&key)?;
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
