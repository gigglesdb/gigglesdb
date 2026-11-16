//! Track A owns this module.
//! Raw engine keys to range keys (GD-0075 design.md section 1).
//!
//! A range owns, in each keyspace's column family, the user keys whose `range_key` lies inside
//! its bounds. Catalog, Kernel, Rows and Index keys are versioned (`user || (!ts) BE8`: commit.rs
//! writes them through codec::user_key_seek_at, the same layout as codec::row_key/index_key);
//! Seq and Gc keys are unversioned (txn.rs rejects transactional writes there; seq.rs, xact.rs
//! and gc.rs write the raw key). Meta is never in a range: RangeStore handles its range-local
//! Meta keys (`range/<id BE8>/...`, section 6) itself, so the helpers here reject Meta.
use std::ops::Bound;

use super::super::engine::{codec, kv::Keyspace};
use super::super::range_api::{RangeDescriptor, RangeError, RangeSpace, range_key};

/// Raw-key bounds within one keyspace's column family.
pub(crate) type RawInterval = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// Range space of a keyspace; None for Meta.
#[allow(dead_code)]
pub(crate) fn space_of(ks: Keyspace) -> Option<RangeSpace> {
    Some(match ks {
        Keyspace::Meta => return None,
        Keyspace::Catalog => RangeSpace::Catalog,
        Keyspace::Kernel => RangeSpace::Kernel,
        Keyspace::Seq => RangeSpace::Seq,
        Keyspace::Gc => RangeSpace::Gc,
        Keyspace::Rows => RangeSpace::Rows,
        Keyspace::Index => RangeSpace::Index,
    })
}

/// Keyspace (column family) of a range space.
#[allow(dead_code)]
pub(crate) fn keyspace_of(space: RangeSpace) -> Keyspace {
    match space {
        RangeSpace::Catalog => Keyspace::Catalog,
        RangeSpace::Kernel => Keyspace::Kernel,
        RangeSpace::Seq => Keyspace::Seq,
        RangeSpace::Gc => Keyspace::Gc,
        RangeSpace::Rows => Keyspace::Rows,
        RangeSpace::Index => Keyspace::Index,
    }
}

/// Raw keys carry an 8-byte inverted-timestamp suffix.
#[allow(dead_code)]
pub(crate) fn is_versioned(space: RangeSpace) -> bool {
    !matches!(space, RangeSpace::Seq | RangeSpace::Gc)
}

/// User key of a raw key: suffix stripped for versioned spaces, identity otherwise.
#[allow(dead_code)]
pub(crate) fn user_key(space: RangeSpace, raw_key: &[u8]) -> Result<&[u8], RangeError> {
    if !is_versioned(space) {
        return Ok(raw_key);
    }
    codec::split_versioned(raw_key)
        .map(|(user, _)| user)
        .map_err(|error| RangeError::Corrupt(format!("{space:?} raw key: {error:?}")))
}

/// Range key of a raw key; None for Meta.
#[allow(dead_code)]
pub(crate) fn range_key_of(ks: Keyspace, raw_key: &[u8]) -> Result<Option<Vec<u8>>, RangeError> {
    let Some(space) = space_of(ks) else {
        return Ok(None);
    };
    Ok(Some(range_key(space, user_key(space, raw_key)?)))
}

#[allow(dead_code)]
fn non_meta(ks: Keyspace) -> Result<RangeSpace, RangeError> {
    space_of(ks).ok_or_else(|| RangeError::Corrupt("meta key has no range key".into()))
}

/// Exact ownership test: `desc.contains(range_key(space, user_key))`. Meta is an error; the
/// caller (RangeStore) handles range-local Meta keys itself.
#[allow(dead_code)]
pub(crate) fn contains_raw(
    desc: &RangeDescriptor,
    ks: Keyspace,
    raw_key: &[u8],
) -> Result<bool, RangeError> {
    let space = non_meta(ks)?;
    Ok(desc.contains(&range_key(space, user_key(space, raw_key)?)))
}

/// Raw-key interval of `space`'s column family owned by the range; None = owns nothing there.
///
/// A bound whose tag sorts below the space's tag is Unbounded as a start and owns nothing as an
/// end; above, the reverse; the same tag turns its user part into the raw bound (Included start,
/// Excluded end). For unversioned spaces this is exact. For versioned spaces `user || suffix`
/// compares to a bound's user part `b` like `user` compares to `b` unless `user` is a proper
/// prefix of `b` (codec.rs user_key_upper_bound: "prefix-free user keys"). R5 bounds are whole
/// user keys or `table BE8` prefixes: every Rows/Index user key starts with its 8-byte table or
/// index id, so none is a proper prefix of a `table BE8` bound, and row keys (`table || pk`) and
/// index keys (`index || tuple || pk`) are prefix-free within one table or index because
/// encode_key components are self-delimiting for a fixed arity. Only an arbitrary byte string
/// bound (e.g. `user || [0]`) would break the equivalence; contains_raw stays exact.
#[allow(dead_code)]
pub(crate) fn cf_interval(desc: &RangeDescriptor, space: RangeSpace) -> Option<RawInterval> {
    let tag = space.tag();
    let start = match desc.start.split_first() {
        None => Bound::Unbounded,
        Some((&t, _)) if t < tag => Bound::Unbounded,
        Some((&t, _)) if t > tag => return None,
        Some((_, [])) => Bound::Unbounded,
        Some((_, user)) => Bound::Included(user.to_vec()),
    };
    let end = match desc.end.as_deref().map(<[u8]>::split_first) {
        None => Bound::Unbounded,
        Some(None) => return None,
        Some(Some((&t, _))) if t < tag => return None,
        Some(Some((&t, _))) if t > tag => Bound::Unbounded,
        Some(Some((_, []))) => return None,
        Some(Some((_, user))) => Bound::Excluded(user.to_vec()),
    };
    if let (Bound::Included(lo), Bound::Excluded(hi)) = (&start, &end)
        && lo >= hi
    {
        return None;
    }
    Some((start, end))
}

/// Whether an `Op::DeleteRange [start, end)` on a non-Meta keyspace lies wholly inside the
/// range's cf_interval. An empty or reversed interval deletes nothing and is always inside.
#[allow(dead_code)]
pub(crate) fn interval_in_range(
    desc: &RangeDescriptor,
    ks: Keyspace,
    start: &[u8],
    end: &[u8],
) -> Result<bool, RangeError> {
    let space = non_meta(ks)?;
    if start >= end {
        return Ok(true);
    }
    let Some((lo, hi)) = cf_interval(desc, space) else {
        return Ok(false);
    };
    // cf_interval yields only Included/Unbounded starts and Excluded/Unbounded ends.
    let lo_ok = !matches!(&lo, Bound::Included(lo) if start < lo.as_slice());
    let hi_ok = !matches!(&hi, Bound::Excluded(hi) if end > hi.as_slice());
    Ok(lo_ok && hi_ok)
}

#[cfg(test)]
mod tests {
    use super::super::super::engine::{codec::Direction, hlc::Ts};
    use super::super::super::range_api::{Epoch, RangeId, first_user_key};
    use super::*;
    use crate::relational::Value;
    use std::ops::RangeBounds;

    const SPACES: [RangeSpace; 6] = [
        RangeSpace::Catalog,
        RangeSpace::Kernel,
        RangeSpace::Seq,
        RangeSpace::Gc,
        RangeSpace::Rows,
        RangeSpace::Index,
    ];
    const TS: [Ts; 4] = [Ts(0), Ts(1), Ts((5 << 16) | 7), Ts(u64::MAX)];

    fn desc(start: Vec<u8>, end: Option<Vec<u8>>) -> RangeDescriptor {
        RangeDescriptor {
            id: RangeId(2),
            epoch: Epoch(1),
            start,
            end,
            replicas: vec![],
        }
    }
    fn table(space: RangeSpace, id: u64) -> Vec<u8> {
        range_key(space, &id.to_be_bytes())
    }
    fn pk(n: i64) -> Vec<u8> {
        codec::pk_bytes(&[Value::Int(n)]).unwrap()
    }
    fn strip(raw: Vec<u8>) -> Vec<u8> {
        codec::split_versioned(&raw).unwrap().0.to_vec()
    }
    /// Row-shaped (table 6..=8) and index-shaped (index 6..=8) user keys.
    fn users() -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for id in [6, 7, 8] {
            for n in [-1, 0, 5, 1000] {
                out.push(strip(codec::row_key(id, &pk(n), Ts(0))));
                for text in ["", "a", "a\0b"] {
                    let tuple =
                        codec::index_tuple_bytes(&[Value::Text(text.into())], &[Direction::Asc])
                            .unwrap();
                    out.push(strip(codec::index_key(id, &tuple, &pk(n), Ts(0))));
                }
            }
        }
        out
    }
    fn raws(space: RangeSpace, user: &[u8]) -> Vec<Vec<u8>> {
        if is_versioned(space) {
            TS.iter()
                .map(|ts| codec::user_key_seek_at(user, *ts))
                .collect()
        } else {
            vec![user.to_vec()]
        }
    }
    fn via_interval(desc: &RangeDescriptor, space: RangeSpace, raw: &[u8]) -> bool {
        cf_interval(desc, space).is_some_and(|iv| iv.contains(&raw.to_vec()))
    }

    #[test]
    fn keyspaces_map_to_spaces() {
        for ks in Keyspace::ALL {
            match space_of(ks) {
                None => assert_eq!(ks, Keyspace::Meta),
                Some(space) => assert_eq!(keyspace_of(space), ks),
            }
        }
        for space in SPACES {
            assert_eq!(space_of(keyspace_of(space)), Some(space));
            assert_eq!(
                is_versioned(space),
                !matches!(space, RangeSpace::Seq | RangeSpace::Gc)
            );
        }
        let all = desc(vec![], None);
        assert_eq!(range_key_of(Keyspace::Meta, b"range/x"), Ok(None));
        assert!(matches!(
            contains_raw(&all, Keyspace::Meta, b"x"),
            Err(RangeError::Corrupt(_))
        ));
        assert!(matches!(
            interval_in_range(&all, Keyspace::Meta, b"a", b"b"),
            Err(RangeError::Corrupt(_))
        ));
        assert!(matches!(
            user_key(RangeSpace::Rows, &[0; 8]),
            Err(RangeError::Corrupt(_))
        ));
        assert_eq!(user_key(RangeSpace::Kernel, &[1; 9]), Ok(&[1][..]));
        assert_eq!(user_key(RangeSpace::Seq, &[]), Ok(&[][..]));
    }

    #[test]
    fn range_key_order_follows_tag_then_user_key() {
        let keys: [&[u8]; 7] = [
            &[0],
            &[0, 0],
            &[0, 255],
            &[1],
            &[1, 0, 0],
            &[255],
            &[255, 255],
        ];
        for (i, s1) in SPACES.iter().enumerate() {
            for (j, s2) in SPACES.iter().enumerate() {
                for a in keys {
                    for b in keys {
                        for raw_a in raws(*s1, a) {
                            for raw_b in raws(*s2, b) {
                                let ka = range_key_of(keyspace_of(*s1), &raw_a).unwrap();
                                let kb = range_key_of(keyspace_of(*s2), &raw_b).unwrap();
                                assert_eq!(ka.cmp(&kb), (i, a).cmp(&(j, b)));
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn versions_never_straddle_bounds() {
        for space in [RangeSpace::Rows, RangeSpace::Index] {
            let ks = keyspace_of(space);
            for user in users() {
                let at = range_key(space, &user);
                let mut below = at.clone();
                match below.pop() {
                    Some(0) | None => {}
                    Some(last) => below.extend([last - 1, 0xff, 0xff, 0xff, 0xff]),
                }
                let mut above = at.clone();
                above.push(0);
                let id = u64::from_be_bytes(user[..8].try_into().unwrap());
                for bound in [at, below, above, table(space, id), table(space, id + 1)] {
                    for d in [desc(bound.clone(), None), desc(vec![], Some(bound))] {
                        let seen: Vec<bool> = raws(space, &user)
                            .iter()
                            .map(|raw| contains_raw(&d, ks, raw).unwrap())
                            .collect();
                        assert!(seen.iter().all(|s| *s == seen[0]), "{d:?} {user:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn contains_raw_matches_cf_interval() {
        let users = users();
        let mut descs = vec![
            desc(vec![], Some(first_user_key())),
            desc(table(RangeSpace::Rows, 7), Some(table(RangeSpace::Rows, 8))),
            desc(table(RangeSpace::Rows, 7), None),
            desc(
                table(RangeSpace::Index, 7),
                Some(table(RangeSpace::Index, 8)),
            ),
            desc(first_user_key(), Some(table(RangeSpace::Index, 7))),
            desc(vec![], None),
        ];
        for split in [&users[5], &users[20], &users[40]] {
            descs.push(desc(range_key(RangeSpace::Rows, split), None));
            descs.push(desc(vec![], Some(range_key(RangeSpace::Index, split))));
        }
        for d in &descs {
            for space in SPACES {
                for user in &users {
                    for raw in raws(space, user) {
                        let exact = contains_raw(d, keyspace_of(space), &raw).unwrap();
                        assert_eq!(
                            exact,
                            via_interval(d, space, &raw),
                            "{d:?} {space:?} {raw:?}"
                        );
                    }
                }
            }
        }
        let system = &descs[0];
        assert_eq!(
            cf_interval(system, RangeSpace::Gc),
            Some((Bound::Unbounded, Bound::Unbounded))
        );
        assert_eq!(cf_interval(system, RangeSpace::Rows), None);
        let rows_7 = (
            Bound::Included(7_u64.to_be_bytes().to_vec()),
            Bound::Excluded(8_u64.to_be_bytes().to_vec()),
        );
        assert_eq!(cf_interval(&descs[1], RangeSpace::Rows), Some(rows_7));
        assert_eq!(cf_interval(&descs[1], RangeSpace::Index), None);
        assert_eq!(cf_interval(&descs[1], RangeSpace::Seq), None);
        assert_eq!(
            cf_interval(&descs[2], RangeSpace::Index),
            Some((Bound::Unbounded, Bound::Unbounded))
        );
        // A bound extending a stored user key breaks the raw equivalence (documented above).
        let user = &users[0];
        let mut odd = range_key(RangeSpace::Rows, user);
        odd.push(0);
        let d = desc(odd, None);
        let raw = codec::user_key_seek_at(user, Ts(1));
        assert!(!contains_raw(&d, Keyspace::Rows, &raw).unwrap());
        assert!(via_interval(&d, RangeSpace::Rows, &raw));
    }

    #[test]
    fn delete_range_containment() {
        let d = desc(table(RangeSpace::Rows, 7), Some(table(RangeSpace::Rows, 8)));
        let user = strip(codec::row_key(7, &pk(5), Ts(0)));
        let versions = (
            codec::user_key_seek_at(&user, Ts(u64::MAX)),
            codec::user_key_upper_bound(&user),
        );
        let t = |id: u64| id.to_be_bytes().to_vec();
        let row = |id: u64, n: i64| codec::row_key(id, &pk(n), Ts(3));
        let cases: [(Keyspace, Vec<u8>, Vec<u8>, bool); 9] = [
            (Keyspace::Rows, versions.0.clone(), versions.1.clone(), true),
            (Keyspace::Rows, t(7), t(8), true),
            (Keyspace::Rows, row(7, 0), row(7, 1000), true),
            (Keyspace::Rows, row(7, 0), row(8, 0), false),
            (Keyspace::Rows, row(6, 0), row(7, 0), false),
            (Keyspace::Rows, t(9), t(10), false),
            (Keyspace::Index, t(7), t(8), false),
            (Keyspace::Index, t(7), t(7), true),
            (Keyspace::Seq, t(9), t(1), true),
        ];
        for (ks, start, end, inside) in cases {
            assert_eq!(
                interval_in_range(&d, ks, &start, &end),
                Ok(inside),
                "{ks:?} {start:?} {end:?}"
            );
        }
        let system = desc(vec![], Some(first_user_key()));
        assert_eq!(
            interval_in_range(&system, Keyspace::Catalog, &[], &[255; 20]),
            Ok(true)
        );
        assert_eq!(
            interval_in_range(&system, Keyspace::Rows, &t(0), &t(1)),
            Ok(false)
        );
    }
}
