//! Track A owns this module.
//! Typed helpers over the range-local Meta keys (layout owned by engine::meta).
use crate::storage::engine::meta::{RangeLocalKey, range_local_key, range_local_prefix};
use crate::storage::range_api::RangeId;
use crate::storage::replicated::AppliedPosition;

#[allow(dead_code)]
pub(crate) fn applied_key(id: RangeId) -> Vec<u8> {
    range_local_key(id, &RangeLocalKey::Applied)
}

/// index BE8 || term BE8.
#[allow(dead_code)]
pub(crate) fn encode_applied(at: AppliedPosition) -> [u8; 16] {
    let mut out = [0; 16];
    out[..8].copy_from_slice(&at.index.to_be_bytes());
    out[8..].copy_from_slice(&at.term.to_be_bytes());
    out
}

#[allow(dead_code)]
pub(crate) fn decode_applied(bytes: &[u8]) -> Option<AppliedPosition> {
    let (index, term) = bytes.split_first_chunk::<8>()?;
    Some(AppliedPosition {
        index: u64::from_be_bytes(*index),
        term: decode_u64(term)?,
    })
}

#[allow(dead_code)]
pub(crate) fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

#[allow(dead_code)]
pub(crate) fn decode_u64(bytes: &[u8]) -> Option<u64> {
    bytes.try_into().ok().map(u64::from_be_bytes)
}

/// Meta interval holding exactly this range's local keys: [prefix, prefix with '/' -> '0').
#[allow(dead_code)]
pub(crate) fn local_interval(id: RangeId) -> (Vec<u8>, Vec<u8>) {
    let start = range_local_prefix(id);
    let mut end = start.clone();
    *end.last_mut().expect("prefix ends with '/'") = b'0';
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applied_and_u64_round_trip() {
        let at = AppliedPosition {
            index: 0x0102_0304_0506_0708,
            term: u64::MAX,
        };
        let bytes = encode_applied(at);
        assert_eq!(bytes[..8], at.index.to_be_bytes());
        assert_eq!(decode_applied(&bytes), Some(at));
        for len in [0, 8, 15, 17] {
            assert_eq!(decode_applied(&vec![0; len]), None);
        }
        assert_eq!(decode_u64(&encode_u64(42)), Some(42));
        assert_eq!(decode_u64(&[0; 7]), None);
        assert_eq!(decode_u64(&[0; 9]), None);
        assert_eq!(applied_key(RangeId(3)), {
            let mut key = b"range/".to_vec();
            key.extend_from_slice(&3u64.to_be_bytes());
            key.extend_from_slice(b"/applied");
            key
        });
    }

    #[test]
    fn local_interval_covers_exactly_the_range_keys() {
        let id = RangeId(7);
        let (start, end) = local_interval(id);
        let inside = |key: &[u8]| key >= start.as_slice() && key < end.as_slice();
        for sub in [
            RangeLocalKey::Applied,
            RangeLocalKey::Install,
            RangeLocalKey::Prepared(u64::MAX),
            RangeLocalKey::Marker(0),
            RangeLocalKey::GcWatermark,
        ] {
            let key = range_local_key(id, &sub);
            assert!(inside(&key));
            assert_eq!(
                crate::storage::engine::meta::parse_range_local_key(&key),
                Some((id, sub))
            );
        }
        assert!(inside(&start));
        assert!(!inside(&start[..start.len() - 1]));
        for other in [RangeId(6), RangeId(8)] {
            assert!(!inside(&range_local_key(other, &RangeLocalKey::Applied)));
        }
        assert!(!inside(b"raft_applied"));
        assert_eq!(&end[..end.len() - 1], &start[..start.len() - 1]);
    }
}
