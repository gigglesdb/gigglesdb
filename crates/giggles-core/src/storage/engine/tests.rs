use super::codec::*;
use super::crc32c::crc32c;
use super::hlc::{HlcClock, Ts};
use crate::relational::{Value, encode_key};
use std::cmp::Ordering;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn value(&mut self, null: bool) -> Value {
        match self.next() % if null { 13 } else { 12 } {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            2 => Value::Int(i64::MIN),
            3 => Value::Int(i64::MAX),
            4 => Value::Int(-1),
            5 => Value::Int(0),
            6 => Value::Int(self.next() as i64),
            7 => Value::Text(
                ["", "\0", "a\0b", "é東京", "a", "aa"][(self.next() % 6) as usize].into(),
            ),
            8 => Value::Bytes(vec![0, 255, self.next() as u8]),
            9 => Value::Bytes(vec![]),
            10 => Value::Bytes(vec![255]),
            11 => Value::Bytes(vec![0]),
            _ => Value::Null,
        }
    }
    fn tuple(&mut self, arity: usize, null: bool) -> Vec<Value> {
        (0..arity).map(|_| self.value(null)).collect()
    }
}

// Deliberately independent of the byte codec. NULL ranks after all real values.
fn rank(v: &Value) -> u8 {
    match v {
        Value::Bool(_) => 1,
        Value::Int(_) => 2,
        Value::Text(_) => 3,
        Value::Bytes(_) => 4,
        Value::Null => 255,
        #[allow(unreachable_patterns)]
        _ => panic!("extend the reference comparator for this value type"),
    }
}
fn natural(a: &Value, b: &Value) -> Ordering {
    let tags = rank(a).cmp(&rank(b));
    if tags != Ordering::Equal {
        return tags;
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        _ => panic!("unsupported reference comparison"),
    }
}
fn reference(a: &[Value], b: &[Value], dirs: &[Direction]) -> Ordering {
    for ((a, b), dir) in a.iter().zip(b).zip(dirs) {
        let order = natural(a, b);
        let order = match dir {
            Direction::Asc => order,
            Direction::Desc => order.reverse(),
        };
        if order != Ordering::Equal {
            return order;
        }
    }
    Ordering::Equal
}

#[test]
fn crc32c_standard_vector() {
    assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    assert_eq!(crc32c(b""), 0);
}

#[test]
fn primary_keys_preserve_the_frozen_relational_encoding() {
    let mut rng = Rng(0x6372_636b_6579);
    for _ in 0..2_000 {
        let arity = 1 + (rng.next() % 3) as usize;
        let values = rng.tuple(arity, false);
        assert_eq!(pk_bytes(&values).unwrap(), encode_key(&values).unwrap());
    }
    assert!(matches!(
        pk_bytes(&[Value::Null]),
        Err(CodecError::NullInKey)
    ));
}

#[test]
fn index_order_agrees_with_an_independent_reference() {
    let mut rng = Rng(0x8176_9273_aa51);
    for _ in 0..20_000 {
        let arity = 1 + (rng.next() % 3) as usize;
        let dirs: Vec<_> = (0..arity)
            .map(|_| {
                if rng.next() & 1 == 0 {
                    Direction::Asc
                } else {
                    Direction::Desc
                }
            })
            .collect();
        let a = rng.tuple(arity, true);
        let b = rng.tuple(arity, true);
        assert_eq!(
            index_tuple_bytes(&a, &dirs)
                .unwrap()
                .cmp(&index_tuple_bytes(&b, &dirs).unwrap()),
            reference(&a, &b, &dirs)
        );
    }
    assert!(matches!(
        index_tuple_bytes(&[Value::Int(0)], &[]),
        Err(CodecError::Arity)
    ));
}

#[test]
fn fixed_arity_tuple_encodings_are_prefix_free() {
    let mut rng = Rng(0x8a14_6c92);
    // A schema fixes arity and directions. Concatenation does not promise
    // prefix freedom across schemas with different arities.
    for dirs in [
        vec![Direction::Asc],
        vec![Direction::Desc, Direction::Asc],
        vec![Direction::Asc, Direction::Desc, Direction::Asc],
    ] {
        let mut tuples: Vec<_> = (0..2_000)
            .map(|_| index_tuple_bytes(&rng.tuple(dirs.len(), true), &dirs).unwrap())
            .collect();
        tuples.sort();
        tuples.dedup();
        for (i, a) in tuples.iter().enumerate() {
            for b in &tuples[i + 1..] {
                assert!(!b.starts_with(a));
            }
        }
    }
    let one = index_tuple_bytes(&[Value::Int(1)], &[Direction::Asc]).unwrap();
    let two = index_tuple_bytes(
        &[Value::Int(1), Value::Int(2)],
        &[Direction::Asc, Direction::Asc],
    )
    .unwrap();
    assert!(two.starts_with(&one));
}

#[test]
fn clock_is_monotone_through_clock_changes_and_logical_carry() {
    let mut clock = HlcClock::new(Ts(0));
    let mut previous = Ts(0);
    for i in 0..100_000u64 {
        let now = match i % 3 {
            0 => i,
            1 => i / 2,
            _ => 1,
        };
        let next = clock.next(now).unwrap();
        assert!(next > previous);
        assert_eq!(Ts::from_inverted_be(next.inverted_be()), next);
        assert!(next.inverted_be() < previous.inverted_be());
        previous = next;
    }
    let mut clock = HlcClock::new(Ts((42 << 16) | 0xffff));
    assert_eq!(clock.next(42).unwrap(), Ts(43 << 16));
    assert!(matches!(clock.next(1 << 48), Err(CodecError::Clock)));
    let mut clock = HlcClock::new(Ts(u64::MAX));
    assert!(matches!(clock.next(0), Err(CodecError::Clock)));
    for n in [0, 1, u64::MAX - 1, u64::MAX] {
        assert_eq!(Ts::from_inverted_be(Ts(n).inverted_be()), Ts(n));
    }
}

#[test]
fn versioned_keys_sort_seek_and_bound_all_versions() {
    let mut rng = Rng(0xa782_10bc_9887);
    for _ in 0..1_000 {
        let pk = pk_bytes(&[Value::Int(rng.next() as i64)]).unwrap();
        let tuple = index_tuple_bytes(&[rng.value(true)], &[Direction::Desc]).unwrap();
        let id = rng.next();
        let mut timestamps = vec![0, u64::MAX, rng.next(), rng.next(), rng.next()];
        timestamps.sort_unstable();
        timestamps.dedup();
        timestamps.reverse();
        for indexed in [false, true] {
            let keys: Vec<_> = timestamps
                .iter()
                .map(|ts| {
                    if indexed {
                        index_key(id, &tuple, &pk, Ts(*ts))
                    } else {
                        row_key(id, &pk, Ts(*ts))
                    }
                })
                .collect();
            assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
            let (user, _) = split_versioned(&keys[0]).unwrap();
            for (key, ts) in keys.iter().zip(&timestamps) {
                assert_eq!(split_versioned(key).unwrap(), (user, Ts(*ts)));
                assert!(key < &user_key_upper_bound(user));
            }
            for snapshot in [0, 1, rng.next(), u64::MAX] {
                let seek = user_key_seek_at(user, Ts(snapshot));
                let actual = keys
                    .iter()
                    .find(|key| *key >= &seek)
                    .map(|key| split_versioned(key).unwrap().1.0);
                assert_eq!(
                    actual,
                    timestamps.iter().copied().find(|ts| *ts <= snapshot)
                );
            }
        }
    }
    let a = pk_bytes(&[Value::Int(1)]).unwrap();
    let b = pk_bytes(&[Value::Int(2)]).unwrap();
    let key = row_key(7, &a, Ts(0));
    let (user, _) = split_versioned(&key).unwrap();
    let mut expected = user.to_vec();
    expected.extend([255; 8]);
    expected.push(0);
    assert_eq!(user_key_upper_bound(user), expected);
    assert!(key < expected);
    assert!(expected < row_key(7, &b, Ts(u64::MAX)));
    for len in 0..9 {
        assert!(split_versioned(&vec![0; len]).is_err());
    }

    // Small domains deliberately collide identifiers, tuples, and primary keys.
    let mut rows = Vec::new();
    let mut indexes = Vec::new();
    for _ in 0..2_000 {
        let id = rng.next() % 3;
        let value = (rng.next() % 5) as i64 - 2;
        let indexed = (rng.next() % 4) as i64;
        let ts = rng.next() % 7;
        let pk = pk_bytes(&[Value::Int(value)]).unwrap();
        let tuple = index_tuple_bytes(&[Value::Int(indexed)], &[Direction::Desc]).unwrap();
        rows.push(((id, value, std::cmp::Reverse(ts)), row_key(id, &pk, Ts(ts))));
        indexes.push((
            (id, std::cmp::Reverse(indexed), value, std::cmp::Reverse(ts)),
            index_key(id, &tuple, &pk, Ts(ts)),
        ));
    }
    rows.sort_by_key(|entry| entry.0);
    indexes.sort_by_key(|entry| entry.0);
    assert!(rows.windows(2).all(|pair| pair[0].1 <= pair[1].1));
    assert!(indexes.windows(2).all(|pair| pair[0].1 <= pair[1].1));
}

fn rejected(result: Result<RowRecord, CodecError>) {
    assert!(matches!(
        result,
        Err(CodecError::Checksum | CodecError::Corrupt(_))
    ));
}
fn resign(key: &[u8], record: &mut [u8]) {
    let end = record.len() - 4;
    let mut covered = key.to_vec();
    covered.extend_from_slice(&record[..end]);
    record[end..].copy_from_slice(&crc32c(&covered).to_be_bytes());
}

#[test]
fn rows_roundtrip_and_reject_every_single_bit_change_and_truncation() {
    let mut rng = Rng(0x00ab_b1a3_2198);
    for _ in 0..200 {
        let arity = (rng.next() % 4) as usize;
        let values = rng.tuple(arity, true);
        let version = rng.next();
        let key = row_key(
            rng.next(),
            &pk_bytes(&[Value::Int(rng.next() as i64)]).unwrap(),
            Ts(rng.next()),
        );
        for tombstone in [false, true] {
            let record = if tombstone {
                encode_tombstone(&key, version).unwrap()
            } else {
                encode_row(&key, version, &values).unwrap()
            };
            let decoded = decode_row(&key, &record).unwrap();
            assert_eq!(decoded.row_version, version);
            assert_eq!(decoded.tombstone, tombstone);
            assert_eq!(
                decoded.values,
                if tombstone { vec![] } else { values.clone() }
            );
            for byte in 0..record.len() {
                for bit in 0..8 {
                    let mut changed = record.clone();
                    changed[byte] ^= 1 << bit;
                    rejected(decode_row(&key, &changed));
                }
            }
            for byte in 0..key.len() {
                for bit in 0..8 {
                    let mut changed = key.clone();
                    changed[byte] ^= 1 << bit;
                    rejected(decode_row(&changed, &record));
                }
            }
            for end in 0..record.len() {
                rejected(decode_row(&key, &record[..end]));
            }
        }
    }
}

#[test]
fn authenticated_malformed_headers_and_payloads_are_rejected() {
    let _wide = crate::relational::ValueBudget::enter(crate::relational::DEFAULT_MAX_VALUE_BYTES, true);
    assert!(matches!(
        index_tuple_bytes(
            &vec![Value::Null; crate::relational::MAX_COLUMNS + 1],
            &[Direction::Asc; crate::relational::MAX_COLUMNS + 1]
        ),
        Err(CodecError::Limit)
    ));
    let key = row_key(1, &pk_bytes(&[Value::Int(0)]).unwrap(), Ts(1));
    let good = encode_row(&key, 3, &[Value::Text("hello".into())]).unwrap();
    assert_eq!(
        &good[16..good.len() - 4],
        &super::super::encode(&vec![Value::Text("hello".into())]).unwrap()
    );
    for (offset, bytes) in [
        (0, vec![2]),
        (1, vec![2]),
        (10, vec![0, 2]),
        (10, vec![255, 255]),
        (12, u32::MAX.to_be_bytes().to_vec()),
    ] {
        let mut bad = good.clone();
        bad[offset..offset + bytes.len()].copy_from_slice(&bytes);
        resign(&key, &mut bad);
        rejected(decode_row(&key, &bad));
    }
    let mut trailing = good[..good.len() - 4].to_vec();
    trailing.push(0);
    let len = u32::from_be_bytes(good[12..16].try_into().unwrap()) + 1;
    trailing[12..16].copy_from_slice(&len.to_be_bytes());
    trailing.extend([0; 4]);
    resign(&key, &mut trailing);
    rejected(decode_row(&key, &trailing));
    let mut forged = good.clone();
    // Bincode's fixed-width vector count must be checked without allocating it.
    forged[16..24].copy_from_slice(&u64::MAX.to_be_bytes());
    resign(&key, &mut forged);
    rejected(decode_row(&key, &forged));
    let mut tombstone = good.clone();
    tombstone[1] = 1;
    resign(&key, &mut tombstone);
    rejected(decode_row(&key, &tombstone));
    assert!(
        encode_row(
            &key,
            0,
            &vec![Value::Null; crate::relational::MAX_COLUMNS + 1]
        )
        .is_err()
    );
    let wide = encode_row(&key, 0, &[Value::Bytes(vec![0; crate::relational::MAX_ROW_BYTES + 1])]).unwrap();
    {
        let _small = crate::relational::ValueBudget::enter(crate::relational::MAX_ROW_BYTES, true);
        // Session limits constrain the serialized payload when writing, while
        // reading existing records uses the codec's independent hard maximum.
        assert!(encode_row(&key, 0, &[Value::Bytes(vec![0; crate::relational::MAX_ROW_BYTES])]).is_err());
        assert!(decode_row(&key, &wide).is_ok());
    }
}

#[test]
fn codec_errors_map_to_existing_core_error_categories() {
    assert!(matches!(
        crate::CoreError::from(CodecError::Limit),
        crate::CoreError::Limit
    ));
    assert!(matches!(
        crate::CoreError::from(CodecError::Sql(crate::relational::SqlError::Type)),
        crate::CoreError::Sql(crate::relational::SqlError::Type)
    ));
    for error in [
        CodecError::NullInKey,
        CodecError::Arity,
        CodecError::Corrupt("test"),
        CodecError::Checksum,
        CodecError::Clock,
    ] {
        assert!(matches!(
            crate::CoreError::from(error),
            crate::CoreError::Corrupt(_)
        ));
    }
}
