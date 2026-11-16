use super::*;
use crate::relational::Value;

const T: u64 = 7;

fn key(pk: i64) -> Vec<u8> {
    let mut k = T.to_be_bytes().to_vec();
    k.extend(pk.to_be_bytes());
    k
}

fn row(id: i64, v: i64, version: u64) -> SqlRow {
    SqlRow { values: vec![Value::Int(id), Value::Int(v)], version }
}

#[test]
fn merge_keeps_first_old_last_new() {
    let mut e = Effects::default();
    let (a, b, c) = (row(1, 10, 1), row(1, 20, 2), row(1, 30, 3));
    e.record(T, &key(1), Some(&a), Some(&b)).unwrap();
    e.record(T, &key(1), Some(&b), Some(&c)).unwrap();
    assert_eq!(e.len(), 1);
    let only = e.iter().next().unwrap();
    assert_eq!(only.old.as_ref(), Some(&a));
    assert_eq!(only.new.as_ref(), Some(&c));
    assert_eq!(only.table, T);
    assert_eq!(only.pk, 1i64.to_be_bytes().to_vec());
}

#[test]
fn broken_chain_is_corrupt() {
    let mut e = Effects::default();
    e.record(T, &key(1), Some(&row(1, 10, 1)), Some(&row(1, 20, 2))).unwrap();
    // Same values but a different version still breaks the chain.
    let err = e.record(T, &key(1), Some(&row(1, 20, 9)), Some(&row(1, 30, 10))).unwrap_err();
    assert!(matches!(err, CoreError::Corrupt(m) if m == "effects chain"));
    let err = e.record(T, &key(1), None, Some(&row(1, 30, 10))).unwrap_err();
    assert!(matches!(err, CoreError::Corrupt(m) if m == "effects chain"));
}

#[test]
fn insert_then_delete_vanishes() {
    let mut e = Effects::default();
    let r = row(5, 1, 1);
    e.record(T, &key(5), None, Some(&r)).unwrap();
    e.record(T, &key(5), Some(&r), None).unwrap();
    assert!(e.is_empty());
    e.cross_check(std::iter::empty()).unwrap();
}

#[test]
fn noop_detected_by_values_not_version() {
    let mut e = Effects::default();
    e.record(T, &key(1), Some(&row(1, 10, 1)), Some(&row(1, 10, 2))).unwrap();
    e.record(T, &key(2), Some(&row(2, 10, 1)), Some(&row(2, 11, 1))).unwrap();
    let rows = e.iter().collect::<Vec<_>>();
    assert!(Effects::is_noop(rows[0]));
    assert!(!Effects::is_noop(rows[1]));
}

#[test]
fn pk_move_yields_delete_plus_insert() {
    let mut e = Effects::default();
    e.record(T, &key(1), Some(&row(1, 10, 1)), None).unwrap();
    e.record(T, &key(2), None, Some(&row(2, 10, 2))).unwrap();
    let rows = e.iter().collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].old.is_some() && rows[0].new.is_none());
    assert!(rows[1].old.is_none() && rows[1].new.is_some());
    assert!(!Effects::is_noop(rows[0]) && !Effects::is_noop(rows[1]));
}

#[test]
fn cross_check_fails_on_extra_row_write() {
    let mut e = Effects::default();
    e.record(T, &key(1), Some(&row(1, 10, 1)), Some(&row(1, 11, 2))).unwrap();
    let written = vec![key(1), key(2)];
    let err = e.cross_check(&written).unwrap_err();
    assert!(matches!(err, CoreError::Corrupt(m) if m == "protected effects incomplete"));
    e.cross_check(&written[..1]).unwrap();
}

#[test]
fn cross_check_fails_on_missing_row_write() {
    let mut e = Effects::default();
    e.record(T, &key(1), Some(&row(1, 10, 1)), Some(&row(1, 11, 2))).unwrap();
    e.record(T, &key(2), Some(&row(2, 10, 1)), Some(&row(2, 11, 2))).unwrap();
    let written = vec![key(2)];
    let err = e.cross_check(&written).unwrap_err();
    assert!(matches!(err, CoreError::Corrupt(m) if m == "protected effects incomplete"));
}

#[test]
fn paged_update_of_500_rows_collects_all() {
    // Direct `record` calls in pages of 64, the shape `apply` produces for a
    // paged searched UPDATE (the seam-level variant lives in R3-606's tests).
    let mut e = Effects::default();
    let ids = (0..500i64).collect::<Vec<_>>();
    for page in ids.chunks(64) {
        for id in page {
            e.record(T, &key(*id), Some(&row(*id, 0, 1)), Some(&row(*id, 1, 2))).unwrap();
        }
    }
    assert_eq!(e.len(), 500);
    let written = ids.iter().map(|id| key(*id)).collect::<Vec<_>>();
    e.cross_check(&written).unwrap();
    // Physical-key order.
    let keys = e.iter().map(|r| r.pk.clone()).collect::<Vec<_>>();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
}

#[test]
fn key_of_other_table_is_rejected() {
    let mut e = Effects::default();
    let err = e.record(T + 1, &key(1), None, Some(&row(1, 1, 1))).unwrap_err();
    assert!(matches!(err, CoreError::Corrupt(_)));
}
