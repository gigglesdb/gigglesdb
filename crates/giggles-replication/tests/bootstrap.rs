//! Pre-split bootstrap (GD-0075 R5-24): descriptors, round-robin voters, directory
//! records, and routing every key of a pre-split database to its range.
use giggles_core::range_api::{
    RangeId, RangeSpace, SYSTEM_RANGE, first_user_key, range_key,
};
use giggles_replication::{
    bootstrap::{directory_records, hosted_by, presplit},
    directory::{decode_record, record_key},
    route::RouteCache,
};

fn table(id: u64) -> Vec<u8> {
    range_key(RangeSpace::Rows, &id.to_be_bytes())
}

#[test]
fn presplit_builds_contiguous_ranges_with_round_robin_voters() {
    let splits = [table(10), table(20), table(30)];
    let descs = presplit(&splits, &[3, 1, 2, 4], 3).unwrap();
    assert_eq!(descs.len(), 5);
    assert_eq!(descs[0].id, SYSTEM_RANGE);
    assert_eq!(descs[0].start, Vec::<u8>::new());
    assert_eq!(descs[0].end, Some(first_user_key()));
    for (i, d) in descs.iter().enumerate() {
        assert_eq!(d.id, RangeId(1 + i as u64));
        assert_eq!(d.epoch.0, 1);
        if i > 0 {
            assert_eq!(Some(&d.start), descs[i - 1].end.as_ref());
        }
        let nodes: Vec<u64> = d.replicas.iter().map(|r| r.node).collect();
        assert_eq!(nodes.len(), 3);
        // Round-robin over sorted nodes [1, 2, 3, 4], starting one later per range.
        let mut expected: Vec<u64> = (0..3).map(|k| [1, 2, 3, 4][(i + k) % 4]).collect();
        expected.sort_unstable();
        assert_eq!(nodes, expected);
    }
    assert_eq!(descs[4].end, None);
    // Every node hosts some ranges; together each range is hosted three times.
    let hosted: usize = (1..=4).map(|n| hosted_by(&descs, n).len()).sum();
    assert_eq!(hosted, 5 * 3);
    // Replication is capped by the node count.
    assert!(presplit(&[], &[7], 3).unwrap()[0].replicas.len() == 1);
}

#[test]
fn presplit_rejects_bad_splits() {
    assert!(presplit(&[table(5), table(5)], &[1], 1).is_err());
    assert!(presplit(&[table(9), table(5)], &[1], 1).is_err());
    assert!(presplit(&[first_user_key()], &[1], 1).is_err());
    assert!(presplit(&[vec![0x05]], &[1], 1).is_err());
    assert!(presplit(&[], &[], 1).is_err());
    assert!(presplit(&[], &[1], 0).is_err());
}

#[test]
fn every_key_routes_to_its_range_and_records_round_trip() {
    let descs = presplit(&[table(10), table(20)], &[1, 2, 3], 3).unwrap();
    let records = directory_records(&descs).unwrap();
    assert_eq!(records.len(), descs.len());
    for (w, d) in records.iter().zip(&descs) {
        assert_eq!(w.space, RangeSpace::Catalog);
        assert_eq!(w.key, record_key(d.end.as_deref()));
        let mut raw = w.key.clone();
        raw.extend_from_slice(&[0xff; 8]);
        let mut value = vec![0];
        value.extend_from_slice(w.value.as_ref().unwrap());
        assert_eq!(decode_record(&raw, &value).unwrap().as_ref(), Some(d));
    }
    let mut cache = RouteCache::default();
    for d in &descs {
        cache.insert(d.clone());
    }
    let probes = [
        (range_key(RangeSpace::Catalog, b"x"), 1),
        (range_key(RangeSpace::Seq, &[1]), 1),
        (table(1), 2),
        (table(9), 2),
        (table(10), 3),
        (range_key(RangeSpace::Rows, &[0, 0, 0, 0, 0, 0, 0, 15, 7]), 3),
        (table(20), 4),
        (range_key(RangeSpace::Index, &[1]), 4),
    ];
    for (key, range) in probes {
        assert_eq!(cache.lookup(&key).unwrap().id, RangeId(range), "{key:02x?}");
    }
}
