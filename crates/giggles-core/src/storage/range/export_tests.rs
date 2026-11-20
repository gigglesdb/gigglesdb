use super::*;
use crate::storage::range::store::{
    RangeApplyOutcome,
    tests::{Dir, SPLIT_TABLE, descriptors, pos, row, rows, two_ranges},
};
use std::sync::Arc;

const B: RangeId = RangeId(2);

fn batch(n: u64) -> Vec<u8> {
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Rows,
        row(SPLIT_TABLE, n, 1),
        n.to_be_bytes().to_vec(),
    );
    batch.put(Keyspace::Index, row(SPLIT_TABLE + 5, n, 1), b"i".to_vec());
    batch.put(
        Keyspace::Meta,
        range_local_key(B, &RangeLocalKey::LastCommitTs),
        n.to_be_bytes().to_vec(),
    );
    batch.encode()
}

/// The range's data and local keys (Meta keys of other ranges and node keys excluded).
type Contents = Vec<(Keyspace, Vec<(Vec<u8>, Vec<u8>)>)>;

fn range_contents(store: &RangeStore, range: RangeId) -> Contents {
    let descriptor = store.descriptor(range).unwrap().unwrap();
    let (lo, hi) = local_interval(range);
    Keyspace::ALL
        .into_iter()
        .map(|ks| {
            let rows = rows(store, ks)
                .into_iter()
                .filter(|(key, _)| match ks {
                    Keyspace::Meta => key >= &lo && key < &hi,
                    _ => super::super::keys::contains_raw(&descriptor, ks, key).unwrap(),
                })
                .collect();
            (ks, rows)
        })
        .collect()
}

/// A destination holding stale data in range 2 and data of range 1 that must survive.
fn stale_destination(dir: &Dir) -> RangeStore {
    let dest = RangeStore::create(dir.sub("dest"), [7; 16]).unwrap();
    for d in descriptors() {
        dest.create_range(&d).unwrap();
    }
    for n in 1..=3 {
        let mut stale = WriteBatch::default();
        stale.put(
            Keyspace::Rows,
            row(SPLIT_TABLE, 100 + n, 1),
            b"stale".to_vec(),
        );
        stale.put(Keyspace::Rows, row(SPLIT_TABLE, n, 1), b"old".to_vec());
        stale.put(
            Keyspace::Meta,
            range_local_key(B, &RangeLocalKey::Prepared(n)),
            crate::storage::range::engine::prepared::Prepared {
                home: RangeId(2),
                at: crate::storage::engine::hlc::Ts(1),
                writes: Default::default(),
                locks: vec![],
                children: vec![],
            }
            .encode()
            .unwrap(),
        );
        dest.apply(B, &stale.encode(), pos(n, 1)).unwrap();
        let mut keep = WriteBatch::default();
        keep.put(Keyspace::Rows, row(SPLIT_TABLE - 1, n, 1), b"keep".to_vec());
        dest.apply(RangeId(1), &keep.encode(), pos(n, 1)).unwrap();
    }
    dest
}

#[test]
fn export_during_writes_installs_exactly_its_position() {
    let dir = Dir::new("range-export");
    let source = Arc::new(two_ranges(&dir));
    for n in 1..=50 {
        source.apply(B, &batch(n), pos(n, 1)).unwrap();
    }
    let writer = {
        let source = source.clone();
        std::thread::spawn(move || {
            for n in 51..=400 {
                assert_eq!(
                    source.apply(B, &batch(n), pos(n, 2)).unwrap(),
                    RangeApplyOutcome::Applied
                );
            }
        })
    };
    let at = source.export(B, &dir.sub("snap")).unwrap();
    writer.join().unwrap();
    assert!(at.index >= 50 && at.index <= 400);
    let dest = stale_destination(&dir);
    let kept = rows(&dest, Keyspace::Rows)
        .into_iter()
        .filter(|(k, _)| k < &row(SPLIT_TABLE, 0, 0))
        .collect::<Vec<_>>();
    assert_eq!(dest.install(B, &dir.sub("snap")).unwrap(), at);
    assert_eq!(dest.applied(B).unwrap(), Some(at));
    // Expected: replay 1..=at into a fresh store.
    let expect = RangeStore::create(dir.sub("expect"), [7; 16]).unwrap();
    for d in descriptors() {
        expect.create_range(&d).unwrap();
    }
    for n in 1..=at.index {
        expect
            .apply(B, &batch(n), pos(n, if n <= 50 { 1 } else { 2 }))
            .unwrap();
    }
    assert_eq!(range_contents(&dest, B), range_contents(&expect, B));
    let still = rows(&dest, Keyspace::Rows)
        .into_iter()
        .filter(|(k, _)| k < &row(SPLIT_TABLE, 0, 0))
        .collect::<Vec<_>>();
    assert_eq!(still, kept);
    // Installing twice converges; the source files stay (move_files=false).
    dest.install(B, &dir.sub("snap")).unwrap();
    assert_eq!(range_contents(&dest, B), range_contents(&expect, B));
    assert!(dir.sub("snap").join(MANIFEST).is_file());
    // The installed range keeps applying after its position.
    let next = pos(at.index + 1, 3);
    assert_eq!(
        dest.apply(B, &batch(at.index + 1), next).unwrap(),
        RangeApplyOutcome::Applied
    );
    drop(dest);
    RangeStore::open(dir.sub("dest")).unwrap();
}

#[test]
fn manifest_round_trips_and_rejects_damage() {
    let dir = Dir::new("range-export");
    let source = two_ranges(&dir);
    for n in 1..=3 {
        source.apply(B, &batch(n), pos(n, 1)).unwrap();
    }
    source.export(B, &dir.sub("snap")).unwrap();
    let bytes = std::fs::read(dir.sub("snap").join(MANIFEST)).unwrap();
    let manifest = Manifest::decode(&bytes).unwrap();
    assert_eq!(manifest.encode(), bytes);
    assert_eq!(manifest.applied, pos(3, 1));
    for cut in [0, 1, 9, bytes.len() - 1] {
        assert!(Manifest::decode(&bytes[..cut]).is_err(), "cut {cut}");
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(Manifest::decode(&trailing).is_err());
    let mut version = bytes.clone();
    version[0] = 2;
    assert!(Manifest::decode(&version).is_err());
    let mut wrong = manifest.clone();
    wrong.applied = pos(9, 9);
    assert!(Manifest::decode(&wrong.encode()).is_err());
    let mut parts = manifest.clone();
    parts.parts.pop();
    assert!(Manifest::decode(&parts.encode()).is_err());
    // An export of range 2 is not installed as range 1, and a missing file is refused.
    let dest_dir = Dir::new("range-export-d");
    let dest = two_ranges(&dest_dir);
    assert!(dest.install(RangeId(1), &dir.sub("snap")).is_err());
    let file = manifest.parts.iter().find_map(|p| p.file.clone()).unwrap();
    std::fs::remove_file(dir.sub("snap").join(file)).unwrap();
    assert!(dest.install(B, &dir.sub("snap")).is_err());
    let marker = encode_marker(&descriptors()[1], &dir.sub("snap")).unwrap();
    assert_eq!(
        decode_marker(&marker).unwrap(),
        (descriptors()[1].clone(), dir.sub("snap"))
    );
    assert!(decode_marker(&marker[..marker.len() - 1]).is_err());
}

#[test]
fn a_crash_at_each_install_step_recovers_to_a_consistent_range() {
    let dir = Dir::new("range-export");
    let source = two_ranges(&dir);
    for n in 1..=20 {
        source.apply(B, &batch(n), pos(n, 1)).unwrap();
    }
    let at = source.export(B, &dir.sub("snap")).unwrap();
    let expect = range_contents(&source, B);
    for (i, step) in [Step::Marker, Step::FirstPart, Step::Data]
        .into_iter()
        .enumerate()
    {
        // Source intact: open completes the install.
        let sub = Dir::new(&format!("range-crash-{i}"));
        let dest = stale_destination(&sub);
        dest.install_until(B, &dir.sub("snap"), step).unwrap();
        assert!(matches!(
            dest.apply(B, &batch(99), pos(4, 1)),
            Err(CoreError::Corrupt(_))
        ));
        drop(dest);
        let dest = RangeStore::open(sub.sub("dest")).unwrap();
        assert_eq!(dest.applied(B).unwrap(), Some(at), "{step:?}");
        assert_eq!(range_contents(&dest, B), expect, "{step:?}");
        drop(dest);
        // Source gone: open rolls the range back to empty with its descriptor kept.
        let sub = Dir::new(&format!("range-crash-gone-{i}"));
        let copy = sub.sub("snap");
        std::fs::create_dir_all(&copy).unwrap();
        for entry in std::fs::read_dir(dir.sub("snap")).unwrap() {
            let entry = entry.unwrap();
            std::fs::copy(entry.path(), copy.join(entry.file_name())).unwrap();
        }
        let dest = stale_destination(&sub);
        dest.install_until(B, &copy, step).unwrap();
        drop(dest);
        std::fs::remove_dir_all(&copy).unwrap();
        let dest = RangeStore::open(sub.sub("dest")).unwrap();
        assert_eq!(dest.applied(B).unwrap(), None, "{step:?}");
        assert_eq!(dest.descriptor(B).unwrap(), Some(descriptors()[1].clone()));
        let left = range_contents(&dest, B);
        assert!(
            left.iter().all(|(ks, rows)| match ks {
                Keyspace::Meta => rows.len() == 2, // descriptor + needs-snapshot marker
                _ => rows.is_empty(),
            }),
            "{step:?}: {left:?}"
        );
        assert_eq!(rows(&dest, Keyspace::Rows).len(), 3, "range 1 rows survive");
        // The rolled-back range applies nothing until a snapshot is installed, also after reopen.
        assert!(matches!(
            dest.apply(B, &batch(1), pos(1, 1)),
            Err(CoreError::Corrupt(_))
        ));
        assert!(dest.export(B, &sub.sub("again")).is_err());
        drop(dest);
        let dest = RangeStore::open(sub.sub("dest")).unwrap();
        assert!(matches!(
            dest.apply(B, &batch(1), pos(1, 1)),
            Err(CoreError::Corrupt(_))
        ));
        assert_eq!(dest.install(B, &dir.sub("snap")).unwrap(), at);
        assert_eq!(range_contents(&dest, B), expect, "{step:?} reinstalled");
    }
}

#[test]
fn install_fences_refuse_before_writing() {
    let dir = Dir::new("range-export");
    let source = two_ranges(&dir);
    for n in 1..=5 {
        source.apply(B, &batch(n), pos(n, 1)).unwrap();
    }
    source.export(B, &dir.sub("snap")).unwrap();
    // A reused, non-empty export directory is refused.
    assert!(source.export(B, &dir.sub("snap")).is_err());
    let marker = range_local_key(B, &RangeLocalKey::Install);
    let refused = |dest: &RangeStore| {
        assert!(dest.install(B, &dir.sub("snap")).is_err());
        assert_eq!(dest.kv().get(Keyspace::Meta, &marker).unwrap(), None);
    };
    // Behind the destination's position.
    let ahead_dir = Dir::new("range-fence-a");
    let ahead = two_ranges(&ahead_dir);
    for n in 1..=6 {
        ahead.apply(B, &batch(n), pos(n, 1)).unwrap();
    }
    refused(&ahead);
    // Another database.
    let other_dir = Dir::new("range-fence-b");
    let other = RangeStore::create(other_dir.sub("db"), [8; 16]).unwrap();
    for d in descriptors() {
        other.create_range(&d).unwrap();
    }
    refused(&other);
    // Overlaps a range stored here: range 1 widened to MAX.
    let fence = Dir::new("range-fence-c");
    let wide = RangeStore::create(fence.sub("db"), [7; 16]).unwrap();
    let mut first = descriptors()[0].clone();
    first.end = None;
    wide.create_range(&first).unwrap();
    wide.create_range(&descriptors()[1]).unwrap();
    refused(&wide);
    // A destination at the same position takes it.
    let same_dir = Dir::new("range-fence-d");
    let same = two_ranges(&same_dir);
    for n in 1..=5 {
        same.apply(B, &batch(n), pos(n, 1)).unwrap();
    }
    assert_eq!(same.install(B, &dir.sub("snap")).unwrap(), pos(5, 1));
}

#[test]
fn snapshot_wide_rows_require_manifest_bit_before_target_changes() {
    use crate::storage::engine::{codec, meta::WIDE_ROWS, mvcc};
    let dir = Dir::new("range-snapshot-wide");
    let source = two_ranges(&dir);
    source.apply_noop(B, pos(3, 1)).unwrap_err(); // positions cannot skip
    for n in 1..=3 {
        source.apply_noop(B, pos(n, 1)).unwrap();
    }
    let _budget =
        crate::relational::ValueBudget::enter(crate::relational::DEFAULT_MAX_VALUE_BYTES, true);
    let wide = codec::encode_row(
        b"k",
        1,
        &[crate::relational::Value::Text("x".repeat(70_000))],
    )
    .unwrap();
    // Bypass apply to model a snapshot produced by an unprotected old receiver.
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Rows,
        row(SPLIT_TABLE, 9, 1),
        mvcc::encode_live(&wide),
    );
    source.kv().write(batch, Durability::Sync).unwrap();
    let snap = dir.sub("wide-snap");
    source.export(B, &snap).unwrap();
    let dest = stale_destination(&dir);
    let before: Vec<_> = Keyspace::ALL
        .into_iter()
        .map(|ks| rows(&dest, ks))
        .collect();
    assert!(matches!(dest.install(B, &snap), Err(CoreError::Corrupt(_))));
    assert_eq!(
        Keyspace::ALL
            .into_iter()
            .map(|ks| rows(&dest, ks))
            .collect::<Vec<_>>(),
        before
    );
    assert!(!dest.needs_snapshot(B).unwrap());
    // Model a crash from an older receiver that wrote its install marker
    // without validating the unflagged oversized Rows SST.
    let recovery_dir = Dir::new("range-snapshot-wide-recovery");
    let recovery = stale_destination(&recovery_dir);
    let recovery_manifest = Manifest::decode(&std::fs::read(snap.join(MANIFEST)).unwrap()).unwrap();
    let source_files: Vec<_> = recovery_manifest.parts.iter()
        .filter_map(|p| p.file.as_ref().map(|f| snap.join(f))).collect();
    let mut marker = WriteBatch::default();
    marker.put(Keyspace::Meta, range_local_key(B, &RangeLocalKey::Install),
        encode_marker(&descriptors()[1], &snap).unwrap());
    recovery.kv().write(marker, Durability::Sync).unwrap();
    drop(recovery);
    CHECK_SCRATCH_PATHS.with_borrow_mut(Vec::clear);
    let recovery = RangeStore::open(recovery_dir.sub("dest")).unwrap();
    assert!(recovery.needs_snapshot(B).unwrap());
    assert_eq!(recovery.applied(B).unwrap(), None);
    assert!(recovery.apply(B, &self::batch(1), pos(1, 1)).is_err());
    CHECK_SCRATCH_PATHS.with_borrow_mut(|paths| {
        assert!(!paths.is_empty());
        assert!(paths.iter().all(|path| !path.exists()));
        paths.clear();
    });
    assert!(!source_files.is_empty());
    assert!(source_files.iter().all(|path| path.is_file()));
    let bytes = std::fs::read(snap.join(MANIFEST)).unwrap();
    let mut manifest = Manifest::decode(&bytes).unwrap();
    let mut values = std::collections::BTreeMap::new();
    for op in manifest.meta.ops() {
        if let Op::Put { key, value, .. } = op {
            values.insert(key.clone(), value.clone());
        }
    }
    let features = values
        .entry(range_local_key(B, &RangeLocalKey::Features))
        .or_insert_with(|| 0u64.to_be_bytes().to_vec());
    *features = (u64::from_be_bytes(features.as_slice().try_into().unwrap()) | WIDE_ROWS)
        .to_be_bytes()
        .to_vec();
    let mut meta = WriteBatch::default();
    for (key, value) in values {
        meta.put(Keyspace::Meta, key, value);
    }
    manifest.meta = meta;
    write_synced(&snap.join(MANIFEST), &manifest.encode()).unwrap();
    dest.install(B, &snap).unwrap();
    assert!(
        rows(&dest, Keyspace::Rows)
            .iter()
            .any(|(_, v)| v == &mvcc::encode_live(&wide))
    );
    let bits = dest.kv().get(Keyspace::Meta, b"features").unwrap().unwrap();
    assert_ne!(u64::from_be_bytes(bits.try_into().unwrap()) & WIDE_ROWS, 0);
}
