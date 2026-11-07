use giggles_engine_bench::{keys::*, kv};
use std::collections::BTreeMap;

#[test]
fn signed_and_timestamp_ordering() {
    let mut rng = fastrand::Rng::with_seed(1729);
    let mut pks = vec![i64::MIN, i64::MAX, -1, 0, 1];
    pks.extend((0..1000).map(|_| rng.i64(..)));
    let mut encoded: Vec<_> = pks.iter().map(|&p| encode_user_key(19, p)).collect();
    pks.sort();
    encoded.sort();
    assert_eq!(
        encoded,
        pks.iter()
            .map(|&p| encode_user_key(19, p))
            .collect::<Vec<_>>()
    );
    for pk in pks {
        let user = encode_user_key(19, pk);
        let mut timestamps = vec![0, 1, u64::MAX];
        timestamps.extend((0..8).map(|_| rng.u64(..)));
        let mut full: Vec<_> = timestamps.iter().map(|&t| encode_key(&user, t)).collect();
        full.sort();
        timestamps.sort_by(|a, b| b.cmp(a));
        assert_eq!(
            full.iter()
                .map(|k| decode_key(k).unwrap().1)
                .collect::<Vec<_>>(),
            timestamps
        );
    }
    let mut tuples = Vec::new();
    for table in [0, 1, u64::MAX] {
        for pk in [i64::MIN, -1, 0, 1, i64::MAX] {
            for ts in [0, 1, u64::MAX] {
                tuples.push((table, pk, ts));
            }
        }
    }
    tuples.extend((0..2000).map(|_| (rng.u64(..), rng.i64(..), rng.u64(..))));
    rng.shuffle(&mut tuples);
    let mut encoded: Vec<_> = tuples
        .iter()
        .map(|&(t, p, s)| encode_key(&encode_user_key(t, p), s))
        .collect();
    tuples.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(b.2.cmp(&a.2)));
    encoded.sort();
    assert_eq!(
        encoded,
        tuples
            .iter()
            .map(|&(t, p, s)| encode_key(&encode_user_key(t, p), s))
            .collect::<Vec<_>>()
    );
}

#[test]
fn randomized_mvcc_matches_reference_backends() -> kv::Result<()> {
    #[cfg(not(feature = "rocksdb"))]
    let engines = ["redb", "fjall"];
    #[cfg(feature = "rocksdb")]
    let engines = ["redb", "fjall", "rocksdb"];
    for engine in engines {
        let dir = std::env::temp_dir().join(format!(
            "keys-{engine}-{}-{}",
            std::process::id(),
            fastrand::u64(..)
        ));
        std::fs::create_dir_all(&dir)?;
        let backend = kv::open(engine, &dir, 8 * 1024 * 1024)?;
        let mut rng = fastrand::Rng::with_seed(4721);
        let mut reference: BTreeMap<[u8; 17], BTreeMap<u64, Option<Vec<u8>>>> = BTreeMap::new();
        let mut ops = Vec::new();
        for ts in 0u64..600 {
            let user = encode_user_key(rng.u64(0..3), rng.i64(-15..16));
            let value = if rng.u8(0..4) == 0 {
                None
            } else {
                Some(ts.to_be_bytes().to_vec())
            };
            reference.entry(user).or_default().insert(ts, value.clone());
            ops.push((encode_key(&user, ts), Some(encode_value(value.as_deref()))));
        }
        rng.shuffle(&mut ops);
        backend.write_batch(&ops, true)?;
        let mut snapshots = vec![0, 1, 10, 100, 300, 599, u64::MAX];
        snapshots.extend((0..20).map(|_| rng.u64(0..650)));
        for snapshot in snapshots {
            for (&user, versions) in &reference {
                let expected = versions
                    .range(..=snapshot)
                    .next_back()
                    .and_then(|(_, v)| v.clone());
                assert_eq!(
                    get_at(backend.as_ref(), &user, snapshot)?,
                    expected,
                    "{engine} {snapshot}"
                );
            }
            for _ in 0..20 {
                let start = encode_user_key(rng.u64(0..3), rng.i64(-18..19));
                let limit = rng.usize(0..40);
                let expected: Vec<_> = reference
                    .range(start..)
                    .filter_map(|(user, v)| {
                        v.range(..=snapshot)
                            .next_back()
                            .and_then(|(_, v)| v.clone())
                            .map(|v| (user.to_vec(), v))
                    })
                    .take(limit)
                    .collect();
                assert_eq!(
                    scan_at(backend.as_ref(), &start, limit, snapshot)?,
                    expected,
                    "{engine} {snapshot}"
                );
            }
        }
        backend.flush()?;
        drop(backend);
        let backend = kv::open(engine, &dir, 8 * 1024 * 1024)?;
        assert_eq!(backend.iter_from(&[])?.count(), ops.len());
        let pin = backend.pin_snapshot()?;
        let mut old_iter = backend.iter_from(&[])?;
        let first = old_iter.next().transpose()?.unwrap();
        backend.write_batch(&[(first.0.clone(), None)], true)?;
        // The existing streaming read retains its native engine snapshot.
        assert_eq!(old_iter.count() + 1, ops.len());
        assert_eq!(backend.iter_from(&[])?.count(), ops.len() - 1);
        drop(pin);
        backend.compact()?;
        assert_eq!(backend.iter_from(&[])?.count(), ops.len() - 1);
        backend.flush()?;
        drop(backend);
        let backend = kv::open(engine, &dir, 8 * 1024 * 1024)?;
        assert_eq!(backend.iter_from(&[])?.count(), ops.len() - 1);
        drop(backend);
        std::fs::remove_dir_all(&dir)?;
    }
    Ok(())
}
