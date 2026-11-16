//! R5-04a: per-interval SST export/ingest on `RocksKv`.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "kv-rocksdb-sst-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn open(path: &Path) -> RocksKv {
    RocksKv::open(path, 8 * 1024 * 1024).unwrap()
}

fn key(prefix: u8, n: u64) -> Vec<u8> {
    let mut key = vec![prefix];
    key.extend_from_slice(&n.to_be_bytes());
    key
}

fn interval(read: &dyn KvRead, ks: Keyspace, start: &OwnedBound, end: &OwnedBound) -> Vec<KvPair> {
    read.range(
        ks,
        start.as_ref().map(Vec::as_slice),
        end.as_ref().map(Vec::as_slice),
        Direction::Forward,
    )
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

fn all(read: &dyn KvRead, ks: Keyspace) -> Vec<KvPair> {
    interval(read, ks, &Bound::Unbounded, &Bound::Unbounded)
}

fn put(db: &RocksKv, ks: Keyspace, pairs: &[(Vec<u8>, &[u8])]) {
    let mut batch = WriteBatch::default();
    for (key, value) in pairs {
        batch.put(ks, key.clone(), value.to_vec());
    }
    db.write(batch, Durability::NoSync).unwrap();
}

/// Intervals covering every bound shape; `0x7f` keys stay outside all of them.
fn parts() -> Vec<SstPart> {
    vec![
        (
            Keyspace::Rows,
            Bound::Included(vec![0x40]),
            Bound::Excluded(vec![0x50]),
        ),
        (
            Keyspace::Index,
            Bound::Excluded(key(0x40, 10)),
            Bound::Included(key(0x40, 200)),
        ),
        (
            Keyspace::Meta,
            Bound::Included(vec![0x90]),
            Bound::Unbounded,
        ),
        (Keyspace::Gc, Bound::Unbounded, Bound::Excluded(vec![0x10])),
        // Empty at the source.
        (
            Keyspace::Kernel,
            Bound::Included(vec![0x40]),
            Bound::Excluded(vec![0x50]),
        ),
    ]
}

fn installs(export: &SstExport<impl Sized>, parts: &[SstPart]) -> Vec<SstInstall> {
    parts
        .iter()
        .zip(&export.files)
        .map(|((ks, start, end), file)| {
            assert_eq!(file.ks, *ks);
            (*ks, start.clone(), end.clone(), file.path.clone())
        })
        .collect()
}

#[test]
fn export_under_concurrent_writes_matches_snapshot_and_ingest_replaces_intervals() {
    let dir = TestDir::new();
    let source = Arc::new(open(&dir.0.join("source")));
    for n in 0..300 {
        put(
            &source,
            Keyspace::Rows,
            &[(key(0x40, n), b"r0"), (key(0x7f, n), b"out")],
        );
        put(&source, Keyspace::Index, &[(key(0x40, n), b"i0")]);
        put(
            &source,
            Keyspace::Meta,
            &[(key(0x90, n), b"m0"), (key(0x20, n), b"out")],
        );
        put(
            &source,
            Keyspace::Gc,
            &[(key(0x05, n), b"g0"), (key(0x7f, n), b"out")],
        );
    }
    put(
        &source,
        Keyspace::Seq,
        &[(b"applied".to_vec(), &0u64.to_be_bytes())],
    );
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (db, stop) = (Arc::clone(&source), Arc::clone(&stop));
        thread::spawn(move || {
            let mut i = 1u64;
            while !stop.load(Ordering::Relaxed) || i < 200 {
                let value = i.to_be_bytes();
                let mut batch = WriteBatch::default();
                for ks in [
                    Keyspace::Rows,
                    Keyspace::Index,
                    Keyspace::Meta,
                    Keyspace::Gc,
                ] {
                    let prefix = if ks == Keyspace::Gc {
                        0x05
                    } else if ks == Keyspace::Meta {
                        0x90
                    } else {
                        0x40
                    };
                    batch.put(ks, key(prefix, i % 400), value.to_vec());
                    batch.delete(ks, key(prefix, (i * 7) % 300));
                }
                batch.put(Keyspace::Seq, b"applied".to_vec(), value.to_vec());
                db.write(batch, Durability::NoSync).unwrap();
                i += 1;
            }
        })
    };
    while source.get(Keyspace::Seq, b"applied").unwrap() == Some(0u64.to_be_bytes().to_vec()) {
        thread::yield_now();
    }
    let parts = parts();
    let export = source
        .export_sst(&dir.0.join("sst"), &parts, |read| {
            // The applied marker plus a full copy of every exported interval.
            let applied = read.get(Keyspace::Seq, b"applied")?;
            let copies: Vec<_> = parts
                .iter()
                .map(|(ks, s, e)| interval(read, *ks, s, e))
                .collect();
            Ok((applied, copies))
        })
        .unwrap();
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    let (applied, expected) = &export.at_snapshot;
    assert!(applied.is_some());
    assert_eq!(export.files.len(), parts.len());
    for (file, rows) in export.files.iter().zip(expected) {
        assert_eq!(file.keys, rows.len() as u64);
        assert_eq!(file.path.is_some(), !rows.is_empty());
        assert_eq!(file.bytes > 0, !rows.is_empty());
    }
    assert!(export.files[4].path.is_none(), "kernel part must be empty");
    assert!(export.files[..4].iter().all(|file| file.keys > 0));

    // Destination: stale and extra keys inside the intervals, sentinels outside.
    let target = open(&dir.0.join("target"));
    for n in 0..500 {
        put(
            &target,
            Keyspace::Rows,
            &[(key(0x40, n), b"stale"), (key(0x4f, n), b"extra")],
        );
        put(&target, Keyspace::Index, &[(key(0x40, n), b"stale")]);
        put(&target, Keyspace::Meta, &[(key(0xff, n), b"extra")]);
        put(&target, Keyspace::Gc, &[(key(0x00, n), b"extra")]);
        put(&target, Keyspace::Kernel, &[(key(0x41, n), b"extra")]);
    }
    let outside: [(Keyspace, Vec<u8>); 7] = [
        (Keyspace::Rows, vec![0x50]),
        (Keyspace::Rows, vec![0x3f, 0xff]),
        (Keyspace::Index, key(0x40, 10)),
        (Keyspace::Index, successor(&key(0x40, 200))),
        (Keyspace::Meta, vec![0x8f, 0xff]),
        (Keyspace::Gc, vec![0x10]),
        (Keyspace::Kernel, vec![0x50]),
    ];
    for (ks, sentinel) in &outside {
        put(&target, *ks, &[(sentinel.clone(), b"sentinel")]);
    }
    let installs = installs(&export, &parts);
    for round in 0..2 {
        // The second round checks idempotence of a repeated install.
        target.ingest_sst(&installs).unwrap();
        for ((ks, start, end), rows) in parts.iter().zip(expected) {
            assert_eq!(
                &interval(&target, *ks, start, end),
                rows,
                "round {round}, {ks:?}"
            );
        }
        for (ks, sentinel) in &outside {
            assert_eq!(
                target.get(*ks, sentinel).unwrap().as_deref(),
                Some(&b"sentinel"[..])
            );
        }
    }
    for (_, _, _, path) in &installs {
        if let Some(path) = path {
            assert!(path.exists(), "move_files=false keeps {path:?}");
        }
    }
    // Keyspaces outside every part keep only their own keys.
    assert!(all(&target, Keyspace::Seq).is_empty());
    assert_eq!(
        all(&target, Keyspace::Kernel),
        vec![(vec![0x50], b"sentinel".to_vec())]
    );
}

#[test]
fn empty_export_and_empty_ingest() {
    let dir = TestDir::new();
    let source = open(&dir.0.join("source"));
    put(&source, Keyspace::Rows, &[(key(0x10, 1), b"outside")]);
    let parts = vec![
        (
            Keyspace::Rows,
            Bound::Included(vec![0x40]),
            Bound::Unbounded,
        ),
        (Keyspace::Catalog, Bound::Unbounded, Bound::Unbounded),
        (
            Keyspace::Rows,
            Bound::Excluded(vec![0x41]),
            Bound::Excluded(vec![0x41]),
        ),
    ];
    let export = source
        .export_sst(&dir.0.join("sst"), &parts, |_| Ok(()))
        .unwrap();
    assert!(
        export
            .files
            .iter()
            .all(|f| f.path.is_none() && f.keys == 0 && f.bytes == 0)
    );

    let target = open(&dir.0.join("target"));
    put(
        &target,
        Keyspace::Rows,
        &[(key(0x40, 1), b"stale"), (key(0x10, 1), b"kept")],
    );
    put(
        &target,
        Keyspace::Catalog,
        &[(Vec::new(), b"empty-key"), (vec![0xff; 9], b"top")],
    );
    let installs = installs(&export, &parts);
    target.ingest_sst(&installs).unwrap();
    target.ingest_sst(&installs).unwrap();
    assert_eq!(
        all(&target, Keyspace::Rows),
        vec![(key(0x10, 1), b"kept".to_vec())]
    );
    assert!(all(&target, Keyspace::Catalog).is_empty());
    // Installing into an empty CF with an unbounded end is a no-op.
    target.ingest_sst(&installs).unwrap();
}
