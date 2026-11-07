#![cfg(feature = "rocksdb")]

use giggles_engine_bench::kv;

#[test]
fn snapshot_pin_outlives_backend_and_releases_database_lock() -> kv::Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "rocksdb-pin-{}-{}",
        std::process::id(),
        fastrand::u64(..)
    ));
    std::fs::create_dir_all(&dir)?;
    // Match workload::checked_directory, including Windows verbatim disk paths.
    let dir = dir.canonicalize()?;
    let backend = kv::open("rocksdb", &dir, 8 * 1024 * 1024)?;
    backend.write_batch(&[(b"key".to_vec(), Some(b"old".to_vec()))], true)?;
    let pin = backend.pin_snapshot()?.expect("native snapshot pin");
    backend.write_batch(&[(b"key".to_vec(), Some(b"new".to_vec()))], false)?;
    backend.flush()?;
    assert!(backend.supports_compaction());
    backend.compact()?;
    drop(backend);
    // The real snapshot must keep its database alive, including the native lock.
    assert!(kv::open("rocksdb", &dir, 8 * 1024 * 1024).is_err());
    drop(pin);
    let reopened = kv::open("rocksdb", &dir, 8 * 1024 * 1024)?;
    assert_eq!(
        reopened.seek_first_at_or_after(b"key")?,
        Some((b"key".to_vec(), b"new".to_vec()))
    );
    drop(reopened);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
