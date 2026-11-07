use giggles_engine_bench::{keys, kv, workload};
use std::path::PathBuf;

fn task_dir(label: &str) -> PathBuf {
    let root =
        PathBuf::from(std::env::var_os("TEMP").expect("TEMP must point to the task directory"));
    assert!(
        root.starts_with("J:/") || root.starts_with("J:\\"),
        "tests require task TEMP under J:/"
    );
    let path = root.join(format!(
        "f0-workload-{label}-{}-{}",
        std::process::id(),
        fastrand::u64(..)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn gc_keeps_future_versions_and_one_live_watermark_version() {
    #[cfg(not(feature = "rocksdb"))]
    let engines = ["redb", "fjall"];
    #[cfg(feature = "rocksdb")]
    let engines = ["redb", "fjall", "rocksdb"];
    for engine in engines {
        let dir = task_dir(engine);
        let db = kv::open(engine, &dir, 16 * 1024 * 1024).unwrap();
        let live = keys::encode_user_key(0, 0);
        let dead = keys::encode_user_key(0, 1);
        let ops = vec![
            (
                keys::encode_key(&live, 9),
                Some(keys::encode_value(Some(b"future"))),
            ),
            (
                keys::encode_key(&live, 5),
                Some(keys::encode_value(Some(b"current"))),
            ),
            (
                keys::encode_key(&live, 2),
                Some(keys::encode_value(Some(b"old"))),
            ),
            (
                keys::encode_key(&dead, 8),
                Some(keys::encode_value(Some(b"reborn"))),
            ),
            (keys::encode_key(&dead, 5), Some(keys::encode_value(None))),
            (
                keys::encode_key(&dead, 1),
                Some(keys::encode_value(Some(b"deleted"))),
            ),
        ];
        db.write_batch(&ops, true).unwrap();
        assert_eq!(workload::gc_pass(db.as_ref(), 5).unwrap(), (6, 3));
        assert_eq!(
            keys::get_at(db.as_ref(), &live, 5).unwrap(),
            Some(b"current".to_vec())
        );
        assert_eq!(
            keys::get_at(db.as_ref(), &live, 9).unwrap(),
            Some(b"future".to_vec())
        );
        assert_eq!(keys::get_at(db.as_ref(), &dead, 5).unwrap(), None);
        assert_eq!(
            keys::get_at(db.as_ref(), &dead, 9).unwrap(),
            Some(b"reborn".to_vec())
        );
        assert_eq!(db.iter_from(&[]).unwrap().count(), 3);
        drop(db);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn directory_guard_rejects_workspace_and_resolves_nonexistent_j_child() {
    // A path outside J: must be refused regardless of where the test runs from.
    assert!(workload::checked_directory(std::path::Path::new("C:/"), false).is_err());
    let dir = task_dir("guard");
    let nested = dir.join("new-child");
    let resolved = workload::checked_directory(&nested, false).unwrap();
    assert_eq!(resolved, dir.canonicalize().unwrap().join("new-child"));
    assert!(workload::checked_directory(&std::env::current_dir().unwrap(), true).is_ok());
    std::fs::remove_dir_all(dir).unwrap();
}
