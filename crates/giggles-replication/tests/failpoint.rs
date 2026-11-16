use giggles_replication::{failpoint, log_store::LogStore};
use raft::{Storage, eraftpb::Entry};
use std::panic::{AssertUnwindSafe, catch_unwind};

#[test]
fn persist_crashes_respect_commit_boundary() {
    let cluster = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_be_bytes();
    let dir = std::env::temp_dir().join(format!(
        "failpoint-{}-{:x}",
        std::process::id(),
        u128::from_be_bytes(cluster)
    ));
    let entry = Entry {
        index: 1,
        term: 3,
        ..Default::default()
    };
    for (point, committed) in [("log.persist.before", false), ("log.persist.after", true)] {
        let log = LogStore::open(&dir, cluster, 1).unwrap();
        failpoint::arm(cluster, point, 1);
        assert!(
            catch_unwind(AssertUnwindSafe(|| log
                .persist(std::slice::from_ref(&entry), None)
                .unwrap()))
            .is_err()
        );
        failpoint::disarm(cluster);
        assert_eq!(failpoint::hits(cluster, point), 1);
        drop(log);
        let reopened = LogStore::open(&dir, cluster, 1).unwrap();
        assert_eq!(reopened.last_index().unwrap(), u64::from(committed));
        if committed {
            assert_eq!(reopened.term(1).unwrap(), 3);
        }
    }
    assert_eq!(
        failpoint::fired(cluster),
        vec![
            ("log.persist.before", Some(1)),
            ("log.persist.after", Some(1))
        ]
    );
    std::fs::remove_dir_all(dir).unwrap();
}
