//! Group identity (GD-0075 R5-10): a log, entry or snapshot of one group is
//! rejected by every other group where R4 rejects another cluster's.
use giggles_replication::{
    entry::{self, EntryError, EntryKind},
    error::Error,
    group::{group_cluster, group_of},
    log_store::LogStore,
    snapshot::Manifest,
};

const BASE: [u8; 16] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
];

#[test]
fn group_zero_is_the_database_cluster_and_ids_round_trip() {
    assert_eq!(group_cluster(BASE, 0), BASE);
    for group in [0, 1, 2, 7, 1 << 40, u64::MAX] {
        let id = group_cluster(BASE, group);
        assert_eq!(id[..8], BASE[..8]);
        assert_eq!(group_of(BASE, id), Some(group));
    }
    assert_ne!(group_cluster(BASE, 1), group_cluster(BASE, 2));
    let mut other = BASE;
    other[0] ^= 1;
    assert_eq!(group_of(BASE, group_cluster(other, 1)), None);
}

#[test]
fn another_groups_entry_is_rejected() {
    let bytes = entry::encode(group_cluster(BASE, 1), EntryKind::Batch, b"x").unwrap();
    assert_eq!(
        entry::decode(group_cluster(BASE, 1), &bytes).unwrap(),
        (EntryKind::Batch, b"x".as_slice())
    );
    for group in [0, 2] {
        assert_eq!(
            entry::decode(group_cluster(BASE, group), &bytes),
            Err(EntryError::WrongCluster)
        );
    }
}

#[test]
fn another_groups_log_and_snapshot_are_rejected() {
    let dir = std::env::temp_dir().join(format!("r5-group-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    drop(LogStore::open(&dir, group_cluster(BASE, 3), 1).unwrap());
    assert!(LogStore::open(&dir, group_cluster(BASE, 3), 1).is_ok());
    for group in [0, 4] {
        assert!(matches!(
            LogStore::open(&dir, group_cluster(BASE, group), 1),
            Err(Error::Identity(_))
        ));
    }
    std::fs::remove_dir_all(&dir).unwrap();

    let manifest = Manifest {
        cluster: group_cluster(BASE, 5),
        index: 9,
        term: 2,
        conf: Default::default(),
        membership: Vec::new(),
        files: Vec::new(),
    };
    let bytes = manifest.encode();
    assert_eq!(Manifest::decode(group_cluster(BASE, 5), &bytes).unwrap(), manifest);
    assert!(matches!(
        Manifest::decode(group_cluster(BASE, 6), &bytes),
        Err(Error::Identity(_))
    ));
}
