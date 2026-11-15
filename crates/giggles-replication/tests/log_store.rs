use giggles_replication::{error::Error, log_store::LogStore};
use protobuf::Message;
use raft::{GetEntriesContext, Storage, StorageError, eraftpb::*};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "log-store-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
    fn open(&self) -> LogStore {
        LogStore::open(&self.0, [7; 16], 1).unwrap()
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn entry(index: u64, term: u64) -> Entry {
    Entry {
        index,
        term,
        ..Default::default()
    }
}
fn entries(s: &LogStore, low: u64, high: u64, max: Option<u64>) -> raft::Result<Vec<Entry>> {
    s.entries(low, high, max, GetEntriesContext::empty(false))
}
#[test]
fn reopen_and_identity() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<LogStore>();
    let dir = Temp::new();
    let s = dir.open();
    let mut hs = HardState {
        term: 3,
        vote: 2,
        commit: 1,
        ..Default::default()
    };
    let mut cs = ConfState::default();
    cs.set_voters(vec![1, 2, 3]);
    s.persist(&[entry(1, 3)], Some(&hs)).unwrap();
    s.set_conf_state(&cs).unwrap();
    drop(s);
    for (cluster, node) in [([8; 16], 1), ([7; 16], 2)] {
        assert!(matches!(
            LogStore::open(&dir.0, cluster, node),
            Err(Error::Identity(_))
        ));
    }
    let s = dir.open();
    assert_eq!(s.identity(), ([7; 16], 1));
    assert_eq!(s.hard_state().unwrap(), hs);
    assert_eq!(s.conf_state().unwrap(), cs);
    assert_eq!(s.initial_state().unwrap().hard_state, hs);
    assert_eq!(s.initial_state().unwrap().conf_state, cs);
    assert_eq!(entries(&s, 1, 2, None).unwrap(), vec![entry(1, 3)]);
    hs.term = 4;
    s.set_hard_state(&hs).unwrap();
    drop(s);
    assert_eq!(dir.open().hard_state().unwrap(), hs);
}
#[test]
fn replacement_and_gap_are_atomic() {
    let dir = Temp::new();
    let s = dir.open();
    s.append(&[entry(1, 1), entry(2, 1), entry(3, 1)]).unwrap();
    s.append(&[entry(2, 2)]).unwrap();
    assert_eq!(
        entries(&s, 1, 3, None).unwrap(),
        vec![entry(1, 1), entry(2, 2)]
    );
    let hs = HardState {
        term: 9,
        ..Default::default()
    };
    for batch in [
        vec![entry(4, 9)],
        vec![entry(2, 9), entry(4, 9)],
        vec![entry(0, 9)],
    ] {
        assert!(matches!(
            s.persist(&batch, Some(&hs)),
            Err(Error::Corrupt(_))
        ));
        assert_eq!(s.hard_state().unwrap(), HardState::default());
        assert_eq!(s.last_index().unwrap(), 2);
        assert_eq!(s.term(2).unwrap(), 2);
    }
    s.append(&[]).unwrap();
    drop(s);
    assert_eq!(dir.open().term(2).unwrap(), 2);
}
#[test]
fn voters_initialize_only_once() {
    let dir = Temp::new();
    let s = dir.open();
    s.initialize_voters(&[1, 2, 3]).unwrap();
    s.append(&[entry(1, 1)]).unwrap();
    drop(s);
    let s = dir.open();
    s.initialize_voters(&[1, 2, 3]).unwrap();
    assert!(matches!(
        s.initialize_voters(&[1, 2, 4]),
        Err(Error::Corrupt(_))
    ));
    drop(s);
    let other = Temp::new();
    let s = other.open();
    s.append(&[entry(1, 1)]).unwrap();
    assert!(matches!(s.initialize_voters(&[1]), Err(Error::Corrupt(_))));
}
#[test]
fn storage_contract() {
    let dir = Temp::new();
    let s = dir.open();
    assert_eq!(
        (
            s.first_index().unwrap(),
            s.last_index().unwrap(),
            s.term(0).unwrap()
        ),
        (1, 0, 0)
    );
    assert!(entries(&s, 1, 1, None).unwrap().is_empty());
    s.append(&[entry(1, 1), entry(2, 2), entry(3, 3)]).unwrap();
    assert_eq!(
        (
            s.first_index().unwrap(),
            s.last_index().unwrap(),
            s.term(2).unwrap()
        ),
        (1, 3, 2)
    );
    assert_eq!(
        entries(&s, 2, 4, None).unwrap(),
        vec![entry(2, 2), entry(3, 3)]
    );
    assert_eq!(entries(&s, 1, 4, Some(0)).unwrap(), vec![entry(1, 1)]);
    let size = u64::from(entry(1, 1).compute_size() + entry(2, 2).compute_size());
    assert_eq!(entries(&s, 1, 4, Some(size)).unwrap().len(), 2);
    assert_eq!(entries(&s, 0, 1, None), Err(StorageError::Compacted.into()));
    for (low, high) in [(1, 5), (4, 5), (3, 2)] {
        assert_eq!(
            entries(&s, low, high, None),
            Err(StorageError::Unavailable.into())
        );
    }
    assert_eq!(s.term(4), Err(StorageError::Unavailable.into()));
    assert_eq!(
        s.snapshot(0, 0),
        Err(StorageError::SnapshotTemporarilyUnavailable.into())
    );
}
#[test]
fn reads_persisted_snapshot_boundary() {
    let dir = Temp::new();
    drop(dir.open());
    {
        use redb::{ReadableDatabase, TableDefinition};
        let db = redb::Database::open(dir.0.join("raft.redb")).unwrap();
        let mut tx = db.begin_write().unwrap();
        tx.set_durability(redb::Durability::Immediate).unwrap();
        {
            let mut state = tx
                .open_table(TableDefinition::<&str, &[u8]>::new("state"))
                .unwrap();
            let meta = SnapshotMetadata {
                index: 5,
                term: 3,
                ..Default::default()
            };
            state
                .insert("snapshot_meta", meta.write_to_bytes().unwrap().as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
        assert!(db.begin_read().is_ok());
    }
    let s = dir.open();
    assert_eq!((s.first_index().unwrap(), s.last_index().unwrap()), (6, 5));
    assert_eq!(s.term(5).unwrap(), 3);
    assert_eq!(s.term(4), Err(StorageError::Compacted.into()));
    assert_eq!(entries(&s, 5, 6, None), Err(StorageError::Compacted.into()));
    assert!(entries(&s, 6, 6, None).unwrap().is_empty());
    s.append(&[entry(6, 4)]).unwrap();
    assert_eq!(s.term(6).unwrap(), 4);
}

#[test]
fn compact_bounds_and_reopen() {
    let dir = Temp::new();
    let s = dir.open();
    s.append(&[entry(1, 1), entry(2, 2), entry(3, 3)]).unwrap();
    s.compact(0).unwrap();
    assert!(matches!(s.compact(4), Err(Error::Corrupt(_))));
    assert_eq!(s.snapshot_meta().unwrap(), (0, 0));
    s.compact(2).unwrap();
    s.compact(1).unwrap();
    s.compact(2).unwrap();
    drop(s);
    let s = dir.open();
    assert_eq!(s.snapshot_meta().unwrap(), (2, 2));
    assert_eq!((s.first_index().unwrap(), s.last_index().unwrap()), (3, 3));
    assert_eq!(s.term(2).unwrap(), 2);
    assert_eq!(s.term(1), Err(StorageError::Compacted.into()));
    assert_eq!(entries(&s, 2, 4, None), Err(StorageError::Compacted.into()));
    assert_eq!(entries(&s, 3, 4, None).unwrap(), vec![entry(3, 3)]);
    s.compact(3).unwrap();
    assert_eq!((s.first_index().unwrap(), s.last_index().unwrap()), (4, 3));
    s.append(&[entry(4, 4)]).unwrap();
}
#[test]
fn install_snapshot_keeps_matching_tail_and_state() {
    let dir = Temp::new();
    let s = dir.open();
    let hs = HardState {
        term: 8,
        vote: 3,
        commit: 1,
        ..Default::default()
    };
    s.persist(&[entry(1, 1), entry(2, 2), entry(3, 3)], Some(&hs))
        .unwrap();
    let mut cs = ConfState::default();
    cs.set_voters(vec![2, 3, 4]);
    s.install_snapshot_meta(2, 2, &cs).unwrap();
    drop(s);
    let s = dir.open();
    assert_eq!(s.snapshot_meta().unwrap(), (2, 2));
    assert_eq!((s.first_index().unwrap(), s.last_index().unwrap()), (3, 3));
    assert_eq!(s.term(2).unwrap(), 2);
    assert_eq!(s.term(1), Err(StorageError::Compacted.into()));
    assert_eq!(entries(&s, 2, 3, None), Err(StorageError::Compacted.into()));
    assert_eq!(entries(&s, 3, 4, None).unwrap(), vec![entry(3, 3)]);
    assert_eq!(s.conf_state().unwrap(), cs);
    assert_eq!(s.hard_state().unwrap(), HardState { commit: 2, ..hs });
    for index in [1, 2] {
        assert!(matches!(
            s.install_snapshot_meta(index, 9, &ConfState::default()),
            Err(Error::Corrupt(_))
        ));
    }
    assert_eq!(s.snapshot_meta().unwrap(), (2, 2));
    assert_eq!(s.conf_state().unwrap(), cs);
    assert_eq!(s.last_index().unwrap(), 3);
    s.append(&[entry(4, 4)]).unwrap();
}
#[test]
fn install_snapshot_drops_mismatching_or_absent_tail() {
    for index in [2, 5] {
        let dir = Temp::new();
        let s = dir.open();
        let hs = HardState {
            term: 8,
            vote: 3,
            commit: 9,
            ..Default::default()
        };
        s.persist(&[entry(1, 1), entry(2, 2), entry(3, 3)], Some(&hs))
            .unwrap();
        let mut cs = ConfState::default();
        cs.set_voters(vec![1, 3]);
        s.install_snapshot_meta(index, 7, &cs).unwrap();
        drop(s);
        let s = dir.open();
        assert_eq!(s.snapshot_meta().unwrap(), (index, 7));
        assert_eq!(
            (s.first_index().unwrap(), s.last_index().unwrap()),
            (index + 1, index)
        );
        assert_eq!(s.term(index).unwrap(), 7);
        assert_eq!(s.term(index - 1), Err(StorageError::Compacted.into()));
        assert_eq!(s.term(index + 1), Err(StorageError::Unavailable.into()));
        assert!(entries(&s, index + 1, index + 1, None).unwrap().is_empty());
        assert_eq!(s.conf_state().unwrap(), cs);
        assert_eq!(s.hard_state().unwrap(), hs);
        assert!(matches!(
            s.append(&[entry(index + 2, 8)]),
            Err(Error::Corrupt(_))
        ));
        s.append(&[entry(index + 1, 8)]).unwrap();
        assert_eq!(
            entries(&s, index + 1, index + 2, None).unwrap(),
            vec![entry(index + 1, 8)]
        );
    }
}

#[test]
fn size_limit_stops_before_reading_later_entries() {
    let dir = Temp::new();
    let s = dir.open();
    s.append(&[entry(1, 1), entry(2, 2), entry(3, 3)]).unwrap();
    drop(s);
    {
        let db = redb::Database::open(dir.0.join("raft.redb")).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut log = tx
                .open_table(redb::TableDefinition::<u64, &[u8]>::new("entries"))
                .unwrap();
            log.insert(3, &[0xff][..]).unwrap();
        }
        tx.commit().unwrap();
    }
    let s = dir.open();
    let size = u64::from(entry(1, 1).compute_size());
    for max in [0, size, size + 1] {
        assert_eq!(entries(&s, 1, 4, Some(max)).unwrap(), vec![entry(1, 1)]);
    }
    assert!(entries(&s, 1, 4, None).is_err());
}
