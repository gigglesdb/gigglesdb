use super::*;
use crate::{Engine, relational::Value};
use std::{fs, path::PathBuf};

struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path =
            std::env::temp_dir().join(format!("replica-{:032x}", u128::from_be_bytes(nonce)));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn contents(store: &ReplicaStore) -> Vec<(Keyspace, Vec<KvPair>)> {
    Keyspace::ALL
        .into_iter()
        .map(|ks| {
            let rows = store
                .kv
                .range(ks, Bound::Unbounded, Bound::Unbounded, Direction::Forward)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            (ks, rows)
        })
        .collect()
}
fn pos(index: u64, term: u64) -> AppliedPosition {
    AppliedPosition { index, term }
}
fn batch(key: &[u8]) -> Vec<u8> {
    let mut b = WriteBatch::default();
    b.put(Keyspace::Rows, key.to_vec(), b"value".to_vec());
    b.encode()
}

#[test]
fn deterministic_creation_and_open() {
    let dirs = [Dir::new(), Dir::new(), Dir::new()];
    let stores = dirs
        .iter()
        .map(|d| ReplicaStore::create(&d.0, [7; 16]).unwrap())
        .collect::<Vec<_>>();
    for store in &stores {
        assert_eq!(contents(store)[0], contents(&stores[0])[0]);
        assert_eq!(store.applied().unwrap(), None);
        assert_eq!(store.domain(), [7; 16]);
    }
    drop(stores);
    assert!(ReplicaStore::create(&dirs[0].0, [7; 16]).is_err());
    let opened = ReplicaStore::open(&dirs[0].0).unwrap();
    assert_eq!(opened.domain(), [7; 16]);
    let nonempty = Dir::new();
    fs::write(nonempty.0.join("foreign"), b"data").unwrap();
    assert!(ReplicaStore::create(&nonempty.0, [7; 16]).is_err());
    assert!(ReplicaStore::open(&Dir::new().0).is_err());
    let single = Dir::new();
    drop(Admin::create_engine(&single.0, Limits::default(), Engine::V2).unwrap());
    assert!(matches!(
        ReplicaStore::open(&single.0),
        Err(CoreError::Unsupported)
    ));
}

#[test]
fn ordered_atomic_apply_and_reopen() {
    let dir = Dir::new();
    let store = ReplicaStore::create(&dir.0, [8; 16]).unwrap();
    assert_eq!(
        store.apply(&batch(b"one"), pos(1, 1)).unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(
        store.apply(b"garbage", pos(1, 99)).unwrap(),
        ApplyOutcome::Skipped
    );
    assert!(matches!(
        store.apply(&batch(b"two"), pos(3, 1)),
        Err(CoreError::Corrupt(_))
    ));
    assert_eq!(store.apply_noop(pos(2, 1)).unwrap(), ApplyOutcome::Applied);
    assert_eq!(store.apply_noop(pos(2, 99)).unwrap(), ApplyOutcome::Skipped);
    assert!(store.apply_noop(pos(4, 1)).is_err());
    assert_eq!(
        store.apply(&batch(b"two"), pos(3, 2)).unwrap(),
        ApplyOutcome::Applied
    );
    let before = contents(&store);
    assert!(store.apply(b"garbage", pos(4, 2)).is_err());
    for mode in 0..3 {
        let mut b = WriteBatch::default();
        match mode {
            0 => b.put(Keyspace::Meta, b"raft_applied".to_vec(), vec![0; 16]),
            1 => b.delete(Keyspace::Meta, b"raft_applied".to_vec()),
            _ => b.delete_range(Keyspace::Meta, b"raft_".to_vec(), b"raft_z".to_vec()),
        }
        assert!(store.apply(&b.encode(), pos(4, 2)).is_err());
        assert_eq!(contents(&store), before);
    }
    assert_eq!(store.applied().unwrap(), Some(pos(3, 2)));
    store.flush().unwrap();
    drop(store);
    let store = ReplicaStore::open(&dir.0).unwrap();
    assert_eq!(store.applied().unwrap(), Some(pos(3, 2)));
    assert_eq!(contents(&store), before);
    for key in [b"one", b"two"] {
        assert_eq!(
            store.kv.get(Keyspace::Rows, key).unwrap(),
            Some(b"value".to_vec())
        );
    }
    let mut corrupt = WriteBatch::default();
    corrupt.put(Keyspace::Meta, b"raft_applied".to_vec(), vec![0]);
    store.kv.write(corrupt, Durability::NoSync).unwrap();
    assert!(matches!(store.applied(), Err(CoreError::Corrupt(_))));
}

struct Fanout {
    stores: [Arc<ReplicaStore>; 3],
    next: Mutex<u64>,
}
impl BatchSink for Fanout {
    fn replicate(&self, bytes: Vec<u8>) -> Result<(), ReplicateError> {
        let mut next = self.next.lock().unwrap();
        for store in &self.stores {
            store
                .apply(&bytes, pos(*next, 1))
                .map_err(|e| ReplicateError::Unavailable(e.to_string()))?;
        }
        *next += 1;
        Ok(())
    }
}
fn assert_equal(stores: &[Arc<ReplicaStore>; 3]) {
    assert_eq!(contents(&stores[0]), contents(&stores[1]));
    assert_eq!(contents(&stores[0]), contents(&stores[2]));
}

#[test]
fn sql_leader_handoff_and_checkpoint() {
    let dirs = [Dir::new(), Dir::new(), Dir::new()];
    let stores = dirs
        .each_ref()
        .map(|d| Arc::new(ReplicaStore::create(&d.0, [9; 16]).unwrap()));
    let sink = Arc::new(Fanout {
        stores: stores.clone(),
        next: Mutex::new(1),
    });
    let admin = stores[0]
        .open_admin(sink, Limits::default(), v2::DEFAULT_WRITE_SET_BYTES)
        .unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t(id int PRIMARY KEY, v text)", &[], None)
        .unwrap();
    session
        .execute(
            "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            &[],
            None,
        )
        .unwrap();
    assert_equal(&stores);
    drop(session);
    drop(admin);
    let next = stores[1].applied().unwrap().unwrap().index + 1;
    let sink = Arc::new(Fanout {
        stores: [stores[1].clone(), stores[0].clone(), stores[2].clone()],
        next: Mutex::new(next),
    });
    let admin = stores[1]
        .open_admin(sink, Limits::default(), v2::DEFAULT_WRITE_SET_BYTES)
        .unwrap();
    let mut session = admin.sql_session();
    let result = session
        .execute("SELECT id, v FROM t ORDER BY id", &[], None)
        .unwrap();
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int(1), Value::Text("one".into())],
            vec![Value::Int(2), Value::Text("two".into())],
            vec![Value::Int(3), Value::Text("three".into())],
        ]
    );
    session
        .execute("INSERT INTO t VALUES (4, 'four')", &[], None)
        .unwrap();
    assert_equal(&stores);
    let target_dir = Dir::new();
    let target = target_dir.0.join("checkpoint");
    let files = stores[0].checkpoint(&target).unwrap();
    assert!(
        files
            .iter()
            .any(|(name, size)| name == "GIGGLES_FORMAT" && *size == 11)
    );
    assert!(files.windows(2).all(|w| w[0].0 < w[1].0));
    let copy = ReplicaStore::open(&target).unwrap();
    assert_eq!(copy.applied().unwrap(), stores[0].applied().unwrap());
    assert_eq!(contents(&copy), contents(&stores[0]));
}
