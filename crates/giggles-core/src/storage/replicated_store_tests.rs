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

#[test]
fn replica_apply_rejects_commit_group_mutations() {
    let dir = Dir::new();
    let store = ReplicaStore::create(&dir.0, [9; 16]).unwrap();
    let at = pos(1, 1);
    let before = contents(&store);
    let mut put_then_delete = WriteBatch::default();
    put_then_delete.put(Keyspace::Rows, b"row".to_vec(), b"value".to_vec());
    put_then_delete.put(
        Keyspace::Meta,
        COMMIT_GROUP_KEY.to_vec(),
        1u64.to_be_bytes().to_vec(),
    );
    put_then_delete.delete(Keyspace::Meta, COMMIT_GROUP_KEY.to_vec());

    let mut delete = WriteBatch::default();
    delete.delete(Keyspace::Meta, COMMIT_GROUP_KEY.to_vec());
    let mut range = WriteBatch::default();
    range.delete_range(Keyspace::Meta, b"commit_".to_vec(), b"commit_z".to_vec());

    for invalid in [put_then_delete, delete, range] {
        assert_eq!(
            store.apply(&invalid.encode(), at),
            Err(CoreError::Corrupt("batch touches commit_group".into()))
        );
        assert_eq!(store.applied().unwrap(), None);
        assert_eq!(contents(&store), before);
    }
    assert_eq!(store.apply(&batch(COMMIT_GROUP_KEY), at), Ok(ApplyOutcome::Applied));
    assert_eq!(store.applied().unwrap(), Some(at));
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
fn exclusive_after_admin_and_sessions_drop() {
    let dirs = [Dir::new(), Dir::new(), Dir::new()];
    let stores = dirs
        .each_ref()
        .map(|d| Arc::new(ReplicaStore::create(&d.0, [9; 16]).unwrap()));
    assert!(stores[0].is_exclusive());
    let sink = Arc::new(Fanout {
        stores: stores.clone(),
        next: Mutex::new(1),
    });
    let admin = stores[0]
        .open_admin(sink, Limits::default(), v2::DEFAULT_WRITE_SET_BYTES)
        .unwrap();
    assert!(!stores[0].is_exclusive());
    let session = admin.sql_session();
    drop(admin);
    assert!(!stores[0].is_exclusive());
    drop(session);
    assert!(stores[0].is_exclusive());
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

#[test]
fn foreign_key_identity_replicates_and_reopens() {
    use super::meta::{KEY_DEPENDENCY_IDENTITY, REPLICATED};

    let dirs = [Dir::new(), Dir::new(), Dir::new()];
    let stores = dirs.each_ref()
        .map(|d| Arc::new(ReplicaStore::create(&d.0, [10; 16]).unwrap()));
    let sink = Arc::new(Fanout {
        stores: stores.clone(),
        next: Mutex::new(1),
    });
    let admin = stores[0]
        .open_admin(sink, Limits::default(), v2::DEFAULT_WRITE_SET_BYTES)
        .unwrap();
    let mut session = admin.sql_session();
    session.execute("CREATE TABLE parent(id int PRIMARY KEY)", &[], None).unwrap();
    session.execute(
        "CREATE TABLE child(id int, parent_id int, CONSTRAINT child_parent_fk FOREIGN KEY(parent_id) REFERENCES parent(id))",
        &[], None,
    ).unwrap();
    let targets = session.foreign_key_targets().unwrap();
    assert_eq!(targets.get("child_parent_fk").map(String::as_str), Some("pk:parent_pkey"));
    let required = REPLICATED | KEY_DEPENDENCY_IDENTITY | 64;
    for store in &stores {
        let bytes = store.kv.get(Keyspace::Meta, b"features").unwrap().unwrap();
        let features = u64::from_be_bytes(bytes.try_into().unwrap());
        assert_eq!(features & required, required);
    }
    assert_equal(&stores);
    let checkpoint_dir = Dir::new();
    let checkpoint = checkpoint_dir.0.join("checkpoint");
    stores[0].checkpoint(&checkpoint).unwrap();
    let original = contents(&stores[0]);
    drop(session);
    drop(admin);
    drop(stores);

    let stores = dirs.each_ref().map(|d| Arc::new(ReplicaStore::open(&d.0).unwrap()));
    assert_equal(&stores);
    let copy = Arc::new(ReplicaStore::open(&checkpoint).unwrap());
    assert_eq!(contents(&copy), original);
    let next = stores[0].applied().unwrap().unwrap().index + 1;
    let sink = Arc::new(Fanout { stores: stores.clone(), next: Mutex::new(next) });
    for store in [&stores[1], &copy] {
        let admin = store
            .open_admin(sink.clone(), Limits::default(), v2::DEFAULT_WRITE_SET_BYTES)
            .unwrap();
        assert_eq!(admin.sql_session().foreign_key_targets().unwrap(), targets);
    }
}

mod native_once {
    //! GD-0074 R4-41: native once receipts on replicated v2 databases.
    use super::*;
    use crate::operations::{
        Bindings, Engine as NativeEngine, Error, Request, ValueExpr,
        once::{
            ONCE_CONTRACT_VERSION, OnceBody, OnceClockUnavailable, OnceError, OnceIdentity,
            OnceRequest, OnceStatus, OnceTime,
        },
    };
    use std::{
        sync::atomic::{AtomicU8, Ordering},
        time::{Duration, Instant},
    };

    const DELIVER: u8 = 0;
    const LOSE_REPLY: u8 = 1;
    const LOSE_ENTRY: u8 = 2;

    /// Fanout whose reply can be lost after the entry applied, or before it did.
    struct Lossy {
        fanout: Fanout,
        mode: AtomicU8,
    }
    impl BatchSink for Lossy {
        fn replicate(&self, bytes: Vec<u8>) -> Result<(), ReplicateError> {
            match self.mode.load(Ordering::SeqCst) {
                LOSE_ENTRY => Err(ReplicateError::Unavailable("entry lost".into())),
                mode => {
                    self.fanout.replicate(bytes)?;
                    if mode == LOSE_REPLY {
                        Err(ReplicateError::Unavailable("reply lost".into()))
                    } else {
                        Ok(())
                    }
                }
            }
        }
    }

    fn cluster(domain: u8) -> ([Dir; 3], [Arc<ReplicaStore>; 3]) {
        let dirs = [Dir::new(), Dir::new(), Dir::new()];
        let stores = dirs
            .each_ref()
            .map(|d| Arc::new(ReplicaStore::create(&d.0, [domain; 16]).unwrap()));
        (dirs, stores)
    }

    /// Open the SQL engine on `stores[leader]`, continuing the shared log.
    fn lead(stores: &[Arc<ReplicaStore>; 3], leader: usize) -> (Admin, Arc<Lossy>) {
        let next = stores[leader].applied().unwrap().map_or(1, |p| p.index + 1);
        let order = [leader, (leader + 1) % 3, (leader + 2) % 3];
        let sink = Arc::new(Lossy {
            fanout: Fanout {
                stores: order.map(|i| stores[i].clone()),
                next: Mutex::new(next),
            },
            mode: AtomicU8::new(DELIVER),
        });
        let admin = stores[leader]
            .open_admin(sink.clone(), Limits::default(), v2::DEFAULT_WRITE_SET_BYTES)
            .unwrap();
        (admin, sink)
    }

    fn request(admin: &Admin, key: u8, id: i64) -> OnceRequest {
        OnceRequest {
            contract_version: ONCE_CONTRACT_VERSION,
            identity: OnceIdentity {
                database_domain: admin.authority_metadata().unwrap().0,
                tenant: 1,
                namespace: "replicated".into(),
                key: vec![key],
            },
            retention_seconds: 60,
            body: OnceBody::Operation(Request::Insert {
                table: "t".into(),
                values: vec![
                    ("id".into(), ValueExpr::Literal(Value::Int(id))),
                    ("n".into(), ValueExpr::Literal(Value::Int(10))),
                ],
            }),
        }
    }

    fn at(lower: u64) -> impl Fn() -> Result<OnceTime, OnceClockUnavailable> {
        move || {
            Ok(OnceTime {
                lower_unix_seconds: lower,
                upper_unix_seconds: lower + 1,
                valid_until: Instant::now() + Duration::from_secs(10),
            })
        }
    }

    fn rows(admin: &Admin) -> usize {
        admin
            .sql_session()
            .execute("SELECT id FROM t", &[], None)
            .unwrap()
            .rows
            .len()
    }

    fn warm(admin: &Admin, id: i64) {
        admin
            .sql_session()
            .execute(&format!("INSERT INTO t VALUES ({id}, 0)"), &[], None)
            .unwrap();
    }

    fn assert_receipt_format(stores: &[Arc<ReplicaStore>; 3]) {
        use crate::storage::engine::meta::{NATIVE_OUTCOMES, NATIVE_SUBVERSION, REPLICATED};
        for store in stores {
            let info = crate::storage::engine::meta::open_checks(store.kv.as_ref()).unwrap();
            assert_eq!(info.subversion, NATIVE_SUBVERSION);
            let bits = NATIVE_OUTCOMES | REPLICATED;
            assert_eq!(info.features & bits, bits);
        }
        assert_equal(stores);
    }

    #[test]
    fn receipts_replicate_replay_compact_and_survive_leader_handoff() {
        let (_dirs, stores) = cluster(11);
        let (admin, _) = lead(&stores, 0);
        admin
            .sql_session()
            .execute("CREATE TABLE t(id int PRIMARY KEY, n int)", &[], None)
            .unwrap();
        let engine = NativeEngine::new(admin.clone());
        let original = request(&admin, 1, 1);
        assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Absent));
        let first = engine
            .execute_once(&original, &Bindings::new(), &at(1000))
            .unwrap();
        assert!(!first.replayed);
        assert!(first.outcome.is_ok());
        let mut expected = first;
        expected.replayed = true;
        assert_eq!(
            engine.execute_once(&original, &Bindings::new(), &at(1000)),
            Ok(expected.clone())
        );
        assert_eq!(
            engine.outcome(&original.identity),
            Ok(OnceStatus::Retained(expected.clone()))
        );
        let mut changed = original.clone();
        changed.retention_seconds += 1;
        assert_eq!(
            engine.execute_once(&changed, &Bindings::new(), &at(1000)),
            Err(OnceError::IdempotencyConflict)
        );
        assert_eq!(rows(&admin), 1);
        assert_receipt_format(&stores);
        drop(engine);
        drop(admin);

        // A new leader opens over its own replica and finds the same receipt.
        let (admin, _) = lead(&stores, 1);
        let engine = NativeEngine::new(admin.clone());
        assert_eq!(
            engine.outcome(&original.identity),
            Ok(OnceStatus::Retained(expected.clone()))
        );
        assert_eq!(
            engine.execute_once(&original, &Bindings::new(), &at(1000)),
            Ok(expected)
        );
        assert_eq!(rows(&admin), 1);
        // Compaction is an ordinary replicated write; the identity stays consumed.
        assert_eq!(engine.compact_outcomes(&at(2000)), Ok(1));
        let expired = OnceStatus::Expired {
            retry_until_seconds: 1061,
        };
        assert_eq!(engine.outcome(&original.identity), Ok(expired.clone()));
        assert_eq!(
            engine.execute_once(&original, &Bindings::new(), &at(2000)),
            Err(OnceError::KeyExpired {
                retry_until_seconds: 1061
            })
        );
        assert_eq!(rows(&admin), 1);
        assert_receipt_format(&stores);
        drop(engine);
        drop(admin);
        let (admin, _) = lead(&stores, 2);
        assert_eq!(
            NativeEngine::new(admin).outcome(&original.identity),
            Ok(expired)
        );
    }

    #[test]
    fn lost_reply_resolves_on_the_next_leader_without_a_second_apply() {
        let (_dirs, stores) = cluster(12);
        let (admin, sink) = lead(&stores, 0);
        admin
            .sql_session()
            .execute("CREATE TABLE t(id int PRIMARY KEY, n int)", &[], None)
            .unwrap();
        let engine = NativeEngine::new(admin.clone());
        let committed = request(&admin, 1, 1);
        let lost = request(&admin, 2, 2);
        let fenced = OnceError::Operation(Error::Core(CoreError::Fenced.to_string()));
        // The first insert on an engine reserves row-id/version blocks with their
        // own replicated writes; reserve them so the lost reply is the commit's.
        warm(&admin, 100);

        // Committed on every replica, but the leader never learns it.
        sink.mode.store(LOSE_REPLY, Ordering::SeqCst);
        assert_eq!(
            engine.execute_once(&committed, &Bindings::new(), &at(1000)),
            Err(OnceError::Unknown)
        );
        sink.mode.store(DELIVER, Ordering::SeqCst);
        assert_eq!(engine.outcome(&committed.identity), Err(fenced.clone()));
        assert_eq!(
            engine.execute_once(&committed, &Bindings::new(), &at(1000)),
            Err(fenced)
        );
        drop(engine);
        drop(admin);
        let (admin, sink) = lead(&stores, 1);
        let engine = NativeEngine::new(admin.clone());
        let Ok(OnceStatus::Retained(receipt)) = engine.outcome(&committed.identity) else {
            panic!("committed receipt missing on the new leader");
        };
        assert!(receipt.replayed && receipt.outcome.is_ok());
        assert_eq!(
            engine.execute_once(&committed, &Bindings::new(), &at(1000)),
            Ok(receipt)
        );
        assert_eq!(rows(&admin), 2);

        // Never committed: Absent on the next leader, then applied exactly once.
        warm(&admin, 101);
        let before = contents(&stores[2]);
        sink.mode.store(LOSE_ENTRY, Ordering::SeqCst);
        assert_eq!(
            engine.execute_once(&lost, &Bindings::new(), &at(1000)),
            Err(OnceError::Unknown)
        );
        assert_eq!(contents(&stores[2]), before);
        drop(engine);
        drop(admin);
        let (admin, _) = lead(&stores, 2);
        let engine = NativeEngine::new(admin.clone());
        assert_eq!(engine.outcome(&lost.identity), Ok(OnceStatus::Absent));
        let applied = engine
            .execute_once(&lost, &Bindings::new(), &at(1000))
            .unwrap();
        assert!(!applied.replayed && applied.outcome.is_ok());
        let mut replayed = applied;
        replayed.replayed = true;
        assert_eq!(
            engine.execute_once(&lost, &Bindings::new(), &at(1000)),
            Ok(replayed)
        );
        assert_eq!(rows(&admin), 4);
        assert_receipt_format(&stores);
    }
}

#[test]
fn applied_versions_bypass_admin_lock_and_origin_caches() {
    use crate::storage::{Metadata, encode};
    use crate::storage::engine::{codec, hlc::Ts, mvcc, txn::{Isolation, SerializationReason, TxnError}};

    for isolation in [Isolation::ReadCommitted, Isolation::RepeatableRead, Isolation::Serializable] {
        for ks in [Keyspace::Rows, Keyspace::Index] {
            let dir = Dir::new();
            let store = Arc::new(ReplicaStore::create(&dir.0, [7; 16]).unwrap());
            let key = 42u64.to_be_bytes();
            let mut metadata = Metadata {
                format: 4, domain: u128::from_be_bytes(store.domain()), schema: 0,
                last_time: 0, next_approval: 0, origin: Some(11),
            };
            let sink = Arc::new(Fanout {
                stores: [store.clone(), store.clone(), store.clone()], next: Mutex::new(1),
            });
            let admin = store.open_admin(sink, Limits::default(), v2::DEFAULT_WRITE_SET_BYTES).unwrap();
            let db = admin.test_v2();
            assert!(!db.kv.is_single_writer());
            let mut seed = db.begin(Isolation::ReadCommitted);
            seed.lock_for_write(ks, &key, None, &|| false).unwrap();
            seed.put(ks, key.to_vec(), b"old".to_vec()).unwrap();
            seed.put(Keyspace::Kernel, vec![0], encode(&metadata).unwrap()).unwrap();
            seed.commit().unwrap();
            let next = store.applied().unwrap().unwrap().index + 1;
            let mut txn = db.begin(isolation);
            let ts = txn.snapshot_ts();
            assert_eq!(txn.lock_for_write(ks, &key, None, &|| false).unwrap().current, Some(b"old".to_vec()));
            assert_eq!(db.metadata_state(Some(&txn)).unwrap().origin, Some(11));

            // Raft apply bypasses this admin's engine locks and visible timestamp.
            let mut applied = WriteBatch::default();
            applied.put(ks, codec::user_key_seek_at(&key, Ts(ts.0 + 1)), mvcc::encode_live(b"new"));
            metadata.origin = Some(22);
            // Even an already-visible timestamp can acquire a different origin.
            applied.put(Keyspace::Kernel, codec::user_key_seek_at(&[0], ts),
                mvcc::encode_live(&encode(&metadata).unwrap()));
            store.apply(&applied.encode(), pos(next, 1)).unwrap();
            let again = txn.lock_for_write(ks, &key, None, &|| false);
            if isolation == Isolation::ReadCommitted {
                let target = again.unwrap();
                assert_eq!(target.current, Some(b"new".to_vec()));
                assert!(target.changed_since_snapshot);
            } else {
                assert!(matches!(again, Err(TxnError::Serialization(SerializationReason::WriteConflict))));
            }
            assert_eq!(db.metadata_state(Some(&txn)).unwrap().origin, Some(22));
            db.inspect_metadata(Some(&txn), |_| Ok(())).unwrap();
            // A malformed replacement must still fail after a successful read.
            let mut corrupt = WriteBatch::default();
            corrupt.put(Keyspace::Kernel, codec::user_key_seek_at(&[0], ts), mvcc::encode_live(b"invalid"));
            store.apply(&corrupt.encode(), pos(next + 1, 1)).unwrap();
            assert!(db.inspect_metadata(Some(&txn), |_| Ok(())).is_err());
        }
    }
}
