use super::*;
use engine::kv::Keyspace;
use std::{path::PathBuf, sync::mpsc, thread, time::Duration};
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let mut token = [0; 16];
        getrandom::fill(&mut token).unwrap();
        Self(PathBuf::from(format!("J:/gigglesdb-work/r2/v2once/rest/t9-{:032x}", u128::from_be_bytes(token))))
    }
    fn open(&self) -> Admin {
        Admin::create_engine(&self.0, Limits::default(), Engine::V2).unwrap()
    }
}
impl Drop for Dir {
    fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); }
}
fn db(admin: &Admin) -> crate::storage::EngineLease {
    admin.test_v2()
}
fn slots(admin: &Admin) -> usize { admin.inner.sql_transactions.load(Ordering::Acquire) }
fn wait_for_lock(holder: &SqlSession) {
    let until = std::time::Instant::now() + Duration::from_secs(5);
    while holder.v2_transaction.as_ref().unwrap().sql_waiting_count() == 0 {
        assert!(std::time::Instant::now() < until, "no lock waiter");
        thread::yield_now();
    }
}
#[test]
fn fresh_lock_wait_and_snapshot() {
    let dir = Dir::new();
    let admin = dir.open();
    admin.sql_session().execute("CREATE TABLE t (id BIGINT PRIMARY KEY)", &[], None).unwrap();
    let mut first = admin.sql_session();
    let setup = first.begin_once_engine(None).unwrap();
    assert!(setup.before.is_none());
    assert_eq!(setup.image.0.view.domain, u128::from_be_bytes(db(&admin).meta.domain));
    first.v2_transaction.as_mut().unwrap().rollback_to(&setup.initial).unwrap();
    let (sent, recv) = mpsc::channel();
    let other = admin.clone();
    let worker = thread::spawn(move || {
        let mut session = other.sql_session();
        let cancel = crate::cancellation::Cancellation::new(Some(std::time::Instant::now() + Duration::from_secs(10)));
        let result = cancel.scope(|| session.begin_once_engine(None));
        sent.send(result.map(|s| s.fence)).unwrap();
    });
    wait_for_lock(&first);
    assert!(recv.try_recv().is_err());
    admin.sql_session().execute("INSERT INTO t VALUES (1)", &[], None).unwrap();
    first.begin_native_step().unwrap();
    assert_eq!(first.v2_transaction.as_mut().unwrap().snapshot_ts(), setup.fence);
    assert!(first.execute("SELECT * FROM t", &[], None).unwrap().rows.is_empty());
    first.rollback();
    assert!(recv.recv_timeout(Duration::from_secs(10)).unwrap().unwrap() > setup.fence);
    worker.join().unwrap();
    assert_eq!(slots(&admin), 0);
}
#[test]
fn bootstrap_stage_and_preserved_origin() {
    let dir = Dir::new();
    let admin = dir.open();
    let db = db(&admin);
    let mut txn = db.begin(Isolation::RepeatableRead);
    let mut guard = Kernel::lock(&mut txn, &db.kv, None, &|| false).unwrap();
    let mut image = KernelImage(empty(u128::from_be_bytes(db.meta.domain)));
    image.0.origin = Some(817);
    assert!(Kernel::stage_from(&mut guard, None, &image).unwrap() > 0);
    let presence = Kernel::probe(guard.transaction()).unwrap();
    assert!(presence.metadata && !presence.ledger);
    assert_eq!(guard.transaction().get(Keyspace::Kernel, &[255]).unwrap(), Some(vec![]));
    txn.commit().unwrap();
    let mut probe = db.begin(Isolation::RepeatableRead);
    assert!(Kernel::probe(&mut probe).unwrap().metadata);
    let mut session = admin.sql_session();
    let setup = session.begin_once_engine(None).unwrap();
    assert!(setup.before.is_some());
    assert_eq!(setup.image.0.origin, Some(817));
}
#[test]
fn corrupt_ledger_releases_slot_and_lock() {
    let dir = Dir::new();
    let admin = dir.open();
    let db = db(&admin);
    let mut batch = engine::kv::WriteBatch::default();
    let mut key = vec![7];
    key.extend_from_slice(&(!0u64).to_be_bytes());
    batch.put(Keyspace::Kernel, key, engine::mvcc::encode_live(&[0]));
    db.kv.write(batch, engine::kv::Durability::Sync).unwrap();
    let prior = slots(&admin);
    let mut session = admin.sql_session();
    for _ in 0..2 {
        assert!(matches!(session.begin_once_engine(None), Err(CoreError::Corrupt(_))));
        assert_eq!(slots(&admin), prior);
        assert!(session.v2_transaction.is_none());
    }
}
#[test]
fn cancelled_wait_releases_slot() {
    let dir = Dir::new();
    let admin = dir.open();
    let mut first = admin.sql_session();
    first.begin_once_engine(None).unwrap();
    let cancel = crate::cancellation::Cancellation::default();
    let child_cancel = cancel.clone();
    let other = admin.clone();
    let (sent, recv) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut session = other.sql_session();
        let result = child_cancel.scope(|| session.begin_once_engine(None));
        sent.send(matches!(result, Err(CoreError::Cancelled))).unwrap();
    });
    wait_for_lock(&first);
    assert_eq!(slots(&admin), 2);
    cancel.cancel();
    assert!(recv.recv_timeout(Duration::from_secs(5)).unwrap());
    worker.join().unwrap();
    assert_eq!(slots(&admin), 1);
    first.rollback();
    admin.sql_session().begin_once_engine(None).unwrap();
    assert_eq!(slots(&admin), 0);
}

#[test]
fn refuses_active_and_aborted_sessions() {
    let dir = Dir::new();
    let admin = dir.open();
    let mut session = admin.sql_session();
    session.aborted = true;
    assert!(matches!(session.begin_once_engine(None), Err(CoreError::Sql(relational::SqlError::Transaction))));
    assert_eq!(slots(&admin), 0);
    session.aborted = false;
    session.begin_once_engine(None).unwrap();
    assert!(matches!(session.begin_once_engine(None), Err(CoreError::Sql(relational::SqlError::Transaction))));
    assert_eq!(slots(&admin), 1);
    session.rollback();
    assert_eq!(slots(&admin), 0);
}
#[test]
fn mismatched_domain_releases_slot_and_lock() {
    let dir = Dir::new();
    let admin = dir.open();
    let db = db(&admin);
    let wrong = empty(u128::from_be_bytes(db.meta.domain) ^ 1);
    let value = records(&wrong).unwrap().remove(&vec![0]).unwrap();
    let mut key = vec![0];
    key.extend_from_slice(&(!0u64).to_be_bytes());
    let mut batch = engine::kv::WriteBatch::default();
    batch.put(Keyspace::Kernel, key, engine::mvcc::encode_live(&value));
    db.kv.write(batch, engine::kv::Durability::Sync).unwrap();
    let mut session = admin.sql_session();
    for _ in 0..2 {
        assert!(matches!(session.begin_once_engine(None), Err(CoreError::Corrupt(_))));
        assert_eq!(slots(&admin), 0);
    }
}
