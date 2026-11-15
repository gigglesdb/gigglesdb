use super::*;
use crate::storage::{
    engine::{kv_mem::MemKv, meta},
    v2::DEFAULT_WRITE_SET_BYTES,
};
use crate::{Admin, CoreError, Limits, relational::Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Loopback {
    store: Arc<MemKv>,
    calls: AtomicUsize,
    fail: AtomicBool,
}

impl BatchSink for Loopback {
    fn replicate(&self, encoded_batch: Vec<u8>) -> Result<(), ReplicateError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(ReplicateError::NotLeader);
        }
        let batch = WriteBatch::decode(&encoded_batch)
            .map_err(|e| ReplicateError::Unavailable(e.to_string()))?;
        self.store
            .write(batch, Durability::Sync)
            .map_err(|e| ReplicateError::Unavailable(e.to_string()))
    }
}

fn contents(store: &dyn KvRead) -> Vec<(Keyspace, Vec<KvPair>)> {
    Keyspace::ALL
        .into_iter()
        .map(|ks| {
            let rows = store
                .range(ks, Bound::Unbounded, Bound::Unbounded, Direction::Forward)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            (ks, rows)
        })
        .collect()
}

#[test]
fn sql_loopback_and_ambiguous_write_fences() {
    let mem = Arc::new(MemKv::new());
    let mut domain = [0; 16];
    getrandom::fill(&mut domain).unwrap();
    meta::write_initial_meta(mem.as_ref(), domain, 0).unwrap();
    let sink = Arc::new(Loopback {
        store: mem.clone(),
        calls: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
    });
    let rkv = ReplicatedKv::new(mem.clone(), sink.clone());
    let admin = Admin::attach_v2_backend(Arc::new(rkv), Limits::default(), DEFAULT_WRITE_SET_BYTES)
        .unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t(id int PRIMARY KEY, v text)", &[], None)
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'one'), (2, 'two')", &[], None)
        .unwrap();
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
        ]
    );
    assert!(sink.calls.load(Ordering::SeqCst) > 0);
    let before = contents(mem.as_ref());
    sink.fail.store(true, Ordering::SeqCst);
    let calls = sink.calls.load(Ordering::SeqCst);
    let error = session
        .execute("INSERT INTO t VALUES (3, 'three')", &[], None)
        .unwrap_err();
    assert_eq!(
        error,
        CoreError::Storage("engine commit outcome unknown".into())
    );
    assert_eq!(sink.calls.load(Ordering::SeqCst), calls + 1);
    assert_eq!(
        session.execute("SELECT * FROM t", &[], None).unwrap_err(),
        CoreError::Fenced
    );
    assert_eq!(
        admin
            .sql_session()
            .execute("SELECT * FROM t", &[], None)
            .unwrap_err(),
        CoreError::Fenced
    );
    assert_eq!(contents(mem.as_ref()), before);
}

#[test]
fn errors_and_backend_contract() {
    assert_eq!(ReplicateError::NotLeader.to_string(), "not the leader");
    assert_eq!(
        ReplicateError::Unavailable("offline".into()).to_string(),
        "replication unavailable: offline"
    );
    let mem = Arc::new(MemKv::new());
    let sink = Arc::new(Loopback {
        store: mem.clone(),
        calls: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
    });
    let rkv = ReplicatedKv::new(mem, sink.clone());
    assert_eq!(rkv.engine_name(), "replicated");
    rkv.flush_barrier().unwrap();
    assert_eq!(sink.calls.load(Ordering::SeqCst), 0);
    let snapshot = rkv.snapshot().unwrap();
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Rows, b"key".to_vec(), b"value".to_vec());
    rkv.write(batch, Durability::NoSync).unwrap();
    assert_eq!(
        rkv.get(Keyspace::Rows, b"key").unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(
        rkv.seek_ge(Keyspace::Rows, b"a").unwrap(),
        Some((b"key".to_vec(), b"value".to_vec()))
    );
    assert_eq!(snapshot.get(Keyspace::Rows, b"key").unwrap(), None);
    assert_eq!(
        contents(&rkv)[2].1,
        vec![(b"key".to_vec(), b"value".to_vec())]
    );
    assert_eq!(sink.calls.load(Ordering::SeqCst), 1);
}
