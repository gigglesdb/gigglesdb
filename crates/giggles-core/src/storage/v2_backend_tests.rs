use super::{DEFAULT_WRITE_SET_BYTES, engine};
use crate::{Admin, CoreError, Engine, Limits, relational::Value};
use engine::kv::{Direction, Keyspace};
use std::{ops::Bound, sync::Arc};

fn initialized_backend() -> Arc<engine::kv_mem::MemKv> {
    let backend = Arc::new(engine::kv_mem::MemKv::new());
    let mut domain = [0; 16];
    getrandom::fill(&mut domain).unwrap();
    engine::meta::write_initial_meta(backend.as_ref(), domain, 0).unwrap();
    backend
}

fn rows(admin: &Admin) -> Vec<Vec<Value>> {
    admin
        .sql_session()
        .execute("SELECT id, v FROM t ORDER BY id", &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

#[test]
fn supplied_backend_supports_sql_and_reopen() {
    let backend = initialized_backend();
    let admin =
        Admin::attach_v2_backend(backend.clone(), Limits::default(), DEFAULT_WRITE_SET_BYTES)
            .unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE t(id int PRIMARY KEY, v text)",
        "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        "UPDATE t SET v = 'updated' WHERE id = 2",
        "DELETE FROM t WHERE id = 1",
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    let expected = vec![
        vec![Value::Int(2), Value::Text("updated".into())],
        vec![Value::Int(3), Value::Text("three".into())],
    ];
    assert_eq!(rows(&admin), expected);
    drop(session);
    drop(admin);
    let reopened =
        Admin::attach_v2_backend(backend, Limits::default(), DEFAULT_WRITE_SET_BYTES).unwrap();
    assert_eq!(rows(&reopened), expected);
}

#[test]
fn configured_write_set_limit_rolls_back_all_rows() {
    let backend = initialized_backend();
    // Create the schema under the ordinary limit so only DML hits the small limit.
    let admin =
        Admin::attach_v2_backend(backend.clone(), Limits::default(), DEFAULT_WRITE_SET_BYTES)
            .unwrap();
    admin
        .sql_session()
        .execute("CREATE TABLE t(id int PRIMARY KEY, v text)", &[], None)
        .unwrap();
    drop(admin);
    let admin = Admin::attach_v2_backend(backend, Limits::default(), 4096).unwrap();
    let mut session = admin.sql_session();
    session.execute("BEGIN", &[], None).unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'small')", &[], None)
        .unwrap();
    let text = "x".repeat(3000);
    let sql = format!("INSERT INTO t VALUES (2, '{text}'), (3, '{text}'), (4, '{text}')");
    let error = session.execute(&sql, &[], None).unwrap_err();
    assert!(matches!(error, CoreError::Limit), "{error:?}");
    // TxnError::WriteSetLimit becomes CoreError::Limit at the SQL API boundary.
    assert_eq!(crate::sqlstate(&error), "54000");
    assert!(rows(&admin).is_empty());
    session.execute("ROLLBACK", &[], None).unwrap();
    assert!(rows(&admin).is_empty());
}

#[test]
fn single_node_creation_preserves_initial_meta() {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!(
        "v2-backend-meta-{:032x}",
        u128::from_be_bytes(nonce),
    ));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let db = admin.test_v2();
    let actual = db
        .kv
        .snapshot()
        .unwrap()
        .range(
            Keyspace::Meta,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut expected = vec![
        (b"family".to_vec(), 2u32.to_be_bytes().to_vec()),
        (
            b"subversion".to_vec(),
            engine::meta::INITIAL_SUBVERSION.to_be_bytes().to_vec(),
        ),
        (b"domain".to_vec(), db.meta.domain.to_vec()),
        (b"engine".to_vec(), b"rocksdb".to_vec()),
        (b"features".to_vec(), 0u64.to_be_bytes().to_vec()),
    ];
    expected.sort();
    assert_eq!(actual, expected);
    drop(db);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn single_node_open_refuses_replicated_without_meta_changes() {
    use super::recovery_tests::retry_writer_lock;
    use engine::kv::{KvBackend, KvRead};
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!(
        "v2-replicated-meta-{:032x}",
        u128::from_be_bytes(nonce)
    ));
    let meta = |kv: &dyn KvRead| {
        kv.range(
            Keyspace::Meta,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    };
    let backend = engine::meta::create_with_features(&path, nonce, engine::meta::REPLICATED, |p| {
        Ok(Arc::new(engine::kv_rocksdb::RocksKv::open(p, 8 << 20)?))
    })
    .unwrap();
    let before = meta(backend.as_ref());
    drop(backend);
    assert!(matches!(
        retry_writer_lock(|| Admin::open(&path, Limits::default())),
        Err(CoreError::Unsupported)
    ));
    let backend = retry_writer_lock(|| engine::kv_rocksdb::RocksKv::open(&path, 8 << 20)).unwrap();
    assert_eq!(meta(backend.snapshot().unwrap().as_ref()), before);
    drop(backend);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn supplied_replicated_backend_supports_sql() {
    use engine::kv::{Durability, KvBackend, WriteBatch};
    let backend = Arc::new(engine::kv_mem::MemKv::new());
    engine::meta::write_initial_meta(backend.as_ref(), [3; 16], engine::meta::REPLICATED).unwrap();
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Meta, b"raft_applied".to_vec(), vec![0; 16]);
    backend.write(batch, Durability::Sync).unwrap();
    let admin =
        Admin::attach_v2_backend(backend, Limits::default(), DEFAULT_WRITE_SET_BYTES).unwrap();
    admin
        .sql_session()
        .execute("CREATE TABLE t(id int PRIMARY KEY)", &[], None)
        .unwrap();
}

#[test]
fn metadata_origin_cache_uses_exact_snapshot_and_never_caches_errors() {
    use crate::storage::{Metadata, encode};
    use engine::txn::Isolation;
    // This private backend has no external writers; exercise the native cache path.
    let db = super::V2Db::from_kv_with_limit(
        Arc::new(engine::kv::Kv::new_single_writer(initialized_backend())),
        DEFAULT_WRITE_SET_BYTES, false, None, None, super::EngineOptions::default(),
    ).unwrap();
    let old = db.begin(Isolation::ReadCommitted);
    let ts = old.current_snapshot_ts().unwrap();
    assert_eq!(db.validated_origin_at(ts).unwrap(), None);
    assert_eq!(*db.origin_cache.lock().unwrap(), Some((ts, None)));
    let metadata = Metadata {
        format: 4,
        domain: u128::from_be_bytes(db.meta.domain),
        schema: 0,
        last_time: 0,
        next_approval: 0,
        origin: Some(77),
    };
    let mut writer = db.begin(Isolation::ReadCommitted);
    writer
        .put(Keyspace::Kernel, vec![0], encode(&metadata).unwrap())
        .unwrap();
    let next = writer.commit().unwrap();
    assert_eq!(db.validated_origin_at(next).unwrap(), Some(77));
    assert_eq!(db.validated_origin_at(ts).unwrap(), None);
    let mut bad = metadata;
    bad.domain ^= 1;
    let mut writer = db.begin(Isolation::ReadCommitted);
    writer
        .put(Keyspace::Kernel, vec![0], encode(&bad).unwrap())
        .unwrap();
    let invalid = writer.commit().unwrap();
    for _ in 0..2 {
        assert!(matches!(
            db.validated_origin_at(invalid),
            Err(CoreError::Corrupt(_))
        ));
        assert_eq!(*db.origin_cache.lock().unwrap(), Some((ts, None)));
    }
    db.kv.fence();
    assert!(db.validated_origin_at(ts).is_err());
}

#[test]
fn repeated_sql_writes_upserts_fk_and_metadata_recreate() {
    let admin = Admin::attach_v2_backend(
        initialized_backend(),
        Limits::default(),
        DEFAULT_WRITE_SET_BYTES,
    )
    .unwrap();
    let mut s = admin.sql_session();
    for sql in [
        "CREATE TABLE parent (id int PRIMARY KEY)",
        "CREATE TABLE child (id int PRIMARY KEY, p int REFERENCES parent(id), v int)",
        "INSERT INTO parent VALUES (1)",
        "INSERT INTO child VALUES (1, 1, 0)",
        "BEGIN",
        "UPDATE child SET v = v + 1 WHERE id = 1",
        "UPDATE child SET v = v + 1 WHERE id = 1",
        "INSERT INTO child VALUES (1, 1, 9) ON CONFLICT (id) DO NOTHING",
        "INSERT INTO child VALUES (1, 1, 3) ON CONFLICT (id) DO UPDATE SET v = excluded.v",
        "COMMIT",
    ] {
        s.execute(sql, &[], None).unwrap();
    }
    assert_eq!(
        s.execute("SELECT v FROM child", &[], None).unwrap().rows[0].values,
        vec![Value::Int(3)]
    );
    assert!(
        s.execute("DELETE FROM parent WHERE id = 1", &[], None)
            .is_err()
    );
    s.execute("DROP TABLE child", &[], None).unwrap();
    s.execute("CREATE TABLE child (replacement text)", &[], None)
        .unwrap();
    let plan = s.prepare("SELECT * FROM child").unwrap();
    assert_eq!(s.describe(&plan).unwrap().columns.len(), 1);
    assert!(s.execute("SELECT v FROM child", &[], None).is_err());
}
