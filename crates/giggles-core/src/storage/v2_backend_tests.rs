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
    let crate::storage::Backend::V2(db) = &admin.inner.backend else {
        unreachable!();
    };
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
            engine::meta::CURRENT_SUBVERSION.to_be_bytes().to_vec(),
        ),
        (b"domain".to_vec(), db.meta.domain.to_vec()),
        (b"engine".to_vec(), b"rocksdb".to_vec()),
        (b"features".to_vec(), 0u64.to_be_bytes().to_vec()),
    ];
    expected.sort();
    assert_eq!(actual, expected);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
