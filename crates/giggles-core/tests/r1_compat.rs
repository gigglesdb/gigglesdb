mod common;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{StatementClock, Value as V},
};
use redb::{Database, ReadableTable, TableDefinition};
use std::{fs, path::PathBuf};
const FIXTURE: &[u8] = include_bytes!("fixtures/r1_legacy/data.redb");
fn copy_fixture() -> PathBuf {
    let path = common::fresh("r1-legacy");
    fs::create_dir(&path).unwrap();
    fs::write(path.join("data.redb"), FIXTURE).unwrap();
    path
}
fn run(s: &mut SqlSession, sql: &str) -> giggles_core::relational::SqlResult {
    s.execute_at(
        sql,
        &[],
        None,
        &StatementClock {
            unavailable: None,
            transaction_us: 100,
            statement_us: 100,
            zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
            wall: None,
        },
    )
    .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    run(s, sql).rows.into_iter().map(|r| r.values).collect()
}
#[test]
fn r1_server_database_preserves_rows_indexes_and_sequences() {
    let path = copy_fixture();
    let a = Admin::open(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    let expected = vec![
        vec![V::Int(1), V::Text("value-001".into())],
        vec![V::Int(2), V::Text("value-002".into())],
        vec![V::Int(3), V::Text("updated".into())],
    ];
    for table in ["mv_pk", "mv_heap"] {
        assert_eq!(
            rows(&mut s, &format!("SELECT id,label FROM {table} ORDER BY id")),
            expected
        );
    }
    assert_eq!(
        rows(&mut s, "SELECT id FROM mv_schema.items"),
        vec![vec![V::Int(7)]]
    );
    assert_eq!(
        rows(&mut s, "SELECT id FROM mv_schema.serials"),
        vec![vec![V::Int(1)]]
    );
    assert_eq!(
        rows(&mut s, "SELECT id,mood FROM mv_enum"),
        vec![vec![V::Int(1), V::Text("happy".into())]]
    );
    let (_, indexes) = a.secondary_index_catalog().unwrap();
    assert!(indexes.iter().any(|i| i.name == "mv_label"));
    assert!(indexes.iter().any(|i| i.name == "items_idx"));
    assert_eq!(
        rows(&mut s, "SELECT id,label FROM mv_pk WHERE label='updated'"),
        vec![expected[2].clone()]
    );
    assert_eq!(
        rows(&mut s, "SELECT id FROM mv_schema.items WHERE id=7"),
        vec![vec![V::Int(7)]]
    );
    // R1 resumes beyond its durable 32-value reservation on reopen.
    assert_eq!(
        rows(&mut s, "SELECT nextval('mv_seq')"),
        vec![vec![V::Int(50)]]
    );
    assert_eq!(
        rows(&mut s, "SELECT nextval('mv_schema.serials_id_seq')"),
        vec![vec![V::Int(34)]]
    );
    run(&mut s, "INSERT INTO mv_schema.serials DEFAULT VALUES");
    drop(s);
    drop(a);
    let a = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(
        rows(
            &mut a.sql_session(),
            "SELECT id FROM mv_schema.serials ORDER BY id"
        ),
        vec![vec![V::Int(1)], vec![V::Int(35)]]
    );
    drop(a);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn refused_and_invalid_catalog_opens_preserve_exact_file_bytes() {
    let path = copy_fixture();
    let file = path.join("data.redb");
    assert!(matches!(
        Admin::open(
            &path,
            Limits {
                admitted: 0,
                ..Limits::default()
            }
        ),
        Err(CoreError::Limit)
    ));
    assert_eq!(fs::read(&file).unwrap(), FIXTURE);
    // Valid redb with an unsupported relational format: admission must fail
    // after redb opens, without its Drop allocator transaction reaching disk.
    {
        let db = Database::open(&file).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut t = tx
                .open_table(TableDefinition::<&[u8], &[u8]>::new("giggles_v1"))
                .unwrap();
            let mut bytes = t.get(&[5u8][..]).unwrap().unwrap().value().to_vec();
            bytes[..4].copy_from_slice(&99u32.to_be_bytes());
            t.insert(&[5u8][..], bytes.as_slice()).unwrap();
        }
        tx.commit().unwrap();
    }
    let before = fs::read(&file).unwrap();
    for _ in 0..2 {
        assert!(matches!(
            Admin::open(&path, Limits::default()),
            Err(CoreError::Corrupt(_) | CoreError::Unsupported)
        ));
        assert_eq!(fs::read(&file).unwrap(), before);
    }
    // A supported relational catalog followed by an invalid namespace must
    // likewise fail late, after decoding and validating the catalog.
    {
        let db = Database::open(&file).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut t = tx
                .open_table(TableDefinition::<&[u8], &[u8]>::new("giggles_v1"))
                .unwrap();
            let mut bytes = t.get(&[5u8][..]).unwrap().unwrap().value().to_vec();
            bytes[..4].copy_from_slice(&2u32.to_be_bytes());
            t.insert(&[5u8][..], bytes.as_slice()).unwrap();
            t.insert(&[255u8][..], &[0u8][..]).unwrap();
        }
        tx.commit().unwrap();
    }
    let before = fs::read(&file).unwrap();
    assert!(matches!(
        Admin::open(&path, Limits::default()),
        Err(CoreError::Corrupt(_))
    ));
    assert_eq!(fs::read(&file).unwrap(), before);
    fs::remove_dir_all(path).unwrap();
}
