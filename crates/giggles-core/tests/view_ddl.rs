mod common;
use giggles_core::{Admin, CoreError, Limits, relational::SqlError};

#[test]
fn legacy_refuses_view_ddl() {
    let admin = Admin::create(common::fresh("view-legacy"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE t(id int)", &[], None).unwrap();
    for sql in ["CREATE VIEW v AS SELECT * FROM t", "DROP VIEW IF EXISTS v"] {
        assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(SqlError::Unsupported)));
    }
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn view_ddl_lifecycle_and_statement_lock() {
    let path = common::fresh("view-ddl");
    let admin = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE SCHEMA s", "CREATE TABLE s.t(id int, v varchar(8))", "CREATE SEQUENCE s.seq", "INSERT INTO s.t VALUES (1,'row')", "BEGIN",
        "CREATE VIEW s.v(\"C\", value) AS SELECT id, v FROM s.t"] { s.execute(sql, &[], None).unwrap(); }
    let (tx, rx) = std::sync::mpsc::channel();
    let mut other = admin.sql_session();
    let thread = std::thread::spawn(move || tx.send(other.execute("DROP TABLE s.t", &[], None)).unwrap());
    assert!(rx.recv_timeout(std::time::Duration::from_millis(100)).is_err());
    s.execute("COMMIT", &[], None).unwrap();
    assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(), Err(CoreError::Sql(SqlError::DependentObjects)));
    thread.join().unwrap();
    for sql in ["CREATE VIEW s.w AS SELECT * FROM s.v", "CREATE VIEW s.\"count\"(n) AS SELECT id FROM s.t", "DROP VIEW s.\"count\"", "BEGIN", "DROP VIEW s.v CASCADE", "ROLLBACK"] { s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}")); }
    for (sql, error) in [("CREATE VIEW s.v AS SELECT id FROM s.t", SqlError::DuplicateTable),
        ("CREATE VIEW bad AS SELECT id, id FROM s.t", SqlError::DuplicateColumn),
        ("CREATE VIEW bad(a,b,c) AS SELECT id,v FROM s.t", SqlError::Syntax),
        ("CREATE VIEW bad AS SELECT id+1 FROM s.t", SqlError::Unsupported),
        ("DROP VIEW s.v", SqlError::DependentObjects), ("DROP VIEW s.t", SqlError::WrongObjectType), ("DROP VIEW IF EXISTS s.seq", SqlError::WrongObjectType),
        ("DROP TABLE s.v", SqlError::WrongObjectType), ("ALTER TABLE s.t DROP COLUMN id", SqlError::DependentObjects)] {
        assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(error)), "{sql}");
    }
    s.execute("CREATE VIEW filtered AS SELECT * FROM s.t WHERE id=1", &[], None).unwrap();
    assert_eq!(s.execute("SELECT * FROM filtered", &[], None).unwrap().rows.len(), 1);
    s.execute("DROP VIEW filtered", &[], None).unwrap();
    assert_eq!(s.execute("SELECT id FROM s.t", &[], None).unwrap().rows.len(), 1);
    drop(s); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for sql in ["DROP VIEW s.v, s.w RESTRICT", "CREATE VIEW s.v AS SELECT * FROM s.t",
        "CREATE VIEW s.w AS SELECT * FROM s.v", "DROP VIEW s.v CASCADE", "DROP VIEW IF EXISTS s.w",
        "CREATE VIEW s.v AS SELECT * FROM s.t", "DROP TABLE s.t CASCADE", "DROP VIEW IF EXISTS s.v"] {
        s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn where_views_filter_nested_aliases_dependencies_and_reopen() {
    let path = common::fresh("where-views");
    let admin = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE t(id int PRIMARY KEY, v text, b boolean)",
        "INSERT INTO t VALUES(1,'x',true),(2,NULL,false),(3,'z',true)",
        "CREATE VIEW v AS SELECT id AS a, v FROM t WHERE id > 1",
        "CREATE VIEW w AS SELECT a AS n FROM v WHERE v IS NULL",
        "CREATE VIEW hidden AS SELECT id FROM t WHERE NOT b AND v IS NULL",
        "CREATE VIEW boolean_view AS SELECT id FROM t WHERE (id > 0 AND v <> 'x') OR b"] {
        s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    for sql in ["SELECT * FROM w", "SELECT * FROM w WHERE n=2", "SELECT * FROM hidden"] {
        assert_eq!(s.execute(sql,&[],None).unwrap().rows[0].values, vec![giggles_core::relational::Value::Int(2)]);
    }
    assert_eq!(s.execute("SELECT * FROM v",&[],None).unwrap().rows.len(),2);
    let joined = s.execute("SELECT t.id,w.n FROM t LEFT JOIN w ON t.id=w.n ORDER BY t.id",&[],None).unwrap();
    use giggles_core::relational::Value::{Int,Null};
    assert_eq!(joined.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),vec![vec![Int(1),Null],vec![Int(2),Int(2)],vec![Int(3),Null]]);
    assert_eq!(s.execute("SELECT * FROM boolean_view",&[],None).unwrap().rows.len(),2);
    for column in ["b", "v"] {
        assert_eq!(s.execute(&format!("ALTER TABLE t DROP COLUMN {column}"),&[],None), Err(CoreError::Sql(SqlError::DependentObjects)));
    }
    s.execute("CREATE OR REPLACE VIEW v AS SELECT id AS a, v FROM t WHERE id > 2",&[],None).unwrap();
    assert!(s.execute("SELECT * FROM w",&[],None).unwrap().rows.is_empty());
    drop(s); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    assert!(s.execute("SELECT * FROM w",&[],None).unwrap().rows.is_empty());
    assert_eq!(s.execute("SELECT * FROM hidden",&[],None).unwrap().rows.len(),1);
    s.execute("ALTER TABLE t DROP COLUMN b CASCADE",&[],None).unwrap();
    assert_eq!(s.execute("SELECT * FROM hidden",&[],None),Err(CoreError::Sql(SqlError::UnknownTable)));
    s.execute("DROP VIEW v CASCADE",&[],None).unwrap();
    drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
