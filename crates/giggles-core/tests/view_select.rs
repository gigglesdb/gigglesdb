#[cfg(feature = "engine-rocksdb")]
#[test]
fn projection_views_preserve_bindings_scope_and_snapshots() {
    use giggles_core::{Admin, CoreError, Engine, Limits, relational::{SqlError, Value}};
    let path = std::env::temp_dir().join(format!("view-select-{}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE t(a int PRIMARY KEY,b int,label varchar(8))", "INSERT INTO t VALUES(1,2,'one'),(3,4,'three')",
        "CREATE VIEW v(x,y,label) AS SELECT b,a,label FROM t", "CREATE VIEW repeated AS SELECT a AS x,a AS y FROM t",
        "CREATE VIEW nested AS SELECT y,x FROM v", "ALTER TABLE t RENAME COLUMN a TO renamed", "ALTER TABLE t ADD COLUMN later int"] {
        s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let prepared = s.prepare("SELECT * FROM v").unwrap();
    for sql in ["CREATE INDEX ix ON t(renamed)","CREATE VIEW swapped(renamed,b) AS SELECT b,renamed FROM t"] { s.execute(sql,&[],None).unwrap(); }
    assert!(prepared.read_tables().contains(&"t".into()));
    let desc = s.describe(&prepared).unwrap();
    assert_eq!(desc.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["x","y","label"]);
    assert_eq!(desc.typmods, [-1,-1,12]);
    for (sql, expected) in [
        ("SELECT x,y FROM v ORDER BY x", vec![2,1]),
        ("SELECT * FROM repeated ORDER BY x", vec![1,1]),
        ("SELECT * FROM nested ORDER BY y", vec![1,2]),
        ("SELECT renamed FROM swapped WHERE renamed=2", vec![2]),
        ("SELECT x,y FROM v WHERE y>0 ORDER BY x DESC LIMIT 1 OFFSET 1", vec![2,1]),
        ("SELECT v.x,t.renamed FROM v JOIN t ON v.y=t.renamed ORDER BY v.x", vec![2,1]),
        ("SELECT v.x,n.y FROM v JOIN nested n ON v.y=n.y ORDER BY v.x", vec![2,1]),
        ("SELECT count(*),sum(x) FROM v", vec![2,6]),
        ("SELECT x,count(*) FROM v GROUP BY x ORDER BY x", vec![2,1]),
        ("SELECT count(*) FROM (SELECT x FROM v ORDER BY x LIMIT 1 OFFSET 1) sub", vec![1]),
        ("SELECT * FROM v ORDER BY 1 DESC LIMIT 1", vec![4,3]),
    ] {
        let row = s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}")).rows.remove(0);
        assert_eq!(&row.values[..expected.len()], expected.into_iter().map(Value::Int).collect::<Vec<_>>(), "{sql}");
    }
    for (sql, error) in [("SELECT ctid FROM v",SqlError::UnknownColumn),("SELECT later FROM v",SqlError::UnknownColumn),
        ("INSERT INTO v VALUES(5,6,'x')",SqlError::Unsupported),("UPDATE v SET x=9",SqlError::Unsupported),("DELETE FROM v",SqlError::Unsupported)] {
        assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(error)), "{sql}");
    }
    for sql in ["BEGIN","INSERT INTO t VALUES(5,6,'private',NULL)"] { s.execute(sql,&[],None).unwrap(); }
    assert_eq!(s.execute("SELECT count(*) FROM v",&[],None).unwrap().rows[0].values,[Value::Int(3)]);
    s.execute("ROLLBACK",&[],None).unwrap();
    assert_eq!(s.execute("SELECT count(*) FROM v",&[],None).unwrap().rows[0].values,[Value::Int(2)]);
    drop(s); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap(); let mut s = admin.sql_session();
    assert_eq!(s.execute("SELECT x FROM v ORDER BY x",&[],None).unwrap().rows[0].values,[Value::Int(2)]);
    let prepared = s.prepare("SELECT * FROM v").unwrap();
    s.execute("DROP VIEW v CASCADE",&[],None).unwrap();
    assert_eq!(s.describe(&prepared).unwrap_err(),CoreError::Sql(SqlError::UnknownTable));
    assert_eq!(s.execute_prepared(&prepared,&[],None),Err(CoreError::Sql(SqlError::UnknownTable)));
    drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
