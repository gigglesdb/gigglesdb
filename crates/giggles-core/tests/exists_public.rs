use giggles_core::{*, relational::{SqlError, SqlResult, Value as V}};
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
#[test]
fn exists_public_oracle_on_each_engine() {
    for engine in [Engine::Legacy, #[cfg(feature="engine-rocksdb")] Engine::V2] {
        let path = std::env::temp_dir().join(format!("c8-oracle-{}-{engine:?}", std::process::id()));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for sql in ["CREATE TABLE o(id int)", "CREATE TABLE i(k int,v int)",
            "INSERT INTO o VALUES(1),(2),(3),(NULL)",
            "INSERT INTO i VALUES(1,NULL),(2,20),(2,21),(NULL,99)"] { run(&mut s, sql); }
        for (term, expected) in [
            ("EXISTS(SELECT NULL FROM i WHERE k=o.id)", vec![V::Int(1),V::Int(2)]),
            ("NOT EXISTS(SELECT v FROM i WHERE k=o.id)", vec![V::Int(3),V::Null]),
            ("EXISTS(SELECT 1/0 FROM i WHERE k=o.id)", vec![V::Int(1),V::Int(2)]),
            ("EXISTS(SELECT 1 FROM i x WHERE EXISTS(SELECT NULL WHERE x.k=o.id))", vec![V::Int(1),V::Int(2)]),
            ("EXISTS(SELECT 1 FROM i o WHERE o.k=1)", vec![V::Int(1),V::Int(2),V::Int(3),V::Null]),
            ("EXISTS(SELECT 1 FROM i LIMIT 0)", vec![]),
            ("EXISTS(SELECT count(*) FROM i WHERE false)", vec![V::Int(1),V::Int(2),V::Int(3),V::Null]),
        ] {
            let sql = format!("SELECT id FROM o WHERE {term} ORDER BY id");
            let p = s.prepare(&sql).unwrap();
            s.describe(&p).unwrap();
            let result = s.execute_prepared(&p, &[], None).unwrap();
            assert_eq!(result.rows.into_iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(), expected, "{engine:?}: {sql}");
        }
        for (sql, expected) in [
            ("SELECT id FROM o WHERE EXISTS(SELECT missing FROM i)", SqlError::UnknownColumn),
            ("SELECT id FROM o WHERE EXISTS(SELECT nosuchfn() FROM i)", SqlError::UndefinedFunction),
        ] { assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(expected))); }
        for sql in ["SELECT (SELECT 1)", "SELECT id FROM o WHERE id=(SELECT 1)"] {
            assert_eq!(run(&mut s, sql).rows.into_iter().map(|r| r.values).collect::<Vec<_>>(), vec![vec![V::Int(1)]], "{engine:?}: {sql}");
        }
        assert_eq!(run(&mut s, "UPDATE o SET id=10 WHERE EXISTS(SELECT NULL FROM i WHERE k=o.id)").affected, 2);
        assert_eq!(run(&mut s, "DELETE FROM o WHERE NOT EXISTS(SELECT NULL FROM i WHERE k=o.id)").affected, 4);
        assert_eq!(s.prepare("SELECT 1 WHERE EXISTS(SELECT abs(-1))").unwrap().table(), None);
        assert!(s.prepare("SELECT 1 WHERE EXISTS(SELECT nosuchfn())").unwrap().table().is_some());
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}
#[cfg(feature="engine-rocksdb")]
#[test]
fn exists_v2_audit_scale() {
    let path = std::env::temp_dir().join(format!("c8-scale-{}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TABLE users(id int PRIMARY KEY)");
    run(&mut s, "CREATE TABLE orders(id int PRIMARY KEY,user_id int)");
    for (table, count) in [("users",5000), ("orders",20000)] {
        for start in (1..=count).step_by(100) {
            let values = (start..start+100).map(|id| if table=="users" { format!("({id})") }
                else { format!("({id},{})", (id-1)%5000+1) }).collect::<Vec<_>>().join(",");
            run(&mut s, &format!("INSERT INTO {table} VALUES{values}"));
        }
    }
    let now = std::time::Instant::now();
    let result = s.execute("SELECT count(*) FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)", &[], None);
    eprintln!("C8_SCALE users=5000 orders=20000 elapsed={:?} result={result:?}", now.elapsed());
    assert_eq!(result.unwrap().rows[0].values, vec![V::Int(5000)]);
    drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
