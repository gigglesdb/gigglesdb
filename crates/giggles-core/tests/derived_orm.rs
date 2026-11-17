use giggles_core::{Admin, Engine, Limits, relational::{Value, SqlError}};
#[test]
fn derived_aggregates_preserve_inner_pages_and_projection_scope() {
    for engine in [Engine::Legacy, #[cfg(feature = "engine-rocksdb")] Engine::V2] {
        let path = std::env::temp_dir().join(format!("fix7-derived-{}-{engine:?}", std::process::id()));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE users(id int PRIMARY KEY, v int)", &[], None).unwrap();
        s.execute("INSERT INTO users VALUES(1,10),(2,20),(3,30),(4,40)", &[], None).unwrap();
        let sql = "SELECT COUNT(*) AS \"_count$_all\" FROM (SELECT \"public\".\"users\".\"id\" FROM \"public\".\"users\" WHERE \"public\".\"users\".\"id\" = $1 OFFSET $2) AS \"sub\"";
        for (offset, count) in [(0,1),(1,0),(9,0)] {
            assert_eq!(s.execute(sql, &[Value::Int(1),Value::Int(offset)], None).unwrap().rows[0].values, [Value::Int(count)]);
        }
        for (sql, expected) in [
            ("SELECT count(*), sum(id), min(id), max(id) FROM (SELECT id FROM users WHERE id>1 ORDER BY id DESC LIMIT 2 OFFSET 1) sub", vec![2,5,2,3]),
            ("SELECT id FROM (SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1) sub WHERE id>2", vec![3]),
            ("SELECT * FROM (SELECT id AS x FROM users ORDER BY id DESC LIMIT 1 OFFSET 1) sub", vec![3]),
            ("SELECT count(*) FROM (SELECT id FROM users LIMIT 0) sub", vec![0]),
            ("SELECT sub.id FROM (SELECT id FROM users ORDER BY id DESC LIMIT $1 OFFSET $2) sub ORDER BY id LIMIT $3", vec![2]),
        ] {
            let args = if sql.contains("$1") { vec![Value::Int(2),Value::Int(1),Value::Int(1)] } else { vec![] };
            assert_eq!(s.execute(sql, &args, None).unwrap_or_else(|e| panic!("{engine:?}: {sql}: {e:?}")).rows[0].values, expected.into_iter().map(Value::Int).collect::<Vec<_>>());
        }
        assert!(matches!(s.execute("SELECT v FROM (SELECT id FROM users) sub", &[], None), Err(giggles_core::CoreError::Sql(SqlError::UnknownColumn))));
        assert_eq!(s.execute("SELECT count(*) FROM (SELECT count(*) FROM users) sub", &[], None).unwrap().rows[0].values, [Value::Int(1)]);
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}
