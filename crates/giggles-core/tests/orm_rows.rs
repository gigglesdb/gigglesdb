use giggles_core::{Admin, Engine, Limits, relational::Value};
#[test]
fn row_comparisons_preserve_unknown_and_infer_parameters() {
    for engine in [Engine::Legacy, Engine::V2].into_iter().filter(|e| *e == Engine::Legacy || cfg!(feature = "engine-rocksdb")) {
        let path = std::env::temp_dir().join(format!("orm-rows-{}", std::process::id()));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE t (id integer PRIMARY KEY, sku text)", &[], None).unwrap();
        s.execute("INSERT INTO t VALUES (1,'book'),(2,NULL)", &[], None).unwrap();
        for (predicate, expected) in [
            ("(1,NULL)=(1,2)", Value::Null), ("(1,NULL)=(2,2)", Value::Bool(false)),
            ("(NULL,1)=(2,2)", Value::Bool(false)),
            ("(1,NULL)<>(1,2)", Value::Null), ("(1,NULL)<>(2,2)", Value::Bool(true)),
            ("(1,NULL) IN ((2,2),(1,2))", Value::Null),
            ("(1,NULL) NOT IN ((2,2),(1,2))", Value::Null),
            ("(1,2) IN ((1,NULL),(1,2))", Value::Bool(true)),
            ("(1,2)!=(1,3)", Value::Bool(true)),
        ] {
            assert_eq!(s.execute(&format!("SELECT {predicate}"), &[], None).unwrap_or_else(|e| panic!("{predicate}: {e:?}")).rows[0].values, [expected], "{predicate}");
        }
        assert_eq!(s.prepare("SELECT (1,2)=(1,2,3)").unwrap_err(), giggles_core::CoreError::Sql(giggles_core::relational::SqlError::Syntax));
        let args = [Value::Int(1), Value::Text("book".into())];
        assert_eq!(s.execute("SELECT t.id FROM t INNER JOIN t other ON t.id=other.id WHERE (t.id,t.sku)=($1,$2)", &args, None).unwrap().rows.len(), 1);
        assert_eq!(s.execute("DELETE FROM t WHERE (id,sku) IN (($1,$2),(2,NULL))", &args, None).unwrap().affected, 1);
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}
