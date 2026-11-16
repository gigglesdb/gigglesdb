mod common;
use giggles_core::{Admin, Engine, Limits, relational::*};
use giggles_core::operations::{Bindings, Engine as NativeEngine, Expr, Intent, Request, ValueExpr};

fn engines() -> Vec<Engine> {
    #[allow(unused_mut)]
    let mut engines = vec![Engine::Legacy];
    #[cfg(feature = "engine-rocksdb")]
    engines.push(Engine::V2);
    engines
}

#[test]
fn sql_assignments_precede_column_not_null() {
    for engine in engines() {
        let path = common::fresh("parity-update-eval-order");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for sql in ["CREATE TABLE t(a int,b smallint NOT NULL,c int)",
            "INSERT INTO t VALUES(1,1,NULL)",
            "CREATE TABLE t3(x int NOT NULL,y smallint,z int)",
            "INSERT INTO t3 VALUES(1,1,70000)"] {
            s.execute(sql, &[], None).unwrap();
        }
        // PostgreSQL 18.6: expression/assignment failures precede column NOT NULL.
        for (sql, table, state) in [
            ("UPDATE t SET a=NULL,b=100000", "t", "22003"),
            ("UPDATE t SET b=NULL,c=1", "t", "23502"),
            ("UPDATE t SET b=100000,c=NULL", "t", "22003"),
            ("UPDATE t SET b=NULL,a=1/0", "t", "22012"),
            ("UPDATE t SET a=1/0,b=NULL", "t", "22012"),
            ("UPDATE t3 SET x=NULL,y=70000", "t3", "22003"),
            ("UPDATE t3 SET x=NULL,y=z", "t3", "22003"),
            ("UPDATE t3 SET y=z,x=NULL", "t3", "22003"),
            ("UPDATE t3 SET x=NULL,y=z/0", "t3", "22012"),
            ("UPDATE t3 SET y=z/0,x=NULL", "t3", "22012"),
            ("INSERT INTO t3 VALUES(NULL,70000,1)", "t3", "22003"),
        ] {
            let query = format!("SELECT * FROM {table}");
            let before = s.execute(&query, &[], None).unwrap();
            let error = s.execute(sql, &[], None).unwrap_err();
            assert_eq!(giggles_core::sqlstate(&error), state, "{engine:?}: {sql}: {error:?}");
            assert_eq!(s.execute(&query, &[], None).unwrap(), before, "{sql}");
        }
        assert_eq!(s.execute("UPDATE t3 SET x=NULL,y=z WHERE false", &[], None).unwrap().affected, 0);
        let error = s.execute("UPDATE t3 SET x=NULL,y=70000 WHERE false", &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "22003");
        assert_eq!(s.execute("SELECT * FROM t3", &[], None).unwrap().rows[0].values,
            vec![Value::Int(1), Value::Int(1), Value::Int(70000)]);
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn native_assignments_precede_column_not_null() {
    for engine in engines() {
        let path = common::fresh("parity-native-update-eval-order");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let native = NativeEngine::new(admin.clone());
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE t(id int PRIMARY KEY,x int NOT NULL,y smallint,z int)", &[], None).unwrap();
        s.execute("INSERT INTO t VALUES(1,1,1,70000)", &[], None).unwrap();
        let before = s.execute("SELECT * FROM t", &[], None).unwrap();
        let lit = |v| Expr::Value(ValueExpr::Literal(v));
        for reversed in [false, true] {
            for computed in [false, true] {
                let mut values = vec![("x".into(), lit(Value::Null)),
                    ("y".into(), if computed { Expr::Field("z".into()) } else { lit(Value::Int(70000)) })];
                if reversed { values.reverse(); }
                let request = if computed {
                    Request::UpdateExpressions { table: "t".into(), filter: lit(Value::Bool(true)),
                        guard: None, values, intent: Intent::One }
                } else {
                    Request::Update { table: "t".into(), filter: lit(Value::Bool(true)), guard: None,
                        values: values.into_iter().map(|(n, e)| {
                            let Expr::Value(v) = e else { unreachable!() }; (n, v)
                        }).collect(), intent: Intent::One }
                };
                let error = native.execute(&request, &Bindings::new()).unwrap_err();
                assert_eq!(error.sqlstate(), Some("22003"), "{engine:?}: {computed}: {error:?}");
                assert_eq!(s.execute("SELECT * FROM t", &[], None).unwrap(), before);
            }
        }
        drop(s); drop(native); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}
