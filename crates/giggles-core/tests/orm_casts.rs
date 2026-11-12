use giggles_core::{
    Admin, CoreError, Engine, Limits,
    relational::{DataType, SqlError, Value},
};
use giggles_pg_types::Numeric;

fn engines() -> Vec<Engine> {
    if cfg!(feature = "engine-rocksdb") {
        vec![Engine::Legacy, Engine::V2]
    } else {
        vec![Engine::Legacy]
    }
}

#[test]
fn cast_parameters_in_mutations_and_pagination() {
    for engine in engines() {
        let path = std::env::temp_dir().join(format!("orm-casts-{}", std::process::id()));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        s.execute(
            "CREATE TABLE shop_order (id integer PRIMARY KEY, status varchar(8))",
            &[],
            None,
        )
        .unwrap();
        for id in 1..=3 {
            s.execute("INSERT INTO shop_order VALUES ($1, 'open')", &[Value::Int(id)], None)
                .unwrap();
        }
        let update = s
            .prepare("UPDATE shop_order SET status=$1::VARCHAR WHERE shop_order.id = $2::INTEGER")
            .unwrap()
            .with_parameter_types(&[Some(DataType::Varchar), Some(DataType::SmallInt)])
            .unwrap();
        assert_eq!(
            s.describe(&update).unwrap().parameters,
            [DataType::Varchar, DataType::SmallInt]
        );
        assert_eq!(
            s.execute_prepared(&update, &[Value::Text("paid".into()), Value::Int(1)], None)
                .unwrap()
                .affected,
            1
        );
        let delete = s
            .prepare("DELETE FROM shop_order WHERE id=$1::INTEGER AND status=$2::VARCHAR")
            .unwrap()
            .with_parameter_types(&[Some(DataType::SmallInt), Some(DataType::Varchar)])
            .unwrap();
        assert_eq!(
            s.execute_prepared(&delete, &[Value::Int(1), Value::Text("paid".into())], None)
                .unwrap()
                .affected,
            1
        );
        for (ty, value) in [
            ("integer", Value::Int(1)),
            ("bigint", Value::Int(1)),
            ("numeric", Value::Numeric(Numeric::parse("0.6").unwrap())),
        ] {
            let sql = format!("SELECT id FROM shop_order ORDER BY id LIMIT $1::{ty} OFFSET $2::{ty}");
            let p = s.prepare(&sql).unwrap();
            assert_eq!(
                s.execute_prepared(&p, &[value.clone(), value], None).unwrap().rows[0].values,
                [Value::Int(3)]
            );
            assert_eq!(
                s.execute_prepared(&p, &[Value::Null, Value::Null], None)
                    .unwrap()
                    .rows
                    .len(),
                2
            );
            for (args, error) in [
                ([Value::Int(-1), Value::Int(0)], SqlError::InvalidRowCountInLimitClause),
                (
                    [Value::Int(1), Value::Int(-1)],
                    SqlError::InvalidRowCountInResultOffsetClause,
                ),
            ] {
                let args = if ty == "numeric" {
                    args.map(|v| match v {
                        Value::Int(n) => Value::Numeric(Numeric::parse(&n.to_string()).unwrap()),
                        _ => v,
                    })
                } else {
                    args
                };
                assert_eq!(s.execute_prepared(&p, &args, None), Err(CoreError::Sql(error)));
            }
        }
        for sql in [
            "SELECT count(*) FROM shop_order LIMIT $1::INTEGER OFFSET $2::INTEGER",
            "SELECT id FROM shop_order WHERE EXISTS (SELECT id FROM shop_order LIMIT $1::INTEGER OFFSET $2::INTEGER) ORDER BY id",
        ] {
            assert!(
                !s.execute(sql, &[Value::Int(1), Value::Int(0)], None)
                    .unwrap()
                    .rows
                    .is_empty()
            );
        }
        drop(s);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn opclass_names_build_ordinary_indexes() {
    for engine in engines() {
        let path = std::env::temp_dir().join(format!("orm-opclass-{}", std::process::id()));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE t (id bigint PRIMARY KEY, email varchar(40))", &[], None)
            .unwrap();
        for (i, class) in [
            "text_pattern_ops",
            "varchar_pattern_ops",
            "bpchar_pattern_ops",
            "text_ops",
            "pg_catalog.text_ops",
        ]
        .iter()
        .enumerate()
        {
            s.execute(&format!("CREATE INDEX ix{i} ON t (email {class})"), &[], None)
                .unwrap();
        }
        assert_eq!(
            s.prepare("CREATE INDEX bad ON t (email missing_ops)").unwrap_err(),
            CoreError::Sql(SqlError::UndefinedObject)
        );
        s.execute("INSERT INTO t VALUES (1, 'hello')", &[], None).unwrap();
        assert_eq!(
            s.execute("SELECT id FROM t WHERE email='hello'", &[], None)
                .unwrap()
                .rows[0]
                .values,
            [Value::Int(1)]
        );
        drop(s);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
