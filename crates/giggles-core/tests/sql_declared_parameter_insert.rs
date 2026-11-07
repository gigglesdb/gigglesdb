use giggles_core::{Admin, CoreError, Limits, relational::*};

fn fresh() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join("sql-declared-parameter-insert");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

#[test]
fn declared_insert_assignment_conversions_and_ranges() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t(n INT PRIMARY KEY)", &[], None)
        .unwrap();
    for sql in ["INSERT INTO t VALUES($1)", "INSERT INTO t(n) VALUES($1)"] {
        let prepared = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&[Some(DataType::BigInt)])
            .unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::BigInt]
        );
        assert!(matches!(
            session.execute_prepared(&prepared, &[Value::Int(3_000_000_000)], None),
            Err(CoreError::Sql(SqlError::Overflow))
        ));
    }
    for (table, ty, declared, input, expected) in [
        (
            "big",
            "BIGINT",
            DataType::SmallInt,
            Value::Int(7),
            Value::Int(7),
        ),
        (
            "txt",
            "TEXT",
            DataType::Integer,
            Value::Int(42),
            Value::Text("42".into()),
        ),
        (
            "bytes_text",
            "TEXT",
            DataType::Bytes,
            Value::Bytes(vec![0, 255]),
            Value::Text("\\x00ff".into()),
        ),
        (
            "small",
            "SMALLINT",
            DataType::BigInt,
            Value::Int(5),
            Value::Int(5),
        ),
    ] {
        session
            .execute(
                &format!("CREATE TABLE {table}(id INT PRIMARY KEY,v {ty})"),
                &[],
                None,
            )
            .unwrap();
        let sql = format!("INSERT INTO {table} VALUES(1,$1) RETURNING v");
        let prepared = session
            .prepare(&sql)
            .unwrap()
            .with_parameter_types(&[Some(declared.clone())])
            .unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![declared]
        );
        assert_eq!(
            session
                .execute_prepared(&prepared, &[input], None)
                .unwrap()
                .rows[0]
                .values,
            vec![expected]
        );
    }
    let prepared = session
        .prepare("INSERT INTO small VALUES(2,$1)")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Integer)])
        .unwrap();
    assert!(matches!(
        session.execute_prepared(&prepared, &[Value::Int(40_000)], None),
        Err(CoreError::Sql(SqlError::Overflow))
    ));
}

#[test]
fn incompatible_insert_declarations_fail_before_writes() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,b BYTEA)",
            &[],
            None,
        )
        .unwrap();
    for (column, declared, value) in [
        ("n", DataType::Text, Value::Text("1".into())),
        ("b", DataType::Integer, Value::Int(1)),
        ("b", DataType::Text, Value::Text("x".into())),
    ] {
        for suffix in ["", ",(2,$1)"] {
            let sql = format!("INSERT INTO t(id,{column}) VALUES(1,$1){suffix}");
            let prepared = session
                .prepare(&sql)
                .unwrap()
                .with_parameter_types(&[Some(declared.clone())])
                .unwrap();
            assert!(
                matches!(
                    session.describe(&prepared),
                    Err(CoreError::Sql(SqlError::Type))
                ),
                "{sql}"
            );
            assert!(
                matches!(
                    session.execute_prepared(&prepared, std::slice::from_ref(&value), None),
                    Err(CoreError::Sql(SqlError::Type))
                ),
                "{sql}"
            );
        }
    }
    assert!(
        session
            .execute("SELECT * FROM t ORDER BY id", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn declared_multi_insert_is_atomic_and_conflict_rows_convert() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t(n INT PRIMARY KEY)", &[], None)
        .unwrap();
    let prepared = session
        .prepare("INSERT INTO t VALUES($1),($2) RETURNING n")
        .unwrap()
        .with_parameter_types(&[Some(DataType::BigInt), Some(DataType::BigInt)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::BigInt; 2]
    );
    let result = session
        .execute_prepared(&prepared, &[Value::Int(6), Value::Int(7)], None)
        .unwrap();
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        vec![vec![Value::Int(6)], vec![Value::Int(7)]]
    );
    let before = session
        .execute("SELECT n FROM t ORDER BY n", &[], None)
        .unwrap();
    assert!(matches!(
        session.execute_prepared(&prepared, &[Value::Int(8), Value::Int(3_000_000_000)], None),
        Err(CoreError::Sql(SqlError::Overflow))
    ));
    assert_eq!(
        session
            .execute("SELECT n FROM t ORDER BY n", &[], None)
            .unwrap(),
        before
    );
    session
        .execute(
            "CREATE TABLE text_values(id INT PRIMARY KEY,v TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO text_values VALUES(1,'old')", &[], None)
        .unwrap();
    let conflict = session.prepare("INSERT INTO text_values VALUES(1,$1) ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING v").unwrap().with_parameter_types(&[Some(DataType::Bytes)]).unwrap();
    assert_eq!(
        session.describe(&conflict).unwrap().parameters,
        vec![DataType::Bytes]
    );
    assert_eq!(
        session
            .execute_prepared(&conflict, &[Value::Bytes(vec![0, 255])], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("\\x00ff".into())]
    );
}

#[test]
fn insert_declaration_lengths_holes_and_undeclared_inference() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,s TEXT)", &[], None)
        .unwrap();
    for sql in [
        "INSERT INTO t VALUES($1,$2,$3)",
        "INSERT INTO t VALUES($1,$2,$3),(4,5,'x')",
    ] {
        assert!(matches!(
            session
                .prepare(sql)
                .unwrap()
                .with_parameter_types(&[None, None, None, None]),
            Err(SqlError::Parameters)
        ));
        for declarations in [
            vec![],
            vec![None],
            vec![None, None, None],
            vec![Some(DataType::SmallInt)],
        ] {
            let expected = if declarations.first() == Some(&Some(DataType::SmallInt)) {
                DataType::SmallInt
            } else {
                DataType::Integer
            };
            let prepared = session
                .prepare(sql)
                .unwrap()
                .with_parameter_types(&declarations)
                .unwrap();
            assert_eq!(
                session.describe(&prepared).unwrap().parameters,
                vec![expected, DataType::Integer, DataType::Text]
            );
        }
        let prepared = session.prepare(sql).unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Integer, DataType::Integer, DataType::Text]
        );
    }
    let prepared = session.prepare("INSERT INTO t(id,n) VALUES(1,$1)").unwrap();
    assert!(matches!(
        session.execute_prepared(&prepared, &[Value::Int(3_000_000_000)], None),
        Err(CoreError::Sql(SqlError::Type))
    ));
    let conflict = session
        .prepare("INSERT INTO t(id,n) VALUES($1,$1)")
        .unwrap();
    assert_eq!(
        session.describe(&conflict).unwrap().parameters,
        vec![DataType::Integer]
    );
}

#[test]
fn declared_conflict_set_assignments_and_guard_comparisons() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,s TEXT)", &[], None)
        .unwrap();
    session
        .execute("INSERT INTO t VALUES(1,10,'a')", &[], None)
        .unwrap();
    let sql = "INSERT INTO t(id,n) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET n=$3 RETURNING n";
    let prepared = session
        .prepare(sql)
        .unwrap()
        .with_parameter_types(&[
            Some(DataType::BigInt),
            Some(DataType::BigInt),
            Some(DataType::BigInt),
        ])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::BigInt; 3]
    );
    assert_eq!(
        session
            .execute_prepared(
                &prepared,
                &[Value::Int(1), Value::Int(5), Value::Int(7)],
                None,
            )
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(7)]
    );
    session
        .execute("UPDATE t SET n=10 WHERE id=1", &[], None)
        .unwrap();
    assert!(matches!(
        session.execute_prepared(
            &prepared,
            &[Value::Int(1), Value::Int(5), Value::Int(3_000_000_000)],
            None,
        ),
        Err(CoreError::Sql(SqlError::Overflow))
    ));
    assert_eq!(
        session
            .execute("SELECT n FROM t WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(10)]
    );
    let prepared = session
        .prepare("INSERT INTO t(id,n) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET s=$3 RETURNING s")
        .unwrap()
        .with_parameter_types(&[
            Some(DataType::BigInt),
            Some(DataType::BigInt),
            Some(DataType::Integer),
        ])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::BigInt, DataType::BigInt, DataType::Integer]
    );
    assert_eq!(
        session
            .execute_prepared(
                &prepared,
                &[Value::Int(1), Value::Int(5), Value::Int(42)],
                None,
            )
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("42".into())]
    );
    let prepared = session
        .prepare(sql)
        .unwrap()
        .with_parameter_types(&[
            Some(DataType::BigInt),
            Some(DataType::BigInt),
            Some(DataType::Text),
        ])
        .unwrap();
    assert!(matches!(
        session.describe(&prepared),
        Err(CoreError::Sql(SqlError::Type))
    ));
    let prepared = session
        .prepare("INSERT INTO t(id,n) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET n=excluded.n WHERE t.n<$3 RETURNING n")
        .unwrap()
        .with_parameter_types(&[
            Some(DataType::BigInt),
            Some(DataType::BigInt),
            Some(DataType::BigInt),
        ])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::BigInt; 3]
    );
    assert_eq!(
        session
            .execute_prepared(
                &prepared,
                &[Value::Int(1), Value::Int(5), Value::Int(100)],
                None,
            )
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(5)]
    );
}

#[test]
fn declared_null_conflict_set_into_not_null_column_is_not_null_violation() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT NOT NULL,s SMALLINT,x TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t(id,n) VALUES(1,10)", &[], None)
        .unwrap();
    let before = session.execute("SELECT * FROM t", &[], None).unwrap();
    let sql = "INSERT INTO t(id,n) VALUES(1,1) ON CONFLICT(id) DO UPDATE SET n=$1";
    for declared in [false, true] {
        let prepared = session.prepare(sql).unwrap();
        let prepared = if declared {
            prepared
                .with_parameter_types(&[Some(DataType::Integer)])
                .unwrap()
        } else {
            prepared
        };
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Integer]
        );
        assert!(matches!(
            session.execute_prepared(&prepared, &[Value::Null], None),
            Err(CoreError::Sql(SqlError::Null))
        ));
        assert_eq!(
            session.execute("SELECT * FROM t", &[], None).unwrap(),
            before
        );
    }
    assert!(matches!(
        session.execute(
            "INSERT INTO t(id,n) VALUES(1,1) ON CONFLICT(id) DO UPDATE SET n=NULL",
            &[],
            None
        ),
        Err(CoreError::Sql(SqlError::Null))
    ));
    assert_eq!(
        session.execute("SELECT * FROM t", &[], None).unwrap(),
        before
    );
    let prepared = session
        .prepare("INSERT INTO t(id,n) VALUES(9,1) ON CONFLICT(id) DO UPDATE SET n=$1")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Integer)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::Integer]
    );
    assert_eq!(
        session
            .execute_prepared(&prepared, &[Value::Null], None)
            .unwrap()
            .affected,
        1
    );
    assert_eq!(
        session
            .execute("SELECT n FROM t WHERE id=9", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1)]
    );
}

#[test]
fn declared_insert_expressions_preserve_types_and_assignment_ranges() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT NOT NULL,s SMALLINT,x TEXT)",
            &[],
            None,
        )
        .unwrap();
    for (sql, declared, input, column, expected) in [
        (
            "INSERT INTO t(id,n) VALUES(2,$1+1)",
            DataType::BigInt,
            5,
            "n",
            Value::Int(6),
        ),
        (
            "INSERT INTO t(id,n) VALUES(3,abs($1))",
            DataType::BigInt,
            -9,
            "n",
            Value::Int(9),
        ),
        (
            "INSERT INTO t(id,n) VALUES(4,abs($1::integer))",
            DataType::BigInt,
            -4,
            "n",
            Value::Int(4),
        ),
        (
            "INSERT INTO t(id,n,x) VALUES(6,1,$1+1)",
            DataType::Integer,
            41,
            "x",
            Value::Text("42".into()),
        ),
    ] {
        let prepared = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&[Some(declared.clone())])
            .unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![declared]
        );
        // Run failures before successful inserts so duplicate keys cannot hide them.
        if sql.contains("VALUES(3,") || sql.contains("VALUES(4,") {
            let input = if sql.contains("VALUES(3,") {
                -3_000_000_000
            } else {
                3_000_000_000
            };
            assert!(matches!(
                session.execute_prepared(&prepared, &[Value::Int(input)], None),
                Err(CoreError::Sql(SqlError::Overflow))
            ));
            let id = if sql.contains("VALUES(3,") { 3 } else { 4 };
            assert!(
                session
                    .execute(&format!("SELECT * FROM t WHERE id={id}"), &[], None)
                    .unwrap()
                    .rows
                    .is_empty()
            );
        }
        assert_eq!(
            session
                .execute_prepared(&prepared, &[Value::Int(input)], None)
                .unwrap()
                .affected,
            1
        );
        let id = if column == "x" {
            6
        } else if sql.contains("VALUES(2,") {
            2
        } else if sql.contains("VALUES(3,") {
            3
        } else {
            4
        };
        assert_eq!(
            session
                .execute(&format!("SELECT {column} FROM t WHERE id={id}"), &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![expected]
        );
    }
    for (sql, declared) in [
        ("INSERT INTO t(id,n) VALUES(5,$1+1)", DataType::Text),
        ("INSERT INTO t(id,n) VALUES(8,abs($1))", DataType::Bytes),
    ] {
        let prepared = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&[Some(declared.clone())])
            .unwrap();
        // PG 18.6 gives 42883 for both text + integer and abs(bytea).
        let expected = SqlError::UndefinedFunction;
        assert!(matches!(
            session.describe(&prepared),
            Err(CoreError::Sql(error)) if error == expected
        ));
    }
    let prepared = session
        .prepare("INSERT INTO t(id,n,s) VALUES(7,1,$1+1)")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Integer)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::Integer]
    );
    assert!(matches!(
        session.execute_prepared(&prepared, &[Value::Int(40_000)], None),
        Err(CoreError::Sql(SqlError::Overflow))
    ));
    assert!(
        session
            .execute("SELECT * FROM t WHERE id=7", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    let inferred = session
        .prepare("INSERT INTO t(id,n) VALUES(12,$1+1)")
        .unwrap();
    assert_eq!(
        session.describe(&inferred).unwrap().parameters,
        vec![DataType::Integer]
    );
    let remapped = session
        .prepare("INSERT INTO t(id,n) VALUES(13,$2+1) RETURNING $1,n")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Text), Some(DataType::BigInt)])
        .unwrap();
    let description = session.describe(&remapped).unwrap();
    assert_eq!(
        description.parameters,
        vec![DataType::Text, DataType::BigInt]
    );
    assert_eq!(
        description
            .columns
            .iter()
            .map(|column| column.data_type.clone())
            .collect::<Vec<_>>(),
        vec![DataType::Text, DataType::Integer]
    );
    assert_eq!(
        session
            .execute_prepared(
                &remapped,
                &[Value::Text("label".into()), Value::Int(40)],
                None
            )
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("label".into()), Value::Int(41)]
    );
}

#[test]
fn conflict_bare_parameter_guards_require_boolean() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT NOT NULL,s SMALLINT,x TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t(id,n) VALUES(1,10)", &[], None)
        .unwrap();
    let sql = "INSERT INTO t(id,n) VALUES(1,1) ON CONFLICT(id) DO UPDATE SET n=2 WHERE $1";
    for declared in [
        DataType::Integer,
        DataType::BigInt,
        DataType::Text,
        DataType::Bytes,
    ] {
        let prepared = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&[Some(declared)])
            .unwrap();
        assert!(matches!(
            session.describe(&prepared),
            Err(CoreError::Sql(SqlError::Type))
        ));
        assert_eq!(
            session
                .execute("SELECT n FROM t WHERE id=1", &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Int(10)]
        );
    }
    for declared in [false, true] {
        session
            .execute("UPDATE t SET n=10 WHERE id=1", &[], None)
            .unwrap();
        let prepared = session.prepare(sql).unwrap();
        let prepared = if declared {
            prepared
                .with_parameter_types(&[Some(DataType::Boolean)])
                .unwrap()
        } else {
            prepared
        };
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Boolean]
        );
        assert_eq!(
            session
                .execute_prepared(&prepared, &[Value::Bool(false)], None)
                .unwrap()
                .affected,
            0
        );
        assert_eq!(
            session
                .execute("SELECT n FROM t WHERE id=1", &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Int(10)]
        );
        assert_eq!(
            session
                .execute_prepared(&prepared, &[Value::Bool(true)], None)
                .unwrap()
                .affected,
            1
        );
        assert_eq!(
            session
                .execute("SELECT n FROM t WHERE id=1", &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Int(2)]
        );
    }
}
