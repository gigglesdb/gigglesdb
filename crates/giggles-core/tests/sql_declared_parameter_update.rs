use giggles_core::{Admin, CoreError, Limits, relational::*};

fn fresh() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join("sql-declared-parameter-update");
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
fn undeclared_update_position_defaults_to_text() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,b BYTEA,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t(id,n) VALUES(1,0)", &[], None)
        .unwrap();
    for empty_declaration in [false, true] {
        let prepared = session
            .prepare("UPDATE t SET n=position($1 in $2) WHERE id=1")
            .unwrap();
        let prepared = if empty_declaration {
            prepared.with_parameter_types(&[]).unwrap()
        } else {
            prepared
        };
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Text; 2]
        );
        assert_eq!(
            session
                .execute_prepared(
                    &prepared,
                    &[Value::Text("z".into()), Value::Text("éz".into())],
                    None
                )
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
#[test]
fn declared_bytea_update_position_and_nulls() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,b BYTEA,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t(id,n) VALUES(1,0)", &[], None)
        .unwrap();
    let prepared = session
        .prepare("UPDATE t SET n=position($1 in $2) WHERE id=1")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes), Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::Bytes; 2]
    );
    for (needle, haystack, expected) in [
        (
            Value::Bytes(vec![0, 255]),
            Value::Bytes(vec![1, 0, 255, 2]),
            Value::Int(2),
        ),
        (
            Value::Bytes(vec![255, 0]),
            Value::Bytes(vec![1, 0, 255, 2]),
            Value::Int(0),
        ),
        (Value::Null, Value::Bytes(vec![1, 0, 255, 2]), Value::Null),
        (Value::Bytes(vec![0, 255]), Value::Null, Value::Null),
    ] {
        assert_eq!(
            session
                .execute_prepared(&prepared, &[needle, haystack], None)
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
            vec![expected]
        );
    }
}

#[test]
fn update_declaration_prefix_holes_searched_and_returning() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,b BYTEA,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t(id,n) VALUES(1,0),(2,0)", &[], None)
        .unwrap();
    let values = [Value::Bytes(vec![0, 255]), Value::Bytes(vec![1, 0, 255, 2])];
    let prepared = session
        .prepare("UPDATE t SET n=position($1 in $2) WHERE id=$3")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes), Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::Bytes, DataType::Bytes, DataType::Integer]
    );
    assert_eq!(
        session
            .execute_prepared(
                &prepared,
                &[values[0].clone(), values[1].clone(), Value::Int(1)],
                None
            )
            .unwrap()
            .affected,
        1
    );
    let text = session
        .prepare("UPDATE t SET n=position($1 in $2) WHERE id=1")
        .unwrap()
        .with_parameter_types(&[None, None])
        .unwrap();
    assert_eq!(
        session.describe(&text).unwrap().parameters,
        vec![DataType::Text; 2]
    );
    assert_eq!(
        session
            .execute_prepared(
                &text,
                &[Value::Text("z".into()), Value::Text("az".into())],
                None
            )
            .unwrap()
            .affected,
        1
    );
    let searched = session
        .prepare("UPDATE t SET n=position($1 in $2) WHERE n IS NOT NULL OR id > 0")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes), Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&searched).unwrap().parameters,
        vec![DataType::Bytes; 2]
    );
    assert_eq!(
        session
            .execute_prepared(&searched, &values, None)
            .unwrap()
            .affected,
        2
    );
    assert!(
        session
            .execute("SELECT n FROM t ORDER BY id", &[], None)
            .unwrap()
            .rows
            .iter()
            .all(|row| row.values == vec![Value::Int(2)])
    );
    let returning = session
        .prepare("UPDATE t SET n=position($1 in $2) WHERE id=1 RETURNING n")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes), Some(DataType::Bytes)])
        .unwrap();
    let description = session.describe(&returning).unwrap();
    assert_eq!(description.parameters, vec![DataType::Bytes; 2]);
    assert_eq!(description.columns.len(), 1);
    assert_eq!(description.columns[0].name, "n");
    assert_eq!(description.columns[0].data_type, DataType::Integer);
    assert_eq!(
        session
            .execute_prepared(&returning, &values, None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(2)]
    );
}

#[test]
fn plain_update_declarations_keep_column_conflicts_and_length_checks() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,b BYTEA,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t(id,n,s) VALUES(1,0,'unchanged')", &[], None)
        .unwrap();
    let prepared = session
        .prepare("UPDATE t SET b=$1 WHERE id=$2")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::Bytes, DataType::Integer]
    );
    assert_eq!(
        session
            .execute_prepared(
                &prepared,
                &[Value::Bytes(vec![0, 255]), Value::Int(1)],
                None
            )
            .unwrap()
            .affected,
        1
    );
    assert_eq!(
        session
            .execute("SELECT b FROM t WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Bytes(vec![0, 255])]
    );
    let converted = session
        .prepare("UPDATE t SET s=$1 WHERE id=1")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&converted).unwrap().parameters,
        vec![DataType::Bytes]
    );
    session
        .execute_prepared(&converted, &[Value::Bytes(vec![0])], None)
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT s FROM t WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("\\x00".into())]
    );
    let conflict = session
        .prepare("UPDATE t SET s=$1 WHERE id=$2")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Text), Some(DataType::Bytes)])
        .unwrap();
    assert!(matches!(
        session.describe(&conflict),
        Err(CoreError::Sql(SqlError::UndefinedFunction))
    ));
    assert!(matches!(
        session.execute_prepared(
            &conflict,
            &[Value::Text("changed".into()), Value::Bytes(vec![0])],
            None
        ),
        Err(CoreError::Sql(SqlError::UndefinedFunction))
    ));
    assert_eq!(
        session
            .execute("SELECT s FROM t WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("\\x00".into())]
    );
    assert!(matches!(
        session
            .prepare("UPDATE t SET n=position($1 in $2) WHERE id=1")
            .unwrap()
            .with_parameter_types(&[
                Some(DataType::Bytes),
                Some(DataType::Bytes),
                Some(DataType::Bytes)
            ]),
        Err(SqlError::Parameters)
    ));
}

#[test]
fn declared_plain_assignments_convert_and_check_destination_ranges() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,sm SMALLINT,big BIGINT,b BYTEA,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO t(id,n,sm,big) VALUES(1,0,0,0),(2,0,0,0)",
            &[],
            None,
        )
        .unwrap();
    for (column, declared, input, expected) in [
        ("n", DataType::BigInt, Value::Int(42), Value::Int(42)),
        ("big", DataType::SmallInt, Value::Int(12), Value::Int(12)),
        (
            "s",
            DataType::Integer,
            Value::Int(42),
            Value::Text("42".into()),
        ),
        (
            "s",
            DataType::Bytes,
            Value::Bytes(vec![0, 255]),
            Value::Text("\\x00ff".into()),
        ),
        ("s", DataType::Bytes, Value::Null, Value::Null),
        ("s", DataType::Integer, Value::Null, Value::Null),
    ] {
        let sql = format!("UPDATE t SET {column}=$1 WHERE id=1 RETURNING {column}");
        let prepared = session
            .prepare(&sql)
            .unwrap()
            .with_parameter_types(&[Some(declared.clone())])
            .unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![declared],
            "{sql}"
        );
        assert_eq!(
            session
                .execute_prepared(&prepared, &[input], None)
                .unwrap()
                .rows[0]
                .values,
            vec![expected],
            "{sql}"
        );
    }
    for (sql, declared, input) in [
        (
            "UPDATE t SET n=$1 WHERE id=1",
            DataType::BigInt,
            Value::Int(3_000_000_000),
        ),
        (
            "UPDATE t SET sm=$1 WHERE id=1",
            DataType::Integer,
            Value::Int(40_000),
        ),
        (
            "UPDATE t SET n=$1 WHERE n IS NOT NULL OR id>0",
            DataType::BigInt,
            Value::Int(3_000_000_000),
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
        let before = session
            .execute("SELECT * FROM t ORDER BY id", &[], None)
            .unwrap();
        assert!(
            matches!(
                session.execute_prepared(&prepared, &[input], None),
                Err(CoreError::Sql(SqlError::Overflow))
            ),
            "{sql}"
        );
        assert_eq!(
            session
                .execute("SELECT * FROM t ORDER BY id", &[], None)
                .unwrap(),
            before
        );
    }
    // Mixing a scalar assignment selects UpdateExpressions; its plain SET value
    // uses the same declared assignment conversion as the plain UPDATE path.
    let mixed = session
        .prepare("UPDATE t SET s=$1,n=n+1 WHERE id=1 RETURNING s,n")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&mixed).unwrap().parameters,
        vec![DataType::Bytes]
    );
    assert_eq!(
        session
            .execute_prepared(&mixed, &[Value::Bytes(vec![0, 255])], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("\\x00ff".into()), Value::Int(43)]
    );
}

#[test]
fn declared_plain_assignment_and_comparison_errors_are_distinct_and_atomic() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,sm SMALLINT,big BIGINT,b BYTEA,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO t(id,n,sm,big) VALUES(1,1,1,1),(2,2,2,2)",
            &[],
            None,
        )
        .unwrap();
    for (sql, declared, value, error) in [
        (
            "UPDATE t SET n=$1 WHERE id=1",
            DataType::Bytes,
            Value::Bytes(vec![0]),
            SqlError::Type,
        ),
        (
            "UPDATE t SET n=$1 WHERE id=1",
            DataType::Text,
            Value::Text("1".into()),
            SqlError::Type,
        ),
        (
            "UPDATE t SET b=$1 WHERE id=1",
            DataType::Text,
            Value::Text("x".into()),
            SqlError::Type,
        ),
        (
            "UPDATE t SET b=$1 WHERE id=1",
            DataType::Integer,
            Value::Int(1),
            SqlError::Type,
        ),
        (
            "UPDATE t SET n=9 WHERE n=$1",
            DataType::Bytes,
            Value::Bytes(vec![0]),
            SqlError::UndefinedFunction,
        ),
        (
            "UPDATE t SET n=9 WHERE n=$1",
            DataType::Text,
            Value::Text("1".into()),
            SqlError::UndefinedFunction,
        ),
        (
            "UPDATE t SET n=9 WHERE s=$1",
            DataType::Integer,
            Value::Int(1),
            SqlError::UndefinedFunction,
        ),
        (
            "UPDATE t SET n=9 WHERE s=$1",
            DataType::Bytes,
            Value::Bytes(vec![0]),
            SqlError::UndefinedFunction,
        ),
        (
            "UPDATE t SET n=9 WHERE b=$1",
            DataType::Text,
            Value::Text("x".into()),
            SqlError::UndefinedFunction,
        ),
        (
            "UPDATE t SET n=9 WHERE n=$1 OR id>0",
            DataType::Bytes,
            Value::Bytes(vec![0]),
            SqlError::UndefinedFunction,
        ),
    ] {
        let prepared = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&[Some(declared)])
            .unwrap();
        let before = session
            .execute("SELECT * FROM t ORDER BY id", &[], None)
            .unwrap();
        assert!(
            matches!(session.describe(&prepared), Err(CoreError::Sql(actual)) if actual == error),
            "describe {sql}"
        );
        assert!(
            matches!(session.execute_prepared(&prepared, &[value], None), Err(CoreError::Sql(actual)) if actual == error),
            "execute {sql}"
        );
        assert_eq!(
            session
                .execute("SELECT * FROM t ORDER BY id", &[], None)
                .unwrap(),
            before,
            "{sql}"
        );
    }
    let comparison = session
        .prepare("UPDATE t SET n=9 WHERE big=$1 RETURNING id")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Integer)])
        .unwrap();
    assert_eq!(
        session.describe(&comparison).unwrap().parameters,
        vec![DataType::Integer]
    );
    assert_eq!(
        session
            .execute_prepared(&comparison, &[Value::Int(1)], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1)]
    );
}

#[test]
fn undeclared_mutation_column_conflicts_and_integer_key_fallback() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,big BIGINT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t VALUES(1,1,1),(2,2,2)", &[], None)
        .unwrap();
    let conflict = session.prepare("UPDATE t SET big=$1 WHERE n=$1").unwrap();
    assert!(matches!(
        session.describe(&conflict),
        Err(CoreError::Sql(SqlError::Type))
    ));
    assert!(matches!(
        session.execute_prepared(&conflict, &[Value::Int(1)], None),
        Err(CoreError::Sql(SqlError::Type))
    ));
    let point = session.prepare("UPDATE t SET n=9 WHERE id=$1").unwrap();
    assert_eq!(
        session.describe(&point).unwrap().parameters,
        vec![DataType::Integer]
    );
    // Routing reaches the searched parameter check rather than rejecting the key.
    assert!(matches!(
        session.execute_prepared(&point, &[Value::Int(3_000_000_000)], None),
        Err(CoreError::Sql(SqlError::Type))
    ));
    let before = session
        .execute("SELECT * FROM t ORDER BY id", &[], None)
        .unwrap();
    let literal = session
        .prepare("UPDATE t SET n=9 WHERE id=3000000000")
        .unwrap();
    assert_eq!(
        session
            .execute_prepared(&literal, &[], None)
            .unwrap()
            .affected,
        0
    );
    assert_eq!(
        session
            .execute("SELECT * FROM t ORDER BY id", &[], None)
            .unwrap(),
        before
    );
}

#[test]
fn reversed_update_comparisons_honor_declared_types() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t(id INT PRIMARY KEY,s TEXT)", &[], None)
        .unwrap();
    session
        .execute("INSERT INTO t VALUES(1,'one'),(2,'two')", &[], None)
        .unwrap();
    let prepared = session
        .prepare("UPDATE t SET s='r' WHERE $1=id")
        .unwrap()
        .with_parameter_types(&[Some(DataType::BigInt)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::BigInt]
    );
    assert_eq!(
        session
            .execute_prepared(&prepared, &[Value::Int(3_000_000_000)], None)
            .unwrap()
            .affected,
        0
    );
    assert_eq!(
        session
            .execute_prepared(&prepared, &[Value::Int(1)], None)
            .unwrap()
            .affected,
        1
    );
    assert_eq!(
        session
            .execute("SELECT s FROM t WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("r".into())]
    );
    for sql in [
        "UPDATE t SET s='bad' WHERE $1=id",
        "UPDATE t SET s='bad' WHERE id=$1",
    ] {
        let prepared = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&[Some(DataType::Bytes)])
            .unwrap();
        assert!(
            matches!(
                session.describe(&prepared),
                Err(CoreError::Sql(SqlError::UndefinedFunction))
            ),
            "describe {sql}"
        );
        assert!(
            matches!(
                session.execute_prepared(&prepared, &[Value::Bytes(vec![1])], None),
                Err(CoreError::Sql(SqlError::UndefinedFunction))
            ),
            "execute {sql}"
        );
    }
    assert_eq!(
        session
            .execute("SELECT s FROM t WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("r".into())]
    );
}

#[test]
fn integer_literal_comparisons_do_not_narrow_to_column_ranges() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,b SMALLINT,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO t VALUES(1,1,'one'),(2,2,'two'),(3,3,'three')",
            &[],
            None,
        )
        .unwrap();
    for sql in [
        "UPDATE t SET s='bad' WHERE id=3000000000",
        "UPDATE t SET s='bad' WHERE id=3000000000 RETURNING id",
    ] {
        let result = session.execute(sql, &[], None).unwrap();
        assert_eq!(result.affected, 0, "{sql}");
        assert!(result.rows.is_empty(), "{sql}");
    }
    assert!(
        session
            .execute("SELECT * FROM t WHERE id=3000000000", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    for (sql, expected) in [
        ("SELECT count(*) FROM t WHERE id<3000000000", 3),
        ("SELECT count(*) FROM t WHERE b=40000", 0),
        ("SELECT count(*) FROM t WHERE id>3000000000", 0),
        ("SELECT count(*) FROM t WHERE id<>3000000000", 3),
    ] {
        assert_eq!(
            session.execute(sql, &[], None).unwrap().rows[0].values,
            vec![Value::Int(expected)],
            "{sql}"
        );
    }
    assert_eq!(
        session
            .execute("SELECT s FROM t ORDER BY id", &[], None)
            .unwrap()
            .rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("one".into())],
            vec![Value::Text("two".into())],
            vec![Value::Text("three".into())]
        ]
    );
}

#[test]
fn declared_update_expressions_preserve_types_and_assignment_ranges() {
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
        .execute("INSERT INTO t(id,n,s,x) VALUES(1,1,1,'old')", &[], None)
        .unwrap();
    for (assignment, declared, input, expected) in [
        ("n=$1+1", DataType::BigInt, 5, Value::Int(6)),
        ("n=abs($1)", DataType::BigInt, -7, Value::Int(7)),
        ("s=$1+1", DataType::Integer, 5, Value::Int(6)),
        ("n=$1+1", DataType::SmallInt, 5, Value::Int(6)),
        ("x=$1+1", DataType::Integer, 41, Value::Text("42".into())),
        ("x=abs($1)", DataType::BigInt, -5, Value::Text("5".into())),
    ] {
        let prepared = session
            .prepare(&format!("UPDATE t SET {assignment} WHERE id=1"))
            .unwrap()
            .with_parameter_types(&[Some(declared.clone())])
            .unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![declared]
        );
        assert_eq!(
            session
                .execute_prepared(&prepared, &[Value::Int(input)], None)
                .unwrap()
                .affected,
            1
        );
        let column = assignment.split('=').next().unwrap();
        assert_eq!(
            session
                .execute(&format!("SELECT {column} FROM t WHERE id=1"), &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![expected]
        );
    }
    for (assignment, declared, input) in [
        ("n=$1+1", DataType::BigInt, 3_000_000_000),
        ("n=abs($1)", DataType::BigInt, -3_000_000_000),
        ("s=$1+1", DataType::Integer, 40_000),
        ("n=$1+1", DataType::Integer, i64::from(i32::MAX)),
    ] {
        let before = session
            .execute("SELECT * FROM t WHERE id=1", &[], None)
            .unwrap();
        let prepared = session
            .prepare(&format!("UPDATE t SET {assignment} WHERE id=1"))
            .unwrap()
            .with_parameter_types(&[Some(declared.clone())])
            .unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![declared]
        );
        assert!(matches!(
            session.execute_prepared(&prepared, &[Value::Int(input)], None),
            Err(CoreError::Sql(SqlError::Overflow))
        ));
        assert_eq!(
            session
                .execute("SELECT * FROM t WHERE id=1", &[], None)
                .unwrap(),
            before
        );
    }
    for (assignment, declared) in [("n=$1+1", DataType::Text), ("n=abs($1)", DataType::Bytes)] {
        let prepared = session
            .prepare(&format!("UPDATE t SET {assignment} WHERE id=1"))
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
    let inferred = session.prepare("UPDATE t SET n=$1+1 WHERE id=1").unwrap();
    assert_eq!(
        session.describe(&inferred).unwrap().parameters,
        vec![DataType::Integer]
    );
}

#[test]
fn null_update_assignments_are_not_null_violations_only_for_matched_rows() {
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
    assert!(matches!(
        session.execute("UPDATE t SET n=NULL WHERE id=1", &[], None),
        Err(CoreError::Sql(SqlError::Null))
    ));
    assert_eq!(
        session.execute("SELECT * FROM t", &[], None).unwrap(),
        before
    );
    for declared in [None, Some(DataType::Integer), Some(DataType::BigInt)] {
        let prepared = session.prepare("UPDATE t SET n=$1 WHERE id=1").unwrap();
        let prepared = if let Some(ty) = declared.clone() {
            prepared.with_parameter_types(&[Some(ty)]).unwrap()
        } else {
            prepared
        };
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![declared.unwrap_or(DataType::Integer)]
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
    // PG 18.6 gives UPDATE 0: NOT NULL is a post-image check, so a non-matching UPDATE succeeds.
    assert_eq!(
        session
            .execute("UPDATE t SET n=NULL WHERE id=999", &[], None)
            .unwrap()
            .affected,
        0
    );
    assert_eq!(
        session.execute("SELECT * FROM t", &[], None).unwrap(),
        before
    );
}
