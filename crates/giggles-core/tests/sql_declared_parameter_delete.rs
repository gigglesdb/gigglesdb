use giggles_core::{Admin, CoreError, Limits, SqlSession, relational::*};

fn fresh() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join("sql-declared-parameter-delete");
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

fn fixture() -> SqlSession {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,sm SMALLINT,big BIGINT,b BYTEA,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session.execute("INSERT INTO t(id,n,sm,big,s) VALUES(1,10,1,10,'one'),(2,20,2,20,'two'),(3,NULL,3,30,'three')", &[], None).unwrap();
    session
        .execute(
            "UPDATE t SET b=$1 WHERE id=2",
            &[Value::Bytes(vec![0, 255])],
            None,
        )
        .unwrap();
    session
}

#[test]
fn declared_delete_point_integer_width_and_bytea_returning() {
    let mut session = fixture();
    let point = session
        .prepare("DELETE FROM t WHERE id=$1")
        .unwrap()
        .with_parameter_types(&[Some(DataType::BigInt)])
        .unwrap();
    assert_eq!(
        session.describe(&point).unwrap().parameters,
        vec![DataType::BigInt]
    );
    let before = session
        .execute("SELECT * FROM t ORDER BY id", &[], None)
        .unwrap();
    assert_eq!(
        session
            .execute_prepared(&point, &[Value::Int(3_000_000_000)], None)
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
    assert_eq!(
        session
            .execute_prepared(&point, &[Value::Int(1)], None)
            .unwrap()
            .affected,
        1
    );
    let bytes = session
        .prepare("DELETE FROM t WHERE b=$1 RETURNING id")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes)])
        .unwrap();
    let description = session.describe(&bytes).unwrap();
    assert_eq!(description.parameters, vec![DataType::Bytes]);
    assert_eq!(description.columns[0].name, "id");
    assert_eq!(description.columns[0].data_type, DataType::Integer);
    let deleted = session
        .execute_prepared(&bytes, &[Value::Bytes(vec![0, 255])], None)
        .unwrap();
    assert_eq!(deleted.affected, 1);
    assert_eq!(deleted.rows[0].values, vec![Value::Int(2)]);
    let survivors = session
        .execute("SELECT id FROM t ORDER BY id", &[], None)
        .unwrap();
    assert_eq!(survivors.rows.len(), 1);
    assert_eq!(survivors.rows[0].values, vec![Value::Int(3)]);
}

#[test]
fn declared_delete_prefix_and_none_holes_keep_other_inference() {
    for (declarations, expected) in [
        (
            vec![Some(DataType::BigInt)],
            vec![DataType::BigInt, DataType::Integer],
        ),
        (
            vec![None, Some(DataType::BigInt)],
            vec![DataType::Integer, DataType::BigInt],
        ),
    ] {
        let mut session = fixture();
        let prepared = session
            .prepare("DELETE FROM t WHERE n=$1 OR id>$2 RETURNING id")
            .unwrap()
            .with_parameter_types(&declarations)
            .unwrap();
        assert_eq!(session.describe(&prepared).unwrap().parameters, expected);
        let result = session
            .execute_prepared(&prepared, &[Value::Int(10), Value::Int(2)], None)
            .unwrap();
        assert_eq!(result.affected, 2);
        assert_eq!(
            result
                .rows
                .iter()
                .map(|row| row.values.clone())
                .collect::<Vec<_>>(),
            vec![vec![Value::Int(1)], vec![Value::Int(3)]]
        );
    }
}

#[test]
fn searched_delete_declared_bigint_comparisons_do_not_narrow_to_columns() {
    let mut session = fixture();
    let prepared = session
        .prepare("DELETE FROM t WHERE n=$1 OR id>$2 RETURNING id")
        .unwrap()
        .with_parameter_types(&[Some(DataType::BigInt), Some(DataType::BigInt)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::BigInt; 2]
    );
    let result = session
        .execute_prepared(&prepared, &[Value::Int(3_000_000_000), Value::Int(1)], None)
        .unwrap();
    assert_eq!(result.affected, 2);
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
    assert_eq!(
        session
            .execute("SELECT id FROM t ORDER BY id", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1)]
    );
}

#[test]
fn declared_delete_comparison_mismatches_fail_before_deletion() {
    let mut session = fixture();
    for (sql, declared, input) in [
        (
            "DELETE FROM t WHERE n=$1",
            DataType::Bytes,
            Value::Bytes(vec![0]),
        ),
        ("DELETE FROM t WHERE s=$1", DataType::Integer, Value::Int(1)),
        (
            "DELETE FROM t WHERE n=$1 OR id>0",
            DataType::Bytes,
            Value::Bytes(vec![0]),
        ),
        (
            "DELETE FROM t WHERE id=$1",
            DataType::Text,
            Value::Text("1".into()),
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
            matches!(
                session.describe(&prepared),
                Err(CoreError::Sql(SqlError::UndefinedFunction))
            ),
            "describe {sql}"
        );
        assert!(
            matches!(
                session.execute_prepared(&prepared, &[input], None),
                Err(CoreError::Sql(SqlError::UndefinedFunction))
            ),
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
}

#[test]
fn delete_excess_declarations_and_undeclared_inference_are_unchanged() {
    let mut session = fixture();
    assert!(matches!(
        session
            .prepare("DELETE FROM t WHERE id=$1")
            .unwrap()
            .with_parameter_types(&[Some(DataType::Integer), Some(DataType::Integer)]),
        Err(SqlError::Parameters)
    ));
    for declarations in [vec![], vec![None, None]] {
        let prepared = session
            .prepare("DELETE FROM t WHERE n=$1 OR id>$2")
            .unwrap()
            .with_parameter_types(&declarations)
            .unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Integer; 2]
        );
        assert_eq!(
            session
                .execute_prepared(&prepared, &[Value::Int(99), Value::Int(99)], None)
                .unwrap()
                .affected,
            0
        );
    }
    let point = session.prepare("DELETE FROM t WHERE id=$1").unwrap();
    assert_eq!(
        session.describe(&point).unwrap().parameters,
        vec![DataType::Integer]
    );
    assert!(matches!(
        session.execute_prepared(&point, &[Value::Int(3_000_000_000)], None),
        Err(CoreError::Sql(SqlError::Type))
    ));
    assert_eq!(
        session
            .execute("SELECT id FROM t ORDER BY id", &[], None)
            .unwrap()
            .rows
            .len(),
        3
    );
}

#[test]
fn reversed_delete_comparisons_honor_declared_types() {
    let mut session = fixture();
    let point = session
        .prepare("DELETE FROM t WHERE $1=id")
        .unwrap()
        .with_parameter_types(&[Some(DataType::BigInt)])
        .unwrap();
    assert_eq!(
        session.describe(&point).unwrap().parameters,
        vec![DataType::BigInt]
    );
    assert_eq!(
        session
            .execute_prepared(&point, &[Value::Int(3_000_000_000)], None)
            .unwrap()
            .affected,
        0
    );
    let searched = session
        .prepare("DELETE FROM t WHERE $1<id AND id<3 RETURNING id")
        .unwrap()
        .with_parameter_types(&[Some(DataType::BigInt)])
        .unwrap();
    let description = session.describe(&searched).unwrap();
    assert_eq!(description.parameters, vec![DataType::BigInt]);
    assert_eq!(description.columns[0].data_type, DataType::Integer);
    let result = session
        .execute_prepared(&searched, &[Value::Int(1)], None)
        .unwrap();
    assert_eq!(result.affected, 1);
    assert_eq!(result.rows[0].values, vec![Value::Int(2)]);
    let before = session
        .execute("SELECT * FROM t ORDER BY id", &[], None)
        .unwrap();
    for sql in ["DELETE FROM t WHERE $1=id", "DELETE FROM t WHERE id=$1"] {
        let prepared = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&[Some(DataType::Text)])
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
                session.execute_prepared(&prepared, &[Value::Text("1".into())], None),
                Err(CoreError::Sql(SqlError::UndefinedFunction))
            ),
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
}

#[test]
fn delete_out_of_range_integer_literals_match_no_rows() {
    let mut session = fixture();
    let before = session
        .execute("SELECT * FROM t ORDER BY id", &[], None)
        .unwrap();
    for sql in [
        "DELETE FROM t WHERE id=3000000000",
        "DELETE FROM t WHERE id=3000000000 RETURNING id",
        "DELETE FROM t WHERE id > -3000000000 AND id<0",
    ] {
        let result = session.execute(sql, &[], None).unwrap();
        assert_eq!(result.affected, 0, "{sql}");
        assert!(result.rows.is_empty(), "{sql}");
        assert_eq!(
            session
                .execute("SELECT * FROM t ORDER BY id", &[], None)
                .unwrap(),
            before,
            "{sql}"
        );
    }
}
