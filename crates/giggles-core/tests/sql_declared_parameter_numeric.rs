use giggles_core::{Admin, CoreError, Limits, relational::*};

fn fresh() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join("sql-declared-parameter-numeric");
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
fn declared_integer_numeric_assignments_preserve_exact_values() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t(id INT PRIMARY KEY,n NUMERIC)", &[], None)
        .unwrap();
    for (declared, number) in [
        (DataType::BigInt, i64::MIN),
        (DataType::Integer, i64::from(i32::MAX)),
        (DataType::SmallInt, i64::from(i16::MIN)),
    ] {
        let expected = Value::Numeric(Numeric::parse(&number.to_string()).unwrap());
        for sql in [
            "INSERT INTO t VALUES(1,$1) RETURNING n",
            "INSERT INTO t VALUES(2,$1),(3,$1) RETURNING n",
            "UPDATE t SET n=$1 WHERE id=1 RETURNING n",
            "INSERT INTO t VALUES(1,0) ON CONFLICT(id) DO UPDATE SET n=$1 RETURNING n",
        ] {
            let prepared = session
                .prepare(sql)
                .unwrap()
                .with_parameter_types(&[Some(declared.clone())])
                .unwrap();
            assert_eq!(
                session.describe(&prepared).unwrap().parameters,
                vec![declared.clone()]
            );
            let result = session
                .execute_prepared(&prepared, &[Value::Int(number)], None)
                .unwrap();
            assert_eq!(
                result.rows.len(),
                if sql.contains("(3,$1)") { 2 } else { 1 }
            );
            for row in result.rows {
                assert_eq!(row.values, vec![expected.clone()], "{sql}");
            }
        }
        let rows = session
            .execute("SELECT n FROM t ORDER BY id", &[], None)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 3);
        for row in rows {
            assert_eq!(row.values, vec![expected.clone()]);
        }
        session.execute("DELETE FROM t", &[], None).unwrap();
    }
    session
        .execute("INSERT INTO t VALUES(1,0)", &[], None)
        .unwrap();
    for sql in [
        "INSERT INTO t VALUES(4,$1)",
        "INSERT INTO t VALUES(4,$1),(5,$1)",
        "UPDATE t SET n=$1 WHERE id=1",
        "INSERT INTO t VALUES(1,0) ON CONFLICT(id) DO UPDATE SET n=$1",
    ] {
        let prepared = session.prepare(sql).unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Numeric]
        );
        assert!(
            matches!(
                session.execute_prepared(&prepared, &[Value::Int(7)], None),
                Err(CoreError::Sql(SqlError::Type))
            ),
            "{sql}"
        );
    }
    assert_eq!(
        session.execute("SELECT n FROM t", &[], None).unwrap().rows[0].values,
        vec![Value::Numeric(Numeric::parse("0").unwrap())]
    );
}
