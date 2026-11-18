mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{DataType, SqlError, Value},
};

fn rows(s: &mut SqlSession, sql: &str, params: &[Value], types: &[Option<DataType>]) -> Vec<i64> {
    let p = s
        .prepare(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .with_parameter_types(types)
        .unwrap();
    let expected = s.execute_prepared(&p, params, None).unwrap().rows;
    let mut c = s.open_cursor(&p, params).unwrap();
    let mut actual = Vec::new();
    while !c.is_exhausted() {
        actual.extend(c.fetch(1, 1024).unwrap());
    }
    assert_eq!(actual, expected, "{sql}");
    actual
        .into_iter()
        .map(|r| match r.values[0] {
            Value::Int(n) => n,
            _ => panic!("integer"),
        })
        .collect()
}

fn exercise(engine: Engine) {
    let path = common::fresh("key-bounds-offset");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for key in ["smallint", "integer", "bigint"] {
        s.execute(
            &format!("CREATE TABLE t_{key}(id {key} PRIMARY KEY)"),
            &[],
            None,
        )
        .unwrap();
        s.execute(
            &format!("INSERT INTO t_{key} VALUES (-2),(1),(3)"),
            &[],
            None,
        )
        .unwrap();
        for (op, value, expected) in [
            ("=", 1, vec![1]),
            ("=", 2, vec![]),
            (">", 1, vec![3]),
            (">=", 1, vec![1, 3]),
            ("<", 1, vec![-2]),
            ("<=", 1, vec![-2, 1]),
            ("=", 100000, vec![]),
            ("<", 100000, vec![-2, 1, 3]),
            (">", -100000, vec![-2, 1, 3]),
            ("=", 3000000000, vec![]),
        ] {
            assert_eq!(
                rows(
                    &mut s,
                    &format!("SELECT id FROM t_{key} WHERE id {op} {value} ORDER BY id"),
                    &[],
                    &[]
                ),
                expected
            );
            for ty in [DataType::SmallInt, DataType::Integer, DataType::BigInt] {
                if (ty == DataType::SmallInt && i16::try_from(value).is_err())
                    || (ty == DataType::Integer && i32::try_from(value).is_err())
                {
                    continue;
                }
                assert_eq!(
                    rows(
                        &mut s,
                        &format!("SELECT id FROM t_{key} WHERE id {op} $1 ORDER BY id"),
                        &[Value::Int(value)],
                        &[Some(ty)]
                    ),
                    expected
                );
            }
        }
        for predicate in ["id BETWEEN -2 AND 1", "id <= 1.2"] {
            assert_eq!(
                rows(
                    &mut s,
                    &format!("SELECT id FROM t_{key} WHERE {predicate} ORDER BY id"),
                    &[],
                    &[]
                ),
                vec![-2, 1]
            );
        }
    }
    for (clause, expected) in [
        ("ORDER BY id OFFSET 0", vec![-2, 1, 3]),
        ("ORDER BY id LIMIT 1 OFFSET 1", vec![1]),
        ("ORDER BY id OFFSET 9", vec![]),
        ("ORDER BY id LIMIT 0 OFFSET 1", vec![]),
        ("ORDER BY id LIMIT NULL OFFSET 1", vec![1, 3]),
        ("ORDER BY id LIMIT ALL OFFSET 1", vec![1, 3]),
        ("OFFSET 1", vec![1, 3]),
        ("ORDER BY id DESC LIMIT 1 OFFSET 1", vec![1]),
        ("ORDER BY id + 1 LIMIT 1 OFFSET 1", vec![1]),
    ] {
        assert_eq!(
            rows(
                &mut s,
                &format!("SELECT id FROM t_bigint {clause}"),
                &[],
                &[]
            ),
            expected
        );
    }
    for (limit, offset, expected) in [
        (Value::Int(1), Value::Int(1), vec![1]),
        (Value::Null, Value::Int(1), vec![1, 3]),
        (Value::Int(1), Value::Null, vec![-2]),
    ] {
        assert_eq!(
            rows(
                &mut s,
                "SELECT id FROM t_bigint ORDER BY id LIMIT $1 OFFSET $2",
                &[limit, offset],
                &[Some(DataType::BigInt), Some(DataType::Integer)]
            ),
            expected
        );
    }
    for (clause, error) in [
        ("LIMIT -1", SqlError::InvalidRowCountInLimitClause),
        ("OFFSET -1", SqlError::InvalidRowCountInResultOffsetClause),
        (
            "LIMIT 0 OFFSET -1",
            SqlError::InvalidRowCountInResultOffsetClause,
        ),
    ] {
        let p = s
            .prepare(&format!("SELECT id FROM t_bigint {clause}"))
            .unwrap();
        assert!(matches!(s.execute_prepared(&p, &[], None), Err(CoreError::Sql(e)) if e == error));
        let mut c = s.open_cursor(&p, &[]).unwrap();
        assert!(matches!(c.fetch(1,1024), Err(CoreError::Sql(e)) if e == error));
    }
    drop(s);
    drop(admin);
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}
#[test]
fn legacy_integer_bounds_and_offset() {
    exercise(Engine::Legacy);
}
#[test]
#[cfg(feature = "engine-rocksdb")]
fn v2_integer_bounds_and_offset() {
    exercise(Engine::V2);
}

#[test]
fn row_count_error_protocol_identities() {
    use bincode::Options;
    for (tag, error) in [
        (54u32, SqlError::InvalidRowCountInLimitClause),
        (55, SqlError::InvalidRowCountInResultOffsetClause),
        (56, SqlError::CardinalityViolation),
        (57, SqlError::InvalidColumnReference),
        (58, SqlError::InvalidRecursion),
        (59, SqlError::DuplicateAlias),
        (60, SqlError::WindowingError),
        (61, SqlError::InvalidFrameOffset),
        (62, SqlError::InvalidNtileArgument),
        (63, SqlError::InvalidNthValueArgument),
        (64, SqlError::InvalidTablesampleArgument),
    ] {
        let le = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_little_endian();
        let be = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian();
        assert_eq!(
            le.deserialize::<SqlError>(&tag.to_le_bytes()).unwrap(),
            error
        );
        assert_eq!(
            be.deserialize::<SqlError>(&tag.to_be_bytes()).unwrap(),
            error
        );
        assert_eq!(le.serialize(&error).unwrap(), tag.to_le_bytes());
        assert_eq!(be.serialize(&error).unwrap(), tag.to_be_bytes());
        assert_eq!(
            giggles_core::local::failure(CoreError::Sql(error)).code,
            if tag == 64 { 266 } else { 200 + tag }
        );
        assert!(le.deserialize::<SqlError>(&65u32.to_le_bytes()).is_err());
        assert!(be.deserialize::<SqlError>(&65u32.to_be_bytes()).is_err());
    }
}
