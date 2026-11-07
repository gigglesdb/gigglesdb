use giggles_core::{Admin, CoreError, Limits, relational::*};
use std::time::{SystemTime, UNIX_EPOCH};

// Parallel tests can read the same nanosecond on Windows; a counter keeps paths unique.
static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct Db {
    admin: Option<Admin>,
    path: std::path::PathBuf,
}
impl Db {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "giggles-infix-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn numeric(s: &str) -> Value {
    Value::Numeric(Numeric::parse(s).unwrap())
}

#[test]
fn precedence_unary_comments_and_numeric_scale() {
    let db = Db::new();
    let mut s = db.admin.as_ref().unwrap().sql_session();
    for (expr, expected) in [
        ("2+3*4", Value::Int(14)),
        ("(2+3)*4", Value::Int(20)),
        ("-2*3", Value::Int(-6)),
        ("10-2-3", Value::Int(5)),
        ("2*3%4", Value::Int(2)),
        ("'a'||1+2", Value::Text("a3".into())),
        ("-1::int2", Value::Int(-1)),
        ("7/2", Value::Int(3)),
        ("7%3", Value::Int(1)),
        ("-7%3", Value::Int(-1)),
        ("1.10+2.2", numeric("3.30")),
        ("mod('NaN'::numeric,0)", numeric("NaN")),
        ("1.5*1.25", numeric("1.875")),
        ("7.0/2", numeric("3.5000000000000000")),
        ("1::numeric/3", numeric("0.33333333333333333333")),
        ("10::numeric/4", numeric("2.5000000000000000")),
        ("7.5%2", numeric("1.5")),
        ("-7.5%2", numeric("-1.5")),
        ("2/* outer /* nested */ still */+3", Value::Int(5)),
        ("2 -- comment\n +3", Value::Int(5)),
        ("1--c", Value::Int(1)),
        ("/* outer /* nested */ */2", Value::Int(2)),
        ("NULL::integer+1", Value::Null),
        ("NULL::integer/0", Value::Null),
        ("NULL::text||'a'", Value::Null),
        ("'a'||2", Value::Text("a2".into())),
        ("true||'x'", Value::Text("truex".into())),
        ("'\\x01'::bytea::text||'x'", Value::Text("\\x01x".into())),
        ("'\\x01'::bytea||'\\x02'::bytea", Value::Bytes(vec![1, 2])),
    ] {
        let p = s
            .prepare(&format!("SELECT {expr}"))
            .unwrap_or_else(|e| panic!("{expr}: {e:?}"));
        let got = s
            .execute_prepared(&p, &[], None)
            .unwrap_or_else(|e| panic!("{expr}: {e:?}"));
        assert_eq!(got.rows[0].values, vec![expected.clone()], "{expr}");
        if let Value::Numeric(expected) = expected {
            let Value::Numeric(actual) = &got.rows[0].values[0] else {
                panic!()
            };
            assert_eq!(actual.to_string(), expected.to_string(), "{expr}");
            assert_eq!(actual.scale(), expected.scale(), "{expr}");
        }
    }
}

#[test]
fn operator_descriptions_and_parameter_inference() {
    let db = Db::new();
    let mut s = db.admin.as_ref().unwrap().sql_session();
    for (expr, ty, params) in [
        ("1::int2+2::int2", DataType::SmallInt, vec![]),
        ("1::int2+2", DataType::Integer, vec![]),
        ("1+2::int8", DataType::BigInt, vec![]),
        ("1+2::numeric", DataType::Numeric, vec![]),
        ("1+2::float8", DataType::DoublePrecision, vec![]),
        ("1::real+2::real", DataType::Real, vec![]),
        ("1::real+2", DataType::DoublePrecision, vec![]),
        ("$1+1", DataType::Integer, vec![DataType::Integer]),
        ("$1||'a'", DataType::Text, vec![DataType::Text]),
        ("$1||1", DataType::Text, vec![DataType::Text]),
        ("-1::int2", DataType::SmallInt, vec![]),
    ] {
        let p = s.prepare(&format!("SELECT {expr}")).unwrap();
        let d = s.describe(&p).unwrap();
        assert_eq!(d.columns[0].data_type, ty, "{expr}");
        assert_eq!(d.parameters, params, "{expr}");
    }
}

#[test]
fn division_zero_and_width_overflow_are_runtime_errors() {
    let db = Db::new();
    let mut s = db.admin.as_ref().unwrap().sql_session();
    for expr in [
        "1/0",
        "1%0",
        "mod(5,0)",
        "mod(5.0,0)",
        "mod('Infinity'::numeric,0)",
        "1::int2/0::int2",
        "1::int2%0::int2",
        "1::int4/0",
        "1::int8/0::int8",
        "1::int8%0::int8",
        "1.0/0",
        "1::numeric/0::numeric",
        "1::numeric%0::numeric",
        "1.0::float8/0::float8",
        "1::float4/0::float4",
        "0.0::float8/0::float8",
    ] {
        let p = s.prepare(&format!("SELECT {expr}")).unwrap();
        s.describe(&p).unwrap();
        assert!(
            matches!(
                s.execute_prepared(&p, &[], None),
                Err(CoreError::Sql(SqlError::DivisionByZero))
            ),
            "{expr}"
        );
    }
    for expr in [
        "32767::int2+1::int2",
        "(-32768)::int2-1::int2",
        "200::int2*200::int2",
        "2147483647+1",
        "(-2147483648)-1",
        "2147483647*2",
        "9223372036854775807::int8+1",
        "(-9223372036854775808)::int8-1",
        "9223372036854775807::int8*2",
        "-(-32768::int2)",
        "(-2147483648)::int4/(-1)",
        "(-32768)::int2/(-1)::int2",
        "(-9223372036854775808)::int8/(-1)",
    ] {
        assert!(
            matches!(
                s.execute(&format!("SELECT {expr}"), &[], None),
                Err(CoreError::Sql(SqlError::Overflow))
            ),
            "{expr}"
        );
    }
    for expr in [
        "(-32768)::int2%(-1)::int2",
        "(-2147483648)::int4%(-1)",
        "(-9223372036854775808)::int8%(-1)",
    ] {
        assert_eq!(
            s.execute(&format!("SELECT {expr}"), &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Int(0)],
            "{expr}"
        );
    }
}

#[test]
fn typed_parameter_division_and_remainder_pg18_rows() {
    let db = Db::new();
    let mut s = db.admin.as_ref().unwrap().sql_session();
    // Core has no wire OID or format API: explicit casts are the typed equivalents
    // of the text/binary extended-protocol rows in the PG18 reproduction.
    for (ty, min) in [
        ("int2", "-32768"),
        ("integer", "-2147483648"),
        ("int8", "-9223372036854775808"),
        ("numeric", "-9223372036854775808"),
        ("float4", "-32768"),
        ("float8", "-32768"),
    ] {
        for op in ["/", "%"] {
            let sql = format!("SELECT $1::{ty}{op}$2::{ty}");
            if op == "%" && ty.starts_with("float") {
                assert!(
                    matches!(
                        s.prepare(&sql).and_then(|p| s.describe(&p)),
                        Err(CoreError::Sql(SqlError::UndefinedFunction))
                    ),
                    "{sql}"
                );
                continue;
            }
            let p = s.prepare(&sql).unwrap();
            let value = |n: &str| match ty {
                "numeric" => numeric(n),
                "float4" => Value::Float4(Float4::from_bits(n.parse::<f32>().unwrap().to_bits())),
                "float8" => Value::Float8(Float8::from_bits(n.parse::<f64>().unwrap().to_bits())),
                _ => Value::Int(n.parse().unwrap()),
            };
            assert!(
                matches!(
                    s.execute_prepared(&p, &[value("1"), value("0")], None),
                    Err(CoreError::Sql(SqlError::DivisionByZero))
                ),
                "{sql}"
            );
            if ty.starts_with("float") && op == "/" {
                assert!(matches!(
                    s.execute_prepared(&p, &[value("0"), value("0")], None),
                    Err(CoreError::Sql(SqlError::DivisionByZero))
                ));
            }
            let cases = if op == "/" {
                vec![("7", "2"), (min, "-1")]
            } else {
                vec![("7", "3"), ("-7", "3"), (min, "-1")]
            };
            for (left, right) in cases {
                let r = s.execute_prepared(&p, &[value(left), value(right)], None);
                if left == min && op == "/" && !ty.starts_with("float") && ty != "numeric" {
                    assert!(
                        matches!(r, Err(CoreError::Sql(SqlError::Overflow))),
                        "{sql}"
                    );
                } else {
                    let got = r.unwrap().rows[0].values[0].clone();
                    let expected = if op == "%" {
                        if left == min {
                            "0"
                        } else if left == "-7" {
                            "-1"
                        } else {
                            "1"
                        }
                    } else if left == "7" {
                        if ty == "numeric" {
                            "3.5000000000000000"
                        } else if ty.starts_with("float") {
                            "3.5"
                        } else {
                            "3"
                        }
                    } else if ty == "numeric" {
                        "9223372036854775808"
                    } else {
                        "32768"
                    };
                    assert_eq!(got, value(expected), "{sql}: {left},{right}");
                }
            }
        }
    }
    for expr in ["+$1", "-$1", "$1+$2+1"] {
        assert!(
            matches!(
                s.prepare(&format!("SELECT {expr}"))
                    .and_then(|p| s.describe(&p)),
                Err(CoreError::Sql(SqlError::AmbiguousFunction))
            ),
            "{expr}"
        );
    }
    for op in ["/", "%"] {
        assert!(matches!(
            s.prepare(&format!("SELECT $1{op}$2"))
                .and_then(|p| s.describe(&p)),
            Err(CoreError::Sql(SqlError::AmbiguousFunction))
        ));
    }
}

#[test]
fn arithmetic_columns_filter_before_projection_and_returning() {
    let db = Db::new();
    let mut s = db.admin.as_ref().unwrap().sql_session();
    s.execute(
        "CREATE TABLE t (id integer primary key, a integer, b integer)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO t VALUES (1,7,2),(2,1,0)", &[], None)
        .unwrap();
    for (sql, expected) in [
        ("SELECT a/b FROM t WHERE id=1", 3),
        ("SELECT a%b FROM t WHERE id=1", 1),
        ("SELECT a+1 FROM t WHERE a+1>5", 8),
        ("SELECT id FROM t WHERE a+1>5", 1),
    ] {
        assert_eq!(
            s.execute(sql, &[], None).unwrap().rows[0].values,
            vec![Value::Int(expected)],
            "{sql}"
        );
    }
    for sql in [
        "SELECT a/b FROM t",
        "SELECT a%b FROM t",
        "SELECT * FROM t WHERE a/b=1",
        "SELECT a/b FROM t WHERE id=2",
    ] {
        assert!(
            matches!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(SqlError::DivisionByZero))
            ),
            "{sql}"
        );
    }
    let p = s.prepare("SELECT a+$1 FROM t WHERE id=1").unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![DataType::Integer]);
    assert_eq!(
        s.execute_prepared(&p, &[Value::Int(1)], None).unwrap().rows[0].values,
        vec![Value::Int(8)]
    );
    assert_eq!(
        s.execute("INSERT INTO t VALUES (3,1+1,2*3) RETURNING a+b", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(8)]
    );
    let p = s
        .prepare("INSERT INTO t VALUES ($1,$2+1,$3*2) RETURNING a+b")
        .unwrap();
    let description = s.describe(&p).unwrap();
    assert_eq!(description.parameters, vec![DataType::Integer; 3]);
    assert_eq!(description.columns[0].data_type, DataType::Integer);
    assert_eq!(
        s.execute_prepared(&p, &[Value::Int(4), Value::Int(1), Value::Int(3)], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(8)]
    );
    assert_eq!(
        s.execute("SELECT * FROM t WHERE id=4", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(4), Value::Int(2), Value::Int(6)]
    );
    assert_eq!(
        s.execute("SELECT * FROM t WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1), Value::Int(7), Value::Int(2)]
    );
    assert_eq!(
        s.execute("SELECT count(*) FROM t", &[], None).unwrap().rows[0].values,
        vec![Value::Int(4)]
    );
}

#[test]
fn float_results_underflow_and_invalid_operators() {
    let db = Db::new();
    let mut s = db.admin.as_ref().unwrap().sql_session();
    for (expr, expected) in [
        ("7::float8/2", 3.5),
        ("1.5::float8*2", 3.0),
        ("'Infinity'::float8+1", f64::INFINITY),
        ("1::float8/ 'Infinity'::float8", 0.0),
    ] {
        assert_eq!(
            s.execute(&format!("SELECT {expr}"), &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Float8(Float8::from_bits(expected.to_bits()))]
        );
    }
    let nan = s.execute("SELECT 'NaN'::float8/0", &[], None).unwrap();
    let Value::Float8(n) = &nan.rows[0].values[0] else {
        panic!()
    };
    assert!(f64::from_bits(n.to_bits()).is_nan());
    for expr in [
        "1e-300::float8*1e-300::float8",
        "1e300::float8*1e300::float8",
        "1e308::float8*10",
        "1e-30::float4*1e-30::float4",
        "1e30::float4*1e30::float4",
    ] {
        assert!(
            matches!(
                s.execute(&format!("SELECT {expr}"), &[], None),
                Err(CoreError::Sql(SqlError::Overflow))
            ),
            "{expr}"
        );
    }
    for expr in [
        "1||2",
        "1::float4%2::float4",
        "1::float8%2::float8",
        "true+1",
    ] {
        assert!(
            matches!(
                s.prepare(&format!("SELECT {expr}"))
                    .and_then(|p| s.describe(&p)),
                Err(CoreError::Sql(SqlError::UndefinedFunction))
            ),
            "{expr}"
        );
    }
}

#[test]
fn signed_legacy_statements_and_limit_rejection() {
    let db = Db::new();
    let mut s = db.admin.as_ref().unwrap().sql_session();
    s.execute(
        "CREATE TABLE signed_rows (id integer primary key, n integer)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO signed_rows VALUES (-1,-2)", &[], None)
        .unwrap();
    assert_eq!(
        s.execute("SELECT n FROM signed_rows WHERE id=-1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(-2)]
    );
    s.execute("UPDATE signed_rows SET n=-3 WHERE id=-1", &[], None)
        .unwrap();
    assert_eq!(
        s.execute("SELECT n FROM signed_rows WHERE id=-1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(-3)]
    );
    assert!(matches!(
        s.execute("SELECT * FROM signed_rows LIMIT -1", &[], None),
        Err(CoreError::Sql(SqlError::Limit))
    ));
    s.execute("DELETE FROM signed_rows WHERE id=-1", &[], None)
        .unwrap();
    assert!(
        s.execute("SELECT * FROM signed_rows", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn expression_admission_and_concat_byte_budgets() {
    let db = Db::new();
    let mut s = db.admin.as_ref().unwrap().sql_session();
    let too_deep = format!("SELECT {}1", "- ".repeat(40));
    assert!(matches!(
        s.prepare(&too_deep),
        Err(CoreError::Sql(SqlError::Limit))
    ));
    let mut wide = "1".to_owned();
    for _ in 0..12 {
        wide = format!("({wide}+{wide})");
    }
    assert!(matches!(
        s.prepare(&format!("SELECT {wide}")),
        Err(CoreError::Sql(SqlError::Limit))
    ));
    let p = s.prepare("SELECT $1::bytea||$2::bytea").unwrap();
    assert_eq!(
        s.execute_prepared(
            &p,
            &[Value::Bytes(vec![1; 32768]), Value::Bytes(vec![2; 32768])],
            None
        )
        .unwrap()
        .rows[0]
            .values[0],
        Value::Bytes([vec![1; 32768], vec![2; 32768]].concat())
    );
    assert!(matches!(
        s.execute_prepared(
            &p,
            &[Value::Bytes(vec![1; 32768]), Value::Bytes(vec![2; 32769])],
            None
        ),
        Err(CoreError::Sql(SqlError::Limit))
    ));
    assert_eq!(
        s.execute("SELECT 1+2", &[], None).unwrap().rows[0].values,
        vec![Value::Int(3)]
    );
}

#[test]
fn unary_minus_cast_precedence_pg18_regressions() {
    let db = Db::new();
    let mut s = db.admin.as_ref().unwrap().sql_session();
    for sql in ["SELECT -2147483648::integer", "SELECT -32768::smallint"] {
        assert!(
            matches!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(SqlError::Overflow))
            ),
            "{sql}"
        );
    }
    for (sql, expected, ty) in [
        (
            "SELECT (-2147483648)::integer",
            Value::Int(-2147483648),
            DataType::Integer,
        ),
        (
            "SELECT -9223372036854775808",
            Value::Int(i64::MIN),
            DataType::BigInt,
        ),
        ("SELECT 2-1", Value::Int(1), DataType::Integer),
        ("SELECT 1 - -1", Value::Int(2), DataType::Integer),
        ("SELECT 1--1", Value::Int(1), DataType::Integer),
        ("SELECT 'a'||1||NULL", Value::Null, DataType::Text),
    ] {
        let p = s.prepare(sql).unwrap();
        assert_eq!(s.describe(&p).unwrap().columns[0].data_type, ty, "{sql}");
        assert_eq!(
            s.execute_prepared(&p, &[], None).unwrap().rows[0].values,
            vec![expected],
            "{sql}"
        );
    }
}

#[test]
fn public_qualified_routines_inside_operators_match_plain_calls() {
    let db = Db::new();
    let mut s = db.admin.as_ref().unwrap().sql_session();
    let plain = s.execute("SELECT public.abs(1)", &[], None).unwrap_err();
    for sql in [
        "SELECT 1 + public.abs(1)",
        "SELECT -public.abs(1)",
        "SELECT public.abs(1) || 'x'",
    ] {
        assert_eq!(s.execute(sql, &[], None).unwrap_err(), plain, "{sql}");
    }
    assert_eq!(plain, CoreError::Sql(SqlError::UndefinedFunction));
    // The server's routing hint must classify nested public calls like the plain call.
    let plain_hint = Prepared::rejects_unsupported_table_free_scalar("SELECT public.abs(1)");
    for sql in [
        "SELECT 1 + public.abs(1)",
        "SELECT -public.abs(1)",
        "SELECT public.abs(1) || 'x'",
    ] {
        assert_eq!(Prepared::rejects_unsupported_table_free_scalar(sql), plain_hint, "{sql}");
    }
}
