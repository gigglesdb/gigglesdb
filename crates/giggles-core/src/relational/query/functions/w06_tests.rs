//! PostgreSQL 18.6 C.UTF-8 differential checks for GD-0092 W0-6.
use super::*;

fn expression(sql: &str) -> R<(DataType, Value)> {
    let mut parser = Parser {
        tokens: lex(sql)?,
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
        privilege_seen: false,
        privilege_casts: false,
    };
    let scalar = Scalar::parse_expr(&mut parser, 0, false)?;
    if parser.at != parser.tokens.len() {
        return Err(SqlError::Unsupported);
    }
    let bound = scalar.bind(&|_, _| Err(SqlError::UnknownColumn), &[], &[])?;
    let row = Tuple {
        source_versions: vec![],
        values: vec![],
        version: 0,
    };
    let value = bound.eval(&row, &[], &mut 0)?;
    Ok((bound.ty, value))
}

fn expect(sql: &str, ty: DataType, value: Value) {
    assert_eq!(expression(sql), Ok((ty, value)), "{sql}");
}

fn error(sql: &str, expected: SqlError) {
    user_types::clear_sql_error_message();
    assert_eq!(expression(sql), Err(expected), "{sql}");
    user_types::clear_sql_error_message();
}

fn float(n: f64) -> Value {
    Value::Float8(Float8::from_bits(n.to_bits()))
}

#[test]
fn math_operators_and_adjacency() {
    use DataType::*;
    for (sql, value) in [
        ("2 ^ 3 ^ 2", 64.0),
        ("-2 ^ 2", 4.0),
        ("2 + 3 ^ 2", 11.0),
        ("2 * 3 ^ 2", 18.0),
        ("2.5::float4 ^ 2", 6.25),
        ("|/ 2.0", std::f64::consts::SQRT_2),
        ("|/ 0", 0.0),
        ("||/ 27.0", 3.0000000000000004),
        ("||/ -27", -3.0000000000000004),
        ("||/ 2", 1.2599210498948734),
        ("||/ 'Infinity'::float8", f64::INFINITY),
        ("@ '-Infinity'::float8", f64::INFINITY),
        // PG 18.6 oracle (lane-f/w1-unicode/w1-edge.out): a float operand wins over numeric.
        ("1::numeric ^ 2::float8", 1.0),
        ("2::numeric ^ 2::real", 4.0),
        ("2::real ^ 2::numeric", 4.0),
    ] {
        expect(sql, DoublePrecision, float(value));
    }
    for (sql, value) in [
        ("2 ^ 0.5", "1.4142135623730950"),
        ("2.0 ^ 0.5", "1.4142135623730950"),
        ("1.5 ^ 2", "2.2500000000000000"),
        ("10::numeric ^ -2", "0.010000000000000000"),
        ("0.0 ^ 0.0", "1.0000000000000000"),
        ("2.0 ^ 3.0", "8.0000000000000000"),
        ("@ (-4.5)", "4.5"),
    ] {
        let (ty, actual) = expression(sql).unwrap();
        assert_eq!(ty, Numeric, "{sql}");
        let Value::Numeric(actual) = actual else {
            panic!("{sql}: {actual:?}");
        };
        assert_eq!(actual.to_string(), value, "{sql}");
    }
    expect("@ -5", Integer, Value::Int(5));
    expect("@ -5::smallint", SmallInt, Value::Int(5));
    expect("@ -5::bigint", BigInt, Value::Int(5));
    expect(
        "@ -4.5::float4",
        Real,
        Value::Float4(Float4::from_bits(4.5f32.to_bits())),
    );
    expect("'a'||'b'", Text, Value::Text("ab".into()));
    error("1||/2", SqlError::UndefinedFunction);
    for sql in [
        "0 ^ -1",
        "0::numeric ^ -1::numeric",
        "(-8) ^ (1.0/3)",
        "(-8)::float8 ^ 0.5",
        "|/ -1",
        "|/ -1.0::numeric",
    ] {
        error(sql, SqlError::InvalidArgumentForPowerFunction);
    }
    for sql in [
        "1e300::float8 ^ 2",
        "10::numeric ^ 1000000",
        "@ (-2147483648)::int",
        "@ (-32768)::smallint",
    ] {
        error(sql, SqlError::Overflow);
    }
}

#[test]
fn bitwise_types_precedence_and_shift_width() {
    use DataType::*;
    for (sql, ty, value) in [
        ("5 & 3", Integer, 1),
        ("5 | 8", Integer, 13),
        ("5 # 2", Integer, 7),
        ("~5", Integer, -6),
        ("5 << 2", Integer, 20),
        ("5 >> 1", Integer, 2),
        ("-3 >> 1", Integer, -2),
        ("1 << 31", Integer, i64::from(i32::MIN)),
        ("1 << 32", Integer, 1),
        ("1 << 33", Integer, 2),
        ("1 << -1", Integer, i64::from(i32::MIN)),
        ("1::bigint << 63", BigInt, i64::MIN),
        ("1::bigint << 64", BigInt, 1),
        ("1::smallint << 15", SmallInt, -32768),
        ("1::smallint << 16", SmallInt, 0),
        ("-1 >> 40", Integer, -1),
        ("5::smallint & 3::smallint", SmallInt, 1),
        ("5::smallint & 3", Integer, 1),
        ("5::smallint & 1::bigint", BigInt, 1),
        ("5::bigint << 2", BigInt, 20),
        ("1 & 2 | 4", Integer, 4),
        ("1 | 2 & 4", Integer, 0),
        ("1 # 3 & 1", Integer, 0),
        ("1 + 1 << 2", Integer, 8),
        ("8 >> 1 + 1", Integer, 2),
        ("~ 1 + 1", Integer, -3),
        ("2 * 3 & 5", Integer, 4),
    ] {
        expect(sql, ty, Value::Int(value));
    }
    expect("1 & 3 = 1", Boolean, Value::Bool(true));
    error("5.0 & 3", SqlError::UndefinedFunction);
    error("'a' & 'b'", SqlError::AmbiguousFunction);
}

#[test]
fn predicates_and_conditional_functions_have_sql_null_semantics() {
    use DataType::*;
    for (sql, value) in [
        ("3 BETWEEN SYMMETRIC 5 AND 1", Some(true)),
        ("3 NOT BETWEEN SYMMETRIC 5 AND 1", Some(false)),
        ("NULL BETWEEN SYMMETRIC 1 AND 5", None),
        ("3 BETWEEN SYMMETRIC NULL AND 5", None),
        ("7 BETWEEN SYMMETRIC NULL AND 5", None),
        ("3 BETWEEN ASYMMETRIC 5 AND 1", Some(false)),
        ("'b' BETWEEN SYMMETRIC 'c' AND 'a'", Some(true)),
        ("ROW(1,2) = ROW(1,2)", Some(true)),
        ("ROW(1,2) <> ROW(1,3)", Some(true)),
        ("ROW(1,2) < ROW(1,3)", Some(true)),
        ("ROW(1,NULL) = ROW(1,2)", None),
        ("ROW(1,NULL) = ROW(2,2)", Some(false)),
        ("ROW(1,2) <= ROW(1,2)", Some(true)),
        ("(1,2) > (0,9)", Some(true)),
        ("(1,2) >= (1,2)", Some(true)),
        ("(NULL,0) < (1,1)", None),
        ("ROW(1,NULL) IS NULL", Some(false)),
        ("ROW(NULL,NULL) IS NULL", Some(true)),
        ("ROW(1,2) IS DISTINCT FROM ROW(1,NULL)", Some(true)),
        ("'abc' ~~ 'a%'", Some(true)),
        ("'abc' !~~ 'a%'", Some(false)),
        ("'ABC' ~~* 'a%'", Some(true)),
        ("'ABC' !~~* 'a%'", Some(false)),
        ("NULL ~~ 'a'", None),
        (r"'a_c' ~~ 'a\_c'", Some(true)),
        (r"'abc' ~~ 'a\_c'", Some(false)),
        ("'abc' ^@ 'a'", Some(true)),
        ("'abc' ^@ 'b'", Some(false)),
        ("'abc' ^@ ''", Some(true)),
        ("NULL ^@ 'a'", None),
    ] {
        expect(sql, Boolean, value.map_or(Value::Null, Value::Bool));
    }
    for (sql, ty, value) in [
        ("nullif(1,1)", Integer, Value::Null),
        ("nullif(1,2)", Integer, Value::Int(1)),
        ("nullif(NULL,1)", Integer, Value::Null),
        ("nullif(1,NULL)", Integer, Value::Int(1)),
        ("nullif('a','a')", Text, Value::Null),
        ("nullif('a'::text,'b')", Text, Value::Text("a".into())),
        ("nullif(1::smallint,2)", SmallInt, Value::Int(1)),
        ("nullif(1,1.0)", Numeric, Value::Null),
        ("num_nulls(1,NULL,'a',NULL::text)", Integer, Value::Int(2)),
        (
            "num_nonnulls(1,NULL,'a',NULL::text)",
            Integer,
            Value::Int(2),
        ),
        ("num_nulls(NULL)", Integer, Value::Int(1)),
        ("num_nonnulls(NULL)", Integer, Value::Int(0)),
        (
            "num_nulls(VARIADIC ARRAY[0,NULL,3])",
            Integer,
            Value::Int(1),
        ),
        (
            "num_nonnulls(VARIADIC ARRAY[0,NULL,3])",
            Integer,
            Value::Int(2),
        ),
    ] {
        expect(sql, ty, value);
    }
    error("nullif(1,'x')", SqlError::InvalidTextRepresentation);
    error("nullif(1)", SqlError::Syntax);
    error("num_nulls()", SqlError::UndefinedFunction);
    error("num_nonnulls()", SqlError::UndefinedFunction);
    error("ROW(1,2) = ROW(1,2,3)", SqlError::Syntax);
    error("ROW(1,'a') = ROW(1,2)", SqlError::InvalidTextRepresentation);
}

#[test]
fn width_bucket_overloads_and_errors() {
    for (arguments, value) in [
        ("5,0,10,5", Some(3)),
        ("5.0,0,10,5", Some(3)),
        ("2::numeric,0,10,5", Some(2)),
        ("10::numeric,10,0,5", Some(1)),
        ("0,10,0,5", Some(6)),
        ("-1,0,10,5", Some(0)),
        ("10,0,10,5", Some(6)),
        ("11,10,0,5", Some(0)),
        ("NULL,0,10,5", None),
        ("5,NULL,10,5", None),
        ("5.0::float8,0,10,5", Some(3)),
        ("3.3::float8,0.0::float8,10.0::float8,3", Some(1)),
        ("1e308::float8,-1e308::float8,1e308::float8,10", Some(11)),
        ("0::float8,-1e308,1e308,10", Some(6)),
        ("0.333333::numeric,0,1,3", Some(1)),
        ("2.5::float4,0,10,4", Some(2)),
        ("'Infinity'::numeric,0,10,3", Some(4)),
        ("'-Infinity'::float8,0,10,3", Some(0)),
        ("5,ARRAY[0,3,6,9]", Some(2)),
        ("5::bigint,ARRAY[0,3,6,9]::bigint[]", Some(2)),
        ("'c',ARRAY['a','b','d']", Some(2)),
        ("-1,ARRAY[0,3]", Some(0)),
        ("3,ARRAY[0,3]", Some(2)),
        ("NULL,ARRAY[0,3]", None),
        ("1,ARRAY[]::int[]", Some(0)),
        ("1,ARRAY[3,0]", Some(2)),
    ] {
        expect(
            &format!("width_bucket({arguments})"),
            DataType::Integer,
            value.map_or(Value::Null, Value::Int),
        );
    }
    for (arguments, expected) in [
        ("1,0,10,0", SqlError::InvalidArgumentForWidthBucket),
        ("1,0,10,-1", SqlError::InvalidArgumentForWidthBucket),
        ("1,5,5,3", SqlError::InvalidArgumentForWidthBucket),
        ("1::float8,5,5,3", SqlError::InvalidArgumentForWidthBucket),
        (
            "'NaN'::float8,0,10,3",
            SqlError::InvalidArgumentForWidthBucket,
        ),
        (
            "1::float8,'NaN',10,3",
            SqlError::InvalidArgumentForWidthBucket,
        ),
        (
            "1::float8,0,'Infinity',3",
            SqlError::InvalidArgumentForWidthBucket,
        ),
        (
            "'NaN'::numeric,0,10,3",
            SqlError::InvalidArgumentForWidthBucket,
        ),
        ("10::float8,0,10,2147483647", SqlError::Overflow),
        ("10::numeric,0,10,2147483647", SqlError::Overflow),
        ("1,ARRAY[0,NULL,3]", SqlError::NullValueNotAllowed),
    ] {
        error(&format!("width_bucket({arguments})"), expected);
    }
}

#[test]
fn unicode_normalization_escapes_and_case() {
    use DataType::*;
    for (sql, value) in [
        (r"normalize(U&'e\0301')", "é"),
        (r"normalize(U&'\00E9',NFD)", "e\u{301}"),
        (r"normalize(U&'\FB01',NFKC)", "fi"),
        (r"normalize(U&'\FB01',NFKD)", "fi"),
        (r"normalize(U&'\00E9',nfc)", "é"),
        (r"unistr('\0061\+01F600')", "a😀"),
        (r"unistr('a\\b')", "a\\b"),
        (r"unistr('\u0041\U0001F600x')", "A😀x"),
        (r"unistr('d\0061t\+000061')", "data"),
        (r"unistr('\D83D\DE00')", "😀"),
        ("upper('éclair')", "ÉCLAIR"),
        ("lower('ÉCLAIR')", "éclair"),
        ("upper('straße')", "STRAßE"),
        (r"lower(U&'\0130')", "i"),
        (r"upper(U&'\1FB3')", "\u{1fbc}"),
        (r"initcap(U&'\1FB3x')", "\u{1fbc}x"),
        (r"upper(U&'\0149')", "\u{149}"),
        (r"upper(U&'\01F0')", "\u{1f0}"),
        (r"upper(U&'\FB01')", "\u{fb01}"),
        (r"upper(U&'\0587')", "\u{587}"),
        (r"upper(U&'\00B5')", "Μ"),
        (r"lower(U&'\2126')", "ω"),
        (r"lower(U&'\1E9E')", "ß"),
        (r"upper(U&'\0131')", "I"),
        (r"upper(U&'\017F')", "S"),
        (r"initcap(U&'\01C4A')", "\u{1c4}a"),
        (r"initcap(U&'x\0660y \00B2a')", "X٠y ²A"),
        (r"initcap(U&'\00E9\0301a b')", "É\u{301}A B"),
        (r"initcap(U&'\0F40\0F71y')", "ཀཱy"),
        (r"initcap(U&'a\00B7b')", "A·B"),
        ("initcap('o''neil x-ray 3d ab_cd')", "O'Neil X-Ray 3d Ab_Cd"),
        (r#"upper('é' COLLATE "C")"#, "é"),
        (r#"initcap('é' COLLATE "C")"#, "é"),
        (r#"lower('É' COLLATE "C")"#, "É"),
        (r#"upper(U&'\1FB3' COLLATE "C.utf8")"#, "\u{1fbc}"),
        (r#"lower(U&'\0130' COLLATE "C.utf8")"#, "i"),
        (r"normalize(U&'\209D',NFKC)", "\u{209d}"),
        (r"normalize(U&'a\1ACF\0327',NFD)", "a\u{1acf}\u{327}"),
    ] {
        expect(sql, Text, Value::Text(value.into()));
    }
    for (sql, value) in [
        (r"U&'e\0301' IS NFC NORMALIZED", false),
        (r"U&'\00E9' IS NFC NORMALIZED", true),
        (r"U&'\00E9' IS NOT NFD NORMALIZED", true),
        (r"U&'e\0301' IS NORMALIZED", false),
    ] {
        expect(sql, Boolean, Value::Bool(value));
    }
    expect("normalize(NULL::text,NFC)", Text, Value::Null);
    expect("unistr(NULL)", Text, Value::Null);
    for sql in [
        "normalize('a',XYZ)",
        "normalize('a','NFC')",
        r"unistr('\D800')",
        r"unistr('\zzz')",
        r"unistr('\004')",
        r"unistr('a\b')",
        r"unistr('x\')",
    ] {
        error(sql, SqlError::Syntax);
    }
    error("normalize(1,NFC)", SqlError::UndefinedFunction);
    error(r"unistr('\0000')", SqlError::InvalidParameterValue);
    error(r"unistr('\+110000')", SqlError::InvalidParameterValue);
}

#[test]
fn unicode_case_budget_tracks_expansion_and_shrinkage() {
    let mut budget = MAX_RESULT_BYTES - 3;
    assert_eq!(
        unicode::casing("upper", "\u{023f}", &mut budget),
        Ok(Value::Text("\u{2c7e}".into()))
    );
    assert_eq!(budget, MAX_RESULT_BYTES);

    let mut budget = MAX_RESULT_BYTES - 2;
    assert_eq!(
        unicode::casing("upper", "\u{023f}", &mut budget),
        Err(SqlError::Limit)
    );
    assert_eq!(budget, MAX_RESULT_BYTES - 2);

    let mut budget = MAX_RESULT_BYTES - 1;
    assert_eq!(
        unicode::casing("lower", "\u{0130}", &mut budget),
        Ok(Value::Text("i".into()))
    );
    assert_eq!(budget, MAX_RESULT_BYTES);
}
