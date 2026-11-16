use super::*;

fn execute(state: &mut Relational, sql: &str) -> R<SqlResult> {
    state.execute(&Prepared::parse(sql, 1, state.epoch)?, &[], None)
}
fn select(expression: &str) -> SqlResult {
    execute(&mut Relational::default(), &format!("SELECT {expression}")).unwrap()
}
fn number(text: &str) -> Value {
    Value::Numeric(Numeric::parse(text).unwrap())
}

#[test]
fn decimal_and_base_literals_preserve_exact_values() {
    for (sql, expected) in [
        ("1_000", Value::Int(1000)),
        ("12_345_678", Value::Int(12345678)),
        ("1_000.25_00", number("1000.2500")),
        ("1_2e1_0", number("120000000000")),
        ("0xF_F", Value::Int(255)),
        ("0o7_7", Value::Int(63)),
        ("0b10_01", Value::Int(9)),
        ("0X1f", Value::Int(31)),
        ("0O17", Value::Int(15)),
        ("0B11", Value::Int(3)),
        ("0x_1", Value::Int(1)),
        (".5_0", number("0.50")),
        ("1_0.", number("10")),
        ("1e1_0", number("10000000000")),
        ("1.5_5e2", number("155")),
        ("-1_000", Value::Int(-1000)),
        ("1_000::bigint + 2_000", Value::Int(3000)),
        ("0x7FFFFFFFFFFFFFFFFF", number("2361183241434822606847")),
        (
            "1_000_000_000_000_000_000_000",
            number("1000000000000000000000"),
        ),
    ] {
        let result = select(sql);
        assert_eq!(result.rows[0].values, vec![expected], "{sql}");
    }
    let result = select("1_000");
    assert_eq!(result.columns, vec!["?column?"]);
    assert_eq!(select("1 _x").columns, vec!["_x"]);
}

#[test]
fn numeric_literal_type_boundaries() {
    for (sql, ty) in [
        ("1_2e1_0", DataType::Numeric),
        ("0xFFFFFFFFFF", DataType::BigInt),
        ("0x80000000", DataType::BigInt),
        ("-2147483648", DataType::Integer),
        ("2147483648", DataType::BigInt),
        ("9223372036854775808", DataType::Numeric),
        ("-9223372036854775808", DataType::BigInt),
        ("-0x8000000000000000", DataType::BigInt),
        ("1_000.5", DataType::Numeric),
        ("1e3", DataType::Numeric),
        ("1_000", DataType::Integer),
    ] {
        assert_eq!(
            Prepared::parse(&format!("SELECT {sql}"), 1, 0)
                .unwrap()
                .describe(&Relational::default())
                .unwrap()
                .columns[0]
                .data_type,
            ty,
            "{sql}"
        );
    }
}

#[test]
fn numeric_literal_junk_is_syntax_not_an_alias() {
    for sql in [
        "1__000", "1_", "100_", "0x", "1_000abc", "123abc", "0x1g", "0b102", "1.5e", "12abc",
        "1e1_000_", "1._0", ".5_", "0x__1", "0o8", "0b_", "1e_2", "1e+", "1\u{e9}", "1$alias",
    ] {
        assert_eq!(
            execute(&mut Relational::default(), &format!("SELECT {sql}")),
            Err(SqlError::Syntax),
            "{sql}"
        );
    }
}

#[test]
fn numeric_and_integer_text_input() {
    for (sql, expected) in [
        ("'1_000'::int", Value::Int(1000)),
        ("'0x1F'::bigint", Value::Int(31)),
        ("'-0b101'::smallint", Value::Int(-5)),
        ("'0o17'::int", Value::Int(15)),
        ("' 1_0 '::int", Value::Int(10)),
        ("'1_000.5'::numeric", number("1000.5")),
        ("'0x10'::numeric", number("16")),
        ("'-0x_10'::numeric", number("-16")),
        ("'1.0_0e1_0'::numeric", number("10000000000")),
    ] {
        assert_eq!(select(sql).rows[0].values, vec![expected], "{sql}");
    }
}

#[test]
fn text_syntax_and_range_errors_and_float_unchanged() {
    for (sql, error) in [
        ("'1__0'::int", SqlError::InvalidTextRepresentation),
        ("'1_0e1'::float8", SqlError::InvalidTextRepresentation),
        ("'1__0'::numeric", SqlError::InvalidTextRepresentation),
        ("'0x'::numeric", SqlError::InvalidTextRepresentation),
        ("'0b102'::numeric", SqlError::InvalidTextRepresentation),
        ("'1e1_'::numeric", SqlError::InvalidTextRepresentation),
        ("'0x8000'::smallint", SqlError::Overflow),
        ("'1_0e1_0_0_0_0_0'::numeric", SqlError::Overflow),
        ("'2_147_483_648'::int", SqlError::Overflow),
        ("'0x8000000000000000'::bigint", SqlError::Overflow),
    ] {
        assert_eq!(
            execute(&mut Relational::default(), &format!("SELECT {sql}")),
            Err(error),
            "{sql}"
        );
    }
}

#[test]
fn numeric_text_in_columns_and_integer_arrays() {
    let mut state = Relational::default();
    execute(
        &mut state,
        "CREATE TABLE n (a smallint, b int, c bigint, d numeric)",
    )
    .unwrap();
    execute(
        &mut state,
        "INSERT INTO n VALUES ('-0b101', '1_000', '0x1F', '1_000.50')",
    )
    .unwrap();
    assert_eq!(
        execute(&mut state, "SELECT * FROM n").unwrap().rows[0].values,
        vec![
            Value::Int(-5),
            Value::Int(1000),
            Value::Int(31),
            number("1000.50")
        ]
    );
    for ty in [DataType::IntArray, DataType::BigIntArray] {
        assert_eq!(
            logical_types::coerce(Value::Text("{1_000,0x1f}".into()), &ty),
            Ok(Value::Text("{1000,31}".into()))
        );
    }
}

#[test]
fn lexer_normalizes_tokens_without_losing_numeric_scale() {
    assert_eq!(
        lex("1_000.25_00 .5_0 0xF_F 1_2e1_0").unwrap(),
        vec![
            Token::Number("1000.2500".into()),
            Token::Number(".50".into()),
            Token::Number("255".into()),
            Token::Number("12e10".into()),
        ]
    );
}
