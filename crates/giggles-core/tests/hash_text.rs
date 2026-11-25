#[path = "common.rs"]
mod common;

use giggles_core::{
    Admin, Limits,
    relational::{DataType, Value, take_sql_error_message},
};

#[test]
fn postgres18_hash_values_and_types() {
    let path = common::fresh("hash-text");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    // PostgreSQL 18.6 UTF-8, including the empty input, multibyte text and
    // a value long enough for multiple lookup3 rounds.
    let query = concat!(
        "SELECT hashtext('x'), hashtext(''), ",
        "pg_catalog.hashtext('adr:123e4567-e89b-12d3-a456-426614174000'), ",
        "hashtext('change-types:org-1'), hashtext('h\u{e9}llo w\u{f6}rld'), ",
        "hashtext(repeat('abcdefgh', 100)), hashtextextended('x', 0), ",
        "hashtextextended('x', 1), hashtextextended('x', -1)",
    );
    let prepared = sql.prepare(query).unwrap();
    let description = sql.describe(&prepared).unwrap();
    let types: Vec<_> = description
        .columns
        .into_iter()
        .map(|column| column.data_type)
        .collect();
    assert_eq!(
        types,
        [
            DataType::Integer,
            DataType::Integer,
            DataType::Integer,
            DataType::Integer,
            DataType::Integer,
            DataType::Integer,
            DataType::BigInt,
            DataType::BigInt,
            DataType::BigInt,
        ]
    );
    let expected = [
        1074944137,
        -1477818771,
        1697605673,
        631048064,
        1823351448,
        -1015688872,
        -474845166489675639,
        -504344683840945215,
        6068125099253810556,
    ]
    .map(Value::Int);
    assert_eq!(
        sql.execute(query, &[], None).unwrap().rows[0].values,
        expected
    );
    drop(sql);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn strict_hashes_infer_text_and_bigint_parameters() {
    let path = common::fresh("hash-text-null");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    let query = "SELECT hashtext($1), hashtextextended($1, $2), hashtextextended('x', $2)";
    let prepared = sql.prepare(query).unwrap();
    assert_eq!(
        sql.describe(&prepared).unwrap().parameters,
        [DataType::Text, DataType::BigInt]
    );
    assert_eq!(
        sql.execute(query, &[Value::Null, Value::Null], None)
            .unwrap()
            .rows[0]
            .values,
        [Value::Null, Value::Null, Value::Null],
    );
    drop(sql);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn hash_text_uses_implicit_string_casts_and_reports_missing_overloads() {
    let path = common::fresh("hash-text-casts");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    let result = sql.execute(
        "SELECT hashtext('a'::char(3)) = hashtext('a'), hashtext('a  '::varchar) = hashtext('a')",
        &[],
        None,
    ).unwrap();
    assert_eq!(
        result.rows[0].values,
        [Value::Bool(true), Value::Bool(false)]
    );
    let error = sql.execute("SELECT hashtext(1)", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "42883");
    assert_eq!(
        take_sql_error_message().as_deref(),
        Some("function hashtext(integer) does not exist")
    );
    drop(sql);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
