// Float text input against PostgreSQL 18.6 (captured in Docker, postgres:18.6):
// invalid text is 22P02 (invalid_text_representation), out-of-range text is
// 22003, and surrounding whitespace, NaN and the infinities are accepted.
use giggles_core::{Admin, CoreError, Limits, relational::*};
mod common;

struct Fixture {
    admin: Option<Admin>,
    path: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let path = common::fresh("float-text-input");
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}

fn outcome(s: &mut giggles_core::SqlSession, sql: &str) -> Result<Vec<Vec<Value>>, CoreError> {
    s.execute(sql, &[], None)
        .map(|r| r.rows.into_iter().map(|row| row.values).collect())
}

#[test]
fn invalid_float_text_is_invalid_text_representation() {
    let f = Fixture::new();
    let mut s = f.admin.as_ref().unwrap().sql_session();
    for sql in [
        "SELECT 'one'::float4",
        "SELECT 'one'::float8",
        "SELECT 'one'::real",
        "SELECT 'one'::double precision",
        "SELECT ''::float4",
        "SELECT ''::float8",
        "SELECT '1.5x'::float8",
        "SELECT 'one'::text::float8",
    ] {
        assert_eq!(
            outcome(&mut s, sql),
            Err(CoreError::Sql(SqlError::InvalidTextRepresentation)),
            "{sql}"
        );
    }
}

#[test]
fn out_of_range_float_text_is_overflow() {
    let f = Fixture::new();
    let mut s = f.admin.as_ref().unwrap().sql_session();
    for sql in ["SELECT '1e400'::float8", "SELECT '1e40'::float4"] {
        assert_eq!(
            outcome(&mut s, sql),
            Err(CoreError::Sql(SqlError::Overflow)),
            "{sql}"
        );
    }
}

#[test]
fn accepted_float_text() {
    let f = Fixture::new();
    let mut s = f.admin.as_ref().unwrap().sql_session();
    let rows = outcome(&mut s, "SELECT ' 1.5 '::float4, ' 1.5 '::float8").unwrap();
    assert_eq!(
        rows,
        vec![vec![
            Value::Float4(Float4::from_bits(1.5f32.to_bits())),
            Value::Float8(Float8::from_bits(1.5f64.to_bits())),
        ]]
    );
    let rows = outcome(
        &mut s,
        "SELECT 'NaN'::float8, 'inf'::float8, '-Infinity'::float4",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    match &rows[0][..] {
        [Value::Float8(nan), Value::Float8(inf), Value::Float4(minf)] => {
            assert!(f64::from_bits(nan.to_bits()).is_nan());
            assert_eq!(f64::from_bits(inf.to_bits()), f64::INFINITY);
            assert_eq!(f32::from_bits(minf.to_bits()), f32::NEG_INFINITY);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn invalid_float_text_into_float_columns() {
    let f = Fixture::new();
    let mut s = f.admin.as_ref().unwrap().sql_session();
    s.execute(
        "CREATE TABLE t(id integer primary key, a double precision, b real)",
        &[],
        None,
    )
    .unwrap();
    for (sql, expected) in [
        (
            "INSERT INTO t VALUES (1, 'one', 1)",
            SqlError::InvalidTextRepresentation,
        ),
        (
            "INSERT INTO t VALUES (2, 1, 'one')",
            SqlError::InvalidTextRepresentation,
        ),
        ("INSERT INTO t VALUES (3, '1e400', 1)", SqlError::Overflow),
    ] {
        assert_eq!(
            s.execute(sql, &[], None).map(|_| ()),
            Err(CoreError::Sql(expected)),
            "{sql}"
        );
    }
    let rows = outcome(&mut s, "SELECT count(*) FROM t").unwrap();
    assert_eq!(rows, vec![vec![Value::Int(0)]]);
}
