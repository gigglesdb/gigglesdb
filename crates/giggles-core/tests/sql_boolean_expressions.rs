//! DD-0068 F15: PostgreSQL 18.6 boolean-expression oracle.
//! The expected values, result types, parameter types and SQLSTATE-equivalent
//! errors are captured inline, so running this test needs no external oracle.
//! Binary-wire oracle rows below assert their logical core values and types;
//! the transport's byte encoding belongs to the server's wire tests.
//! SQLSTATE mapping: UndefinedFunction=42883, InvalidTextRepresentation=22P02,
//! Type=42804, InvalidEscapeSequence=22025, DivisionByZero=22012,
//! IndeterminateDatatype=42P18 (the server maps these core error variants).
mod common;
use giggles_core::{
    relational::{
        DataType as T, Date, Float8, Numeric, Prepared, SqlError as E, StatementClock, Value as V,
    },
    *,
};
use giggles_pg_types::temporal::SessionZone;

struct Case {
    name: &'static str,
    sql: &'static str,
    expected: Result<Vec<Vec<V>>, E>,
    types: Vec<T>,
    description_error: Option<E>,
    params: Vec<V>,
    declarations: Vec<Option<T>>,
    parameter_types: Vec<T>,
}

impl Case {
    fn ok(name: &'static str, sql: &'static str, rows: Vec<Vec<V>>, types: Vec<T>) -> Self {
        Self {
            name,
            sql,
            expected: Ok(rows),
            types,
            description_error: None,
            params: vec![],
            declarations: vec![],
            parameter_types: vec![],
        }
    }
    fn error(name: &'static str, sql: &'static str, error: E) -> Self {
        Self {
            name,
            sql,
            expected: Err(error.clone()),
            types: vec![],
            description_error: Some(error),
            params: vec![],
            declarations: vec![],
            parameter_types: vec![],
        }
    }
    fn runtime(name: &'static str, sql: &'static str, error: E, types: Vec<T>) -> Self {
        Self {
            name,
            sql,
            expected: Err(error),
            types,
            description_error: None,
            params: vec![],
            declarations: vec![],
            parameter_types: vec![],
        }
    }
    fn parameter(
        mut self,
        value: V,
        declarations: Vec<Option<T>>,
        parameter_types: Vec<T>,
    ) -> Self {
        self.params = vec![value];
        self.declarations = declarations;
        self.parameter_types = parameter_types;
        self
    }
}

fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 1,
        statement_us: 1,
        // PG18 probe sessions use UTC. Cross-zone comparisons require explicit
        // session context, even when their literals happen to carry +00 offsets.
        zone: Some(SessionZone::Fixed(0)),
        wall: None,
    }
}

fn check_cases(session: &mut SqlSession, cases: Vec<Case>) {
    let mut failures = Vec::new();
    for case in cases {
        let prepared = session.prepare(case.sql).and_then(|p| {
            if case.declarations.is_empty() {
                Ok(p)
            } else {
                p.with_parameter_types(&case.declarations)
                    .map_err(CoreError::Sql)
            }
        });
        let description = prepared
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|p| session.describe(p))
            .map(|d| {
                (
                    d.parameters,
                    d.columns
                        .into_iter()
                        .map(|c| c.data_type)
                        .collect::<Vec<_>>(),
                )
            });
        let expected_description = match case.description_error {
            Some(error) => Err(CoreError::Sql(error)),
            None => Ok((case.parameter_types, case.types)),
        };
        if description != expected_description {
            failures.push(format!(
                "{} ({}): describe {description:?}, expected {expected_description:?}",
                case.name, case.sql
            ));
        }
        let actual = if case.params.is_empty() {
            session.execute_at(case.sql, &[], None, &clock())
        } else {
            prepared.and_then(|p| session.execute_prepared_at(&p, &case.params, None, &clock()))
        }
        .map(|r| r.rows.into_iter().map(|r| r.values).collect::<Vec<_>>());
        let expected = case.expected.map_err(CoreError::Sql);
        if actual != expected {
            failures.push(format!(
                "{} ({}): execute {actual:?}, expected {expected:?}",
                case.name, case.sql
            ));
        }
        // Numeric equality ignores scale, which remains observable in PG text output.
        if let (Ok(actual), Ok(expected)) = (&actual, &expected) {
            for (actual, expected) in actual.iter().zip(expected) {
                for (actual, expected) in actual.iter().zip(expected) {
                    if let (V::Numeric(actual), V::Numeric(expected)) = (actual, expected)
                        && actual.to_string() != expected.to_string()
                    {
                        failures.push(format!(
                            "{}: numeric scale {actual}, expected {expected}",
                            case.name
                        ));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn pg18_boolean_expression_oracle() {
    let path = common::fresh("boolean-expressions");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    // Exact probe SETUP, including the primary-key type and nullable columns.
    session
        .execute("DROP TABLE IF EXISTS bt", &[], None)
        .unwrap();
    session.execute("CREATE TABLE bt(id integer primary key, i integer, b boolean, t text, n numeric, f double precision, d date)", &[], None).unwrap();
    session.execute("INSERT INTO bt VALUES (1, 1, true, 'abc', 1.5, 0.5, '2024-01-01'), (2, 2, false, 'Bcd', 2, 2.5, '2024-01-02'), (3, 0, NULL, 'xyc', 0, 0, '2023-12-31'), (4, NULL, true, NULL, NULL, NULL, NULL)", &[], None).unwrap();
    let cases = vec![
        Case::ok(
            "nf eq",
            "SELECT 1 = 1",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf ne",
            "SELECT 1 <> 2",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf bang",
            "SELECT 1 != 1",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf lt",
            "SELECT 1 < 2",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf ge",
            "SELECT 2 >= 3",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf eq null",
            "SELECT 1 = NULL",
            vec![vec![V::Null]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf null eq null",
            "SELECT NULL = NULL",
            vec![vec![V::Null]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf text eq",
            "SELECT 'a' = 'a'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf text lt",
            "SELECT 'a' < 'b'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf num int",
            "SELECT 1.5 > 1",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf int num eq",
            "SELECT 2 = 2.0",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf float int",
            "SELECT 0.1::float8 = 0.1",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf date tstz",
            "SELECT DATE '2024-01-01' = TIMESTAMPTZ '2024-01-01 00:00+00'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf ts date",
            "SELECT TIMESTAMP '2024-01-01' > DATE '2023-12-31'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf time timetz",
            "SELECT TIME '10:00' = TIMETZ '10:00+00'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf time interval",
            "SELECT TIME '24:00' = INTERVAL '1 day'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::error(
            "nf no op",
            "SELECT TIMETZ '10:00+00' = INTERVAL '1 hour'",
            E::UndefinedFunction,
        ),
        Case::error(
            "nf mismatch int text col",
            "SELECT 1 = 'x'",
            E::InvalidTextRepresentation,
        ),
        Case::ok(
            "nf bool lit t",
            "SELECT true",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf bool lit f",
            "SELECT false",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf bool null",
            "SELECT NULL::boolean",
            vec![vec![V::Null]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf bool eq",
            "SELECT true = false",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf bool lt",
            "SELECT false < true",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf bool text",
            "SELECT 't'::boolean",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf and",
            "SELECT true AND false",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf or",
            "SELECT true OR false",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf not",
            "SELECT NOT true",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf not null",
            "SELECT NOT NULL::boolean",
            vec![vec![V::Null]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf and null f",
            "SELECT NULL AND false",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf and null t",
            "SELECT NULL AND true",
            vec![vec![V::Null]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf or null t",
            "SELECT NULL OR true",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf or null f",
            "SELECT NULL OR false",
            vec![vec![V::Null]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf not prec",
            "SELECT NOT 1 = 2",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf and or prec",
            "SELECT true OR false AND false",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf paren",
            "SELECT (true OR false) AND false",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf is null",
            "SELECT 1 IS NULL",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf is not null",
            "SELECT 1 IS NOT NULL",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf null is null",
            "SELECT NULL IS NULL",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf cmp is null",
            "SELECT (1 = NULL) IS NULL",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf cmp is null noparen",
            "SELECT 1 = NULL IS NULL",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf is true",
            "SELECT true IS TRUE",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf null is true",
            "SELECT NULL::boolean IS TRUE",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf is not true",
            "SELECT NULL::boolean IS NOT TRUE",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf is false",
            "SELECT false IS FALSE",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf is not false",
            "SELECT NULL::boolean IS NOT FALSE",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf is unknown",
            "SELECT NULL::boolean IS UNKNOWN",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf is not unknown",
            "SELECT true IS NOT UNKNOWN",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf cmp is true",
            "SELECT 1 = 1 IS TRUE",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::error("nf int is true", "SELECT 1 IS TRUE", E::Type),
        Case::ok(
            "nf distinct",
            "SELECT 1 IS DISTINCT FROM 2",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf not distinct",
            "SELECT 1 IS NOT DISTINCT FROM 1",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf distinct null",
            "SELECT 1 IS DISTINCT FROM NULL",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf null distinct null",
            "SELECT NULL IS DISTINCT FROM NULL",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf null not distinct null",
            "SELECT NULL IS NOT DISTINCT FROM NULL",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf distinct cross",
            "SELECT 1 IS DISTINCT FROM 1.0",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf distinct text",
            "SELECT 'a' IS DISTINCT FROM 'b'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::error(
            "nf distinct mismatch",
            "SELECT 1 IS DISTINCT FROM 'x'",
            E::InvalidTextRepresentation,
        ),
        Case::ok(
            "nf between",
            "SELECT 2 BETWEEN 1 AND 3",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf not between",
            "SELECT 2 NOT BETWEEN 1 AND 3",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf between null",
            "SELECT 2 BETWEEN NULL AND 3",
            vec![vec![V::Null]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf between null f",
            "SELECT 5 BETWEEN NULL AND 3",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf in",
            "SELECT 2 IN (1, 2, 3)",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf not in",
            "SELECT 2 NOT IN (1, 3)",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf in null",
            "SELECT 2 IN (1, NULL)",
            vec![vec![V::Null]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf in null hit",
            "SELECT 1 IN (1, NULL)",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf not in null",
            "SELECT 2 NOT IN (1, NULL)",
            vec![vec![V::Null]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf in cross",
            "SELECT 2 IN (2.0, 3)",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf like",
            "SELECT 'abc' LIKE 'a%'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf not like",
            "SELECT 'abc' NOT LIKE 'a%'",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf ilike",
            "SELECT 'ABC' ILIKE 'a%'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf like null",
            "SELECT NULL LIKE 'a'",
            vec![vec![V::Null]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf like escape",
            "SELECT 'a%' LIKE 'a#%' ESCAPE '#'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::runtime(
            "nf like bad escape",
            "SELECT 'a' LIKE 'a' ESCAPE 'xy'",
            E::InvalidEscapeSequence,
            vec![T::Boolean],
        ),
        Case::ok(
            "nf case searched",
            "SELECT CASE WHEN 1 = 1 THEN 'y' ELSE 'n' END",
            vec![vec![V::Text("y".into())]],
            vec![T::Text],
        ),
        Case::ok(
            "nf case no else",
            "SELECT CASE WHEN 1 = 2 THEN 1 END",
            vec![vec![V::Null]],
            vec![T::Integer],
        ),
        Case::ok(
            "nf case simple",
            "SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
            vec![vec![V::Text("two".into())]],
            vec![T::Text],
        ),
        Case::ok(
            "nf case simple null",
            "SELECT CASE NULL::int WHEN 1 THEN 'one' ELSE 'none' END",
            vec![vec![V::Text("none".into())]],
            vec![T::Text],
        ),
        Case::ok(
            "nf case null when",
            "SELECT CASE WHEN NULL THEN 1 ELSE 2 END",
            vec![vec![V::Int(2)]],
            vec![T::Integer],
        ),
        Case::ok(
            "nf case int num",
            "SELECT CASE WHEN true THEN 1 ELSE 2.5 END",
            vec![vec![V::Numeric(Numeric::parse("1").unwrap())]],
            vec![T::Numeric],
        ),
        Case::ok(
            "nf case int bigint",
            "SELECT CASE WHEN true THEN 1 ELSE 3000000000 END",
            vec![vec![V::Int(1)]],
            vec![T::BigInt],
        ),
        Case::ok(
            "nf case int float",
            "SELECT CASE WHEN false THEN 1 ELSE 2.5::float8 END",
            vec![vec![V::Float8(Float8::parse("2.5").unwrap())]],
            vec![T::DoublePrecision],
        ),
        Case::ok(
            "nf case all unknown",
            "SELECT CASE WHEN true THEN 'a' ELSE 'b' END",
            vec![vec![V::Text("a".into())]],
            vec![T::Text],
        ),
        Case::error(
            "nf case int text",
            "SELECT CASE WHEN true THEN 1 ELSE 'x' END",
            E::InvalidTextRepresentation,
        ),
        Case::error(
            "nf case int textcol",
            "SELECT CASE WHEN true THEN 1 ELSE 'x'::text END",
            E::Type,
        ),
        Case::error(
            "nf case bool int",
            "SELECT CASE WHEN true THEN true ELSE 1 END",
            E::Type,
        ),
        Case::ok(
            "nf case all null",
            "SELECT CASE WHEN true THEN NULL END",
            vec![vec![V::Null]],
            vec![T::Text],
        ),
        Case::ok(
            "nf case bool",
            "SELECT CASE WHEN 1 < 2 THEN 1 = 1 ELSE false END",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf case nested",
            "SELECT CASE WHEN true THEN CASE WHEN false THEN 'a' ELSE 'b' END END",
            vec![vec![V::Text("b".into())]],
            vec![T::Text],
        ),
        Case::error(
            "nf case non-bool when",
            "SELECT CASE WHEN 1 THEN 1 END",
            E::Type,
        ),
        Case::ok(
            "nf case simple cross",
            "SELECT CASE 1 WHEN 1.0 THEN 'y' ELSE 'n' END",
            vec![vec![V::Text("y".into())]],
            vec![T::Text],
        ),
        Case::error(
            "nf case simple mismatch",
            "SELECT CASE 1 WHEN 'x' THEN 'y' END",
            E::InvalidTextRepresentation,
        ),
        Case::ok(
            "nf case date text",
            "SELECT CASE WHEN true THEN DATE '2024-01-01' ELSE '2024-02-02' END",
            vec![vec![V::Date(
                Date::parse("2024-01-01", &Default::default()).unwrap(),
            )]],
            vec![T::Date],
        ),
        // Probe row `nf case varchar text` remains wire-probe coverage only:
        // the existing bounded core has no varchar scalar cast, so that SQL is
        // not core-expressible. A typed text CASE companion appears below.
        Case::ok(
            "nf div guard and",
            "SELECT false AND 1/0 = 0",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf div guard or",
            "SELECT true OR 1/0 = 0",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::runtime(
            "nf div rhs and",
            "SELECT 1/0 = 0 AND false",
            E::DivisionByZero,
            vec![T::Boolean],
        ),
        Case::ok(
            "nf div case",
            "SELECT CASE WHEN false THEN 1/0 ELSE 1 END",
            vec![vec![V::Int(1)]],
            vec![T::Integer],
        ),
        Case::runtime(
            "nf div case when",
            "SELECT CASE WHEN 1/0 = 0 THEN 1 ELSE 1 END",
            E::DivisionByZero,
            vec![T::Integer],
        ),
        Case::ok(
            "nf alias",
            "SELECT 1 = 1 AS eq",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf two",
            "SELECT 1 < 2, NOT false",
            vec![vec![V::Bool(true), V::Bool(true)]],
            vec![T::Boolean, T::Boolean],
        ),
        Case::ok(
            "nf cast to int",
            "SELECT (1 = 1)::int",
            vec![vec![V::Int(1)]],
            vec![T::Integer],
        ),
        Case::ok(
            "nf cast to text",
            "SELECT (1 = 1)::text",
            vec![vec![V::Text("true".into())]],
            vec![T::Text],
        ),
        Case::ok(
            "nf bool text concat",
            "SELECT 'x' || (1 = 1)",
            vec![vec![V::Text("xtrue".into())]],
            vec![T::Text],
        ),
        Case::ok(
            "nf fn arg coalesce",
            "SELECT coalesce(NULL = 1, false)",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf fn arg concat",
            "SELECT concat('v', 1 < 2)",
            vec![vec![V::Text("vt".into())]],
            vec![T::Text],
        ),
        Case::ok(
            "nf fn arg length",
            "SELECT length((1 = 1)::text)",
            vec![vec![V::Int(4)]],
            vec![T::Integer],
        ),
        Case::ok(
            "nf where true",
            "SELECT 1 = 1 WHERE true",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf order",
            "SELECT 1 ORDER BY 1 = 1",
            vec![vec![V::Int(1)]],
            vec![T::Integer],
        ),
        Case::ok(
            "nf bin true",
            "SELECT 1 = 1",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf bin false",
            "SELECT 1 = 2",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "nf bin null",
            "SELECT 1 = NULL",
            vec![vec![V::Null]],
            vec![T::Boolean],
        ),
        Case::ok(
            "p eq",
            "SELECT $1 = 1",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        )
        .parameter(V::Int(1), vec![], vec![T::Integer]),
        Case::ok(
            "p eq int",
            "SELECT $1 = 1",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        )
        .parameter(V::Int(1), vec![Some(T::Integer)], vec![T::Integer]),
        Case::ok(
            "p bool",
            "SELECT $1 AND true",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        )
        .parameter(V::Bool(true), vec![], vec![T::Boolean]),
        Case::ok(
            "p bool decl",
            "SELECT $1 AND true",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        )
        .parameter(V::Bool(false), vec![Some(T::Boolean)], vec![T::Boolean]),
        Case::ok(
            "p not",
            "SELECT NOT $1",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        )
        .parameter(V::Bool(true), vec![], vec![T::Boolean]),
        Case::error("p is null", "SELECT $1 IS NULL", E::IndeterminateDatatype).parameter(
            V::Null,
            vec![],
            vec![],
        ),
        Case::ok(
            "p case",
            "SELECT CASE WHEN $1 THEN 1 ELSE 0 END",
            vec![vec![V::Int(1)]],
            vec![T::Integer],
        )
        .parameter(V::Bool(true), vec![], vec![T::Boolean]),
        Case::ok(
            "p case then",
            "SELECT CASE WHEN true THEN $1 ELSE 'x' END",
            vec![vec![V::Text("v".into())]],
            vec![T::Text],
        )
        .parameter(V::Text("v".into()), vec![], vec![T::Text]),
        Case::ok(
            "p case then int",
            "SELECT CASE WHEN true THEN $1 ELSE 1 END",
            vec![vec![V::Int(7)]],
            vec![T::Integer],
        )
        .parameter(V::Int(7), vec![], vec![T::Integer]),
        Case::ok(
            "p between",
            "SELECT $1 BETWEEN 1 AND 3",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        )
        .parameter(V::Int(2), vec![], vec![T::Integer]),
        Case::ok(
            "p in",
            "SELECT $1 IN (1, 2)",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        )
        .parameter(V::Int(2), vec![], vec![T::Integer]),
        Case::ok(
            "p like",
            "SELECT $1 LIKE 'a%'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        )
        .parameter(V::Text("abc".into()), vec![], vec![T::Text]),
        Case::ok(
            "p distinct",
            "SELECT $1 IS DISTINCT FROM 1",
            vec![vec![V::Bool(false)]],
            vec![T::Boolean],
        )
        .parameter(V::Int(1), vec![], vec![T::Integer]),
        Case::ok(
            "p text decl eq",
            "SELECT $1 = 'a'",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        )
        .parameter(V::Text("a".into()), vec![Some(T::Text)], vec![T::Text]),
        Case::ok(
            "t cols",
            "SELECT id, i = 1, b, NOT b, b AND i > 1, b OR NULL FROM bt ORDER BY id",
            vec![
                vec![
                    V::Int(1),
                    V::Bool(true),
                    V::Bool(true),
                    V::Bool(false),
                    V::Bool(false),
                    V::Bool(true),
                ],
                vec![
                    V::Int(2),
                    V::Bool(false),
                    V::Bool(false),
                    V::Bool(true),
                    V::Bool(false),
                    V::Null,
                ],
                vec![
                    V::Int(3),
                    V::Bool(false),
                    V::Null,
                    V::Null,
                    V::Bool(false),
                    V::Null,
                ],
                vec![
                    V::Int(4),
                    V::Null,
                    V::Bool(true),
                    V::Bool(false),
                    V::Null,
                    V::Bool(true),
                ],
            ],
            vec![
                T::Integer,
                T::Boolean,
                T::Boolean,
                T::Boolean,
                T::Boolean,
                T::Boolean,
            ],
        ),
        Case::ok(
            "t bin",
            "SELECT b, i = 1 FROM bt ORDER BY id",
            vec![
                vec![V::Bool(true), V::Bool(true)],
                vec![V::Bool(false), V::Bool(false)],
                vec![V::Null, V::Bool(false)],
                vec![V::Bool(true), V::Null],
            ],
            vec![T::Boolean, T::Boolean],
        ),
        Case::ok(
            "t is",
            "SELECT id, b IS TRUE, b IS NOT FALSE, b IS UNKNOWN, i IS NULL FROM bt ORDER BY id",
            vec![
                vec![
                    V::Int(1),
                    V::Bool(true),
                    V::Bool(true),
                    V::Bool(false),
                    V::Bool(false),
                ],
                vec![
                    V::Int(2),
                    V::Bool(false),
                    V::Bool(false),
                    V::Bool(false),
                    V::Bool(false),
                ],
                vec![
                    V::Int(3),
                    V::Bool(false),
                    V::Bool(true),
                    V::Bool(true),
                    V::Bool(false),
                ],
                vec![
                    V::Int(4),
                    V::Bool(true),
                    V::Bool(true),
                    V::Bool(false),
                    V::Bool(true),
                ],
            ],
            vec![T::Integer, T::Boolean, T::Boolean, T::Boolean, T::Boolean],
        ),
        Case::ok(
            "t distinct",
            "SELECT id, i IS DISTINCT FROM 2, i IS NOT DISTINCT FROM NULL FROM bt ORDER BY id",
            vec![
                vec![V::Int(1), V::Bool(true), V::Bool(false)],
                vec![V::Int(2), V::Bool(false), V::Bool(false)],
                vec![V::Int(3), V::Bool(true), V::Bool(false)],
                vec![V::Int(4), V::Bool(true), V::Bool(true)],
            ],
            vec![T::Integer, T::Boolean, T::Boolean],
        ),
        Case::ok(
            "t between in",
            "SELECT id, i BETWEEN 1 AND 2, i IN (2, 3), i NOT IN (1) FROM bt ORDER BY id",
            vec![
                vec![V::Int(1), V::Bool(true), V::Bool(false), V::Bool(false)],
                vec![V::Int(2), V::Bool(true), V::Bool(true), V::Bool(true)],
                vec![V::Int(3), V::Bool(false), V::Bool(false), V::Bool(true)],
                vec![V::Int(4), V::Null, V::Null, V::Null],
            ],
            vec![T::Integer, T::Boolean, T::Boolean, T::Boolean],
        ),
        Case::ok(
            "t like",
            "SELECT id, t LIKE 'a%', t ILIKE 'B%', t NOT LIKE '%c' FROM bt ORDER BY id",
            vec![
                vec![V::Int(1), V::Bool(true), V::Bool(false), V::Bool(false)],
                vec![V::Int(2), V::Bool(false), V::Bool(true), V::Bool(true)],
                vec![V::Int(3), V::Bool(false), V::Bool(false), V::Bool(false)],
                vec![V::Int(4), V::Null, V::Null, V::Null],
            ],
            vec![T::Integer, T::Boolean, T::Boolean, T::Boolean],
        ),
        Case::ok(
            "t case",
            "SELECT id, CASE WHEN i > 1 THEN 'big' WHEN i = 1 THEN 'one' ELSE 'none' END FROM bt ORDER BY id",
            vec![
                vec![V::Int(1), V::Text("one".into())],
                vec![V::Int(2), V::Text("big".into())],
                vec![V::Int(3), V::Text("none".into())],
                vec![V::Int(4), V::Text("none".into())],
            ],
            vec![T::Integer, T::Text],
        ),
        Case::ok(
            "t case simple",
            "SELECT id, CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM bt ORDER BY id",
            vec![
                vec![V::Int(1), V::Text("one".into())],
                vec![V::Int(2), V::Text("two".into())],
                vec![V::Int(3), V::Null],
                vec![V::Int(4), V::Null],
            ],
            vec![T::Integer, T::Text],
        ),
        Case::ok(
            "t case num",
            "SELECT id, CASE WHEN b THEN n ELSE i END FROM bt ORDER BY id",
            vec![
                vec![V::Int(1), V::Numeric(Numeric::parse("1.5").unwrap())],
                vec![V::Int(2), V::Numeric(Numeric::parse("2").unwrap())],
                vec![V::Int(3), V::Numeric(Numeric::parse("0").unwrap())],
                vec![V::Int(4), V::Null],
            ],
            vec![T::Integer, T::Numeric],
        ),
        Case::ok(
            "t case float",
            "SELECT id, CASE WHEN b THEN f ELSE n END FROM bt ORDER BY id",
            vec![
                vec![V::Int(1), V::Float8(Float8::parse("0.5").unwrap())],
                vec![V::Int(2), V::Float8(Float8::parse("2").unwrap())],
                vec![V::Int(3), V::Float8(Float8::parse("0").unwrap())],
                vec![V::Int(4), V::Null],
            ],
            vec![T::Integer, T::DoublePrecision],
        ),
        Case::error(
            "t case text int",
            "SELECT id, CASE WHEN b THEN t ELSE i END FROM bt ORDER BY id",
            E::Type,
        ),
        Case::ok(
            "t cross",
            "SELECT id, d = TIMESTAMP '2024-01-01', n = i, f > i FROM bt ORDER BY id",
            vec![
                vec![V::Int(1), V::Bool(true), V::Bool(false), V::Bool(false)],
                vec![V::Int(2), V::Bool(false), V::Bool(true), V::Bool(true)],
                vec![V::Int(3), V::Bool(false), V::Bool(true), V::Bool(false)],
                vec![V::Int(4), V::Null, V::Null, V::Null],
            ],
            vec![T::Integer, T::Boolean, T::Boolean, T::Boolean],
        ),
        Case::ok(
            "t order bool",
            "SELECT id FROM bt ORDER BY i = 2, id",
            vec![
                vec![V::Int(1)],
                vec![V::Int(3)],
                vec![V::Int(2)],
                vec![V::Int(4)],
            ],
            vec![T::Integer],
        ),
        Case::ok(
            "t order bool desc",
            "SELECT id FROM bt ORDER BY b DESC, id",
            vec![
                vec![V::Int(3)],
                vec![V::Int(1)],
                vec![V::Int(4)],
                vec![V::Int(2)],
            ],
            vec![T::Integer],
        ),
        Case::ok(
            "t order case",
            "SELECT id FROM bt ORDER BY CASE WHEN i = 2 THEN 0 ELSE 1 END, id",
            vec![
                vec![V::Int(2)],
                vec![V::Int(1)],
                vec![V::Int(3)],
                vec![V::Int(4)],
            ],
            vec![T::Integer],
        ),
        Case::ok(
            "t where case",
            "SELECT id FROM bt WHERE CASE WHEN i > 1 THEN true ELSE false END ORDER BY id",
            vec![vec![V::Int(2)]],
            vec![T::Integer],
        ),
        Case::ok(
            "t where is true",
            "SELECT id FROM bt WHERE b IS NOT TRUE ORDER BY id",
            vec![vec![V::Int(2)], vec![V::Int(3)]],
            vec![T::Integer],
        ),
        Case::ok(
            "t where distinct",
            "SELECT id FROM bt WHERE i IS DISTINCT FROM 1 ORDER BY id",
            vec![vec![V::Int(2)], vec![V::Int(3)], vec![V::Int(4)]],
            vec![T::Integer],
        ),
        Case::ok(
            "t where coalesce cmp",
            "SELECT id FROM bt WHERE coalesce(i = 1, false) ORDER BY id",
            vec![vec![V::Int(1)]],
            vec![T::Integer],
        ),
        Case::ok(
            "t fn arg",
            "SELECT id, coalesce(b AND i > 1, false) FROM bt ORDER BY id",
            vec![
                vec![V::Int(1), V::Bool(false)],
                vec![V::Int(2), V::Bool(false)],
                vec![V::Int(3), V::Bool(false)],
                vec![V::Int(4), V::Bool(false)],
            ],
            vec![T::Integer, T::Boolean],
        ),
        Case::ok(
            "t div guard",
            "SELECT id, i = 0 OR 10 / i > 3 FROM bt WHERE i IS NOT NULL ORDER BY id",
            vec![
                vec![V::Int(1), V::Bool(true)],
                vec![V::Int(2), V::Bool(true)],
                vec![V::Int(3), V::Bool(true)],
            ],
            vec![T::Integer, T::Boolean],
        ),
        Case::ok(
            "t case div",
            "SELECT id, CASE WHEN i = 0 THEN 0 ELSE 10 / i END FROM bt WHERE i IS NOT NULL ORDER BY id",
            vec![
                vec![V::Int(1), V::Int(10)],
                vec![V::Int(2), V::Int(5)],
                vec![V::Int(3), V::Int(0)],
            ],
            vec![T::Integer, T::Integer],
        ),
        Case::ok(
            "t agg cmp",
            "SELECT count(*) > 1 FROM bt",
            vec![vec![V::Bool(true)]],
            vec![T::Boolean],
        ),
        Case::ok(
            "t agg case",
            "SELECT CASE WHEN count(*) > 1 THEN 'many' ELSE 'few' END FROM bt",
            vec![vec![V::Text("many".into())]],
            vec![T::Text],
        ),
        Case::ok(
            "t group bool",
            "SELECT b, count(*) FROM bt GROUP BY b ORDER BY b",
            vec![
                vec![V::Bool(false), V::Int(1)],
                vec![V::Bool(true), V::Int(2)],
                vec![V::Null, V::Int(1)],
            ],
            vec![T::Boolean, T::BigInt],
        ),
        Case::ok(
            "t bool col eq",
            "SELECT id, b = true, b <> false FROM bt ORDER BY id",
            vec![
                vec![V::Int(1), V::Bool(true), V::Bool(true)],
                vec![V::Int(2), V::Bool(false), V::Bool(false)],
                vec![V::Int(3), V::Null, V::Null],
                vec![V::Int(4), V::Bool(true), V::Bool(true)],
            ],
            vec![T::Integer, T::Boolean, T::Boolean],
        ),
        Case::ok(
            "t count bool",
            "SELECT count(i = 1) FROM bt",
            vec![vec![V::Int(3)]],
            vec![T::BigInt],
        ),
        Case::ok(
            "t null cmp",
            "SELECT id, i = NULL FROM bt ORDER BY id",
            vec![
                vec![V::Int(1), V::Null],
                vec![V::Int(2), V::Null],
                vec![V::Int(3), V::Null],
                vec![V::Int(4), V::Null],
            ],
            vec![T::Integer, T::Boolean],
        ),
        Case::error(
            "t case mismatch col",
            "SELECT CASE WHEN b THEN d ELSE i END FROM bt",
            E::Type,
        ),
    ];
    check_cases(&mut session, cases);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn boolean_where_keeps_rows_and_literal_primary_key_lookup() {
    let path = common::fresh("boolean-where-regressions");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE bt(id integer PRIMARY KEY, b boolean)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO bt VALUES(1,true),(2,false),(3,NULL)",
            &[],
            None,
        )
        .unwrap();
    check_cases(
        &mut session,
        vec![
            Case::ok(
                "parenthesized key",
                "SELECT id FROM bt WHERE (id=1)",
                vec![vec![V::Int(1)]],
                vec![T::Integer],
            ),
            Case::ok(
                "key and boolean",
                "SELECT id FROM bt WHERE id=1 AND b",
                vec![vec![V::Int(1)]],
                vec![T::Integer],
            ),
            Case::ok(
                "parenthesized key and boolean",
                "SELECT id FROM bt WHERE (id=1) AND b",
                vec![vec![V::Int(1)]],
                vec![T::Integer],
            ),
            Case::ok(
                "false boolean",
                "SELECT id FROM bt WHERE id=2 AND b",
                vec![],
                vec![T::Integer],
            ),
            Case::ok(
                "unknown boolean",
                "SELECT id FROM bt WHERE id=3 AND b",
                vec![],
                vec![T::Integer],
            ),
        ],
    );
    for sql in [
        "EXPLAIN SELECT id FROM bt WHERE id=1",
        "EXPLAIN SELECT id FROM bt WHERE (id=1)",
    ] {
        let result = session.execute(sql, &[], None).unwrap();
        let V::Text(plan) = &result.rows[0].values[0] else {
            panic!("{sql}: no text plan")
        };
        assert!(plan.starts_with("PrimaryKeyLookup"), "{sql}: {plan}");
    }
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn boolean_sequence_dependency_and_frozen_parser_guards() {
    let path = common::fresh("boolean-guards");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    // Dependencies are syntactic, including CASE conditions and untaken arms,
    // and must survive before sequence resolution or gateway authorization.
    for sql in [
        "SELECT nextval('s')=1",
        "SELECT CASE WHEN nextval('s')=1 THEN 0 ELSE 0 END",
        "SELECT CASE WHEN false THEN nextval('s') ELSE 0 END",
        "SELECT CASE WHEN true THEN 0 ELSE nextval('s') END",
    ] {
        let prepared = session.prepare(sql).unwrap();
        assert_eq!(prepared.sequences(), vec![Some("s".into())], "{sql}");
        assert_eq!(
            Prepared::sequence_dependencies(sql),
            vec![Some("s".into())],
            "{sql}"
        );
        assert!(
            prepared.table().is_some(),
            "sequence authority must not become table-free: {sql}"
        );
        assert!(prepared.sequence_only(), "{sql}");
    }

    // Execute the same form against an actual owned sequence to check its value
    // and result metadata without depending on standalone CREATE SEQUENCE.
    session
        .execute("CREATE TABLE owner(id serial PRIMARY KEY)", &[], None)
        .unwrap();
    check_cases(
        &mut session,
        vec![
            Case::ok(
                "repeated IS predicates",
                "SELECT true IS TRUE IS TRUE, NULL::boolean IS NOT TRUE IS FALSE",
                vec![vec![V::Bool(true), V::Bool(false)]],
                vec![T::Boolean, T::Boolean],
            ),
            Case::ok(
                "NOT follows IS precedence",
                "SELECT NOT NULL::boolean IS TRUE, (NOT NULL::boolean) IS TRUE",
                vec![vec![V::Bool(true), V::Bool(false)]],
                vec![T::Boolean, T::Boolean],
            ),
            Case::ok(
                "predicate on DISTINCT left",
                "SELECT 1=1 IS DISTINCT FROM false, (1=NULL) IS NOT DISTINCT FROM NULL",
                vec![vec![V::Bool(true), V::Bool(true)]],
                vec![T::Boolean, T::Boolean],
            ),
            Case::ok(
                "typed text CASE companion",
                "SELECT CASE WHEN true THEN 'a'::text ELSE 'b'::text END",
                vec![vec![V::Text("a".into())]],
                vec![T::Text],
            ),
            Case::ok(
                "sequence comparison",
                "SELECT nextval('owner_id_seq')=1",
                vec![vec![V::Bool(true)]],
                vec![T::Boolean],
            ),
            Case::ok(
                "sequence advanced once",
                "SELECT currval('owner_id_seq')",
                vec![vec![V::Int(1)]],
                vec![T::BigInt],
            ),
            Case::error("empty CASE", "SELECT CASE END", E::Syntax),
            Case::error("CASE with ELSE only", "SELECT CASE ELSE 1 END", E::Syntax),
            Case::error(
                "simple CASE nextval",
                "SELECT CASE nextval('s') WHEN 1 THEN 1 END",
                E::Unsupported,
            ),
            Case::error(
                "simple CASE nested nextval",
                "SELECT CASE abs(nextval('s')) WHEN 1 THEN 1 END",
                E::Unsupported,
            ),
            Case::error(
                "simple CASE setval",
                "SELECT CASE setval('s',1) WHEN 1 THEN 1 END",
                E::Unsupported,
            ),
            Case::error(
                "simple CASE currval",
                "SELECT CASE currval('s') WHEN 1 THEN 1 END",
                E::Unsupported,
            ),
            Case::error(
                "simple CASE random",
                "SELECT CASE random() WHEN 1 THEN 1 END",
                E::Unsupported,
            ),
            Case::error(
                "simple CASE clock",
                "SELECT CASE clock_timestamp() WHEN CURRENT_TIMESTAMP THEN 1 END",
                E::Unsupported,
            ),
            Case::error(
                "DISTINCT nextval",
                "SELECT nextval('s') IS DISTINCT FROM 1",
                E::Unsupported,
            ),
            Case::error(
                "DISTINCT nested nextval",
                "SELECT 1 IS DISTINCT FROM abs(nextval('s'))",
                E::Unsupported,
            ),
            Case::error(
                "DISTINCT setval",
                "SELECT setval('s',1) IS DISTINCT FROM 1",
                E::Unsupported,
            ),
            Case::error(
                "DISTINCT currval",
                "SELECT currval('s') IS DISTINCT FROM 1",
                E::Unsupported,
            ),
            Case::error(
                "DISTINCT random",
                "SELECT random() IS DISTINCT FROM 1",
                E::Unsupported,
            ),
            Case::error(
                "DISTINCT clock",
                "SELECT clock_timestamp() IS DISTINCT FROM CURRENT_TIMESTAMP",
                E::Unsupported,
            ),
        ],
    );
    // Frozen limits apply before any arm is evaluated: the arms are deliberately
    // unreachable, so a successful evaluation cannot hide a missing syntax cap.
    let arms = format!(
        "SELECT CASE {} ELSE 1 END",
        "WHEN false THEN 0 ".repeat(giggles_core::relational::MAX_COLUMNS + 1)
    );
    assert_eq!(
        session.prepare(&arms).unwrap_err(),
        CoreError::Sql(E::Limit)
    );
    assert_eq!(
        session.execute(&arms, &[], None),
        Err(CoreError::Sql(E::Limit))
    );
    let deep = format!(
        "SELECT {}1{}",
        "CASE WHEN true THEN ".repeat(40),
        " END".repeat(40)
    );
    assert_eq!(
        session.prepare(&deep).unwrap_err(),
        CoreError::Sql(E::Limit)
    );
    assert_eq!(
        session.execute(&deep, &[], None),
        Err(CoreError::Sql(E::Limit))
    );
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn boolean_parentheses_parse_linearly_at_frozen_depth_limits() {
    let path = common::fresh("boolean-parentheses-depth");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for (depth, expression, expected_error) in [
        (31, "1=1", None),
        (31, "x=)", Some(E::Syntax)),
        (33, "1=1", Some(E::Limit)),
    ] {
        let sql = format!(
            "SELECT {}{expression}{}",
            "(".repeat(depth),
            ")".repeat(depth)
        );
        // Time each parse separately. Execution of the prepared statement below
        // does not parse the deeply parenthesized expression a second time.
        let started = std::time::Instant::now();
        let prepared = session.prepare(&sql);
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "depth {depth}, expression {expression:?}: parsing took {elapsed:?}"
        );
        if let Some(error) = expected_error {
            assert_eq!(prepared.unwrap_err(), CoreError::Sql(error), "{sql}");
        } else {
            let result = session
                .execute_prepared_at(&prepared.unwrap(), &[], None, &clock())
                .unwrap();
            assert_eq!(
                result.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
                vec![vec![V::Bool(true)]],
                "{sql}"
            );
        }
    }
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn boolean_check_case_predicates_enforce_postimages_and_reopen() {
    let path = common::fresh("boolean-check-postimages");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE ck(i integer,b boolean,CONSTRAINT c1 CHECK(CASE WHEN i>0 THEN true ELSE false END))",
            &[],
            None,
        )
        .unwrap();
    for sql in [
        "INSERT INTO ck VALUES(1,true)",
        "CREATE TABLE ck_numeric(i integer,CHECK((CASE WHEN i>0 THEN 1 ELSE 0 END)>0))",
        "INSERT INTO ck_numeric VALUES(1)",
        "CREATE TABLE ck_true(b boolean,CHECK(b IS TRUE))",
        "INSERT INTO ck_true VALUES(true)",
        "CREATE TABLE ck_not_false(b boolean,CHECK(b IS NOT FALSE))",
        "INSERT INTO ck_not_false VALUES(true),(NULL)",
        "CREATE TABLE ck_coalesce(i integer,CHECK(coalesce(i>0,false)))",
        "INSERT INTO ck_coalesce VALUES(1)",
    ] {
        session
            .execute(sql, &[], None)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    for sql in [
        "INSERT INTO ck VALUES(0,true)",
        "INSERT INTO ck VALUES(NULL,true)",
        "INSERT INTO ck_numeric VALUES(0)",
        "INSERT INTO ck_numeric VALUES(NULL)",
        "INSERT INTO ck_true VALUES(false)",
        "INSERT INTO ck_true VALUES(NULL)",
        "INSERT INTO ck_not_false VALUES(false)",
        "INSERT INTO ck_coalesce VALUES(0)",
        "INSERT INTO ck_coalesce VALUES(NULL)",
    ] {
        assert_eq!(
            session.execute(sql, &[], None),
            Err(CoreError::Sql(E::CheckViolation)),
            "{sql}"
        );
    }
    drop(session);
    drop(admin);
    // Reopening validates and reparses the stored canonical expressions. The
    // exact PG18 display spellings are frozen by query::constraints unit tests.
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "INSERT INTO ck VALUES(2,false)",
        "INSERT INTO ck_numeric VALUES(2)",
        "INSERT INTO ck_true VALUES(true)",
        "INSERT INTO ck_not_false VALUES(NULL)",
        "INSERT INTO ck_coalesce VALUES(2)",
    ] {
        session
            .execute(sql, &[], None)
            .unwrap_or_else(|error| panic!("after reopen, {sql}: {error:?}"));
    }
    for sql in [
        "INSERT INTO ck VALUES(NULL,false)",
        "INSERT INTO ck_numeric VALUES(0)",
        "INSERT INTO ck_true VALUES(NULL)",
        "INSERT INTO ck_not_false VALUES(false)",
        "INSERT INTO ck_coalesce VALUES(NULL)",
    ] {
        assert_eq!(
            session.execute(sql, &[], None),
            Err(CoreError::Sql(E::CheckViolation)),
            "after reopen, {sql}"
        );
    }
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn boolean_check_immutability_visits_conditions_arms_and_is_operands() {
    let path = common::fresh("boolean-check-immutability");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE owner(id serial PRIMARY KEY)", &[], None)
        .unwrap();
    // All children must be immutable, even when a constant CASE condition makes
    // an arm unreachable. This is the frozen GigglesDB CHECK admission bound.
    for expression in [
        "CASE WHEN i>0 THEN random()>0 ELSE true END",
        "CASE WHEN random()>0 THEN true ELSE true END",
        "CASE WHEN false THEN random()>0 ELSE true END",
        "CASE WHEN true THEN true ELSE random()>0 END",
        "CASE WHEN false THEN now()=now() ELSE true END",
        "CASE WHEN true THEN true ELSE now()=now() END",
        "CASE WHEN false THEN nextval('owner_id_seq')>0 ELSE true END",
        "CASE WHEN true THEN true ELSE nextval('owner_id_seq')>0 END",
        "(random()>0) IS TRUE",
        "(now()=now()) IS NOT FALSE",
        "(nextval('owner_id_seq')>0) IS TRUE",
    ] {
        let sql = format!("CREATE TABLE bad(i integer,CHECK({expression}))");
        assert_eq!(
            session.execute(&sql, &[], None),
            Err(CoreError::Sql(E::Unsupported)),
            "{sql}"
        );
    }
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn boolean_check_deparse_subset_stays_frozen() {
    let path = common::fresh("boolean-check-subset");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for expression in [
        "CASE WHEN i>0 THEN true END",
        "CASE WHEN i>0 THEN CASE WHEN b THEN true ELSE false END ELSE false END",
        "CASE i WHEN 1 THEN true ELSE false END",
        "i IS DISTINCT FROM 1",
        "b IS UNKNOWN",
        "(i>0) IS NULL",
    ] {
        let sql = format!("CREATE TABLE bad(i integer,b boolean,CHECK({expression}))");
        assert_eq!(
            session.execute(&sql, &[], None),
            Err(CoreError::Sql(E::Unsupported)),
            "{sql}"
        );
    }
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
