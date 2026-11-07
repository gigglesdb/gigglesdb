use crate::relational::clock::{ExecutionClock, StatementClock};
use crate::relational::{DataType, Prepared, Relational, SqlError, SqlResult, Value};
use giggles_pg_types::temporal::SessionZone;

fn utc_clock() -> ExecutionClock {
    ExecutionClock::new(&StatementClock {
        unavailable: None,
        transaction_us: 0,
        statement_us: 0,
        zone: Some(SessionZone::Fixed(0)),
        wall: None,
    })
}

fn run(state: &mut Relational, sql: &str) -> SqlResult {
    let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
    state
        .execute_with_cancel(&prepared, &[], None, || false)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn values(state: &mut Relational, sql: &str) -> Vec<Vec<Value>> {
    run(state, sql).rows.into_iter().map(|r| r.values).collect()
}

#[test]
fn differing_numeric_literals_retain_full_filter_instead_of_integer_keys() {
    let mut state = Relational::default();
    run(
        &mut state,
        "CREATE TABLE ki(k integer PRIMARY KEY,v integer)",
    );
    run(&mut state, "INSERT INTO ki VALUES(1,10),(2,20)");
    for (predicate, expected) in [
        ("k=1.5", vec![]),
        ("k<1.5", vec![vec![Value::Int(1)]]),
        ("k>=1.5", vec![vec![Value::Int(2)]]),
        ("k=2.0", vec![vec![Value::Int(2)]]),
    ] {
        assert_eq!(
            values(&mut state, &format!("SELECT k FROM ki WHERE {predicate}")),
            expected,
            "{predicate}"
        );
    }
    assert_eq!(run(&mut state, "UPDATE ki SET v=v WHERE k=1.5").affected, 0);
    assert_eq!(run(&mut state, "UPDATE ki SET v=v WHERE k=2.0").affected, 1);
}

#[test]
fn bigint_and_numeric_key_comparisons_keep_exact_literal_values() {
    let mut state = Relational::default();
    run(&mut state, "CREATE TABLE kb(k bigint PRIMARY KEY)");
    run(&mut state, "INSERT INTO kb VALUES(9007199254740993)");
    assert_eq!(
        values(&mut state, "SELECT k FROM kb WHERE k=9007199254740993.0"),
        vec![vec![Value::Int(9007199254740993)]]
    );
    run(
        &mut state,
        "CREATE TABLE kn(k numeric PRIMARY KEY,v integer)",
    );
    run(&mut state, "INSERT INTO kn VALUES(1.0,10)");
    assert_eq!(
        values(&mut state, "SELECT 1 FROM kn WHERE k=1"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(run(&mut state, "UPDATE kn SET v=v WHERE k=1").affected, 1);
}

#[test]
fn declared_comparison_parameters_keep_their_input_types() {
    let mut state = Relational::default();
    run(
        &mut state,
        "CREATE TABLE mixed(id integer PRIMARY KEY,i integer,d date,ts timestamp,tz timestamptz)",
    );
    run(
        &mut state,
        "INSERT INTO mixed VALUES(1,1,DATE '2024-01-01',TIMESTAMP '2024-01-01 00:00:00',TIMESTAMPTZ '2024-01-01 00:00:00+00')",
    );
    for (predicate, ty, literal, matches) in [
        ("i=$1", DataType::Numeric, "1.5", false),
        ("i<$1", DataType::Numeric, "1.5", true),
        ("i=$1", DataType::DoublePrecision, "'1'::float8", true),
        (
            "d=$1",
            DataType::Timestamp,
            "TIMESTAMP '2024-01-01 00:00:00'",
            true,
        ),
        ("tz=$1", DataType::Date, "DATE '2024-01-01'", true),
        (
            "ts<$1",
            DataType::TimestampTz,
            "TIMESTAMPTZ '2024-01-02 00:00:00+00'",
            true,
        ),
    ] {
        let parameter = values(&mut state, &format!("SELECT {literal}"))[0][0].clone();
        let prepared = Prepared::parse(
            &format!("SELECT id FROM mixed WHERE {predicate}"),
            1,
            state.epoch,
        )
        .unwrap()
        .with_parameter_types(&[Some(ty.clone())])
        .unwrap();
        assert_eq!(prepared.describe(&state).unwrap().parameters, vec![ty]);
        let result = state
            .execute_clocked(&prepared, &[parameter], None, || false, Some(&utc_clock()))
            .unwrap();
        assert_eq!(
            result
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            if matches {
                vec![vec![Value::Int(1)]]
            } else {
                vec![]
            },
            "{predicate}"
        );
    }
}

#[test]
fn membership_and_between_use_cross_type_comparison_binding() {
    let mut state = Relational::default();
    for predicate in [
        "DATE '2024-01-01' IN (TIMESTAMP '2024-01-01 00:00:00')",
        "DATE '2024-01-01' NOT IN (TIMESTAMP '2024-01-02 00:00:00')",
        "DATE '2024-01-01' BETWEEN TIMESTAMP '2023-12-31 00:00:00' AND TIMESTAMP '2024-01-02 00:00:00'",
        "DATE '2024-01-01' NOT BETWEEN TIMESTAMP '2024-01-02 00:00:00' AND TIMESTAMP '2024-01-03 00:00:00'",
    ] {
        assert_eq!(
            values(&mut state, &format!("SELECT 1 WHERE {predicate}")),
            vec![vec![Value::Int(1)]],
            "{predicate}"
        );
    }
}

#[test]
fn cross_type_comparisons_do_not_add_explicit_temporal_casts() {
    let state = Relational::default();
    for sql in [
        "SELECT (DATE '2024-01-01')::timestamptz",
        "SELECT (TIMESTAMP '2024-01-01 00:00:00')::timestamptz",
    ] {
        let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
        assert!(
            matches!(prepared.describe(&state), Err(SqlError::Type)),
            "{sql}"
        );
    }
}
