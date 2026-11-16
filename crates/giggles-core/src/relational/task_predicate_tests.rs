//! R3-104 task predicate facade: canonical text, validation and evaluation.
use super::task_predicate::{canonical, columns, eval};
use super::*;

fn run(state: &mut Relational, sql: &str) {
    let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
    state.execute(&prepared, &[], None).unwrap();
}

fn fixture() -> Relational {
    let mut state = Relational::default();
    run(
        &mut state,
        "CREATE TABLE payments(id INT PRIMARY KEY, merchant_id INT, status TEXT, \
         failed_at TIMESTAMPTZ, local_at TIMESTAMP, \"Note\" TEXT)",
    );
    run(
        &mut state,
        "INSERT INTO payments VALUES \
         (1, 7, 'failed', '2026-11-05 18:00:00+00', '2026-11-05 18:00:00', 'a'), \
         (2, 7, 'paid', '2026-11-05 16:00:00+00', '2026-11-05 16:00:00', 'b'), \
         (3, 8, NULL, NULL, NULL, NULL)",
    );
    state
}

fn table(state: &Relational) -> TableDef {
    state.tables["payments"].clone()
}

fn row(state: &Relational, id: i64) -> SqlRow {
    let table = table(state);
    state.rows[&table.id]
        .values()
        .find(|row| row.values[0] == Value::Int(id))
        .unwrap()
        .clone()
}

#[test]
fn canonical_is_idempotent() {
    let state = fixture();
    let table = table(&state);
    for sql in [
        "merchant_id = 7",
        "MERCHANT_ID=7 AND (Status <> 'it''s' OR \"Note\" IS NULL)",
        "CASE WHEN merchant_id > 7 THEN true ELSE status = $$x$$ END",
        "merchant_id::bigint + 1 >= 8 -- trailing comment",
        "local_at >= '2026-11-05 17:00:00'",
    ] {
        let once = canonical(&state, &table, sql).unwrap();
        assert_eq!(canonical(&state, &table, &once).unwrap(), once, "{sql}");
    }
}

#[test]
fn canonical_lowercases_and_requotes() {
    let state = fixture();
    let table = table(&state);
    assert_eq!(
        canonical(
            &state,
            &table,
            "MERCHANT_ID=7 AND \"status\"='it''s'  AND \"Note\" IS NULL"
        )
        .unwrap(),
        "merchant_id = 7 and status = 'it''s' and \"Note\" is null"
    );
    assert_eq!(
        canonical(&state, &table, "Status = $$o'k$$").unwrap(),
        "status = 'o''k'"
    );
}

#[test]
fn rejects_parameter() {
    let state = fixture();
    let table = table(&state);
    assert_eq!(
        canonical(&state, &table, "merchant_id = $1"),
        Err(SqlError::Unsupported)
    );
}

#[test]
fn rejects_subquery_and_aggregate() {
    let state = fixture();
    let table = table(&state);
    for sql in [
        "merchant_id IN (SELECT merchant_id FROM payments)",
        "EXISTS (SELECT 1 FROM payments)",
        "count(*) > 0",
        "max(merchant_id) > 0",
    ] {
        assert_eq!(
            canonical(&state, &table, sql),
            Err(SqlError::Unsupported),
            "{sql}"
        );
    }
}

#[test]
fn rejects_now_and_nextval() {
    let mut state = fixture();
    run(&mut state, "CREATE SEQUENCE s");
    let table = table(&state);
    for sql in [
        "failed_at < now()",
        "local_at < localtimestamp",
        "merchant_id < nextval('s')",
        "random() < 0.5",
    ] {
        assert_eq!(
            canonical(&state, &table, sql),
            Err(SqlError::Unsupported),
            "{sql}"
        );
    }
}

#[test]
fn rejects_qualified_column() {
    let state = fixture();
    let table = table(&state);
    for sql in ["payments.merchant_id = 7", "public.payments.merchant_id = 7"] {
        assert_eq!(
            canonical(&state, &table, sql),
            Err(SqlError::Unsupported),
            "{sql}"
        );
    }
}

#[test]
fn columns_lists_merchant_id_status_failed_at() {
    let state = fixture();
    let table = table(&state);
    let text = canonical(
        &state,
        &table,
        "merchant_id = 7 AND status = 'failed' AND failed_at IS NOT NULL",
    )
    .unwrap();
    assert_eq!(
        columns(&state, &table, &text).unwrap(),
        BTreeSet::from(["failed_at".into(), "merchant_id".into(), "status".into()])
    );
    let quoted = canonical(&state, &table, "\"Note\" = 'a'").unwrap();
    assert_eq!(
        columns(&state, &table, &quoted).unwrap(),
        BTreeSet::from(["Note".into()])
    );
}

#[test]
fn eval_null_is_not_true() {
    let state = fixture();
    let table = table(&state);
    let text = canonical(&state, &table, "status = 'failed'").unwrap();
    assert_eq!(eval(&state, &table, &text, &row(&state, 3)), Ok(None));
    assert_eq!(eval(&state, &table, &text, &row(&state, 1)), Ok(Some(true)));
}

#[test]
fn eval_matches_check_semantics_for_true_and_false() {
    let state = fixture();
    let table = table(&state);
    for sql in [
        "merchant_id = 7 AND status = 'failed'",
        "merchant_id > 7 OR status <> 'paid'",
        "CASE WHEN merchant_id = 8 THEN true ELSE status = 'paid' END",
    ] {
        let text = canonical(&state, &table, sql).unwrap();
        for id in 1..=3 {
            let row = row(&state, id);
            let result = eval(&state, &table, &text, &row).unwrap();
            let check = query::constraint_check(&state, &table, &text, &row);
            match result {
                Some(false) => assert_eq!(check, Err(SqlError::CheckViolation), "{sql} {id}"),
                Some(true) | None => assert_eq!(check, Ok(()), "{sql} {id}"),
            }
        }
    }
    let text = canonical(&state, &table, "merchant_id = 7").unwrap();
    assert_eq!(eval(&state, &table, &text, &row(&state, 1)), Ok(Some(true)));
    assert_eq!(eval(&state, &table, &text, &row(&state, 3)), Ok(Some(false)));
}

/// Probe: a `timestamptz` window against an offset-bearing literal.
#[test]
fn timestamptz_window_is_deterministic() {
    let state = fixture();
    let table = table(&state);
    let text = canonical(&state, &table, "failed_at >= '2026-11-05 17:00:00+00'").unwrap();
    assert_eq!(text, "failed_at >= '2026-11-05 17:00:00+00'");
    assert_eq!(eval(&state, &table, &text, &row(&state, 1)), Ok(Some(true)));
    assert_eq!(eval(&state, &table, &text, &row(&state, 2)), Ok(Some(false)));
    assert_eq!(eval(&state, &table, &text, &row(&state, 3)), Ok(None));
}

/// Probe: the same window on a `timestamp` (without time zone) column.
#[test]
fn timestamp_window_is_deterministic() {
    let state = fixture();
    let table = table(&state);
    let text = canonical(&state, &table, "local_at >= '2026-11-05 17:00:00'").unwrap();
    assert_eq!(text, "local_at >= '2026-11-05 17:00:00'");
    assert_eq!(eval(&state, &table, &text, &row(&state, 1)), Ok(Some(true)));
    assert_eq!(eval(&state, &table, &text, &row(&state, 2)), Ok(Some(false)));
    assert_eq!(eval(&state, &table, &text, &row(&state, 3)), Ok(None));
}

/// TimeZone-dependent timestamptz forms stay out: no offset, cross-type compare, date cast.
#[test]
fn timestamptz_rejects_timezone_dependent_forms() {
    let state = fixture();
    let table = table(&state);
    for sql in [
        "failed_at >= '2026-11-05 17:00:00'",
        "failed_at >= local_at",
        "failed_at::date = '2026-11-05'",
    ] {
        assert_eq!(
            canonical(&state, &table, sql),
            Err(SqlError::Unsupported),
            "{sql}"
        );
    }
    assert_eq!(
        canonical(&state, &table, "failed_at >= '2026-11-05 17:00:00+00'::timestamptz"),
        Ok("failed_at >= '2026-11-05 17:00:00+00' : : timestamptz".into())
    );
}
