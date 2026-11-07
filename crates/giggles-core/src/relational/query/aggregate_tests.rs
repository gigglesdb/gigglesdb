use super::*;
fn state() -> Relational {
    let mut state = Relational::default();
    for sql in [
        "CREATE TABLE agg (id bigint primary key, amount integer)",
        "INSERT INTO agg VALUES (1,-2)",
        "INSERT INTO agg VALUES (2,3)",
    ] {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        state.execute_with_cancel(&p, &[], None, || false).unwrap();
    }
    state
}
#[test]
fn explicit_callback_preserves_pure_execution_and_has_no_effects() {
    let mut state = state();
    let before = bincode::serialize(&state).unwrap();
    let p = Prepared::parse("SELECT sum(abs(amount)) FROM agg", 1, state.epoch).unwrap();
    let cancelled = crate::Cancellation::default();
    cancelled.cancel();
    let r = cancelled
        .scope(|| state.execute_with_cancel(&p, &[], None, || false))
        .unwrap();
    assert_eq!(r.rows[0].values, vec![Value::Int(5)]);
    let e = state
        .execute_with_cancel(&p, &[], None, || true)
        .unwrap_err();
    assert_eq!(e, SqlError::Aborted);
    assert_eq!(bincode::serialize(&state).unwrap(), before);
}
#[test]
fn aggregate_cancellation_entry_empty_limit_and_final_fences() {
    let mut state = state();
    for sql in [
        "SELECT count(*) FROM agg WHERE FALSE",
        "SELECT sum(abs(amount)) FROM agg LIMIT 0",
    ] {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        assert_eq!(
            state
                .execute_with_cancel(&p, &[], None, || true)
                .unwrap_err(),
            SqlError::Aborted
        );
    }
    let p = Prepared::parse("SELECT sum(abs($1::integer)) FROM agg", 1, state.epoch).unwrap();
    // The aggregate entry fence precedes BoundQuery parameter type validation.
    assert_eq!(
        state
            .execute_with_cancel(&p, &[Value::Text("bad".into())], None, || true)
            .unwrap_err(),
        SqlError::Aborted
    );
    assert_eq!(
        state
            .execute_with_cancel(&p, &[Value::Text("bad".into())], None, || false)
            .unwrap_err(),
        SqlError::Type
    );
    assert_eq!(
        state
            .execute_with_cancel(&p, &[Value::Int(i32::MIN.into())], None, || false)
            .unwrap_err(),
        SqlError::Overflow
    );
    let p = Prepared::parse("SELECT count(*) FROM agg", 1, state.epoch).unwrap();
    let mut calls = 0;
    let e = state
        .execute_with_cancel(&p, &[], None, || {
            calls += 1;
            calls == 4
        })
        .unwrap_err();
    assert_eq!(e, SqlError::Aborted);
    assert_eq!(calls, 4);
}

#[test]
fn large_join_cancel_lands_mid_scan() {
    let mut state = large_join_state();
    let sql = "SELECT ascii(a.label),initcap(d.label),chr(a.code) FROM adv_work a JOIN adv_work b ON b.id=a.id JOIN adv_work c ON c.id=b.id JOIN adv_work d ON d.id=c.id JOIN adv_work e ON e.id=d.id";
    let p = Prepared::parse(sql, 1, state.epoch).unwrap();
    let params = [];
    let mut calls = 0;
    let result = state.execute_with_cancel(&p, &params, None, || {
        calls += 1;
        calls == 3
    });
    assert_eq!(result, Err(SqlError::Aborted));
    assert_eq!(calls, 3);
}

#[test]
fn large_join_count_cancel_lands_within_first_rows() {
    let mut state = large_join_state();
    let sql = "SELECT count(*) FROM adv_work a JOIN adv_work b ON b.id=a.id JOIN adv_work c ON c.id=b.id JOIN adv_work d ON d.id=c.id JOIN adv_work e ON e.id=d.id";
    let p = Prepared::parse(sql, 1, state.epoch).unwrap();
    let params = [];
    let mut calls = 0;
    let result = state.execute_with_cancel(&p, &params, None, || {
        calls += 1;
        calls == 3
    });
    assert_eq!(result, Err(SqlError::Aborted));
    assert_eq!(calls, 3);
}

fn large_join_state() -> Relational {
    let mut state = Relational::default();
    let p = Prepared::parse(
        "CREATE TABLE adv_work (id INT PRIMARY KEY,label TEXT,code INT)",
        1,
        state.epoch,
    )
    .unwrap();
    state.execute_with_cancel(&p, &[], None, || false).unwrap();
    let p = Prepared::parse("INSERT INTO adv_work VALUES($1,$2,$3)", 1, state.epoch).unwrap();
    for id in 0..1024 {
        state
            .execute_with_cancel(
                &p,
                &[Value::Int(id), Value::Text("abc".into()), Value::Int(65)],
                None,
                || false,
            )
            .unwrap();
    }
    state
}
