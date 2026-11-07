//! Frozen acceptance witnesses for bounded, atomic referential actions.
use super::*;

fn run(state: &mut Relational, sql: &str) {
    let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
    state.execute(&prepared, &[], None).unwrap();
}

fn cascade_fixture(children: usize) -> Relational {
    let mut state = Relational::default();
    run(&mut state, "CREATE TABLE p(id INT PRIMARY KEY)");
    run(
        &mut state,
        "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE)",
    );
    run(&mut state, "INSERT INTO p VALUES(1)");
    let table = state.tables["c"].clone();
    // Populate valid rows directly: this witnesses the cascade event cap, not
    // the SQL INSERT batch cap. No secondary indexes need an overlay here.
    for id in 0..children {
        let row = SqlRow {
            values: vec![Value::Int(id as i64), Value::Int(1)],
            version: allocate(&mut state.next_version).unwrap(),
        };
        state
            .rows
            .get_mut(&table.id)
            .unwrap()
            .insert(pk(&table, &row.values).unwrap(), row);
    }
    state.validate().unwrap();
    state
}

#[test]
fn cascade_event_limit_boundary_and_atomic_rollback() {
    // Root plus MAX_ROWS-1 children is admissible. One more child exceeds the
    // aggregate action cap despite each individual child scan fitting its cap.
    let mut allowed = cascade_fixture(MAX_ROWS - 1);
    run(&mut allowed, "DELETE FROM p WHERE id=1");
    assert!(allowed.rows[&allowed.tables["c"].id].is_empty());
    let mut limited = cascade_fixture(MAX_ROWS);
    let before = limited.clone();
    let prepared = Prepared::parse("DELETE FROM p WHERE id=1", 1, limited.epoch).unwrap();
    assert_eq!(limited.execute(&prepared, &[], None), Err(SqlError::Limit));
    assert_eq!(limited, before);
}

#[test]
fn cancellation_after_partial_cascade_rolls_back_every_mutation() {
    let before = cascade_fixture(8);
    let child = before.tables["c"].id;
    let prepared = Prepared::parse("DELETE FROM p WHERE id=1", 1, before.epoch).unwrap();
    let mut after_root = before.clone();
    let mut events = Vec::new();
    let mut root_ticks = 0usize;
    after_root
        .execute_statement_inner(
            &prepared,
            &[],
            None,
            || {
                root_ticks += 1;
                false
            },
            None,
            None,
            &mut events,
        )
        .unwrap();
    // Find an actual cancellation point after some, but not all, child rows
    // have been deleted. Avoid tying this witness to a brittle poll count.
    let mut partial_stop = None;
    for stop in 1..512 {
        let mut candidate = after_root.clone();
        let mut ticks = 0;
        let result = constraints::enforce(
            &mut candidate,
            &before,
            events.clone(),
            &mut DeferralState::default(),
            &mut || {
                ticks += 1;
                ticks == stop
            },
            None,
        );
        let remaining = candidate.rows[&child].len();
        if matches!(result, Err(SqlError::Aborted)) && (1..8).contains(&remaining) {
            partial_stop = Some(root_ticks + stop);
            break;
        }
    }
    let stop = partial_stop.expect("cancellation polls must reach partial child mutations");
    let mut state = before.clone();
    let mut ticks = 0;
    assert_eq!(
        state.execute_with_cancel(&prepared, &[], None, || {
            ticks += 1;
            ticks == stop
        }),
        Err(SqlError::Aborted)
    );
    assert_eq!(state, before);
}
