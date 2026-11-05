mod common;
use common::*;
use giggles_model::*;
fn rejected(m: &Model, events: &[Event]) {
    assert!(giggles_model::checker::verify(7, events, &m.snapshot()).is_err());
}
fn rejected_at_event(m: &Model, events: &[Event]) {
    let error = giggles_model::checker::verify(7, events, &m.snapshot()).unwrap_err();
    assert!(
        error.starts_with("event "),
        "validator only detected final mismatch: {error}"
    );
}
#[test]
fn checker_rejects_receipt_and_effect_tampering() {
    for mode in 0..11 {
        let (mut m, c) = fixture(2);
        run(&mut m, &c, request(1, &[0, 1])).unwrap();
        let mut events = m.history().to_vec();
        let Event::Decision { result: Ok(r), .. } = events.last_mut().unwrap() else {
            panic!()
        };
        match mode {
            0 => r.charged_transitions += 1,
            1 => r.lifetime_distinct += 1,
            2 => r.lifetime_transitions += 1,
            3 => r.effects[0].after.attempt_count += 1,
            4 => r.effects[0].before.status = Status::Queued,
            5 => {
                r.effects.pop();
            }
            6 => r.effects[0].key.tenant = TenantId(99),
            7 => r.executor = Principal(999),
            8 => r.payload.push(0),
            9 => r.schema += 1,
            _ => r.decided_at += 1,
        };
        rejected_at_event(&m, &events);
    }
}
#[test]
fn checker_rejects_missing_and_duplicate_logical_decisions() {
    let (mut m, c) = fixture(1);
    run(&mut m, &c, request(1, &[0])).unwrap();
    let mut events = m.history().to_vec();
    let e = events.pop().unwrap();
    rejected(&m, &events);
    events.push(e.clone());
    events.push(e);
    rejected(&m, &events);
}
#[test]
fn checker_rejects_changed_final_snapshot() {
    let (mut m, c) = fixture(1);
    run(&mut m, &c, request(1, &[0])).unwrap();
    let mut s = m.snapshot();
    s.tasks.get_mut(&TaskId(1)).unwrap().transitions = 0;
    assert!(giggles_model::checker::verify(7, m.history(), &s).is_err());
}
#[test]
fn checker_rejects_approval_lifecycle_tampering() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    m.execute(&c, r, Time::Certified(11), Some(a), false);
    for mode in 0..4 {
        let mut events = m.history().to_vec();
        for e in &mut events {
            if let Event::Approve(a) = e {
                match mode {
                    0 => a.revoked = true,
                    1 => a.consumed = true,
                    2 => a.approver = Principal(2),
                    _ => a.preview.dependencies.clear(),
                }
            }
        }
        rejected(&m, &events);
    }
}
#[test]
fn checker_detects_forged_success_after_revoke() {
    let (mut m, c) = fixture(1);
    run(&mut m, &c, request(1, &[0])).unwrap();
    let mut events = m.history().to_vec();
    events.insert(events.len() - 1, Event::Revoke(TaskId(1)));
    rejected(&m, &events);
}
#[test]
fn checker_detects_forged_success_after_expiry() {
    let (mut m, c) = fixture(1);
    run(&mut m, &c, request(1, &[0])).unwrap();
    let mut events = m.history().to_vec();
    if let Event::Decision {
        time,
        result: Ok(r),
        ..
    } = events.last_mut().unwrap()
    {
        *time = Time::Certified(1000);
        r.decided_at = 1000;
    }
    rejected(&m, &events);
}
#[test]
fn checker_detects_wrong_database_generation() {
    let (mut m, c) = fixture(1);
    run(&mut m, &c, request(1, &[0])).unwrap();
    assert!(giggles_model::checker::verify(8, m.history(), &m.snapshot()).is_err());
}
#[test]
fn checker_detects_mutated_approval_dependencies_at_apply() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    m.execute(&c, r, Time::Certified(11), Some(a), false);
    let mut events = m.history().to_vec();
    events.insert(
        events.len() - 1,
        Event::Put(
            key(9),
            Row {
                status: Status::Failed,
                failed_at: 103,
                attempt_count: 0,
                version: 0,
            },
        ),
    );
    rejected_at_event(&m, &events);
}

#[test]
fn checker_rejects_forged_history_even_when_snapshot_matches_forgery() {
    let (mut m, c) = fixture(1);
    run(&mut m, &c, request(1, &[0])).unwrap();
    let mut events = m.history().to_vec();
    let Event::Decision { result: Ok(r), .. } = events.last_mut().unwrap() else {
        panic!()
    };
    r.effects[0].after.attempt_count = 123;
    let forged = r.clone();
    let mut state = m.snapshot();
    state.rows.get_mut(&key(0)).unwrap().attempt_count = 123;
    state
        .operations
        .get_mut(&(TaskId(1), OperationId(1)))
        .unwrap()
        .decision = Ok(forged);
    let error = giggles_model::checker::verify(7, &events, &state).unwrap_err();
    assert!(error.contains("after mismatch"), "{error}");
}
