mod common;
use common::*;
use giggles_model::*;

#[test]
fn permitted_repair_has_exact_receipt() {
    let (mut m, c) = fixture(3);
    let r = run(&mut m, &c, request(1, &[2, 0])).unwrap();
    assert_eq!(
        r.effects.iter().map(|e| e.key).collect::<Vec<_>>(),
        vec![key(0), key(2)]
    );
    assert_eq!(r.charged_transitions, 2);
    assert_eq!(r.lifetime_distinct, 2);
    assert_eq!(m.snapshot().rows[&key(1)].status, Status::Failed);
    check(&m);
}
#[test]
fn overbroad_incident_rejects_entire_statement() {
    let (mut m, c) = fixture(2);
    m.put(key(1), row(Status::Failed, 99)).unwrap();
    let before = m.snapshot();
    assert_eq!(run(&mut m, &c, request(1, &[0, 1])), Err(Error::Scope));
    assert_eq!(m.snapshot().rows, before.rows);
    assert_eq!(m.snapshot().tasks, before.tasks);
    check(&m);
}
#[test]
fn successful_delivery_is_untouched_and_causes_atomic_denial() {
    let (mut m, c) = fixture(2);
    m.put(key(1), row(Status::Succeeded, 103)).unwrap();
    assert_eq!(run(&mut m, &c, request(1, &[0, 1])), Err(Error::Scope));
    assert_eq!(m.snapshot().rows[&key(0)].status, Status::Failed);
    check(&m);
}
#[test]
fn tenant_boundary_rejects_mixed_keys() {
    let (mut m, c) = fixture(1);
    let other = Key {
        tenant: TenantId(43),
        row: RowId(0),
    };
    m.put(other, row(Status::Failed, 103)).unwrap();
    let mut r = request(1, &[0]);
    r.selection = Selection::Keys(vec![key(0), other]);
    assert_eq!(run(&mut m, &c, r), Err(Error::Scope));
    assert!(
        m.snapshot()
            .rows
            .values()
            .all(|r| r.status == Status::Failed)
    );
    check(&m);
}
#[test]
fn forbidden_columns_reject_without_mutation() {
    for column in [Column::AttemptCount, Column::FailedAt] {
        let (mut m, c) = fixture(1);
        let mut r = request(1, &[0]);
        r.column = column;
        assert_eq!(run(&mut m, &c, r), Err(Error::Scope));
        check(&m);
    }
}
#[test]
fn incident_interval_is_half_open() {
    for (at, allowed) in [(99, false), (100, true), (106, true), (107, false)] {
        let (mut m, c) = fixture(1);
        m.put(key(0), row(Status::Failed, at)).unwrap();
        assert_eq!(run(&mut m, &c, request(1, &[0])).is_ok(), allowed);
        check(&m);
    }
}
#[test]
fn duplicate_identity_is_rejected() {
    let (mut m, c) = fixture(1);
    assert_eq!(
        run(&mut m, &c, request(1, &[0, 0])),
        Err(Error::DuplicateKey)
    );
    check(&m);
}
#[test]
fn missing_identity_rejects_all() {
    let (mut m, c) = fixture(1);
    assert_eq!(run(&mut m, &c, request(1, &[0, 9])), Err(Error::MissingRow));
    assert_eq!(m.snapshot().rows[&key(0)].status, Status::Failed);
    check(&m);
}
#[test]
fn zero_effect_has_receipt_but_no_charge() {
    let (mut m, c) = fixture(1);
    m.put(key(0), row(Status::Queued, 103)).unwrap();
    let r = run(&mut m, &c, request(1, &[0])).unwrap();
    assert!(r.effects.is_empty());
    assert_eq!(r.lifetime_transitions, 0);
    check(&m);
}
#[test]
fn empty_set_commits_zero_effects() {
    let (mut m, c) = fixture(0);
    assert_eq!(
        run(&mut m, &c, request(1, &[]))
            .unwrap()
            .charged_transitions,
        0
    );
    check(&m);
}
#[test]
fn twenty_six_changes_fail_even_with_approval() {
    let (mut m, c) = fixture(26);
    let r = predicate(1);
    assert_eq!(m.preview(&c, &r, Time::Certified(10)), Err(Error::Budget));
    assert_eq!(run(&mut m, &c, r), Err(Error::Budget));
    assert!(
        m.snapshot()
            .rows
            .values()
            .all(|r| r.status == Status::Failed)
    );
    check(&m);
}
#[test]
fn split_requests_share_lifetime_budget() {
    let (mut m, c) = fixture(30);
    for i in 0..5 {
        run(
            &mut m,
            &c,
            request(i, &(i * 5..i * 5 + 5).collect::<Vec<_>>()),
        )
        .unwrap();
    }
    assert_eq!(run(&mut m, &c, request(5, &[25])), Err(Error::Budget));
    assert_eq!(m.snapshot().tasks[&TaskId(1)].transitions, 25);
    check(&m);
}
#[test]
fn ten_serial_contenders_for_last_slot() {
    let (mut m, c) = fixture(34);
    for i in 0..24 {
        run(&mut m, &c, request(i, &[i])).unwrap();
    }
    let results = (24..34)
        .map(|i| run(&mut m, &c, request(i, &[i])).is_ok())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|v| **v).count(), 1);
    assert_eq!(m.snapshot().tasks[&TaskId(1)].transitions, 25);
    check(&m);
}
#[test]
fn repeated_changes_spend_transition_budget_again() {
    let (mut m, c) = fixture(1);
    for i in 0..25 {
        if i > 0 {
            m.put(key(0), row(Status::Failed, 103)).unwrap();
        }
        run(&mut m, &c, request(i, &[0])).unwrap();
    }
    m.put(key(0), row(Status::Failed, 103)).unwrap();
    assert_eq!(run(&mut m, &c, request(25, &[0])), Err(Error::Budget));
    let t = &m.snapshot().tasks[&TaskId(1)];
    assert_eq!(t.touched.len(), 1);
    assert_eq!(t.transitions, 25);
    check(&m);
}
#[test]
fn distinct_cap_is_independent_of_transition_cap() {
    let mut m = Model::new(7);
    for i in 0..2 {
        m.put(key(i), row(Status::Failed, 103)).unwrap();
    }
    let mut g = grant(1);
    g.policy.distinct_limit = 1;
    g.policy.transition_limit = 50;
    let c = m.grant(g).unwrap();
    run(&mut m, &c, request(1, &[0])).unwrap();
    m.put(key(0), row(Status::Failed, 103)).unwrap();
    run(&mut m, &c, request(2, &[0])).unwrap();
    assert_eq!(run(&mut m, &c, request(3, &[1])), Err(Error::Budget));
    check(&m);
}
#[test]
fn exact_retry_deduplicates_and_key_order_is_canonical() {
    let (mut m, c) = fixture(2);
    let first = run(&mut m, &c, request(1, &[0, 1])).unwrap();
    let before = m.snapshot();
    assert_eq!(run(&mut m, &c, request(1, &[1, 0])), Ok(first));
    assert_eq!(m.snapshot(), before);
    check(&m);
}
#[test]
fn altered_payload_id_reuse_is_conflict() {
    let (mut m, c) = fixture(2);
    run(&mut m, &c, request(1, &[0])).unwrap();
    let before = m.snapshot();
    assert_eq!(run(&mut m, &c, request(1, &[1])), Err(Error::IdConflict));
    assert_eq!(m.snapshot(), before);
    check(&m);
}
#[test]
fn rejected_id_stays_bound_after_repairing_inputs() {
    let (mut m, c) = fixture(1);
    let r = request(1, &[1]);
    assert_eq!(run(&mut m, &c, r.clone()), Err(Error::MissingRow));
    m.put(key(1), row(Status::Failed, 103)).unwrap();
    assert_eq!(run(&mut m, &c, r), Err(Error::MissingRow));
    assert!(run(&mut m, &c, request(2, &[1])).is_ok());
    check(&m);
}
#[test]
fn lost_reply_resolves_and_retry_has_one_logical_receipt() {
    let (mut m, c) = fixture(1);
    let r = request(1, &[0]);
    assert_eq!(
        m.execute(&c, r.clone(), Time::Certified(10), None, true),
        Outcome::Unknown
    );
    let OperationStatus::Decided(Ok(receipt)) = m.status(&c, OperationId(1)).unwrap() else {
        panic!()
    };
    assert_eq!(run(&mut m, &c, r), Ok(receipt));
    assert_eq!(m.snapshot().operations.len(), 1);
    check(&m);
}
#[test]
fn lost_denial_reply_is_resolvable() {
    let (mut m, c) = fixture(0);
    assert_eq!(
        m.execute(&c, request(1, &[0]), Time::Certified(10), None, true),
        Outcome::Unknown
    );
    assert_eq!(
        m.status(&c, OperationId(1)),
        Ok(OperationStatus::Decided(Err(Error::MissingRow)))
    );
    check(&m);
}
#[test]
fn absent_status_does_not_invent_abort() {
    let (m, c) = fixture(0);
    assert_eq!(m.status(&c, OperationId(999)), Ok(OperationStatus::Absent));
}
#[test]
fn replay_after_revoke_returns_original_but_new_op_fails() {
    let (mut m, c) = fixture(2);
    let r = run(&mut m, &c, request(1, &[0])).unwrap();
    m.revoke(TaskId(1)).unwrap();
    assert_eq!(run(&mut m, &c, request(1, &[0])), Ok(r));
    assert_eq!(run(&mut m, &c, request(2, &[1])), Err(Error::Revoked));
    assert!(matches!(
        m.status(&c, OperationId(1)),
        Ok(OperationStatus::Decided(Ok(_)))
    ));
    check(&m);
}
#[test]
fn expiry_is_exclusive_and_not_host_time() {
    for now in [0, 1, 999, 1000] {
        let (mut m, c) = fixture(1);
        let result = m.execute(&c, request(1, &[0]), Time::Certified(now), None, false);
        assert_eq!(
            matches!(result, Outcome::Known(Ok(_))),
            (1..1000).contains(&now)
        );
        check(&m);
    }
}
#[test]
fn uncertain_time_fails_closed() {
    let (mut m, c) = fixture(1);
    assert_eq!(
        m.execute(&c, request(1, &[0]), Time::Uncertain, None, false),
        Outcome::Known(Err(Error::UncertainTime))
    );
    check(&m);
}
#[test]
fn regressed_time_denies_new_decision() {
    let (mut m, c) = fixture(2);
    run(&mut m, &c, request(1, &[0])).unwrap();
    assert_eq!(
        m.execute(&c, request(2, &[1]), Time::Certified(9), None, false),
        Outcome::Known(Err(Error::TimeRegression))
    );
    check(&m);
}
#[test]
fn generation_binding_denies_cloned_credential() {
    let (_, c) = fixture(1);
    let mut other = Model::new(8);
    other.grant(grant(1)).unwrap();
    assert_eq!(
        run(&mut other, &c, request(1, &[])),
        Err(Error::Authentication)
    );
    assert!(other.snapshot().operations.is_empty());
    assert_eq!(other.status(&c, OperationId(1)), Err(Error::Authentication));
}
#[test]
fn operation_ids_are_task_scoped() {
    let (mut m, c) = fixture(2);
    let d = m.grant(grant(2)).unwrap();
    run(&mut m, &c, request(1, &[0])).unwrap();
    run(&mut m, &d, request(1, &[1])).unwrap();
    assert_eq!(m.snapshot().operations.len(), 2);
    check(&m);
}
#[test]
fn grant_replacement_cannot_reset_budget() {
    let (mut m, c) = fixture(1);
    run(&mut m, &c, request(1, &[0])).unwrap();
    assert_eq!(m.grant(grant(1)), Err(Error::TaskExists));
    check(&m);
}
#[test]
fn invalid_grants_are_not_issued() {
    for mode in 0..5 {
        let mut m = Model::new(7);
        let mut g = grant(1);
        match mode {
            0 => g.approver = g.executor,
            1 => g.issuer = g.executor,
            2 => g.policy.incident = (9, 9),
            3 => g.policy.expires = g.policy.valid_from,
            _ => g.policy.columns.clear(),
        };
        assert_eq!(m.grant(g), Err(Error::InvalidGrant));
        assert!(m.snapshot().tasks.is_empty());
    }
}
#[test]
fn predicate_tenant_and_bounds_are_checked() {
    for mode in 0..2 {
        let (mut m, c) = fixture(1);
        let mut r = predicate(1);
        if let Selection::Predicate { tenant, until, .. } = &mut r.selection {
            if mode == 0 {
                *tenant = TenantId(99)
            } else {
                *until = 100
            }
        }
        assert_eq!(
            run(&mut m, &c, r),
            Err(if mode == 0 {
                Error::Scope
            } else {
                Error::InvalidPredicate
            })
        );
        check(&m);
    }
}
#[test]
fn canonical_payload_distinguishes_selection_column_and_value() {
    let r = request(1, &[0, 1]);
    let mut variants = vec![r.clone()];
    let mut v = r.clone();
    v.column = Column::FailedAt;
    variants.push(v);
    let mut v = r.clone();
    v.after = Status::Succeeded;
    variants.push(v);
    variants.push(request(1, &[0, 0, 1]));
    variants.push(predicate(1));
    let codes: std::collections::BTreeSet<_> = variants.iter().map(Request::canonical).collect();
    assert_eq!(codes.len(), variants.len());
}
