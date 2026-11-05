mod common;
use common::*;
use giggles_model::*;

#[test]
fn six_rows_require_exact_approval() {
    let (mut m, c) = fixture(6);
    assert_eq!(run(&mut m, &c, predicate(1)), Err(Error::ApprovalRequired));
    let r = predicate(2);
    let a = approved(&mut m, &c, &r);
    assert!(matches!(
        m.execute(&c, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Ok(_))
    ));
    assert!(m.snapshot().approvals[&a].consumed);
    check(&m);
}
#[test]
fn five_rows_need_no_approval() {
    let (mut m, c) = fixture(5);
    assert_eq!(
        run(&mut m, &c, predicate(1)).unwrap().charged_transitions,
        5
    );
    check(&m);
}
#[test]
fn executor_cannot_approve() {
    let (mut m, c) = fixture(6);
    let p = m.preview(&c, &predicate(1), Time::Certified(10)).unwrap();
    assert_eq!(
        m.approve(Principal(2), p, Time::Certified(10)),
        Err(Error::InvalidApproval)
    );
    assert!(m.snapshot().approvals.is_empty());
}
#[test]
fn forged_preview_is_rejected() {
    for mode in 0..5 {
        let (mut m, c) = fixture(6);
        let mut p = m.preview(&c, &predicate(1), Time::Certified(10)).unwrap();
        match mode {
            0 => p.effects.clear(),
            1 => p.dependencies.clear(),
            2 => p.expires += 1,
            3 => p.domain += 1,
            _ => p.schema += 1,
        };
        assert_eq!(
            m.approve(Principal(3), p, Time::Certified(10)),
            Err(Error::StalePreview)
        );
        check(&m);
    }
}
#[test]
fn stale_changed_selected_row_denies_entire_apply() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    m.put(key(0), row(Status::Succeeded, 103)).unwrap();
    let rows = m.snapshot().rows;
    assert!(matches!(
        m.execute(&c, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Err(Error::StalePreview))
    ));
    assert_eq!(m.snapshot().rows, rows);
    assert!(!m.snapshot().approvals[&a].consumed);
    check(&m);
}
#[test]
fn predicate_phantom_invalidates_complete_set() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    m.put(key(8), row(Status::Failed, 103)).unwrap();
    assert_eq!(
        m.execute(&c, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Err(Error::StalePreview))
    );
    check(&m);
}
#[test]
fn unrelated_tenant_write_preserves_approval() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    m.put(
        Key {
            tenant: TenantId(99),
            row: RowId(0),
        },
        row(Status::Failed, 103),
    )
    .unwrap();
    assert!(matches!(
        m.execute(&c, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Ok(_))
    ));
    check(&m);
}
#[test]
fn unrelated_nonmatching_row_preserves_approval() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    m.put(key(9), row(Status::Succeeded, 103)).unwrap();
    assert!(matches!(
        m.execute(&c, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Ok(_))
    ));
    check(&m);
}
#[test]
fn aba_version_invalidates_preview() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    m.put(key(0), row(Status::Succeeded, 103)).unwrap();
    m.put(key(0), row(Status::Failed, 103)).unwrap();
    assert_eq!(
        m.execute(&c, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Err(Error::StalePreview))
    );
    check(&m);
}
#[test]
fn schema_change_invalidates_approval() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    m.bump_schema().unwrap();
    assert_eq!(
        m.execute(&c, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Err(Error::StalePreview))
    );
    check(&m);
}
#[test]
fn revoked_approval_denies() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    m.revoke_approval(a).unwrap();
    assert_eq!(
        m.execute(&c, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Err(Error::InvalidApproval))
    );
    check(&m);
}
#[test]
fn approval_binds_operation_id_and_exact_payload() {
    for mode in 0..2 {
        let (mut m, c) = fixture(6);
        let r = predicate(1);
        let a = approved(&mut m, &c, &r);
        let mut changed = r.clone();
        if mode == 0 {
            changed.operation = OperationId(2)
        } else {
            changed.selection = Selection::Keys((0..6).map(key).collect())
        }
        assert_eq!(
            m.execute(&c, changed, Time::Certified(11), Some(a), false),
            Outcome::Known(Err(Error::InvalidApproval))
        );
        check(&m);
    }
}
#[test]
fn approval_cannot_cross_tasks() {
    let (mut m, c) = fixture(6);
    let d = m.grant(grant(2)).unwrap();
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    assert_eq!(
        m.execute(&d, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Err(Error::InvalidApproval))
    );
    check(&m);
}
#[test]
fn approval_consumption_and_retry_are_atomic() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    let first = m.execute(&c, r.clone(), Time::Certified(11), Some(a), true);
    assert_eq!(first, Outcome::Unknown);
    let before = m.snapshot();
    assert!(matches!(
        m.execute(&c, r, Time::Certified(12), Some(a), false),
        Outcome::Known(Ok(_))
    ));
    assert_eq!(m.snapshot(), before);
    check(&m);
}
#[test]
fn revoked_task_invalidates_approved_work() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    m.revoke(TaskId(1)).unwrap();
    assert_eq!(
        m.execute(&c, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Err(Error::Revoked))
    );
    check(&m);
}
#[test]
fn expiry_invalidates_approved_work() {
    let (mut m, c) = fixture(6);
    let r = predicate(1);
    let a = approved(&mut m, &c, &r);
    assert_eq!(
        m.execute(&c, r, Time::Certified(1000), Some(a), false),
        Outcome::Known(Err(Error::Expired))
    );
    check(&m);
}
#[test]
fn preview_rechecks_remaining_budget_at_commit() {
    let (mut m, c) = fixture(30);
    let r = request(1, &(0..6).collect::<Vec<_>>());
    let a = approved(&mut m, &c, &r);
    for i in 6..26 {
        run(&mut m, &c, request(i, &[i])).unwrap();
    }
    assert_eq!(
        m.execute(&c, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Err(Error::Budget))
    );
    assert!(!m.snapshot().approvals[&a].consumed);
    check(&m);
}
#[test]
fn key_preview_does_not_claim_complete_predicate_set() {
    let (mut m, c) = fixture(6);
    let r = request(1, &(0..6).collect::<Vec<_>>());
    let a = approved(&mut m, &c, &r);
    m.put(key(7), row(Status::Failed, 103)).unwrap();
    assert!(matches!(
        m.execute(&c, r, Time::Certified(11), Some(a), false),
        Outcome::Known(Ok(_))
    ));
    assert_eq!(m.snapshot().rows[&key(7)].status, Status::Failed);
    check(&m);
}
