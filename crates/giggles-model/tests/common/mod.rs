#![allow(dead_code)]
use giggles_model::*;
use std::collections::BTreeSet;
pub fn key(n: u64) -> Key {
    Key {
        tenant: TenantId(42),
        row: RowId(n),
    }
}
pub fn row(status: Status, failed_at: u64) -> Row {
    Row {
        status,
        failed_at,
        attempt_count: 0,
        version: 999,
    }
}
pub fn grant(task: u64) -> Grant {
    Grant {
        task: TaskId(task),
        issuer: Principal(1),
        executor: Principal(2),
        approver: Principal(3),
        policy: Policy {
            tenant: TenantId(42),
            columns: BTreeSet::from([Column::Status]),
            transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
            incident: (100, 107),
            distinct_limit: 25,
            transition_limit: 25,
            approval_above: 5,
            valid_from: 1,
            expires: 1000,
        },
    }
}
pub fn fixture(n: u64) -> (Model, Credential) {
    let mut m = Model::new(7);
    for i in 0..n {
        m.put(key(i), row(Status::Failed, 103)).unwrap();
    }
    let c = m.grant(grant(1)).unwrap();
    (m, c)
}
pub fn request(op: u64, ids: &[u64]) -> Request {
    Request {
        operation: OperationId(op),
        selection: Selection::Keys(ids.iter().map(|i| key(*i)).collect()),
        column: Column::Status,
        after: Status::Queued,
    }
}
pub fn predicate(op: u64) -> Request {
    Request {
        operation: OperationId(op),
        selection: Selection::Predicate {
            tenant: TenantId(42),
            status: Status::Failed,
            from: 100,
            until: 107,
        },
        column: Column::Status,
        after: Status::Queued,
    }
}
pub fn run(m: &mut Model, c: &Credential, r: Request) -> Decision {
    match m.execute(c, r, Time::Certified(10), None, false) {
        Outcome::Known(d) => d,
        _ => panic!("reply"),
    }
}
pub fn approved(m: &mut Model, c: &Credential, r: &Request) -> ApprovalId {
    let p = m.preview(c, r, Time::Certified(10)).unwrap();
    m.approve(Principal(3), p, Time::Certified(10)).unwrap()
}
pub fn check(m: &Model) {
    giggles_model::checker::verify(7, m.history(), &m.snapshot()).unwrap()
}
