//! Deterministic typed histories for downstream differential replay.
use crate::*;
use std::collections::BTreeSet;
#[derive(Clone, Debug)]
pub struct GoldenHistory {
    pub name: &'static str,
    pub domain: u128,
    pub events: Vec<Event>,
    pub final_state: Snapshot,
    pub transcript: String,
}
fn key(id: u64) -> Key {
    Key {
        tenant: TenantId(42),
        row: RowId(id),
    }
}
fn row(status: Status, at: u64) -> Row {
    Row {
        status,
        failed_at: at,
        attempt_count: 0,
        version: 0,
    }
}
fn setup(n: u64) -> (Model, Credential) {
    let mut m = Model::new(7);
    for id in 0..n {
        m.put(key(id), row(Status::Failed, 103)).unwrap();
    }
    let c = m
        .grant(Grant {
            task: TaskId(1),
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
        })
        .unwrap();
    (m, c)
}
fn req(op: u64, keys: Vec<Key>) -> Request {
    Request {
        operation: OperationId(op),
        selection: Selection::Keys(keys),
        column: Column::Status,
        after: Status::Queued,
    }
}
fn finish(name: &'static str, m: Model, transcript: String) -> GoldenHistory {
    GoldenHistory {
        name,
        domain: 7,
        events: m.history().to_vec(),
        final_state: m.snapshot(),
        transcript,
    }
}
fn result(out: Outcome) -> String {
    match out {
        Outcome::Unknown => "unknown; resolve original operation ID".into(),
        Outcome::Known(Err(e)) => format!("rejected {e:?}"),
        Outcome::Known(Ok(r)) => format!(
            "committed effects={} lifetime_transitions={} lifetime_distinct={}",
            r.charged_transitions, r.lifetime_transitions, r.lifetime_distinct
        ),
    }
}
pub fn golden_histories() -> Vec<GoldenHistory> {
    let mut histories = vec![];
    let (mut m, c) = setup(3);
    let line = result(m.execute(
        &c,
        req(1, vec![key(0), key(2)]),
        Time::Certified(10),
        None,
        false,
    ));
    histories.push(finish("permitted", m, format!("repair rows 0,2\n{line}\n")));
    let (mut m, c) = setup(3);
    m.put(key(2), row(Status::Failed, 99)).unwrap();
    let line = result(m.execute(
        &c,
        req(1, (0..3).map(key).collect()),
        Time::Certified(10),
        None,
        false,
    ));
    histories.push(finish(
        "unsafe-query",
        m,
        format!("repair includes row outside incident\n{line}\nrows unchanged\n"),
    ));
    let (mut m, c) = setup(6);
    let r = req(1, (0..6).map(key).collect());
    let p = m.preview(&c, &r, Time::Certified(10)).unwrap();
    let a = m.approve(Principal(3), p, Time::Certified(10)).unwrap();
    m.put(key(0), row(Status::Succeeded, 103)).unwrap();
    let line = result(m.execute(&c, r, Time::Certified(11), Some(a), false));
    // Explicit keys encounter the changed transition before approval validation.
    histories.push(finish(
        "stale-preview",
        m,
        format!(
            "preview and approve 6 rows; row 0 then succeeds\n{line}\nno repair effects committed\n"
        ),
    ));
    let (mut m, c) = setup(1);
    let r = req(1, vec![key(0)]);
    let first = result(m.execute(&c, r.clone(), Time::Certified(10), None, true));
    let status = m.status(&c, OperationId(1)).unwrap();
    assert!(matches!(status, OperationStatus::Decided(Ok(_))));
    let retry = result(m.execute(&c, r, Time::Certified(11), None, false));
    histories.push(finish("ambiguous-retry",m,format!("repair operation 1; reply lost\n{first}\nstatus 1: committed\nretry 1: {retry}\none logical receipt\n")));
    let (mut m, c) = setup(26);
    for i in 0..25 {
        m.execute(&c, req(i, vec![key(i)]), Time::Certified(10), None, false);
    }
    let line = result(m.execute(&c, req(25, vec![key(25)]), Time::Certified(10), None, false));
    histories.push(finish(
        "lifetime-budget",
        m,
        format!("25 one-row repair operations committed\n26th: {line}\n"),
    ));
    histories
}
pub fn golden_transcript() -> String {
    let mut text = String::new();
    for h in golden_histories() {
        text.push_str(&format!("== {} ==\n{}", h.name, h.transcript));
    }
    text
}
