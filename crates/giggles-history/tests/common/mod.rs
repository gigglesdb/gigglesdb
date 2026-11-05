#![allow(dead_code)]
use giggles_history::*;

pub fn history(attempts: Vec<Attempt>) -> History {
    History {
        version: 1,
        horizon: 100,
        initial: State::new(),
        invariants: Invariants::default(),
        attempts,
    }
}
pub fn tx(
    id: &str,
    invoke: u64,
    complete: u64,
    ops: Vec<Op>,
    observations: Vec<Observation>,
) -> Attempt {
    Attempt {
        id: id.into(),
        logical_id: id.into(),
        invoke,
        complete: Some(complete),
        outcome: Outcome::Committed,
        durable: None,
        decision_at: None,
        ops,
        observations,
        receipt: None,
    }
}
pub fn read(key: i64, value: Option<i64>) -> (Op, Observation) {
    (Op::Read { key }, Observation::Read { value })
}
pub fn writer(id: &str, invoke: u64, complete: u64, key: i64, value: i64) -> Attempt {
    tx(id, invoke, complete, vec![Op::Write { key, value }], vec![])
}
pub fn reader(id: &str, invoke: u64, complete: u64, key: i64, value: Option<i64>) -> Attempt {
    let (op, obs) = read(key, value);
    tx(id, invoke, complete, vec![op], vec![obs])
}
pub fn scanner(
    id: &str,
    invoke: u64,
    complete: u64,
    start: i64,
    end: i64,
    rows: Vec<(i64, i64)>,
) -> Attempt {
    tx(
        id,
        invoke,
        complete,
        vec![Op::Scan { start, end }],
        vec![Observation::Scan { rows }],
    )
}
pub fn verdict(h: &History) -> Verdict {
    check(h, Limits::default()).expect("valid history").verdict
}
pub fn assert_pair(good: &History, bad: &History) {
    assert_eq!(verdict(good), Verdict::Pass);
    assert_eq!(verdict(bad), Verdict::Fail);
}
