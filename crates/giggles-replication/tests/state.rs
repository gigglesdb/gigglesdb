use giggles_replication::state::{
    AppliedPosition as P, ApplyOutcome::*, MemStateMachine, StateMachine,
};

fn pos(index: u64) -> P {
    P { index, term: 2 }
}

#[test]
fn apply_skip_gap_and_noop() {
    let s = MemStateMachine::new();
    assert_eq!(s.applied().unwrap(), None);
    assert_eq!(s.apply(b"zero", pos(0)).unwrap(), Skipped);
    assert!(s.apply(b"gap", pos(2)).is_err());
    assert_eq!(s.applied().unwrap(), None);
    assert_eq!(s.apply(b"one", pos(1)).unwrap(), Applied);
    assert_eq!(
        s.apply(b"duplicate", P { index: 1, term: 9 }).unwrap(),
        Skipped
    );
    assert!(s.apply_noop(pos(3)).is_err());
    assert_eq!(s.apply_noop(pos(2)).unwrap(), Applied);
    assert_eq!(s.apply_noop(pos(1)).unwrap(), Skipped);
    assert_eq!(s.apply(b"three", pos(3)).unwrap(), Applied);
    assert_eq!(s.applied().unwrap(), Some(pos(3)));
    assert_eq!(
        s.applied_batches(),
        vec![(pos(1), b"one".to_vec()), (pos(3), b"three".to_vec())]
    );
    assert!(P { index: 1, term: 99 } < pos(2));
}

#[test]
fn failures_are_once_and_atomic() {
    let s = MemStateMachine::new();
    s.fail_next_apply();
    let error = s.apply(b"one", pos(1)).unwrap_err();
    assert!(!error.to_string().is_empty());
    assert_eq!(s.applied().unwrap(), None);
    assert!(s.applied_batches().is_empty());
    assert_eq!(s.apply(b"one", pos(1)).unwrap(), Applied);
    s.fail_next_apply();
    assert!(s.apply_noop(pos(2)).is_err());
    assert_eq!(s.applied().unwrap(), Some(pos(1)));
    assert_eq!(s.apply_noop(pos(2)).unwrap(), Applied);
    assert_eq!(s.applied_batches().len(), 1);
}
