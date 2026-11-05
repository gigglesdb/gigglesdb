mod common;
use common::*;
use giggles_core::*;

#[test]
fn maximum_selection_limit_does_not_overflow_or_poison_the_database() {
    let path = fresh("maximum-selection-limit");
    let admin = Admin::create(
        &path,
        Limits {
            admitted: 32,
            selected_keys: usize::MAX,
        },
    )
    .unwrap();
    admin.put(key(0), row()).unwrap();
    let executor = admin.grant(grant()).unwrap();
    let result = executor
        .execute(predicate(1), Time::Certified(10), None)
        .unwrap()
        .unwrap();
    assert_eq!(result.charged_transitions, 1);
    assert_eq!(
        admin.snapshot().unwrap().rows[&key(0)].status,
        Status::Queued
    );
    drop(executor);
    drop(admin);
    let reopened = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(
        reopened.snapshot().unwrap().tasks[&TaskId(1)].transitions,
        1
    );
}
