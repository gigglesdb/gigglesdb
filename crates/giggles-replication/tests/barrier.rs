use giggles_replication::barrier::ReadBarrier;

#[test]
fn releases_all_eligible_ready_rounds() {
    let mut b = ReadBarrier::new(7, 2);
    for (w, index) in [(1, 10), (2, 5), (3, 8)] {
        let ctx = b.request(w).unwrap();
        assert!(b.on_read_state(&ctx, index).is_none());
    }
    assert!(!b.is_idle());
    assert_eq!(b.on_applied(8), vec![2, 3]);
    assert!(!b.is_idle());
    assert_eq!(b.on_applied(10), vec![1]);
    assert!(b.is_idle());
}

#[test]
fn hundred_requests_use_two_rounds_and_wait_for_apply() {
    let mut b = ReadBarrier::new(42, 3);
    assert!(b.is_idle());
    let first = b.request(0).unwrap();
    assert_eq!(first, [42u64.to_be_bytes(), 1u64.to_be_bytes()].concat());
    for w in 1..100 {
        assert!(b.request(w).is_none());
    }
    let second = b.on_read_state(&first, 10).unwrap();
    assert_eq!(second, [42u64.to_be_bytes(), 2u64.to_be_bytes()].concat());
    assert!(b.on_applied(9).is_empty());
    assert_eq!(b.on_applied(10), vec![0]);
    assert!(b.on_read_state(&second, 12).is_none());
    assert!(b.on_applied(11).is_empty());
    assert_eq!(b.on_applied(12), (1..100).collect::<Vec<_>>());
    assert!(b.on_applied(12).is_empty());
    assert!(b.is_idle());
}

#[test]
fn late_arrivals_and_stale_contexts() {
    let mut b = ReadBarrier::new(1, 3);
    let first = b.request(1).unwrap();
    b.request(2);
    assert!(b.on_read_state(b"unknown", 99).is_none());
    let second = b.on_read_state(&first, 8).unwrap();
    b.request(3);
    assert!(b.on_read_state(&first, 0).is_none());
    assert_eq!(b.on_applied(8), vec![1]);
    let third = b.on_read_state(&second, 9).unwrap();
    assert_eq!(b.on_applied(9), vec![2]);
    assert!(b.on_read_state(&third, 10).is_none());
    assert_eq!(b.on_applied(10), vec![3]);
    assert!(b.is_idle());
}

#[test]
fn retry_merges_queue_and_invalidates_old_round() {
    let mut b = ReadBarrier::new(1, 2);
    assert!(b.tick().is_none());
    let first = b.request(1).unwrap();
    b.request(2);
    assert!(b.tick().is_none());
    let retry = b.tick().unwrap();
    assert_ne!(first, retry);
    b.request(3);
    assert!(b.on_read_state(&first, 0).is_none());
    assert!(b.on_applied(100).is_empty());
    let next = b.on_read_state(&retry, 5).unwrap();
    assert_eq!(b.on_applied(5), vec![1, 2]);
    b.on_read_state(&next, 6);
    assert_eq!(b.on_applied(6), vec![3]);
}

#[test]
fn fail_all_drains_all_categories_without_reusing_contexts() {
    let mut b = ReadBarrier::new(1, 0);
    let first = b.request(1).unwrap();
    b.request(2);
    let second = b.on_read_state(&first, 5).unwrap();
    b.request(3);
    let mut failed = b.fail_all();
    failed.sort();
    assert_eq!(failed, vec![1, 2, 3]);
    assert!(b.is_idle());
    assert!(b.fail_all().is_empty());
    let fresh = b.request(4).unwrap();
    assert_ne!(second, fresh);
    assert!(b.on_read_state(&second, 0).is_none());
    assert!(b.tick().is_some());
    assert_eq!(b.fail_all(), vec![4]);
}
