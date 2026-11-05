mod common;
use common::*;
use giggles_outbox::*;

#[test]
fn app_and_enqueue_rollback() {
    let (_, o) = db();
    schema(&o);
    let mut t = o.begin().unwrap();
    t.execute("INSERT INTO orders VALUES (1,'new')", &[], None)
        .unwrap();
    t.enqueue(intent("tenant", "a", b"a")).unwrap();
    t.rollback();
    assert!(o.scan_application(scan()).unwrap().rows.is_empty());
    assert!(o.status("tenant", "a").unwrap().is_none());
    {
        let mut t = o.begin().unwrap();
        t.execute("INSERT INTO orders VALUES (2,'new')", &[], None)
            .unwrap();
        t.enqueue(intent("tenant", "b", b"b")).unwrap();
    }
    assert!(o.scan_application(scan()).unwrap().rows.is_empty());
    assert!(o.status("tenant", "b").unwrap().is_none());
}
#[test]
fn commit_reopen_preserves_both() {
    let (p, o) = db();
    schema(&o);
    let mut t = o.begin().unwrap();
    t.execute(
        "INSERT INTO orders VALUES ($1,$2)",
        &[Value::Int(1), Value::Text("new".into())],
        None,
    )
    .unwrap();
    let receipt = t.enqueue(intent("tenant", "a", b"hi")).unwrap();
    assert_eq!(t.commit().unwrap(), CommitOutcome::Committed);
    drop(o);
    let o = Outbox::open(p).unwrap();
    assert_eq!(
        o.scan_application(scan()).unwrap().rows[0].values,
        vec![Value::Int(1), Value::Text("new".into())]
    );
    assert_eq!(o.status("tenant", "a").unwrap().unwrap().receipt, receipt);
}
#[test]
fn same_id_permanent_conflict_and_streams() {
    let (_, o) = db();
    seed(&o, "a");
    let d = claim(&o, "owner", 0).remove(0);
    o.ack(&d.token, now(1)).unwrap();
    let original = o.status("tenant", "a").unwrap().unwrap().receipt;
    let mut t = o.begin().unwrap();
    assert_eq!(
        t.enqueue(intent("tenant", "a", b"hello")).unwrap(),
        original
    );
    t.commit().unwrap();
    assert_eq!(
        o.status("tenant", "a").unwrap().unwrap().phase,
        Phase::Acked
    );
    for i in [
        intent("tenant", "a", b"different"),
        EnqueueIntent {
            destination_kind: "other",
            ..intent("tenant", "a", b"hello")
        },
    ] {
        let mut t = o.begin().unwrap();
        assert_eq!(t.enqueue(i), Err(Error::Conflict));
        assert_eq!(t.commit(), Err(Error::Aborted));
    }
    let mut t = o.begin().unwrap();
    t.enqueue(intent("other", "a", b"different")).unwrap();
    t.commit().unwrap();
    assert_eq!(o.reconcile().unwrap().retained_ids, 2);
}
#[test]
fn batch_refusal_no_partial_write() {
    let (_, o) = db();
    schema(&o);
    seed(&o, "existing");
    let mut t = o.begin().unwrap();
    t.execute("INSERT INTO orders VALUES (1,'new')", &[], None)
        .unwrap();
    assert_eq!(
        t.enqueue_batch(&[
            intent("tenant", "new", b"a"),
            intent("tenant", "existing", b"bad")
        ]),
        Err(Error::Conflict)
    );
    assert_eq!(t.commit(), Err(Error::Aborted));
    assert!(o.status("tenant", "new").unwrap().is_none());
    assert!(o.scan_application(scan()).unwrap().rows.is_empty());
}
#[test]
fn controls_and_internal_sql_blocked() {
    let (_, o) = db();
    schema(&o);
    for sql in [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SELECT * FROM orders LIMIT 1",
        "EXPLAIN SELECT * FROM orders LIMIT 1",
        "SELECT orders.id FROM orders JOIN outbox_internal_state ON orders.id=outbox_internal_state.id LIMIT 1",
        "UPDATE outbox_internal_state SET body=$1 WHERE id=1",
        "DELETE FROM outbox_internal_state WHERE id=1",
        "CREATE TABLE outbox_internal_escape (id BIGINT PRIMARY KEY)",
    ] {
        let mut t = o.begin().unwrap();
        t.execute("INSERT INTO orders VALUES (1,'staged')", &[], None)
            .unwrap();
        assert!(t.execute(sql, &[], None).is_err(), "{sql}");
        assert_eq!(t.commit(), Err(Error::Aborted));
        assert!(o.scan_application(scan()).unwrap().rows.is_empty());
    }
    let mut s = scan();
    s.table = "outbox_internal_state".into();
    assert_eq!(o.scan_application(s), Err(Error::Forbidden));
}
#[test]
fn app_cas_conflict_not_replayed() {
    let (_, o) = db();
    schema(&o);
    let mut t = o.begin().unwrap();
    t.execute("INSERT INTO orders VALUES (1,'new')", &[], None)
        .unwrap();
    t.commit().unwrap();
    let v = o.scan_application(scan()).unwrap().rows[0].version;
    let mut a = o.begin().unwrap();
    let mut b = o.begin().unwrap();
    a.execute("UPDATE orders SET status='a' WHERE id=1", &[], Some(v))
        .unwrap();
    a.enqueue(intent("tenant", "a", b"a")).unwrap();
    b.execute("UPDATE orders SET status='b' WHERE id=1", &[], Some(v))
        .unwrap();
    b.enqueue(intent("tenant", "b", b"b")).unwrap();
    a.commit().unwrap();
    assert_eq!(b.commit(), Err(Error::Conflict));
    assert!(o.status("tenant", "b").unwrap().is_none());
    assert_eq!(
        o.scan_application(scan()).unwrap().rows[0].values[1],
        Value::Text("a".into())
    );
    let mut t = o.begin().unwrap();
    assert_eq!(
        t.execute("UPDATE orders SET status='bad' WHERE id=1", &[], Some(v)),
        Err(Error::Conflict)
    );
}
#[test]
fn acknowledged_quota_retained() {
    let p = fresh("quota");
    let o = Outbox::create(
        p,
        Limits {
            effects: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    seed(&o, "a");
    let d = claim(&o, "o", 0).remove(0);
    o.ack(&d.token, now(1)).unwrap();
    let mut t = o.begin().unwrap();
    assert_eq!(t.enqueue(intent("tenant", "b", b"b")), Err(Error::Limit));
    assert_eq!(t.commit(), Err(Error::Aborted));
    assert_eq!(o.reconcile().unwrap().retained_ids, 1);
}
#[test]
fn payload_and_statement_admission_poison_app_writes() {
    let p = fresh("admission");
    let o = Outbox::create(
        p,
        Limits {
            statements: 1,
            payload_bytes: 4,
            ..Limits::default()
        },
    )
    .unwrap();
    schema(&o);
    let mut t = o.begin().unwrap();
    t.execute("INSERT INTO orders VALUES (1,'new')", &[], None)
        .unwrap();
    assert_eq!(
        t.enqueue(intent("tenant", "a", b"12345")),
        Err(Error::Limit)
    );
    assert_eq!(t.commit(), Err(Error::Aborted));
    let mut t = o.begin().unwrap();
    t.execute("INSERT INTO orders VALUES (1,'new')", &[], None)
        .unwrap();
    assert_eq!(
        t.execute("INSERT INTO orders VALUES (2,'new')", &[], None),
        Err(Error::Limit)
    );
    assert_eq!(t.commit(), Err(Error::Aborted));
    assert!(o.scan_application(scan()).unwrap().rows.is_empty());
}
#[test]
fn duplicate_ids_inside_batch_atomic() {
    let (_, o) = db();
    let mut t = o.begin().unwrap();
    let r = t
        .enqueue_batch(&[intent("tenant", "a", b"1"), intent("tenant", "a", b"1")])
        .unwrap();
    assert_eq!(r[0], r[1]);
    t.commit().unwrap();
    assert_eq!(o.reconcile().unwrap().retained_ids, 1);
    let mut t = o.begin().unwrap();
    assert_eq!(
        t.enqueue_batch(&[intent("tenant", "b", b"1"), intent("tenant", "b", b"2")]),
        Err(Error::Conflict)
    );
    assert_eq!(t.commit(), Err(Error::Aborted));
    assert!(o.status("tenant", "b").unwrap().is_none());
}
#[test]
fn invalid_identifiers_and_limits_refused() {
    assert!(
        Outbox::create(
            fresh("invalid"),
            Limits {
                effects: 0,
                ..Limits::default()
            }
        )
        .is_err()
    );
    let (_, o) = db();
    for id in ["", "has space", "bad\n", &"x".repeat(65)] {
        let mut t = o.begin().unwrap();
        assert_eq!(t.enqueue(intent("tenant", id, b"a")), Err(Error::Invalid));
    }
}

#[test]
fn negative_control_effect_dedupe_does_not_dedupe_arbitrary_application_sql() {
    let (_, o) = db();
    schema(&o);
    for order in [1, 2] {
        let mut t = o.begin().unwrap();
        t.execute(
            "INSERT INTO orders VALUES ($1,'accepted')",
            &[Value::Int(order)],
            None,
        )
        .unwrap();
        t.enqueue(intent("tenant", "same-effect", b"same")).unwrap();
        t.commit().unwrap();
    }
    assert_eq!(o.reconcile().unwrap().retained_ids, 1);
    assert_eq!(
        o.scan_application(scan()).unwrap().rows.len(),
        2,
        "caller explicitly repeated business work; event dedupe must not imply business exactly-once"
    );
}
