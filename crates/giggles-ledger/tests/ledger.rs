use giggles_ledger::*;
use std::{
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "giggles-ledger-{label}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn def() -> Definition {
    Definition {
        ledger: "main".into(),
        asset: "USD".into(),
        scale: 2,
    }
}
fn account(id: &str, rule: BalanceRule) -> AccountDefinition {
    AccountDefinition {
        id: id.into(),
        rule,
        max_credit: None,
    }
}
fn setup() -> (PathBuf, Ledger) {
    let p = path("db");
    let l = Ledger::create(&p, def()).unwrap();
    l.create_account(account("mint", BalanceRule::Unrestricted))
        .unwrap();
    for id in ["a", "b", "c"] {
        l.create_account(account(id, BalanceRule::DebitsMayNotExceedCredits))
            .unwrap();
    }
    (p, l)
}
fn t(debit: &str, credit: &str, n: u128) -> Transfer {
    Transfer {
        ledger: "main".into(),
        asset: "USD".into(),
        debit: debit.into(),
        credit: credit.into(),
        amount: Amount(n),
    }
}
fn intent(id: &str, operation: Operation, now: u64) -> Intent {
    Intent {
        id: id.into(),
        clock: Clock {
            lower: now,
            upper: now,
        },
        operation,
    }
}
fn post(id: &str, d: &str, c: &str, n: u128) -> Intent {
    intent(id, Operation::Post(t(d, c, n)), 0)
}
fn reserve(id: &str, n: u128) -> Intent {
    intent(
        id,
        Operation::Reserve {
            transfer: t("a", "b", n),
            deadline: 100,
        },
        1,
    )
}
fn resolve(id: &str, r: &str, kind: u8, now: u64) -> Intent {
    intent(
        id,
        match kind {
            0 => Operation::Capture { reserve: r.into() },
            1 => Operation::Void { reserve: r.into() },
            _ => Operation::Expire { reserve: r.into() },
        },
        now,
    )
}
fn totals(l: &Ledger, id: &str) -> Totals {
    l.account(id).unwrap().totals
}

#[test]
fn create_reopen_schema_fail_closed() {
    let (p, l) = setup();
    assert_eq!(l.definition().unwrap(), def());
    assert!(Ledger::create(&p, def()).is_err());
    drop(l);
    let l = Ledger::open(&p).unwrap();
    assert_eq!(l.reconcile().unwrap().accounts, 4);
    assert!(l.status("absent").unwrap().is_none());
    assert!(matches!(l.account("absent"), Err(Error::Missing(_))));
}
#[test]
fn durable_duplicate_digest() {
    let (p, l) = setup();
    let i = post("permanent", "mint", "a", i64::MAX as u128 + 100);
    let r = l.execute(i.clone()).unwrap();
    drop(l);
    let l = Ledger::open(&p).unwrap();
    let mut fresh = i.clone();
    fresh.clock.lower = 99;
    fresh.clock.upper = 100;
    assert_eq!(l.execute(fresh).unwrap(), r);
    let mut changed = i;
    changed.operation = Operation::Post(t("mint", "a", 1));
    assert_eq!(l.execute(changed), Err(Error::DuplicateMismatch));
    assert_eq!(l.reconcile().unwrap().receipts, 1);
    assert_eq!(totals(&l, "a").posted_credits.0, i64::MAX as u128 + 100);
}
#[test]
fn zero_self_cross_ledger_asset_and_missing_reject() {
    let (_, l) = setup();
    let mut cases = vec![t("mint", "a", 0), t("a", "a", 1), t("missing", "a", 1)];
    let mut wrong = t("mint", "a", 1);
    wrong.ledger = "other".into();
    cases.push(wrong);
    let mut wrong = t("mint", "a", 1);
    wrong.asset = "EUR".into();
    cases.push(wrong);
    for (n, t) in cases.into_iter().enumerate() {
        assert!(
            l.execute(intent(&format!("invalid{n}"), Operation::Post(t), 0))
                .is_err()
        );
    }
    assert_eq!(l.reconcile().unwrap().receipts, 0);
    assert_eq!(totals(&l, "mint"), Totals::default());
}
#[test]
fn account_definition_immutable_and_invalid() {
    let (_, l) = setup();
    assert!(
        l.create_account(account("a", BalanceRule::Unrestricted))
            .is_err()
    );
    assert!(
        l.create_account(account("bad id", BalanceRule::Unrestricted))
            .is_err()
    );
    assert_eq!(
        l.account("a").unwrap().definition.rule,
        BalanceRule::DebitsMayNotExceedCredits
    );
}
#[test]
fn account_and_permanent_history_capacity_fail_atomically() {
    let (_, l) = setup();
    for n in 4..MAX_ACCOUNTS {
        l.create_account(account(&format!("account{n}"), BalanceRule::Unrestricted))
            .unwrap();
    }
    assert_eq!(
        l.create_account(account("excess", BalanceRule::Unrestricted)),
        Err(Error::Limit)
    );
    assert_eq!(l.reconcile().unwrap().accounts, MAX_ACCOUNTS);
    // The byte envelope can bind before the count limit; no receipts may be pruned.
    let (_, l) = setup();
    let original = post("permanent", "mint", "a", 1);
    let receipt = l.execute(original.clone()).unwrap();
    let mut successes = 1;
    for n in 1..=MAX_RECEIPTS {
        let id = format!("p{n}");
        match l.execute(post(&id, "mint", "a", 1)) {
            Ok(_) => successes += 1,
            Err(Error::Limit) => {
                assert!(l.status(&id).unwrap().is_none());
                break;
            }
            other => panic!("unexpected capacity result {other:?}"),
        }
    }
    assert!(successes > 32 && successes <= MAX_RECEIPTS);
    assert_eq!(l.reconcile().unwrap().receipts, successes);
    assert_eq!(totals(&l, "a").posted_credits, Amount(successes as u128));
    assert_eq!(l.execute(original).unwrap(), receipt);
}
#[test]
fn pending_incoming_is_not_spendable() {
    let (_, l) = setup();
    l.execute(intent(
        "hold",
        Operation::Reserve {
            transfer: t("mint", "a", 9),
            deadline: 100,
        },
        1,
    ))
    .unwrap();
    assert_eq!(
        l.execute(intent("spend", Operation::Post(t("a", "b", 1)), 1)),
        Err(Error::InsufficientFunds)
    );
    l.execute(resolve("capture", "hold", 0, 2)).unwrap();
    l.execute(intent("spend", Operation::Post(t("a", "b", 1)), 2))
        .unwrap();
}
#[test]
fn full_history_retains_capacity_for_each_terminal_decision() {
    for kind in 0..3 {
        let (_, l) = setup();
        l.execute(post("fund", "mint", "a", 10)).unwrap();
        let hold = "H".repeat(64);
        l.execute(reserve(&hold, 7)).unwrap();
        let mut reached = false;
        for n in 0..MAX_RECEIPTS {
            let i = intent(&format!("filler{n}"), Operation::Post(t("mint", "c", 1)), 1);
            match l.execute(i) {
                Ok(_) => {}
                Err(Error::Limit) => {
                    reached = true;
                    break;
                }
                r => panic!("{r:?}"),
            }
        }
        assert!(reached);
        let before = l.reconcile().unwrap().receipts;
        let now = if kind == 2 { 100 } else { 2 };
        l.execute(resolve(&"T".repeat(64), &hold, kind, now))
            .unwrap();
        let after = l.reconcile().unwrap();
        assert_eq!(after.receipts, before + 1);
        assert_eq!(after.pending, 0);
    }
}
#[test]
fn atomic_rollback_and_individual_batch() {
    let (_, l) = setup();
    let good = post("fund", "mint", "a", 10);
    let bad = post("overspend", "a", "b", 11);
    assert_eq!(
        l.atomic_batch(vec![good.clone(), bad.clone()]),
        Err(Error::InsufficientFunds)
    );
    assert!(l.status("fund").unwrap().is_none());
    assert_eq!(totals(&l, "a"), Totals::default());
    let rs = l.individual_batch(vec![good, bad]).unwrap();
    assert!(rs[0].is_ok());
    assert_eq!(rs[1], Err(Error::InsufficientFunds));
    assert_eq!(l.reconcile().unwrap().receipts, 1);
}
#[test]
fn batch_duplicate_ids_and_caps() {
    let (_, l) = setup();
    let i = post("same", "mint", "a", 2);
    let rs = l.atomic_batch(vec![i.clone(), i.clone()]).unwrap();
    assert_eq!(rs[0], rs[1]);
    let mut changed = i.clone();
    changed.operation = Operation::Post(t("mint", "a", 3));
    assert_eq!(
        l.atomic_batch(vec![post("new", "mint", "b", 1), changed]),
        Err(Error::DuplicateMismatch)
    );
    assert!(l.status("new").unwrap().is_none());
    assert_eq!(
        l.atomic_batch(vec![i.clone(); MAX_BATCH + 1]),
        Err(Error::Limit)
    );
    assert_eq!(l.individual_batch(vec![]), Err(Error::Limit));
    assert_eq!(l.atomic_batch(vec![]), Err(Error::Limit));
    // Duplicate mismatches entirely inside a fresh batch must also roll back.
    assert_eq!(
        l.atomic_batch(vec![
            post("fresh", "mint", "a", 2),
            post("fresh", "mint", "a", 3)
        ]),
        Err(Error::DuplicateMismatch)
    );
    assert!(l.status("fresh").unwrap().is_none());
}
#[test]
fn u128_overflow_rollback() {
    let (_, l) = setup();
    l.execute(post("max", "mint", "a", u128::MAX)).unwrap();
    assert_eq!(
        l.execute(post("overflow", "mint", "b", 1)),
        Err(Error::Overflow)
    );
    assert!(l.status("overflow").unwrap().is_none());
    assert_eq!(totals(&l, "b"), Totals::default());
    assert_eq!(totals(&l, "mint").posted_debits.0, u128::MAX);
    l.execute(post("spendmax", "a", "b", u128::MAX)).unwrap();
    assert_eq!(totals(&l, "a").posted_debits.0, u128::MAX);
    assert_eq!(totals(&l, "b").posted_credits.0, u128::MAX);
    assert!(l.execute(post("more", "a", "c", 1)).is_err());
}
#[test]
fn pending_capacity_and_credit_cap() {
    let (_, l) = setup();
    l.execute(post("fund", "mint", "a", 10)).unwrap();
    l.execute(reserve("hold", 7)).unwrap();
    assert_eq!(
        l.execute(intent("spend", Operation::Post(t("a", "c", 4)), 1)),
        Err(Error::InsufficientFunds)
    );
    assert_eq!(totals(&l, "a").pending_debits, Amount(7));
    assert_eq!(totals(&l, "b").pending_credits, Amount(7));
    l.create_account(AccountDefinition {
        id: "limited".into(),
        rule: BalanceRule::DebitsMayNotExceedCredits,
        max_credit: Some(Amount(3)),
    })
    .unwrap();
    assert_eq!(
        l.execute(intent("limit", Operation::Post(t("mint", "limited", 4)), 1)),
        Err(Error::CreditLimit)
    );
    l.execute(intent(
        "limitok",
        Operation::Post(t("mint", "limited", 3)),
        1,
    ))
    .unwrap();
    assert_eq!(
        l.execute(intent(
            "limitpending",
            Operation::Reserve {
                transfer: t("mint", "limited", 1),
                deadline: 100
            },
            1
        )),
        Err(Error::CreditLimit)
    );
}
#[test]
fn reserve_capture_void_expire() {
    for kind in 0..3 {
        let (_, l) = setup();
        l.execute(post("fund", "mint", "a", 10)).unwrap();
        let pending = l.execute(reserve("hold", 7)).unwrap();
        let now = if kind == 2 { 100 } else { 2 };
        let action = resolve("terminal", "hold", kind, now);
        let r = l.execute(action.clone()).unwrap();
        assert_eq!(l.execute(action).unwrap(), r);
        assert_eq!(l.status("hold").unwrap().unwrap(), pending);
        assert_eq!(
            l.execute(resolve("second", "hold", 1, now)),
            Err(Error::Terminal)
        );
        let a = totals(&l, "a");
        let b = totals(&l, "b");
        assert_eq!(a.pending_debits, Amount(0));
        assert_eq!(b.pending_credits, Amount(0));
        assert_eq!(a.posted_debits, Amount(if kind == 0 { 7 } else { 0 }));
        assert_eq!(b.posted_credits, a.posted_debits);
        assert_eq!(l.reconcile().unwrap().pending, 0);
        assert!(matches!(
            l.position("hold").unwrap().unwrap().state,
            TransferState::Captured { .. }
                | TransferState::Voided { .. }
                | TransferState::Expired { .. }
        ));
    }
}
#[test]
fn expiry_checked_at_capture_uncertainty_and_regression() {
    let (_, l) = setup();
    l.execute(post("fund", "mint", "a", 10)).unwrap();
    l.execute(reserve("hold", 5)).unwrap();
    assert_eq!(
        l.execute(resolve("late", "hold", 0, 100)),
        Err(Error::Expired)
    );
    let mut uncertain = resolve("uncertain", "hold", 0, 99);
    uncertain.clock.upper = 100;
    assert_eq!(l.execute(uncertain), Err(Error::TimeUncertain));
    assert_eq!(
        l.execute(resolve("earlyexpire", "hold", 2, 99)),
        Err(Error::TimeUncertain)
    );
    let mut invalid = resolve("badclock", "hold", 0, 5);
    invalid.clock.upper = 4;
    assert_eq!(l.execute(invalid), Err(Error::TimeUncertain));
    assert_eq!(
        l.execute(intent("regress", Operation::Post(t("mint", "c", 1)), 0)),
        Err(Error::ClockRegression)
    );
    l.execute(resolve("expire", "hold", 2, 100)).unwrap();
    assert_eq!(totals(&l, "a").pending_debits, Amount(0));
}
#[test]
fn reserve_deadline_bounds() {
    let (_, l) = setup();
    for deadline in [0, 1] {
        assert_eq!(
            l.execute(intent(
                "bad",
                Operation::Reserve {
                    transfer: t("mint", "a", 1),
                    deadline
                },
                1
            )),
            Err(Error::Expired)
        );
    }
    assert_eq!(
        l.execute(intent(
            "long",
            Operation::Reserve {
                transfer: t("mint", "a", 1),
                deadline: MAX_HOLD + 2
            },
            1
        )),
        Err(Error::Limit)
    );
    l.execute(intent(
        "last",
        Operation::Reserve {
            transfer: t("mint", "a", 1),
            deadline: u64::MAX,
        },
        u64::MAX - 10,
    ))
    .unwrap();
    assert_eq!(
        l.execute(resolve("capture", "last", 0, u64::MAX)),
        Err(Error::Expired)
    );
    l.execute(resolve("expire", "last", 2, u64::MAX)).unwrap();
}
#[test]
fn whole_reversal_once_and_no_reversal_of_reversal() {
    let (_, l) = setup();
    l.execute(post("fund", "mint", "a", 10)).unwrap();
    let reverse = intent(
        "reverse",
        Operation::Reverse {
            original: "fund".into(),
        },
        1,
    );
    let r = l.execute(reverse.clone()).unwrap();
    assert_eq!(l.execute(reverse).unwrap(), r);
    assert_eq!(
        l.execute(intent(
            "overreverse",
            Operation::Reverse {
                original: "fund".into()
            },
            1
        )),
        Err(Error::AlreadyReversed)
    );
    assert!(matches!(
        l.execute(intent(
            "rereverse",
            Operation::Reverse {
                original: "reverse".into()
            },
            1
        )),
        Err(Error::Invalid(_))
    ));
    assert_eq!(totals(&l, "a").posted_debits, Amount(10));
    assert_eq!(totals(&l, "mint").posted_credits, Amount(10));
    assert_eq!(
        l.position("fund").unwrap().unwrap().reversal,
        Some("reverse".into())
    );
}
#[test]
fn captured_reserve_reversal_and_pending_reversal_rejected() {
    let (_, l) = setup();
    l.execute(post("fund", "mint", "a", 10)).unwrap();
    l.execute(reserve("hold", 7)).unwrap();
    assert!(matches!(
        l.execute(intent(
            "earlyreverse",
            Operation::Reverse {
                original: "hold".into()
            },
            1
        )),
        Err(Error::Invalid(_))
    ));
    l.execute(resolve("capture", "hold", 0, 2)).unwrap();
    // Capture receipt aliases cannot be used as another original.
    assert!(matches!(
        l.execute(intent(
            "aliasreverse",
            Operation::Reverse {
                original: "capture".into()
            },
            2
        )),
        Err(Error::Invalid(_))
    ));
    l.execute(intent(
        "reverse",
        Operation::Reverse {
            original: "hold".into(),
        },
        2,
    ))
    .unwrap();
    assert_eq!(totals(&l, "a").posted_credits, Amount(17));
    assert_eq!(totals(&l, "a").posted_debits, Amount(7));
}
#[test]
fn reversal_checks_available_funds() {
    let (_, l) = setup();
    l.execute(post("fund", "mint", "a", 10)).unwrap();
    l.execute(post("spend", "a", "b", 10)).unwrap();
    assert_eq!(
        l.execute(intent(
            "reverse",
            Operation::Reverse {
                original: "fund".into()
            },
            1
        )),
        Err(Error::InsufficientFunds)
    );
    assert_eq!(l.position("fund").unwrap().unwrap().reversal, None);
}
#[test]
fn export_restore_duplicate() {
    let (_, l) = setup();
    let i = post("fund", "mint", "a", 99);
    let r = l.execute(i.clone()).unwrap();
    let image = path("export");
    l.export(&image).unwrap();
    let restored = Ledger::restore(&image, path("restored")).unwrap();
    assert_eq!(restored.execute(i).unwrap(), r);
    assert_eq!(restored.reconcile().unwrap(), l.reconcile().unwrap());
}
#[test]
fn ambiguous_io_reopen_resolves_original_intent() {
    for (fault, committed) in [
        (Fault::ErrorBeforeCommit, false),
        (Fault::ErrorAfterCommit, true),
    ] {
        let (p, l) = setup();
        let i = post("stable", "mint", "a", 5);
        assert_eq!(
            l.execute_with_fault(i.clone(), fault),
            Err(Error::Unknown {
                ids: vec!["stable".into()]
            })
        );
        drop(l);
        let l = Ledger::open(p).unwrap();
        assert_eq!(l.status("stable").unwrap().is_some(), committed);
        l.execute(i).unwrap();
        assert_eq!(l.reconcile().unwrap().receipts, 1);
        assert_eq!(totals(&l, "a").posted_credits, Amount(5));
    }
}

fn race(l: &Ledger, ops: Vec<Intent>) -> Vec<Result<Receipt>> {
    let barrier = Arc::new(Barrier::new(ops.len()));
    std::thread::scope(|scope| {
        let threads: Vec<_> = ops
            .into_iter()
            .map(|i| {
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    l.execute(i)
                })
            })
            .collect();
        threads.into_iter().map(|h| h.join().unwrap()).collect()
    })
}
#[test]
fn concurrent_overspend() {
    for _ in 0..8 {
        let (_, l) = setup();
        l.execute(post("fund", "mint", "a", 10)).unwrap();
        let rs = race(&l, vec![post("one", "a", "b", 7), post("two", "a", "c", 7)]);
        assert_eq!(rs.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            rs.iter()
                .filter(|r| matches!(r, Err(Error::InsufficientFunds)))
                .count(),
            1
        );
        assert_eq!(totals(&l, "a").posted_debits, Amount(7));
        l.reconcile().unwrap();
    }
}
#[test]
fn capture_vs_void() {
    for _ in 0..8 {
        let (_, l) = setup();
        l.execute(post("fund", "mint", "a", 10)).unwrap();
        l.execute(reserve("hold", 7)).unwrap();
        let rs = race(
            &l,
            vec![
                resolve("capture", "hold", 0, 2),
                resolve("void", "hold", 1, 2),
            ],
        );
        assert_eq!(rs.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            rs.iter()
                .filter(|r| matches!(r, Err(Error::Terminal)))
                .count(),
            1
        );
        assert_eq!(totals(&l, "a").pending_debits, Amount(0));
        assert_eq!(l.reconcile().unwrap().receipts, 3);
    }
}
#[test]
fn capture_vs_expire() {
    for _ in 0..8 {
        let (_, l) = setup();
        l.execute(post("fund", "mint", "a", 10)).unwrap();
        l.execute(reserve("hold", 7)).unwrap();
        let rs = race(
            &l,
            vec![
                resolve("capture", "hold", 0, 99),
                resolve("expire", "hold", 2, 100),
            ],
        );
        assert_eq!(rs.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(totals(&l, "a").pending_debits, Amount(0));
        assert_eq!(l.reconcile().unwrap().receipts, 3);
    }
}
#[test]
fn same_id_race() {
    let (_, l) = setup();
    let i = post("same", "mint", "a", 1);
    let rs = race(&l, vec![i.clone(), i.clone(), i.clone(), i]);
    assert!(
        rs.iter()
            .all(|r| r.as_ref().unwrap() == rs[0].as_ref().unwrap())
    );
    assert_eq!(l.reconcile().unwrap().receipts, 1);
}

fn privileged_mutation(p: &PathBuf, mutate: impl FnOnce(&mut serde_json::Value)) {
    use giggles_core::{Admin, Limits, relational::Value};
    let admin = Admin::open(p, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    let r = s
        .execute("SELECT payload FROM ledger_state WHERE slot=1", &[], None)
        .unwrap();
    let Value::Bytes(b) = &r.rows[0].values[0] else {
        panic!("bytes")
    };
    let mut v: serde_json::Value = serde_json::from_slice(b).unwrap();
    mutate(&mut v);
    s.execute(
        "UPDATE ledger_state SET payload=$1 WHERE slot=1",
        &[Value::Bytes(serde_json::to_vec(&v).unwrap())],
        None,
    )
    .unwrap();
}
#[test]
fn replay_negative_controls() {
    // Re-encode in original struct order through string replacement to avoid
    // failing only the canonical-object-order guard before semantic audit.
    use giggles_core::{Admin, Limits, relational::Value};
    for target in ["totals", "digest", "state", "ordinal", "version"] {
        let (p, l) = setup();
        l.execute(post("fund", "mint", "a", 5)).unwrap();
        drop(l);
        let admin = Admin::open(&p, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        let r = s
            .execute("SELECT payload FROM ledger_state WHERE slot=1", &[], None)
            .unwrap();
        let Value::Bytes(b) = &r.rows[0].values[0] else {
            panic!("bytes")
        };
        let original = String::from_utf8(b.clone()).unwrap();
        let altered = match target {
            "totals" => original.replacen(
                "\"posted_credits\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5]",
                "\"posted_credits\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,6]",
                1,
            ),
            "digest" => {
                let start = original.find("\"digest\":[").unwrap() + 10;
                let end = original[start..].find(',').unwrap() + start;
                format!(
                    "{}{}{}",
                    &original[..start],
                    if &original[start..end] == "0" {
                        "1"
                    } else {
                        "0"
                    },
                    &original[end..]
                )
            }
            "state" => original.replacen(
                "\"state\":\"Posted\"",
                "\"state\":{\"Voided\":{\"by\":\"fake\"}}",
                1,
            ),
            "ordinal" => original.replacen("\"ordinal\":1", "\"ordinal\":2", 1),
            _ => original.replacen("\"format\":1", "\"format\":2", 1),
        };
        assert_ne!(original, altered, "negative control changed data: {target}");
        s.execute(
            "UPDATE ledger_state SET payload=$1 WHERE slot=1",
            &[Value::Bytes(altered.into_bytes())],
            None,
        )
        .unwrap();
        drop(s);
        drop(admin);
        assert!(
            matches!(Ledger::open(&p), Err(Error::Corrupt(_))),
            "{target}"
        );
    }
}
#[test]
fn malformed_payload_rejected() {
    let (p, l) = setup();
    drop(l);
    privileged_mutation(&p, |v| {
        v["positions"] = serde_json::json!({"fake":{}});
    });
    assert!(matches!(Ledger::open(&p), Err(Error::Corrupt(_))));
}
#[test]
fn foreign_schema_and_extra_rows_rejected() {
    use giggles_core::{Admin, Limits, relational::Value};
    let p = path("foreign");
    let a = Admin::create(&p, Limits::default()).unwrap();
    a.sql_session()
        .execute("CREATE TABLE other (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    drop(a);
    assert!(matches!(Ledger::open(p), Err(Error::Corrupt(_))));
    let (p, l) = setup();
    drop(l);
    let a = Admin::open(&p, Limits::default()).unwrap();
    a.sql_session()
        .execute(
            "INSERT INTO ledger_state VALUES($1,$2)",
            &[Value::Int(2), Value::Bytes(vec![])],
            None,
        )
        .unwrap();
    drop(a);
    assert!(matches!(Ledger::open(p), Err(Error::Corrupt(_))));
}

#[test]
fn randomized_histories_independent_model() {
    // Independent model uses small exact u128 counters, never ledger transition/replay.
    // Eight deterministic seeds exercise >500 decisions including failures/reopen.
    for seed in 1..=8 {
        let (p, mut l) = setup();
        l.execute(post("fund", "mint", "a", 100)).unwrap();
        let mut credits = [100u128, 0, 0];
        let mut debits = [0u128; 3];
        let mut pending_d = [0u128; 3];
        let mut pending_c = [0u128; 3];
        let mut holds: Vec<(String, usize, usize, u128, bool)> = vec![];
        let mut rng: u64 = seed;
        for step in 0..64 {
            rng = rng.wrapping_mul(6364136223846793005u64).wrapping_add(1);
            let d = (rng % 3) as usize;
            let c = (d + 1 + (rng as usize / 3) % 2) % 3;
            let n = (rng / 13 % 15 + 1) as u128;
            let id = format!("s{step}");
            let clock = Clock {
                lower: step + 1,
                upper: step + 1,
            };
            let names = ["a", "b", "c"];
            let choice = rng / 29 % 4;
            if choice < 2 || holds.iter().all(|h| !h.4) {
                let pending = choice == 1;
                let valid = debits[d] + pending_d[d] + n <= credits[d];
                let op = if pending {
                    Operation::Reserve {
                        transfer: t(names[d], names[c], n),
                        deadline: 1000,
                    }
                } else {
                    Operation::Post(t(names[d], names[c], n))
                };
                let r = l.execute(Intent {
                    id: id.clone(),
                    clock,
                    operation: op,
                });
                assert_eq!(r.is_ok(), valid, "seed {seed} step {step}: {r:?}");
                if valid {
                    if pending {
                        pending_d[d] += n;
                        pending_c[c] += n;
                        holds.push((id, d, c, n, true));
                    } else {
                        debits[d] += n;
                        credits[c] += n;
                    }
                }
            } else {
                let index = holds.iter().position(|h| h.4).unwrap();
                let (ref hold, d, c, n, _) = holds[index];
                let capture = choice == 2;
                let op = if capture {
                    Operation::Capture {
                        reserve: hold.clone(),
                    }
                } else {
                    Operation::Void {
                        reserve: hold.clone(),
                    }
                };
                l.execute(Intent {
                    id,
                    clock,
                    operation: op,
                })
                .unwrap();
                pending_d[d] -= n;
                pending_c[c] -= n;
                if capture {
                    debits[d] += n;
                    credits[c] += n;
                }
                holds[index].4 = false;
            }
            for index in 0..3 {
                let got = totals(&l, names[index]);
                assert_eq!(
                    got,
                    Totals {
                        posted_debits: Amount(debits[index]),
                        posted_credits: Amount(credits[index]),
                        pending_debits: Amount(pending_d[index]),
                        pending_credits: Amount(pending_c[index])
                    },
                    "seed {seed} step {step}"
                );
            }
            assert_eq!(
                credits.iter().sum::<u128>(),
                100 + debits.iter().sum::<u128>()
            );
            assert_eq!(
                pending_d.iter().sum::<u128>(),
                pending_c.iter().sum::<u128>()
            );
            if step % 16 == 0 {
                drop(l);
                l = Ledger::open(&p).unwrap();
            }
        }
        // At the deadline all remaining holds expire and cannot later capture.
        for (number, (hold, d, c, n, active)) in holds.into_iter().enumerate() {
            if active {
                let i = resolve(&format!("expire{number}"), &hold, 2, 1000);
                let r = l.execute(i.clone()).unwrap();
                assert_eq!(l.execute(i).unwrap(), r);
                pending_d[d] -= n;
                pending_c[c] -= n;
                assert_eq!(
                    l.execute(resolve(&format!("late{number}"), &hold, 0, 1000)),
                    Err(Error::Terminal)
                );
            }
        }
        for (index, name) in ["a", "b", "c"].iter().enumerate() {
            assert_eq!(totals(&l, name).pending_debits, Amount(pending_d[index]));
            assert_eq!(totals(&l, name).pending_credits, Amount(pending_c[index]));
        }
        assert_eq!(l.reconcile().unwrap().pending, 0);
    }
}
