mod common;

use giggles_core::{
    Admin, Cancellation, CrashPoint, DecisionPoint, Limits,
    operations::{
        Arithmetic, Bindings, Compare, ConflictAction, EmitExpr, Engine, Error, Expr, Intent,
        LocalRef, Projection, Request, Transaction, TransactionStep, ValueExpr,
        once::{
            ONCE_CONTRACT_VERSION, OnceBody, OnceClockUnavailable, OnceError, OnceIdentity,
            OnceOutcome, OnceRequest, OnceStatus, OnceTime,
        },
    },
    relational::{SqlRow, Value as V},
};
use std::{
    path::PathBuf,
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

fn with_sql_parser_forbidden<T>(work: impl FnOnce() -> T) -> T {
    #[cfg(feature = "test-sql-parser-trap")]
    {
        giggles_core::relational::with_sql_parser_forbidden(work)
    }
    #[cfg(not(feature = "test-sql-parser-trap"))]
    {
        work()
    }
}

fn val(value: V) -> Expr {
    Expr::Value(ValueExpr::Literal(value))
}
fn int(value: i64) -> Expr {
    val(V::Int(value))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn eq(a: Expr, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), Compare::Eq, Box::new(b))
}
fn update(filter: Expr, value: Expr) -> Request {
    Request::UpdateExpressions {
        table: "items".into(),
        filter,
        guard: None,
        values: vec![("n".into(), value)],
        intent: Intent::One,
    }
}
fn increment() -> Request {
    Request::Returning {
        request: Box::new(update(
            eq(field("id"), int(1)),
            Expr::Arithmetic(Box::new(field("n")), Arithmetic::Add, Box::new(int(1))),
        )),
        projection: vec![],
    }
}
fn fixture() -> (PathBuf, Admin, Engine) {
    let path = common::fresh("native-once");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, n BIGINT NOT NULL)",
        &[],
        None,
    )
    .unwrap();
    sql.execute("INSERT INTO items (id,n) VALUES (1,10)", &[], None)
        .unwrap();
    let engine = Engine::new(admin.clone());
    (path, admin, engine)
}
fn request(admin: &Admin, key: &[u8], body: OnceBody) -> OnceRequest {
    OnceRequest {
        contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity {
            database_domain: admin.snapshot().unwrap().domain,
            tenant: 42,
            namespace: "application.jobs".into(),
            key: key.to_vec(),
        },
        retention_seconds: 60,
        body,
    }
}
fn at(lower: u64, upper: u64) -> Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime {
        lower_unix_seconds: lower,
        upper_unix_seconds: upper,
        valid_until: Instant::now() + Duration::from_secs(10),
    })
}
fn clock() -> Result<OnceTime, OnceClockUnavailable> {
    at(1000, 1001)
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT id,n FROM items ORDER BY id LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("native-once-export")).unwrap()
}
fn no_clock() -> Result<OnceTime, OnceClockUnavailable> {
    panic!("replay and lookup do not need a clock")
}

#[test]
fn replay_preserves_original_rows_versions_and_identity_before_live_catalog_binding() {
    let (_, admin, engine) = fixture();
    let original = request(&admin, b"job-1", OnceBody::Operation(increment()));
    let first =
        with_sql_parser_forbidden(|| engine.execute_once(&original, &Bindings::new(), &clock))
            .unwrap();
    assert!(!first.replayed);
    assert_eq!(first.retry_until_seconds, 1061);
    let OnceOutcome::Operation(outcome) = first.outcome.as_ref().unwrap() else {
        panic!("wrong outcome kind")
    };
    assert_eq!(outcome.rows[0].values, vec![V::Int(1), V::Int(11)]);
    assert_eq!(outcome.changed, 1);
    admin
        .sql_session()
        .execute("CREATE TABLE other(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    admin
        .sql_session()
        .execute("UPDATE items SET n=99 WHERE id=1", &[], None)
        .unwrap();
    let before = digest(&admin);
    let repeated =
        with_sql_parser_forbidden(|| engine.execute_once(&original, &Bindings::new(), &no_clock))
            .unwrap();
    assert!(repeated.replayed);
    assert_eq!(repeated.outcome, first.outcome);
    assert_eq!(repeated.accepted_fingerprint, first.accepted_fingerprint);
    assert_eq!(repeated.retry_until_seconds, first.retry_until_seconds);
    assert_eq!(
        engine.outcome(&original.identity),
        Ok(OnceStatus::Retained(repeated))
    );
    assert_eq!(
        digest(&admin),
        before,
        "replay must not publish a generation"
    );
    let mut changed = original.clone();
    changed.retention_seconds += 1;
    assert_eq!(
        engine.execute_once(&changed, &Bindings::new(), &no_clock),
        Err(OnceError::IdempotencyConflict)
    );
    assert_eq!(digest(&admin), before);
}

#[test]
fn bound_values_share_literal_identity_and_scope_labels_partition_keys() {
    let (_, admin, engine) = fixture();
    let body = update(
        eq(field("id"), int(1)),
        Expr::Value(ValueExpr::Parameter("amount".into())),
    );
    let original = request(&admin, b"same-key", OnceBody::Operation(body));
    let first = engine
        .execute_once(
            &original,
            &Bindings::from([("amount".into(), V::Int(20))]),
            &clock,
        )
        .unwrap();
    let mut literal = original.clone();
    literal.body = OnceBody::Operation(update(eq(field("id"), int(1)), int(20)));
    assert_eq!(
        engine
            .execute_once(&literal, &Bindings::new(), &no_clock)
            .unwrap()
            .outcome,
        first.outcome
    );
    assert_eq!(
        engine.execute_once(
            &original,
            &Bindings::from([("amount".into(), V::Int(21))]),
            &no_clock
        ),
        Err(OnceError::IdempotencyConflict)
    );
    for (tenant, namespace) in [(43, "application.jobs"), (42, "application.other")] {
        let mut distinct = request(&admin, b"same-key", OnceBody::Operation(increment()));
        distinct.identity.tenant = tenant;
        distinct.identity.namespace = namespace.into();
        assert!(
            !engine
                .execute_once(&distinct, &Bindings::new(), &clock)
                .unwrap()
                .replayed
        );
    }
    assert_eq!(rows(&admin)[0].values[1], V::Int(22));
}

#[test]
fn retained_business_failures_replay_without_retesting_and_discard_earlier_writes() {
    for expected in [
        Error::NotFound,
        Error::AmbiguousMatch,
        Error::ConditionFailed,
        Error::TooManyMatches,
        Error::RequirementFailed,
    ] {
        let (_, admin, engine) = fixture();
        admin
            .sql_session()
            .execute("INSERT INTO items (id,n) VALUES (2,20)", &[], None)
            .unwrap();
        let before = rows(&admin);
        let body = match expected {
            Error::NotFound => OnceBody::Operation(update(eq(field("id"), int(99)), int(5))),
            Error::AmbiguousMatch => OnceBody::Operation(update(val(V::Bool(true)), int(5))),
            Error::ConditionFailed => OnceBody::Operation(Request::UpdateExpressions {
                table: "items".into(),
                filter: eq(field("id"), int(1)),
                guard: Some(val(V::Bool(false))),
                values: vec![("n".into(), int(5))],
                intent: Intent::One,
            }),
            Error::TooManyMatches => OnceBody::Operation(Request::UpdateExpressions {
                table: "items".into(),
                filter: val(V::Bool(true)),
                guard: None,
                values: vec![("n".into(), int(5))],
                intent: Intent::Many {
                    cap: ValueExpr::Literal(V::Int(1)),
                },
            }),
            Error::RequirementFailed => OnceBody::Transaction(Transaction {
                read_only: false,
                steps: vec![
                    TransactionStep::Execute(increment()),
                    TransactionStep::Require(val(V::Bool(false))),
                ],
                emit: None,
            }),
            _ => unreachable!(),
        };
        let original = request(&admin, b"failed", body);
        let receipt = engine
            .execute_once(&original, &Bindings::new(), &clock)
            .unwrap();
        assert_eq!(receipt.outcome, Err(expected.clone()));
        assert_eq!(rows(&admin), before);
        for id in [1, 2] {
            admin
                .sql_session()
                .execute(&format!("DELETE FROM items WHERE id={id}"), &[], None)
                .unwrap();
        }
        if expected == Error::NotFound {
            admin
                .sql_session()
                .execute("INSERT INTO items (id,n) VALUES (99,0)", &[], None)
                .unwrap();
        }
        let before_replay = digest(&admin);
        assert_eq!(
            engine
                .execute_once(&original, &Bindings::new(), &no_clock)
                .unwrap()
                .outcome,
            Err(expected)
        );
        assert_eq!(digest(&admin), before_replay);
    }
}

#[test]
fn admission_runtime_and_cancel_failures_leave_no_key_and_allow_a_fresh_attempt() {
    let (_, admin, engine) = fixture();
    let mut original = request(
        &admin,
        b"available",
        OnceBody::Operation(update(
            eq(field("id"), int(1)),
            Expr::Arithmetic(Box::new(int(1)), Arithmetic::Divide, Box::new(int(0))),
        )),
    );
    let before = digest(&admin);
    assert_eq!(
        engine.execute_once(&original, &Bindings::new(), &no_clock),
        Err(OnceError::Operation(Error::DivisionByZero))
    );
    assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Absent));
    assert_eq!(digest(&admin), before);
    original.body = OnceBody::Operation(increment());
    let cancel = Cancellation::default();
    let contender = cancel.clone();
    admin.set_decision_hook(Some(Arc::new(move |point| {
        if point == DecisionPoint::BeforeDecision {
            assert!(contender.cancel());
        }
    })));
    let error = cancel
        .scope(|| engine.execute_once(&original, &Bindings::new(), &clock))
        .unwrap_err();
    assert!(
        matches!(error, OnceError::Operation(Error::Core(_))),
        "{error:?}"
    );
    admin.set_decision_hook(None);
    assert_eq!(digest(&admin), before);
    assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Absent));
    assert!(
        !engine
            .execute_once(&original, &Bindings::new(), &clock)
            .unwrap()
            .replayed
    );
    assert_eq!(rows(&admin)[0].values[1], V::Int(11));
}

#[test]
fn clock_failure_rolls_back_and_retention_is_extended_until_explicit_compaction() {
    let (_, admin, engine) = fixture();
    let original = request(&admin, b"retention", OnceBody::Operation(increment()));
    let before = digest(&admin);
    assert_eq!(
        engine.execute_once(&original, &Bindings::new(), &|| Err(OnceClockUnavailable)),
        Err(OnceError::ClockUncertain)
    );
    assert_eq!(digest(&admin), before);
    assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Absent));
    engine
        .execute_once(&original, &Bindings::new(), &clock)
        .unwrap();
    // At the deadline, or straddling it, the lower bound does not prove expiry.
    assert_eq!(engine.compact_outcomes(&|| at(1061, 1063)), Ok(0));
    assert!(
        engine
            .execute_once(&original, &Bindings::new(), &no_clock)
            .unwrap()
            .replayed
    );
    assert_eq!(engine.compact_outcomes(&|| at(1064, 1065)), Ok(1));
    let protected = digest(&admin);
    assert_eq!(
        engine.outcome(&original.identity),
        Ok(OnceStatus::Expired {
            retry_until_seconds: 1061
        })
    );
    assert_eq!(
        engine.execute_once(&original, &Bindings::new(), &no_clock),
        Err(OnceError::KeyExpired {
            retry_until_seconds: 1061
        })
    );
    let mut changed = original.clone();
    changed.body = OnceBody::Operation(update(eq(field("id"), int(1)), int(777)));
    assert_eq!(
        engine.execute_once(&changed, &Bindings::new(), &no_clock),
        Err(OnceError::IdempotencyConflict)
    );
    assert_eq!(
        engine.compact_outcomes(&|| at(1000, 1001)),
        Err(OnceError::ClockUncertain)
    );
    assert_eq!(digest(&admin), protected);
    assert_eq!(rows(&admin)[0].values[1], V::Int(11));
}

#[test]
fn identical_and_altered_concurrent_submissions_choose_one_durable_decision() {
    for altered in [false, true] {
        let (_, admin, engine) = fixture();
        let original = request(&admin, b"race", OnceBody::Operation(increment()));
        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|index| {
                let engine = engine.clone();
                let barrier = barrier.clone();
                let mut submission = original.clone();
                if altered && index % 2 == 1 {
                    submission.retention_seconds += 1;
                }
                std::thread::spawn(move || {
                    barrier.wait();
                    let result = engine.execute_once(&submission, &Bindings::new(), &clock);
                    (submission, result)
                })
            })
            .collect();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        let winners: Vec<_> = results
            .iter()
            .filter_map(|(request, result)| {
                result
                    .as_ref()
                    .ok()
                    .filter(|r| !r.replayed)
                    .map(|receipt| (request, receipt))
            })
            .collect();
        assert_eq!(winners.len(), 1, "{results:?}");
        let (winner, receipt) = winners[0];
        for (submission, result) in &results {
            match result {
                Ok(replay) => {
                    assert_eq!(submission.retention_seconds, winner.retention_seconds);
                    assert_eq!(replay.accepted_fingerprint, receipt.accepted_fingerprint);
                    assert_eq!(replay.retry_until_seconds, receipt.retry_until_seconds);
                    assert_eq!(replay.outcome, receipt.outcome);
                }
                Err(OnceError::InProgress) => {
                    assert_eq!(submission.retention_seconds, winner.retention_seconds)
                }
                Err(OnceError::IdempotencyConflict) => {
                    assert_ne!(submission.retention_seconds, winner.retention_seconds)
                }
                other => panic!("unexpected race result {other:?}"),
            }
        }
        assert_eq!(rows(&admin)[0].values[1], V::Int(11));
        assert_eq!(
            engine
                .execute_once(winner, &Bindings::new(), &no_clock)
                .unwrap()
                .outcome,
            receipt.outcome
        );
    }
}

#[test]
fn lost_commit_ack_is_resolved_by_the_original_key_after_reopen() {
    for (point, committed) in [
        (CrashPoint::ErrorBeforeCommit, false),
        (CrashPoint::ErrorAfterDecisionCommit, true),
    ] {
        let (path, admin, engine) = fixture();
        let original = request(&admin, b"ambiguous", OnceBody::Operation(increment()));
        admin.set_crash_point(point).unwrap();
        assert_eq!(
            engine.execute_once(&original, &Bindings::new(), &clock),
            Err(OnceError::Unknown)
        );
        drop(engine);
        drop(admin);
        let reopened = Admin::open(&path, Limits::default()).unwrap();
        let engine = Engine::new(reopened.clone());
        assert_eq!(
            matches!(
                engine.outcome(&original.identity).unwrap(),
                OnceStatus::Retained(_)
            ),
            committed
        );
        let receipt = engine
            .execute_once(&original, &Bindings::new(), &clock)
            .unwrap();
        assert_eq!(receipt.replayed, committed);
        assert_eq!(rows(&reopened)[0].values[1], V::Int(11));
    }
}

#[test]
fn restore_requires_original_domain_on_both_lookup_and_execution() {
    let (_, admin, engine) = fixture();
    let original = request(&admin, b"restore", OnceBody::Operation(increment()));
    engine
        .execute_once(&original, &Bindings::new(), &clock)
        .unwrap();
    let export = common::fresh("once-restore-export");
    admin.export(&export).unwrap();
    let restored =
        Admin::restore(&export, common::fresh("once-restored"), Limits::default()).unwrap();
    let restored_engine = Engine::new(restored.clone());
    assert_ne!(
        restored.snapshot().unwrap().domain,
        original.identity.database_domain
    );
    let before = digest(&restored);
    assert_eq!(
        restored_engine.outcome(&original.identity),
        Err(OnceError::DomainMismatch)
    );
    assert_eq!(
        restored_engine.execute_once(&original, &Bindings::new(), &no_clock),
        Err(OnceError::DomainMismatch)
    );
    assert_eq!(digest(&restored), before);
    assert_eq!(rows(&restored)[0].values[1], V::Int(11));
}

#[test]
fn transaction_and_upsert_receipts_keep_original_emit_descriptors_and_tuple_mapping() {
    let (_, admin, engine) = fixture();
    let upsert = Request::Returning {
        request: Box::new(Request::OnConflict {
            request: Box::new(Request::InsertMany {
                table: "items".into(),
                columns: vec!["id".into(), "n".into()],
                rows: vec![
                    vec![int(3), int(30)],
                    vec![int(1), int(90)],
                    vec![int(2), int(20)],
                ],
                cap: ValueExpr::Literal(V::Int(3)),
            }),
            target: vec!["id".into()],
            action: ConflictAction::Nothing,
        }),
        projection: vec![],
    };
    let original = request(&admin, b"upsert", OnceBody::Operation(upsert));
    let first = engine
        .execute_once(&original, &Bindings::new(), &clock)
        .unwrap();
    let OnceOutcome::Operation(result) = first.outcome.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        vec![V::Int(2), V::Int(3)]
    );
    assert_eq!(
        result
            .conflict_results
            .as_ref()
            .unwrap()
            .iter()
            .map(|r| r.returned_row)
            .collect::<Vec<_>>(),
        vec![Some(1), None, Some(0)]
    );
    let transaction = Transaction {
        read_only: false,
        steps: vec![TransactionStep::Let {
            name: "saved".into(),
            request: increment(),
        }],
        emit: Some(vec![
            EmitExpr::Row("saved".into()),
            EmitExpr::Scalar(Expr::Local(LocalRef::Changed {
                local: "saved".into(),
            })),
        ]),
    };
    let block = request(&admin, b"block", OnceBody::Transaction(transaction));
    let block_first = engine
        .execute_once(&block, &Bindings::new(), &clock)
        .unwrap();
    for id in [1, 2, 3] {
        admin
            .sql_session()
            .execute(&format!("UPDATE items SET n=999 WHERE id={id}"), &[], None)
            .unwrap();
    }
    assert_eq!(
        engine
            .execute_once(&original, &Bindings::new(), &no_clock)
            .unwrap()
            .outcome,
        first.outcome
    );
    assert_eq!(
        engine
            .execute_once(&block, &Bindings::new(), &no_clock)
            .unwrap()
            .outcome,
        block_first.outcome
    );
}

#[test]
fn encoded_receipt_admission_rejects_empty_value_amplification_before_publication() {
    let (_, admin, engine) = fixture();
    admin
        .sql_session()
        .execute("CREATE TABLE source (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    engine
        .execute(
            &Request::InsertMany {
                table: "source".into(),
                columns: vec!["id".into()],
                rows: (0..1024).map(|n| vec![int(n)]).collect(),
                cap: ValueExpr::Literal(V::Int(1024)),
            },
            &Bindings::new(),
        )
        .unwrap();
    let transaction = Transaction {
        read_only: false,
        steps: vec![
            TransactionStep::Execute(increment()),
            TransactionStep::Let {
                name: "empty".into(),
                request: Request::ReadExpressions {
                    table: "source".into(),
                    filter: None,
                    order: vec![],
                    limit: Some(ValueExpr::Literal(V::Int(1024))),
                    projection: (0..64)
                        .map(|n| Projection {
                            expression: val(V::Text(String::new())),
                            alias: Some(format!("c{n}")),
                        })
                        .collect(),
                },
            },
        ],
        emit: Some((0..7).map(|_| EmitExpr::Rows("empty".into())).collect()),
    };
    let mut original = request(&admin, b"large", OnceBody::Transaction(transaction));
    let before = digest(&admin);
    assert_eq!(
        engine.execute_once(&original, &Bindings::new(), &no_clock),
        Err(OnceError::RetentionUnavailable)
    );
    assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Absent));
    assert_eq!(digest(&admin), before);
    let OnceBody::Transaction(transaction) = &mut original.body else {
        panic!()
    };
    transaction.emit.as_mut().unwrap().truncate(1);
    assert!(
        !engine
            .execute_once(&original, &Bindings::new(), &clock)
            .unwrap()
            .replayed
    );
    assert_eq!(rows(&admin)[0].values[1], V::Int(11));
}

#[test]
fn zero_change_successes_still_retain_exact_outcomes_and_preserve_row_versions() {
    let cases = vec![
        Request::Returning {
            request: Box::new(update(eq(field("id"), int(1)), int(10))),
            projection: vec![],
        },
        Request::Returning {
            request: Box::new(Request::OnConflict {
                request: Box::new(Request::InsertExpressions {
                    table: "items".into(),
                    values: vec![("id".into(), int(1)), ("n".into(), int(50))],
                }),
                target: vec!["id".into()],
                action: ConflictAction::Nothing,
            }),
            projection: vec![],
        },
        Request::Returning {
            request: Box::new(Request::InsertMany {
                table: "items".into(),
                columns: vec!["id".into(), "n".into()],
                rows: vec![],
                cap: ValueExpr::Literal(V::Int(0)),
            }),
            projection: vec![],
        },
        Request::UpdateExpressions {
            table: "items".into(),
            filter: Expr::And(
                Box::new(eq(field("id"), int(99))),
                Box::new(eq(field("n"), int(3))),
            ),
            guard: None,
            values: vec![("n".into(), int(0))],
            intent: Intent::Claim {
                order: vec![giggles_core::operations::Sort {
                    field: "id".into(),
                    descending: false,
                }],
            },
        },
    ];
    for body in cases {
        let (_, admin, engine) = fixture();
        let empty_claim = matches!(
            &body,
            Request::UpdateExpressions {
                intent: Intent::Claim { .. },
                ..
            }
        );
        let original = request(&admin, b"zero", OnceBody::Operation(body));
        let before = rows(&admin);
        let first = engine
            .execute_once(&original, &Bindings::new(), &clock)
            .unwrap();
        let OnceOutcome::Operation(outcome) = first.outcome.as_ref().unwrap() else {
            panic!()
        };
        assert_eq!(outcome.changed, 0);
        assert_eq!(rows(&admin), before);
        if empty_claim {
            admin
                .sql_session()
                .execute("INSERT INTO items (id,n) VALUES (99,3)", &[], None)
                .unwrap();
        }
        let decided = digest(&admin);
        assert_eq!(
            engine
                .execute_once(&original, &Bindings::new(), &no_clock)
                .unwrap()
                .outcome,
            first.outcome
        );
        assert_eq!(digest(&admin), decided);
        if empty_claim {
            let mut fresh = original.clone();
            fresh.identity.key = b"new-claim".to_vec();
            let OnceOutcome::Operation(outcome) = engine
                .execute_once(&fresh, &Bindings::new(), &clock)
                .unwrap()
                .outcome
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(outcome.changed, 1);
        }
    }
}

#[test]
fn unsupported_envelopes_and_invalid_certificates_leave_keys_available() {
    let (_, admin, engine) = fixture();
    let original = request(&admin, b"admission", OnceBody::Operation(increment()));
    let before = digest(&admin);
    let mut cases = Vec::new();
    let mut version = original.clone();
    version.contract_version = 2;
    cases.push((version, OnceError::Operation(Error::Unsupported)));
    let mut retention = original.clone();
    retention.retention_seconds = 0;
    cases.push((retention, OnceError::RetentionUnavailable));
    let mut readonly = original.clone();
    readonly.body = OnceBody::Transaction(Transaction {
        read_only: true,
        steps: vec![TransactionStep::Require(val(V::Bool(true)))],
        emit: None,
    });
    cases.push((readonly, OnceError::Operation(Error::Unsupported)));
    let mut read = original.clone();
    read.body = OnceBody::Operation(Request::Read {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![],
    });
    cases.push((read, OnceError::Operation(Error::Unsupported)));
    for (case, expected) in cases {
        assert_eq!(
            engine.execute_once(&case, &Bindings::new(), &no_clock),
            Err(expected)
        );
    }
    for certificate in [
        OnceTime {
            lower_unix_seconds: 2,
            upper_unix_seconds: 1,
            valid_until: Instant::now() + Duration::from_secs(10),
        },
        OnceTime {
            lower_unix_seconds: 0,
            upper_unix_seconds: u64::MAX,
            valid_until: Instant::now() + Duration::from_secs(10),
        },
        OnceTime {
            lower_unix_seconds: 1000,
            upper_unix_seconds: 1001,
            valid_until: Instant::now(),
        },
        OnceTime {
            lower_unix_seconds: 1000,
            upper_unix_seconds: 1001,
            valid_until: Instant::now() + Duration::from_secs(60),
        },
    ] {
        assert_eq!(
            engine.execute_once(&original, &Bindings::new(), &|| Ok(certificate)),
            Err(OnceError::ClockUncertain)
        );
        assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Absent));
        assert_eq!(digest(&admin), before);
    }
    assert!(
        !engine
            .execute_once(&original, &Bindings::new(), &clock)
            .unwrap()
            .replayed
    );
}

#[test]
#[ignore = "subprocess crash worker"]
fn once_process_worker() {
    let path = std::env::var("GIGGLES_ONCE_PATH").unwrap();
    let mode = std::env::var("GIGGLES_ONCE_CRASH").unwrap();
    let admin = Admin::open(path, Limits::default()).unwrap();
    let original = request(&admin, b"process", OnceBody::Operation(increment()));
    let point = match mode.as_str() {
        "before" => CrashPoint::BeforeCommit,
        "after" => CrashPoint::AfterCommit,
        "ack" => CrashPoint::BeforeAck,
        _ => panic!("unknown crash mode"),
    };
    admin.set_crash_point(point).unwrap();
    Engine::new(admin)
        .execute_once(&original, &Bindings::new(), &clock)
        .unwrap();
    panic!("crash seam did not terminate")
}

#[test]
fn real_process_crashes_publish_data_and_receipt_together() {
    for mode in ["before", "after", "ack"] {
        let (path, admin, engine) = fixture();
        let original = request(&admin, b"process", OnceBody::Operation(increment()));
        drop(engine);
        drop(admin);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "once_process_worker", "--ignored", "--nocapture"])
            .env("GIGGLES_ONCE_PATH", &path)
            .env("GIGGLES_ONCE_CRASH", mode)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(86), "{output:?}");
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let engine = Engine::new(admin.clone());
        assert_eq!(
            matches!(
                engine.outcome(&original.identity).unwrap(),
                OnceStatus::Retained(_)
            ),
            mode != "before"
        );
        assert_eq!(
            rows(&admin)[0].values[1],
            V::Int(if mode == "before" { 10 } else { 11 })
        );
        engine
            .execute_once(&original, &Bindings::new(), &clock)
            .unwrap();
        assert_eq!(rows(&admin)[0].values[1], V::Int(11));
    }
}
