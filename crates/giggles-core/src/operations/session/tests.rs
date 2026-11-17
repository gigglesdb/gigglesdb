use super::*;
use crate::{
    Cancellation, CrashPoint, DecisionPoint, Limits, relational::with_sql_parser_forbidden,
};

fn fixture() -> Admin {
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let path = crate::test_artifact_root()
        .join(format!(
            "native-session-{:032x}",
            u128::from_be_bytes(token)
        ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
    admin
        .sql_session()
        .execute("CREATE TABLE app (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    admin
}
fn insert(id: i64) -> Request {
    Request::Insert {
        table: "app".into(),
        values: vec![("id".into(), ValueExpr::Literal(Value::Int(id)))],
    }
}
fn read() -> Request {
    Request::Read {
        table: "app".into(),
        filter: None,
        order: vec![],
        limit: Some(ValueExpr::Literal(Value::Int(10))),
        projection: vec!["id".into()],
    }
}
fn count(admin: &Admin) -> usize {
    admin
        .sql_session()
        .execute("SELECT id FROM app LIMIT 10", &[], None)
        .unwrap()
        .rows
        .len()
}
fn block(fail: bool) -> Transaction {
    Transaction {
        read_only: false,
        steps: vec![
            TransactionStep::Execute(insert(2)),
            TransactionStep::Require(Expr::Value(ValueExpr::Literal(Value::Bool(!fail)))),
        ],
        emit: None,
    }
}

#[test]
fn mixed_sql_native_staging_and_typed_controls_never_parse_sql() {
    let admin = fixture();
    let mut session = admin.sql_session();
    with_sql_parser_forbidden(|| session.begin_native()).unwrap();
    session
        .execute("INSERT INTO app VALUES (1)", &[], None)
        .unwrap();
    let out =
        with_sql_parser_forbidden(|| session.execute_native(&read(), &Bindings::new())).unwrap();
    assert_eq!(out.rows.len(), 1);
    with_sql_parser_forbidden(|| {
        session.execute_native_transaction(&block(false), &Bindings::new())
    })
    .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM app LIMIT 10", &[], None)
            .unwrap()
            .rows
            .len(),
        2
    );
    assert_eq!(count(&admin), 0);
    with_sql_parser_forbidden(|| session.commit_native()).unwrap();
    assert_eq!(count(&admin), 2);
    session.execute("BEGIN", &[], None).unwrap();
    with_sql_parser_forbidden(|| session.execute_native(&insert(3), &Bindings::new())).unwrap();
    with_sql_parser_forbidden(|| session.rollback_native()).unwrap();
    assert_eq!(count(&admin), 2);
}

#[test]
fn idle_bridge_matches_embedded_engine_and_releases_transaction_slots() {
    let left = fixture();
    let right = fixture();
    let engine = Engine::new(left);
    let mut session = right.sql_session();
    for request in [insert(1), read()] {
        let expected =
            with_sql_parser_forbidden(|| engine.execute(&request, &Bindings::new())).unwrap();
        let actual =
            with_sql_parser_forbidden(|| session.execute_native(&request, &Bindings::new()))
                .unwrap();
        assert_eq!(actual, expected);
        assert!(!session.in_transaction());
    }
    let mut held: Vec<_> = (0..right.max_transactions())
        .map(|_| right.sql_session())
        .collect();
    for session in &mut held {
        session.begin_native().unwrap();
    }
    assert!(matches!(
        right.sql_session().begin_native(),
        Err(CoreError::Busy)
    ));
    for session in &mut held {
        session.rollback_native().unwrap();
    }
    session.begin_native().unwrap();
    session.rollback_native().unwrap();
}

#[test]
fn failing_block_aborts_outer_work_and_idle_block_publishes_nothing() {
    let admin = fixture();
    let mut session = admin.sql_session();
    assert_eq!(
        session.execute_native_transaction(&block(true), &Bindings::new()),
        Err(Error::RequirementFailed)
    );
    assert!(!session.in_transaction());
    assert_eq!(count(&admin), 0);
    session.begin_native().unwrap();
    session
        .execute_native(&insert(1), &Bindings::new())
        .unwrap();
    assert_eq!(
        session.execute_native_transaction(&block(true), &Bindings::new()),
        Err(Error::RequirementFailed)
    );
    assert!(session.is_aborted());
    assert_eq!(
        session.commit_native(),
        Err(CoreError::Sql(SqlError::Aborted))
    );
    assert!(session.execute_native(&read(), &Bindings::new()).is_err());
    with_sql_parser_forbidden(|| session.rollback_native()).unwrap();
    assert_eq!(count(&admin), 0);
}

#[test]
fn read_only_block_preserves_outer_staging_and_idle_discards_its_snapshot() {
    let admin = fixture();
    let mut session = admin.sql_session();
    let block = Transaction {
        read_only: true,
        steps: vec![TransactionStep::Let {
            name: "rows".into(),
            request: read(),
        }],
        emit: Some(vec![EmitExpr::Rows("rows".into())]),
    };
    session
        .execute_native_transaction(&block, &Bindings::new())
        .unwrap();
    assert!(!session.in_transaction());
    session.begin_native().unwrap();
    session
        .execute_native(&insert(1), &Bindings::new())
        .unwrap();
    let result = session
        .execute_native_transaction(&block, &Bindings::new())
        .unwrap();
    assert!(matches!(&result.emitted[0], EmittedValue::Rows { rows, .. } if rows.len() == 1));
    assert!(session.in_transaction());
    assert_eq!(count(&admin), 0);
    session.commit_native().unwrap();
    assert_eq!(count(&admin), 1);
}

#[test]
fn validation_and_control_errors_preserve_aborted_rollback_escape() {
    let admin = fixture();
    let mut session = admin.sql_session();
    assert_eq!(
        session.commit_native(),
        Err(CoreError::Sql(SqlError::Transaction))
    );
    assert_eq!(
        session.rollback_native(),
        Err(CoreError::Sql(SqlError::Transaction))
    );
    session.begin_native().unwrap();
    assert_eq!(
        session.begin_native(),
        Err(CoreError::Sql(SqlError::Transaction))
    );
    assert!(session.is_aborted());
    session.rollback_native().unwrap();
    session.begin_native().unwrap();
    let bindings = Bindings::from([("unused".into(), Value::Int(1))]);
    assert!(matches!(
        session.execute_native(&insert(1), &bindings),
        Err(Error::Bind(_))
    ));
    assert!(session.is_aborted());
    session.rollback_native().unwrap();
    assert_eq!(count(&admin), 0);
}

#[test]
fn cancellation_and_panics_never_leave_usable_partial_staging() {
    for outer in [false, true] {
        let admin = fixture();
        let mut session = admin.sql_session();
        if outer {
            session.begin_native().unwrap();
        }
        let cancelled = Cancellation::default();
        let result = cancelled.scope(|| {
            session.native_scope(
                false,
                || Ok(()),
                |session| {
                    session.execute_native(&insert(1), &Bindings::new())?;
                    cancelled.cancel();
                    Ok(())
                },
            )
        });
        assert!(result.is_err());
        assert_eq!(session.in_transaction(), outer);
        assert_eq!(session.is_aborted(), outer);
        session.rollback();
        if outer {
            session.begin_native().unwrap();
        }
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<()> = session.native_scope(
                false,
                || Ok(()),
                |session| {
                    session.execute_native(&insert(1), &Bindings::new())?;
                    panic!("injected native staging panic")
                },
            );
        }));
        assert!(caught.is_err());
        assert_eq!(session.in_transaction(), outer);
        assert_eq!(session.is_aborted(), outer);
        session.rollback();
        assert_eq!(count(&admin), 0);
    }
}

#[test]
fn native_publication_uses_exact_cancellation_fence() {
    for outer in [false, true] {
        for point in [
            DecisionPoint::BeforeDecision,
            DecisionPoint::Decided,
            DecisionPoint::Committed,
        ] {
            let admin = fixture();
            let mut session = admin.sql_session();
            if outer {
                session.begin_native().unwrap();
                session
                    .execute_native(&insert(1), &Bindings::new())
                    .unwrap();
            }
            admin.set_decision_hook(Some(std::sync::Arc::new(move |seen| {
                if seen == point {
                    Cancellation::current().unwrap().cancel();
                }
            })));
            let context = Cancellation::default();
            let success = with_sql_parser_forbidden(|| {
                context.scope(|| {
                    if outer {
                        session.commit_native().is_ok()
                    } else {
                        session.execute_native(&insert(1), &Bindings::new()).is_ok()
                    }
                })
            });
            admin.set_decision_hook(None);
            assert_eq!(success, point != DecisionPoint::BeforeDecision);
            assert_eq!(count(&admin), usize::from(success));
            if outer && !success {
                assert!(session.is_aborted());
            }
            session.rollback();
        }
    }
}

#[test]
fn failed_commit_is_ambiguous_with_owned_cleanup_and_outer_abort() {
    for outer in [false, true] {
        let admin = fixture();
        let mut session = admin.sql_session();
        if outer {
            session.begin_native().unwrap();
            session
                .execute_native(&insert(1), &Bindings::new())
                .unwrap();
        }
        admin
            .set_crash_point(CrashPoint::ErrorBeforeCommit)
            .unwrap();
        if outer {
            assert!(matches!(
                session.commit_native(),
                Err(CoreError::Storage(_))
            ));
            assert!(session.is_aborted());
            session.rollback_native().unwrap();
        } else {
            assert_eq!(
                session.execute_native(&insert(1), &Bindings::new()),
                Err(Error::Unknown)
            );
            assert!(!session.in_transaction());
        }
    }
}
