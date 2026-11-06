use super::*;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

type StepHook = Box<dyn FnMut(usize)>;
thread_local! {
    static STEP_HOOK: RefCell<Option<StepHook>> = const { RefCell::new(None) };
}
pub(super) fn before_step(index: usize) {
    STEP_HOOK.with_borrow_mut(|hook| {
        if let Some(hook) = hook {
            hook(index);
        }
    });
}
fn with_step_hook<T>(hook: impl FnMut(usize) + 'static, work: impl FnOnce() -> T) -> T {
    struct Restore(Option<StepHook>);
    impl Drop for Restore {
        fn drop(&mut self) {
            STEP_HOOK.with_borrow_mut(|hook| *hook = self.0.take());
        }
    }
    let _restore = Restore(STEP_HOOK.with_borrow_mut(|current| current.replace(Box::new(hook))));
    work()
}
fn fixture() -> (std::path::PathBuf, Admin, Engine) {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("artifacts");
    std::fs::create_dir_all(&root).unwrap();
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let path = root.join(format!(
        "transaction-snapshot-{:032x}",
        u128::from_be_bytes(token)
    ));
    let admin = Admin::create(&path, crate::Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)",
        &[],
        None,
    )
    .unwrap();
    sql.execute("INSERT INTO items VALUES (1,0)", &[], None)
        .unwrap();
    (path, admin.clone(), Engine::new(admin))
}
fn read() -> Request {
    Request::Read {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec!["value".into()],
    }
}
fn run_with_outside_commit(
    path: &std::path::Path,
    admin: &Admin,
    engine: &Engine,
    transaction: &Transaction,
    prepared: bool,
) -> (Result<TransactionOutcome>, u64) {
    let bound = prepared.then(|| {
        engine
            .prepare_transaction_bound(transaction, &Bindings::new())
            .unwrap()
    });
    let outside = admin.clone();
    let outside_path = path.with_extension("outside-export");
    let outside_digest = Rc::new(Cell::new(None));
    let captured_digest = outside_digest.clone();
    let outside_version = Rc::new(Cell::new(0));
    let captured_version = outside_version.clone();
    let catalog = admin.catalog().unwrap();
    let calls = Rc::new(Cell::new(0));
    let captured_calls = calls.clone();
    let outcome = with_step_hook(
        move |index| {
            if index == 1 {
                // The first block read has completed. Commit a separate SQL
                // writer before the block reaches its next statement.
                outside
                    .sql_session()
                    .execute("UPDATE items SET value=1 WHERE id=1", &[], None)
                    .unwrap();
                captured_digest.set(Some(outside.export(&outside_path).unwrap()));
                captured_version.set(
                    outside
                        .sql_session()
                        .execute("SELECT value FROM items LIMIT 1", &[], None)
                        .unwrap()
                        .rows[0]
                        .version,
                );
                captured_calls.set(captured_calls.get() + 1);
            }
        },
        || {
            if let Some(bound) = &bound {
                engine.execute_transaction_bound(bound)
            } else {
                engine.execute_transaction(transaction, &Bindings::new())
            }
        },
    );
    assert_eq!(calls.get(), 1);
    let final_digest = admin.export(path.with_extension("final-export")).unwrap();
    // Read-only completion publishes nothing. SQL certification refusals
    // may advance existing decision metadata, so writing conflicts instead
    // check the exact stored rows, catalog and next version below.
    if transaction.read_only {
        assert_eq!(Some(final_digest), outside_digest.get());
    }
    assert_eq!(admin.catalog().unwrap(), catalog);
    let actual = admin
        .sql_session()
        .execute("SELECT value FROM items LIMIT 1", &[], None)
        .unwrap()
        .rows
        .remove(0);
    assert_eq!(actual.values, vec![Value::Int(1)]);
    assert_eq!(actual.version, outside_version.get());
    (outcome, actual.version)
}

#[test]
fn read_only_block_pins_snapshot_across_an_interleaved_sql_commit() {
    for prepared in [false, true] {
        let (path, admin, engine) = fixture();
        let initial = admin
            .sql_session()
            .execute("SELECT value FROM items LIMIT 1", &[], None)
            .unwrap()
            .rows;
        let transaction = Transaction {
            read_only: true,
            steps: vec![
                TransactionStep::Let {
                    name: "first".into(),
                    request: read(),
                },
                TransactionStep::Let {
                    name: "second".into(),
                    request: read(),
                },
            ],
            emit: Some(vec![
                EmitExpr::Rows("first".into()),
                EmitExpr::Rows("second".into()),
            ]),
        };
        let (outcome, _) = run_with_outside_commit(&path, &admin, &engine, &transaction, prepared);
        let outcome = outcome.unwrap();
        assert_eq!(outcome.emitted.len(), 2);
        for emitted in outcome.emitted {
            let EmittedValue::Rows { rows, complete, .. } = emitted else {
                panic!("expected rows")
            };
            assert!(complete);
            assert_eq!(rows, initial);
        }
    }
}

#[test]
fn writing_block_rejects_an_interleaved_sql_commit_without_data_effects() {
    for prepared in [false, true] {
        let (path, admin, engine) = fixture();
        let transaction = Transaction {
            read_only: false,
            steps: vec![
                TransactionStep::Let {
                    name: "first".into(),
                    request: read(),
                },
                TransactionStep::Execute(Request::Update {
                    table: "items".into(),
                    filter: Expr::Compare(
                        Box::new(Expr::Field("id".into())),
                        Compare::Eq,
                        Box::new(Expr::Value(ValueExpr::Literal(Value::Int(1)))),
                    ),
                    guard: None,
                    values: vec![("value".into(), ValueExpr::Literal(Value::Int(2)))],
                    intent: Intent::One,
                }),
            ],
            emit: Some(vec![EmitExpr::Rows("first".into())]),
        };
        let (outcome, outside_version) =
            run_with_outside_commit(&path, &admin, &engine, &transaction, prepared);
        assert_eq!(outcome, Err(Error::Conflict));
        admin
            .sql_session()
            .execute("INSERT INTO items VALUES (2,0)", &[], None)
            .unwrap();
        let next = admin
            .sql_session()
            .execute("SELECT value FROM items WHERE id=2 LIMIT 1", &[], None)
            .unwrap()
            .rows
            .remove(0);
        assert_eq!(next.version, outside_version + 1);
    }
}

#[test]
fn insert_many_block_rejects_an_interleaved_sql_commit_atomically() {
    for prepared in [false, true] {
        let (path, admin, engine) = fixture();
        let integer = |value| Expr::Value(ValueExpr::Literal(Value::Int(value)));
        let transaction = Transaction {
            read_only: false,
            steps: vec![
                TransactionStep::Let {
                    name: "first".into(),
                    request: read(),
                },
                TransactionStep::Let {
                    name: "batch".into(),
                    request: Request::Returning {
                        request: Box::new(Request::InsertMany {
                            table: "items".into(),
                            columns: vec!["id".into(), "value".into()],
                            rows: vec![
                                vec![integer(2), integer(20)],
                                vec![integer(3), integer(30)],
                            ],
                            cap: ValueExpr::Literal(Value::Int(2)),
                        }),
                        projection: vec![],
                    },
                },
            ],
            emit: Some(vec![
                EmitExpr::Rows("first".into()),
                EmitExpr::Scalar(Expr::Local(LocalRef::Changed {
                    local: "batch".into(),
                })),
            ]),
        };
        let (outcome, outside_version) =
            run_with_outside_commit(&path, &admin, &engine, &transaction, prepared);
        assert_eq!(outcome, Err(Error::Conflict));
        let mut sql = admin.sql_session();
        let rows = sql
            .execute("SELECT * FROM items LIMIT 1024", &[], None)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, vec![Value::Int(1), Value::Int(1)]);
        // Both proposed rows and their version allocations must disappear.
        sql.execute("INSERT INTO items VALUES (2,0)", &[], None)
            .unwrap();
        let inserted = sql
            .execute("SELECT value FROM items WHERE id=2 LIMIT 1", &[], None)
            .unwrap()
            .rows
            .remove(0);
        assert_eq!(inserted.version, outside_version + 1);
    }
}
