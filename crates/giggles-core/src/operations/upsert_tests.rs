use super::*;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

type PublishHook = Box<dyn FnMut()>;
thread_local! {
    static PUBLISH_HOOK: RefCell<Option<PublishHook>> = const { RefCell::new(None) };
}
pub(super) fn before_publish() {
    PUBLISH_HOOK.with_borrow_mut(|hook| {
        if let Some(hook) = hook {
            hook();
        }
    });
}
fn with_publish_hook<T>(hook: impl FnMut() + 'static, work: impl FnOnce() -> T) -> T {
    struct Restore(Option<PublishHook>);
    impl Drop for Restore {
        fn drop(&mut self) {
            PUBLISH_HOOK.with_borrow_mut(|hook| *hook = self.0.take());
        }
    }
    let _restore = Restore(PUBLISH_HOOK.with_borrow_mut(|current| current.replace(Box::new(hook))));
    work()
}
fn fixture() -> (Admin, Engine) {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("artifacts");
    std::fs::create_dir_all(&root).unwrap();
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let admin = Admin::create(
        root.join(format!(
            "upsert-snapshot-{:032x}",
            u128::from_be_bytes(token)
        )),
        crate::Limits::default(),
    )
    .unwrap();
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, n BIGINT NOT NULL)",
        &[],
        None,
    )
    .unwrap();
    sql.execute("INSERT INTO items VALUES (1,10)", &[], None)
        .unwrap();
    (admin.clone(), Engine::new(admin))
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn field(source: ConflictSource) -> Expr {
    Expr::ConflictField {
        source,
        field: "n".into(),
    }
}
fn proposal(id: i64, action: ConflictAction) -> Request {
    Request::Returning {
        request: Box::new(Request::OnConflict {
            request: Box::new(Request::InsertExpressions {
                table: "items".into(),
                values: vec![
                    ("id".into(), Expr::Value(ValueExpr::Literal(Value::Int(id)))),
                    ("n".into(), Expr::Value(ValueExpr::Literal(Value::Int(5)))),
                ],
            }),
            target: vec!["id".into()],
            action,
        }),
        projection: vec![],
    }
}

#[test]
fn upsert_certifies_insert_update_skip_and_noop_against_interleaved_sql() {
    for prepared in [false, true] {
        // Absent -> skip, absent -> update, changed update, all skipped,
        // unchanged update, and existing -> insert after an outside DELETE.
        for race in 0..6 {
            let (admin, engine) = fixture();
            let id = if race < 2 { 2 } else { 1 };
            let action = if matches!(race, 0 | 3) {
                ConflictAction::Nothing
            } else {
                ConflictAction::Update {
                    guard: None,
                    values: vec![(
                        "n".into(),
                        if race == 4 {
                            field(ConflictSource::Existing)
                        } else {
                            Expr::Arithmetic(
                                Box::new(field(ConflictSource::Existing)),
                                Arithmetic::Add,
                                Box::new(field(ConflictSource::Incoming)),
                            )
                        },
                    )],
                }
            };
            let request = proposal(id, action);
            let bound = prepared.then(|| engine.prepare_bound(&request, &Bindings::new()).unwrap());
            let initial = rows(&admin);
            let catalog = admin.catalog().unwrap();
            let outside = admin.clone();
            let committed = Rc::new(RefCell::new(None));
            let captured = committed.clone();
            let calls = Rc::new(Cell::new(0));
            let captured_calls = calls.clone();
            let outcome = with_publish_hook(
                move || {
                    assert_eq!(rows(&outside), initial, "staged upsert leaked");
                    let mut sql = outside.sql_session();
                    if race < 2 {
                        sql.execute("INSERT INTO items VALUES (2,40)", &[], None)
                            .unwrap();
                    } else if race == 5 {
                        sql.execute("DELETE FROM items WHERE id=1", &[], None)
                            .unwrap();
                    } else {
                        sql.execute("UPDATE items SET n=40 WHERE id=1", &[], None)
                            .unwrap();
                    }
                    *captured.borrow_mut() = Some(rows(&outside));
                    captured_calls.set(captured_calls.get() + 1);
                },
                || match &bound {
                    Some(bound) => engine.execute_bound(bound),
                    None => engine.execute(&request, &Bindings::new()),
                },
            );
            assert_eq!(calls.get(), 1);
            assert_eq!(outcome, Err(Error::Conflict));
            assert_eq!(Some(rows(&admin)), *committed.borrow());
            assert_eq!(admin.catalog().unwrap(), catalog);

            // Certification refusals can publish decision metadata, but cannot
            // publish speculative rows, RETURN/tuple metadata or allocations.
            let outside_last_version = if race == 5 { 1 } else { 2 };
            admin
                .sql_session()
                .execute("INSERT INTO items VALUES (100,0)", &[], None)
                .unwrap();
            let marker = rows(&admin)
                .into_iter()
                .find(|row| row.values[0] == Value::Int(100))
                .unwrap();
            assert_eq!(marker.version, outside_last_version + 1);

            // An explicit fresh execution reclassifies the original intent.
            // It is not a replay receipt and makes no automatic retry promise.
            let fresh = match &bound {
                Some(bound) => engine.execute_bound(bound),
                None => engine.execute(&request, &Bindings::new()),
            }
            .unwrap();
            let details = fresh.conflict_results.as_ref().unwrap();
            assert_eq!(details.len(), 1);
            assert_eq!(details[0].input_index, 0);
            assert_eq!(fresh.matched, 1);
            let skipped = matches!(race, 0 | 3);
            let changed = !skipped && race != 4;
            assert_eq!(fresh.affected, usize::from(!skipped));
            assert_eq!(fresh.changed, usize::from(changed));
            assert_eq!(details[0].changed, changed);
            if skipped {
                assert_eq!(details[0].status, ConflictStatus::Skipped);
                assert_eq!(details[0].returned_row, None);
                assert!(fresh.rows.is_empty());
            } else {
                assert_eq!(details[0].status, ConflictStatus::Success);
                assert_eq!(details[0].returned_row, Some(0));
                assert_eq!(fresh.rows.len(), 1);
                let expected = if race == 5 {
                    5
                } else if race == 4 {
                    40
                } else {
                    45
                };
                assert_eq!(fresh.rows[0].values, [Value::Int(id), Value::Int(expected)]);
                assert_eq!(
                    fresh.rows[0].version,
                    if changed {
                        marker.version + 1
                    } else {
                        outside_last_version
                    }
                );
            }
            admin
                .sql_session()
                .execute("INSERT INTO items VALUES (101,0)", &[], None)
                .unwrap();
            let final_marker = rows(&admin)
                .into_iter()
                .find(|row| row.values[0] == Value::Int(101))
                .unwrap();
            assert_eq!(
                final_marker.version,
                marker.version + u64::from(changed) + 1
            );
        }
    }
}

#[test]
fn upsert_scoped_scalar_evaluation_borrows_selected_source_payloads() {
    let payload_bytes = crate::relational::MAX_ROW_BYTES - 18;
    for bytes in [false, true] {
        let existing = if bytes {
            Value::Bytes(vec![1; payload_bytes])
        } else {
            Value::Text("e".repeat(payload_bytes))
        };
        let incoming = if bytes {
            Value::Bytes(vec![2; payload_bytes])
        } else {
            Value::Text("i".repeat(payload_bytes))
        };
        for selected in [ConflictSource::Existing, ConflictSource::Incoming] {
            let expression = Bound::Case {
                operand: None,
                arms: vec![(
                    Bound::Value(Value::Bool(true)),
                    Bound::Call {
                        function: ScalarFunction::Coalesce,
                        arguments: vec![
                            Bound::Value(Value::Null),
                            Bound::ConflictField {
                                source: selected,
                                index: 0,
                            },
                        ],
                    },
                )],
                else_expr: Some(Box::new(Bound::ConflictField {
                    source: if selected == ConflictSource::Existing {
                        ConflictSource::Incoming
                    } else {
                        ConflictSource::Existing
                    },
                    index: 0,
                })),
            };
            let existing_calls = Cell::new(0);
            let incoming_calls = Cell::new(0);
            let result = expression
                .eval_conflict(
                    &|index| {
                        assert_eq!(index, 0);
                        existing_calls.set(existing_calls.get() + 1);
                        &existing
                    },
                    &|index| {
                        assert_eq!(index, 0);
                        incoming_calls.set(incoming_calls.get() + 1);
                        &incoming
                    },
                )
                .unwrap();
            let Cow::Borrowed(value) = result else {
                panic!("selected conflict payload must remain borrowed until admission")
            };
            assert!(std::ptr::eq(
                value,
                if selected == ConflictSource::Existing {
                    &existing
                } else {
                    &incoming
                }
            ));
            assert_eq!(
                existing_calls.get(),
                usize::from(selected == ConflictSource::Existing)
            );
            assert_eq!(
                incoming_calls.get(),
                usize::from(selected == ConflictSource::Incoming)
            );
        }
    }
}

#[test]
fn upsert_last_slot_race_rechecks_capacity_on_explicit_retry() {
    for prepared in [false, true] {
        let (admin, engine) = fixture();
        let mut sql = admin.sql_session();
        sql.execute("BEGIN", &[], None).unwrap();
        for id in 2..MAX_ROWS {
            sql.execute(
                "INSERT INTO items VALUES ($1,0)",
                &[Value::Int(id as i64)],
                None,
            )
            .unwrap();
        }
        sql.execute("COMMIT", &[], None).unwrap();
        assert_eq!(rows(&admin).len(), MAX_ROWS - 1);
        let request = proposal(MAX_ROWS as i64, ConflictAction::Nothing);
        let bound = prepared.then(|| engine.prepare_bound(&request, &Bindings::new()).unwrap());
        let outside = admin.clone();
        let calls = Rc::new(Cell::new(0));
        let captured_calls = calls.clone();
        let outcome = with_publish_hook(
            move || {
                assert_eq!(rows(&outside).len(), MAX_ROWS - 1);
                outside
                    .sql_session()
                    .execute("INSERT INTO items VALUES (99999,40)", &[], None)
                    .unwrap();
                captured_calls.set(captured_calls.get() + 1);
            },
            || match &bound {
                Some(bound) => engine.execute_bound(bound),
                None => engine.execute(&request, &Bindings::new()),
            },
        );
        assert_eq!(calls.get(), 1);
        assert_eq!(outcome, Err(Error::Conflict));
        let stored = rows(&admin);
        assert_eq!(stored.len(), MAX_ROWS);
        assert!(
            stored
                .iter()
                .all(|row| row.values[0] != Value::Int(MAX_ROWS as i64))
        );
        assert_eq!(stored.last().unwrap().version, MAX_ROWS as u64);
        let fresh = match &bound {
            Some(bound) => engine.execute_bound(bound),
            None => engine.execute(&request, &Bindings::new()),
        };
        assert_eq!(fresh, Err(Error::Limit));
        assert_eq!(rows(&admin), stored);
        sql.execute("DELETE FROM items WHERE id=99999", &[], None)
            .unwrap();
        sql.execute("INSERT INTO items VALUES (99998,0)", &[], None)
            .unwrap();
        assert_eq!(rows(&admin).last().unwrap().version, MAX_ROWS as u64 + 1);
    }
}
