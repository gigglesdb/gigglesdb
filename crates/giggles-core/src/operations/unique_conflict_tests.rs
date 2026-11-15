use super::*;

#[test]
fn into_sql_preserves_division_by_zero_and_type_errors() {
    assert_eq!(
        into_sql(PlanError::Engine(Error::DivisionByZero)),
        SqlError::DivisionByZero
    );
    assert_eq!(into_sql(PlanError::Engine(Error::Type)), SqlError::Type);
}

fn fixture() -> (Admin, SqlSession) {
    fixture_with_indexes(false)
}
fn fixture_with_indexes(pair_only: bool) -> (Admin, SqlSession) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!("gd42-k1-{:032x}", u128::from_be_bytes(nonce)));
    let admin = Admin::create(path, crate::Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session.execute("CREATE TABLE items (tenant BIGINT, id BIGINT, email TEXT, code TEXT, n INTEGER, PRIMARY KEY (tenant,id))", &[], None).unwrap();
    let engine = Engine::new(admin.clone());
    for (name, columns) in [
        ("email_unique", vec!["email"]),
        ("code_unique", vec!["code"]),
        ("pair_unique", vec!["email", "code"]),
    ] {
        if pair_only && name != "pair_unique" {
            continue;
        }
        engine
            .execute_unique_index(&UniqueIndexRequest {
                table: "items".into(),
                name: name.into(),
                columns: columns.into_iter().map(str::to_owned).collect(),
            })
            .unwrap();
    }
    for statement in [
        "INSERT INTO items VALUES (1,1,'a','x',10)",
        "INSERT INTO items VALUES (1,2,'b','y',20)",
    ] {
        session.execute(statement, &[], None).unwrap();
    }
    session.execute("BEGIN", &[], None).unwrap();
    (admin, session)
}
fn row(id: i64, email: Option<&str>, code: Option<&str>, n: i64) -> Vec<Value> {
    vec![
        Value::Int(1),
        Value::Int(id),
        email.map_or(Value::Null, |value| Value::Text(value.into())),
        code.map_or(Value::Null, |value| Value::Text(value.into())),
        Value::Int(n),
    ]
}
fn key(id: i64) -> Vec<u8> {
    crate::relational::encode_key(&[Value::Int(1), Value::Int(id)]).unwrap()
}
fn incoming(index: usize) -> Bound {
    Bound::ConflictField {
        source: ConflictSource::Incoming,
        index,
    }
}
fn update(index: usize) -> BoundConflict {
    BoundConflict::Update {
        guard: Bound::Value(Value::Bool(true)),
        values: vec![(index, incoming(index))],
    }
}
#[derive(Debug)]
struct Observed {
    changes: Vec<(Vec<u8>, SqlRow, Option<Vec<u8>>)>,
    base: u64,
    next: u64,
    outcome: Outcome,
}
#[expect(
    clippy::too_many_arguments,
    reason = "test covers the frozen planner's independent inputs"
)]
fn attempt(
    session: &mut SqlSession,
    rows: &[Vec<Value>],
    action: &BoundConflict,
    target_name: &str,
    returning: bool,
    retained: Option<&mut usize>,
    alter: impl FnOnce(&mut TableDef, &mut u64, &mut Descriptor),
    cancel: &mut dyn FnMut() -> bool,
) -> Result<Observed> {
    let fence = session.batch_fence_engine().map_err(core)?;
    let mut planner_error = None;
    let result = session.apply_batch_engine(fence, &mut || false, |src, apply| {
        let snapshot = src
            .secondary_access_engine(&src.tables["items"], src.epoch, || false)?
            .1
            .clone();
        let base = src.next_version_engine();
        let mut table = src.tables["items"].clone();
        let mut epoch = src.epoch;
        let mut target = src
            .secondary_state_engine(|| false)?
            .descriptors()
            .iter()
            .find(|descriptor| descriptor.name == target_name)
            .unwrap()
            .clone();
        alter(&mut table, &mut epoch, &mut target);
        let projection = table
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| BoundProjection {
                expression: Bound::Field(index),
                column: column.clone(),
            })
            .collect::<Vec<_>>();
        let plan = match plan_unique_conflict(
            src,
            &table,
            epoch,
            Arbiter::Unique(&target),
            Semantics::Native,
            rows,
            action,
            returning.then_some(projection.as_slice()),
            retained,
            cancel,
        )
        .map_err(PlanError::into_operations)
        {
            Ok(plan) => plan,
            Err(error) => {
                assert_eq!(
                    src.secondary_access_engine(&src.tables["items"], src.epoch, || false)?
                        .1,
                    &snapshot
                );
                assert_eq!(src.next_version_engine(), base);
                planner_error = Some(error);
                return Err(SqlError::Type);
            }
        };
        let observed = Observed {
            changes: plan
                .changes
                .iter()
                .map(|change| {
                    (
                        change.new.0.clone(),
                        change.new.1.clone(),
                        change.old.map(|(key, _)| key.to_vec()),
                    )
                })
                .collect(),
            base: src.next_version_engine(),
            next: plan.next_version,
            outcome: plan.outcome.clone(),
        };
        let applied = apply.apply(&plan.batch(), plan.next_version);
        assert_eq!(
            src.secondary_access_engine(&src.tables["items"], src.epoch, || false)?
                .1,
            &snapshot
        );
        assert_eq!(src.next_version_engine(), base);
        applied?;
        Ok(observed)
    });
    if let Some(error) = planner_error {
        return Err(error);
    }
    result.map(|(observed, _)| observed).map_err(core)
}
fn run(session: &mut SqlSession, rows: &[Vec<Value>], action: &BoundConflict) -> Result<Observed> {
    attempt(
        session,
        rows,
        action,
        "email_unique",
        true,
        None,
        |_, _, _| {},
        &mut || false,
    )
}
fn actual(session: &mut SqlSession) -> Vec<SqlRow> {
    session
        .execute(
            "SELECT * FROM items ORDER BY tenant,id LIMIT 1024",
            &[],
            None,
        )
        .unwrap()
        .rows
}
fn counters(observed: &Observed, matched: usize, affected: usize, changed: usize) {
    assert_eq!(
        (
            observed.outcome.matched,
            observed.outcome.affected,
            observed.outcome.changed
        ),
        (matched, affected, changed)
    );
    assert_eq!(observed.next, observed.base + observed.changes.len() as u64);
}

#[test]
fn target_identity_is_exact() {
    for inputs in [vec![], vec![row(3, None, None, 30)]] {
        for case in 0..9 {
            let (_, mut session) = fixture();
            let error = attempt(
                &mut session,
                &inputs,
                &BoundConflict::Nothing,
                "pair_unique",
                false,
                None,
                |table, epoch, target| match case {
                    0 => target.id = u64::MAX,
                    1 => target.unique = false,
                    2 => target.table += 1,
                    3 => target.columns.reverse(),
                    4 => target.key_encoding += 1,
                    5 => *epoch += 1,
                    6 => table.name.push('z'),
                    7 => target.columns.clear(),
                    _ => target.columns[0] = u64::MAX,
                },
                &mut || false,
            )
            .unwrap_err();
            assert_eq!(
                error,
                if matches!(case, 5 | 6) {
                    sql(SqlError::StaleSchema).into_operations()
                } else {
                    Error::Unsupported
                }
            );
            assert!(session.is_aborted());
        }
    }
}

#[test]
fn null_arbiter_means_insert() {
    let (_, mut session) = fixture_with_indexes(true);
    let rows = [
        row(3, None, Some("x"), 30),
        row(4, Some("a"), None, 40),
        row(5, None, None, 50),
    ];
    let result = attempt(
        &mut session,
        &rows,
        &BoundConflict::Nothing,
        "pair_unique",
        true,
        None,
        |_, _, _| {},
        &mut || false,
    )
    .unwrap();
    counters(&result, 3, 3, 3);
    assert!(result.changes.iter().all(|change| change.2.is_none()));
    assert_eq!(actual(&mut session).len(), 5);
}

#[test]
fn winner_update_keeps_winner_pk() {
    let (_, mut session) = fixture();
    let result = run(
        &mut session,
        &[row(99, Some("a"), Some("unused"), 31)],
        &update(4),
    )
    .unwrap();
    counters(&result, 1, 1, 1);
    assert_eq!(result.changes[0].0, key(1));
    assert_eq!(result.changes[0].2, Some(key(1)));
    assert_eq!(
        result.outcome.rows[0].values,
        row(1, Some("a"), Some("x"), 31)
    );
    assert_eq!(result.outcome.rows[0].version, result.base);
    let rows = actual(&mut session);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], result.changes[0].1);
    assert!(rows.iter().all(|row| row.values[1] != Value::Int(99)));
}

#[test]
fn do_nothing_skips() {
    let (_, mut session) = fixture();
    let before = actual(&mut session);
    let result = run(
        &mut session,
        &[row(99, Some("a"), Some("y"), 31)],
        &BoundConflict::Nothing,
    )
    .unwrap();
    counters(&result, 1, 0, 0);
    assert!(result.changes.is_empty());
    assert!(result.outcome.rows.is_empty());
    assert_eq!(
        result.outcome.conflict_results.unwrap(),
        vec![ConflictTupleOutcome {
            input_index: 0,
            status: ConflictStatus::Skipped,
            changed: false,
            returned_row: None
        }]
    );
    assert_eq!(actual(&mut session), before);
}

#[test]
fn duplicate_rules() {
    for rows in [
        vec![row(8, Some("a"), None, 1), row(9, Some("a"), None, 2)],
        vec![
            row(8, Some("fresh"), None, 1),
            row(9, Some("fresh"), None, 2),
        ],
        vec![row(8, None, None, 1), row(8, None, None, 2)],
    ] {
        for action in [BoundConflict::Nothing, update(4)] {
            let (_, mut session) = fixture();
            assert_eq!(
                run(&mut session, &rows, &action).unwrap_err(),
                Error::DuplicateTarget
            );
            assert!(session.is_aborted());
        }
    }
    for (rows, action, expected) in [
        (
            vec![row(3, None, None, 3), row(4, None, None, 4)],
            BoundConflict::Nothing,
            (2, 2),
        ),
        (
            vec![row(9, Some("a"), None, 3), row(9, Some("b"), None, 4)],
            update(4),
            (2, 2),
        ),
        (
            vec![row(3, Some("a"), None, 3), row(3, Some("c"), None, 4)],
            update(4),
            (2, 2),
        ),
    ] {
        let (_, mut session) = fixture();
        let result = run(&mut session, &rows, &action).unwrap();
        counters(&result, 2, expected.0, expected.1);
        assert!(!session.is_aborted());
    }
    // An actual insert never redirects through a source PK collision.
    let (_, mut session) = fixture();
    assert_eq!(
        run(
            &mut session,
            &[row(1, Some("fresh"), None, 1)],
            &BoundConflict::Nothing
        )
        .unwrap_err(),
        sql(SqlError::Duplicate).into_operations()
    );
    assert!(session.is_aborted());
}

#[test]
fn all_guards_before_rhs() {
    let (_, mut session) = fixture();
    let action = BoundConflict::Update {
        guard: Bound::Compare(
            Box::new(incoming(4)),
            Compare::Eq,
            Box::new(Bound::Value(Value::Int(1))),
        ),
        values: vec![(
            4,
            Bound::Arithmetic(
                Box::new(Bound::Value(Value::Int(1))),
                Arithmetic::Divide,
                Box::new(Bound::Value(Value::Int(0))),
            ),
        )],
    };
    assert_eq!(
        run(
            &mut session,
            &[row(8, Some("a"), None, 1), row(9, Some("b"), None, 0)],
            &action
        )
        .unwrap_err(),
        Error::ConditionFailed
    );
    assert!(session.is_aborted());
}

#[test]
fn no_op_update() {
    let (_, mut session) = fixture();
    let before = actual(&mut session);
    let result = run(&mut session, &[row(99, Some("a"), None, 10)], &update(4)).unwrap();
    counters(&result, 1, 1, 0);
    assert!(result.changes.is_empty());
    assert_eq!(result.next, result.base);
    assert_eq!(result.outcome.rows, vec![before[0].clone()]);
    assert_eq!(
        result.outcome.conflict_results.unwrap()[0],
        ConflictTupleOutcome {
            input_index: 0,
            status: ConflictStatus::Success,
            changed: false,
            returned_row: Some(0)
        }
    );
    assert_eq!(actual(&mut session), before);
}

#[test]
fn versions_follow_actual_key_order() {
    let mut assignments = Vec::new();
    for reverse in [false, true] {
        let (_, mut session) = fixture();
        let mut rows = vec![
            row(4, Some("d"), None, 40),
            row(99, Some("a"), None, 11),
            row(3, Some("c"), None, 30),
            row(98, Some("b"), None, 21),
        ];
        if reverse {
            rows.reverse();
        }
        let result = run(&mut session, &rows, &update(4)).unwrap();
        counters(&result, 4, 4, 4);
        assert_eq!(
            result
                .changes
                .iter()
                .map(|change| change.0.clone())
                .collect::<Vec<_>>(),
            (1..=4).map(key).collect::<Vec<_>>()
        );
        for (offset, change) in result.changes.iter().enumerate() {
            assert_eq!(change.1.version, result.base + offset as u64);
        }
        assert_eq!(
            actual(&mut session),
            result
                .changes
                .iter()
                .map(|change| change.1.clone())
                .collect::<Vec<_>>()
        );
        assignments.push(result.changes);
    }
    assert_eq!(assignments[0], assignments[1]);
}

#[test]
fn swap_and_collision_through_publisher() {
    for collision in [false, true] {
        let (admin, mut session) = fixture();
        let before = actual(&mut session);
        let proposals = [
            row(99, Some("a"), Some("y"), 0),
            row(98, Some("b"), Some(if collision { "y" } else { "x" }), 0),
        ];
        let result = run(&mut session, &proposals, &update(3));
        if collision {
            assert_eq!(
                result.unwrap_err(),
                sql(SqlError::Duplicate).into_operations()
            );
            assert!(session.is_aborted());
            assert_eq!(
                session.execute("SELECT * FROM items", &[], None),
                Err(CoreError::Sql(SqlError::Aborted))
            );
            assert_eq!(actual(&mut admin.sql_session()), before);
            session.rollback();
            assert_eq!(actual(&mut session), before);
        } else {
            let result = result.unwrap();
            counters(&result, 2, 2, 2);
            let rows = actual(&mut session);
            assert_eq!(rows[0].values[3], Value::Text("y".into()));
            assert_eq!(rows[1].values[3], Value::Text("x".into()));
            assert_eq!(
                rows,
                result
                    .changes
                    .into_iter()
                    .map(|change| change.1)
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn capacity_limit() {
    let (_, mut session) = fixture();
    let rows = (3..=crate::relational::MAX_ROWS as i64)
        .map(|id| row(id, None, None, 0))
        .collect::<Vec<_>>();
    let filled = run(&mut session, &rows, &BoundConflict::Nothing).unwrap();
    counters(&filled, 1022, 1022, 1022);
    assert_eq!(actual(&mut session).len(), crate::relational::MAX_ROWS);
    assert_eq!(
        run(
            &mut session,
            &[row(2048, None, None, 0)],
            &BoundConflict::Nothing
        )
        .unwrap_err(),
        Error::Limit
    );
    assert!(session.is_aborted());
}

#[test]
fn admission_first() {
    for oversized in [false, true] {
        let (_, mut session) = fixture();
        let mut proposal = row(3, None, None, 0);
        if oversized {
            proposal[2] = Value::Text("z".repeat(crate::relational::MAX_ROW_BYTES + 1));
        } else {
            proposal.pop();
        }
        let mut calls = 0;
        let error = attempt(
            &mut session,
            &[proposal],
            &BoundConflict::Nothing,
            "email_unique",
            false,
            None,
            |_, epoch, _| *epoch += 1,
            &mut || {
                calls += 1;
                true
            },
        )
        .unwrap_err();
        assert_eq!(error, if oversized { Error::Limit } else { Error::Type });
        assert_eq!(calls, 0);
        assert!(session.is_aborted());
    }
}

#[test]
fn cancellation() {
    let (_, mut session) = fixture();
    assert_eq!(
        attempt(
            &mut session,
            &[row(3, None, None, 0)],
            &BoundConflict::Nothing,
            "email_unique",
            false,
            None,
            |_, _, _| {},
            &mut || true
        )
        .unwrap_err(),
        sql(SqlError::Aborted).into_operations()
    );
    assert!(session.is_aborted());
}

#[test]
fn returning_and_retained() {
    let (admin, mut session) = fixture();
    let before = actual(&mut session);
    let proposals = [
        row(3, Some("c"), None, 30),
        row(99, Some("a"), None, 11),
        row(98, Some("b"), None, 20),
    ];
    let result = run(&mut session, &proposals, &update(4)).unwrap();
    counters(&result, 3, 3, 2);
    assert_eq!(
        result.outcome.columns,
        ["tenant", "id", "email", "code", "n"]
    );
    assert_eq!(
        result
            .outcome
            .rows
            .iter()
            .map(|row| row.values[1].clone())
            .collect::<Vec<_>>(),
        [Value::Int(1), Value::Int(2), Value::Int(3)]
    );
    assert_eq!(
        result
            .outcome
            .rows
            .iter()
            .map(|row| row.version)
            .collect::<Vec<_>>(),
        [result.base, before[1].version, result.base + 1]
    );
    assert_eq!(
        result.outcome.conflict_results.unwrap(),
        vec![
            ConflictTupleOutcome {
                input_index: 0,
                status: ConflictStatus::Success,
                changed: true,
                returned_row: Some(2)
            },
            ConflictTupleOutcome {
                input_index: 1,
                status: ConflictStatus::Success,
                changed: true,
                returned_row: Some(0)
            },
            ConflictTupleOutcome {
                input_index: 2,
                status: ConflictStatus::Success,
                changed: false,
                returned_row: Some(1)
            },
        ]
    );
    session.rollback();
    session.execute("BEGIN", &[], None).unwrap();
    let mut retained = 0;
    assert_eq!(
        attempt(
            &mut session,
            &proposals,
            &update(4),
            "email_unique",
            true,
            Some(&mut retained),
            |_, _, _| {},
            &mut || false
        )
        .unwrap_err(),
        Error::Limit
    );
    assert!(session.is_aborted());
    assert_eq!(actual(&mut admin.sql_session()), before);
}

#[derive(Clone, Copy)]
enum TestArbiter<'a> {
    Unique(&'a str),
    PrimaryKey,
    AnyUnique,
}
fn sql_attempt(
    session: &mut SqlSession,
    rows: &[Vec<Value>],
    action: &BoundConflict,
    target_name: TestArbiter<'_>,
    semantics: Semantics,
) -> Result<Observed> {
    let fence = session.batch_fence_engine().map_err(core)?;
    let mut planner_error = None;
    let result = session.apply_batch_engine(fence, &mut || false, |src, apply| {
        let snapshot = src
            .secondary_access_engine(&src.tables["items"], src.epoch, || false)?
            .1
            .clone();
        let base = src.next_version_engine();
        let table = src.tables["items"].clone();
        let epoch = src.epoch;
        let target = src
            .secondary_state_engine(|| false)?
            .descriptors()
            .iter()
            .find(|descriptor| {
                descriptor.name
                    == match target_name {
                        TestArbiter::Unique(name) => name,
                        _ => "email_unique",
                    }
            })
            .unwrap()
            .clone();

        let projection = table
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| BoundProjection {
                expression: Bound::Field(index),
                column: column.clone(),
            })
            .collect::<Vec<_>>();
        let plan = match plan_unique_conflict(
            src,
            &table,
            epoch,
            match target_name {
                TestArbiter::Unique(_) => Arbiter::Unique(&target),
                TestArbiter::PrimaryKey => Arbiter::PrimaryKey,
                TestArbiter::AnyUnique => Arbiter::AnyUnique,
            },
            semantics,
            rows,
            action,
            Some(&projection),
            None,
            &mut || false,
        )
        .map_err(PlanError::into_operations)
        {
            Ok(plan) => plan,
            Err(error) => {
                assert_eq!(
                    src.secondary_access_engine(&src.tables["items"], src.epoch, || false)?
                        .1,
                    &snapshot
                );
                assert_eq!(src.next_version_engine(), base);
                planner_error = Some(error);
                return Err(SqlError::Type);
            }
        };
        let observed = Observed {
            changes: plan
                .changes
                .iter()
                .map(|change| {
                    (
                        change.new.0.clone(),
                        change.new.1.clone(),
                        change.old.map(|(key, _)| key.to_vec()),
                    )
                })
                .collect(),
            base: src.next_version_engine(),
            next: plan.next_version,
            outcome: plan.outcome.clone(),
        };
        let applied = apply.apply(&plan.batch(), plan.next_version);
        assert_eq!(
            src.secondary_access_engine(&src.tables["items"], src.epoch, || false)?
                .1,
            &snapshot
        );
        assert_eq!(src.next_version_engine(), base);
        applied?;
        Ok(observed)
    });
    if let Some(error) = planner_error {
        return Err(error);
    }
    result.map(|(observed, _)| observed).map_err(core)
}
fn sql_run(
    session: &mut SqlSession,
    rows: &[Vec<Value>],
    action: &BoundConflict,
) -> Result<Observed> {
    sql_attempt(
        session,
        rows,
        action,
        TestArbiter::Unique("email_unique"),
        Semantics::Sql,
    )
}
fn statuses(observed: &Observed) -> Vec<ConflictStatus> {
    observed
        .outcome
        .conflict_results
        .as_ref()
        .unwrap()
        .iter()
        .map(|result| result.status)
        .collect()
}

#[test]
fn sql_pk_arbiter_do_update_and_do_nothing() {
    let (_, mut session) = fixture();
    let before = actual(&mut session);
    let result = sql_attempt(
        &mut session,
        &[
            row(1, Some("unused"), None, 10),
            row(3, Some("c"), Some("z"), 30),
        ],
        &update(4),
        TestArbiter::PrimaryKey,
        Semantics::Sql,
    )
    .unwrap();
    counters(&result, 2, 2, 2);
    assert_eq!(result.changes[0].2, Some(key(1)));
    assert_eq!(result.changes[0].1.values, before[0].values);
    assert!(result.changes[0].1.version > before[0].version);
    assert_eq!(actual(&mut session).len(), 3);
    let before = actual(&mut session);
    let result = sql_attempt(
        &mut session,
        &[row(1, None, None, 99)],
        &BoundConflict::Nothing,
        TestArbiter::PrimaryKey,
        Semantics::Sql,
    )
    .unwrap();
    counters(&result, 1, 0, 0);
    assert_eq!(statuses(&result), [ConflictStatus::Skipped]);
    assert_eq!(actual(&mut session), before);
    // The existing admission check rejects NULL in each NOT NULL PK component.
    for index in 0..2 {
        let (_, mut session) = fixture();
        let mut proposal = row(3, None, None, 30);
        proposal[index] = Value::Null;
        assert_eq!(
            sql_attempt(
                &mut session,
                &[proposal],
                &update(4),
                TestArbiter::PrimaryKey,
                Semantics::Sql
            )
            .unwrap_err(),
            Error::Sql { sqlstate: "23502".into() }
        );
        assert!(session.is_aborted());
    }
    // PK tuples of proposed inserts participate for both PK-capable arbiters.
    for arbiter in [TestArbiter::PrimaryKey, TestArbiter::AnyUnique] {
        let (_, mut session) = fixture();
        let result = sql_attempt(
            &mut session,
            &[row(3, Some("c"), None, 30), row(3, Some("d"), None, 40)],
            &BoundConflict::Nothing,
            arbiter,
            Semantics::Sql,
        )
        .unwrap();
        counters(&result, 2, 1, 1);
        assert_eq!(actual(&mut session).len(), 3);
    }
}

#[test]
fn sql_unique_arbiter_in_statement_duplicate_do_nothing() {
    let (_, mut session) = fixture();
    let result = sql_run(
        &mut session,
        &[
            row(3, Some("fresh"), None, 30),
            row(4, Some("fresh"), None, 40),
            row(5, Some("a"), None, 50),
            row(6, Some("a"), None, 60),
        ],
        &BoundConflict::Nothing,
    )
    .unwrap();
    counters(&result, 4, 1, 1);
    assert_eq!(
        statuses(&result),
        [
            ConflictStatus::Success,
            ConflictStatus::Skipped,
            ConflictStatus::Skipped,
            ConflictStatus::Skipped
        ]
    );
    assert_eq!(result.outcome.rows.len(), 1);
    let rows = actual(&mut session);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].values, row(3, Some("fresh"), None, 30));
}

#[test]
fn sql_do_update_affects_row_twice() {
    for email in ["a", "fresh"] {
        let (admin, mut session) = fixture();
        let before = actual(&mut session);
        assert_eq!(
            sql_run(
                &mut session,
                &[row(3, Some(email), None, 30), row(4, Some(email), None, 40),],
                &update(4)
            )
            .unwrap_err(),
            Error::DuplicateTarget
        );
        assert!(session.is_aborted());
        assert_eq!(actual(&mut admin.sql_session()), before);
    }
    // Even a guard-skipped source winner is claimed for this statement.
    let (_, mut session) = fixture();
    let action = BoundConflict::Update {
        guard: Bound::Value(Value::Bool(false)),
        values: vec![(4, incoming(4))],
    };
    assert_eq!(
        sql_run(
            &mut session,
            &[row(3, Some("a"), None, 30), row(4, Some("a"), None, 40),],
            &action
        )
        .unwrap_err(),
        Error::DuplicateTarget
    );
}

#[test]
fn sql_guard_false_skips() {
    let guard = || {
        Bound::Compare(
            Box::new(incoming(4)),
            Compare::Eq,
            Box::new(Bound::Value(Value::Int(1))),
        )
    };
    let overflow = || {
        Bound::Arithmetic(
            Box::new(Bound::Value(Value::Int(i64::MAX))),
            Arithmetic::Add,
            Box::new(Bound::Value(Value::Int(1))),
        )
    };
    let (_, mut session) = fixture();
    let before = actual(&mut session);
    let result = sql_run(
        &mut session,
        &[row(3, Some("a"), None, 0), row(4, Some("b"), None, 1)],
        &BoundConflict::Update {
            guard: guard(),
            values: vec![(4, incoming(4))],
        },
    )
    .unwrap();
    counters(&result, 2, 1, 1);
    assert_eq!(
        statuses(&result),
        [ConflictStatus::Skipped, ConflictStatus::Success]
    );
    let rows = actual(&mut session);
    assert_eq!(rows[0], before[0]);
    assert_eq!(rows[1].values[4], Value::Int(1));

    let (_, mut session) = fixture();
    assert_eq!(
        sql_run(
            &mut session,
            &[row(3, Some("a"), None, 0), row(4, Some("b"), None, 1),],
            &BoundConflict::Update {
                guard: guard(),
                values: vec![(4, overflow())]
            }
        )
        .unwrap_err(),
        Error::Overflow
    );

    let (_, mut session) = fixture();
    let before = actual(&mut session);
    let result = sql_run(
        &mut session,
        &[row(3, Some("a"), None, 0)],
        &BoundConflict::Update {
            guard: guard(),
            values: vec![(4, overflow())],
        },
    )
    .unwrap();
    counters(&result, 1, 0, 0);
    assert_eq!(statuses(&result), [ConflictStatus::Skipped]);
    assert_eq!(actual(&mut session), before);

    // A later guard error wins over an earlier RHS overflow: all guards run first.
    let (_, mut session) = fixture();
    let action = BoundConflict::Update {
        guard: Bound::Compare(
            Box::new(Bound::Arithmetic(
                Box::new(Bound::Value(Value::Int(1))),
                Arithmetic::Divide,
                Box::new(incoming(4)),
            )),
            Compare::Eq,
            Box::new(Bound::Value(Value::Int(1))),
        ),
        values: vec![(4, overflow())],
    };
    assert_eq!(
        sql_run(
            &mut session,
            &[row(3, Some("a"), None, 1), row(4, Some("b"), None, 0),],
            &action
        )
        .unwrap_err(),
        Error::DivisionByZero
    );
}

#[test]
fn sql_any_unique_do_nothing() {
    let (_, mut session) = fixture();
    let before = actual(&mut session);
    let result = sql_attempt(
        &mut session,
        &[
            row(1, Some("c"), Some("z"), 30),
            row(3, Some("a"), Some("z"), 30),
            row(4, Some("d"), Some("x"), 40),
            row(5, Some("e"), Some("v"), 50),
        ],
        &BoundConflict::Nothing,
        TestArbiter::AnyUnique,
        Semantics::Sql,
    )
    .unwrap();
    counters(&result, 4, 1, 1);
    assert_eq!(
        statuses(&result),
        [
            ConflictStatus::Skipped,
            ConflictStatus::Skipped,
            ConflictStatus::Skipped,
            ConflictStatus::Success
        ]
    );
    let rows = actual(&mut session);
    assert_eq!(&rows[..2], &before);
    assert_eq!(rows[2].values, row(5, Some("e"), Some("v"), 50));
    for (arbiter, semantics, action) in [
        (TestArbiter::AnyUnique, Semantics::Sql, update(4)),
        (
            TestArbiter::PrimaryKey,
            Semantics::Native,
            BoundConflict::Nothing,
        ),
        (
            TestArbiter::AnyUnique,
            Semantics::Native,
            BoundConflict::Nothing,
        ),
    ] {
        let (_, mut session) = fixture();
        assert_eq!(
            sql_attempt(&mut session, &[], &action, arbiter, semantics).unwrap_err(),
            Error::Unsupported
        );
    }
}

#[test]
fn sql_insert_pk_duplicate_left_to_publisher() {
    let (admin, mut session) = fixture();
    let before = actual(&mut session);
    // sql_attempt reaches apply.apply only after a successful plan; its exact
    // publisher error is propagated separately from planner errors.
    assert_eq!(
        sql_run(
            &mut session,
            &[row(3, Some("c"), None, 30), row(3, Some("d"), None, 40),],
            &update(4)
        )
        .unwrap_err(),
        sql(SqlError::Duplicate).into_operations()
    );
    assert!(session.is_aborted());
    assert_eq!(actual(&mut admin.sql_session()), before);
    session.rollback();
    assert_eq!(actual(&mut session), before);
}

#[test]
fn sql_null_arbiter() {
    for action in [BoundConflict::Nothing, update(4)] {
        let (_, mut session) = fixture();
        let result = sql_run(
            &mut session,
            &[row(3, None, None, 30), row(4, None, None, 40)],
            &action,
        )
        .unwrap();
        counters(&result, 2, 2, 2);
        assert_eq!(
            statuses(&result),
            [ConflictStatus::Success, ConflictStatus::Success]
        );
        let rows = actual(&mut session);
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[2].values, row(3, None, None, 30));
        assert_eq!(rows[3].values, row(4, None, None, 40));
    }
}

#[test]
fn sql_proposed_update_image_conflict() {
    let (admin, mut session) = fixture();
    session
        .execute("DELETE FROM items WHERE tenant=1 AND id=1", &[], None)
        .unwrap();
    session.execute("COMMIT", &[], None).unwrap();
    session.execute("BEGIN", &[], None).unwrap();
    let before = actual(&mut session);
    // Input 1 finds code y and proposes code x; input 2 has no source
    // winner for x but conflicts with that proposed post-update image.
    let action = BoundConflict::Update {
        guard: Bound::Value(Value::Bool(true)),
        values: vec![(3, Bound::Value(Value::Text("x".into())))],
    };
    assert_eq!(
        sql_attempt(
            &mut session,
            &[row(3, None, Some("y"), 30), row(4, None, Some("x"), 40),],
            &action,
            TestArbiter::Unique("code_unique"),
            Semantics::Sql
        )
        .unwrap_err(),
        Error::DuplicateTarget
    );
    assert!(session.is_aborted());
    assert_eq!(actual(&mut admin.sql_session()), before);
}
