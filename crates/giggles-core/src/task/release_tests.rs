//! R3-301: release types and the fail-closed plain exits.
use super::*;
use crate::operations::{Bindings, Request, Sort};
use crate::task::definition::TaskName;

fn admin() -> crate::storage::Admin {
    let root = crate::test_artifact_root();
    std::fs::create_dir_all(&root).unwrap();
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let path = root.join(format!("task-release-{:032x}", u128::from_be_bytes(token)));
    #[cfg(feature = "engine-rocksdb")]
    let engine = crate::storage::Engine::V2;
    #[cfg(not(feature = "engine-rocksdb"))]
    let engine = crate::storage::Engine::Legacy;
    let admin =
        crate::storage::Admin::create_engine(&path, crate::storage::Limits::default(), engine)
            .unwrap();
    // `deliveries` is reserved (relational/ident.rs), so the worked example's table is `shipments`.
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text)",
        &[],
        None,
    )
    .unwrap();
    sql.execute(
        "INSERT INTO shipments VALUES (1, 42, 'failed'), (2, 42, 'delivered'), (3, 7, 'failed')",
        &[],
        None,
    )
    .unwrap();
    admin
}

fn context() -> TaskContext {
    TaskContext {
        task: TaskObjectId(9),
        name: TaskName::new("inc-436").unwrap(),
        executor: "agent_exec".into(),
        bound_def_version: 1,
        credential_fingerprint: [7; 32],
    }
}

/// A session bound to a task. On v2 the task exists, because a task statement
/// that reads a table binds the task's visible rows (R3-305); tasks are v2 only,
/// so the legacy engine keeps the synthetic context.
fn task_session(admin: &crate::storage::Admin) -> SqlSession {
    let mut sql = admin.sql_session();
    #[cfg(feature = "engine-rocksdb")]
    let ctx = real_task(admin);
    #[cfg(not(feature = "engine-rocksdb"))]
    let ctx = context();
    sql.bind_task(ctx).unwrap();
    sql
}

#[cfg(feature = "engine-rocksdb")]
fn real_task(admin: &crate::storage::Admin) -> TaskContext {
    use crate::storage::roles::{NewRole, TablePrivilege};
    use crate::task::definition::{
        DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, WallClockMs,
    };
    // 2026-11-05 17:00:00 UTC.
    let now = WallClockMs(1_793_898_000_000);
    let roles = admin.roles();
    for (name, task_only) in [("incident_controller", false), ("agent_exec", true)] {
        roles.create(NewRole { name: name.into(), login: true, ..NewRole::default() }).unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    roles.grant_table("shipments", "incident_controller", &[TablePrivilege::Select]).unwrap();
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: "inc-436".into(),
                executor: "agent_exec".into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: vec!["id".into(), "status".into()],
                masked: Vec::new(),
                untrusted_text: Vec::new(),
                allowed_write: None,
                mutation_caps: MutationCaps::default(),
                disclosure_caps: DisclosureCaps { rows_read: 10, rows_returned: 10, bytes_returned: 4096 },
                limits: LoadLimits::default(),
                approval_threshold: 0,
                expires: "2026-11-05 17:30:00+00".into(),
            },
            now,
        )
        .unwrap();
    let sql = admin.sql_session();
    // Release checks expiry against the database clock: pin it to `now`.
    sql.v2_db().unwrap().set_wall_clock(std::sync::Arc::new(
        crate::storage::engine::commit::ManualClock::new(now.0 as u64),
    ));
    crate::task::context::open_context(&sql, &context().name, &"agent_exec".to_string(), [7; 32], now, 0)
        .unwrap()
}

fn ids(admin: &crate::storage::Admin) -> Vec<Vec<Value>> {
    admin
        .sql_session()
        .execute("SELECT id FROM shipments ORDER BY id", &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn row(values: &[i64]) -> SqlRow {
    SqlRow {
        values: values.iter().copied().map(Value::Int).collect(),
        version: 1,
    }
}

#[test]
fn no_task_release_is_move() {
    let admin = admin();
    let sql = admin.sql_session();
    // A released page is the producer's buffer itself: no copy, no allocation,
    // and no engine operation (no transaction, read, write or scan).
    let ops = crate::storage::engine::txn::engine_ops();
    for i in 0..10_000 {
        let rows = vec![row(&[i, i + 1])];
        let buffer = rows.as_ptr();
        let page = Unreleased::new(rows, sql.release_tag());
        assert!(!page.is_task());
        let released = sql.release_page(page).unwrap();
        assert_eq!(released.get().as_ptr(), buffer);
        assert_eq!(released.into_inner(), vec![row(&[i, i + 1])]);
        sql.still_valid().unwrap();
        sql.finish_task_statement(Ok(()));
    }
    assert_eq!(crate::storage::engine::txn::engine_ops(), ops);
    // The counter does count: one transaction with one read.
    #[cfg(feature = "engine-rocksdb")]
    {
        let db = sql.v2_db().unwrap();
        let mut txn = db.begin(crate::storage::Isolation::ReadCommitted);
        txn.get(crate::storage::Keyspace::Kernel, b"x").unwrap();
        assert!(crate::storage::engine::txn::engine_ops() >= ops + 2);
    }
    // A real exit outside a task session releases the same rows `execute` returns.
    let mut sql = admin.sql_session();
    let page = sql
        .execute_unreleased("SELECT id, status FROM shipments ORDER BY id", &[], None)
        .unwrap();
    assert!(!page.is_task());
    let released = sql.release_page(page).unwrap().into_inner();
    let plain = sql
        .execute("SELECT id, status FROM shipments ORDER BY id", &[], None)
        .unwrap();
    assert_eq!(released, plain);
    assert_eq!(released.row_count(), 3);
    sql.still_valid().unwrap();
    sql.finish_task_statement(Ok(()));
}

#[test]
fn plain_exit_fails_closed_in_task_session() {
    let admin = admin();
    let refused = CoreError::Task(TaskError::NotAllowedInTaskSession(StatementClass::Other));
    let mut sql = task_session(&admin);
    // SQL text, prepared, clocked and typed exits.
    assert_eq!(sql.execute("SELECT id FROM shipments", &[], None), Err(refused.clone()));
    let insert = admin
        .sql_session()
        .prepare("INSERT INTO shipments VALUES (4, 42, 'queued')")
        .unwrap();
    assert_eq!(sql.execute_prepared(&insert, &[], None), Err(refused.clone()));
    assert_eq!(
        sql.execute("INSERT INTO shipments VALUES (5, 42, 'queued')", &[], None),
        Err(refused.clone())
    );
    // Refused before anything ran: no row was written.
    assert_eq!(ids(&admin).len(), 3);
    let copy = admin.sql_session().prepare("COPY shipments FROM STDIN").ok();
    if let Some(copy) = copy {
        assert_eq!(sql.ingest_copy(&copy, vec![]), Err(refused.clone()));
    }
    assert_eq!(sql.begin_copy_input(), Err(refused.clone()));
    assert_eq!(ids(&admin).len(), 3);
    // The twin runs, but its page is tagged and cannot be unwrapped or released.
    let select = admin.sql_session().prepare("SELECT id FROM shipments").unwrap();
    #[cfg(feature = "engine-rocksdb")]
    {
        let page = sql.execute_prepared_unreleased(&select, &[], None).unwrap();
        assert!(page.is_task());
        assert_eq!(format!("{page:?}"), "Unreleased { task: true, .. }");
        assert_eq!(page.untasked(), Err(refused.clone()));
    }
    // The synthetic legacy context names no catalog task: the column rules at
    // statement start refuse it as revoked before anything runs.
    #[cfg(not(feature = "engine-rocksdb"))]
    assert_eq!(
        sql.execute_prepared_unreleased(&select, &[], None).map(|_| ()),
        Err(CoreError::Task(TaskError::Revoked))
    );
    // R3-308: the twin's page is released through the protocol. On v2 the
    // task's visible rows (merchant 42) come out; the synthetic legacy
    // context names no catalog task, so its page is refused as revoked.
    #[cfg(feature = "engine-rocksdb")]
    let page = sql.execute_unreleased("SELECT id FROM shipments ORDER BY id", &[], None).unwrap();
    #[cfg(feature = "engine-rocksdb")]
    assert_eq!(
        sql.release_page(page)
            .unwrap()
            .into_inner()
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    #[cfg(not(feature = "engine-rocksdb"))]
    assert_eq!(
        sql.execute_unreleased("SELECT id FROM shipments ORDER BY id", &[], None).map(|_| ()),
        Err(CoreError::Task(TaskError::Revoked))
    );
    // Cursors: the plain fetch is refused before the cursor advances. (A legacy
    // cursor refuses a task session at open: tasks are v2 only.)
    #[cfg(feature = "engine-rocksdb")]
    {
        let mut cursor = sql.open_cursor(&select, &[]).unwrap();
        assert_eq!(cursor.fetch(10, 1 << 20), Err(refused.clone()));
        let chunk = cursor.fetch_unreleased(10, 1 << 20).unwrap();
        assert!(chunk.is_task());
        assert_eq!(chunk.untasked(), Err(refused.clone()));
    }
    // Native exits.
    let read = Request::Read {
        table: "shipments".into(),
        filter: None,
        order: vec![Sort { field: "id".into(), descending: false }],
        limit: None,
        projection: vec![],
    };
    let bindings = Bindings::new();
    let native = crate::operations::Error::Core(refused.to_string());
    assert_eq!(sql.execute_native(&read, &bindings), Err(native.clone()));
    assert_eq!(sql.read_page(&read, None, &bindings), Err(native.clone()));
    // The native twin binds under the task's column rules (R3-304). On v2 the
    // session holds a real task (R3-305), so the twin runs and its page is
    // tagged; the synthetic legacy context names no catalog task, so it is
    // refused as revoked.
    #[cfg(feature = "engine-rocksdb")]
    {
        let page = sql.execute_native_unreleased(&read, &bindings).unwrap();
        assert!(page.is_task());
        assert_eq!(page.untasked(), Err(refused.clone()));
    }
    #[cfg(not(feature = "engine-rocksdb"))]
    {
        let error = TaskError::Revoked;
        assert_eq!(error.sqlstate(), "28T01");
        assert_eq!(
            sql.execute_native_unreleased(&read, &bindings).map(|_| ()),
            Err(crate::operations::Error::Task(error))
        );
    }
    // Outside a task session the same calls work.
    let mut plain = admin.sql_session();
    assert_eq!(plain.execute("SELECT id FROM shipments", &[], None).unwrap().rows.len(), 3);
    let mut cursor = plain.open_cursor(&select, &[]).unwrap();
    assert_eq!(cursor.fetch(10, 1 << 20).unwrap().len(), 3);
    assert_eq!(plain.execute_native(&read, &bindings).unwrap().rows.len(), 3);
}

/// The compile-time half; the `compile_fail` doctests in `release.rs` show
/// the missing field, accessor, `Clone` and `Serialize`.
#[test]
fn unreleased_has_no_accessors() {
    // Each line compiles only while the trait is NOT implemented: with an
    // implementation the `_` parameter becomes ambiguous.
    trait NotImpl<A> {
        fn check() {}
    }
    impl<T: ?Sized> NotImpl<()> for T {}
    struct IsClone;
    struct IsSerialize;
    struct IsDeref;
    struct IsCopy;
    impl<T: Clone> NotImpl<IsClone> for T {}
    impl<T: ?Sized + serde::Serialize> NotImpl<IsSerialize> for T {}
    impl<T: ?Sized + std::ops::Deref> NotImpl<IsDeref> for T {}
    impl<T: Copy> NotImpl<IsCopy> for T {}
    <Unreleased<SqlResult> as NotImpl<_>>::check();
    <Unreleased<Vec<SqlRow>> as NotImpl<_>>::check();
    <Unreleased<Outcome> as NotImpl<_>>::check();
    <Unreleased<TransactionOutcome> as NotImpl<_>>::check();
    // Debug never shows the value.
    let page = Unreleased::new(vec![row(&[1234567])], None);
    let text = format!("{page:?}");
    assert_eq!(text, "Unreleased { task: false, .. }");
    assert!(!text.contains("1234567"));
}

#[test]
fn synthetic_round_trip() {
    let result = SqlResult {
        columns: vec!["setting".into()],
        rows: vec![SqlRow { values: vec![Value::Text("on".into())], version: 0 }],
        affected: 0,
    };
    let released = Released::synthetic(result.clone());
    assert_eq!(released.get(), &result);
    assert_eq!(released.clone().into_inner(), result);
    let mapped = released.map(|r| r.rows.len());
    assert_eq!(mapped.into_inner(), 1);
    // Every page shape visits its rows, a native scalar emit as a one-value row.
    let mut outcome = TransactionOutcome {
        summaries: vec![],
        emitted: vec![
            EmittedValue::Scalar(Value::Int(1)),
            EmittedValue::Row { columns: vec![], row: row(&[2, 3]) },
            EmittedValue::Rows { columns: vec![], rows: vec![row(&[4]), row(&[5])], complete: true },
        ],
    };
    assert_eq!(outcome.row_count(), 4);
    outcome.for_each_row_mut(&mut |values| values.iter_mut().for_each(|v| *v = Value::Null));
    let mut seen = Vec::new();
    outcome.for_each_row(&mut |values| seen.push(values.len()));
    assert_eq!(seen, [1, 2, 1, 1]);
    let mut nulls = 0;
    outcome.for_each_row(&mut |values| nulls += values.iter().filter(|v| **v == Value::Null).count());
    assert_eq!(nulls, 5);
    let native = Outcome { rows: vec![row(&[1]), row(&[2])], ..Outcome::default() };
    assert_eq!(native.row_count(), 2);
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows.row_count(), 1);
}

/// R3-308 (R3-301 open item): a write in a task session runs only inside the
/// protected commit. The twins refuse it before it runs: nothing is written.
#[test]
fn task_write_refused_before_it_runs() {
    let admin = admin();
    let mut sql = task_session(&admin);
    let insert = CoreError::Task(TaskError::NotAllowedInTaskSession(StatementClass::Insert));
    assert_eq!(
        sql.execute_unreleased("INSERT INTO shipments VALUES (4, 42, 'queued')", &[], None)
            .map(|_| ()),
        Err(insert)
    );
    let update = admin
        .sql_session()
        .prepare("UPDATE shipments SET status = 'queued' WHERE id = 1")
        .unwrap();
    assert_eq!(
        sql.execute_prepared_unreleased(&update, &[], None).map(|_| ()),
        Err(CoreError::Task(TaskError::NotAllowedInTaskSession(StatementClass::Update)))
    );
    for ddl in ["DELETE FROM shipments", "CREATE TABLE carriers (id bigint primary key)", "DROP TABLE shipments"] {
        let e = sql.execute_unreleased(ddl, &[], None).map(|_| ()).unwrap_err();
        assert_eq!(crate::sqlstate(&e), "0A000", "{ddl}: {e:?}");
    }
    // A cursor over a write never opens a write either.
    let e = sql.open_cursor(&update, &[]).map(|_| ()).unwrap_err();
    assert_ne!(crate::sqlstate(&e), "00000");
    // Native writes: refused after the column rules, before anything runs.
    #[cfg(feature = "engine-rocksdb")]
    {
        use crate::operations::{Expr, Intent, ValueExpr};
        let request = Request::Update {
            table: "shipments".into(),
            filter: Expr::Value(ValueExpr::Literal(Value::Bool(true))),
            guard: None,
            values: vec![("status".into(), ValueExpr::Literal(Value::Text("queued".into())))],
            intent: Intent::Many { cap: ValueExpr::Literal(Value::Int(10)) },
        };
        let e = sql.execute_native_unreleased(&request, &Bindings::new()).map(|_| ()).unwrap_err();
        assert!(matches!(e.sqlstate(), Some("42501" | "0A000")), "{e:?}");
    }
    // Nothing was written.
    let statuses = admin
        .sql_session()
        .execute("SELECT id, status FROM shipments ORDER BY id", &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect::<Vec<_>>();
    assert_eq!(
        statuses,
        vec![
            vec![Value::Int(1), Value::Text("failed".into())],
            vec![Value::Int(2), Value::Text("delivered".into())],
            vec![Value::Int(3), Value::Text("failed".into())],
        ]
    );
}
