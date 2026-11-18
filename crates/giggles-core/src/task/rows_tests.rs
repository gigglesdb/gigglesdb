//! R3-305: scoped row source, read through the crate-internal page split so the
//! returned rows themselves can be compared.
#![cfg(feature = "engine-rocksdb")]
use super::*;
use crate::relational::Value;
use crate::storage::roles::{NewRole, TablePrivilege};
use crate::storage::{Admin, Engine, Limits, SqlSession};
use crate::task::context::open_context;
use crate::task::definition::{
    AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskName, WallClockMs,
};

const NOW: WallClockMs = WallClockMs(1_793_898_000_000);

fn admin() -> (std::path::PathBuf, Admin) {
    scoped_admin(false, "merchant_id = 42", LoadLimits::default())
}

fn scoped_admin(prefix: bool, predicate: &str, limits: LoadLimits) -> (std::path::PathBuf, Admin) {
    let root = crate::test_artifact_root();
    std::fs::create_dir_all(&root).unwrap();
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let path = root.join(format!("task-rows-{:032x}", u128::from_be_bytes(token)));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    for sql in [
        if prefix { "CREATE TABLE shipments (id bigint, merchant_id bigint, attempt_count int, PRIMARY KEY (merchant_id, id))" }
        else { "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, attempt_count int)" },
        "CREATE TABLE carriers (id bigint primary key)",
        if prefix { "INSERT INTO shipments VALUES (1, 42, 1), (2, 7, 0), (3, 42, 2), (4, 9, 0)" }
        else { "INSERT INTO shipments VALUES (1, 42, 1), (2, 7, 0), (3, 42, 2), (4, NULL, 0)" },
        "INSERT INTO carriers VALUES (1)",
    ] {
        admin.sql_session().execute(sql, &[], None).unwrap();
    }
    let roles = admin.roles();
    for (name, task_only) in [("issuer", false), ("agent", true)] {
        roles
            .create(NewRole { name: name.into(), login: true, ..NewRole::default() })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    roles.grant_table("shipments", "issuer", &[TablePrivilege::Select, TablePrivilege::Update]).unwrap();
    let issuer = roles.get("issuer").unwrap().unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: "inc-436".into(),
                executor: "agent".into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText(predicate.into()),
                readable: vec!["id".into(), "attempt_count".into()],
                masked: Vec::new(),
                untrusted_text: Vec::new(),
                // R3-304 applies the column rules to native writes, so the
                // native UPDATE in `update_and_native_outcomes_count_only_scope_rows`
                // needs a writable column.
                allowed_write: Some(AllowedWrite {
                    columns: ["attempt_count".to_string()].into_iter().collect(),
                    old_row: PredicateText("attempt_count >= 0".into()),
                    new_row: PredicateText("attempt_count >= 0".into()),
                }),
                mutation_caps: MutationCaps { distinct_rows: 25, transitions: 25 },
                disclosure_caps: DisclosureCaps { rows_read: 10, rows_returned: 10, bytes_returned: 4096 },
                limits,
                approval_threshold: 25,
                expires: "2026-11-05 17:30:00+00".into(),
            },
            NOW,
        )
        .unwrap();
    (path, admin)
}

fn agent(admin: &Admin) -> SqlSession {
    let mut s = admin.sql_session();
    let ctx = open_context(&s, &TaskName::new("inc-436").unwrap(), &"agent".to_string(), [1; 32], NOW, 0)
        .unwrap();
    s.bind_task(ctx).unwrap();
    s
}

fn ids(rows: &[crate::relational::SqlRow]) -> Vec<Value> {
    rows.iter().map(|row| row.values[0].clone()).collect()
}

#[test]
fn scoped_rows_on_cursor_and_buffered_paths() {
    let (path, admin) = admin();
    let expected = vec![Value::Int(1), Value::Int(3)];
    for sql in ["SELECT id FROM shipments", "SELECT id FROM shipments ORDER BY id"] {
        let mut s = agent(&admin);
        let (result, tag) = s.execute_unreleased(sql, &[], None).unwrap().into_parts();
        assert!(tag.is_some());
        assert_eq!(ids(&result.rows), expected, "buffered {sql}");
        let p = s.prepare(sql).unwrap();
        let mut cursor = s.open_cursor(&p, &[]).unwrap();
        let mut rows = Vec::new();
        while !cursor.is_exhausted() {
            rows.extend(cursor.fetch_unreleased(1, 4096).unwrap().into_parts().0);
        }
        assert_eq!(ids(&rows), expected, "cursor {sql}");
    }
    let mut s = agent(&admin);
    let (result, _) = s
        .execute_unreleased("SELECT count(*) FROM shipments", &[], None)
        .unwrap()
        .into_parts();
    assert_eq!(result.rows[0].values[0], Value::Int(2));
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn other_table_is_denied() {
    let (path, admin) = admin();
    for cursor in [false, true] {
        let mut s = agent(&admin);
        let sql = "SELECT id FROM carriers";
        // The column rules refuse another table at prepare (design-3 call
        // site 1), before any cursor opens or row is read.
        let error = if cursor {
            s.prepare(sql).map(|_| ()).unwrap_err()
        } else {
            s.execute_unreleased(sql, &[], None).map(|_| ()).unwrap_err()
        };
        assert!(
            matches!(error, CoreError::Task(TaskError::Denied(DenyRule::OtherTable))),
            "cursor={cursor}: {error:?}"
        );
    }
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn predicate_cache_keyed_by_catalog_version() {
    let (path, admin) = admin();
    let read = || {
        let mut s = agent(&admin);
        let _ = s.execute_unreleased("SELECT id FROM shipments", &[], None).unwrap();
        let p = s.prepare("SELECT id FROM shipments").unwrap();
        let _ = s.open_cursor(&p, &[]).unwrap();
    };
    read();
    let first = BINDS.get();
    assert!(first >= 1);
    for _ in 0..3 {
        read();
    }
    assert_eq!(BINDS.get(), first, "cached per (task, def_version, catalog version)");
    // A catalog change is a new version: the predicate is bound again.
    admin.sql_session().execute("CREATE TABLE other (id int primary key)", &[], None).unwrap();
    read();
    assert!(BINDS.get() > first);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn changed_definition_is_refused() {
    let (path, admin) = admin();
    let mut s = admin.sql_session();
    let mut ctx = open_context(&s, &TaskName::new("inc-436").unwrap(), &"agent".to_string(), [1; 32], NOW, 0)
        .unwrap();
    ctx.bound_def_version += 1;
    s.bind_task(ctx).unwrap();
    let error = s
        .execute_unreleased("SELECT id FROM shipments", &[], None)
        .map(|_| ())
        .unwrap_err();
    assert!(
        matches!(error, CoreError::Task(TaskError::Denied(DenyRule::DefinitionChanged))),
        "{error:?}"
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

/// R3-306: the values behind the integration tests' probes. A task UPDATE,
/// SQL or native, matches and affects only the visible rows (1 and 3).
#[test]
fn update_and_native_outcomes_count_only_scope_rows() {
    use crate::operations::{Bindings, Compare, Expr, Intent, Request, ValueExpr};
    let (path, admin) = admin();
    let literal = |value: i64| ValueExpr::Literal(Value::Int(value));
    // A task session refuses writes outside the protected commit (R3-308);
    // this test lifts that on its thread to see the scoped results.
    let refused = agent(&admin)
        .execute_unreleased("UPDATE shipments SET attempt_count = 5", &[], None)
        .map(|_| ())
        .unwrap_err();
    assert_eq!(crate::sqlstate(&refused), "0A000");
    crate::task::release::allow_task_writes_for_test(true);
    // SQL, outside the protected commit: the statement result is scoped too.
    let (result, tag) = agent(&admin)
        .execute_unreleased("UPDATE shipments SET attempt_count = attempt_count + 1", &[], None)
        .unwrap()
        .into_parts();
    assert!(tag.is_some());
    assert_eq!(result.affected, 2);
    let update = Request::Update {
        table: "shipments".into(),
        filter: Expr::Compare(
            Box::new(Expr::Field("attempt_count".into())),
            Compare::Ge,
            Box::new(Expr::Value(literal(0))),
        ),
        guard: None,
        values: vec![("attempt_count".into(), literal(9))],
        intent: Intent::Many { cap: literal(10) },
    };
    let (outcome, tag) = agent(&admin)
        .execute_native_unreleased(&update, &Bindings::new())
        .unwrap()
        .into_parts();
    assert!(tag.is_some());
    assert_eq!((outcome.matched, outcome.affected), (2, 2));
    // An invisible key is not found, and a single-row intent whose filter
    // also matches an invisible row matches exactly the visible one.
    let update_where = |op: Compare, id: i64| Request::Update {
        table: "shipments".into(),
        filter: Expr::Compare(
            Box::new(Expr::Field("id".into())),
            op,
            Box::new(Expr::Value(literal(id))),
        ),
        guard: None,
        values: vec![("attempt_count".into(), literal(9))],
        intent: Intent::One,
    };
    assert_eq!(
        agent(&admin)
            .execute_native_unreleased(&update_where(Compare::Eq, 2), &Bindings::new())
            .map(|_| ()),
        Err(crate::operations::Error::NotFound)
    );
    let one = update_where(Compare::Le, 2);
    let (outcome, _) = agent(&admin)
        .execute_native_unreleased(&one, &Bindings::new())
        .unwrap()
        .into_parts();
    assert_eq!((outcome.matched, outcome.affected), (1, 1));
    crate::task::release::allow_task_writes_for_test(false);
    let read = Request::Read {
        table: "shipments".into(),
        filter: None,
        order: Vec::new(),
        limit: Some(literal(1024)),
        projection: Vec::new(),
    };
    let (outcome, _) = agent(&admin)
        .execute_native_unreleased(&read, &Bindings::new())
        .unwrap()
        .into_parts();
    assert_eq!(ids(&outcome.rows), [Value::Int(1), Value::Int(3)]);
    assert_eq!(outcome.matched, 2);
    // Outside the task every row is there, and only 1 and 3 changed.
    let all = admin
        .sql_session()
        .execute("SELECT id, attempt_count FROM shipments ORDER BY id", &[], None)
        .unwrap()
        .rows;
    let counts: Vec<_> = all.iter().map(|row| row.values[1].clone()).collect();
    assert_eq!(counts, [Value::Int(9), Value::Int(0), Value::Int(9), Value::Int(0)]);
    let outcome = admin.sql_session().execute_native(&update, &Bindings::new()).unwrap();
    assert_eq!(outcome.matched, 4);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn prefix_scan_counts_only_its_range_and_non_key_still_hits_limit() {
    for (predicate, succeeds) in [("merchant_id = 42", true), ("attempt_count >= 0", false)] {
        let (path, admin) = scoped_admin(true, predicate, LoadLimits {
            rows_scanned: 2, ..LoadLimits::default()
        });
        // Even a statement index must not bypass the primary-key scope.
        admin.sql_session().execute("CREATE INDEX attempts ON shipments(attempt_count)", &[], None).unwrap();
        let mut s = agent(&admin);
        let result = s.execute_unreleased("SELECT id FROM shipments WHERE attempt_count >= 0", &[], None);
        if succeeds {
            assert_eq!(ids(&result.unwrap().into_parts().0.rows), vec![Value::Int(1), Value::Int(3)]);
            assert_eq!(take_last_counts().unwrap().scanned, 2);
        } else {
            assert!(matches!(result, Err(CoreError::Task(TaskError::BudgetExceeded(
                crate::task::Budget::RowsScanned
            )))), "{result:?}");
        }
        drop(s);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
