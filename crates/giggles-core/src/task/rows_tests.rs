//! R3-305: scoped row source, read through the crate-internal page split so the
//! returned rows themselves can be compared.
#![cfg(feature = "engine-rocksdb")]
use super::*;
use crate::relational::Value;
use crate::storage::roles::{NewRole, TablePrivilege};
use crate::storage::{Admin, Engine, Limits, SqlSession};
use crate::task::context::open_context;
use crate::task::definition::{
    DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskName, WallClockMs,
};

const NOW: WallClockMs = WallClockMs(1_793_898_000_000);

fn admin() -> (std::path::PathBuf, Admin) {
    let root = crate::test_artifact_root();
    std::fs::create_dir_all(&root).unwrap();
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let path = root.join(format!("task-rows-{:032x}", u128::from_be_bytes(token)));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    for sql in [
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, attempt_count int)",
        "CREATE TABLE carriers (id bigint primary key)",
        "INSERT INTO shipments VALUES (1, 42, 1), (2, 7, 0), (3, 42, 2), (4, NULL, 0)",
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
    roles.grant_table("shipments", "issuer", &[TablePrivilege::Select]).unwrap();
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
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: vec!["id".into(), "attempt_count".into()],
                masked: Vec::new(),
                untrusted_text: Vec::new(),
                allowed_write: None,
                mutation_caps: MutationCaps::default(),
                disclosure_caps: DisclosureCaps { rows_read: 10, rows_returned: 10, bytes_returned: 4096 },
                limits: LoadLimits::default(),
                approval_threshold: 0,
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
        let error = if cursor {
            let p = s.prepare(sql).unwrap();
            let mut c = s.open_cursor(&p, &[]).unwrap();
            c.fetch_unreleased(10, 4096).map(|_| ()).unwrap_err()
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
