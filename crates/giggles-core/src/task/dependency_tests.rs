//! R3-106 table guard at the catalog seam, for a column type change. SQL
//! `ALTER COLUMN ... TYPE` is 0A000 at this base, so the rule is driven
//! through `apply_diff` and `check_table_ddl` directly.
#![allow(non_snake_case)]
use super::*;
use crate::storage::V2Db;
use crate::{
    storage::roles::{NewRole, TablePrivilege},
    Admin, Engine, Limits,
};

struct Directory(std::path::PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn database(schema: &str) -> (Directory, Admin, CatalogRole) {
    let mut id = [0; 16];
    getrandom::fill(&mut id).unwrap();
    let dir = Directory(
        crate::test_artifact_root().join(format!("task-deps-{:032x}", u128::from_be_bytes(id))),
    );
    std::fs::create_dir_all(&dir.0).unwrap();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    if schema != "public" {
        session
            .execute(&format!("CREATE SCHEMA {schema}"), &[], None)
            .unwrap();
    }
    session
        .execute(
            &format!(
                "CREATE TABLE {schema}.shipments (id bigint primary key, merchant_id bigint, \
                 status text, customer_email text, note text)"
            ),
            &[],
            None,
        )
        .unwrap();
    let roles = admin.roles();
    for (name, task_only) in [("incident_controller", false), ("agent_exec", true)] {
        roles
            .create(NewRole {
                name: name.into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    roles
        .grant_table(
            &crate::relational::relation_key(schema, "shipments"),
            "incident_controller",
            &[TablePrivilege::Select],
        )
        .unwrap();
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    (dir, admin, issuer)
}

fn task() -> NewTask {
    NewTask {
        name: "inc-436".into(),
        executor: "agent_exec".into(),
        approver: None,
        schema: None,
        table: "shipments".into(),
        visible_rows: PredicateText("merchant_id = 42".into()),
        readable: vec!["id".into(), "status".into()],
        masked: vec![("customer_email".into(), None)],
        untrusted_text: Vec::new(),
        allowed_write: None,
        mutation_caps: MutationCaps::default(),
        disclosure_caps: DisclosureCaps {
            rows_read: 10,
            rows_returned: 10,
            bytes_returned: 4096,
        },
        limits: LoadLimits::default(),
        approval_threshold: 0,
        expires: "2026-11-05 17:30:00+00".into(),
    }
}

const NOW: WallClockMs = WallClockMs(1_793_898_000_000);

/// `old` and a copy with `column` of shipments retyped to varchar (same id).
fn retyped(db: &V2Db, txn: &mut Transaction, column: &str) -> (CatalogParts, CatalogParts) {
    let old = db.catalog().parts(txn).unwrap();
    let mut new = old.clone();
    let table = new.tables.get_mut("shipments").unwrap();
    let def = table.columns.iter_mut().find(|c| c.name == column).unwrap();
    assert_eq!(def.data_type, DataType::Text);
    def.data_type = DataType::Varchar;
    (old, new)
}

fn assert_ddl_waits_for_first_task(
    admin: &Admin,
    issuer: CatalogRole,
    new_task: NewTask,
    expect_lock_wait: bool,
    ddl: impl FnOnce() -> Result<(), CoreError> + Send + 'static,
) {
    use std::{sync::mpsc, thread, time::Duration};

    let task_name = new_task.name.clone();
    let observer = admin.test_v2().begin(Isolation::ReadCommitted);
    let hook = crate::CommitSyncHook::before_publish();
    admin.set_commit_sync_hook(Some(hook.clone()));
    let writer = thread::spawn({
        let admin = admin.clone();
        move || admin.tasks().create(&issuer, new_task, NOW)
    });
    let entered = hook.wait_entered(Duration::from_secs(10));
    let (starting, started) = mpsc::channel();
    let ddl_worker = thread::spawn(move || {
        starting.send(()).unwrap();
        ddl()
    });
    let ddl_started = started.recv_timeout(Duration::from_secs(10)).is_ok();
    // SQL can wait for the catalog cache before reaching the lock manager.
    let deadline = Instant::now() + Duration::from_secs(1);
    while observer.sql_waiting_count() == 0
        && !ddl_worker.is_finished()
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(1));
    }
    let ddl_waiting = !ddl_worker.is_finished();
    let lock_waiting = observer.sql_waiting_count() > 0;
    hook.release(false);
    let created = writer.join().unwrap().unwrap();
    let result = ddl_worker.join().unwrap();
    admin.set_commit_sync_hook(None);

    assert!(entered, "task writer never reached the publication hook");
    assert!(ddl_started, "DDL worker never started");
    assert!(
        ddl_waiting,
        "DDL did not wait during publication pause: {result:?}"
    );
    if expect_lock_wait {
        assert!(lock_waiting, "DDL never waited for the task-catalog lock");
    }
    let error = result.expect_err("DDL broke a live task");
    assert!(
        matches!(crate::sqlstate(&error), "2BP01" | "40001"),
        "{error:?}"
    );
    let persisted = admin.tasks().get(&task_name).unwrap().unwrap();
    assert_eq!(persisted.id, created.id);
    assert!(!persisted.revoked);
}

#[test]
fn unpublished_first_task_blocks_ddl_at_the_task_guard() {
    let (_dir, admin, issuer) = database("public");
    let db = admin.test_v2();
    let mut txn = db.begin(Isolation::ReadCommitted);
    let (old, new) = retyped(&db, &mut txn, "customer_email");
    // Enter the guard directly so a table or role lock cannot mask a skipped probe.
    assert_ddl_waits_for_first_task(&admin, issuer, task(), true, move || {
        check_table_ddl(&mut txn, &*db.kv, &old, &new, None, &|| false)
    });
}

#[test]
fn unpublished_first_task_blocks_table_ddl() {
    let (_dir, admin, issuer) = database("public");
    let mut session = admin.sql_session();
    assert_ddl_waits_for_first_task(&admin, issuer, task(), false, move || {
        session
            .execute(
                "ALTER TABLE shipments DROP COLUMN customer_email",
                &[],
                None,
            )
            .map(|_| ())
    });
}

#[test]
fn unpublished_first_task_blocks_schema_cascade() {
    let (_dir, admin, issuer) = database("shipping");
    let mut new_task = task();
    new_task.schema = Some("shipping".into());
    let mut session = admin.sql_session();
    assert_ddl_waits_for_first_task(&admin, issuer, new_task, false, move || {
        session
            .execute("DROP SCHEMA shipping CASCADE", &[], None)
            .map(|_| ())
    });
}

#[test]
fn retyping_masked_column_is_2BP01_and_unreferenced_passes() {
    let (_dir, admin, issuer) = database("public");
    let tasks = admin.tasks();
    tasks.create(&issuer, task(), NOW).unwrap();
    let db = tasks.db().unwrap();

    let mut txn = db.begin(Isolation::ReadCommitted);
    let (old, new) = retyped(&db, &mut txn, "customer_email");
    let refused = db
        .catalog()
        .apply_diff(&mut txn, &old, &new, &(0..0), None, &|| false);
    assert!(
        matches!(refused, Err(CoreError::Sql(SqlError::DependentObjects))),
        "{refused:?}"
    );
    drop(txn);

    let mut txn = db.begin(Isolation::ReadCommitted);
    let (old, new) = retyped(&db, &mut txn, "note");
    check_table_ddl(&mut txn, &*db.kv, &old, &new, None, &|| false).unwrap();
    drop(txn);

    // A revoked task no longer blocks the change.
    tasks.revoke(&issuer, "inc-436").unwrap();
    let mut txn = db.begin(Isolation::ReadCommitted);
    let (old, new) = retyped(&db, &mut txn, "customer_email");
    check_table_ddl(&mut txn, &*db.kv, &old, &new, None, &|| false).unwrap();
}

#[test]
fn task_committed_after_ddl_snapshot_is_40001() {
    let (_dir, admin, issuer) = database("public");
    let tasks = admin.tasks();
    let db = tasks.db().unwrap();
    // A task on another table makes kind 19 non-empty, so the slow path runs.
    let mut txn = db.begin(Isolation::ReadCommitted);
    let _ = txn.snapshot_ts();
    tasks.create(&issuer, task(), NOW).unwrap();
    let (old, mut new) = retyped(&db, &mut txn, "note");
    new.tables.remove("shipments");
    let result = check_table_ddl(&mut txn, &*db.kv, &old, &new, None, &|| false);
    assert_eq!(result.map_err(|e| crate::sqlstate(&e)), Err("40001"));
}
