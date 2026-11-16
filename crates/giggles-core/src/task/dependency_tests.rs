//! R3-106 table guard at the catalog seam, for a column type change. SQL
//! `ALTER COLUMN ... TYPE` is 0A000 at this base, so the rule is driven
//! through `apply_diff` and `check_table_ddl` directly.
#![allow(non_snake_case)]
use super::*;
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

fn database() -> (Directory, Admin, CatalogRole) {
    let mut id = [0; 16];
    getrandom::fill(&mut id).unwrap();
    let dir = Directory(
        crate::test_artifact_root().join(format!("task-deps-{:032x}", u128::from_be_bytes(id))),
    );
    std::fs::create_dir_all(&dir.0).unwrap();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    admin
        .sql_session()
        .execute(
            "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
             customer_email text, note text)",
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
        .grant_table("shipments", "incident_controller", &[TablePrivilege::Select])
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

#[test]
fn retyping_masked_column_is_2BP01_and_unreferenced_passes() {
    let (_dir, admin, issuer) = database();
    let tasks = admin.tasks();
    tasks.create(&issuer, task(), NOW).unwrap();
    let db = Arc::clone(tasks.db().unwrap());

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
    let (_dir, admin, issuer) = database();
    let tasks = admin.tasks();
    let db = Arc::clone(tasks.db().unwrap());
    // A task on another table makes kind 19 non-empty, so the slow path runs.
    let mut txn = db.begin(Isolation::ReadCommitted);
    let _ = txn.snapshot_ts();
    tasks.create(&issuer, task(), NOW).unwrap();
    let (old, mut new) = retyped(&db, &mut txn, "note");
    new.tables.remove("shipments");
    let result = check_table_ddl(&mut txn, &*db.kv, &old, &new, None, &|| false);
    assert_eq!(result.map_err(|e| crate::sqlstate(&e)), Err("40001"));
}
