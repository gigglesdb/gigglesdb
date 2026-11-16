//! GD-0061 R3-106: dependency guards for table DDL and role changes
//! (design-1 section 8). Names carry the SQLSTATE.
#![cfg(feature = "engine-rocksdb")]
#![allow(non_snake_case)]
mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits,
    roles::{CatalogRole, NewRole, RoleError, TablePrivilege},
    task::{
        catalog::TaskDdlError,
        definition::{
            AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText,
            TaskDef, TaskPrivileges, WallClockMs,
        },
    },
};
use std::{path::PathBuf, sync::Arc};

/// 2026-11-05 17:00:00 UTC.
const NOW: WallClockMs = WallClockMs(1_793_898_000_000);

const TABLE: &str = "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, \
    status text, failed_at timestamp, attempt_count int, customer_email text, \
    card_token text, note text)";

fn run(admin: &Admin, sql: &str) -> Result<(), CoreError> {
    admin.sql_session().execute(sql, &[], None).map(|_| ())
}

fn ok(admin: &Admin, sql: &str) {
    run(admin, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

fn refused(admin: &Admin, sql: &str, sqlstate: &str) {
    match run(admin, sql) {
        Err(e) => assert_eq!(giggles_core::sqlstate(&e), sqlstate, "{sql}: {e:?}"),
        Ok(()) => panic!("{sql}: expected {sqlstate}"),
    }
}

fn role(admin: &Admin, name: &str, task_only: bool) -> CatalogRole {
    let roles = admin.roles();
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
    roles.get(name).unwrap().unwrap()
}

struct Fixture {
    path: PathBuf,
    admin: Option<Admin>,
    issuer: CatalogRole,
}

impl Fixture {
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }

    /// Closes the database and opens it again.
    fn reopen(&mut self) {
        self.admin = None;
        self.admin = Some(Admin::open(&self.path, Limits::default()).unwrap());
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin = None;
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// `shipments` plus an issuer holding SELECT and UPDATE on it, a task-only
/// executor and an approver. No task yet.
fn fixture(name: &str) -> Fixture {
    let path = common::fresh(name);
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    ok(&admin, TABLE);
    let issuer = role(&admin, "incident_controller", false);
    role(&admin, "repair_agent", true);
    role(&admin, "oncall_lead", false);
    admin
        .roles()
        .grant_table(
            "shipments",
            "incident_controller",
            &[TablePrivilege::Select, TablePrivilege::Update],
        )
        .unwrap();
    Fixture { path, admin: Some(admin), issuer }
}

/// References id, status, attempt_count (readable), customer_email (masked)
/// and merchant_id (visible-rows predicate only). failed_at, card_token and
/// note are unreferenced.
fn new_task(name: &str, table: &str) -> NewTask {
    NewTask {
        name: name.into(),
        executor: "repair_agent".into(),
        approver: Some("oncall_lead".into()),
        schema: None,
        table: table.into(),
        visible_rows: PredicateText("merchant_id = 42".into()),
        readable: ["id", "status", "attempt_count"].map(String::from).to_vec(),
        masked: vec![("customer_email".into(), None)],
        untrusted_text: Vec::new(),
        allowed_write: Some(AllowedWrite {
            columns: ["status".to_string()].into_iter().collect(),
            old_row: PredicateText("status = 'failed'".into()),
            new_row: PredicateText("status = 'queued'".into()),
        }),
        mutation_caps: MutationCaps {
            distinct_rows: 25,
            transitions: 25,
        },
        disclosure_caps: DisclosureCaps {
            rows_read: 1000,
            rows_returned: 1000,
            bytes_returned: 2_097_152,
        },
        limits: LoadLimits::default(),
        approval_threshold: 5,
        expires: "2026-11-05 17:30:00+00".into(),
    }
}

fn with_task(name: &str) -> (Fixture, TaskDef) {
    let f = fixture(name);
    let def = f
        .admin()
        .tasks()
        .create(&f.issuer, new_task("inc-436", "shipments"), NOW)
        .unwrap();
    (f, def)
}

fn revoke(f: &Fixture) {
    f.admin().tasks().revoke(&f.issuer, "inc-436").unwrap();
}

#[test]
fn drop_table_with_live_task_2BP01() {
    let (f, _) = with_task("taskdep-drop");
    refused(f.admin(), "DROP TABLE shipments", "2BP01");
    // Nothing changed: the table and the task are both still there.
    ok(f.admin(), "SELECT id FROM shipments");
    assert!(!f.admin().tasks().get("inc-436").unwrap().unwrap().revoked);
}

#[test]
fn drop_table_cascade_still_2BP01() {
    let (f, _) = with_task("taskdep-cascade");
    refused(f.admin(), "DROP TABLE shipments CASCADE", "2BP01");
    ok(f.admin(), "SELECT id FROM shipments");
}

#[test]
fn drop_table_after_revoke_ok_and_show_survives_reopen() {
    let (mut f, def) = with_task("taskdep-revoked");
    revoke(&f);
    ok(f.admin(), "DROP TABLE shipments");
    let shown = f.admin().tasks().get("inc-436").unwrap().unwrap();
    assert!(shown.revoked);
    assert_eq!(shown.table, def.table);
    // The open check accepts a revoked task whose table is gone.
    f.reopen();
    let listed = f.admin().tasks().list().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name.as_str(), "inc-436");
    assert!(listed[0].revoked);
    assert_eq!(listed[0].table, def.table);
}

#[test]
fn drop_referenced_column_2BP01() {
    let (f, _) = with_task("taskdep-dropcol");
    // Read only by the visible-rows predicate.
    refused(f.admin(), "ALTER TABLE shipments DROP COLUMN merchant_id", "2BP01");
    // Masked and readable columns.
    refused(f.admin(), "ALTER TABLE shipments DROP COLUMN customer_email", "2BP01");
    refused(f.admin(), "ALTER TABLE shipments DROP COLUMN attempt_count", "2BP01");
    ok(f.admin(), "SELECT merchant_id, customer_email, attempt_count FROM shipments");
}

#[test]
fn rename_predicate_column_2BP01() {
    let (f, _) = with_task("taskdep-rename");
    refused(
        f.admin(),
        "ALTER TABLE shipments RENAME COLUMN merchant_id TO merchant",
        "2BP01",
    );
    // Writable column, named by both write predicates.
    refused(f.admin(), "ALTER TABLE shipments RENAME COLUMN status TO state", "2BP01");
    // Renaming the table is allowed: the task holds the table id.
    ok(f.admin(), "ALTER TABLE shipments RENAME TO parcels");
    let def = f.admin().tasks().get("inc-436").unwrap().unwrap();
    assert!(!def.revoked);
    ok(f.admin(), "SELECT merchant_id FROM parcels");
}

#[test]
fn alter_type_of_masked_column_2BP01() {
    let (f, _) = with_task("taskdep-type");
    // `ALTER COLUMN ... TYPE` is not supported at this base (0A000), so SQL
    // cannot reach the guard; the rule itself (retyping a referenced column is
    // 2BP01, an unreferenced one passes) is covered at the catalog seam by
    // `task::catalog::dependency_tests`. Whatever the SQL layer returns, the
    // column keeps its type and the task stays live.
    match run(
        f.admin(),
        "ALTER TABLE shipments ALTER COLUMN customer_email TYPE varchar(200)",
    ) {
        Err(e) => assert!(
            ["0A000", "2BP01"].contains(&giggles_core::sqlstate(&e)),
            "{e:?}"
        ),
        Ok(()) => panic!("retyping a masked column must be refused"),
    }
    ok(f.admin(), "INSERT INTO shipments (id, customer_email) VALUES (1, 'a@b')");
    assert!(!f.admin().tasks().get("inc-436").unwrap().unwrap().revoked);
}

#[test]
fn add_column_ok_and_hidden() {
    let (f, _) = with_task("taskdep-add");
    ok(f.admin(), "ALTER TABLE shipments ADD COLUMN secret text");
    let def = f.admin().tasks().get("inc-436").unwrap().unwrap();
    // Unlisted columns are Hidden.
    assert!(!def.columns.contains_key("secret"));
    assert!(!def.untrusted_allowed.contains("secret"));
    assert!(!def.revoked);
}

#[test]
fn drop_unreferenced_column_ok() {
    let (f, _) = with_task("taskdep-dropfree");
    ok(f.admin(), "ALTER TABLE shipments DROP COLUMN card_token");
    ok(f.admin(), "ALTER TABLE shipments DROP COLUMN failed_at");
    ok(f.admin(), "ALTER TABLE shipments RENAME COLUMN note TO remark");
    assert!(!f.admin().tasks().get("inc-436").unwrap().unwrap().revoked);
}

fn role_refused(result: Result<impl std::fmt::Debug, RoleError>) {
    match result {
        Err(RoleError::Dependent) => {}
        other => panic!("expected RoleError::Dependent (2BP01), got {other:?}"),
    }
}

#[test]
fn drop_role_issuer_2BP01() {
    let (f, _) = with_task("taskdep-issuer");
    let roles = f.admin().roles();
    role_refused(roles.drop("incident_controller"));
    role_refused(roles.drop("repair_agent"));
    role_refused(roles.drop("oncall_lead"));
    assert!(roles.get("incident_controller").unwrap().is_some());
    // A role no task names still drops.
    role(f.admin(), "bystander", false);
    roles.drop("bystander").unwrap();
}

#[test]
fn drop_role_grantee_of_revoked_task_2BP01() {
    let (f, _) = with_task("taskdep-grantee");
    role(f.admin(), "auditor", false);
    f.admin()
        .tasks()
        .grant(&f.issuer, "inc-436", "auditor", TaskPrivileges::INSPECT, true)
        .unwrap();
    revoke(&f);
    let roles = f.admin().roles();
    // Revoked tasks still count: a re-created "auditor" would inherit INSPECT.
    role_refused(roles.drop("auditor"));
    role_refused(roles.drop("incident_controller"));
    assert!(roles.get("auditor").unwrap().is_some());
}

#[test]
fn notaskonly_on_executor_2BP01() {
    let (f, _) = with_task("taskdep-notaskonly");
    let roles = f.admin().roles();
    role_refused(roles.set_task_only("repair_agent", false));
    assert!(roles.is_task_only("repair_agent").unwrap());
    // Still refused once the task is revoked: the record names the executor.
    revoke(&f);
    role_refused(roles.set_task_only("repair_agent", false));
    // A task-only role that executes nothing can be cleared.
    role(f.admin(), "spare_agent", true);
    roles.set_task_only("spare_agent", false).unwrap();
    // Other attribute changes on the executor pass.
    roles.set_task_only("repair_agent", true).unwrap();
}

#[test]
fn ddl_racing_create_task_serializes() {
    let f = fixture("taskdep-race");
    for round in 0..4 {
        let table = format!("race{round}");
        ok(f.admin(), &TABLE.replace("shipments", &table));
        f.admin()
            .roles()
            .grant_table(
                &table,
                "incident_controller",
                &[TablePrivilege::Select, TablePrivilege::Update],
            )
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let task = {
            let (admin, issuer, barrier) = (f.admin().clone(), f.issuer.clone(), barrier.clone());
            let table = table.clone();
            std::thread::spawn(move || -> Result<TaskDef, TaskDdlError> {
                barrier.wait();
                admin.tasks().create(&issuer, new_task(&format!("race-{table}"), &table), NOW)
            })
        };
        let ddl = {
            let (admin, barrier) = (f.admin().clone(), barrier.clone());
            let sql = if round % 2 == 0 {
                format!("DROP TABLE {table}")
            } else {
                format!("ALTER TABLE {table} DROP COLUMN merchant_id")
            };
            std::thread::spawn(move || {
                barrier.wait();
                run(&admin, &sql)
            })
        };
        let created = task.join().unwrap();
        let altered = ddl.join().unwrap();
        match (&created, &altered) {
            // Task first: the DDL sees it (2BP01) or conflicts with it (40001).
            (Ok(_), Err(e)) => {
                let state = giggles_core::sqlstate(e);
                assert!(state == "2BP01" || state == "40001", "round {round}: {e:?}");
            }
            // DDL first: the task no longer validates.
            (Err(e), Ok(())) => {
                assert!(
                    ["42P01", "42703"].contains(&e.sqlstate),
                    "round {round}: {e:?}"
                );
            }
            other => panic!("round {round}: exactly one must win, got {other:?}"),
        }
        // The catalog is consistent: a live task's table and columns exist.
        if created.is_ok() {
            ok(f.admin(), &format!("SELECT merchant_id, customer_email FROM {table}"));
        }
    }
}

#[test]
fn database_without_tasks_ddl_unaffected() {
    let f = fixture("taskdep-none");
    ok(f.admin(), "ALTER TABLE shipments DROP COLUMN merchant_id");
    ok(f.admin(), "ALTER TABLE shipments RENAME COLUMN status TO state");
    ok(f.admin(), "ALTER TABLE shipments ADD COLUMN extra int");
    ok(f.admin(), "DROP TABLE shipments");
    f.admin().roles().drop("repair_agent").unwrap();
    // A task on one table does not block DDL on another.
    let (g, _) = with_task("taskdep-other");
    ok(g.admin(), "CREATE TABLE other (id int primary key, v text)");
    ok(g.admin(), "ALTER TABLE other DROP COLUMN v");
    ok(g.admin(), "DROP TABLE other");
}
