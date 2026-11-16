//! Task catalog store (GD-0061 R3-105).
// Test names carry the SQLSTATE, e.g. `writable_pk_0A000`.
#![allow(non_snake_case)]
use super::*;
use crate::{
    storage::roles::{NewRole, TablePrivilege},
    Admin, Engine, Limits,
};
use std::path::PathBuf;

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 2026-11-05 17:00:00 UTC, the worked example's incident time.
const NOW: WallClockMs = WallClockMs(1_793_898_000_000);

fn open(engine: Engine) -> (Directory, Admin) {
    let mut id = [0; 16];
    getrandom::fill(&mut id).unwrap();
    let dir = Directory(
        std::env::temp_dir().join(format!("task-store-{:032x}", u128::from_be_bytes(id))),
    );
    let admin = Admin::create_engine(&dir.0, Limits::default(), engine).unwrap();
    (dir, admin)
}

fn sql(admin: &Admin, statement: &str) {
    admin
        .sql_session()
        .execute(statement, &[], None)
        .unwrap_or_else(|e| panic!("{statement}: {e:?}"));
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
    dir: Directory,
    admin: Admin,
    issuer: CatalogRole,
}

fn fixture() -> Fixture {
    let (dir, admin) = open(Engine::V2);
    sql(
        &admin,
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
         failed_at timestamp, attempt_count int, customer_email text, card_token text, \
         tracking_code text UNIQUE)",
    );
    // D7: rewriting shipments.tracking_code cascades into shipment_events.
    sql(
        &admin,
        "CREATE TABLE shipment_events (id bigint primary key, tracking_code text \
         REFERENCES shipments (tracking_code) ON UPDATE CASCADE)",
    );
    sql(&admin, "CREATE SEQUENCE attempts_seq");
    sql(
        &admin,
        "ALTER TABLE shipments ALTER COLUMN attempt_count SET DEFAULT nextval('attempts_seq')",
    );
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
    Fixture { dir, admin, issuer }
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|c| c.to_string()).collect()
}

/// The worked example (design-1 section 5) on `shipments`.
fn inc_436() -> NewTask {
    NewTask {
        name: "inc-436".into(),
        executor: "repair_agent".into(),
        approver: Some("oncall_lead".into()),
        schema: None,
        table: "shipments".into(),
        visible_rows: PredicateText("merchant_id = 42".into()),
        readable: names(&[
            "id",
            "status",
            "failed_at",
            "attempt_count",
            "tracking_code",
        ]),
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

fn err(result: Result<impl std::fmt::Debug, TaskDdlError>, sqlstate: &str) {
    match result {
        Err(e) => assert_eq!(e.sqlstate, sqlstate, "{e}"),
        Ok(v) => panic!("expected {sqlstate}, got {v:?}"),
    }
}

fn with_write(column: &str) -> NewTask {
    let mut new = inc_436();
    if let Some(write) = &mut new.allowed_write {
        write.columns = [column.to_string()].into_iter().collect();
    }
    new
}

#[test]
fn create_inc_436_and_reload() {
    let f = fixture();
    let def = f.admin.tasks().create(&f.issuer, inc_436(), NOW).unwrap();
    assert_eq!(def.def_version, 1);
    assert_eq!(def.issuer, "incident_controller");
    assert_eq!(def.executor, "repair_agent");
    assert_eq!(def.approver.as_deref(), Some("oncall_lead"));
    assert_eq!(def.visible_rows, PredicateText("merchant_id = 42".into()));
    assert_eq!(
        def.columns.get("customer_email"),
        Some(&ColumnMode::Masked { constant: None })
    );
    assert_eq!(def.columns.get("status"), Some(&ColumnMode::Readable));
    assert!(!def.columns.contains_key("card_token"));
    // 17:30 UTC is half an hour after NOW.
    assert_eq!(def.expires_at, WallClockMs(NOW.0 + 1_800_000));
    assert!(!def.revoked && def.grants.is_empty());
    assert_ne!(def.id, TaskObjectId(0));
    let read = f.admin.tasks().get("inc-436").unwrap().unwrap();
    assert_eq!(*read, def);
    assert_eq!(
        f.admin.tasks().get_by_id(def.id).unwrap().unwrap().name,
        def.name
    );
    let path = f.dir.0.clone();
    drop(f.admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let reloaded = admin.tasks().get("inc-436").unwrap().unwrap();
    assert_eq!(*reloaded, def);
    assert_eq!(admin.tasks().list().unwrap().len(), 1);
}

#[test]
fn duplicate_name_42710() {
    let f = fixture();
    f.admin.tasks().create(&f.issuer, inc_436(), NOW).unwrap();
    err(f.admin.tasks().create(&f.issuer, inc_436(), NOW), "42710");
}

#[test]
fn executor_not_task_only_22023() {
    let f = fixture();
    role(&f.admin, "plain", false);
    let mut new = inc_436();
    new.executor = "plain".into();
    err(f.admin.tasks().create(&f.issuer, new, NOW), "22023");
}

#[test]
fn issuer_is_executor_22023() {
    let f = fixture();
    let mut new = inc_436();
    new.executor = "incident_controller".into();
    err(f.admin.tasks().create(&f.issuer, new, NOW), "22023");
}

#[test]
fn approver_missing_when_threshold_below_cap_22023() {
    let f = fixture();
    let mut new = inc_436();
    new.approver = None;
    err(f.admin.tasks().create(&f.issuer, new, NOW), "22023");
    // At the cap every write is within the threshold: no approver needed.
    let mut new = inc_436();
    new.approver = None;
    new.approval_threshold = 25;
    f.admin.tasks().create(&f.issuer, new, NOW).unwrap();
}

#[test]
fn issuer_without_update_42501() {
    let f = fixture();
    let other = role(&f.admin, "bystander", false);
    err(f.admin.tasks().create(&other, inc_436(), NOW), "42501");
    // SELECT alone is enough for a read-only task, not for a writing one.
    f.admin
        .roles()
        .grant_table("shipments", "bystander", &[TablePrivilege::Select])
        .unwrap();
    err(f.admin.tasks().create(&other, inc_436(), NOW), "42501");
    let mut read_only = inc_436();
    read_only.allowed_write = None;
    read_only.mutation_caps = MutationCaps::default();
    read_only.approval_threshold = 0;
    f.admin.tasks().create(&other, read_only, NOW).unwrap();
}

#[test]
fn owner_may_issue() {
    let f = fixture();
    let owner = role(&f.admin, "table_owner", false);
    f.admin
        .roles()
        .set_table_owner("shipments", "table_owner")
        .unwrap();
    f.admin.tasks().create(&owner, inc_436(), NOW).unwrap();
}

#[test]
fn unknown_column_42703() {
    let f = fixture();
    let mut new = inc_436();
    new.readable.push("no_such_column".into());
    err(f.admin.tasks().create(&f.issuer, new, NOW), "42703");
    let mut new = inc_436();
    new.masked.push(("status".into(), None));
    err(f.admin.tasks().create(&f.issuer, new, NOW), "42701");
}

#[test]
fn writable_pk_0A000() {
    let f = fixture();
    err(
        f.admin.tasks().create(&f.issuer, with_write("id"), NOW),
        "0A000",
    );
}

#[test]
fn writable_sequence_default_0A000() {
    let f = fixture();
    err(
        f.admin
            .tasks()
            .create(&f.issuer, with_write("attempt_count"), NOW),
        "0A000",
    );
}

#[test]
fn writable_fk_cascade_target_0A000() {
    let f = fixture();
    // `tracking_code` is referenced by shipment_events ON UPDATE CASCADE (D7).
    err(
        f.admin
            .tasks()
            .create(&f.issuer, with_write("tracking_code"), NOW),
        "0A000",
    );
}

#[test]
fn nondeterministic_predicate_0A000() {
    let f = fixture();
    let mut new = inc_436();
    new.visible_rows = PredicateText("failed_at < now()".into());
    err(f.admin.tasks().create(&f.issuer, new, NOW), "0A000");
}

#[test]
fn expiry_in_past_22023() {
    let f = fixture();
    let mut new = inc_436();
    new.expires = "2026-11-05 16:59:59+00".into();
    err(f.admin.tasks().create(&f.issuer, new, NOW), "22023");
    let mut new = inc_436();
    new.expires = "2026-11-05 17:30:00".into();
    err(f.admin.tasks().create(&f.issuer, new, NOW), "22007");
}

#[test]
fn revoke_bumps_def_version_once() {
    let f = fixture();
    f.admin.tasks().create(&f.issuer, inc_436(), NOW).unwrap();
    let first = f.admin.tasks().revoke(&f.issuer, "inc-436").unwrap();
    assert!(first.revoked);
    assert_eq!(first.def_version, 2);
    let again = f.admin.tasks().revoke(&f.issuer, "inc-436").unwrap();
    assert_eq!(again.def_version, 2);
    assert_eq!(
        f.admin.tasks().get("inc-436").unwrap().unwrap().def_version,
        2
    );
    let bystander = role(&f.admin, "bystander", false);
    err(f.admin.tasks().revoke(&bystander, "inc-436"), "42501");
    err(f.admin.tasks().revoke(&f.issuer, "no-such-task"), "42704");
}

#[test]
fn grant_does_not_bump_def_version() {
    let f = fixture();
    role(&f.admin, "auditor", false);
    f.admin.tasks().create(&f.issuer, inc_436(), NOW).unwrap();
    f.admin
        .tasks()
        .grant(
            &f.issuer,
            "inc-436",
            "auditor",
            TaskPrivileges::INSPECT,
            true,
        )
        .unwrap();
    let def = f.admin.tasks().get("inc-436").unwrap().unwrap();
    assert_eq!(def.def_version, 1);
    assert_eq!(def.grants.get("auditor"), Some(&TaskPrivileges::INSPECT));
    f.admin
        .tasks()
        .grant(
            &f.issuer,
            "inc-436",
            "auditor",
            TaskPrivileges::INSPECT,
            false,
        )
        .unwrap();
    let def = f.admin.tasks().get("inc-436").unwrap().unwrap();
    assert!(def.grants.is_empty());
    assert_eq!(def.def_version, 1);
    err(
        f.admin.tasks().grant(
            &f.issuer,
            "inc-436",
            "auditor",
            TaskPrivileges::APPROVE,
            true,
        ),
        "0A000",
    );
}

#[test]
fn grant_to_executor_rejected() {
    let f = fixture();
    f.admin.tasks().create(&f.issuer, inc_436(), NOW).unwrap();
    err(
        f.admin.tasks().grant(
            &f.issuer,
            "inc-436",
            "repair_agent",
            TaskPrivileges::INSPECT,
            true,
        ),
        "22023",
    );
}

#[test]
fn purge_frees_the_name() {
    let f = fixture();
    let def = f.admin.tasks().create(&f.issuer, inc_436(), NOW).unwrap();
    f.admin.tasks().revoke(&f.issuer, "inc-436").unwrap();
    f.admin.tasks().purge(def.id).unwrap();
    assert!(f.admin.tasks().get("inc-436").unwrap().is_none());
    let again = f.admin.tasks().create(&f.issuer, inc_436(), NOW).unwrap();
    assert_ne!(again.id, def.id);
}

#[test]
fn check_against_follows_the_table() {
    let f = fixture();
    let def = f.admin.tasks().create(&f.issuer, inc_436(), NOW).unwrap();
    let TaskCatalog { db, .. } = f.admin.tasks();
    let db = db.unwrap();
    let mut txn = db.begin(Isolation::ReadCommitted);
    let view = db.catalog().committed_view(&mut txn).unwrap();
    def.check_against(&view).unwrap();
    let mut missing = def.clone();
    missing.columns.insert("gone".into(), ColumnMode::Readable);
    assert_eq!(missing.check_against(&view), Err(SqlError::UnknownColumn));
    let mut cascade = def.clone();
    if let Some(write) = &mut cascade.allowed_write {
        write.columns.insert("tracking_code".into());
    }
    assert_eq!(cascade.check_against(&view), Err(SqlError::Unsupported));
}

#[test]
fn legacy_engine_0A000() {
    let (_dir, admin) = open(Engine::Legacy);
    let issuer = CatalogRole {
        name: "x".into(),
        superuser: true,
        ..template_role()
    };
    err(admin.tasks().create(&issuer, inc_436(), NOW), "0A000");
    err(admin.tasks().get("inc-436"), "0A000");
    err(admin.tasks().list(), "0A000");
    err(admin.tasks().revoke(&issuer, "inc-436"), "0A000");
    err(
        admin
            .tasks()
            .grant(&issuer, "inc-436", "y", TaskPrivileges::UNDO, true),
        "0A000",
    );
}

fn template_role() -> CatalogRole {
    let (_dir, admin) = open(Engine::V2);
    role(&admin, "template", false)
}

#[test]
fn two_concurrent_creates_same_name_one_wins() {
    let f = fixture();
    let results: Vec<_> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let tasks = f.admin.tasks();
                let issuer = f.issuer.clone();
                scope.spawn(move || tasks.create(&issuer, inc_436(), NOW))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    let loser = results.iter().find_map(|r| r.as_ref().err()).unwrap();
    assert_eq!(loser.sqlstate, "42710");
    assert_eq!(f.admin.tasks().list().unwrap().len(), 1);
}

#[test]
fn parsed_statement_creates_the_same_task() {
    let f = fixture();
    let sql = "CREATE TASK \"inc-436\" EXECUTOR repair_agent APPROVER oncall_lead ON shipments \
         VISIBLE ROWS WHERE (merchant_id = 42) \
         READABLE (id, status, failed_at, attempt_count, tracking_code) MASKED (customer_email) \
         ALLOW UPDATE (status) OLD ROW WHERE (status = 'failed') NEW ROW WHERE (status = 'queued') \
         AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS APPROVE ABOVE 5 \
         DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
         EXPIRES '2026-11-05 17:30:00+00'";
    let Some(super::super::statements::TaskStatement::Create(parsed)) =
        super::super::statements::parse(sql)
    else {
        panic!("expected CREATE TASK");
    };
    assert_eq!(*parsed, inc_436());
    let def = f.admin.tasks().create(&f.issuer, *parsed, NOW).unwrap();
    assert_eq!(def.approval_threshold, 5);
}
