//! Task-only role attribute (GD-0061 R3-103, design section 7).
use super::{
    Backend,
    engine::{content_check, meta::TASK_CATALOG},
    roles::{CatalogRole, NewRole, RoleChanges, RoleError},
};
use crate::{
    Admin, CoreError, Engine, Limits,
    task::definition::{TaskOnlyViolation, check_task_only},
};
use std::path::PathBuf;

const ROLES: u64 = super::roles::engine::FEATURE;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let mut id = [0; 16];
        getrandom::fill(&mut id).unwrap();
        Self(std::env::temp_dir().join(format!("task-only-{:032x}", u128::from_be_bytes(id))))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn v2() -> (Directory, Admin) {
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    (dir, admin)
}

fn role(name: &str) -> NewRole {
    NewRole { name: name.into(), login: true, ..NewRole::default() }
}

fn features(admin: &Admin) -> u64 {
    let Backend::V2(db) = &admin.inner.backend else { panic!("expected v2") };
    super::engine::meta::validate_meta(db.kv.as_ref()).unwrap().features
}

fn invalid<T: std::fmt::Debug>(result: Result<T, RoleError>) {
    assert!(matches!(result, Err(RoleError::Invalid)), "{result:?}");
}

#[test]
fn existing_role_bytes_unchanged() {
    let role = CatalogRole {
        inherit: true,
        name: "reader".into(),
        oid: 16384,
        superuser: false,
        login: true,
        createrole: false,
        password: None,
        generation: 1,
        task_only: false,
    };
    const FROZEN: &str = r#"{"name":"reader","oid":16384,"superuser":false,"login":true,"createrole":false,"password":null,"generation":1}"#;
    assert_eq!(serde_json::to_string(&role).unwrap(), FROZEN);
    assert_eq!(serde_json::from_str::<CatalogRole>(FROZEN).unwrap(), role);
    let flagged = CatalogRole { task_only: true, ..role };
    let json = serde_json::to_string(&flagged).unwrap();
    assert_eq!(json, FROZEN.replace(r#""generation":1}"#, r#""generation":1,"task_only":true}"#));
    assert_eq!(serde_json::from_str::<CatalogRole>(&json).unwrap(), flagged);
    // Ordinary roles still need only the role-catalog bit.
    let key = [b"\x08reader".as_slice(), b"\0"].concat();
    let value = [b"\x01".as_slice(), FROZEN.as_bytes()].concat();
    assert_eq!(content_check::required_bits_for_catalog_value(&key, &value).unwrap(), ROLES);
    let value = [b"\x01".as_slice(), json.as_bytes()].concat();
    assert_eq!(
        content_check::required_bits_for_catalog_value(&key, &value).unwrap(),
        ROLES | TASK_CATALOG
    );
}

#[test]
fn set_task_only_requires_bit_and_reopens() {
    let (dir, admin) = v2();
    let roles = admin.roles();
    let created = roles.create(role("agent")).unwrap();
    assert!(!roles.is_task_only("agent").unwrap());
    assert_eq!(features(&admin) & TASK_CATALOG, 0);
    let changed = roles.set_task_only("agent", true).unwrap();
    assert!(changed.task_only);
    assert_eq!(changed.generation, created.generation + 1);
    assert_eq!(features(&admin) & (TASK_CATALOG | ROLES), TASK_CATALOG | ROLES);
    assert!(matches!(roles.set_task_only("absent", true), Err(RoleError::Missing)));
    assert!(matches!(roles.is_task_only("absent"), Err(RoleError::Missing)));
    drop(roles);
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    let roles = admin.roles();
    assert!(roles.is_task_only("agent").unwrap());
    assert_eq!(roles.get("agent").unwrap().unwrap(), changed);
    // Clearing keeps the (monotone) bit and survives reopen too.
    let cleared = roles.set_task_only("agent", false).unwrap();
    assert!(!cleared.task_only);
    assert_eq!(features(&admin) & TASK_CATALOG, TASK_CATALOG);
    drop(roles);
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    assert!(!admin.roles().is_task_only("agent").unwrap());
}

#[test]
fn task_only_superuser_rejected() {
    let (_dir, admin) = v2();
    let roles = admin.roles();
    roles.create(NewRole { superuser: true, ..role("root") }).unwrap();
    invalid(roles.set_task_only("root", true));
    assert!(!roles.is_task_only("root").unwrap());
    assert_eq!(features(&admin) & TASK_CATALOG, 0);
}

#[test]
fn task_only_createrole_rejected() {
    let (_dir, admin) = v2();
    let roles = admin.roles();
    roles.create(NewRole { createrole: true, ..role("admin") }).unwrap();
    invalid(roles.set_task_only("admin", true));
    roles.create(role("agent")).unwrap();
    roles.set_task_only("agent", true).unwrap();
    invalid(roles.alter("agent", RoleChanges { createrole: Some(true), ..RoleChanges::default() }));
    assert!(!roles.get("agent").unwrap().unwrap().createrole);
}

#[test]
fn task_only_cannot_own_table() {
    let (_dir, admin) = v2();
    let roles = admin.roles();
    roles.create(role("agent")).unwrap();
    roles.create(role("owner")).unwrap();
    roles.set_table_owner("deliveries", "owner").unwrap();
    roles.set_task_only("agent", true).unwrap();
    invalid(roles.set_table_owner("deliveries", "agent"));
    assert_eq!(roles.table_owner("deliveries").unwrap().as_deref(), Some("owner"));
    // An owner cannot become task-only.
    invalid(roles.set_task_only("owner", true));
    assert!(!roles.is_task_only("owner").unwrap());
}

#[test]
fn task_only_cannot_join_or_have_members() {
    let (_dir, admin) = v2();
    let roles = admin.roles();
    for name in ["agent", "group", "member"] {
        roles.create(role(name)).unwrap();
    }
    roles.set_task_only("agent", true).unwrap();
    invalid(roles.grant_membership("group", "agent"));
    invalid(roles.grant_membership("agent", "member"));
    assert!(roles.memberships("agent").unwrap().is_empty());
    assert!(roles.memberships("member").unwrap().is_empty());
    // Existing memberships on either side block the attribute.
    roles.set_task_only("agent", false).unwrap();
    roles.grant_membership("group", "agent").unwrap();
    invalid(roles.set_task_only("agent", true));
    roles.revoke_membership("group", "agent").unwrap();
    roles.grant_membership("agent", "member").unwrap();
    invalid(roles.set_task_only("agent", true));
    roles.revoke_membership("agent", "member").unwrap();
    roles.set_task_only("agent", true).unwrap();
}

#[test]
fn alter_to_superuser_rejected_for_task_only() {
    let (_dir, admin) = v2();
    let roles = admin.roles();
    roles.create(role("agent")).unwrap();
    let before = roles.set_task_only("agent", true).unwrap();
    invalid(roles.alter("agent", RoleChanges { superuser: Some(true), ..RoleChanges::default() }));
    assert_eq!(roles.get("agent").unwrap().unwrap(), before);
    // LOGIN and a password remain allowed.
    let changed = roles
        .alter(
            "agent",
            RoleChanges {
                login: Some(true),
                password: Some(Some("SCRAM-SHA-256$opaque-verifier".into())),
                ..RoleChanges::default()
            },
        )
        .unwrap();
    assert!(changed.task_only);
}

#[test]
fn legacy_engine_refuses() {
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::Legacy).unwrap();
    let roles = admin.roles();
    roles.create(role("agent")).unwrap();
    assert!(matches!(
        roles.set_task_only("agent", true),
        Err(RoleError::Core(CoreError::Unsupported))
    ));
    assert!(!roles.is_task_only("agent").unwrap());
}

#[test]
fn check_task_only_matrix() {
    let role = |task_only| CatalogRole {
        inherit: true,
        name: "r".into(),
        oid: 16384,
        superuser: false,
        login: true,
        createrole: false,
        password: None,
        generation: 1,
        task_only,
    };
    assert_eq!(check_task_only(&role(true), true), Ok(()));
    assert_eq!(check_task_only(&role(false), false), Ok(()));
    assert_eq!(
        check_task_only(&role(true), false),
        Err(TaskOnlyViolation::TaskOnlyRoleOutsideTask)
    );
    assert_eq!(
        check_task_only(&role(false), true),
        Err(TaskOnlyViolation::OrdinaryRoleInTask)
    );
}
