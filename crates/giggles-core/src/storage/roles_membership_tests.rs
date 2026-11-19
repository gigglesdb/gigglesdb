use super::*;
use crate::{Engine, Limits};

struct Directory(std::path::PathBuf);
impl Directory {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn database() -> (Directory, Admin) {
    let mut id = [0; 16];
    getrandom::fill(&mut id).unwrap();
    let dir = Directory(
        std::env::temp_dir().join(format!("membership-{:032x}", u128::from_be_bytes(id))),
    );
    let engine = if cfg!(feature = "engine-rocksdb") { Engine::V2 } else { Engine::Legacy };
    let admin = Admin::create_engine(dir.path(), Limits::default(), engine).unwrap();
    for name in ["postgres", "g", "u", "peer", "other"] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                superuser: name == "postgres",
                ..Default::default()
            })
            .unwrap();
    }
    (dir, admin)
}
fn raw(roles: &RoleCatalog) -> BTreeMap<Vec<u8>, Vec<u8>> {
    roles.read(|t| Ok(t.unwrap().0.clone())).unwrap()
}
fn bits(admin: &Admin) -> u64 {
    let super::super::Backend::V2(db) = &admin.inner.backend else {
        // Legacy refuses extended records and has no feature-bit metadata.
        return 0;
    };
    super::super::engine::meta::validate_meta(db.kv.as_ref())
        .unwrap()
        .features
}
const FEATURE: u64 = super::super::engine::meta::ROLE_MEMBERSHIP;

#[test]
fn predefined_reads_preserve_old_bytes_and_legacy_grants() {
    let (dir, admin) = database();
    let roles = admin.roles();
    roles.grant_membership("g", "u").unwrap();
    let before = raw(&roles);
    assert_eq!(before[&grant_key(b"member/", "g", "u")], Vec::<u8>::new());
    assert_eq!(bits(&admin) & FEATURE, 0);
    for (oid, name) in PREDEFINED {
        let r = roles.get(name).unwrap().unwrap();
        assert_eq!(r.oid, *oid);
        assert!(!r.login && !r.superuser && r.inherit);
        assert_eq!(roles.image().unwrap().get(name).unwrap(), Some(r));
    }
    assert_eq!(roles.list().unwrap().len(), 21);
    assert!(roles.is_member("pg_monitor", "pg_read_all_stats").unwrap());
    assert!(roles.can_set("pg_monitor", "pg_stat_scan_tables").unwrap());
    assert_eq!(raw(&roles), before);
    drop(roles);
    drop(admin);
    let admin = Admin::open(dir.path(), Limits::default()).unwrap();
    assert_eq!(raw(&admin.roles()), before);
    assert_eq!(bits(&admin) & FEATURE, 0);
}

#[test]
fn reserved_names_and_database_owner_and_cycles() {
    let (_dir, admin) = database();
    let r = admin.roles();
    assert!(matches!(
        r.create(NewRole {
            name: "pg_new".into(),
            ..Default::default()
        }),
        Err(RoleError::ReservedName)
    ));
    r.create(NewRole {
        name: "Pg_new".into(),
        ..Default::default()
    })
    .unwrap();
    assert!(matches!(
        r.alter("pg_monitor", Default::default()),
        Err(RoleError::ReservedAlter)
    ));
    assert!(matches!(r.drop("pg_monitor"), Err(RoleError::ReservedDrop)));
    assert!(matches!(
        r.grant_membership("pg_database_owner", "u"),
        Err(RoleError::DatabaseOwnerMembers)
    ));
    assert!(matches!(
        r.grant_membership("g", "pg_database_owner"),
        Err(RoleError::DatabaseOwnerMember)
    ));
    r.grant_membership("g", "u").unwrap();
    assert!(
        matches!(r.grant_membership_options("u", "g", "postgres", Default::default()), Err(RoleError::MembershipCycle { role, member }) if role == "u" && member == "g")
    );
    assert!(matches!(
        r.grant_membership_options("g", "g", "postgres", Default::default()),
        Err(RoleError::MembershipCycle { .. })
    ));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn inherit_defaults_options_and_multiple_grantors_roundtrip() {
    let (dir, admin) = database();
    let r = admin.roles();
    let old = raw(&r)[&key(b"role/", "u")].clone();
    assert!(!String::from_utf8(old.clone()).unwrap().contains("inherit"));
    r.alter(
        "u",
        RoleChanges {
            inherit: Some(false),
            ..Default::default()
        },
    )
    .unwrap();
    assert_ne!(bits(&admin) & FEATURE, 0);
    assert_eq!(
        r.grant_membership_options("g", "u", "postgres", Default::default())
            .unwrap(),
        MembershipGrant::Granted
    );
    r.alter(
        "u",
        RoleChanges {
            inherit: Some(true),
            ..Default::default()
        },
    )
    .unwrap();
    let e = r
        .edges()
        .unwrap()
        .into_iter()
        .find(|e| e.role == "g")
        .unwrap();
    assert!(!e.inherit && !e.admin && e.set);
    assert_eq!(
        r.grant_membership_options("g", "u", "postgres", Default::default())
            .unwrap(),
        MembershipGrant::Unchanged
    );
    assert_eq!(
        r.grant_membership_options(
            "g",
            "u",
            "postgres",
            MembershipOptions {
                admin: Some(true),
                ..Default::default()
            }
        )
        .unwrap(),
        MembershipGrant::Updated
    );
    r.grant_membership_options("g", "u", "other", Default::default())
        .unwrap();
    assert_eq!(
        r.edges().unwrap().iter().filter(|e| e.role == "g").count(),
        2
    );
    assert!(
        r.revoke_membership_options("g", "u", "postgres", Some(MembershipOption::Admin), false)
            .unwrap()
    );
    assert!(
        r.revoke_membership_options("g", "u", "postgres", Some(MembershipOption::Set), false)
            .unwrap()
    );
    assert!(
        !r.revoke_membership_options("g", "peer", "postgres", None, false)
            .unwrap()
    );
    let before = raw(&r);
    drop(r);
    drop(admin);
    let admin = Admin::open(dir.path(), Limits::default()).unwrap();
    assert_eq!(raw(&admin.roles()), before);
    assert!(admin.roles().get("u").unwrap().unwrap().inherit);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn dependency_restrict_cascade_and_drop_cleanup() {
    let (_dir, admin) = database();
    let r = admin.roles();
    r.grant_membership_options(
        "g",
        "u",
        "postgres",
        MembershipOptions {
            admin: Some(true),
            ..Default::default()
        },
    )
    .unwrap();
    r.grant_membership_options(
        "g",
        "peer",
        "u",
        MembershipOptions {
            admin: Some(true),
            ..Default::default()
        },
    )
    .unwrap();
    r.grant_membership_options("g", "other", "peer", Default::default())
        .unwrap();
    assert!(matches!(
        r.drop("u"),
        Err(RoleError::DependentMembership { .. })
    ));
    for option in [None, Some(MembershipOption::Admin)] {
        assert!(matches!(
            r.revoke_membership_options("g", "u", "postgres", option, false),
            Err(RoleError::DependentMembership { .. })
        ));
    }
    r.revoke_membership_options("g", "u", "postgres", Some(MembershipOption::Admin), true)
        .unwrap();
    assert_eq!(
        r.edges().unwrap().iter().filter(|e| e.role == "g").count(),
        1
    );
    r.drop("u").unwrap();
    assert!(!r.edges().unwrap().iter().any(|e| e.role == "g"));
    r.grant_membership("g", "peer").unwrap();
    r.drop("g").unwrap();
    assert!(!r.edges().unwrap().iter().any(|e| e.role == "g"));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn graph_queries_follow_each_option_without_superuser_shortcuts() {
    let (_dir, admin) = database();
    let r = admin.roles();
    r.grant_membership_options(
        "g",
        "u",
        "postgres",
        MembershipOptions {
            inherit: Some(false),
            ..Default::default()
        },
    )
    .unwrap();
    r.grant_membership_options(
        "peer",
        "g",
        "postgres",
        MembershipOptions {
            admin: Some(true),
            set: Some(false),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(r.is_member("u", "peer").unwrap());
    assert!(r.can_set("u", "g").unwrap());
    assert!(!r.can_set("u", "peer").unwrap());
    assert_eq!(r.privileges_of("u").unwrap(), BTreeSet::from(["u".into()]));
    assert!(!r.admin_of("u", "peer").unwrap());
    r.grant_membership_options(
        "g",
        "u",
        "postgres",
        MembershipOptions {
            inherit: Some(true),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(r.admin_of("u", "peer").unwrap());
    assert!(r.privileges_of("u").unwrap().contains("peer"));
    assert!(r.can_set("u", "u").unwrap());
    assert!(!r.is_member("postgres", "g").unwrap());
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn feature_witness_including_predefined_roles_and_old_reader_rejection() {
    let (_dir, admin) = database();
    let r = admin.roles();
    r.grant_membership("g", "u").unwrap();
    assert_eq!(bits(&admin) & FEATURE, 0);
    r.grant_membership("pg_monitor", "u").unwrap();
    assert_ne!(bits(&admin) & FEATURE, 0);
    let k = engine::encode_key(&grant_key(b"member/", "pg_monitor", "u")).unwrap();
    // Empty legacy payloads alone cannot distinguish stored from synthesized endpoints.
    // stage() and the open-time whole-catalog witness supply that context.
    assert_eq!(engine::membership_features(&k, &[1]), 0);
    assert!(super::super::engine::meta::check_features(1 << 26).is_err());
    // Frozen pre-change reader capability set (bits 0..=25 only).
    assert_ne!(bits(&admin) & !((1u64 << 26) - 1), 0);
}

#[test]
fn synthesized_edges_do_not_prevent_bootstrap_drop() {
    let (_dir, admin) = database();
    let roles = admin.roles();
    assert!(raw(&roles).keys().all(|key| !key.starts_with(b"member/")));
    roles.drop("postgres").unwrap();
    assert!(roles.get("postgres").unwrap().is_none());
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn public_acl_has_no_role_name_collision_and_is_feature_guarded() {
    let (dir, admin) = database(); let roles = admin.roles();
    roles.create(NewRole { name: "public".into(), ..Default::default() }).unwrap();
    roles.grant_table("t", "public", &[TablePrivilege::Insert]).unwrap();
    assert!(roles.table_privileges("t", "u").unwrap().is_empty());
    assert_eq!(bits(&admin) & FEATURE, 0);
    roles.change_public_table("t", &[TablePrivilege::Select], true).unwrap();
    assert_eq!(roles.table_privileges("t", "u").unwrap(), BTreeSet::from([TablePrivilege::Select]));
    assert_eq!(roles.table_privileges("t", "public").unwrap(), BTreeSet::from([TablePrivilege::Select, TablePrivilege::Insert]));
    assert_ne!(bits(&admin) & FEATURE, 0);
    roles.change_public_schema("s", &[SchemaPrivilege::Usage], true).unwrap();
    assert_eq!(roles.table_acl("t").unwrap()[""], BTreeSet::from([TablePrivilege::Select]));
    drop(roles); drop(admin);
    let admin = Admin::open(dir.path(), Limits::default()).unwrap();
    let roles = admin.roles();
    assert!(roles.schema_privileges("s", "u").unwrap().contains(&SchemaPrivilege::Usage));
    assert!(roles.table_privileges("t", "u").unwrap().contains(&TablePrivilege::Select));
    roles.change_public_table("t", &[TablePrivilege::Select], false).unwrap();
    assert!(roles.table_privileges("t", "u").unwrap().is_empty());
}

#[test]
fn main_format_pg_roles_preserve_identity_and_bytes() {
    let (dir, admin) = database();
    let roles = admin.roles();
    // Main's role JSON has no inherit field; its memberships have empty values.
    roles.write(|table| {
        for (name, oid, task_only) in [("pg_monitor", 17001, false), ("pg_custom", 17002, true)] {
            let role = CatalogRole { name: name.into(), oid, login: !task_only,
                superuser: false, createrole: false, password: None, generation: 1, task_only, inherit: true };
            table.insert(&key(b"role/", name), &json(&role)?)?;
            if !task_only { table.insert(&grant_key(b"member/", name, "u"), &[])?; }
        }
        Ok(())
    }).unwrap();
    let before = raw(&roles);
    assert_eq!(bits(&admin) & FEATURE, 0);
    let stored = roles.get("pg_monitor").unwrap().unwrap();
    assert!(stored.login);
    assert_eq!(stored.oid, 17001);
    assert_eq!(roles.image().unwrap().get("pg_monitor").unwrap(), Some(stored));
    assert_eq!(roles.list().unwrap().iter().filter(|r| r.name == "pg_monitor").count(), 1);
    assert!(roles.get("pg_custom").unwrap().unwrap().task_only);
    assert!(roles.can_set("u", "pg_monitor").unwrap());
    assert!(!roles.is_member("pg_monitor", "pg_read_all_stats").unwrap());
    assert_eq!(raw(&roles), before);
    drop(roles); drop(admin);
    let admin = Admin::open(dir.path(), Limits::default()).unwrap();
    assert_eq!(raw(&admin.roles()), before);
    assert_eq!(bits(&admin) & FEATURE, 0);
    assert!(admin.roles().get("pg_monitor").unwrap().unwrap().login);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn bootstrap_key_survives_superuser_demotion_and_reopen() {
    let (dir, admin) = database();
    let roles = admin.roles();
    roles.alter("peer", RoleChanges { superuser: Some(true), ..Default::default() }).unwrap();
    roles.grant_membership_options("g", "u", "peer", MembershipOptions { admin: Some(true), ..Default::default() }).unwrap();
    assert!(raw(&roles).contains_key(&grant_key(b"member/", "g", "u")));
    roles.alter("postgres", RoleChanges { superuser: Some(false), ..Default::default() }).unwrap();
    roles.drop("postgres").unwrap();
    drop(roles); drop(admin);
    let admin = Admin::open(dir.path(), Limits::default()).unwrap();
    let roles = admin.roles();
    roles.grant_membership_options("g", "u", "peer", MembershipOptions { admin: Some(false), ..Default::default() }).unwrap();
    assert_eq!(raw(&roles).keys().filter(|k| k.starts_with(b"member/g\0u")).count(), 1);
    assert!(roles.revoke_membership_options("g", "u", "peer", None, false).unwrap());
    assert!(!roles.is_member("u", "g").unwrap());
}

#[test]
fn legacy_refuses_synthesized_membership_without_writing() {
    let mut id = [0; 16]; getrandom::fill(&mut id).unwrap();
    let dir = Directory(std::env::temp_dir().join(format!("membership-legacy-{:032x}", u128::from_be_bytes(id))));
    let admin = Admin::create_engine(dir.path(), Limits::default(), Engine::Legacy).unwrap();
    let roles = admin.roles();
    roles.create(NewRole { name: "u".into(), ..Default::default() }).unwrap();
    let before = raw(&roles);
    assert!(matches!(roles.grant_membership("pg_monitor", "u"), Err(RoleError::Core(CoreError::Unsupported))));
    assert_eq!(raw(&roles), before);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn named_bootstrap_is_distinct_from_logical_bootstrap() {
    let (dir, admin) = database();
    let roles = admin.roles();
    roles.create(NewRole { name: "bootstrap".into(), ..Default::default() }).unwrap();
    roles.grant_membership_options("g", "u", BOOTSTRAP, MembershipOptions { admin: Some(true), ..Default::default() }).unwrap();
    roles.grant_membership_options("g", "u", "bootstrap", Default::default()).unwrap();
    let before = raw(&roles);
    assert!(before.contains_key(&grant_key(b"member/", "g", "u")));
    assert!(before.contains_key(b"member/g\0u\0bootstrap".as_slice()));
    drop(roles); drop(admin);
    let admin = Admin::open(dir.path(), Limits::default()).unwrap();
    let roles = admin.roles();
    assert_eq!(raw(&roles), before);
    roles.revoke_membership_options("g", "u", "bootstrap", None, false).unwrap();
    assert!(roles.is_member("u", "g").unwrap());
    roles.revoke_membership_options("g", "u", BOOTSTRAP, None, false).unwrap();
    assert!(!roles.is_member("u", "g").unwrap());
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn promoted_named_grantor_addresses_existing_edge_after_reopen() {
    let (dir, admin) = database();
    let roles = admin.roles();
    roles.grant_membership_options("g", "peer", BOOTSTRAP, MembershipOptions { admin: Some(true), ..Default::default() }).unwrap();
    roles.grant_membership_options("g", "u", "peer", MembershipOptions { admin: Some(true), ..Default::default() }).unwrap();
    roles.grant_membership("g", "u").unwrap();
    roles.alter("peer", RoleChanges { superuser: Some(true), ..Default::default() }).unwrap();
    drop(roles); drop(admin);
    let admin = Admin::open(dir.path(), Limits::default()).unwrap();
    let roles = admin.roles();
    roles.grant_membership_options_explicit("g", "u", "peer", MembershipOptions { admin: Some(false), set: Some(false), ..Default::default() }).unwrap();
    let edges = roles.edges().unwrap();
    let named = edges.iter().find(|e| e.role == "g" && e.member == "u" && e.grantor == "peer").unwrap();
    assert!(!named.admin && !named.set);
    assert_eq!(raw(&roles).keys().filter(|k| k.starts_with(b"member/g\0u")).count(), 2);
    drop(roles); drop(admin);
    let admin = Admin::open(dir.path(), Limits::default()).unwrap();
    let roles = admin.roles();
    assert!(roles.revoke_membership_options_explicit("g", "u", "peer", None, false).unwrap());
    assert!(roles.is_member("u", "g").unwrap());
    assert_eq!(raw(&roles).keys().filter(|k| k.starts_with(b"member/g\0u")).count(), 1);
    roles.grant_membership_options_explicit("g", "other", "peer", Default::default()).unwrap();
    assert!(raw(&roles).contains_key(b"member/g\0other\0peer".as_slice()));
    assert!(!raw(&roles).contains_key(b"member/g\0other".as_slice()));
    // A missing explicit edge must never select the coexisting bootstrap grant.
    let before = raw(&roles);
    assert!(!roles.revoke_membership_options_explicit("g", "u", "peer", None, false).unwrap());
    for option in [MembershipOption::Admin, MembershipOption::Set, MembershipOption::Inherit] {
        assert!(!roles.revoke_membership_options_explicit("g", "u", "peer", Some(option), false).unwrap());
    }
    assert_eq!(raw(&roles), before);
    assert!(roles.is_member("u", "g").unwrap());
    // The compatibility/implicit API still addresses bootstrap for superusers.
    assert!(roles.revoke_membership_options("g", "u", "peer", None, false).unwrap());
    assert!(!roles.is_member("u", "g").unwrap());
}

#[test]
fn main_format_capability_collisions_preserve_bytes() {
    let (dir, admin) = database();
    let roles = admin.roles();
    roles.write(|table| {
        for (i, (_, name)) in PREDEFINED.iter().enumerate() {
            let role = CatalogRole { name: (*name).into(), oid: 18000+i as u32, login: true,
                superuser: false, createrole: false, password: None, generation: 1, task_only: false, inherit: true };
            table.insert(&key(b"role/", name), &json(&role)?)?;
            table.insert(&grant_key(b"member/", name, "u"), &[])?;
        }
        Ok(())
    }).unwrap();
    let before = raw(&roles);
    assert_eq!(bits(&admin) & FEATURE, 0);
    for (_, name) in PREDEFINED {
        assert!(roles.builtin_capabilities_of(name).unwrap().is_empty());
    }
    assert!(roles.builtin_capabilities_of("u").unwrap().is_empty());
    assert!(roles.privileges_of("u").unwrap().contains("pg_read_all_data"));
    assert_eq!(raw(&roles), before);
    drop(roles); drop(admin);
    let admin = Admin::open(dir.path(), Limits::default()).unwrap();
    assert!(admin.roles().builtin_capabilities_of("u").unwrap().is_empty());
    assert_eq!(raw(&admin.roles()), before);
    assert_eq!(bits(&admin) & FEATURE, 0);
}
