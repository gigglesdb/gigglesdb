use super::*;
use crate::{Engine, Limits, storage::Backend};
use std::path::PathBuf;

fn version(db: &crate::storage::v2::V2Db) -> crate::storage::engine::hlc::Ts {
    let txn = db.begin(crate::storage::engine::txn::Isolation::ReadCommitted);
    db.catalog().binding_snapshot(&txn).unwrap().1.unwrap()
}

#[test]
fn role_cache_versions_and_reuse() {
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    let Backend::V2(db) = &admin.inner.backend else {
        unreachable!()
    };
    let roles = admin.roles();
    let before = version(db);
    roles.create(role("reader")).unwrap();
    assert_ne!(before, version(db));
    assert!(roles.get("reader").unwrap().is_some());
    let first = roles.image().unwrap();
    let second = roles.image().unwrap();
    assert!(Arc::ptr_eq(&first.image, &second.image));
    first.get("reader").unwrap();
    assert_eq!(first.image.decoded.get().unwrap().len(), 1);
    for grant in [true, false, true, false] {
        let before = version(db);
        if grant {
            roles
                .grant_table("items", "reader", &[TablePrivilege::Select])
                .unwrap();
        } else {
            roles
                .revoke_table("items", "reader", &[TablePrivilege::Select])
                .unwrap();
        }
        assert_ne!(before, version(db));
        assert_eq!(
            roles
                .table_privileges("items", "reader")
                .unwrap()
                .contains(&TablePrivilege::Select),
            grant
        );
    }
    let before = version(db);
    roles.set_table_owner("items", "reader").unwrap();
    assert_ne!(before, version(db));
    assert_eq!(
        roles.table_owner("items").unwrap().as_deref(),
        Some("reader")
    );
    assert_eq!(
        roles.image().unwrap().table_owner("items").unwrap(),
        roles.table_owner("items").unwrap()
    );
    let before = version(db);
    roles
        .alter(
            "reader",
            RoleChanges {
                login: Some(false),
                ..RoleChanges::default()
            },
        )
        .unwrap();
    assert_ne!(before, version(db));
    assert!(!roles.get("reader").unwrap().unwrap().login);
    assert!(first.get("reader").unwrap().unwrap().login);
    assert!(matches!(first.get(""), Err(RoleError::Invalid)));
    assert!(matches!(first.table_owner(""), Err(RoleError::Invalid)));
}

#[test]
fn role_cache_snapshot_and_private_write_bypass() {
    use crate::storage::engine::{kv::Keyspace, txn::Isolation};
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    let Backend::V2(db) = &admin.inner.backend else {
        unreachable!()
    };
    let roles = admin.roles();
    roles.create(role("reader")).unwrap();
    let mut old = db.begin(Isolation::ReadCommitted);
    // Leave the old image unbuilt until AFTER the role write.
    roles
        .alter(
            "reader",
            RoleChanges {
                login: Some(false),
                ..RoleChanges::default()
            },
        )
        .unwrap();
    let old_image = engine::image_txn(db.catalog(), &mut old).unwrap();
    assert!(old_image.get("reader").unwrap().unwrap().login);
    assert!(!roles.get("reader").unwrap().unwrap().login);
    let mut own = db.begin(Isolation::ReadCommitted);
    let raw = key(b"role/", "reader");
    let mut changed = roles.get("reader").unwrap().unwrap();
    changed.login = true;
    own.put(
        Keyspace::Catalog,
        engine::encode_key(&raw).unwrap(),
        [vec![1], json(&changed).unwrap()].concat(),
    )
    .unwrap();
    let private = engine::image_txn(db.catalog(), &mut own).unwrap();
    assert_eq!(private.table.0, engine::load_txn(&mut own).unwrap().0);
    assert!(private.get("reader").unwrap().unwrap().login);
    assert!(!Arc::ptr_eq(&private, &roles.image().unwrap().image));
    let again = engine::image_txn(db.catalog(), &mut own).unwrap();
    assert!(!Arc::ptr_eq(&private, &again));
}

#[test]
fn role_cache_old_snapshot_fallback_bypass() {
    use crate::storage::engine::{catalog::Catalog, locks::LockManager, txn::Isolation};
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    let Backend::V2(db) = &admin.inner.backend else {
        unreachable!()
    };
    let roles = admin.roles();
    roles.create(role("reader")).unwrap();
    let mut old = db.begin(Isolation::ReadCommitted);
    roles
        .alter(
            "reader",
            RoleChanges {
                login: Some(false),
                ..RoleChanges::default()
            },
        )
        .unwrap();
    // Opening at the new visible timestamp leaves the old transaction before
    // this catalog's earliest known version, exercising the real fallback.
    let reopened = Catalog::open(
        db.sequencer.clone(),
        Arc::new(LockManager::new(Default::default())),
        db.sequences.clone(),
    )
    .unwrap();
    assert!(reopened.binding_snapshot(&old).unwrap().1.is_none());
    let image = engine::image_txn(&reopened, &mut old).unwrap();
    assert_eq!(image.table.0, engine::load_txn(&mut old).unwrap().0);
    assert!(image.get("reader").unwrap().unwrap().login);
    assert!(!Arc::ptr_eq(
        &image,
        &engine::image_txn(&reopened, &mut old).unwrap()
    ));
}

#[test]
fn role_cache_failed_build_is_retryable() {
    use crate::storage::engine::txn::Isolation;
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    let Backend::V2(db) = &admin.inner.backend else {
        unreachable!()
    };
    let mut txn = db.begin(Isolation::ReadCommitted);
    let (snapshot, _) = db.catalog().binding_snapshot(&txn).unwrap();
    assert!(matches!(
        snapshot.roles(|| Err(RoleError::Invalid)),
        Err(RoleError::Invalid)
    ));
    let image = snapshot.roles(|| engine::load_txn(&mut txn)).unwrap();
    let again = snapshot
        .roles(|| panic!("successful image must be reused"))
        .unwrap();
    assert!(Arc::ptr_eq(&image, &again));
}

#[test]
fn role_image_session_owner_prefers_own_ddl() {
    use crate::storage::engine::{kv::Keyspace, txn::Isolation};
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    let Backend::V2(db) = &admin.inner.backend else {
        unreachable!()
    };
    let roles = admin.roles();
    roles.create(role("owner")).unwrap();
    roles.set_table_owner("items", "owner").unwrap();
    let image = roles.image().unwrap();
    let mut session = admin.sql_session();
    assert_eq!(
        session.table_owner_in(&image, "items").unwrap().as_deref(),
        Some("owner")
    );
    let mut txn = db.begin(Isolation::ReadCommitted);
    let owner_key = engine::encode_key(&key(b"owner/", "items")).unwrap();
    txn.delete(Keyspace::Catalog, owner_key).unwrap();
    session.v2_transaction = Some(txn);
    assert_eq!(session.table_owner_in(&image, "items").unwrap(), None);
    assert_eq!(
        session.table_owner_in(&image, "items").unwrap(),
        session.table_owner("items").unwrap()
    );
    assert_eq!(
        image.table_owner("items").unwrap().as_deref(),
        Some("owner")
    );
}

#[test]
fn role_image_legacy_and_v2_accessor_parity() {
    for backend in [Engine::Legacy, Engine::V2] {
        let dir = Directory::new();
        let admin = Admin::create_engine(&dir.0, Limits::default(), backend).unwrap();
        let roles = admin.roles();
        roles.create(role("reader")).unwrap();
        roles.set_table_owner("items", "reader").unwrap();
        let image = roles.image().unwrap();
        for name in ["reader", "missing"] {
            assert_eq!(image.get(name).unwrap(), roles.get(name).unwrap());
        }
        for table in ["items", "missing"] {
            assert_eq!(
                image.table_owner(table).unwrap(),
                roles.table_owner(table).unwrap()
            );
        }
        assert!(matches!(image.get(""), Err(RoleError::Invalid)));
        assert!(matches!(image.table_owner(""), Err(RoleError::Invalid)));
        // Missing schemas are invalid only on legacy, matching RoleCatalog.
        assert_eq!(
            image.table_owner("absent\0items").is_err(),
            roles.table_owner("absent\0items").is_err()
        );
    }
}

#[test]
fn role_cache_concurrent_grant_revoke() {
    use std::sync::{Barrier, atomic::AtomicBool};
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    let roles = admin.roles();
    roles.create(role("reader")).unwrap();
    let barrier = Barrier::new(9);
    let expected = AtomicBool::new(false);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let roles = &roles;
            let barrier = &barrier;
            let expected = &expected;
            scope.spawn(move || {
                for _ in 0..200 {
                    assert!(roles.get("reader").unwrap().unwrap().login);
                    let grants = roles.table_privileges("items", "reader").unwrap();
                    assert!(
                        grants.is_empty() || grants == BTreeSet::from([TablePrivilege::Select])
                    );
                    // Writer has committed before the first barrier. No next
                    // write until every reader has checked that commit.
                    barrier.wait();
                    assert_eq!(
                        roles
                            .table_privileges("items", "reader")
                            .unwrap()
                            .contains(&TablePrivilege::Select),
                        expected.load(Ordering::Acquire)
                    );
                    barrier.wait();
                }
            });
        }
        for i in 0..200 {
            let grant = i % 2 == 0;
            if grant {
                roles
                    .grant_table("items", "reader", &[TablePrivilege::Select])
                    .unwrap();
            } else {
                roles
                    .revoke_table("items", "reader", &[TablePrivilege::Select])
                    .unwrap();
            }
            expected.store(grant, Ordering::Release);
            barrier.wait();
            barrier.wait();
        }
    });
}
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let mut id = [0; 16];
        getrandom::fill(&mut id).unwrap();
        Self(std::env::temp_dir().join(format!("role-parity-{:032x}", u128::from_be_bytes(id))))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn role(name: &str) -> NewRole {
    NewRole {
        name: name.into(),
        login: true,
        ..NewRole::default()
    }
}
#[derive(Debug, PartialEq, Eq)]
struct Authority {
    roles: Vec<CatalogRole>,
    owner: Option<String>,
    table: BTreeSet<TablePrivilege>,
    schema: BTreeSet<SchemaPrivilege>,
    visible: BTreeSet<String>,
    memberships: BTreeSet<String>,
}
fn authority(roles: &RoleCatalog) -> Authority {
    Authority {
        roles: roles.list().unwrap(),
        owner: roles.table_owner("items").unwrap(),
        table: roles.table_privileges("items", "reader").unwrap(),
        schema: roles.schema_privileges("public", "reader").unwrap(),
        visible: roles.visible_tables("reader").unwrap(),
        memberships: roles.memberships("reader").unwrap(),
    }
}
fn script(admin: &Admin) -> Authority {
    let roles = admin.roles();
    roles
        .create(NewRole {
            superuser: true,
            ..role("owner")
        })
        .unwrap();
    let original = roles.create(role("reader")).unwrap();
    roles.create(role("group")).unwrap();
    assert!(matches!(
        roles.create(role("reader")),
        Err(RoleError::Duplicate)
    ));
    let changed = roles
        .alter(
            "reader",
            RoleChanges {
                password: Some(Some("SCRAM-SHA-256$opaque-verifier".into())),
                ..RoleChanges::default()
            },
        )
        .unwrap();
    assert_eq!(changed.generation, original.generation + 1);
    assert_eq!(changed.oid, original.oid);
    roles.grant_membership("group", "reader").unwrap();
    roles.grant_membership("group", "reader").unwrap();
    assert!(matches!(
        roles.grant_membership("reader", "group"),
        Err(RoleError::Invalid)
    ));
    roles.revoke_membership("group", "reader").unwrap();
    assert!(roles.memberships("reader").unwrap().is_empty());
    roles.grant_membership("group", "reader").unwrap();
    roles.set_table_owner("items", "owner").unwrap();
    assert!(matches!(roles.drop("owner"), Err(RoleError::Dependent)));
    roles
        .grant_table(
            "items",
            "reader",
            &[TablePrivilege::Select, TablePrivilege::Insert],
        )
        .unwrap();
    roles
        .grant_table("items", "reader", &[TablePrivilege::Select])
        .unwrap();
    roles
        .revoke_table(
            "items",
            "reader",
            &[TablePrivilege::Insert, TablePrivilege::Delete],
        )
        .unwrap();
    roles
        .grant_schema(
            "public",
            "reader",
            &[SchemaPrivilege::Usage, SchemaPrivilege::Create],
        )
        .unwrap();
    roles
        .revoke_schema("public", "reader", &[SchemaPrivilege::Create])
        .unwrap();
    assert!(
        roles
            .table_privileges("items", "reader")
            .unwrap()
            .contains(&TablePrivilege::Select)
    );
    assert!(
        !roles
            .table_privileges("items", "reader")
            .unwrap()
            .contains(&TablePrivilege::Insert)
    );
    assert_eq!(
        roles.visible_tables("reader").unwrap(),
        BTreeSet::from(["items".into()])
    );
    let oid = roles.create(role("temporary")).unwrap().oid;
    roles.grant_membership("temporary", "reader").unwrap();
    roles
        .grant_table("items", "temporary", &[TablePrivilege::Delete])
        .unwrap();
    roles.drop("temporary").unwrap();
    assert!(
        roles
            .table_privileges("items", "temporary")
            .unwrap()
            .is_empty()
    );
    assert!(!roles.memberships("reader").unwrap().contains("temporary"));
    assert!(roles.create(role("temporary")).unwrap().oid > oid);
    roles.drop("temporary").unwrap();
    authority(&roles)
}
#[test]
fn legacy_v2_role_authorization_and_reopen_parity() {
    let mut results = Vec::new();
    for engine in [Engine::Legacy, Engine::V2] {
        let dir = Directory::new();
        let admin = Admin::create_engine(&dir.0, Limits::default(), engine).unwrap();
        let expected = script(&admin);
        drop(admin);
        let admin = Admin::open(&dir.0, Limits::default()).unwrap();
        assert_eq!(authority(&admin.roles()), expected);
        assert_eq!(admin.roles().get("reader").unwrap().unwrap().generation, 2);
        admin
            .roles()
            .revoke_table("items", "reader", &[TablePrivilege::Select])
            .unwrap();
        assert!(admin.roles().visible_tables("reader").unwrap().is_empty());
        admin.roles().forget_table("items").unwrap();
        admin.roles().drop("owner").unwrap();
        assert_eq!(
            admin.roles().create(role("after_reopen")).unwrap().oid,
            FIRST_ROLE_OID + 5
        );
        drop(admin);
        let admin = Admin::open(&dir.0, Limits::default()).unwrap();
        assert!(admin.roles().table_owner("items").unwrap().is_none());
        assert!(
            admin
                .roles()
                .table_privileges("items", "reader")
                .unwrap()
                .is_empty()
        );
        drop(admin);
        results.push(expected);
    }
    assert_eq!(results[0], results[1]);
}
fn features(admin: &Admin) -> u64 {
    let Backend::V2(db) = &admin.inner.backend else {
        panic!("expected v2")
    };
    crate::storage::engine::meta::validate_meta(db.kv.as_ref())
        .unwrap()
        .features
}
#[test]
fn bit_11_is_lazy_monotone_and_rejected_by_the_old_binary_guard() {
    // Frozen pre-F8r check_features guard: the old binary understood bits 0..10.
    fn old_guard(bits: u64) -> Result<(), crate::storage::engine::meta::FormatError> {
        if bits & !2047 != 0 {
            return Err(crate::storage::engine::meta::FormatError::Unsupported(
                "feature bits".into(),
            ));
        }
        Ok(())
    }
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    let before = features(&admin);
    assert_eq!(before & engine::FEATURE, 0);
    assert!(admin.roles().is_empty().unwrap());
    admin.roles().forget_table("absent").unwrap();
    assert!(matches!(
        admin.roles().create(role("")),
        Err(RoleError::Invalid)
    ));
    assert_eq!(features(&admin), before);
    old_guard(before).unwrap();
    old_guard(crate::storage::engine::meta::TEXT_BACKED_COLUMN_TYPES).unwrap();
    admin.roles().create(role("reader")).unwrap();
    assert_eq!(features(&admin), before | engine::FEATURE);
    assert!(matches!(
        old_guard(features(&admin)),
        Err(crate::storage::engine::meta::FormatError::Unsupported(_))
    ));
    admin.roles().drop("reader").unwrap();
    assert_eq!(features(&admin), before | engine::FEATURE);
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    assert!(admin.roles().is_empty().unwrap());
    assert_eq!(features(&admin), before | engine::FEATURE);
    assert_eq!(
        admin.roles().create(role("next")).unwrap().oid,
        FIRST_ROLE_OID + 1
    );
    drop(admin);
}
#[test]
fn v2_concurrent_role_writers_preserve_oids_and_generations() {
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    admin.roles().create(role("shared")).unwrap();
    let workers: Vec<_> = (0..6)
        .map(|index| {
            let admin = admin.clone();
            std::thread::spawn(move || {
                let oid = admin
                    .roles()
                    .create(role(&format!("r{index}")))
                    .unwrap()
                    .oid;
                for _ in 0..4 {
                    admin
                        .roles()
                        .alter("shared", RoleChanges::default())
                        .unwrap();
                }
                oid
            })
        })
        .collect();
    let oids: BTreeSet<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(oids.len(), 6);
    assert_eq!(admin.roles().get("shared").unwrap().unwrap().generation, 25);
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    assert_eq!(admin.roles().get("shared").unwrap().unwrap().generation, 25);
    drop(admin);
}
#[test]
fn appended_role_keys_and_payloads_fail_closed() {
    for key in [
        vec![8],
        vec![8, 0],
        vec![8, 255, 0],
        vec![9, b'a', 0],
        vec![13, 0],
        vec![10, 0],
        vec![11, 0],
        b"\x0aalpha\0t\0".to_vec(),
        b"\x0balpha\0t\0reader\0".to_vec(),
    ] {
        assert!(engine::classify_key(&key).is_err());
    }
    for (key, payload) in [
        (b"\x08r\0".as_slice(), b"\x01{}".as_slice()),
        (b"\x09g\0r\0", b"\x01x"),
        (b"\x0at\0", b"\x01"),
        (b"\x0bt\0r\0", b"\x01[\"Unknown\"]"),
        (b"\x0cs\0r\0", b"\x02[]"),
        (b"\x0d", b"\x01\0\0\0\0"),
    ] {
        assert!(engine::validate_record(key, payload).is_err());
    }
}

#[test]
fn v2_ddl_renames_and_drops_authority_atomically() {
    use crate::{
        relational::{ColumnDef, DataType, TableDef},
        storage::engine::{locks::LockMode, txn::Isolation},
    };
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    admin.roles().create(role("owner")).unwrap();
    let Backend::V2(db) = &admin.inner.backend else {
        unreachable!()
    };
    // Stale authority for a name must be cleared when a new table gets that name.
    admin.roles().set_table_owner("items", "owner").unwrap();
    let mut tx = db.begin(Isolation::ReadCommitted);
    db.catalog()
        .create_table(
            &mut tx,
            TableDef {
                id: 0,
                name: "items".into(),
                columns: vec![ColumnDef {
                    id: 0,
                    name: "id".into(),
                    data_type: DataType::BigInt,
                    not_null: true,
                }],
                primary_key: vec![0],
            },
            false,
            None,
            &|| false,
        )
        .unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    assert!(admin.roles().table_owner("items").unwrap().is_none());
    admin.roles().set_table_owner("items", "owner").unwrap();
    admin
        .roles()
        .grant_table("items", "owner", &[TablePrivilege::Select])
        .unwrap();
    let mut tx = db.begin(Isolation::ReadCommitted);
    db.catalog()
        .resolve(&mut tx, "items", LockMode::AccessExclusive, None, &|| false)
        .unwrap();
    let old = db.catalog().parts(&mut tx).unwrap();
    let mut new = old.clone();
    let mut table = new.tables.remove("items").unwrap();
    table.name = "renamed".into();
    new.tables.insert("renamed".into(), table);
    db.catalog()
        .apply_diff(&mut tx, &old, &new, &(0..0), None, &|| false)
        .unwrap();
    // Buffered authority changes remain invisible before commit and rollback.
    assert_eq!(
        admin.roles().table_owner("items").unwrap().as_deref(),
        Some("owner")
    );
    assert!(admin.roles().table_owner("renamed").unwrap().is_none());
    drop(tx);
    let mut tx = db.begin(Isolation::ReadCommitted);
    db.catalog()
        .apply_diff(&mut tx, &old, &new, &(0..0), None, &|| false)
        .unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    assert!(admin.roles().table_owner("items").unwrap().is_none());
    assert_eq!(
        admin.roles().table_owner("renamed").unwrap().as_deref(),
        Some("owner")
    );
    assert_eq!(
        admin.roles().table_privileges("renamed", "owner").unwrap(),
        BTreeSet::from([TablePrivilege::Select])
    );
    let mut stale = db.begin(Isolation::RepeatableRead);
    let snapshot = stale.snapshot_ts();
    admin
        .roles()
        .grant_table("renamed", "owner", &[TablePrivilege::Insert])
        .unwrap();
    assert!(matches!(
        db.catalog()
            .drop_table(&mut stale, "renamed", None, &|| false),
        Err(crate::storage::engine::catalog::CatalogError::Core(
            CoreError::Sql(crate::relational::SqlError::Conflict)
        )) | Err(crate::storage::engine::catalog::CatalogError::Txn(
            crate::storage::engine::txn::TxnError::Serialization(_)
        ))
    ));
    assert_eq!(stale.snapshot_ts(), snapshot);
    drop(stale);
    assert!(
        admin
            .roles()
            .table_privileges("renamed", "owner")
            .unwrap()
            .contains(&TablePrivilege::Insert)
    );
    let mut tx = db.begin(Isolation::ReadCommitted);
    db.catalog()
        .drop_table(&mut tx, "renamed", None, &|| false)
        .unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    assert!(admin.roles().table_owner("renamed").unwrap().is_none());
    assert!(
        admin
            .roles()
            .table_privileges("renamed", "owner")
            .unwrap()
            .is_empty()
    );
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    assert!(admin.roles().visible_tables("owner").unwrap().is_empty());
    drop(admin);
}

#[test]
fn qualified_authority_is_isolated_and_survives_reopen_on_both_engines() {
    let long_schema = "s".repeat(MAX_ROLE_NAME);
    let long_table = format!("{long_schema}\0{}", "t".repeat(MAX_ROLE_NAME));
    let a = "alpha\0t";
    let b = "beta\0t";
    for engine in [Engine::Legacy, Engine::V2] {
        let dir = Directory::new();
        let admin = Admin::create_engine(&dir.0, Limits::default(), engine).unwrap();
        if matches!(&admin.inner.backend, Backend::Legacy(_)) {
            let mut sql = admin.sql_session();
            for schema in ["alpha", "beta", &long_schema] {
                sql.execute(&format!("CREATE SCHEMA {schema}"), &[], None)
                    .unwrap();
            }
        }
        let roles = admin.roles();
        for name in ["owner", "reader", "alpha", "t"] {
            roles.create(role(name)).unwrap();
        }
        for (table, owner, privilege) in [
            (a, "owner", TablePrivilege::Select),
            (b, "reader", TablePrivilege::Insert),
            ("t", "owner", TablePrivilege::Update),
            ("alpha", "owner", TablePrivilege::Update),
            (&long_table, "owner", TablePrivilege::Delete),
        ] {
            roles.set_table_owner(table, owner).unwrap();
            roles.grant_table(table, "reader", &[privilege]).unwrap();
        }
        // Before the qualified-key marker these bare/qualified grant keys
        // would be prefixes of each other: alpha->t and alpha.t->reader.
        roles
            .grant_table("alpha", "t", &[TablePrivilege::Insert])
            .unwrap();
        if let Backend::V2(db) = &admin.inner.backend {
            use crate::storage::engine::{
                kv::{Direction, Keyspace},
                txn::Isolation,
            };
            use std::ops::Bound;
            let mut tx = db.begin(Isolation::ReadCommitted);
            let keys: Vec<_> = tx
                .scan(
                    Keyspace::Catalog,
                    Bound::Included(&[8]),
                    Bound::Excluded(&[14]),
                    Direction::Forward,
                    None,
                )
                .unwrap()
                .map(|entry| entry.unwrap().0)
                .collect();
            assert!(keys.windows(2).all(|pair| !pair[1].starts_with(&pair[0])));
            assert!(keys.contains(&b"\x0a\0alpha\0t\0".to_vec()));
            assert!(keys.contains(&b"\x0aalpha\0".to_vec()));
            assert!(keys.contains(&b"\x0b\0alpha\0t\0reader\0".to_vec()));
            assert!(keys.contains(&b"\x0balpha\0t\0".to_vec()));
        }
        for invalid in ["public\0t", "alpha\0t\0extra", "\0t", "alpha\0"] {
            assert!(matches!(
                roles.set_table_owner(invalid, "owner"),
                Err(RoleError::Invalid)
            ));
            assert!(matches!(
                roles.grant_table(invalid, "reader", &[TablePrivilege::Select]),
                Err(RoleError::Invalid)
            ));
        }
        roles
            .grant_table(a, "alpha", &[TablePrivilege::Delete])
            .unwrap();
        roles.drop("alpha").unwrap();
        assert!(roles.table_privileges(a, "alpha").unwrap().is_empty());
        assert_eq!(
            roles.table_privileges(a, "reader").unwrap(),
            BTreeSet::from([TablePrivilege::Select])
        );
        drop(roles);
        drop(admin);
        let admin = Admin::open(&dir.0, Limits::default()).unwrap();
        let roles = admin.roles();
        for (table, owner, privilege) in [
            (a, "owner", TablePrivilege::Select),
            (b, "reader", TablePrivilege::Insert),
            ("t", "owner", TablePrivilege::Update),
            ("alpha", "owner", TablePrivilege::Update),
            (&long_table, "owner", TablePrivilege::Delete),
        ] {
            assert_eq!(roles.table_owner(table).unwrap().as_deref(), Some(owner));
            assert_eq!(
                roles.table_privileges(table, "reader").unwrap(),
                BTreeSet::from([privilege])
            );
        }
        assert_eq!(
            roles.visible_tables("reader").unwrap(),
            BTreeSet::from([
                a.into(),
                b.into(),
                "t".into(),
                "alpha".into(),
                long_table.clone()
            ])
        );
        roles
            .revoke_table(a, "reader", &[TablePrivilege::Select])
            .unwrap();
        assert!(!roles.visible_tables("reader").unwrap().contains(a));
        roles.forget_table(a).unwrap();
        assert_eq!(roles.table_owner(a).unwrap(), None);
        assert_eq!(roles.table_owner(b).unwrap().as_deref(), Some("reader"));
        roles.set_table_owner(b, "owner").unwrap();
        roles.drop("reader").unwrap();
        for table in [a, b, "t", "alpha", &long_table] {
            assert!(roles.table_privileges(table, "reader").unwrap().is_empty());
        }
        assert_eq!(
            roles.table_privileges("alpha", "t").unwrap(),
            BTreeSet::from([TablePrivilege::Insert])
        );
        drop(roles);
        drop(admin);
    }
}

#[test]
fn qualified_authority_follows_ddl_identity_not_the_bare_table_name() {
    use crate::{
        relational::TableDef,
        storage::engine::{catalog::CatalogParts, txn::Isolation},
    };
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    let roles = admin.roles();
    roles.create(role("owner")).unwrap();
    roles.create(role("reader")).unwrap();
    for table in ["alpha\0t", "beta\0t", "t"] {
        roles.set_table_owner(table, "owner").unwrap();
        roles
            .grant_table(table, "reader", &[TablePrivilege::Select])
            .unwrap();
    }
    let Backend::V2(db) = &admin.inner.backend else {
        unreachable!()
    };
    // Exercise the storage DDL reconciler with the main branch's qualified map
    // keys and bare TableDef names. This does not add v2 schema DDL support.
    let definition = |id| TableDef {
        id,
        name: "t".into(),
        columns: vec![],
        primary_key: vec![],
    };
    let old = CatalogParts {
        tables: BTreeMap::from([
            ("alpha\0t".into(), definition(1)),
            ("beta\0t".into(), definition(2)),
            ("t".into(), definition(3)),
        ]),
        ..CatalogParts::default()
    };
    let mut renamed = old.clone();
    let moved = renamed.tables.remove("alpha\0t").unwrap();
    renamed.tables.insert("gamma\0t".into(), moved);
    let mut tx = db.begin(Isolation::ReadCommitted);
    engine::reconcile_tables(&mut tx, db.kv.as_ref(), &old, &renamed, None, &|| false).unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    assert_eq!(roles.table_owner("alpha\0t").unwrap(), None);
    assert_eq!(
        roles.table_owner("gamma\0t").unwrap().as_deref(),
        Some("owner")
    );
    assert_eq!(
        roles.table_privileges("gamma\0t", "reader").unwrap(),
        BTreeSet::from([TablePrivilege::Select])
    );
    let mut dropped = renamed.clone();
    dropped.tables.remove("gamma\0t");
    let mut tx = db.begin(Isolation::ReadCommitted);
    engine::reconcile_tables(&mut tx, db.kv.as_ref(), &renamed, &dropped, None, &|| false).unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    assert_eq!(roles.table_owner("gamma\0t").unwrap(), None);
    assert!(
        roles
            .table_privileges("gamma\0t", "reader")
            .unwrap()
            .is_empty()
    );
    for table in ["beta\0t", "t"] {
        assert_eq!(roles.table_owner(table).unwrap().as_deref(), Some("owner"));
        assert_eq!(
            roles.table_privileges(table, "reader").unwrap(),
            BTreeSet::from([TablePrivilege::Select])
        );
    }
    drop(roles);
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    assert_eq!(
        admin.roles().visible_tables("reader").unwrap(),
        BTreeSet::from(["beta\0t".into(), "t".into()])
    );
    drop(admin);
}

#[test]
fn extended_type_and_role_feature_bits_coexist_after_reopen() {
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    admin
        .sql_session()
        .execute(
            "CREATE TABLE extended (id BIGINT PRIMARY KEY, payload UUID)",
            &[],
            None,
        )
        .unwrap();
    let column_features = features(&admin);
    assert_ne!(
        column_features & crate::storage::engine::meta::TEXT_BACKED_COLUMN_TYPES,
        0
    );
    assert_eq!(column_features & engine::FEATURE, 0);
    admin.roles().create(role("reader")).unwrap();
    assert_eq!(features(&admin), column_features | engine::FEATURE);
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    assert_eq!(features(&admin) & 0xC00, 0xC00);
    assert_eq!(features(&admin), column_features | engine::FEATURE);
    assert!(admin.roles().get("reader").unwrap().is_some());
    drop(admin);
}

#[test]
fn role_cache_decoded_misses_keep_raw_errors() {
    let good = role("good");
    let mut table = RoleTable::default();
    table.0.insert(key(b"role/", "good"), json(&CatalogRole {
        name: good.name, oid: FIRST_ROLE_OID, superuser: good.superuser,
        login: good.login, createrole: good.createrole, password: good.password,
        generation: 1, task_only: false,
    }).unwrap());
    table.0.insert(key(b"role/", "broken"), b"not json".to_vec());
    let image = RoleImage::new(table);
    assert!(image.get("good").unwrap().is_some());
    assert_eq!(image.decoded.get().unwrap().len(), 1);
    assert!(image.get("absent").unwrap().is_none());
    let expected = decode::<CatalogRole>(b"not json").unwrap_err().to_string();
    assert_eq!(image.get("broken").unwrap_err().to_string(), expected);
}
