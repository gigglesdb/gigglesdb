//! GD-0061 R3-602 and R3-111 (D8): export and restore refuse task databases
//! (PROTECTED_RECORDS or TASK_CATALOG).
use super::*;
use crate::{
    Engine,
    task::records::{self as rec, TaskObjectId},
};

fn directory() -> std::path::PathBuf {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    std::env::temp_dir().join(format!("v2-export-task-{:032x}", u128::from_be_bytes(nonce)))
}

fn v2(admin: &Admin) -> crate::storage::EngineLease {
    match &admin.inner.backend {
        Backend::V2(_) => admin.test_v2(),
        _ => panic!("expected v2"),
    }
}

fn database(root: &Path) -> Admin {
    let admin = Admin::create_engine(root.join("db"), Limits::default(), Engine::V2).unwrap();
    let mut sql = admin.sql_session();
    sql.execute("CREATE TABLE items (id BIGINT PRIMARY KEY, body TEXT)", &[], None)
        .unwrap();
    sql.execute("INSERT INTO items VALUES (1,'one'),(2,'two')", &[], None)
        .unwrap();
    drop(sql);
    admin
}

fn listing(dir: &Path) -> Vec<std::ffi::OsString> {
    let mut names: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    names
}

#[test]
fn export_refused_with_protected_bit_and_no_file_created() {
    let root = directory();
    fs::create_dir(&root).unwrap();
    let admin = database(&root);
    let db = v2(&admin);
    let mut txn = db.begin(engine::txn::Isolation::ReadCommitted);
    txn.put(
        Keyspace::Kernel,
        rec::mutation_head_key(TaskObjectId(1)),
        rec::MutationHead { next_seq: 1, ..Default::default() }.encode(),
    )
    .unwrap();
    txn.require_features(engine::meta::PROTECTED_RECORDS);
    txn.commit().unwrap();
    let features = engine::meta::validate_meta(db.kv.as_ref()).unwrap().features;
    assert_ne!(features & engine::meta::PROTECTED_RECORDS, 0);
    let before = listing(&root);
    assert!(matches!(admin.export(root.join("backup")), Err(CoreError::Unsupported)));
    assert!(matches!(
        export_inner(&db, &root.join("backup"), RecoveryPoint::None, |_| {
            panic!("refusal must precede the export snapshot")
        }),
        Err(CoreError::Unsupported)
    ));
    // Neither the destination nor a staging file was created.
    assert!(!root.join("backup").exists());
    assert_eq!(listing(&root), before);
    drop(db);
    drop(admin);
    // The refusal also holds after reopen.
    let admin = Admin::open(root.join("db"), Limits::default()).unwrap();
    assert!(matches!(admin.export(root.join("backup")), Err(CoreError::Unsupported)));
    assert_eq!(listing(&root), before);
    drop(admin);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn restore_refuses_header_with_bit() {
    let root = directory();
    fs::create_dir(&root).unwrap();
    let admin = database(&root);
    admin.export(root.join("backup")).unwrap();
    drop(admin);
    let mut bytes = fs::read(root.join("backup")).unwrap();
    let features = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
    assert_eq!(features & engine::meta::EXPORT_REFUSED, 0);
    bytes[12..20].copy_from_slice(&(features | engine::meta::PROTECTED_RECORDS).to_be_bytes());
    let end = bytes.len() - 32;
    let digest = Sha256::digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&digest);
    fs::write(root.join("tagged"), &bytes).unwrap();
    let before = listing(&root);
    assert!(matches!(
        Admin::restore(root.join("tagged"), root.join("restored"), Limits::default()),
        Err(CoreError::Unsupported)
    ));
    assert!(!root.join("restored").exists());
    assert_eq!(listing(&root), before);
    assert!(matches!(Admin::verify_export(root.join("tagged")), Err(CoreError::Unsupported)));
    // The untouched export still restores.
    let restored =
        Admin::restore(root.join("backup"), root.join("restored"), Limits::default()).unwrap();
    drop(restored);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn export_bytes_unchanged_without_bits() {
    let root = directory();
    fs::create_dir(&root).unwrap();
    let admin = database(&root);
    let db = v2(&admin);
    let features = engine::meta::validate_meta(db.kv.as_ref()).unwrap().features;
    assert_eq!(features & engine::meta::EXPORT_REFUSED, 0);
    let first = admin.export(root.join("first")).unwrap();
    let second = admin.export(root.join("second")).unwrap();
    assert_eq!(first, second);
    let bytes = fs::read(root.join("first")).unwrap();
    assert_eq!(bytes, fs::read(root.join("second")).unwrap());
    // Layout: magic, version 1, the database's features, the snapshot, then records.
    assert_eq!(&bytes[..8], MAGIC);
    assert_eq!(bytes[8..12], 1u32.to_be_bytes());
    assert_eq!(u64::from_be_bytes(bytes[12..20].try_into().unwrap()), features);
    assert_eq!(Sha256::digest(&bytes[..bytes.len() - 32])[..], first);
    let read = ExportRead::open(&root.join("first")).unwrap();
    read.verify().unwrap();
    let mut offset = HEADER;
    let mut tags = Vec::new();
    while let Some(record) = read.record(&mut offset).unwrap() {
        tags.push(record.tag);
    }
    assert!(tags.iter().all(|tag| *tag <= 4) && tags.contains(&2));
    assert!(
        read.range(Keyspace::Kernel, Bound::Unbounded, Bound::Unbounded, Direction::Forward)
            .unwrap()
            .next()
            .is_none()
    );
    drop(read);
    let restored =
        Admin::restore(root.join("first"), root.join("restored"), Limits::default()).unwrap();
    assert_eq!(
        restored
            .sql_session()
            .execute("SELECT * FROM items", &[], None)
            .unwrap()
            .rows
            .len(),
        2
    );
    drop(restored);
    drop(db);
    drop(admin);
    fs::remove_dir_all(root).unwrap();
}

// ---- GD-0061 R3-111 (D8): TASK_CATALOG databases are refused too. ----

/// The refusal mask R3-111 reuses from R3-602 (`EXPORT_REFUSED`).
const REFUSED_FOR_EXPORT: u64 = engine::meta::EXPORT_REFUSED;

fn features_of(admin: &Admin) -> u64 {
    engine::meta::validate_meta(v2(admin).kv.as_ref()).unwrap().features
}

fn put_task_definition(admin: &Admin) {
    use crate::task::{
        catalog::{encode_task, task_key},
        definition::{
            ColumnMode, DisclosureCaps, LoadLimits, MutationCaps, PredicateText, TaskDef,
            TaskName, TaskObjectId as DefId, WallClockMs,
        },
    };
    use std::collections::{BTreeMap, BTreeSet};
    let def = TaskDef {
        id: DefId(500),
        name: TaskName::new("inc-436").unwrap(),
        def_version: 1,
        issuer: "issuer".into(),
        executor: "agent".into(),
        approver: None,
        table: 1,
        visible_rows: PredicateText("id = 1".into()),
        columns: BTreeMap::from([("id".to_string(), ColumnMode::Readable)]),
        untrusted_allowed: BTreeSet::new(),
        allowed_write: None,
        mutation_caps: MutationCaps::default(),
        disclosure_caps: DisclosureCaps { rows_read: 10, rows_returned: 10, bytes_returned: 4096 },
        limits: LoadLimits::default(),
        approval_threshold: 0,
        expires_at: WallClockMs(1_793_899_800_000),
        revoked: false,
        grants: BTreeMap::new(),
    };
    let db = v2(admin);
    let mut tx = db.begin(engine::txn::Isolation::ReadCommitted);
    tx.require_features(engine::meta::TASK_CATALOG | crate::storage::roles::engine::FEATURE);
    tx.put(Keyspace::Catalog, task_key(def.id.0), encode_task(&def).unwrap())
        .unwrap();
    db.catalog().commit_ddl(tx).unwrap();
}

fn assert_export_refused(root: &Path, admin: &Admin) {
    let before = listing(root);
    assert!(matches!(admin.export(root.join("backup")), Err(CoreError::Unsupported)));
    assert!(matches!(
        export_inner(&v2(admin), &root.join("backup"), RecoveryPoint::None, |_| {
            panic!("refusal must precede the export snapshot")
        }),
        Err(CoreError::Unsupported)
    ));
    assert!(!root.join("backup").exists());
    assert_eq!(listing(root), before);
}

#[test]
fn export_refuses_database_with_task() {
    assert_ne!(REFUSED_FOR_EXPORT & engine::meta::TASK_CATALOG, 0);
    let root = directory();
    fs::create_dir(&root).unwrap();
    let admin = database(&root);
    admin.export(root.join("plain")).unwrap();
    fs::remove_file(root.join("plain")).unwrap();
    put_task_definition(&admin);
    assert_ne!(features_of(&admin) & engine::meta::TASK_CATALOG, 0);
    assert_export_refused(&root, &admin);
    drop(admin);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn export_refuses_database_with_only_task_only_role() {
    let root = directory();
    fs::create_dir(&root).unwrap();
    let admin = database(&root);
    let roles = admin.roles();
    roles
        .create(crate::storage::roles::NewRole { name: "agent".into(), login: true, ..crate::storage::roles::NewRole::default() })
        .unwrap();
    roles.set_task_only("agent", true).unwrap();
    // Clearing the attribute keeps the monotone bit; the database stays refused.
    roles.set_task_only("agent", false).unwrap();
    drop(roles);
    assert_ne!(features_of(&admin) & engine::meta::TASK_CATALOG, 0);
    assert_export_refused(&root, &admin);
    drop(admin);
    let admin = Admin::open(root.join("db"), Limits::default()).unwrap();
    assert_export_refused(&root, &admin);
    drop(admin);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn export_leaves_no_partial_file() {
    let root = directory();
    fs::create_dir(&root).unwrap();
    let admin = database(&root);
    put_task_definition(&admin);
    let nested = root.join("nested");
    assert!(matches!(admin.export(nested.join("backup")), Err(CoreError::Unsupported)));
    assert!(!nested.exists());
    let before = listing(&root);
    for name in ["backup", "backup.tmp", "backup.partial"] {
        assert!(matches!(admin.export(root.join(name)), Err(CoreError::Unsupported)));
        assert!(!root.join(name).exists());
    }
    assert_eq!(listing(&root), before);
    drop(admin);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn restore_refuses_header_with_task_catalog_bit() {
    let root = directory();
    fs::create_dir(&root).unwrap();
    let admin = database(&root);
    admin.export(root.join("backup")).unwrap();
    drop(admin);
    let mut bytes = fs::read(root.join("backup")).unwrap();
    let features = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
    assert_eq!(features & REFUSED_FOR_EXPORT, 0);
    bytes[12..20].copy_from_slice(
        &(features | engine::meta::TASK_CATALOG | crate::storage::roles::engine::FEATURE).to_be_bytes(),
    );
    let end = bytes.len() - 32;
    let digest = Sha256::digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&digest);
    fs::write(root.join("tagged"), &bytes).unwrap();
    let before = listing(&root);
    assert!(matches!(
        Admin::restore(root.join("tagged"), root.join("restored"), Limits::default()),
        Err(CoreError::Unsupported)
    ));
    assert!(!root.join("restored").exists());
    assert_eq!(listing(&root), before);
    assert!(matches!(Admin::verify_export(root.join("tagged")), Err(CoreError::Unsupported)));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn database_without_tasks_exports_as_before() {
    let root = directory();
    fs::create_dir(&root).unwrap();
    let admin = database(&root);
    // An ordinary role does not set the bit.
    admin
        .roles()
        .create(crate::storage::roles::NewRole { name: "plain".into(), login: true, ..crate::storage::roles::NewRole::default() })
        .unwrap();
    assert_eq!(features_of(&admin) & REFUSED_FOR_EXPORT, 0);
    let first = admin.export(root.join("first")).unwrap();
    let bytes = fs::read(root.join("first")).unwrap();
    assert_eq!(&bytes[..8], MAGIC);
    assert_eq!(
        u64::from_be_bytes(bytes[12..20].try_into().unwrap()),
        features_of(&admin)
    );
    assert_eq!(Sha256::digest(&bytes[..bytes.len() - 32])[..], first);
    let restored =
        Admin::restore(root.join("first"), root.join("restored"), Limits::default()).unwrap();
    assert_eq!(
        restored
            .sql_session()
            .execute("SELECT * FROM items", &[], None)
            .unwrap()
            .rows
            .len(),
        2
    );
    drop(restored);
    drop(admin);
    fs::remove_dir_all(root).unwrap();
}
