//! Catalog kind 19 (task definitions), the TASK_CATALOG bit, its commit gate
//! and the open check (GD-0061 R3-102, design section 3).
use super::{
    catalog::Catalog,
    codec,
    commit::{ManualClock, Sequencer, SequencerConfig},
    content_check,
    hlc::Ts,
    kv::{Durability, Keyspace, Kv, KvBackend, KvRead, WriteBatch},
    kv_mem::MemKv,
    locks::{LockConfig, LockManager, TxnId},
    meta::{self, FormatError, TASK_CATALOG},
    mvcc,
    seq::SequenceAllocator,
    txn::{Isolation, Transaction, TxnError},
};
use crate::{
    relational::{ColumnDef, DataType, TableDef},
    task::{
        catalog::{decode_task, encode_task, task_key},
        definition::{
            ColumnMode, DisclosureCaps, LoadLimits, MutationCaps, PredicateText, TaskDef, TaskName,
            TaskObjectId, WallClockMs,
        },
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

const ROLES: u64 = crate::storage::roles::engine::FEATURE;

struct Fixture {
    mem: Arc<MemKv>,
    seq: Arc<Sequencer>,
    locks: Arc<LockManager>,
    catalog: Catalog,
}

fn open(mem: Arc<MemKv>) -> Result<Fixture, crate::CoreError> {
    let kv = Arc::new(Kv::new(mem.clone()));
    let seq = Sequencer::open(
        kv.clone(),
        Arc::new(ManualClock::new(10)),
        SequencerConfig {
            group_max: 0,
            group_wait: Duration::ZERO,
            ..SequencerConfig::default()
        },
    )
    .unwrap();
    let locks = Arc::new(LockManager::new(LockConfig::default()));
    let catalog = Catalog::open(seq.clone(), locks.clone(), Arc::new(SequenceAllocator::new(kv, 64)))?;
    Ok(Fixture { mem, seq, locks, catalog })
}

fn setup() -> Fixture {
    open(Arc::new(MemKv::new())).unwrap()
}

fn txn(f: &Fixture, id: u64) -> Transaction {
    Transaction::begin(&f.seq, &f.locks, TxnId(id), Isolation::ReadCommitted)
}

/// Creates table "shipments" and returns its id.
fn create_table(f: &Fixture) -> u64 {
    let column = |name: &str| ColumnDef {
        id: 0,
        name: name.into(),
        data_type: DataType::BigInt,
        not_null: name == "delivery_id",
    };
    let mut tx = txn(f, 1);
    let entry = f
        .catalog
        .create_table(
            &mut tx,
            TableDef {
                id: 0,
                name: "shipments".into(),
                columns: vec![column("delivery_id"), column("merchant_id")],
                primary_key: vec![0],
            },
            false,
            None,
            &|| false,
        )
        .unwrap();
    f.catalog.commit_ddl(tx).unwrap();
    entry.def.id
}

fn task(id: u64, name: &str, table: u64) -> TaskDef {
    TaskDef {
        id: TaskObjectId(id),
        name: TaskName::new(name).unwrap(),
        def_version: 1,
        issuer: "issuer".into(),
        executor: "agent".into(),
        approver: None,
        table,
        visible_rows: PredicateText("merchant_id = 42".into()),
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
    }
}

fn put_task(f: &Fixture, txn_id: u64, def: &TaskDef, features: u64) -> Result<Ts, TxnError> {
    let mut tx = txn(f, txn_id);
    tx.require_features(features);
    tx.put(Keyspace::Catalog, task_key(def.id.0), encode_task(def).unwrap())
        .unwrap();
    f.catalog.commit_ddl(tx)
}

fn features(kv: &dyn KvRead) -> u64 {
    kv.get(Keyspace::Meta, b"features")
        .unwrap()
        .map_or(0, |value| u64::from_be_bytes(value.try_into().unwrap()))
}

fn raw(mem: &MemKv, space: Keyspace, key: Vec<u8>, value: Vec<u8>) {
    let mut batch = WriteBatch::default();
    batch.put(space, key, value);
    mem.write(batch, Durability::Sync).unwrap();
}

/// A raw catalog version, bypassing the sequencer's gate and overlay check.
fn raw_catalog(mem: &MemKv, key: Vec<u8>, ts: u64, value: Option<Vec<u8>>) {
    raw(
        mem,
        Keyspace::Catalog,
        codec::user_key_seek_at(&key, Ts(ts)),
        value.map_or_else(mvcc::encode_tombstone, |v| mvcc::encode_live(&v)),
    );
}

/// Make forged versions at timestamps below 100 visible to a reopened sequencer.
fn seal(mem: &MemKv) {
    raw(mem, Keyspace::Meta, b"last_commit_ts".to_vec(), 100u64.to_be_bytes().to_vec());
}

fn view(f: &Fixture) -> Arc<super::catalog::CatalogView> {
    f.catalog.view_at(f.seq.visible_ts()).unwrap()
}

#[test]
fn commit_without_bit_is_refused() {
    let f = setup();
    let table = create_table(&f);
    let before = features(f.seq.kv());
    // A raw kind-19 put whose transaction never asked for TASK_CATALOG.
    assert!(put_task(&f, 2, &task(500, "inc-436", table), ROLES).is_err());
    assert_eq!(features(f.seq.kv()) & TASK_CATALOG, 0);
    assert_eq!(features(f.seq.kv()) & !ROLES, before);
    assert!(view(&f).tasks.is_empty());
    // TASK_CATALOG without role_catalog is not a valid feature set either.
    assert!(put_task(&f, 3, &task(500, "inc-436", table), TASK_CATALOG).is_err());
    assert_eq!(features(f.seq.kv()) & TASK_CATALOG, 0);
    assert!(view(&f).task_names.is_empty());
}

#[test]
fn commit_with_bit_sets_features() {
    let f = setup();
    let table = create_table(&f);
    let def = task(500, "inc-436", table);
    put_task(&f, 2, &def, TASK_CATALOG | ROLES).unwrap();
    assert_eq!(features(f.seq.kv()) & (TASK_CATALOG | ROLES), TASK_CATALOG | ROLES);
    let view = view(&f);
    assert_eq!(view.tasks.get(&500).map(|t| t.as_ref()), Some(&def));
    assert_eq!(view.task_names, BTreeMap::from([("inc-436".to_string(), 500)]));
    // Not part of the SQL catalog parts.
    assert_eq!(view.parts(), {
        let mut plain = view.as_ref().clone();
        plain.tasks.clear();
        plain.task_names.clear();
        plain.parts()
    });
}

#[test]
fn reopen_loads_task_into_view() {
    let f = setup();
    let table = create_table(&f);
    let live = task(500, "inc-436", table);
    let mut revoked = task(501, "inc-437", table);
    revoked.revoked = true;
    revoked.def_version = 2;
    put_task(&f, 2, &live, TASK_CATALOG | ROLES).unwrap();
    put_task(&f, 3, &revoked, TASK_CATALOG | ROLES).unwrap();
    let bits = features(f.seq.kv());
    content_check::check(f.seq.kv(), bits).unwrap();
    let reopened = open(Arc::new(f.mem.crash())).unwrap();
    let view = view(&reopened);
    assert_eq!(view.tasks.len(), 2);
    assert_eq!(view.tasks[&500].as_ref(), &live);
    assert_eq!(view.tasks[&501].as_ref(), &revoked);
    assert_eq!(view.task_names["inc-437"], 501);
}

#[test]
fn open_rejects_task_record_when_bit_cleared() {
    let f = setup();
    let table = create_table(&f);
    put_task(&f, 2, &task(500, "inc-436", table), TASK_CATALOG | ROLES).unwrap();
    let cleared = features(f.seq.kv()) & !TASK_CATALOG;
    raw(&f.mem, Keyspace::Meta, b"features".to_vec(), cleared.to_be_bytes().to_vec());
    assert_eq!(features(f.seq.kv()), cleared);
    assert!(matches!(
        content_check::check(f.seq.kv(), cleared),
        Err(FormatError::Corrupt(_))
    ));
}

#[test]
fn open_rejects_duplicate_task_name() {
    let f = setup();
    let table = create_table(&f);
    put_task(&f, 2, &task(500, "inc-436", table), TASK_CATALOG | ROLES).unwrap();
    // The overlay check refuses a second task with the same name.
    assert!(put_task(&f, 3, &task(501, "inc-436", table), TASK_CATALOG | ROLES).is_err());
    assert_eq!(view(&f).tasks.len(), 1);
    // Forged on disk (revoked, so only the name rule applies), both open
    // paths refuse it.
    let mem = MemKv::new();
    for id in [500, 501] {
        let mut def = task(id, "inc-436", 77);
        def.revoked = true;
        raw_catalog(&mem, task_key(id), 5, Some(encode_task(&def).unwrap()));
    }
    seal(&mem);
    assert!(matches!(
        content_check::check(&mem, TASK_CATALOG | ROLES),
        Err(FormatError::Corrupt(_))
    ));
    assert!(matches!(open(Arc::new(mem)), Err(crate::CoreError::Corrupt(_))));
}

#[test]
fn open_rejects_live_task_on_missing_table() {
    let f = setup();
    let table = create_table(&f);
    // The commit path refuses it before anything is written.
    assert!(matches!(
        put_task(&f, 2, &task(500, "inc-436", table + 1000), TASK_CATALOG | ROLES),
        Err(TxnError::DependentObjects)
    ));
    // Dropping the table of a live task fails with 2BP01 too: the R3-106
    // dependency guard (task::catalog::check_table_ddl) refuses it while the
    // DDL is staged, before anything is written.
    put_task(&f, 3, &task(500, "inc-436", table), TASK_CATALOG | ROLES).unwrap();
    let mut tx = txn(&f, 4);
    assert!(matches!(
        f.catalog.drop_table(&mut tx, "shipments", None, &|| false),
        Err(super::catalog::CatalogError::Core(crate::CoreError::Sql(
            crate::relational::SqlError::DependentObjects
        )))
    ));
    drop(tx);
    assert!(view(&f).by_id.contains_key(&table));
    // Forged on disk, both open paths refuse it.
    let mem = MemKv::new();
    raw_catalog(&mem, task_key(500), 5, Some(encode_task(&task(500, "inc-436", 77)).unwrap()));
    seal(&mem);
    assert!(matches!(
        content_check::check(&mem, TASK_CATALOG | ROLES),
        Err(FormatError::Corrupt(_))
    ));
    assert!(matches!(open(Arc::new(mem)), Err(crate::CoreError::Corrupt(_))));
}

#[test]
fn revoked_task_on_missing_table_opens() {
    let mut def = task(500, "inc-436", 77);
    def.revoked = true;
    def.def_version = 2;
    let mem = MemKv::new();
    raw_catalog(&mem, task_key(500), 5, Some(encode_task(&def).unwrap()));
    seal(&mem);
    content_check::check(&mem, TASK_CATALOG | ROLES).unwrap();
    let f = open(Arc::new(mem)).unwrap();
    assert_eq!(view(&f).tasks[&500].as_ref(), &def);
    // The table of a revoked task may also be dropped.
    let g = setup();
    let table = create_table(&g);
    let mut revoked = task(500, "inc-436", table);
    revoked.revoked = true;
    put_task(&g, 2, &revoked, TASK_CATALOG | ROLES).unwrap();
    let mut tx = txn(&g, 3);
    g.catalog.drop_table(&mut tx, "shipments", None, &|| false).unwrap();
    g.catalog.commit_ddl(tx).unwrap();
    assert!(view(&g).tasks.contains_key(&500));
}

#[test]
fn tombstoned_task_key_still_requires_bit() {
    assert_eq!(
        content_check::required_bits_for_catalog_key(&task_key(500)).unwrap(),
        TASK_CATALOG
    );
    assert!(content_check::required_bits_for_catalog_key(&[19, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    assert!(content_check::required_bits_for_catalog_key(&[19, 0, 1]).is_err());
    let mem = MemKv::new();
    raw_catalog(&mem, task_key(500), 5, None);
    assert!(matches!(content_check::check(&mem, ROLES), Err(FormatError::Corrupt(_))));
    content_check::check(&mem, TASK_CATALOG | ROLES).unwrap();
    // A tombstone over a live version needs it too.
    let mem = MemKv::new();
    raw_catalog(&mem, task_key(500), 5, Some(encode_task(&task(500, "t", 77)).unwrap()));
    raw_catalog(&mem, task_key(500), 6, None);
    assert!(matches!(content_check::check(&mem, ROLES), Err(FormatError::Corrupt(_))));
    content_check::check(&mem, TASK_CATALOG | ROLES).unwrap();
    // The value check decodes the record.
    let mut value = encode_task(&task(500, "t", 77)).unwrap();
    assert_eq!(
        content_check::required_bits_for_catalog_value(&task_key(500), &value).unwrap(),
        TASK_CATALOG
    );
    value.push(0);
    assert!(content_check::required_bits_for_catalog_value(&task_key(500), &value).is_err());
    assert_eq!(decode_task(&task_key(500), &value[..value.len() - 1]).unwrap().name.as_str(), "t");
}

#[test]
fn bit_20_without_role_catalog_is_corrupt() {
    assert_eq!(TASK_CATALOG, 1 << 20);
    assert!(matches!(meta::check_features(TASK_CATALOG), Err(FormatError::Corrupt(_))));
    assert!(matches!(meta::check_features(TASK_CATALOG | 0x3F7), Err(FormatError::Corrupt(_)))); // 0x3FF minus retired bit 8 (GD-0074 E142)
    meta::check_features(TASK_CATALOG | ROLES).unwrap();
    let mut batch = WriteBatch::default();
    assert!(meta::put_features(&mut batch, TASK_CATALOG).is_err());
    assert!(batch.is_empty());
}

#[test]
fn unassigned_bits_are_unsupported() {
    for bit in [26, 35, 63] {
        let unknown = 1 << bit;
        for bits in [unknown, unknown | TASK_CATALOG | ROLES, unknown | TASK_CATALOG] {
            assert_eq!(
                meta::check_features(bits),
                Err(FormatError::Unsupported("feature bits".into()))
            );
        }
    }
}

#[test]
fn kind_24_requires_task_catalog_bit() {
    use crate::task::credential::{credential_key, encode_credential, CredentialRecord};
    let key = credential_key(500, 600);
    assert_eq!(content_check::required_bits_for_catalog_key(&key).unwrap(), TASK_CATALOG);
    for key in [credential_key(0, 600), credential_key(500, 0)] {
        assert!(content_check::required_bits_for_catalog_key(&key).is_err());
    }
    let record = CredentialRecord { issued_at: WallClockMs(1), expires_at: None, issued_by: "issuer".into(), verifier: [7; 32] };
    let encoded = encode_credential(&record).unwrap();
    assert_eq!(content_check::required_bits_for_catalog_value(&key, &encoded).unwrap(), TASK_CATALOG);
    let f = setup();
    let table = create_table(&f);
    put_task(&f, 2, &task(500, "t", table), TASK_CATALOG | ROLES).unwrap();
    let mut tx = txn(&f, 3);
    tx.put(Keyspace::Catalog, key.clone(), encoded).unwrap();
    // A persisted feature bit already admits subsequent credential writes.
    f.catalog.commit_ddl(tx).unwrap();
    let fresh = setup();
    let mut tx = txn(&fresh, 1);
    tx.delete(Keyspace::Catalog, key.clone()).unwrap();
    assert!(fresh.catalog.commit_ddl(tx).is_err());
    // Historical versions and tombstones must also witness the bit.
    for tombstone in [false, true] {
        let f = setup();
        raw_catalog(&f.mem, key.clone(), 1, Some(encode_credential(&record).unwrap()));
        if tombstone { raw_catalog(&f.mem, key.clone(), 2, None); }
        seal(&f.mem);
        assert!(content_check::check(f.mem.as_ref(), ROLES).is_err());
        if tombstone { content_check::check(f.mem.as_ref(), TASK_CATALOG | ROLES).unwrap(); }
    }
}

#[test]
fn view_loads_credentials_and_rejects_orphan() {
    use crate::task::credential::{credential_key, encode_credential, CredentialRecord};
    let f = setup();
    let table = create_table(&f);
    put_task(&f, 2, &task(500, "t", table), TASK_CATALOG | ROLES).unwrap();
    let record = CredentialRecord { issued_at: WallClockMs(1), expires_at: None, issued_by: "issuer".into(), verifier: [7; 32] };
    let put = |task, id, tx_id| {
        let mut tx = txn(&f, tx_id);
        tx.require_features(TASK_CATALOG | ROLES);
        tx.put(Keyspace::Catalog, credential_key(task, id), encode_credential(&record).unwrap()).unwrap();
        f.catalog.commit_ddl(tx)
    };
    put(500, 600, 3).unwrap();
    assert_eq!(view(&f).task_credentials[&600].0, 500);
    assert_eq!(*view(&f).task_credentials[&600].1, record);
    let reopened = open(f.mem.clone()).unwrap();
    assert_eq!(view(&reopened).task_credentials, view(&f).task_credentials);
    assert!(put(999, 601, 4).is_err());
    put_task(&f, 5, &task(501, "u", table), TASK_CATALOG | ROLES).unwrap();
    assert!(put(501, 600, 6).is_err());
    let mut tx = txn(&f, 7);
    tx.require_features(TASK_CATALOG);
    tx.delete(Keyspace::Catalog, credential_key(500, 600)).unwrap();
    f.catalog.commit_ddl(tx).unwrap();
    assert!(view(&f).task_credentials.is_empty());
    put(500, 600, 8).unwrap();
    // Full-load duplicate and orphan checks, bypassing the overlay.
    raw_catalog(&f.mem, credential_key(501, 600), 99, Some(encode_credential(&record).unwrap()));
    seal(&f.mem);
    assert!(matches!(open(f.mem.clone()), Err(crate::CoreError::Corrupt(_))));
    let orphan = setup();
    raw_catalog(&orphan.mem, credential_key(999, 601), 99, Some(encode_credential(&record).unwrap()));
    seal(&orphan.mem);
    assert!(matches!(open(orphan.mem.clone()), Err(crate::CoreError::Corrupt(_))));
}

#[test]
fn collation_and_credential_records_stay_separate() {
    use super::catalog_codec::{classify_catalog_key, collation_key, decode_collation, encode_collation, CatalogKind};
    use crate::task::credential::{credential_key, decode_credential, decode_credential_key, encode_credential, CredentialRecord};
    // Kind 22 (user collation) and kind 24 (task credential) share one id on purpose.
    let mut collation = crate::relational::collations::builtin("C").unwrap();
    collation.name = "mine".into();
    collation.schema = "public".into();
    let record = CredentialRecord { issued_at: WallClockMs(1), expires_at: None, issued_by: "issuer".into(), verifier: [7; 32] };
    let (coll_key, cred_key) = (collation_key(900), credential_key(500, 900));
    let (coll_value, cred_value) = (encode_collation(&collation).unwrap(), encode_credential(&record).unwrap());
    assert_eq!((coll_key[0], cred_key[0]), (22, 24));
    assert_eq!(classify_catalog_key(&coll_key).unwrap(), CatalogKind::Collation);
    assert_eq!(classify_catalog_key(&cred_key).unwrap(), CatalogKind::TaskCredential);
    assert_eq!(content_check::required_bits_for_catalog_key(&coll_key).unwrap(), meta::USER_COLLATIONS);
    assert_eq!(content_check::required_bits_for_catalog_key(&cred_key).unwrap(), TASK_CATALOG);
    // Neither key or value decodes as the other record type.
    let mut old_layout = cred_key.clone();
    old_layout[0] = 22;
    assert!(classify_catalog_key(&old_layout).is_err());
    assert!(decode_credential_key(&coll_key).is_err());
    assert!(decode_credential_key(&old_layout).is_err());
    assert!(decode_collation(&cred_value).is_err());
    assert!(decode_credential(&coll_value).is_err());

    let f = setup();
    let table = create_table(&f);
    put_task(&f, 2, &task(500, "t", table), TASK_CATALOG | ROLES).unwrap();
    let mut tx = txn(&f, 3);
    tx.require_features(TASK_CATALOG | ROLES | meta::USER_COLLATIONS);
    tx.put(Keyspace::Catalog, coll_key, coll_value).unwrap();
    tx.put(Keyspace::Catalog, cred_key, cred_value).unwrap();
    f.catalog.commit_ddl(tx).unwrap();
    let check = |v: &super::catalog::CatalogView| {
        assert_eq!(v.collations.len(), 1);
        assert_eq!(v.collations[&900], collation);
        assert_eq!(v.task_credentials.len(), 1);
        assert_eq!(v.task_credentials[&900].0, 500);
        assert_eq!(*v.task_credentials[&900].1, record);
    };
    check(&view(&f));
    let reopened = open(f.mem.clone()).unwrap();
    check(&view(&reopened));
    let all = features(f.mem.as_ref());
    assert_eq!(all & (TASK_CATALOG | meta::USER_COLLATIONS), TASK_CATALOG | meta::USER_COLLATIONS);
    content_check::check(f.mem.as_ref(), all).unwrap();
}
