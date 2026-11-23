use super::*;
use super::super::super::{codec, meta, mvcc, kv::{Durability, KvBackend, WriteBatch}};
use catalog_codec::sql_views::*;
fn fixture(ids: &Range<u64>) -> CatalogParts {
    let t = ids.start;
    let mut p = serial_parts(ids);
    p.ext = CatalogExt::default();
    p.ext.schemas.insert("s".into(), relational::schemas::Schema { id: t+2, owner: "owner".into() });
    p.sql_views.insert(t+3, SqlViewDef { schema: t+2, name: "v".into(),
        columns: vec![SqlViewColumn { id: t+4, name: "c".into(), storage_type: DataType::BigInt, pg_type_oid: 20, typmod: -1, source: Some((t, t+1)) }],
        select: "SELECT id FROM serial_table".into(),
        dependencies: vec![SqlViewDependency { relation: t, columns: vec![t+1] }] });
    p
}
#[test]
fn views_overlay_rollback_reopen_cache_and_guards() {
    let (mem, seq, locks, catalog) = setup();
    let ids = catalog.reserve_ids(8).unwrap();
    let mut wanted = fixture(&ids);
    wanted.sql_views.get_mut(&(ids.start+3)).unwrap().schema = 0;
    wanted.ext.schemas.clear();
    wanted.comments.insert((ids.start+3, 0), "public view".into());
    wanted.comments.insert((ids.start+3, ids.start+4), "public column".into());
    let mut tx = txn(&seq, &locks, 1);
    catalog.apply_diff(&mut tx, &empty(), &wanted, &ids, None, &|| false).unwrap();
    assert_eq!(catalog.parts(&mut tx).unwrap().sql_views, wanted.sql_views);
    tx.rollback();
    assert!(parts(&catalog, &seq).sql_views.is_empty());
    assert_eq!(features(seq.kv()), 0);
    let mut tx = txn(&seq, &locks, 2);
    catalog.apply_diff(&mut tx, &empty(), &wanted, &ids, None, &|| false).unwrap();
    catalog.commit_ddl(tx).unwrap();
    let bits = features(seq.kv());
    assert_ne!(bits & meta::SQL_VIEWS, 0);
    let mut reader = txn(&seq, &locks, 3);
    let cached = catalog.snapshot(&mut reader).unwrap();
    assert_eq!(cached.parts.sql_views, wanted.sql_views);
    assert!(Arc::ptr_eq(&cached, &catalog.snapshot(&mut reader).unwrap()));
    reader.rollback();
    let kv = Arc::new(Kv::new(Arc::new(mem.crash())));
    content_check::check(kv.as_ref(), bits).unwrap();
    let reopened_seq = Sequencer::open(kv.clone(), Arc::new(ManualClock::new(1)), SequencerConfig::default()).unwrap();
    let reopened = Catalog::open(reopened_seq.clone(), locks.clone(), Arc::new(SequenceAllocator::new(kv, 64))).unwrap();
    assert_eq!(parts(&reopened, &reopened_seq).sql_views, wanted.sql_views);
    assert_eq!(parts(&reopened, &reopened_seq).comments, wanted.comments);
    for live in [true, false] {
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Catalog, codec::user_key_seek_at(&view_key(ids.start+3), Ts(u64::MAX)),
            if live { mvcc::encode_live(&encode_view(wanted.sql_views.values().next().unwrap()).unwrap()) } else { mvcc::encode_tombstone() });
        mem.write(batch, Durability::Sync).unwrap();
        assert!(content_check::check(mem.as_ref(), bits & !meta::SQL_VIEWS).is_err());
    }
}
#[test]
fn views_validate_names_ids_dependencies_comments_and_drops() {
    let (_, seq, locks, catalog) = setup();
    let ids = catalog.reserve_ids(8).unwrap();
    let mut wanted = fixture(&ids);
    let t = ids.start;
    wanted.comments.insert((t+3, 0), "view".into());
    wanted.comments.insert((t+3, t+4), "column".into());
    parts_records(&wanted).unwrap();
    for case in 0..7 {
        let mut bad = wanted.clone();
        match case {
            0 => { bad.sql_views.get_mut(&(t+3)).unwrap().schema = 999; }
            1 => { bad.sql_views.get_mut(&(t+3)).unwrap().dependencies[0].relation = 999; }
            2 => { bad.sql_views.get_mut(&(t+3)).unwrap().dependencies[0].columns = vec![999]; }
            3 => { let v = bad.sql_views.get_mut(&(t+3)).unwrap(); v.dependencies[0] = SqlViewDependency { relation: t+3, columns: vec![t+4] }; }
            4 => { let table = bad.tables.remove("serial_table").unwrap(); bad.ext.table_schemas.insert(t, "s".into()); let mut table = table; table.name = "v".into(); bad.tables.insert("s\0v".into(), table); }
            5 => { bad.sql_views.get_mut(&(t+3)).unwrap().columns[0].id = t+1; }
            _ => { bad.comments.insert((t+3, 999), "bad".into()); }
        }
        assert!(parts_records(&bad).is_err(), "case {case}");
    }
    let mut tx = txn(&seq, &locks, 1);
    catalog.apply_diff(&mut tx, &empty(), &wanted, &ids, None, &|| false).unwrap();
    catalog.commit_ddl(tx).unwrap();
    let mut dropped = wanted.clone(); dropped.tables.clear();
    let mut tx = txn(&seq, &locks, 2);
    assert!(matches!(catalog.apply_diff(&mut tx, &wanted, &dropped, &ids, None, &|| false), Err(CoreError::Sql(SqlError::DependentObjects))));
    let mut changed = wanted.clone();
    changed.tables.get_mut("serial_table").unwrap().columns[0].id = t+7;
    assert!(matches!(catalog.apply_diff(&mut tx, &wanted, &changed, &ids, None, &|| false), Err(CoreError::Sql(SqlError::DependentObjects))));
    let mut chain = wanted.clone();
    let mut v = chain.sql_views[&(t+3)].clone();
    v.name = "w".into(); v.columns[0].id = t+6;
    v.dependencies = vec![SqlViewDependency { relation: t+3, columns: vec![t+4] }];
    v.columns[0].source = Some((t+3, t+4));
    chain.sql_views.insert(t+5, v);
    parts_records(&chain).unwrap();
    let mut removed = chain.clone(); removed.sql_views.remove(&(t+3)); removed.comments.clear();
    assert!(matches!(parts_records(&removed), Err(CoreError::Sql(SqlError::DependentObjects))));
    chain.sql_views.get_mut(&(t+3)).unwrap().dependencies = vec![SqlViewDependency { relation: t+5, columns: vec![t+6] }];
    chain.sql_views.get_mut(&(t+3)).unwrap().columns[0].source = Some((t+5, t+6));
    assert!(matches!(parts_records(&chain), Err(CoreError::Sql(SqlError::DependentObjects))));
    // Direct writes exercise commit-time validation against the latest catalog.
    tx.delete(Keyspace::Catalog, table_key(t)).unwrap();
    tx.delete(Keyspace::Catalog, name_key("serial_table")).unwrap();
    assert_eq!(catalog.commit_ddl(tx), Err(TxnError::DependentObjects));
    let mut tx = txn(&seq, &locks, 3);
    let def = changed.tables["serial_table"].clone();
    tx.put(Keyspace::Catalog, table_key(t), encode_entry(&TableEntry { primary_index_birth: None, def, heap: false, columns_ext: BTreeMap::new() }).unwrap()).unwrap();
    assert_eq!(catalog.commit_ddl(tx), Err(TxnError::DependentObjects));
    assert_eq!(parts(&catalog, &seq).sql_views, wanted.sql_views);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn write_old_binary_view_image() {
    let Ok(path) = std::env::var("GIGGLES_VIEW_IMAGE") else { return };
    drop(crate::Admin::create_engine(&path, crate::Limits::default(), crate::Engine::V2).unwrap());
    let kv = Arc::new(super::super::super::kv_rocksdb::RocksKv::open(std::path::Path::new(&path), 1 << 20).unwrap());
    let kv = Arc::new(Kv::new(kv));
    let seq = Sequencer::open(kv.clone(), Arc::new(ManualClock::new(1)), SequencerConfig::default()).unwrap();
    let locks = Arc::new(LockManager::new(LockConfig::default()));
    let catalog = Catalog::open(seq.clone(), locks.clone(), Arc::new(SequenceAllocator::new(kv, 64))).unwrap();
    let ids = catalog.reserve_ids(8).unwrap();
    let mut tx = txn(&seq, &locks, 1);
    let mut wanted = fixture(&ids);
    wanted.sql_views.get_mut(&(ids.start+3)).unwrap().schema = 0;
    catalog.apply_diff(&mut tx, &empty(), &wanted, &ids, None, &|| false).unwrap();
    catalog.commit_ddl(tx).unwrap();
    content_check::check(seq.kv(), features(seq.kv())).unwrap();
}
#[test]
fn loaded_catalog_limits_keep_their_classification() {
    assert_eq!(loaded_catalog_error(CoreError::Limit), CoreError::Limit);
    assert!(matches!(loaded_catalog_error(CoreError::Sql(SqlError::InvalidSchema)), CoreError::Corrupt(_)));
}
#[test]
fn view_schema_race_and_relation_namespace_errors() {
    let (_, seq, locks, catalog) = setup();
    let ids = catalog.reserve_ids(8).unwrap();
    let wanted = fixture(&ids);
    let mut old = wanted.clone(); old.sql_views.clear();
    let mut tx = txn(&seq, &locks, 1);
    catalog.apply_diff(&mut tx, &empty(), &old, &ids, None, &|| false).unwrap();
    catalog.commit_ddl(tx).unwrap();
    let mut dropped = old.clone(); dropped.ext.schemas.clear();
    let mut drop = txn(&seq, &locks, 2);
    catalog.apply_diff(&mut drop, &old, &dropped, &ids, None, &|| false).unwrap();
    let mut create = txn(&seq, &locks, 3);
    catalog.apply_diff(&mut create, &old, &wanted, &ids, None, &|| false).unwrap();
    catalog.commit_ddl(create).unwrap();
    assert!(matches!(catalog.commit_ddl(drop), Err(TxnError::Serialization(_))));
    let mut public = wanted.clone();
    let v = public.sql_views.get_mut(&(ids.start+3)).unwrap(); v.schema = 0; v.name = "serial_table".into();
    assert!(matches!(parts_records(&public), Err(CoreError::Sql(SqlError::Duplicate))));
    for sequence in [false, true] {
        let mut p = wanted.clone();
        let mut seq = serial_parts(&ids).ext.sequences.into_values().next().unwrap(); seq.name = "s\0v".into(); seq.owned_by = None; seq.identity = false;
        if sequence { p.ext.sequences.insert(ids.start+5, seq); }
        else { p.indexes.push(relational::secondary_index::Descriptor { id: ids.start+5, table: ids.start, name: "s\0v".into(), columns: vec![ids.start+1], unique: false, predicate: None, key_encoding: 1 }); }
        assert!(matches!(parts_records(&p), Err(CoreError::Sql(SqlError::Duplicate))));
    }
    let mut p = wanted.clone();
    p.tables.get_mut("serial_table").unwrap().columns.push(ColumnDef { id: ids.start+7, name: "other".into(), data_type: DataType::BigInt, not_null: false });
    p.sql_views.get_mut(&(ids.start+3)).unwrap().dependencies[0].columns = vec![ids.start+7, ids.start+1];
    assert!(matches!(parts_records(&p), Err(CoreError::Corrupt(_))));
}
#[test]
fn view_column_rewrite_waiting_behind_comment_serializes() {
    use std::{thread, time::Instant};
    for rewrite_first in [false, true] {
    let (_, seq, locks, catalog) = setup(); let catalog = Arc::new(catalog);
    let ids = catalog.reserve_ids(8).unwrap(); let old = fixture(&ids);
    let mut tx = txn(&seq, &locks, 1);
    catalog.apply_diff(&mut tx, &empty(), &old, &ids, None, &|| false).unwrap(); catalog.commit_ddl(tx).unwrap();
    let mut comment = txn(&seq, &locks, 2); let mut wanted = old.clone();
    wanted.comments.insert((ids.start+3, ids.start+4), "concurrent".into());
    let mut changed = old.clone(); changed.sql_views.get_mut(&(ids.start+3)).unwrap().columns[0].id = ids.start+7;
    let (wanted, changed) = if rewrite_first { (changed, wanted) } else { (wanted, changed) };
    catalog.apply_diff(&mut comment, &old, &wanted, &ids, None, &|| false).unwrap();
    let (c, s, l) = (catalog.clone(), seq.clone(), locks.clone());
    let rewrite = thread::spawn(move || {
        let mut tx = txn(&s, &l, 3);
        c.apply_diff(&mut tx, &old, &changed, &ids, None, &|| false).unwrap(); c.commit_ddl(tx)
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while locks.waiting_count() == 0 { assert!(Instant::now() < deadline); thread::yield_now(); }
    catalog.commit_ddl(comment).unwrap();
    assert!(matches!(rewrite.join().unwrap(), Err(TxnError::Serialization(_))));
    }
}

#[test]
fn invalid_view_bindings_fail_commit_content_check_and_open() {
    let (mem, seq, locks, catalog) = setup(); let ids = catalog.reserve_ids(8).unwrap();
    let wanted = fixture(&ids); let view = &wanted.sql_views[&(ids.start+3)];
    let mut tx = txn(&seq, &locks, 1);
    catalog.apply_diff(&mut tx, &empty(), &wanted, &ids, None, &|| false).unwrap();
    catalog.commit_ddl(tx).unwrap();
    let mut bad = encode_view(view).unwrap(); let end = bad.len();
    bad[end-8..].copy_from_slice(&999u64.to_be_bytes());
    let mut tx = txn(&seq, &locks, 2);
    tx.put(Keyspace::Catalog, view_key(ids.start+3), bad.clone()).unwrap();
    assert!(matches!(catalog.commit_ddl(tx), Err(TxnError::Kv(KvError::Corrupt(_)))));
    assert_eq!(parts(&catalog, &seq).sql_views, wanted.sql_views);
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Catalog, codec::user_key_seek_at(&view_key(ids.start+3), seq.visible_ts()), mvcc::encode_live(&bad));
    mem.write(batch, Durability::Sync).unwrap();
    assert!(matches!(content_check::check(seq.kv(), features(seq.kv())), Err(super::super::super::meta::FormatError::Corrupt(_))));
    assert!(matches!(Catalog::open(seq.clone(), locks, Arc::new(SequenceAllocator::new(Arc::new(Kv::new(mem)), 64))), Err(CoreError::Corrupt(_))));
}
