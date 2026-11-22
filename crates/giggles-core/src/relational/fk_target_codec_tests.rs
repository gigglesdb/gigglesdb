use super::*;
use bincode::Options;
fn codec() -> impl Options { bincode::DefaultOptions::new().with_fixint_encoding().with_big_endian().reject_trailing_bytes() }
fn fk(target: ReferencedKey) -> Constraint {
    Constraint { id: 20, name: "fk".into(), kind: ConstraintKind::ForeignKey {
        columns: vec![5, 6], ref_table: 1, ref_columns: vec![3, 2], on_delete: Action::NoAction,
        on_update: Action::NoAction, not_valid: true, deferral: Deferral::NotDeferrable, target,
    } }
}
fn fixture() -> (Vec<TableDef>, CatalogExt, Vec<secondary_index::Descriptor>) {
    let table = |id, a, b| TableDef { id, name: format!("t{id}"), columns: vec![
        ColumnDef { id: a, name: "a".into(), data_type: DataType::BigInt, not_null: true },
        ColumnDef { id: b, name: "b".into(), data_type: DataType::BigInt, not_null: true },
    ], primary_key: vec![0, 1] };
    let mut ext = CatalogExt::default();
    ext.primary_index_birth.insert(1, 10);
    ext.constraints.insert(4, vec![fk(ReferencedKey::Secondary(11))]);
    Relational::normalize_ext(&mut ext);
    (vec![table(1, 2, 3), table(4, 5, 6)], ext, vec![secondary_index::Descriptor {
        id: 11, table: 1, name: "u".into(), columns: vec![2, 3], key_encoding: 1, unique: true,
    }])
}
#[test]
fn extension_v8_mixed_targets_and_old_adapter_bytes() {
    let (_, mut ext, _) = fixture();
    ext.constraints.get_mut(&4).unwrap().extend([fk(ReferencedKey::Unknown), fk(ReferencedKey::PrimaryKey)]);
    let bytes = codec().serialize(&ext).unwrap();
    assert_eq!(&bytes[..4], &8u32.to_be_bytes());
    assert_eq!(codec().deserialize::<CatalogExt>(&bytes).unwrap(), ext);
    for n in 0..bytes.len() { assert!(codec().deserialize::<CatalogExt>(&bytes[..n]).is_err()); }
    assert_eq!(ReferencedKey::default(), ReferencedKey::Unknown);
    // The frozen v2-7 FK tuple must still end in deferral, with no target bytes.
    let frozen = ConstraintV2To7::from(&fk(ReferencedKey::Unknown));
    let tuple = (20u64, "fk", 1u32, vec![5u64, 6], 1u64, vec![3u64, 2], 0u32, 0u32, true, 0u32);
    assert_eq!(codec().serialize(&frozen).unwrap(), codec().serialize(&tuple).unwrap());
    let old: Constraint = codec().deserialize::<ConstraintV2To7>(&codec().serialize(&tuple).unwrap()).unwrap().into();
    assert!(matches!(old.kind, ConstraintKind::ForeignKey { target: ReferencedKey::Unknown, .. }));
}
#[test]
fn extension_v8_rejects_version_tag_empty_and_truncated_births() {
    let (_, mut ext, _) = fixture();
    ext.constraints.clear();
    let bytes = codec().serialize(&ext).unwrap();
    let mut unknown = bytes.clone(); unknown[..4].copy_from_slice(&12u32.to_be_bytes());
    assert!(codec().deserialize::<CatalogExt>(&unknown).unwrap_err().to_string().contains(UNSUPPORTED_CATALOG_EXT));
    // Seven empty maps followed by a birth map.
    let mut empty = codec().serialize(&(8u32, [0u64; 8])).unwrap();
    assert!(codec().deserialize::<CatalogExt>(&empty).unwrap_err().to_string().contains("noncanonical"));
    empty[60..68].copy_from_slice(&u64::MAX.to_be_bytes());
    assert!(codec().deserialize::<CatalogExt>(&empty).is_err());
    ext.primary_index_birth.clear(); ext.constraints.insert(4, vec![fk(ReferencedKey::PrimaryKey)]);
    let mut bad = codec().serialize(&ext).unwrap();
    let at = bad.len() - 52; // target precedes six remaining map lengths.
    bad[at..at + 4].copy_from_slice(&3u32.to_be_bytes());
    assert!(codec().deserialize::<CatalogExt>(&bad).is_err());
}
#[test]
fn target_validator_accepts_permutations_and_shared_dependencies() {
    let (tables, mut ext, indexes) = fixture();
    let mut other = fk(ReferencedKey::Secondary(11)); other.id += 1; other.name = "fk2".into();
    ext.constraints.get_mut(&4).unwrap().push(other);
    assert_eq!(validate_key_targets(&ext, tables.iter(), indexes.iter(), 30), Ok(()));
    for c in ext.constraints.get_mut(&4).unwrap() {
        if let ConstraintKind::ForeignKey { target, .. } = &mut c.kind { *target = ReferencedKey::PrimaryKey; }
    }
    assert_eq!(validate_key_targets(&ext, tables.iter(), indexes.iter(), 30), Ok(()));
}
#[test]
fn target_validator_rejects_forged_secondary_and_primary() {
    let (mut tables, mut ext, indexes) = fixture();
    for target in [ReferencedKey::Secondary(0), ReferencedKey::Secondary(30), ReferencedKey::Secondary(12)] {
        ext.constraints.insert(4, vec![fk(target)]);
        assert!(validate_key_targets(&ext, tables.iter(), indexes.iter(), 30).is_err());
    }
    ext.constraints.insert(4, vec![fk(ReferencedKey::Secondary(11))]);
    for (table, unique, columns) in [(4, true, vec![2, 3]), (1, false, vec![2, 3]), (1, true, vec![2])] {
        let mut bad = indexes.clone(); bad[0].table = table; bad[0].unique = unique; bad[0].columns = columns;
        assert!(validate_key_targets(&ext, tables.iter(), bad.iter(), 30).is_err());
    }
    ext.constraints.insert(4, vec![fk(ReferencedKey::PrimaryKey)]);
    tables[0].primary_key = vec![0];
    assert!(validate_key_targets(&ext, tables.iter(), indexes.iter(), 30).is_err());
    tables[0].columns[0].name = HIDDEN_ROWID.into();
    ext.primary_index_birth.clear();
    assert!(validate_key_targets(&ext, tables.iter(), indexes.iter(), 30).is_err());
}
#[test]
fn birth_validator_rejects_zero_range_collisions_and_wrong_table() {
    let (mut tables, mut ext, indexes) = fixture();
    for birth in [0, 30, 1, 2, 11, 20] {
        ext.primary_index_birth.insert(1, birth);
        assert!(validate_key_targets(&ext, tables.iter(), indexes.iter(), 30).is_err());
    }
    ext.primary_index_birth.insert(1, 10); ext.primary_index_birth.insert(4, 10);
    assert!(validate_key_targets(&ext, tables.iter(), indexes.iter(), 30).is_err());
    ext.primary_index_birth.remove(&4); ext.primary_index_birth.insert(99, 12);
    assert!(validate_key_targets(&ext, tables.iter(), indexes.iter(), 30).is_err());
    ext.primary_index_birth.remove(&99); tables[0].primary_key.clear();
    assert!(validate_key_targets(&ext, tables.iter(), indexes.iter(), 30).is_err());
}

#[test]
fn versions_two_through_seven_keep_the_frozen_fk_layout() {
    for version in 2..=7 {
        let mut ext = CatalogExt::default();
        let mut c = fk(ReferencedKey::Unknown);
        if let ConstraintKind::ForeignKey { deferral, .. } = &mut c.kind { *deferral = Deferral::DeferrableImmediate; }
        ext.constraints.insert(4, vec![c]);
        if version >= 3 { ext.columns.insert(5, ColumnExt { text_kind: Some(TextKind::Varchar), ..ColumnExt::default() }); }
        if version >= 4 { ext.logical_kinds.insert(6, LogicalKind::Uuid); }
        if version >= 5 { ext.schemas.insert("s".into(), schemas::Schema { id: 9, owner: "giggles".into() }); }
        if version >= 6 { ext.columns.get_mut(&5).unwrap().type_id = Some(12); }
        if version >= 7 { ext.constraints.get_mut(&4).unwrap().push(Constraint { id: 21, name: "uq".into(), kind: ConstraintKind::Unique { columns: vec![5], index: 11 } }); }
        Relational::normalize_ext(&mut ext);
        assert_eq!(ext.ext_version, version);
        let mut expected = codec().serialize(&version).unwrap();
        if version >= 6 { expected.extend(codec().serialize(&ext.columns).unwrap()); }
        else if version >= 3 {
            let columns: BTreeMap<_, _> = ext.columns.iter().map(|(id, c)| (*id, ColumnV3 {
                typmod: c.typmod, default: c.default.clone(), identity: c.identity.clone(), text_kind: c.text_kind,
            })).collect();
            expected.extend(codec().serialize(&columns).unwrap());
        } else { expected.extend(codec().serialize(&BTreeMap::<u64, LegacyColumnExt>::new()).unwrap()); }
        let frozen: BTreeMap<_, Vec<_>> = ext.constraints.iter().map(|(id, cs)| (*id, cs.iter().map(ConstraintV2To7::from).collect())).collect();
        expected.extend(codec().serialize(&frozen).unwrap());
        expected.extend(codec().serialize(&ext.sequences).unwrap()); expected.extend(codec().serialize(&ext.types).unwrap());
        if version >= 4 { expected.extend(codec().serialize(&ext.logical_kinds).unwrap()); }
        if version >= 5 { expected.extend(codec().serialize(&ext.schemas).unwrap()); expected.extend(codec().serialize(&ext.table_schemas).unwrap()); }
        assert_eq!(codec().serialize(&ext).unwrap(), expected);
        let old: CatalogExt = codec().deserialize(&expected).unwrap();
        assert_eq!(old, ext); assert_eq!(codec().serialize(&old).unwrap(), expected);
    }
}
#[test]
fn births_cannot_collide_with_types_enum_labels_or_schemas() {
    let (tables, mut ext, indexes) = fixture();
    ext.types.insert(12, TypeDef::Enum { name: "e".into(), schema: "public".into(), labels: vec!["a".into()] });
    ext.schemas.insert("s".into(), schemas::Schema { id: 14, owner: "giggles".into() });
    for birth in [12, 13, 14] {
        ext.primary_index_birth.insert(1, birth);
        assert!(validate_key_targets(&ext, tables.iter(), indexes.iter(), 30).is_err());
    }
}

#[test]
fn legacy_paired_records_validate_targets_after_index_restore() {
    let mut sql = Relational::default();
    for text in ["CREATE TABLE p(id bigint PRIMARY KEY)", "CREATE UNIQUE INDEX u ON p(id)",
        "CREATE TABLE c(id bigint REFERENCES p(id))"] {
        sql.execute(&Prepared::parse(text, 1, sql.epoch).unwrap(), &[], None).unwrap();
    }
    let p = sql.tables["p"].id; let c = sql.tables["c"].id;
    let index = sql.secondary_descriptors_engine()[0].id;
    let birth = sql.next_id; sql.next_id += 1;
    std::sync::Arc::make_mut(&mut sql.ext).primary_index_birth.insert(p, birth);
    if let ConstraintKind::ForeignKey { target, .. } = &mut std::sync::Arc::make_mut(&mut sql.ext).constraints.get_mut(&c).unwrap()[0].kind { *target = ReferencedKey::Secondary(index); }
    Relational::normalize_ext(std::sync::Arc::make_mut(&mut sql.ext));
    let (base, payload) = sql.secondary_persistence_engine(|| false).unwrap();
    let mut restored: Relational = bincode::deserialize(&base).unwrap();
    let pending = || secondary_index::codec::decode_pending(&payload, || false).unwrap();
    restored.secondary_restore_engine(pending(), || false).unwrap();
    assert_eq!(restored, sql);
    for target in [ReferencedKey::Secondary(999), ReferencedKey::Secondary(sql.tables["c"].id)] {
        let mut bad: Relational = bincode::deserialize(&base).unwrap();
        if let ConstraintKind::ForeignKey { target: saved, .. } = &mut std::sync::Arc::make_mut(&mut bad.ext).constraints.get_mut(&c).unwrap()[0].kind { *saved = target; }
        let before = bad.clone();
        assert_eq!(bad.secondary_restore_engine(pending(), || false), Err(SqlError::Type));
        assert_eq!(bad, before);
    }
}
