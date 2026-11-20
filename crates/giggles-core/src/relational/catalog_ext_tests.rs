use super::*;
use bincode::Options;

#[test]
fn constraint_extension_frozen_v1_and_canonical_v2_goldens() {
    // SHA256: 83f6ae813171322a8c2d98e58511cdf92da1035832f003c17216d8dbf6e04479
    const V1: &str = "0000000100000000000000000000000000000001000000000000000100000000000000010000000000000002000000000000000166000000010000000000000001000000000000000300000000000000040000000000000001000000000000000500000000000000000000000000000000000000000000000000";
    // SHA256: d5d280c263193a875365740623adc86257553ef5de6226ad3ed0071344c2bb56
    const V2: &str = "000000020000000000000000000000000000000100000000000000010000000000000001000000000000000200000000000000016600000001000000000000000100000000000000030000000000000004000000000000000100000000000000050000000000000000000000000200000000000000000000000000000000";
    let bytes = |s: &str| {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect::<Vec<_>>()
    };
    let mut ext = CatalogExt::default();
    ext.constraints.insert(
        1,
        vec![Constraint {
            id: 2,
            name: "f".into(),
            kind: ConstraintKind::ForeignKey {
                columns: vec![3],
                ref_table: 4,
                ref_columns: vec![5],
                on_delete: Action::NoAction,
                on_update: Action::NoAction,
                not_valid: false,
                deferral: Deferral::NotDeferrable,
                target: crate::relational::catalog_ext::ReferencedKey::Unknown,
            },
        }],
    );
    assert_eq!(codec().serialize(&ext).unwrap(), bytes(V1));
    assert_eq!(codec().deserialize::<CatalogExt>(&bytes(V1)).unwrap(), ext);
    if let ConstraintKind::ForeignKey { deferral, .. } =
        &mut ext.constraints.get_mut(&1).unwrap()[0].kind
    {
        *deferral = Deferral::DeferrableDeferred;
    }
    ext.ext_version = 2;
    assert_eq!(codec().serialize(&ext).unwrap(), bytes(V2));
    assert_eq!(codec().deserialize::<CatalogExt>(&bytes(V2)).unwrap(), ext);
    let mut noncanonical = bytes(V2);
    // The final FK field is the four-byte deferral discriminant.
    let at = noncanonical.len() - 20;
    noncanonical[at..at + 4].fill(0);
    assert!(
        codec()
            .deserialize::<CatalogExt>(&noncanonical)
            .unwrap_err()
            .to_string()
            .contains("noncanonical")
    );
    let mut unknown = bytes(V1);
    unknown[..4].copy_from_slice(&12u32.to_be_bytes());
    assert!(
        codec()
            .deserialize::<CatalogExt>(&unknown)
            .unwrap_err()
            .to_string()
            .contains(catalog_ext::UNSUPPORTED_CATALOG_EXT)
    );
}

fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes()
}

fn fixture(ty: DataType) -> (Relational, u64) {
    let mut state = Relational::default();
    let p = Prepared::parse("CREATE TABLE t (id BIGINT PRIMARY KEY, value TEXT)", 1, 0).unwrap();
    state.execute(&p, &[], None).unwrap();
    let table = std::sync::Arc::make_mut(&mut state.tables).get_mut("t").unwrap();
    table.columns[1].data_type = ty;
    let id = table.columns[1].id;
    (state, id)
}

fn legacy(state: &Relational) -> Vec<u8> {
    codec()
        .serialize(&(
            1u32,
            state.generation,
            state.epoch,
            state.next_id,
            state.next_version,
            state.tables.as_ref(),
            &state.rows,
        ))
        .unwrap()
}

fn wire(state: &Relational) -> Vec<u8> {
    codec()
        .serialize(&(
            2u32,
            state.generation,
            state.epoch,
            state.next_id,
            state.next_version,
            state.tables.as_ref(),
            &state.rows,
            state.ext.as_ref(),
        ))
        .unwrap()
}

fn numeric(p: i32, s: i32) -> i32 {
    ((p << 16) | (s & 0x7ff)) + 4
}

fn sequence(table: u64, column: u64) -> SequenceDef {
    SequenceDef {
        name: "t_identity_seq".into(),
        data_type: DataType::BigInt,
        start: 1,
        increment: 1,
        min: 1,
        max: i64::MAX,
        cache: 1,
        cycle: false,
        owned_by: Some((table, column)),
        identity: true,
        incarnation: 1,
    }
}

#[test]
fn catalog_ext_legacy_is_exactly_seven_fields() {
    let (state, _) = fixture(DataType::Text);
    let bytes = legacy(&state);
    assert_eq!(codec().serialize(&state).unwrap(), bytes);
    assert_eq!(codec().deserialize::<Relational>(&bytes).unwrap(), state);
    assert_eq!(
        bincode::deserialize::<Relational>(&bincode::serialize(&state).unwrap()).unwrap(),
        state
    );
}

#[test]
fn catalog_ext_all_metadata_roundtrips_and_drop_cleans_owned_entries() {
    let (mut state, column) = fixture(DataType::BigInt);
    let table = state.tables["t"].clone();
    let seq = state.add_sequence(sequence(table.id, column)).unwrap();
    state
        .set_column_identity(
            column,
            Some(Identity {
                always: true,
                sequence_id: seq,
            }),
        )
        .unwrap();
    state.set_column_default(column, Some("42".into())).unwrap();
    for (name, kind) in [
        (
            "positive",
            ConstraintKind::Check {
                expr: "value > 0".into(),
                not_valid: true,
            },
        ),
        (
            "parent",
            ConstraintKind::ForeignKey {
                columns: vec![column],
                ref_table: table.id,
                ref_columns: vec![table.columns[0].id],
                on_delete: Action::Cascade,
                on_update: Action::NoAction,
                not_valid: false,
                deferral: Deferral::NotDeferrable,
                target: crate::relational::catalog_ext::ReferencedKey::Unknown,
            },
        ),
        ("explicit_pkey", ConstraintKind::PrimaryKey),
    ] {
        state
            .add_constraint(
                table.id,
                Constraint {
                    id: state.next_id,
                    name: name.into(),
                    kind,
                },
            )
            .unwrap();
    }
    state.validate().unwrap();
    assert_eq!(state.table_constraints(table.id).len(), 3);
    assert!(state.column_default(column).is_some());
    assert!(state.column_identity(column).is_some());
    assert!(state.sequence_by_name("t_identity_seq").is_some());
    assert_eq!(
        codec()
            .deserialize::<Relational>(&codec().serialize(&state).unwrap())
            .unwrap(),
        state
    );
    assert_eq!(
        bincode::deserialize::<Relational>(&bincode::serialize(&state).unwrap()).unwrap(),
        state
    );
    let mut removed = state.clone();
    removed.set_column_identity(column, None).unwrap();
    removed.set_column_default(column, None).unwrap();
    for name in ["positive", "parent", "explicit_pkey"] {
        removed.remove_constraint(table.id, name).unwrap();
    }
    removed.remove_sequence(seq).unwrap();
    assert!(removed.catalog_ext().is_empty());
    assert_eq!(codec().serialize(&removed).unwrap(), legacy(&removed));
    assert!(state.drop_table("t").unwrap());
    assert!(state.catalog_ext().is_empty());
    assert_eq!(codec().serialize(&state).unwrap(), legacy(&state));
}

#[test]
fn catalog_ext_orphan_identity_and_constraint_reject_decode() {
    let (state, column) = fixture(DataType::BigInt);
    for identity_sequence in [false, true] {
        let mut invalid = state.clone();
        let table = invalid.tables["t"].id;
        let mut seq = sequence(table, column);
        seq.identity = identity_sequence;
        let seq_id = invalid.add_sequence(seq).unwrap();
        std::sync::Arc::make_mut(&mut invalid.ext).columns.insert(
            column,
            ColumnExt {
                identity: Some(Identity {
                    always: false,
                    sequence_id: if identity_sequence { u64::MAX } else { seq_id },
                }),
                ..Default::default()
            },
        );
        assert!(codec().deserialize::<Relational>(&wire(&invalid)).is_err());
    }
    let mut invalid = state;
    std::sync::Arc::make_mut(&mut invalid.ext).constraints.insert(
        u64::MAX,
        vec![Constraint {
            id: invalid.next_id,
            name: "orphan".into(),
            kind: ConstraintKind::PrimaryKey,
        }],
    );
    invalid.next_id += 1;
    assert!(codec().deserialize::<Relational>(&wire(&invalid)).is_err());
}

#[test]
fn catalog_ext_foreign_keys_owned_sequences_and_ids_are_validated() {
    let (state, column) = fixture(DataType::BigInt);
    let table = state.tables["t"].clone();
    for (columns, ref_table, ref_columns) in [
        (vec![u64::MAX], table.id, vec![table.columns[0].id]),
        (vec![column], u64::MAX, vec![table.columns[0].id]),
        (vec![column], table.id, vec![u64::MAX]),
        (vec![column], table.id, vec![]),
    ] {
        let mut invalid = state.clone();
        let constraint = Constraint {
            id: invalid.next_id,
            name: "bad_fk".into(),
            kind: ConstraintKind::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                on_delete: Action::NoAction,
                on_update: Action::NoAction,
                not_valid: false,
                deferral: Deferral::NotDeferrable,
                target: crate::relational::catalog_ext::ReferencedKey::Unknown,
            },
        };
        let before = invalid.clone();
        assert!(
            invalid
                .add_constraint(table.id, constraint.clone())
                .is_err()
        );
        assert_eq!(invalid, before);
        std::sync::Arc::make_mut(&mut invalid.ext).constraints.insert(table.id, vec![constraint]);
        invalid.next_id += 1;
        assert!(codec().deserialize::<Relational>(&wire(&invalid)).is_err());
    }
    for owner in [(u64::MAX, column), (table.id, u64::MAX)] {
        let mut invalid = state.clone();
        let mut seq = sequence(table.id, column);
        seq.owned_by = Some(owner);
        let before = invalid.clone();
        assert!(invalid.add_sequence(seq.clone()).is_err());
        assert_eq!(invalid, before);
        std::sync::Arc::make_mut(&mut invalid.ext).sequences.insert(invalid.next_id, seq);
        invalid.next_id += 1;
        assert!(codec().deserialize::<Relational>(&wire(&invalid)).is_err());
    }
    for id in [column, state.next_id] {
        let mut invalid = state.clone();
        std::sync::Arc::make_mut(&mut invalid.ext).constraints.insert(
            table.id,
            vec![Constraint {
                id,
                name: "bad_id".into(),
                kind: ConstraintKind::PrimaryKey,
            }],
        );
        assert!(codec().deserialize::<Relational>(&wire(&invalid)).is_err());
    }
}

#[test]
fn catalog_ext_typmods_roundtrip_and_clear_to_legacy() {
    for (ty, typmod) in [
        (DataType::Numeric, numeric(10, 2)),
        (DataType::Numeric, numeric(2, -3)),
        (DataType::Numeric, numeric(3, 5)),
        (DataType::Numeric, numeric(1000, 1000)),
        (DataType::Timestamp, 0),
        (DataType::TimeTz, 6),
        (DataType::Interval, (0x7fff << 16) | 3),
        (DataType::Interval, ((8 | 1024 | 2048 | 4096) << 16) | 3),
        (DataType::Interval, (6 << 16) | 0xffff),
    ] {
        let (mut state, column) = fixture(ty);
        let before = legacy(&state);
        state.set_column_typmod(column, Some(typmod)).unwrap();
        assert_eq!(state.column_typmod(column), typmod);
        let decoded: Relational = codec()
            .deserialize(&codec().serialize(&state).unwrap())
            .unwrap();
        assert_eq!(decoded, state);
        state.set_column_typmod(column, None).unwrap();
        assert_eq!(state.column_typmod(column), -1);
        assert_eq!(codec().serialize(&state).unwrap(), before);
    }
}

#[test]
fn catalog_ext_invalid_typmods_reject_decode_and_mutation_atomically() {
    for (ty, typmod) in [
        (DataType::Numeric, numeric(0, 0)),
        (DataType::Numeric, numeric(1001, 0)),
        (DataType::Numeric, numeric(10, 1001)),
        (DataType::Numeric, numeric(10, -1001)),
        (DataType::Numeric, numeric(10, 2) | (1 << 11)),
        (DataType::Timestamp, 7),
        (DataType::Timestamp, -2),
        (DataType::Interval, (0x7fff << 16) | 7),
        (DataType::Interval, (3 << 16) | 0xffff),
        (DataType::Interval, (0x7fff << 16) | 0xffff),
        (DataType::Integer, 0),
        (DataType::Text, 5),
    ] {
        let (mut state, column) = fixture(ty.clone());
        let before = state.clone();
        assert!(
            state.set_column_typmod(column, Some(typmod)).is_err(),
            "{ty:?} {typmod}"
        );
        assert_eq!(state, before);
        std::sync::Arc::make_mut(&mut state.ext).columns.insert(
            column,
            ColumnExt {
                typmod: Some(typmod),
                ..Default::default()
            },
        );
        assert!(
            codec().deserialize::<Relational>(&wire(&state)).is_err(),
            "{ty:?} {typmod}"
        );
    }
}

#[test]
fn catalog_ext_orphan_columns_and_empty_format_two_reject_decode() {
    let (state, column) = fixture(DataType::Numeric);
    assert!(codec().deserialize::<Relational>(&wire(&state)).is_err());
    for id in [column, u64::MAX] {
        for ext in [
            ColumnExt {
                typmod: Some(numeric(10, 2)),
                ..Default::default()
            },
            ColumnExt {
                default: Some("42".into()),
                ..Default::default()
            },
        ] {
            let mut invalid = state.clone();
            std::sync::Arc::make_mut(&mut invalid.ext).columns.insert(id, ext);
            if id == column {
                std::sync::Arc::make_mut(&mut invalid.tables).get_mut("t").unwrap().columns.pop();
            }
            assert!(codec().deserialize::<Relational>(&wire(&invalid)).is_err());
        }
    }
}

#[test]
fn catalog_ext_default_limits_and_unknown_columns_are_atomic() {
    let (mut state, column) = fixture(DataType::Text);
    let before = state.clone();
    assert!(state.set_column_typmod(u64::MAX, None).is_err());
    assert!(state.set_column_default(u64::MAX, None).is_err());
    assert!(state.set_column_identity(u64::MAX, None).is_err());
    assert_eq!(state, before);
    for (id, value) in [(u64::MAX, "42".into()), (column, "x".repeat(65537))] {
        let before = state.clone();
        assert!(state.set_column_default(id, Some(value)).is_err());
        assert_eq!(state, before);
    }
    state
        .set_column_default(column, Some("x".repeat(65536)))
        .unwrap();
    assert_eq!(
        codec()
            .deserialize::<Relational>(&codec().serialize(&state).unwrap())
            .unwrap(),
        state
    );
    state.set_column_default(column, None).unwrap();
    assert_eq!(codec().serialize(&state).unwrap(), legacy(&state));
}

#[test]
fn catalog_ext_incoming_foreign_key_drop_is_atomic() {
    let (mut state, column) = fixture(DataType::BigInt);
    for sql in [
        "CREATE TABLE child (id BIGINT PRIMARY KEY, parent_id BIGINT)",
        "INSERT INTO t VALUES (1, 42)",
        "INSERT INTO child VALUES (2, 1)",
    ] {
        let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
        state.execute(&prepared, &[], None).unwrap();
    }
    let parent = state.tables["t"].clone();
    let child = state.tables["child"].clone();
    let seq = state.add_sequence(sequence(parent.id, column)).unwrap();
    state
        .set_column_identity(
            column,
            Some(Identity {
                always: false,
                sequence_id: seq,
            }),
        )
        .unwrap();
    state
        .add_constraint(
            child.id,
            Constraint {
                id: state.next_id,
                name: "child_parent_fk".into(),
                kind: ConstraintKind::ForeignKey {
                    columns: vec![child.columns[1].id],
                    ref_table: parent.id,
                    ref_columns: vec![parent.columns[0].id],
                    on_delete: Action::NoAction,
                    on_update: Action::NoAction,
                    not_valid: false,
                    deferral: Deferral::NotDeferrable,
                    target: crate::relational::catalog_ext::ReferencedKey::Unknown,
                },
            },
        )
        .unwrap();
    state.validate().unwrap();
    let before = state.clone();
    assert!(matches!(
        state.drop_table("t"),
        Err(SqlError::DependentObjects)
    ));
    assert_eq!(state.tables, before.tables);
    assert_eq!(state.rows, before.rows);
    assert_eq!(state.ext, before.ext);
    assert_eq!(state.format, before.format);
    assert_eq!(state.next_id, before.next_id);
    assert_eq!(state.epoch, before.epoch);
    assert_eq!(state, before);
    state.validate().unwrap();
}

#[test]
fn catalog_ext_missing_identity_sequence_mutation_is_atomic() {
    let (mut state, column) = fixture(DataType::BigInt);
    for populated in [false, true] {
        if populated {
            state.set_column_default(column, Some("42".into())).unwrap();
        }
        let before = state.clone();
        assert!(matches!(
            state.set_column_identity(
                column,
                Some(Identity {
                    always: true,
                    sequence_id: state.next_id,
                })
            ),
            Err(SqlError::Type)
        ));
        assert_eq!(state.ext, before.ext);
        assert_eq!(state.format, before.format);
        assert_eq!(state.next_id, before.next_id);
        assert_eq!(state, before);
    }
}

impl Relational {
    pub(crate) fn assert_legacy_format_for_catalog_ext_test(&self) {
        assert_eq!(self.format, 1);
    }
}

#[test]
fn catalog_ext_v5_schema_encoding_is_frozen() {
    let mut ext = CatalogExt {
        ext_version: 5,
        ..Default::default()
    };
    ext.schemas.insert(
        "a".into(),
        schemas::Schema {
            id: 1,
            owner: "giggles".into(),
        },
    );
    ext.table_schemas.insert(2, "a".into());
    let golden = "0000000500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000016100000000000000010000000000000007676967676c657300000000000000010000000000000002000000000000000161";
    let bytes: Vec<_> = (0..golden.len())
        .step_by(2)
        .map(|n| u8::from_str_radix(&golden[n..n + 2], 16).unwrap())
        .collect();
    assert_eq!(codec().serialize(&ext).unwrap(), bytes);
    assert_eq!(codec().deserialize::<CatalogExt>(&bytes).unwrap(), ext);
    for n in 0..bytes.len() {
        assert!(codec().deserialize::<CatalogExt>(&bytes[..n]).is_err());
    }
}

#[test]
fn catalog_ext_v5_preserves_the_complete_v4_prefix() {
    let mut ext = CatalogExt::default();
    ext.logical_kinds.insert(7, catalog_ext::LogicalKind::Uuid);
    Relational::normalize_ext(&mut ext);
    assert_eq!(ext.ext_version, 4);
    let v4 = codec().serialize(&ext).unwrap();
    let decoded = codec().deserialize::<CatalogExt>(&v4).unwrap();
    assert!(decoded.schemas.is_empty() && decoded.table_schemas.is_empty());
    assert_eq!(decoded, ext);
    ext.schemas.insert(
        "typed".into(),
        schemas::Schema {
            id: 1,
            owner: "giggles".into(),
        },
    );
    ext.table_schemas.insert(2, "typed".into());
    Relational::normalize_ext(&mut ext);
    assert_eq!(ext.ext_version, 5);
    let v5 = codec().serialize(&ext).unwrap();
    assert_eq!(&v5[4..v4.len()], &v4[4..]);
    assert_eq!(codec().deserialize::<CatalogExt>(&v5).unwrap(), ext);
    ext.schemas.clear();
    ext.table_schemas.clear();
    Relational::normalize_ext(&mut ext);
    assert_eq!(ext.ext_version, 4);
    assert_eq!(codec().serialize(&ext).unwrap(), v4);
}

#[test]
fn unique_origin_v7_roundtrip_and_old_index_has_no_origin() {
    let mut s = Relational::default();
    let exec = |s: &mut Relational, sql| {
        let p = Prepared::parse(sql, 1, s.epoch).unwrap(); s.execute(&p, &[], None)
    };
    exec(&mut s, "CREATE TABLE u (label text CONSTRAINT uq UNIQUE)").unwrap();
    exec(&mut s, "CREATE UNIQUE INDEX plain ON u(label)").unwrap();
    exec(&mut s, "CREATE TABLE multi (a text, b text, CONSTRAINT ordered UNIQUE(b,a))").unwrap();
    let multi = s.catalog_presentation().unwrap().constraints.into_iter().find(|c| c.name == "ordered").unwrap();
    assert_eq!(multi.columns, vec![s.tables["multi"].columns[1].id, s.tables["multi"].columns[0].id]);
    exec(&mut s, "ALTER TABLE multi DROP COLUMN a").unwrap();
    assert!(!s.secondary_descriptors_engine().iter().any(|d| d.name == "ordered"));
    exec(&mut s, "DROP TABLE multi").unwrap();
    assert_eq!(s.ext.ext_version, 7);
    let ext = codec().serialize(s.ext.as_ref()).unwrap();
    assert_eq!(codec().deserialize::<CatalogExt>(&ext).unwrap(), *s.ext);
    let (base, indexes) = s.secondary_persistence_engine(|| false).unwrap();
    let mut restored: Relational = bincode::deserialize(&base).unwrap();
    restored.secondary_restore_engine(secondary_index::codec::decode_pending(&indexes, || false).unwrap(), || false).unwrap();
    assert_eq!(restored, s);
    let u = s.catalog_presentation().unwrap().constraints.into_iter().find(|c| c.kind == 'u').unwrap();
    assert_eq!(u.name, "uq"); assert_ne!(u.index, u.id);
    assert_eq!(exec(&mut s, "DROP INDEX uq"), Err(SqlError::DependentObjects));
    exec(&mut s, "CREATE TABLE child (label text REFERENCES u(label))").unwrap();
    assert_eq!(exec(&mut s, "ALTER TABLE u DROP CONSTRAINT uq"), Err(SqlError::DependentObjects));
    exec(&mut s, "DROP TABLE child").unwrap();
    exec(&mut s, "ALTER TABLE u DROP CONSTRAINT uq").unwrap();
    assert_eq!(s.secondary_descriptors_engine().len(), 1);
    exec(&mut s, "ALTER TABLE u ADD CONSTRAINT again UNIQUE (label)").unwrap();
    exec(&mut s, "ALTER TABLE u DROP COLUMN label").unwrap_err(); // last logical column
    // Simulate a pre-v7 image: descriptors retain uniqueness, but origin is unknown.
    std::sync::Arc::make_mut(&mut s.ext).constraints.clear(); Relational::normalize_ext(std::sync::Arc::make_mut(&mut s.ext));
    let (base, indexes) = s.secondary_persistence_engine(|| false).unwrap();
    let mut old: Relational = bincode::deserialize(&base).unwrap();
    old.secondary_restore_engine(secondary_index::codec::decode_pending(&indexes, || false).unwrap(), || false).unwrap();
    assert!(old.catalog_presentation().unwrap().constraints.is_empty());
    let mut unknown = ext; unknown[..4].copy_from_slice(&12u32.to_be_bytes());
    assert!(codec().deserialize::<CatalogExt>(&unknown).unwrap_err().to_string().contains(catalog_ext::UNSUPPORTED_CATALOG_EXT));
}

#[test]
fn forged_unique_index_references_fail_legacy_restore_and_save() {
    let mut s = Relational::default();
    for text in ["CREATE TABLE u(a text,b text,CONSTRAINT uq UNIQUE(b,a))",
        "CREATE TABLE v(a text,CONSTRAINT other_table UNIQUE(a))",
        "CREATE INDEX nonunique ON u(b,a)", "CREATE UNIQUE INDEX wrongcols ON u(a)",
        "CREATE UNIQUE INDEX wrongorder ON u(a,b)"] {
        let p = Prepared::parse(text, 1, s.epoch).unwrap(); s.execute(&p, &[], None).unwrap();
    }
    let (base, payload) = s.secondary_persistence_engine(|| false).unwrap();
    for name in ["missing", "nonunique", "other_table", "wrongcols", "wrongorder"] {
        let mut bad = s.clone();
        let id = s.secondary_descriptors_engine().iter().find(|d| d.name == name).map_or(999, |d| d.id);
        let ConstraintKind::Unique { index, .. } = &mut std::sync::Arc::make_mut(&mut bad.ext).constraints.get_mut(&s.tables["u"].id).unwrap()[0].kind else { panic!() };
        *index = id;
        assert_eq!(bad.secondary_persistence_engine(|| false), Err(SqlError::Type), "{name}");
        let mut restored: Relational = bincode::deserialize(&base).unwrap();
        restored.ext = bad.ext;
        let before = restored.clone();
        assert_eq!(restored.secondary_restore_engine(secondary_index::codec::decode_pending(&payload, || false).unwrap(), || false), Err(SqlError::Type), "{name}");
        assert_eq!(restored, before);
    }
}
