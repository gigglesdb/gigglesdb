use super::*;
fn assert_corrupt<T>(result: Result<T, CoreError>) {
    assert!(matches!(result, Err(CoreError::Corrupt(_))));
}
fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
fn table() -> TableEntry {
    TableEntry {
        primary_index_birth: None,
        def: TableDef {
            id: 1,
            name: "t".into(),
            columns: vec![ColumnDef {
                id: 2,
                name: "c".into(),
                data_type: DataType::Integer,
                not_null: true,
            }],
            primary_key: vec![0],
        },
        heap: false,
        columns_ext: BTreeMap::new(),
    }
}
fn sequence() -> SequenceDef {
    SequenceDef {
        name: "s".into(),
        data_type: DataType::BigInt,
        start: 1,
        increment: 1,
        min: 1,
        max: 99,
        cache: 4,
        cycle: false,
        owned_by: Some((1, 2)),
        identity: true,
        incarnation: 7,
    }
}
const TABLE: &str = "02000000000000000100000000000000017400000000000000010000000000000002000000000000000163000000020100000000000000010000000000000000000000000000000000";
const EXT: &str = "02000000000000000100000000000000017400000000000000010000000000000002000000000000000163000000060100000000000000010000000000000000000000000000000001000000000000000201000100040100000000000000013101010000000000000003";
const CHECK: &str = "010000000000000004010000000000000002636b000000000000000000000003633e3001";
const PK: &str = "010000000000000004010000000000000006745f706b657900000002";
const PK_NAME: &str = "010000000000000004010000000000000006637573746f6d00000002";
const FK: &str = "010000000000000004010000000000000002666b000000010000000000000001000000000000000200000000000000080000000000000001000000000000000900000002000000000000000000";
const SEQUENCE: &str = "0100000000000000017300000003000000000000000100000000000000010000000000000001000000000000006300000000000000040001000000000000000100000000000000020100000000000000000000000000000007";
const COUNTER: &str = "00000000000000080100000000000000080000000000000003";
#[test]
fn generated_columns_v2_table_trailer() {
    let entry = table();
    let columns = BTreeMap::new();
    let metadata = TableMetadata {
        generated: BTreeMap::from([(2, crate::relational::generated_columns::GeneratedColumn {
            sql: "1".into(),
            stored: true,
            not_null: false,
            references: vec![],
        })]),
        ..TableMetadata::default()
    };
    let bytes = encode_table_with_metadata(&entry, &columns, &BTreeMap::new(), &metadata).unwrap();
    assert!(bytes.starts_with(&hex(TABLE)));
    assert_corrupt(decode_version::<TableEntryV2Dto>(2, &bytes));
    assert_eq!(decode_table_with_metadata(&bytes).unwrap(), (entry, columns, metadata));
    assert_eq!(
        super::super::content_check::required_bits_for_catalog_value(&table_key(1), &bytes).unwrap(),
        super::super::meta::GENERATED_COLUMNS,
    );
}

#[test]
fn frozen_table_goldens() {
    let mut entry = table();
    let mut ext = BTreeMap::new();
    assert_eq!(encode_table(&entry, &ext).unwrap(), hex(TABLE));
    assert_eq!(
        decode_table(&hex(TABLE)).unwrap(),
        (entry.clone(), ext.clone())
    );
    entry.def.columns[0].data_type = DataType::Numeric;
    ext.insert(
        2,
        ColumnExt {
            typmod: Some(65540),
            default: Some("1".into()),
            identity: Some(Identity {
                always: true,
                sequence_id: 3,
            }),
            text_kind: None,
            type_id: None,
        },
    );
    assert_eq!(encode_table(&entry, &ext).unwrap(), hex(EXT));
    entry.columns_ext = ext.clone();
    assert_eq!(decode_table(&hex(EXT)).unwrap(), (entry.clone(), ext));
    assert_eq!(
        super::super::catalog::decode_entry(&hex(EXT)).unwrap(),
        entry
    );
    for version in [0, 1, 3, 255] {
        let mut bad = hex(TABLE);
        bad[0] = version;
        assert_corrupt(decode_table(&bad));
    }
    let mut bad = hex(TABLE);
    bad.push(0);
    assert_corrupt(decode_table(&bad));
}
#[test]
fn enum_array_tag_with_out_of_range_type_id_is_corrupt() {
    // Changing only the tag reads the following bytes as a type ID outside the OID space.
    let mut bytes = hex(TABLE);
    let tag = bytes.windows(5).position(|chunk| chunk == [b'c', 0, 0, 0, 2]).unwrap() + 1;
    bytes[tag..tag + 4].copy_from_slice(&23u32.to_be_bytes());
    assert_corrupt(decode_table(&bytes));
}

#[test]
fn frozen_constraints() {
    let cases = [
        (
            CHECK,
            Constraint {
                id: 4,
                name: "ck".into(),
                kind: ConstraintKind::Check {
                    expr: "c>0".into(),
                    not_valid: true,
                },
            },
        ),
        (
            PK,
            Constraint {
                id: 4,
                name: "t_pkey".into(),
                kind: ConstraintKind::PrimaryKey,
            },
        ),
        (
            PK_NAME,
            Constraint {
                id: 4,
                name: "custom".into(),
                kind: ConstraintKind::PrimaryKey,
            },
        ),
        (
            FK,
            Constraint {
                id: 4,
                name: "fk".into(),
                kind: ConstraintKind::ForeignKey {
                    columns: vec![2],
                    ref_table: 8,
                    ref_columns: vec![9],
                    on_delete: Action::Cascade,
                    on_update: Action::NoAction,
                    not_valid: false,
                    deferral: Deferral::NotDeferrable,
                    target: crate::relational::catalog_ext::ReferencedKey::Unknown,
                },
            },
        ),
    ];
    for (golden, live) in cases {
        assert_eq!(encode_constraint(&live, "t").unwrap(), hex(golden));
        assert_eq!(decode_constraint(&hex(golden), &table().def).unwrap(), live);
    }
    for (deferral, live, golden) in [
        (
            DeferralDto::DeferrableImmediate,
            Deferral::DeferrableImmediate,
            "010000000000000004010000000000000002666b000000010000000000000001000000000000000200000000000000080000000000000001000000000000000900000002000000000000000001",
        ),
        (
            DeferralDto::DeferrableDeferred,
            Deferral::DeferrableDeferred,
            "010000000000000004010000000000000002666b000000010000000000000001000000000000000200000000000000080000000000000001000000000000000900000002000000000000000002",
        ),
    ] {
        let dto = ConstraintDto {
            id: 4,
            name: Some("fk".into()),
            kind: ConstraintKindDto::ForeignKey {
                columns: vec![2],
                ref_table: 8,
                ref_columns: vec![9],
                on_delete: ActionDto::Cascade,
                on_update: ActionDto::NoAction,
                not_valid: false,
                deferral,
            },
        };
        assert_eq!(encode_version(1, &dto).unwrap(), hex(golden));
        assert_eq!(
            decode_version::<ConstraintDto>(1, &hex(golden)).unwrap(),
            dto
        );
        // E1 82ea06ab: live FKs carry deferral, so the frozen bytes now round-trip.
        let expected = Constraint {
            id: 4,
            name: "fk".into(),
            kind: ConstraintKind::ForeignKey {
                columns: vec![2],
                ref_table: 8,
                ref_columns: vec![9],
                on_delete: Action::Cascade,
                on_update: Action::NoAction,
                not_valid: false,
                deferral: live,
                target: crate::relational::catalog_ext::ReferencedKey::Unknown,
            },
        };
        assert_eq!(
            decode_constraint(&hex(golden), &table().def).unwrap(),
            expected
        );
        assert_eq!(encode_constraint(&expected, "t").unwrap(), hex(golden));
    }
    let missing = ConstraintDto {
        id: 1,
        name: None,
        kind: ConstraintKindDto::Check {
            expr: "true".into(),
            not_valid: false,
        },
    };
    assert_corrupt(Constraint::try_from(missing));
}
#[test]
fn frozen_sequences_and_counters() {
    let def = sequence();
    assert_eq!(encode_sequence(&def).unwrap(), hex(SEQUENCE));
    assert_eq!(decode_sequence(&hex(SEQUENCE)).unwrap(), def);
    let key = counter_key(3, 7);
    assert_eq!(
        key,
        hex("02000000000000000300000000000000000000000000000007")
    );
    let live = SeqCounter {
        incarnation: 7,
        last_value: 2,
        is_called: true,
        log_hw: 8,
        stamp: 3,
    };
    assert_eq!(encode_counter(&live, &def).unwrap(), hex(COUNTER));
    let persisted = SeqCounter {
        incarnation: 7,
        last_value: 8,
        is_called: true,
        log_hw: 8,
        stamp: 3,
    };
    assert_eq!(encode_counter(&persisted, &def).unwrap(), hex(COUNTER));
    let got = decode_counter(&key, &hex(COUNTER), &def).unwrap();
    assert_eq!(
        (
            got.incarnation,
            got.last_value,
            got.is_called,
            got.log_hw,
            got.stamp
        ),
        (
            persisted.incarnation,
            persisted.last_value,
            persisted.is_called,
            persisted.log_hw,
            persisted.stamp
        )
    );
    for len in [0, 24, 26] {
        assert_corrupt(decode_counter(&vec![0; len], &hex(COUNTER), &def));
        assert_corrupt(decode_counter(&key, &vec![0; len], &def));
    }
    let mut bad = key.clone();
    bad[0] = 11;
    assert_corrupt(decode_counter(&bad, &hex(COUNTER), &def));
    bad = key.clone();
    bad[24] = 8;
    assert_corrupt(decode_counter(&bad, &hex(COUNTER), &def));
    let mut value = hex(COUNTER);
    value[8] = 2;
    assert_corrupt(decode_counter(&key, &value, &def));
    value = hex(COUNTER);
    value[7] = 7;
    assert_corrupt(decode_counter(&key, &value, &def));
    for last in [0i64, 100] {
        let value = super::super::super::encode(&(last, true, last, 3u64)).unwrap();
        assert_corrupt(decode_counter(&key, &value, &def));
    }
    let bad = SeqCounter {
        incarnation: 7,
        last_value: 2,
        is_called: false,
        log_hw: 8,
        stamp: 3,
    };
    assert_corrupt(encode_counter(&bad, &def));
}
#[test]
fn versions_trailing_and_empty_type() {
    for bytes in [
        vec![],
        vec![0],
        vec![1],
        vec![1, 0, 0, 0, 0],
        vec![2, 0, 0, 0, 0],
    ] {
        assert_corrupt(decode_type(&bytes));
    }
    for golden in [CHECK, PK, FK, SEQUENCE] {
        let mut v = hex(golden);
        v[0] = 2;
        if golden == SEQUENCE {
            assert_corrupt(decode_sequence(&v));
        } else {
            assert_corrupt(decode_constraint(&v, &table().def));
        }
        v = hex(golden);
        v.push(0);
        if golden == SEQUENCE {
            assert_corrupt(decode_sequence(&v));
        } else {
            assert_corrupt(decode_constraint(&v, &table().def));
        }
    }
}
#[test]
fn catalog_keys() {
    for kind in [0, 2, 8, 255] {
        assert_corrupt(classify_catalog_key(&[kind; 9]));
    }
    for (key, kind) in [
        (table_key(1), CatalogKind::Table),
        (name_key("t"), CatalogKind::Name),
        (constraint_key(1, 2), CatalogKind::Constraint),
        (sequence_key(1), CatalogKind::Sequence),
        (type_key(1), CatalogKind::TypeDefinition),
        (object_key(7, 1), CatalogKind::RoleBootstrap),
    ] {
        assert_eq!(classify_catalog_key(&key).unwrap(), kind);
        let mut bad = key.clone();
        bad.pop();
        assert_corrupt(classify_catalog_key(&bad));
        let mut bad = key;
        bad.push(1);
        assert_corrupt(classify_catalog_key(&bad));
    }
    assert_eq!(
        constraint_key(1, 2),
        hex("0400000000000000010000000000000002")
    );
    assert_eq!(name_key("t"), hex("037400"));
    assert_corrupt(classify_catalog_key(&[3, 255, 0]));
    assert_corrupt(classify_catalog_key(&[3, 0, 0]));
}

#[test]
fn primary_key_names_survive_rename_and_collision_suffix() {
    let mut renamed = table();
    renamed.def.name = "newname".into();
    let mut long = table();
    long.def.name = "a".repeat(60);
    // E1's 63-byte name chooser reserves six bytes for "_pkey1".
    let collision_name = format!("{}_pkey1", "a".repeat(57));
    assert_eq!(collision_name.len(), 63);
    for (entry, name) in [(renamed, "oldname_pkey".into()), (long, collision_name)] {
        assert_eq!(
            decode_table(&encode_table(&entry, &BTreeMap::new()).unwrap())
                .unwrap()
                .0,
            entry
        );
        let constraint = Constraint {
            id: 4,
            name,
            kind: ConstraintKind::PrimaryKey,
        };
        let bytes = encode_constraint(&constraint, &entry.def.name).unwrap();
        let dto: ConstraintDto = decode_version(1, &bytes).unwrap();
        assert_eq!(dto.name.as_ref(), Some(&constraint.name));
        let decoded = decode_constraint(&bytes, &entry.def).unwrap();
        assert_eq!(decoded, constraint);
        assert_eq!(encode_constraint(&decoded, &entry.def.name).unwrap(), bytes);
    }
}

#[test]
fn primary_key_without_persisted_name_is_corrupt() {
    // Former omitted-default-name encoding cannot recover a renamed PK name.
    assert_corrupt(decode_constraint(
        &hex("0100000000000000040000000002"),
        &table().def,
    ));
}

fn table_dto() -> TableEntryV2Dto {
    TableEntryV2Dto::from_entry(&table(), &BTreeMap::new(), &BTreeMap::new()).unwrap()
}

fn assert_bad_table(dto: &TableEntryV2Dto) {
    assert_corrupt(decode_table(&encode_version(2, dto).unwrap()));
}

#[test]
fn decoded_table_column_count_is_bounded() {
    let mut dto = table_dto();
    dto.def.columns.clear();
    assert_bad_table(&dto);
    dto.def.columns = (0..MAX_TABLE_COLUMNS)
        .map(|i| ColumnDefDto {
            id: i as u64 + 2,
            name: format!("c{i}"),
            data_type: DataTypeDto::Integer,
            not_null: true,
        })
        .collect();
    assert!(decode_table(&encode_version(2, &dto).unwrap()).is_ok());
    dto.def.columns.push(ColumnDefDto {
        id: MAX_TABLE_COLUMNS as u64 + 2,
        name: "overflow".into(),
        data_type: DataTypeDto::Integer,
        not_null: true,
    });
    assert_bad_table(&dto);
}

#[test]
fn decoded_column_ids_are_unique_and_nonzero() {
    let mut dto = table_dto();
    dto.def.columns[0].id = 0;
    assert_bad_table(&dto);
    dto = table_dto();
    let mut duplicate = dto.def.columns[0].clone();
    duplicate.name = "other".into();
    dto.def.columns.push(duplicate);
    assert_bad_table(&dto);
}

#[test]
fn decoded_primary_key_offsets_are_in_bounds() {
    let mut dto = table_dto();
    for offset in [1, u64::MAX] {
        dto.def.primary_key = vec![offset];
        assert_bad_table(&dto);
    }
}

#[test]
fn decoded_column_extensions_belong_to_table() {
    let mut dto = table_dto();
    dto.columns_ext
        .insert(99, ColumnExtDto::try_from(ColumnExt::default()).unwrap());
    assert_bad_table(&dto);
}

#[test]
fn decoded_typmods_are_checked_against_column_type() {
    for (data_type, valid, invalid) in [
        (DataType::Numeric, 65540, 4),
        (DataType::Time, 6, 7),
        (DataType::TimeTz, 0, -1),
        (DataType::Timestamp, 3, 7),
        (DataType::TimestampTz, 6, -1),
        (DataType::Interval, (32767 << 16) | 6, (32767 << 16) | 7),
    ] {
        let mut dto = table_dto();
        dto.def.columns[0].data_type = data_type.try_into().unwrap();
        // Temporal values are valid columns, but cannot be CREATE key fields.
        dto.def.primary_key.clear();
        dto.columns_ext.insert(
            2,
            ColumnExtDto {
                typmod: Some(valid),
                default: None,
                identity: None,
            },
        );
        assert!(decode_table(&encode_version(2, &dto).unwrap()).is_ok());
        dto.columns_ext.get_mut(&2).unwrap().typmod = Some(invalid);
        assert_bad_table(&dto);
    }
    let mut dto = table_dto();
    dto.columns_ext.insert(
        2,
        ColumnExtDto {
            typmod: Some(4),
            default: None,
            identity: None,
        },
    );
    assert_bad_table(&dto);
}

#[test]
fn decoded_table_and_column_names_follow_legacy_bounds() {
    for name in [String::new(), "a".repeat(64), "é".repeat(32)] {
        let mut dto = table_dto();
        dto.def.name = name.clone();
        assert_bad_table(&dto);
        dto = table_dto();
        dto.def.columns[0].name = name;
        assert_bad_table(&dto);
    }
    let mut dto = table_dto();
    dto.def.name = format!("{}a", "é".repeat(31));
    dto.def.columns[0].name = "quoted column".into();
    assert!(decode_table(&encode_version(2, &dto).unwrap()).is_ok());
    let mut entry = table();
    entry.def.columns.push(ColumnDef {
        id: 3,
        name: crate::relational::HIDDEN_ROWID.into(),
        data_type: DataType::BigInt,
        not_null: true,
    });
    entry.def.primary_key = vec![1];
    entry.heap = true;
    assert_eq!(
        decode_table(&encode_table(&entry, &BTreeMap::new()).unwrap())
            .unwrap()
            .0,
        entry
    );
}

#[test]
fn decoded_constraint_and_sequence_names_follow_legacy_bounds() {
    for name in [String::new(), "a".repeat(64), "é".repeat(32)] {
        let dto = ConstraintDto {
            id: 4,
            name: Some(name.clone()),
            kind: ConstraintKindDto::PrimaryKey,
        };
        assert_corrupt(decode_constraint(
            &encode_version(1, &dto).unwrap(),
            &table().def,
        ));
        let mut dto = SequenceDefDto::try_from(sequence()).unwrap();
        dto.name = name;
        assert_corrupt(decode_sequence(&encode_version(1, &dto).unwrap()));
    }
    let name = format!("{}a", "é".repeat(31));
    let constraint = Constraint {
        id: 4,
        name: name.clone(),
        kind: ConstraintKind::PrimaryKey,
    };
    assert_eq!(
        decode_constraint(&encode_constraint(&constraint, "t").unwrap(), &table().def).unwrap(),
        constraint
    );
    let mut def = sequence();
    def.name = name;
    assert_eq!(
        decode_sequence(&encode_sequence(&def).unwrap()).unwrap(),
        def
    );
}

#[test]
fn name_keys_follow_legacy_byte_bounds() {
    for name in [String::new(), "a".repeat(64), "é".repeat(32)] {
        assert_corrupt(classify_catalog_key(&name_key(&name)));
    }
    for name in ["a".repeat(63), format!("{}a", "é".repeat(31))] {
        assert_eq!(
            classify_catalog_key(&name_key(&name)).unwrap(),
            CatalogKind::Name
        );
    }
}

#[test]
fn decoded_foreign_key_columns_belong_to_owning_table() {
    let mut dto: ConstraintDto = decode_version(1, &hex(FK)).unwrap();
    let ConstraintKindDto::ForeignKey { columns, .. } = &mut dto.kind else {
        unreachable!();
    };
    *columns = vec![99];
    assert_corrupt(decode_constraint(
        &encode_version(1, &dto).unwrap(),
        &table().def,
    ));
    // The fixture's remote table 8/column 9 need no cross-object lookup here.
    assert!(decode_constraint(&hex(FK), &table().def).is_ok());
}

#[test]
fn appended_column_type_tags_and_existing_typmod_round_trip() {
    use crate::relational::catalog_ext::TextKind;
    for (data_type, tag, kind, typmod) in [
        (DataType::Varchar, 15u32, Some(TextKind::Varchar), Some(14)),
        (DataType::Bpchar, 16, Some(TextKind::Bpchar), Some(7)),
        (DataType::Uuid, 17, None, None),
        (DataType::Jsonb, 18, None, None),
        (DataType::IntArray, 19, None, None),
        (DataType::BigIntArray, 20, None, None),
        (DataType::TextArray, 21, None, None),
        (DataType::UuidArray, 24, None, None),
        (DataType::TsVector, 26, None, None),
        (DataType::TsQuery, 27, None, None),
    ] {
        assert_eq!(
            super::super::super::encode(&DataTypeDto::try_from(data_type.clone()).unwrap())
                .unwrap(),
            tag.to_be_bytes()
        );
        let mut entry = table();
        entry.def.primary_key.clear();
        entry.def.columns[0].data_type = data_type;
        let mut ext = BTreeMap::new();
        if kind.is_some() {
            ext.insert(
                2,
                ColumnExt {
                    typmod,
                    text_kind: kind,
                    ..ColumnExt::default()
                },
            );
        }
        entry.columns_ext = ext.clone();
        let encoded = encode_table(&entry, &ext).unwrap();
        assert_eq!(decode_table(&encoded).unwrap(), (entry, ext));
    }
    // All previously representable catalog bytes remain frozen.
    assert_eq!(
        encode_table(&table(), &BTreeMap::new()).unwrap(),
        hex(TABLE)
    );
}

#[test]
fn inconsistent_text_kind_is_refused_and_user_type_reference_roundtrips() {
    let mut ext = BTreeMap::new();
    ext.insert(
        2,
        ColumnExt {
            text_kind: Some(TextKind::Varchar),
            ..ColumnExt::default()
        },
    );
    assert_corrupt(encode_table(&table(), &ext));
    ext.get_mut(&2).unwrap().text_kind = None;
    ext.get_mut(&2).unwrap().type_id = Some(42);
    let bytes = encode_table(&table(), &ext).unwrap();
    let (_, decoded) = decode_table(&bytes).unwrap();
    assert_eq!(decoded[&2].type_id, Some(42));
}

#[test]
fn schema_and_type_wire_extensions_are_append_only() {
    use crate::relational::schemas::Schema;
    assert_eq!(schema_key("app"), hex("0e61707000"));
    assert_eq!(type_key(42), hex("0f000000000000002a"));
    let schema = Schema {
        id: 42,
        owner: "owner".into(),
    };
    assert_eq!(
        encode_schema(&schema).unwrap(),
        hex("01000000000000002a00000000000000056f776e6572")
    );
    assert_eq!(
        decode_schema(&encode_schema(&schema).unwrap()).unwrap(),
        schema
    );
    let ty = TypeDef::Enum {
        name: "e".into(),
        schema: "app".into(),
        labels: vec!["x".into(), "y".into()],
    };
    let mut expected = vec![1];
    // Legacy v6 TypeDef payload: u32 enum tag, strings and vectors with u64 lengths.
    expected.extend(0u32.to_be_bytes());
    for value in ["e", "app"] {
        expected.extend((value.len() as u64).to_be_bytes());
        expected.extend(value.as_bytes());
    }
    expected.extend(2u64.to_be_bytes());
    for value in ["x", "y"] {
        expected.extend(1u64.to_be_bytes());
        expected.extend(value.as_bytes());
    }
    assert_eq!(encode_type(ty.clone()).unwrap(), expected);
    assert_eq!(decode_type(&expected).unwrap(), ty);
    let mut reference = 22u32.to_be_bytes().to_vec();
    reference.extend(42u64.to_be_bytes());
    assert_eq!(
        super::super::super::encode(&DataTypeDto::UserType(42)).unwrap(),
        reference
    );
    assert_eq!(
        classify_catalog_key(&name_key("app\0t")).unwrap(),
        CatalogKind::Name
    );
    for name in ["\0t", "app\0", "app\0t\0bad", "public\0t"] {
        assert_corrupt(classify_catalog_key(&name_key(name)));
    }
}

#[test]
fn unique_constraint_record_v2_roundtrip_old_record_and_unknown_version() {
    let c = Constraint { id: 4, name: "uq".into(), kind: ConstraintKind::Unique { columns: vec![2], index: 3 } };
    let bytes = encode_constraint(&c, "t").unwrap(); assert_eq!(bytes[0], 2);
    assert_eq!(bytes, hex("020000000000000004010000000000000002757100000003000000000000000100000000000000020000000000000003"));
    assert_eq!(decode_constraint(&bytes, &table().def).unwrap(), c);
    assert!(matches!(decode_constraint(&hex(PK), &table().def).unwrap().kind, ConstraintKind::PrimaryKey));
    assert_corrupt(decode_version::<ConstraintDto>(1, &bytes)); // older reader refuses at version
    for version in [0, 1, 3, 255] {
        let mut bad = bytes.clone(); bad[0] = version; assert_corrupt(decode_constraint(&bad, &table().def));
    }
}

#[test]
fn table_v3_birth_roundtrip_frozen_payload_and_canonicality() {
    let mut entry = table(); entry.primary_index_birth = Some(10);
    let bytes = encode_table(&entry, &BTreeMap::new()).unwrap();
    let mut expected = hex(TABLE); expected[0] = 3; expected.extend(10u64.to_be_bytes());
    assert_eq!(bytes, expected);
    assert_eq!(decode_table(&bytes).unwrap().0, entry);
    assert_eq!(decode_table(&hex(TABLE)).unwrap().0.primary_index_birth, None);
    for n in 0..bytes.len() { assert_corrupt(decode_table(&bytes[..n])); }
    let mut trailing = bytes.clone(); trailing.push(0); assert_corrupt(decode_table(&trailing));
    let mut zero = bytes; let at = zero.len() - 8; zero[at..].fill(0); assert_corrupt(decode_table(&zero));
    for (heap, pk) in [(true, vec![0]), (false, vec![])] {
        entry.heap = heap; entry.def.primary_key = pk;
        assert_corrupt(encode_table(&entry, &BTreeMap::new()));
        let dto = TableEntryV2Dto::from_entry(&entry, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert_corrupt(decode_table(&encode_version(3, &(dto, 10u64)).unwrap()));
    }
}
#[test]
fn constraint_v3_known_targets_preserve_frozen_fk_payload() {
    let old = decode_constraint(&hex(FK), &table().def).unwrap();
    assert!(matches!(old.kind, ConstraintKind::ForeignKey { target: ReferencedKey::Unknown, .. }));
    for target in [ReferencedKey::PrimaryKey, ReferencedKey::Secondary(12)] {
        let mut c = old.clone();
        if let ConstraintKind::ForeignKey { target: saved, .. } = &mut c.kind { *saved = target; }
        let bytes = encode_constraint(&c, "t").unwrap();
        let mut expected = hex(FK); expected[0] = 3;
        expected.extend(if target == ReferencedKey::PrimaryKey { 1u32 } else { 2u32 }.to_be_bytes());
        if let ReferencedKey::Secondary(id) = target { expected.extend(id.to_be_bytes()); }
        assert_eq!(bytes, expected);
        assert_eq!(decode_constraint(&bytes, &table().def).unwrap(), c);
        for n in 0..bytes.len() { assert_corrupt(decode_constraint(&bytes[..n], &table().def)); }
        let mut trailing = bytes; trailing.push(0); assert_corrupt(decode_constraint(&trailing, &table().def));
    }
    let dto = ConstraintDto::from(old);
    for target in [ReferencedKey::Unknown, ReferencedKey::Secondary(0)] {
        assert_corrupt(decode_constraint(&encode_version(3, &(dto.clone(), target)).unwrap(), &table().def));
    }
    let mut tag = hex(FK); tag[0] = 3; tag.extend(3u32.to_be_bytes());
    assert_corrupt(decode_constraint(&tag, &table().def));
    let pk: ConstraintDto = decode_version(1, &hex(PK)).unwrap();
    assert_corrupt(decode_constraint(&encode_version(3, &(pk, ReferencedKey::PrimaryKey)).unwrap(), &table().def));
}

#[test]
fn extension_record_roundtrip_and_absence_preserves_bytes() {
    let mut parts = super::super::catalog::CatalogParts::default();
    let def=table().def;
    parts.tables.insert(def.name.clone(),def);
    let original = super::super::catalog::parts_records(&parts).unwrap();
    parts.ext.extensions.insert("uuid-ossp".into(), crate::relational::InstalledExtension { schema:"public".into(),version:"1.1".into(),oid:70000,owner:10 });
    let records = super::super::catalog::parts_records(&parts).unwrap();
    let value = records.get(&vec![EXTENSIONS_KIND]).unwrap();
    assert_eq!(value[0],1);
    assert_eq!(decode_extensions(value).unwrap(),parts.ext.extensions);
    for (key,value) in &original { assert_eq!(records.get(key),Some(value)); }
    parts.ext.extensions.clear();
    assert_eq!(super::super::catalog::parts_records(&parts).unwrap(),original);
}

#[test]
fn enum_array_columns_use_a_new_tag_only_with_enum_elements() {
    use super::super::{content_check, meta};

    let enum_type = TypeDef::Enum {
        name: "size".into(),
        schema: "public".into(),
        labels: vec!["small".into(), "large".into()],
    };
    let domain = TypeDef::Domain {
        name: "tag_list".into(),
        schema: "public".into(),
        base: DataType::TextArray,
        typmod: None,
        not_null: false,
        check: None,
        constraint: "tag_list_check".into(),
    };
    let old = table();
    assert_eq!(
        encode_table_with_types(&old, &BTreeMap::new(), &BTreeMap::from([(42, enum_type.clone())])).unwrap(),
        hex(TABLE),
    );
    assert_eq!(decode_table(&hex(TABLE)).unwrap().0, old);

    let mut entry = table();
    entry.def.columns.push(ColumnDef {
        id: 3,
        name: "values".into(),
        data_type: DataType::TextArray,
        not_null: false,
    });
    let ext = BTreeMap::from([(3, ColumnExt { type_id: Some(42), ..ColumnExt::default() })]);
    let domain_bytes = encode_table_with_types(&entry, &ext, &BTreeMap::from([(42, domain)])).unwrap();
    assert_eq!(domain_bytes, encode_table(&entry, &ext).unwrap());
    let key = super::super::catalog::table_key(entry.def.id);
    let domain_bits = content_check::required_bits_for_catalog_value(&key, &domain_bytes).unwrap();
    assert_eq!(domain_bits & meta::ENUM_ARRAY_COLUMNS, 0);

    let arrays = encode_table_with_types(&entry, &ext, &BTreeMap::from([(42, enum_type)])).unwrap();
    let mut expected = domain_bytes;
    let marker = hex("00000016000000000000002a");
    let at = expected.windows(marker.len()).position(|bytes| bytes == marker).unwrap();
    expected[at + 3] = 23;
    assert_eq!(arrays, expected);
    let (decoded, metadata) = decode_table(&arrays).unwrap();
    assert_eq!(decoded.def, entry.def);
    assert_eq!(metadata, ext);
    assert_eq!(decoded_enum_array_columns(&decoded.def, &metadata).collect::<Vec<_>>(), [3]);
    assert_eq!(
        content_check::required_bits_for_catalog_value(&key, &arrays).unwrap(),
        domain_bits | meta::TEXT_BACKED_COLUMN_TYPES | meta::ENUM_ARRAY_COLUMNS,
    );
}

#[test]
fn pre_am2_enum_definition_has_the_same_v2_records_until_add_value() {
    use super::super::{catalog, content_check, meta};
    use crate::relational::catalog_ext::AddedEnumLabels;

    // V2 kind 15, version 1 TypeDefinition from the pre-AM-2 encoder.
    let frozen = hex("01000000000000000000000005706861736500000000000000067075626c6963000000000000000200000000000000017a000000000000000161");
    let ty = decode_type(&frozen).unwrap();
    let mut parts = catalog::CatalogParts::default();
    parts.ext.types.insert(1, ty);
    let key = hex("0f0000000000000001");
    let old = catalog::parts_records(&parts).unwrap();
    assert_eq!(old, BTreeMap::from([(key.clone(), frozen)]));
    assert_eq!(
        content_check::required_bits_for_catalog_value(&key, &old[&key]).unwrap(),
        meta::SCHEMAS_AND_USER_TYPES,
    );

    let TypeDef::Enum { labels, .. } = parts.ext.types.get_mut(&1).unwrap() else { panic!() };
    labels.insert(1, "new".into());
    parts.ext.added_enum_labels.insert(1, AddedEnumLabels(BTreeMap::from([("new".into(), 9)])));
    let records = catalog::parts_records(&parts).unwrap();
    let added_key = hex("1a0000000000000001");
    assert_eq!(classify_catalog_key(&added_key).unwrap(), CatalogKind::EnumLabels);
    let added_bytes = hex("01000000000000000100000000000000036e65770000000000000009");
    assert_eq!(records[&added_key], added_bytes);
    assert_eq!(decode_enum_labels(&added_bytes).unwrap(), parts.ext.added_enum_labels[&1]);
    assert_eq!(content_check::required_bits_for_catalog_value(&added_key, &added_bytes).unwrap(),
        64 | meta::SCHEMAS_AND_USER_TYPES | meta::ENUM_LABEL_IDENTITIES);
    assert_eq!(content_check::required_bits_for_catalog_key(&added_key).unwrap(), meta::ENUM_LABEL_IDENTITIES);
    assert_eq!(parts.ext.enum_label_ids().unwrap(), [(1, 2), (1, 9), (1, 3)]);

    // The newly allocated label reserves the entire catalog object slot.
    parts.ext.types.insert(9, TypeDef::Enum { name: "collision".into(), schema: "public".into(), labels: vec![] });
    assert!(catalog::parts_records(&parts).is_err());
}
