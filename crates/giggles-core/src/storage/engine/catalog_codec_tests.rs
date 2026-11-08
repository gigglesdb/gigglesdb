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
        },
    );
    assert_eq!(encode_table(&entry, &ext).unwrap(), hex(EXT));
    assert_eq!(decode_table(&hex(EXT)).unwrap(), (entry, ext));
    assert_corrupt(super::super::catalog::decode_entry(&hex(EXT)));
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
            },
        };
        assert_eq!(decode_constraint(&hex(golden), &table().def).unwrap(), expected);
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
        (type_key(1), CatalogKind::Type),
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
    TableEntryV2Dto::try_from((table(), BTreeMap::new())).unwrap()
}

fn assert_bad_table(dto: &TableEntryV2Dto) {
    assert_corrupt(decode_table(&encode_version(2, dto).unwrap()));
}

#[test]
fn decoded_table_column_count_is_bounded() {
    let mut dto = table_dto();
    dto.def.columns.clear();
    assert_bad_table(&dto);
    dto.def.columns = (0..MAX_COLUMNS)
        .map(|i| ColumnDefDto {
            id: i as u64 + 2,
            name: format!("c{i}"),
            data_type: DataTypeDto::Integer,
            not_null: true,
        })
        .collect();
    assert!(decode_table(&encode_version(2, &dto).unwrap()).is_ok());
    dto.def.columns.push(ColumnDefDto {
        id: 100,
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
        .insert(99, ColumnExtDto::from(ColumnExt::default()));
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
        dto.def.columns[0].data_type = data_type.into();
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
    for name in [String::new(), "a".repeat(64), "é".repeat(32), "a\0b".into()] {
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
        let mut dto = SequenceDefDto::from(sequence());
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
