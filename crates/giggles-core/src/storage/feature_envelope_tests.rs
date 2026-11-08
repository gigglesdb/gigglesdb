use super::*;

fn forged(bits: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut r = records(&empty(17)).unwrap();
    let mut m: Metadata = decode(&r[&vec![0]]).unwrap();
    m.format = 53;
    r.insert(vec![0], encode(&m).unwrap());
    r.insert(vec![10], bits.to_be_bytes().to_vec());
    r
}
fn normal(r: BTreeMap<Vec<u8>, Vec<u8>>) -> Result<State, CoreError> {
    from_records_mode(r, &mut || false, false)
}
#[test]
fn feature_record_precedes_payload_and_is_canonical() {
    let mut r = forged(1 << 10);
    r.insert(vec![5], vec![255]);
    assert!(matches!(normal(r), Err(CoreError::Unsupported)));
    for bits in [0, 1, 63, 64, 128, 256, 320, 512] {
        assert!(
            matches!(normal(forged(bits)), Err(CoreError::Corrupt(_))),
            "{bits}"
        );
    }
    let mut r = forged(64);
    r.remove(&vec![10]);
    assert!(matches!(normal(r), Err(CoreError::Corrupt(_))));
    let mut r = forged(64);
    r.insert(vec![10], vec![0; 7]);
    assert!(matches!(normal(r), Err(CoreError::Corrupt(_))));
    let mut r = records(&empty(17)).unwrap();
    r.insert(vec![10], vec![0; 8]);
    assert!(matches!(normal(r), Err(CoreError::Corrupt(_))));
    let mut r = records(&empty(17)).unwrap();
    r.insert(key(11, &[1, 2]), vec![255]);
    assert!(matches!(normal(r), Err(CoreError::Corrupt(_))));
    assert!(matches!(
        from_records(forged(512)),
        Err(CoreError::Unsupported)
    ));
    assert!(matches!(
        from_records(forged(512 | 256)),
        Err(CoreError::Unsupported)
    ));
    let mut r = forged(320);
    r.insert(key(11, &[1, 2]), vec![255]);
    assert!(matches!(normal(r), Err(CoreError::Corrupt(_))));
}
#[test]
fn legacy_mapping_canonicalizes_only_aliases() {
    for version in 1..=52 {
        let bits = format_policy::legacy_version_to_bits(version).unwrap();
        let canonical = format_policy::bits_to_legacy_version(bits).unwrap();
        let expected = match version {
            1 | 2 => 3,
            27 | 28 => 29,
            _ => version,
        };
        assert_eq!(canonical, expected);
        assert_eq!(format_policy::legacy_version_to_bits(canonical), Some(bits));
    }
    for version in [0, 53, u32::MAX] {
        assert_eq!(format_policy::legacy_version_to_bits(version), None);
    }
    for bits in [12, 63, 64, u64::MAX] {
        assert_eq!(format_policy::bits_to_legacy_version(bits), None);
    }
}
#[test]
fn legacy_writers_preserve_record_maps() {
    use crate::relational::Prepared;
    for (applications, indexed, temporal, heap) in [
        (false, false, false, false),
        (true, false, false, false),
        (false, true, false, false),
        (false, false, true, false),
        (false, false, false, true),
    ] {
        let mut s = empty(17);
        if applications {
            s.applications = Some(Default::default());
        }
        let pk = if heap { "" } else { " PRIMARY KEY" };
        let date = if temporal { ",d DATE" } else { "" };
        let p = Prepared::parse(
            &format!("CREATE TABLE t(id BIGINT{pk},tag TEXT{date})"),
            17,
            s.sql.epoch,
        )
        .unwrap();
        s.sql.execute(&p, &[], None).unwrap();
        if indexed {
            let t = s.sql.tables["t"].clone();
            s.sql
                .secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false)
                .unwrap();
        }
        let r = records(&s).unwrap();
        let f = format_policy::Features {
            typed_scalars: false,
            native_outcomes: false,
        };
        let base = if applications {
            f.version_application(true)
        } else {
            f.version_with(indexed)
        };
        let version = format_policy::heap_version(
            format_policy::temporal_version(base, temporal).unwrap(),
            heap,
        )
        .unwrap();
        let m: Metadata = decode(&r[&vec![0]]).unwrap();
        assert_eq!(m.format, version);
        let mut previous = BTreeMap::new();
        previous.insert(
            vec![0],
            encode(&Metadata {
                format: version,
                domain: s.view.domain,
                schema: s.view.schema,
                last_time: s.last_time,
                next_approval: s.next_approval,
                origin: s.origin,
            })
            .unwrap(),
        );
        if indexed {
            let (base, payload) = secondary_index::persist(&s.sql).unwrap();
            previous.insert(vec![5], base);
            previous.insert(vec![8], payload);
        } else {
            previous.insert(vec![5], encode(&s.sql).unwrap());
        }
        if let Some(a) = &s.applications {
            previous.insert(vec![9], a.bytes().unwrap());
        }
        assert_eq!(r, previous);
        assert_eq!(records(&from_records(r.clone()).unwrap()).unwrap(), r);
    }
}
#[test]
fn add_primary_key_clears_heap_bit_only_after_last_heap() {
    use crate::relational::Prepared;
    let mut state = empty(17);
    for (sql, heap_expected) in [
        ("CREATE TABLE a(id INT)", true),
        ("CREATE TABLE b(id INT)", true),
        ("ALTER TABLE a ADD CONSTRAINT positive CHECK(id>0)", true),
        ("ALTER TABLE a ADD PRIMARY KEY(id)", true),
        ("ALTER TABLE b ADD PRIMARY KEY(id)", false),
    ] {
        let prepared = Prepared::parse(sql, 17, state.sql.epoch).unwrap();
        state.sql.execute(&prepared, &[], None).unwrap();
        let actual = records(&state).unwrap();
        let metadata: Metadata = decode(&actual[&vec![0]]).unwrap();
        let bits = if metadata.format == 53 {
            u64::from_be_bytes(actual[&vec![10]].as_slice().try_into().unwrap())
        } else {
            format_policy::legacy_version_to_bits(metadata.format).unwrap()
        };
        assert_eq!(bits & 32 != 0, heap_expected, "{sql}");
        assert_eq!(state.sql.contains_heap(), heap_expected, "{sql}");
        let reopened = from_records(actual.clone()).unwrap();
        assert_eq!(reopened.sql.contains_heap(), heap_expected, "{sql}");
        assert_eq!(records(&reopened).unwrap(), actual);
    }
}

#[test]
fn export_refuses_role_bootstrap_bits_without_panicking() {
    assert!(matches!(
        export_allowed(ROLE_BOOTSTRAP | 64),
        Err(CoreError::Unsupported)
    ));
    assert!(matches!(
        export_allowed(ROLE_BOOTSTRAP),
        Err(CoreError::Unsupported)
    ));
    assert!(export_allowed(64 | 128 | 256).is_ok());
    assert!(export_allowed(0).is_ok());
}

fn catalog_ext_fixture() -> (State, u64, u64) {
    let mut s = empty(17);
    let p = crate::relational::Prepared::parse(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, value NUMERIC)",
        17,
        s.sql.epoch,
    )
    .unwrap();
    s.sql.execute(&p, &[], None).unwrap();
    let table = &s.sql.tables["t"];
    let (tid, cid) = (table.id, table.columns[1].id);
    (s, tid, cid)
}

#[test]
fn catalog_ext_records_roundtrip_gate_bits_and_restore_legacy() {
    use crate::relational::catalog_ext::*;
    let (mut s, tid, cid) = catalog_ext_fixture();
    let legacy = records(&s).unwrap();
    let metadata: Metadata = decode(&legacy[&vec![0]]).unwrap();
    let bits = format_policy::legacy_version_to_bits(metadata.format).unwrap();
    let seq = s
        .sql
        .add_sequence(SequenceDef {
            name: "t_identity_seq".into(),
            data_type: crate::relational::DataType::BigInt,
            start: 1,
            increment: 1,
            min: 1,
            max: i64::MAX,
            cache: 1,
            cycle: false,
            owned_by: Some((tid, cid)),
            identity: true,
            incarnation: 1,
        })
        .unwrap();
    s.sql.set_column_default(cid, Some("42".into())).unwrap();
    s.sql
        .set_column_identity(
            cid,
            Some(Identity {
                always: true,
                sequence_id: seq,
            }),
        )
        .unwrap();
    for (offset, name, kind) in [
        (
            1,
            "positive",
            ConstraintKind::Check {
                expr: "value > 0".into(),
                not_valid: false,
            },
        ),
        (
            2,
            "parent",
            ConstraintKind::ForeignKey {
                columns: vec![cid],
                ref_table: tid,
                ref_columns: vec![s.sql.tables["t"].columns[0].id],
                on_delete: Action::Restrict,
                on_update: Action::SetDefault,
                not_valid: true,
                deferral: crate::relational::catalog_ext::Deferral::NotDeferrable,
            },
        ),
        (3, "t_pkey", ConstraintKind::PrimaryKey),
    ] {
        s.sql
            .add_constraint(
                tid,
                Constraint {
                    id: seq + offset,
                    name: name.into(),
                    kind,
                },
            )
            .unwrap();
    }
    let r = records(&s).unwrap();
    assert_eq!(decode::<Metadata>(&r[&vec![0]]).unwrap().format, 53);
    assert_eq!(r[&vec![10]], (bits | 64).to_be_bytes());
    let recovered = from_records(r.clone()).unwrap();
    assert_eq!(recovered.sql, s.sql);
    assert_eq!(records(&recovered).unwrap(), r);
    let mut no_typmod = r.clone();
    no_typmod.insert(vec![10], (bits | 64 | 128).to_be_bytes().to_vec());
    assert!(matches!(
        from_records(no_typmod),
        Err(CoreError::Corrupt(_))
    ));
    s.sql
        .set_column_typmod(cid, Some((10 << 16) | 2 | 4))
        .unwrap();
    let typed = records(&s).unwrap();
    assert_eq!(typed[&vec![10]], (bits | 64 | 128).to_be_bytes());
    assert_eq!(
        records(&from_records(typed.clone()).unwrap()).unwrap(),
        typed
    );
    let mut missing_bit = typed;
    missing_bit.insert(vec![10], (bits | 64).to_be_bytes().to_vec());
    assert!(matches!(
        from_records(missing_bit),
        Err(CoreError::Corrupt(_))
    ));
    s.sql.set_column_typmod(cid, None).unwrap();
    s.sql.set_column_default(cid, None).unwrap();
    s.sql.set_column_identity(cid, None).unwrap();
    for name in ["positive", "parent", "t_pkey"] {
        s.sql.remove_constraint(tid, name).unwrap();
    }
    s.sql.remove_sequence(seq).unwrap();
    let restored = records(&s).unwrap();
    assert!(!restored.contains_key(&vec![10]));
    assert_eq!(
        decode::<Metadata>(&restored[&vec![0]]).unwrap().format,
        metadata.format
    );
    // A never-extended state with the same consumed counters has identical bytes.
    let legacy_payload = &restored[&vec![5]];
    let mut expected = legacy.clone();
    let mut expected_payload = expected[&vec![5]].clone();
    // generation, epoch and allocator counters are the only permitted differences.
    expected_payload[4..36].copy_from_slice(&legacy_payload[4..36]);
    expected.insert(vec![5], expected_payload);
    assert_eq!(restored, expected);
}

#[derive(Serialize)]
struct CatalogExtTestWire {
    ext_version: u32,
    columns: BTreeMap<u64, crate::relational::catalog_ext::ColumnExt>,
    constraints: BTreeMap<u64, Vec<crate::relational::catalog_ext::Constraint>>,
    sequences: BTreeMap<u64, crate::relational::catalog_ext::SequenceDef>,
    types: BTreeMap<u64, u32>,
}

fn catalog_ext_raw_records(ext: Vec<u8>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let (s, _, _) = catalog_ext_fixture();
    let mut r = records(&s).unwrap();
    let mut m: Metadata = decode(&r[&vec![0]]).unwrap();
    let bits = format_policy::legacy_version_to_bits(m.format).unwrap();
    m.format = 53;
    r.insert(vec![0], encode(&m).unwrap());
    r.insert(vec![10], (bits | 64).to_be_bytes().to_vec());
    let payload = r.get_mut(&vec![5]).unwrap();
    payload[..4].copy_from_slice(&2u32.to_be_bytes());
    payload.extend(ext);
    r
}

#[test]
fn catalog_ext_wire_errors_distinguish_corrupt_and_unsupported() {
    let empty_ext = || CatalogExtTestWire {
        ext_version: 1,
        columns: BTreeMap::new(),
        constraints: BTreeMap::new(),
        sequences: BTreeMap::new(),
        types: BTreeMap::new(),
    };
    assert!(matches!(
        from_records(catalog_ext_raw_records(encode(&empty_ext()).unwrap())),
        Err(CoreError::Corrupt(_))
    ));
    let mut ext = empty_ext();
    ext.ext_version = 2;
    assert!(matches!(
        from_records(catalog_ext_raw_records(encode(&ext).unwrap())),
        Err(CoreError::Corrupt(_))
    ));
    ext.ext_version = 4;
    // Exercise the relational decoder itself, before format-53 content policy:
    // unknown extensions retain Unsupported on the older namespace-5 path.
    let unknown_records = catalog_ext_raw_records(encode(&ext).unwrap());
    assert!(matches!(
        decode_relational(&unknown_records[&vec![5]]),
        Err(CoreError::Unsupported)
    ));
    assert!(matches!(
        from_records(unknown_records),
        Err(CoreError::Unsupported)
    ));
    let mut ext = empty_ext();
    ext.types.insert(100, 0);
    assert!(matches!(
        from_records(catalog_ext_raw_records(encode(&ext).unwrap())),
        Err(CoreError::Unsupported)
    ));
    // Advertise a huge expression without a payload: bounded decoding must reject
    // its length, before requesting or allocating the missing string bytes.
    let (_, _, column) = catalog_ext_fixture();
    let mut malicious = Vec::new();
    malicious.extend(1u32.to_be_bytes());
    malicious.extend(1u64.to_be_bytes());
    malicious.extend(column.to_be_bytes());
    malicious.push(0); // no typmod
    malicious.push(1); // default present
    malicious.extend(u64::MAX.to_be_bytes());
    assert!(matches!(
        from_records(catalog_ext_raw_records(malicious)),
        Err(CoreError::Corrupt(_)) | Err(CoreError::Limit)
    ));
}

#[test]
fn legacy_envelope_rejects_nonempty_catalog_extension() {
    let (mut s, _, cid) = catalog_ext_fixture();
    let mut legacy = records(&s).unwrap();
    assert!(!legacy.contains_key(&vec![10]));
    s.sql.set_column_default(cid, Some("42".into())).unwrap();
    let extended = records(&s).unwrap();
    legacy.insert(vec![5], extended[&vec![5]].clone());
    assert!(matches!(
        from_records(legacy),
        Err(CoreError::Corrupt(message)) if message == "feature content mismatch"
    ));
}

#[test]
fn catalog_default_preserves_readiness_but_blocks_projectors_until_cleared() {
    use crate::relational::{Prepared, SqlError};
    let mut state = empty(17);
    for sql in [
        "CREATE TABLE t (id BIGINT PRIMARY KEY, value TEXT)",
        "INSERT INTO t VALUES (1, 'one'), (2, 'two')",
    ] {
        let p = Prepared::parse(sql, 17, state.sql.epoch).unwrap();
        state.sql.execute(&p, &[], None).unwrap();
    }
    let table = state.sql.tables["t"].clone();
    let column = table.columns[1].id;
    let baseline = records(&state).unwrap();
    let check = |state: &State, extended: bool| {
        state.sql.assert_legacy_format_for_catalog_ext_test();
        let mut indexed = state.sql.clone();
        let p = Prepared::parse(
            "CREATE UNIQUE INDEX by_value ON t (value)",
            17,
            indexed.epoch,
        )
        .unwrap();
        indexed.execute(&p, &[], None).unwrap();
        let rows =
            state
                .sql
                .with_complete_table_engine(table.id, || false, |_, rows| Ok(rows.len()));
        let counters =
            state
                .sql
                .projector_counters_engine(state.sql.epoch, state.sql.generation, &mut || false);
        if extended {
            assert!(matches!(rows, Err(SqlError::Unsupported)));
            assert!(matches!(counters, Err(SqlError::Unsupported)));
        } else {
            assert_eq!(rows.unwrap(), 2);
            counters.unwrap();
        }
    };
    check(&state, false);
    state
        .sql
        .set_column_default(column, Some("'fallback'".into()))
        .unwrap();
    let expected_ext = state.sql.catalog_ext().clone();
    let reopened = from_records(records(&state).unwrap()).unwrap();
    for mut candidate in [state, reopened] {
        assert_eq!(candidate.sql.catalog_ext(), &expected_ext);
        check(&candidate, true);
        candidate.sql.set_column_default(column, None).unwrap();
        check(&candidate, false);
        assert_eq!(records(&candidate).unwrap()[&vec![5]], baseline[&vec![5]]);
        let reopened = from_records(records(&candidate).unwrap()).unwrap();
        check(&reopened, false);
        assert_eq!(records(&reopened).unwrap()[&vec![5]], baseline[&vec![5]]);
    }
}

#[test]
fn format_one_namespace_rejects_trailing_bytes_and_encoded_extension() {
    let (mut state, _, column) = catalog_ext_fixture();
    let base = records(&state).unwrap();
    state
        .sql
        .set_column_default(column, Some("42".into()))
        .unwrap();
    for suffix in [vec![0xff], encode(state.sql.catalog_ext()).unwrap()] {
        let mut malformed = base.clone();
        malformed.get_mut(&vec![5]).unwrap().extend(suffix);
        assert!(matches!(
            from_records(malformed),
            Err(CoreError::Corrupt(_))
        ));
    }
}

#[test]
fn sequence_counter_feature_bit_matches_records_and_requires_catalog_metadata() {
    let mut s = empty(17);
    let p = crate::relational::Prepared::parse(
        "CREATE TABLE counter_features (id serial PRIMARY KEY)",
        17,
        0,
    )
    .unwrap();
    s.sql.execute(&p, &[], None).unwrap();
    let id = *s.sql.catalog_ext().sequences.keys().next().unwrap();
    crate::storage::sequences::next(&mut s, id).unwrap();
    let r = records(&s).unwrap();
    let bits = u64::from_be_bytes(r[&vec![10]].as_slice().try_into().unwrap());
    assert_eq!(bits & (64 | 256), 64 | 256);
    assert_eq!(records(&normal(r.clone()).unwrap()).unwrap(), r);
    let counter_key = r.keys().find(|k| k.first() == Some(&11)).unwrap().clone();
    let mut missing = r.clone();
    missing.remove(&counter_key);
    assert!(matches!(normal(missing), Err(CoreError::Corrupt(_))));
    for forged_bits in [bits & !256, bits & !64] {
        let mut mismatch = r.clone();
        mismatch.insert(vec![10], forged_bits.to_be_bytes().to_vec());
        assert!(matches!(normal(mismatch), Err(CoreError::Corrupt(_))));
    }
    let mut legacy = r.clone();
    let mut metadata: Metadata = decode(&legacy[&vec![0]]).unwrap();
    metadata.format = format_policy::bits_to_legacy_version(bits & 63).unwrap();
    legacy.insert(vec![0], encode(&metadata).unwrap());
    legacy.remove(&vec![10]);
    assert!(matches!(normal(legacy), Err(CoreError::Corrupt(_))));
    assert!(matches!(
        crate::storage::replica::Image::from_bytes(encode(&r).unwrap()),
        Err(CoreError::Unsupported)
    ));
    // Partition projection begins with the same rejecting replica source decoder.
}
