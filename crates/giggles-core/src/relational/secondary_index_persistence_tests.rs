use super::*;

fn sql(s: &mut Relational, text: &str) {
    let p = Prepared::parse(text, 1, s.epoch).unwrap();
    s.execute(&p, &[], None).unwrap();
}
fn fixture() -> Relational {
    let mut s = Relational::default();
    sql(
        &mut s,
        "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT)",
    );
    sql(&mut s, "INSERT INTO items VALUES (1,'a')");
    let t = &s.tables["items"];
    s.secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false)
        .unwrap();
    s
}
fn pending(bytes: &[u8]) -> si::codec::PendingDecode {
    si::codec::decode_pending(bytes, || false).unwrap()
}
fn pair(s: &Relational) -> (Vec<u8>, Vec<u8>) {
    s.secondary_persistence_engine(|| false).unwrap()
}

#[test]
fn pending_descriptors_then_exact_complete_finalize_and_compatible_wrapper() {
    let s = fixture();
    let (base, payload) = pair(&s);
    let mut restored: Relational = bincode::deserialize(&base).unwrap();
    assert!(!restored.has_secondary_indexes_engine());
    let p = pending(&payload);
    assert_eq!(p.descriptors(), s.secondary.descriptors());
    let snap = snapshot(&restored, p.descriptors(), NONE, &mut || false).unwrap();
    let tuples = snap.tuples();
    let rows = snap.rows(&tuples);
    assert_eq!(
        si::codec::decode_validated(&payload, &rows, &CatalogView::new(&restored), || false).unwrap(),
        p.validate_complete(&rows, &CatalogView::new(&restored), || false)
            .unwrap()
    );
    // A SELECT-sized prefix is never supplied by the real adapter. Exact
    // validation catches an omitted row relative to the persisted payload.
    assert_eq!(
        pending(&payload).validate_complete(&[], &CatalogView::new(&restored), || false),
        Err(si::Error::Corrupt)
    );
    restored
        .secondary_restore_engine(pending(&payload), || false)
        .unwrap();
    assert_eq!(restored, s);
    assert!(bincode::serialize(&restored).is_err());
}

#[test]
fn structural_admission_rejects_malformed_descriptors_and_late_payload() {
    let (_, payload) = pair(&fixture());
    // First descriptor: id16, table24, name length32, name36, arity42,
    // column46, key encoding54 (the name is exactly "by_tag").
    for (offset, bytes) in [
        (16, 0u64.to_le_bytes().to_vec()),
        (24, 0u64.to_le_bytes().to_vec()),
        (32, u32::MAX.to_le_bytes().to_vec()),
        (36, vec![0xff]),
        (42, 0u32.to_le_bytes().to_vec()),
        (46, 0u64.to_le_bytes().to_vec()),
        (54, 2u32.to_le_bytes().to_vec()),
    ] {
        let mut bad = payload.clone();
        bad[offset..offset + bytes.len()].copy_from_slice(&bytes);
        assert!(
            si::codec::decode_pending(&bad, || false).is_err(),
            "offset {offset}"
        );
    }
    for end in 0..payload.len() {
        assert!(si::codec::decode_pending(&payload[..end], || false).is_err());
    }
    let mut late = payload;
    late.push(0);
    assert!(si::codec::decode_pending(&late, || false).is_err());
    let huge = vec![0; si::codec::MAX_PAYLOAD_BYTES + 1];
    let mut calls = 0;
    assert!(matches!(
        si::codec::decode_pending(&huge, || {
            calls += 1;
            false
        }),
        Err(si::Error::Limit)
    ));
    assert_eq!(calls, 0);
}

#[test]
fn exact_restore_rejects_payload_rows_catalog_and_allocator_mismatches_atomically() {
    let s = fixture();
    let (_, payload) = pair(&s);
    let mut bad_payload = payload.clone();
    *bad_payload.last_mut().unwrap() ^= 1; // structurally legal wrong PK payload
    assert!(si::codec::decode_pending(&bad_payload, || false).is_ok());
    let mut target = s.clone();
    assert_eq!(
        target.secondary_restore_engine(pending(&bad_payload), || false),
        Err(SqlError::Type)
    );
    assert_eq!(target, s);
    for case in 0..5 {
        let mut target = s.clone();
        let id = target.tables["items"].id;
        match case {
            0 => {
                target.rows.get_mut(&id).unwrap().clear();
            }
            1 => {
                sql(&mut target, "INSERT INTO items VALUES (2,'b')");
            }
            2 => {
                std::sync::Arc::make_mut(&mut target.tables).get_mut("items").unwrap().columns[1].id += 100;
            }
            3 => {
                std::sync::Arc::make_mut(&mut target.tables).get_mut("items").unwrap().id += 100;
            }
            _ => {
                target.next_id = target.secondary.descriptors()[0].id;
            }
        }
        let before = target.clone();
        assert!(
            target
                .secondary_restore_engine(pending(&payload), || false)
                .is_err()
        );
        assert_eq!(target, before, "case {case}");
    }
    let mut empty = b"GD42IDX!".to_vec();
    empty.extend_from_slice(&1u32.to_le_bytes());
    empty.extend_from_slice(&0u32.to_le_bytes());
    empty.extend_from_slice(&0u32.to_le_bytes());
    assert!(
        target
            .secondary_restore_engine(pending(&empty), || false)
            .is_err()
    );
    assert_eq!(target, s);
}

#[test]
fn cancellation_at_every_pending_finalize_restore_and_encoding_fence() {
    let mut source = fixture();
    let mut target = source.clone();
    let t = &source.tables["items"];
    source
        .secondary_configure_test(t.id, "by_id", &[t.columns[0].id], || false)
        .unwrap();
    target.next_id = source.next_id;
    target.epoch = source.epoch;
    let (_, payload) = pair(&source);
    let mut calls = 0;
    si::codec::decode_pending(&payload, || {
        calls += 1;
        false
    })
    .unwrap();
    for stop in 1..=calls {
        let mut n = 0;
        assert!(matches!(
            si::codec::decode_pending(&payload, || {
                n += 1;
                n == stop
            }),
            Err(si::Error::Cancelled)
        ));
    }
    let p = pending(&payload);
    let snap = snapshot(&target, p.descriptors(), NONE, &mut || false).unwrap();
    let tuples = snap.tuples();
    let rows = snap.rows(&tuples);
    calls = 0;
    p.validate_complete(&rows, &CatalogView::new(&target), || {
        calls += 1;
        false
    })
    .unwrap();
    for stop in 1..=calls {
        let mut n = 0;
        assert_eq!(
            pending(&payload).validate_complete(&rows, &CatalogView::new(&target), || {
                n += 1;
                n == stop
            }),
            Err(si::Error::Cancelled)
        );
    }
    calls = 0;
    let mut success = target.clone();
    success
        .secondary_restore_engine(pending(&payload), || {
            calls += 1;
            false
        })
        .unwrap();
    assert_eq!(success, source);
    for stop in 1..=calls {
        let mut attempt = target.clone();
        let mut n = 0;
        assert_eq!(
            attempt.secondary_restore_engine(pending(&payload), || {
                n += 1;
                n == stop
            }),
            Err(SqlError::Aborted)
        );
        assert_eq!(attempt, target, "restore fence {stop}");
    }
    calls = 0;
    source
        .secondary_persistence_engine(|| {
            calls += 1;
            false
        })
        .unwrap();
    for stop in 1..=calls {
        let mut n = 0;
        assert_eq!(
            source.secondary_persistence_engine(|| {
                n += 1;
                n == stop
            }),
            Err(SqlError::Aborted)
        );
        assert_eq!(source, success);
    }
}

#[test]
fn empty_tables_require_supported_declared_pk_and_index_types() {
    let mut s = fixture();
    sql(&mut s, "DELETE FROM items WHERE id=1");
    let (base, payload) = pair(&s);
    let mut restored: Relational = bincode::deserialize(&base).unwrap();
    restored
        .secondary_restore_engine(pending(&payload), || false)
        .unwrap();
    assert_eq!(restored, s);
    for ty in [DataType::Numeric, DataType::Real, DataType::DoublePrecision] {
        for column in [0, 1] {
            let mut target: Relational = bincode::deserialize(&base).unwrap();
            std::sync::Arc::make_mut(&mut target.tables).get_mut("items").unwrap().columns[column].data_type = ty.clone();
            let before = target.clone();
            assert_eq!(
                target.secondary_restore_engine(pending(&payload), || false),
                Err(SqlError::Unsupported)
            );
            assert_eq!(target, before);
        }
    }
}

#[test]
fn all_six_existing_types_roundtrip_from_complete_authoritative_maps() {
    for (ty, value) in [
        ("BOOLEAN", Value::Bool(true)),
        ("SMALLINT", Value::Int(-3)),
        ("INTEGER", Value::Int(-4)),
        ("BIGINT", Value::Int(-5)),
        ("TEXT", Value::Text("a\0b".into())),
        ("BYTEA", Value::Bytes(vec![0, 255, 1])),
    ] {
        let mut s = Relational::default();
        sql(
            &mut s,
            &format!("CREATE TABLE items (id {ty} PRIMARY KEY, tag {ty})"),
        );
        let p =
            Prepared::insert_engine(&s.tables["items"], vec![value.clone(), value], 1, &s).unwrap();
        s.execute(&p, &[], None).unwrap();
        let t = &s.tables["items"];
        s.secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false)
            .unwrap();
        let (base, payload) = pair(&s);
        let mut restored: Relational = bincode::deserialize(&base).unwrap();
        restored
            .secondary_restore_engine(pending(&payload), || false)
            .unwrap();
        assert_eq!(restored, s, "{ty}");
    }
}

#[test]
fn paired_namespace5_bytes_are_exact_legacy_fields_without_sidecar() {
    let s = fixture();
    let (base, _) = pair(&s);
    // Independent legacy wire struct, including TableDef field order. No new
    // index registry/field is appended to either namespace5 structure.
    #[derive(Serialize)]
    struct LegacyTable<'a> {
        id: u64,
        name: &'a str,
        columns: &'a [ColumnDef],
        primary_key: &'a [usize],
    }
    #[derive(Serialize)]
    struct Legacy<'a> {
        format: u32,
        generation: u64,
        epoch: u64,
        next_id: u64,
        next_version: u64,
        tables: BTreeMap<&'a str, LegacyTable<'a>>,
        rows: &'a BTreeMap<u64, BTreeMap<Vec<u8>, SqlRow>>,
    }
    let legacy = Legacy {
        format: s.format,
        generation: s.generation,
        epoch: s.epoch,
        next_id: s.next_id,
        next_version: s.next_version,
        tables: s
            .tables
            .iter()
            .map(|(name, t)| {
                (
                    name.as_str(),
                    LegacyTable {
                        id: t.id,
                        name: &t.name,
                        columns: &t.columns,
                        primary_key: &t.primary_key,
                    },
                )
            })
            .collect(),
        rows: &s.rows,
    };
    assert_eq!(base, bincode::serialize(&legacy).unwrap());
    let decoded: Relational = bincode::deserialize(&base).unwrap();
    assert!(!decoded.has_secondary_indexes_engine());
    assert_eq!(base, bincode::serialize(&decoded).unwrap());
    assert!(bincode::serialize(&s).is_err());
    assert_eq!(
        Relational::default().secondary_persistence_engine(|| false),
        Err(SqlError::Unsupported)
    );
    // Literal legacy default bytes: u32 format + four u64 counters + two empty maps.
    let mut raw = vec![1, 0, 0, 0];
    for n in [0u64, 0, 1, 1, 0, 0] {
        raw.extend_from_slice(&n.to_le_bytes());
    }
    assert_eq!(bincode::serialize(&Relational::default()).unwrap(), raw);
}

#[test]
fn unique_v2_strict_canonical_codec_and_complete_restore() {
    let mut s = Relational::default();
    sql(
        &mut s,
        "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT)",
    );
    let t = s.tables["items"].clone();
    s.create_secondary_index_internal_engine(
        &t,
        s.epoch,
        "by_tag",
        &[t.columns[1].id],
        true,
        || false,
    )
    .unwrap();
    let (base, empty) = pair(&s);
    assert_eq!(&empty[8..12], &2u32.to_le_bytes());
    assert!(pending(&empty).descriptors()[0].unique);
    let mut restored: Relational = bincode::deserialize(&base).unwrap();
    restored
        .secondary_restore_engine(pending(&empty), || false)
        .unwrap();
    assert_eq!(restored, s);
    // by_tag descriptor bool is exactly byte58, immediately after encoding u32.
    assert_eq!(empty[58], 1);
    for value in [0, 2, 255] {
        let mut bad = empty.clone();
        bad[58] = value;
        assert!(si::codec::decode_pending(&bad, || false).is_err());
    }
    let mut missing = empty.clone();
    missing.remove(58);
    assert!(si::codec::decode_pending(&missing, || false).is_err());
    for end in 0..empty.len() {
        assert!(si::codec::decode_pending(&empty[..end], || false).is_err());
    }
    let mut trailing = empty.clone();
    trailing.push(0);
    assert!(si::codec::decode_pending(&trailing, || false).is_err());
    let mut zero = b"GD42IDX!".to_vec();
    zero.extend(2u32.to_le_bytes());
    zero.extend([0; 8]);
    assert!(si::codec::decode_pending(&zero, || false).is_err());
    for revision in [0, 3, u32::MAX] {
        let mut bad = empty.clone();
        bad[8..12].copy_from_slice(&revision.to_le_bytes());
        assert!(si::codec::decode_pending(&bad, || false).is_err());
    }
    sql(&mut s, "INSERT INTO items VALUES (1,'a')");
    // Add an independent nonunique descriptor using a test fixture, preserving v2.
    s.secondary_configure_test(t.id, "other", &[t.columns[1].id], || false)
        .unwrap();
    let (base, payload) = pair(&s);
    let p = pending(&payload);
    assert!(p.descriptors()[0].unique);
    assert!(!p.descriptors()[1].unique);
    let mut restored: Relational = bincode::deserialize(&base).unwrap();
    restored.secondary_restore_engine(p, || false).unwrap();
    assert_eq!(restored, s);
    // Corrupt complete base independently; a forged payload cannot publish a
    // usable sidecar before recomputation detects duplicate nonNULL ownership.
    let mut base: Relational = bincode::deserialize(&base).unwrap();
    let mut row = base.rows[&t.id].values().next().unwrap().clone();
    row.values[0] = Value::Int(2);
    row.version = base.next_version;
    base.next_version += 1;
    base.rows
        .get_mut(&t.id)
        .unwrap()
        .insert(pk(&t, &row.values).unwrap(), row);
    let before = base.clone();
    assert_eq!(
        base.secondary_restore_engine(pending(&payload), || false),
        Err(SqlError::Duplicate)
    );
    assert_eq!(base, before);
}
