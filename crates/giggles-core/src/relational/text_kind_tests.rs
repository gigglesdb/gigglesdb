use super::*;
use bincode::Options;
fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes()
}
fn fixture() -> (Relational, u64) {
    let mut state = Relational::default();
    let p = Prepared::parse("CREATE TABLE t (id BIGINT PRIMARY KEY, value TEXT)", 1, 0).unwrap();
    state.execute(&p, &[], None).unwrap();
    let id = state.tables["t"].columns[1].id;
    (state, id)
}
fn assert_invalid_catalog(state: &Relational) {
    assert_eq!(state.validate(), Err(SqlError::Type));
    let bytes = codec().serialize(state).unwrap();
    let error = codec().deserialize::<Relational>(&bytes).unwrap_err();
    assert!(
        !error
            .to_string()
            .contains(catalog_ext::UNSUPPORTED_CATALOG_EXT)
    );
}
#[test]
fn text_kind_roundtrips_and_old_namespace_tags() {
    for (kind, ty, typmod) in [
        (TextKind::Varchar, DataType::Varchar, Some(9)),
        (TextKind::Bpchar, DataType::Bpchar, Some(7)),
        (TextKind::Varchar, DataType::Varchar, None),
        (TextKind::Bpchar, DataType::Bpchar, None),
    ] {
        let (mut state, id) = fixture();
        let old_tables = codec().serialize(&state.tables).unwrap();
        state.set_column_text_kind(id, Some(kind)).unwrap();
        state.set_column_typmod(id, typmod).unwrap();
        assert_eq!(state.ext.ext_version, 3);
        assert_eq!(codec().serialize(&state.tables).unwrap(), old_tables);
        let bytes = codec().serialize(&state).unwrap();
        let decoded = codec().deserialize::<Relational>(&bytes).unwrap();
        assert_eq!(decoded.tables["t"].columns[1].data_type, ty);
        assert_eq!(decoded.ext.columns[&id].text_kind, Some(kind));
        assert_eq!(decoded.ext.columns[&id].typmod, typmod);
    }
    assert_eq!(
        codec().serialize(&DataType::Varchar).unwrap(),
        codec().serialize(&DataType::Text).unwrap()
    );
    assert_eq!(
        codec().serialize(&DataType::Bpchar).unwrap(),
        codec().serialize(&DataType::Text).unwrap()
    );
    assert!(
        codec()
            .deserialize::<DataType>(&15u32.to_be_bytes())
            .is_err()
    );
}
#[test]
fn text_kind_invariants_and_wire_rejections() {
    let (mut state, id) = fixture();
    state
        .set_column_text_kind(id, Some(TextKind::Varchar))
        .unwrap();
    for bad in [4, 10_485_765] {
        assert!(state.set_column_typmod(id, Some(bad)).is_err());
    }
    state.set_column_typmod(id, Some(9)).unwrap();
    // Retain varchar(5)'s typmod: without its kind the legacy Text wire type
    // cannot accept that typmod. A bare kind-free Text payload remains legacy.
    state.ext.columns.get_mut(&id).unwrap().text_kind = None;
    state.ext.ext_version = 1;
    assert_invalid_catalog(&state);
    state.ext.columns.get_mut(&id).unwrap().text_kind = Some(TextKind::Varchar);
    state.ext.ext_version = 3;
    state.tables.get_mut("t").unwrap().columns[1].data_type = DataType::Integer;
    assert_invalid_catalog(&state);
    let (mut state, id) = fixture();
    state
        .set_column_text_kind(id, Some(TextKind::Varchar))
        .unwrap();
    let valid = codec().serialize(&state.ext).unwrap();
    let relational = codec().serialize(&state).unwrap();
    let ext_offset = relational.len() - valid.len();
    assert_eq!(&relational[ext_offset..], valid);
    let mut bytes = valid.clone();
    // Version, map length, column id, three absent options, present text kind.
    let kind_offset = 4 + 8 + 8 + 3 + 1;
    bytes[kind_offset..kind_offset + 4].copy_from_slice(&2u32.to_be_bytes());
    let error = codec().deserialize::<CatalogExt>(&bytes).unwrap_err();
    assert!(
        !error
            .to_string()
            .contains(catalog_ext::UNSUPPORTED_CATALOG_EXT)
    );
    let mut unknown_kind = relational.clone();
    unknown_kind[ext_offset..].copy_from_slice(&bytes);
    let error = codec()
        .deserialize::<Relational>(&unknown_kind)
        .unwrap_err();
    assert!(
        !error
            .to_string()
            .contains(catalog_ext::UNSUPPORTED_CATALOG_EXT)
    );
    let mut bytes = valid;
    // A version no build will ever write.
    bytes[..4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(
        codec()
            .deserialize::<CatalogExt>(&bytes)
            .unwrap_err()
            .to_string()
            .contains(catalog_ext::UNSUPPORTED_CATALOG_EXT)
    );
    let mut unsupported = relational;
    unsupported[ext_offset..].copy_from_slice(&bytes);
    assert!(
        codec()
            .deserialize::<Relational>(&unsupported)
            .unwrap_err()
            .to_string()
            .contains(catalog_ext::UNSUPPORTED_CATALOG_EXT)
    );
}

#[test]
fn all_text_kinds_share_one_catalog_and_legacy_payload() {
    let mut state = Relational::default();
    let p = Prepared::parse(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a TEXT, b TEXT, c TEXT, d TEXT)",
        1,
        0,
    )
    .unwrap();
    state.execute(&p, &[], None).unwrap();
    let legacy = codec().serialize(&state).unwrap();
    let ids: Vec<_> = state.tables["t"]
        .columns
        .iter()
        .skip(1)
        .map(|c| c.id)
        .collect();
    let cases = [
        (TextKind::Varchar, Some(9)),
        (TextKind::Bpchar, Some(7)),
        (TextKind::Varchar, None),
        (TextKind::Bpchar, None),
    ];
    for (&id, &(kind, typmod)) in ids.iter().zip(&cases) {
        state.set_column_text_kind(id, Some(kind)).unwrap();
        state.set_column_typmod(id, typmod).unwrap();
    }
    let encoded = codec().serialize(&state).unwrap();
    let decoded: Relational = codec().deserialize(&encoded).unwrap();
    decoded.validate().unwrap();
    assert_eq!(decoded.tables, state.tables);
    assert_eq!(decoded.ext, state.ext);
    for (&id, &(kind, typmod)) in ids.iter().zip(&cases) {
        let column = decoded.tables["t"]
            .columns
            .iter()
            .find(|c| c.id == id)
            .unwrap();
        assert_eq!(
            column.data_type,
            match kind {
                TextKind::Varchar => DataType::Varchar,
                TextKind::Bpchar => DataType::Bpchar,
            }
        );
        assert_eq!(decoded.ext.columns[&id].text_kind, Some(kind));
        assert_eq!(decoded.ext.columns[&id].typmod, typmod);
    }
    // Format is the first u32; all following namespace-five legacy fields
    // occupy exactly the old payload length, with extension trailing them.
    let mut prefix = encoded[..legacy.len()].to_vec();
    prefix[..4].copy_from_slice(&1u32.to_be_bytes());
    assert_eq!(prefix, legacy);
    for bad in [4, 10_485_765] {
        let mut broken = state.clone();
        broken.ext.columns.get_mut(&ids[0]).unwrap().typmod = Some(bad);
        assert_invalid_catalog(&broken);
    }
}

#[test]
fn varchar_uses_text_greatest_least_and_common_types() {
    let (mut state, id) = fixture();
    let insert = Prepared::parse("INSERT INTO t VALUES (1, 'pear  ')", 1, state.epoch).unwrap();
    state.execute(&insert, &[], None).unwrap();
    state
        .set_column_text_kind(id, Some(TextKind::Varchar))
        .unwrap();
    for (sql, expected) in [
        ("SELECT greatest(value, 'z'::text) FROM t", "z"),
        ("SELECT least(value, 'a'::text) FROM t", "a"),
        (
            "SELECT CASE WHEN true THEN value ELSE 'fallback'::text END FROM t",
            "pear  ",
        ),
        (
            "SELECT CASE WHEN false THEN value ELSE 'fallback'::text END FROM t",
            "fallback",
        ),
        ("SELECT coalesce(value, 'fallback'::text) FROM t", "pear  "),
        ("SELECT coalesce(NULL::text, value) FROM t", "pear  "),
    ] {
        let query = Prepared::parse(sql, 1, state.epoch).unwrap();
        let description = query.describe(&state).unwrap();
        assert_eq!(description.columns[0].data_type, DataType::Text, "{sql}");
        let result = state.execute(&query, &[], None).unwrap();
        assert_eq!(result.rows.len(), 1, "{sql}");
        assert_eq!(
            result.rows[0].values,
            vec![Value::Text(expected.into())],
            "{sql}"
        );
    }
}

#[test]
fn bpchar_refuses_greatest_least_and_common_types() {
    let (mut state, id) = fixture();
    let insert = Prepared::parse("INSERT INTO t VALUES (1, 'pear  ')", 1, state.epoch).unwrap();
    state.execute(&insert, &[], None).unwrap();
    state
        .set_column_text_kind(id, Some(TextKind::Bpchar))
        .unwrap();
    for sql in [
        "SELECT greatest(value, 'z'::text) FROM t",
        "SELECT least(value, 'a'::text) FROM t",
        "SELECT CASE WHEN true THEN value ELSE 'fallback'::text END FROM t",
        "SELECT CASE WHEN false THEN value ELSE 'fallback'::text END FROM t",
        "SELECT coalesce(value, 'fallback'::text) FROM t",
        "SELECT coalesce(NULL::text, value) FROM t",
    ] {
        let query = Prepared::parse(sql, 1, state.epoch).unwrap();
        assert_eq!(
            query.describe(&state).unwrap_err(),
            SqlError::Unsupported,
            "{sql}"
        );
        assert_eq!(
            state.execute(&query, &[], None).unwrap_err(),
            SqlError::Unsupported,
            "{sql}"
        );
    }
}

#[test]
fn format_accepts_varchar_templates_and_bpchar_output_arguments() {
    for (kind, value, sql) in [
        (
            TextKind::Varchar,
            "%s|%I|%L",
            "SELECT format(value, 'a  ', 'a  ', 'a  ') FROM t",
        ),
        (
            TextKind::Bpchar,
            "a  ",
            "SELECT format('%s|%I|%L', value, value, value) FROM t",
        ),
    ] {
        let (mut state, id) = fixture();
        let insert = Prepared::parse("INSERT INTO t VALUES (1, $1)", 1, state.epoch).unwrap();
        state
            .execute(&insert, &[Value::Text(value.into())], None)
            .unwrap();
        state.set_column_text_kind(id, Some(kind)).unwrap();
        let query = Prepared::parse(sql, 1, state.epoch).unwrap();
        assert_eq!(
            query.describe(&state).unwrap().columns[0].data_type,
            DataType::Text,
            "{sql}"
        );
        let result = state.execute(&query, &[], None).unwrap();
        assert_eq!(result.rows.len(), 1, "{sql}");
        assert_eq!(
            result.rows[0].values,
            vec![Value::Text("a  |\"a  \"|'a  '".into())],
            "{sql}"
        );
    }
    let (mut state, id) = fixture();
    let insert = Prepared::parse("INSERT INTO t VALUES (1, '%s')", 1, state.epoch).unwrap();
    state.execute(&insert, &[], None).unwrap();
    state
        .set_column_text_kind(id, Some(TextKind::Bpchar))
        .unwrap();
    let query = Prepared::parse("SELECT format(value, 'a') FROM t", 1, state.epoch).unwrap();
    assert_eq!(query.describe(&state).unwrap_err(), SqlError::Unsupported);
    assert_eq!(
        state.execute(&query, &[], None).unwrap_err(),
        SqlError::Unsupported
    );
}
/// Golden ext_version-3 CatalogExt bytes (DD-0068 packet). Column 7 is varchar(5) and
/// column 8 is bare bpchar; the bytes are the canonical big-endian fixint encoding.
const EXT_V3_GOLDEN: &str = "000000030000000000000002000000000000000701000000090000010000000000000000000000080000000100000001000000000000000000000000000000000000000000000000";
#[test]
fn ext_version_3_golden_bytes() {
    let mut ext = CatalogExt::default();
    ext.columns.insert(
        7,
        ColumnExt {
            typmod: Some(9),
            text_kind: Some(TextKind::Varchar),
            ..ColumnExt::default()
        },
    );
    ext.columns.insert(
        8,
        ColumnExt {
            text_kind: Some(TextKind::Bpchar),
            ..ColumnExt::default()
        },
    );
    Relational::normalize_ext(&mut ext);
    assert_eq!(ext.ext_version, 3);
    let bytes = codec().serialize(&ext).unwrap();
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, EXT_V3_GOLDEN);
    assert_eq!(codec().deserialize::<CatalogExt>(&bytes).unwrap(), ext);
}
