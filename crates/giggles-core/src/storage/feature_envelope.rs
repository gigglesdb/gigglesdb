//! Format 53 content policy. Future owners replace only their named predicate.
use super::*;
use crate::relational::Relational;
pub(crate) const KEY_DEPENDENCY_IDENTITY: u64 = 1 << 17;
// Legacy format 53 has no WRITE_FENCE; retain its independent bit assignments.
pub(crate) const COLUMN_COLLATIONS: u64 = 1 << 23;
pub(crate) const USER_COLLATIONS: u64 = 1 << 22;
pub(crate) const UUID_ARRAY_COLUMNS: u64 = crate::storage::engine::meta::UUID_ARRAY_COLUMNS;
pub(crate) const TEXT_SEARCH_TYPES: u64 = super::engine::meta::TEXT_SEARCH_TYPES;
pub(crate) const GIN_INDEXES: u64 = crate::storage::engine::meta::GIN_INDEXES;
pub(crate) const GENERATED_COLUMNS: u64 = crate::storage::engine::meta::GENERATED_COLUMNS;
const WIDE_CATALOG: u64 = super::engine::meta::WIDE_CATALOG;
pub(crate) const PARTIAL_INDEXES: u64 = 1 << 33;
pub(crate) const TEMPORAL_INDEXES: u64 = 1 << 34;
pub(super) const NEW_BITS: u64 =
    0x3c0 | KEY_DEPENDENCY_IDENTITY | USER_COLLATIONS | COLUMN_COLLATIONS | UUID_ARRAY_COLUMNS | WIDE_CATALOG
        | PARTIAL_INDEXES | TEMPORAL_INDEXES | GENERATED_COLUMNS | TEXT_SEARCH_TYPES | GIN_INDEXES;
pub(super) const ROLE_BOOTSTRAP: u64 = 1 << 9;

/// E1 B0b: true when the base namespace-5 extension is nonempty.
fn catalog_ext_present(s: &State) -> bool {
    !s.sql.catalog_ext().namespace5_ext_is_empty()
}
/// Extended scalar metadata requires feature bit seven.
fn extended_types_present(s: &State) -> bool {
    s.sql
        .catalog_ext()
        .columns
        .values()
        .any(|column| column.typmod.is_some() || column.text_kind.is_some())
}
/// E1: true for any namespace-11 sequence counter.
fn sequence_counters_present(s: &State) -> bool {
    !s.seq.is_empty()
}
/// GD-0055 WO19: read the giggles_roles_v1 bootstrap marker in the same read
/// transaction as RECORDS. No bootstrap marker is defined or written today.
fn role_bootstrap_present(_: &State) -> bool {
    false
}

pub(super) fn wide_catalog_present(s: &State) -> bool {
    let ext = s.sql.catalog_ext();
    s.wide_catalog || crate::relational::catalog_scale::Counts {
        tables: s.sql.tables.len(),
        schemas: ext.schemas.len(),
        sequences: ext.sequences.len(),
        types: ext.types.len(),
        collations: ext.collations.len(),
        column_collations: ext.column_collations.len(),
        constraint_tables: ext.constraints.len(),
        constraints: ext.constraints.values().map(Vec::len).sum(),
    }
    .needs_feature(s.sql.secondary_descriptors_engine().iter())
}

pub(super) fn uuid_array_columns_present(s: &State) -> bool {
    s.uuid_array_columns || s.sql.catalog_ext().has_uuid_arrays()
}

pub(super) fn text_search_types_present(s: &State) -> bool {
    s.text_search_types || s.sql.catalog_ext().has_text_search_types()
}

pub(super) fn feature_bits_present(s: &State) -> Result<u64, CoreError> {
    let typed = s.sql.contains_typed()
        || s.native_outcomes
            .as_ref()
            .is_some_and(|n| n.contains_typed().unwrap_or(true));
    let temporal_indexes = crate::relational::secondary_index::has_temporal_indexes(
        s.sql.secondary_descriptors_engine(), s.sql.tables.values(),
    );
    Ok(u64::from(typed)
        | (u64::from(s.native_outcomes.is_some()) << 1)
        | (u64::from(s.sql.has_secondary_indexes_engine()) << 2)
        | (u64::from(contains_temporal(s)?) << 4)
        | (u64::from(s.sql.contains_heap()) << 5)
        | (u64::from(catalog_ext_present(s)) << 6)
        | (u64::from(extended_types_present(s)) << 7)
        | (u64::from(sequence_counters_present(s)) << 8)
        | (u64::from(role_bootstrap_present(s)) << 9)
        | if wide_catalog_present(s) { WIDE_CATALOG } else { 0 }
        | if s.sql.has_partial_indexes_engine() { PARTIAL_INDEXES } else { 0 }
        | if temporal_indexes { TEMPORAL_INDEXES } else { 0 }
        | if !s.sql.catalog_ext().column_collations.is_empty() { COLUMN_COLLATIONS } else { 0 }
        | if !s.sql.catalog_ext().collations.is_empty() { USER_COLLATIONS } else { 0 }
        | if text_search_types_present(s) { TEXT_SEARCH_TYPES } else { 0 }
        | if uuid_array_columns_present(s) { UUID_ARRAY_COLUMNS } else { 0 }
        | if s.sql.catalog_ext().gin_indexes.is_empty() { 0 } else { GIN_INDEXES }
        | if s.sql.catalog_ext().generated_columns.is_empty() { 0 } else { GENERATED_COLUMNS }
        | if s.sql.catalog_ext().has_key_dependency_identity() { KEY_DEPENDENCY_IDENTITY } else { 0 })
}

/// Records-only exports never carry role bootstrap state (DD-0068 bit-9 rule 5).
pub(super) fn export_allowed(bits: u64) -> Result<(), CoreError> {
    if bits & ROLE_BOOTSTRAP != 0 {
        return Err(CoreError::Unsupported);
    }
    Ok(())
}

pub(super) fn decode_bits(
    records: &BTreeMap<Vec<u8>, Vec<u8>>,
    format: u32,
    import: bool,
) -> Result<Option<u64>, CoreError> {
    let record = records.get(&vec![10]);
    if format != 53 {
        if record.is_some() {
            return Err(CoreError::Corrupt(
                "feature record without format 53".into(),
            ));
        }
        return Ok(None);
    }
    let bytes: [u8; 8] = record
        .and_then(|v| v.as_slice().try_into().ok())
        .ok_or_else(|| CoreError::Corrupt("missing or malformed feature record".into()))?;
    let bits = u64::from_be_bytes(bytes);
    if bits & !(63 | NEW_BITS) != 0 {
        return Err(CoreError::Unsupported);
    }
    if import && bits & ROLE_BOOTSTRAP != 0 {
        return Err(CoreError::Unsupported);
    }
    if bits & (PARTIAL_INDEXES | TEMPORAL_INDEXES) != 0 && bits & 4 == 0 {
        return Err(CoreError::Corrupt("index features require secondary indexes".into()));
    }
    if bits & NEW_BITS == 0 || (bits & (256 | KEY_DEPENDENCY_IDENTITY | USER_COLLATIONS | COLUMN_COLLATIONS) != 0 && bits & 64 == 0) {
        return Err(CoreError::Corrupt("noncanonical feature bits".into()));
    }
    Ok(Some(bits))
}

/// Decode the relational payload once while preserving forward-version errors.
pub(super) fn decode_relational(bytes: &[u8]) -> Result<Relational, CoreError> {
    codec().deserialize(bytes).map_err(|error| relational_error(&error))
}

fn relational_error(error: &bincode::ErrorKind) -> CoreError {
        if matches!(error, bincode::ErrorKind::Custom(message)
            if message == crate::relational::catalog_ext::UNSUPPORTED_CATALOG_EXT)
        {
            CoreError::Unsupported
        } else {
            CoreError::Corrupt(error.to_string())
        }
}

pub(super) fn append_relational_metadata(base: &mut Vec<u8>, sql: &Relational) -> Result<(), CoreError> {
    if !sql.catalog_ext().gin_indexes.is_empty() {
        base.extend(encode(&sql.catalog_ext().gin_indexes)?);
    }
    if !sql.catalog_ext().generated_columns.is_empty() {
        base.extend(encode(&sql.catalog_ext().generated_columns)?);
    }
    Ok(())
}

pub(super) fn decode_relational_with_bits(bytes: &[u8], bits: u64) -> Result<Relational, CoreError> {
    if bits & (GENERATED_COLUMNS | GIN_INDEXES) == 0 {
        return decode_relational(bytes);
    }
    let mut input = std::io::Cursor::new(bytes);
    let mut sql: Relational = codec().deserialize_from(&mut input).map_err(|error| relational_error(&error))?;
    let gin = if bits & GIN_INDEXES == 0 { BTreeMap::new() } else {
        codec().deserialize_from(&mut input).map_err(|error| relational_error(&error))?
    };
    let generated = if bits & GENERATED_COLUMNS == 0 { BTreeMap::new() } else {
        codec().deserialize_from(&mut input).map_err(|error| relational_error(&error))?
    };
    if input.position() != bytes.len() as u64 {
        return Err(CoreError::Corrupt("trailing relational catalog bytes".into()));
    }
    if (bits & GIN_INDEXES != 0) == gin.is_empty()
        || (bits & GENERATED_COLUMNS != 0) == generated.is_empty()
    {
        return Err(CoreError::Corrupt("feature content mismatch".into()));
    }
    sql.restore_table_metadata(gin, generated);
    Ok(sql)
}

#[cfg(test)]
#[path = "feature_envelope_tests.rs"]
mod tests;

#[cfg(test)]
#[test]
fn text_kind_requires_catalog_and_extended_feature_bits() {
    use crate::relational::{Prepared, catalog_ext::TextKind};
    let mut state = empty(17);
    let p = Prepared::parse("CREATE TABLE t (id BIGINT PRIMARY KEY, value TEXT)", 17, 0).unwrap();
    state.sql.execute(&p, &[], None).unwrap();
    let id = state.sql.tables["t"].columns[1].id;
    state
        .sql
        .set_column_text_kind(id, Some(TextKind::Varchar))
        .unwrap();
    let bits = feature_bits_present(&state).unwrap();
    assert_eq!(bits & (3 << 6), 3 << 6);
    let mut map = records(&state).unwrap();
    map.insert(vec![10], (bits & !(1 << 7)).to_be_bytes().to_vec());
    assert!(matches!(
        from_records_mode(map, &mut || false, false),
        Err(CoreError::Corrupt(_))
    ));
}

#[cfg(test)]
#[test]
fn user_collations_require_feature_witness_and_roundtrip() {
    use crate::relational::Prepared;
    let mut state=empty(17);
    assert_eq!(feature_bits_present(&state).unwrap() & USER_COLLATIONS,0);
    let p=Prepared::parse("CREATE COLLATION n (provider=icu,locale='und')",17,0).unwrap();
    state.sql.execute(&p,&[],None).unwrap();
    let bits=feature_bits_present(&state).unwrap(); assert_ne!(bits & USER_COLLATIONS,0);
    let records=records(&state).unwrap();
    assert_eq!(from_records_mode(records.clone(),&mut || false,false).unwrap().sql.catalog_ext().collations.len(),1);
    let mut bad=records; bad.insert(vec![10],(bits & !USER_COLLATIONS).to_be_bytes().to_vec());
    assert!(from_records_mode(bad,&mut || false,false).is_err());
}
