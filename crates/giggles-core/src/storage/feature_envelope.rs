//! Format 53 content policy. Future owners replace only their named predicate.
use super::*;
use crate::relational::Relational;
pub(super) const NEW_BITS: u64 = 0x3c0;
pub(super) const ROLE_BOOTSTRAP: u64 = 1 << 9;

/// E1 B0b: true when CatalogExt is nonempty.
fn catalog_ext_present(s: &State) -> bool {
    !s.sql.catalog_ext().is_empty()
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

pub(super) fn feature_bits_present(s: &State) -> Result<u64, CoreError> {
    let typed = s.sql.contains_typed()
        || s.applications.as_ref().is_some_and(|a| a.contains_typed())
        || s.native_outcomes
            .as_ref()
            .is_some_and(|n| n.contains_typed().unwrap_or(true));
    Ok(u64::from(typed)
        | (u64::from(s.native_outcomes.is_some()) << 1)
        | (u64::from(s.sql.has_secondary_indexes_engine()) << 2)
        | (u64::from(s.applications.is_some()) << 3)
        | (u64::from(contains_temporal(s)?) << 4)
        | (u64::from(s.sql.contains_heap()) << 5)
        | (u64::from(catalog_ext_present(s)) << 6)
        | (u64::from(extended_types_present(s)) << 7)
        | (u64::from(sequence_counters_present(s)) << 8)
        | (u64::from(role_bootstrap_present(s)) << 9))
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
    if bits & !1023 != 0 {
        return Err(CoreError::Unsupported);
    }
    if import && bits & ROLE_BOOTSTRAP != 0 {
        return Err(CoreError::Unsupported);
    }
    if bits & NEW_BITS == 0 || (bits & 256 != 0 && bits & 64 == 0) {
        return Err(CoreError::Corrupt("noncanonical feature bits".into()));
    }
    Ok(Some(bits))
}

/// Decode the relational payload once while preserving forward-version errors.
pub(super) fn decode_relational(bytes: &[u8]) -> Result<Relational, CoreError> {
    codec().deserialize(bytes).map_err(|error| {
        if matches!(error.as_ref(), bincode::ErrorKind::Custom(message)
            if message == crate::relational::catalog_ext::UNSUPPORTED_CATALOG_EXT)
        {
            CoreError::Unsupported
        } else {
            CoreError::Corrupt(error.to_string())
        }
    })
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
