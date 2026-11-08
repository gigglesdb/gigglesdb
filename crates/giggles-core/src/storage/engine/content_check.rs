//! Open-time witnesses for the monotone catalog and counter feature bits.
use super::{
    catalog_codec::{self, CatalogKind},
    codec,
    kv::{Direction, Keyspace, KvRead},
    meta::FormatError,
    mvcc,
};
use std::{collections::BTreeMap, ops::Bound};

fn corrupt(error: impl std::fmt::Display) -> FormatError {
    FormatError::Corrupt(error.to_string())
}

pub(crate) fn check(kv: &dyn KvRead, features: u64) -> Result<(), FormatError> {
    let mut previous = None;
    let mut tables = BTreeMap::new();
    let mut sequences = BTreeMap::new();
    let mut constraints = Vec::new();
    let mut required = 0;
    for item in kv
        .range(
            Keyspace::Catalog,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
        )
        .map_err(corrupt)?
    {
        let (key, value) = item.map_err(corrupt)?;
        let (user, _) = codec::split_versioned(&key)
            .map_err(|error| FormatError::Corrupt(format!("{error:?}")))?;
        // Classify even historical/tombstoned keys: unknown kinds cannot hide
        // behind a tombstone or behind a newer, valid physical version.
        let kind = catalog_codec::classify_catalog_key(user).map_err(corrupt)?;
        if previous.as_deref() == Some(user) {
            continue;
        }
        previous = Some(user.to_vec());
        let Some(value) = mvcc::decode(&value).map_err(corrupt)? else {
            continue;
        };
        match kind {
            CatalogKind::Table => {
                let (entry, ext) = catalog_codec::decode_table(value).map_err(corrupt)?;
                if !ext.is_empty() {
                    required |= 64;
                }
                if ext.values().any(|column| column.typmod.is_some()) {
                    required |= 128;
                }
                tables.insert(
                    u64::from_be_bytes(user[1..9].try_into().unwrap()),
                    entry.def,
                );
            }
            CatalogKind::Name => {
                let _: [u8; 8] = value
                    .try_into()
                    .map_err(|_| corrupt("invalid catalog name id"))?;
            }
            CatalogKind::Constraint => {
                required |= 64;
                constraints.push((
                    u64::from_be_bytes(user[1..9].try_into().unwrap()),
                    value.to_vec(),
                ));
            }
            CatalogKind::Sequence => {
                required |= 64;
                sequences.insert(
                    u64::from_be_bytes(user[1..9].try_into().unwrap()),
                    catalog_codec::decode_sequence(value).map_err(corrupt)?,
                );
            }
            CatalogKind::Type => {
                required |= 64;
                catalog_codec::decode_type(value)
                    .map(|_| ())
                    .map_err(corrupt)?;
            }
            // The role-bootstrap gate and its existing opaque payload fixture
            // are owned by validate_meta.
            CatalogKind::RoleBootstrap => {}
        }
    }
    for (id, value) in constraints {
        let table = tables
            .get(&id)
            .ok_or_else(|| corrupt("constraint table is absent"))?;
        catalog_codec::decode_constraint(&value, table).map_err(corrupt)?;
    }
    for item in kv
        .range(
            Keyspace::Seq,
            Bound::Included(&[2]),
            Bound::Excluded(&[3]),
            Direction::Forward,
        )
        .map_err(corrupt)?
    {
        let (key, value) = item.map_err(corrupt)?;
        if !super::commit::valid_counter_key(&key) {
            return Err(corrupt("invalid sequence counter key"));
        }
        required |= 256;
        let id = u64::from_be_bytes(key[1..9].try_into().unwrap());
        let definition = sequences
            .get(&id)
            .ok_or_else(|| corrupt("counter sequence is absent"))?;
        catalog_codec::decode_counter(&key, &value, definition).map_err(corrupt)?;
    }
    if required & !features != 0 {
        return Err(corrupt("content requires unset format feature"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "content_check_tests.rs"]
mod tests;
