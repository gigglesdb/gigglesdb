//! Catalog kind 21: untrusted-text column labels (GD-0061 R3-307, design-3 "Storage").
//!
//! Key: `21 | table_id u64 BE | column_id u64 BE`. Value: `01 | flags u8`, bit 0 =
//! untrusted_text. Every label key, live or tombstoned, requires TASK_CATALOG.
use super::meta::TASK_CATALOG;
use crate::{CoreError, relational::TableDef};
use std::collections::BTreeMap;

pub(crate) const LABEL_KIND: u8 = 21;
pub(crate) const LABEL_VERSION: u8 = 1;
/// Flag bit 0 of a label value.
pub(crate) const UNTRUSTED_TEXT: u8 = 1;
const KNOWN_FLAGS: u8 = UNTRUSTED_TEXT;

fn corrupt(message: &str) -> CoreError {
    CoreError::Corrupt(message.into())
}

pub(crate) fn label_key(table: u64, column: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(LABEL_KIND);
    key.extend(table.to_be_bytes());
    key.extend(column.to_be_bytes());
    key
}

pub(crate) fn decode_label_key(key: &[u8]) -> Result<(u64, u64), CoreError> {
    if key.len() != 17 || key[0] != LABEL_KIND {
        return Err(corrupt("column label key"));
    }
    let table = u64::from_be_bytes(key[1..9].try_into().unwrap());
    let column = u64::from_be_bytes(key[9..17].try_into().unwrap());
    if table == 0 {
        return Err(corrupt("column label table id"));
    }
    Ok((table, column))
}

pub(crate) fn encode_label(flags: u8) -> Result<Vec<u8>, CoreError> {
    if flags & !KNOWN_FLAGS != 0 || flags == 0 {
        return Err(CoreError::Unsupported);
    }
    Ok(vec![LABEL_VERSION, flags])
}

pub(crate) fn decode_label(value: &[u8]) -> Result<u8, CoreError> {
    match value {
        [LABEL_VERSION, flags] if flags & !KNOWN_FLAGS == 0 && *flags != 0 => Ok(*flags),
        [LABEL_VERSION, _] => Err(corrupt("column label flags")),
        [LABEL_VERSION, ..] => Err(corrupt("column label length")),
        _ => Err(corrupt("column label version")),
    }
}

/// Feature bits a label key needs at commit and open (content check).
pub(crate) fn required_bits(key: &[u8]) -> Result<u64, CoreError> {
    decode_label_key(key)?;
    Ok(TASK_CATALOG)
}

/// Every label names an existing column of an existing table (not a view,
/// never the table itself).
pub(crate) fn validate_labels<'a>(
    labels: &BTreeMap<(u64, u64), u8>,
    tables: impl Iterator<Item = &'a TableDef>,
) -> Result<(), CoreError> {
    if labels.is_empty() {
        return Ok(());
    }
    let tables: BTreeMap<_, _> = tables.map(|table| (table.id, table)).collect();
    for (&(table, column), &flags) in labels {
        if flags == 0 || flags & !KNOWN_FLAGS != 0 {
            return Err(corrupt("column label flags"));
        }
        if !tables
            .get(&table)
            .is_some_and(|t| t.columns.iter().any(|c| c.id == column))
        {
            return Err(corrupt("column label column absent"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let key = label_key(7, 3);
        assert_eq!(key[0], LABEL_KIND);
        assert_eq!(decode_label_key(&key).unwrap(), (7, 3));
        let value = encode_label(UNTRUSTED_TEXT).unwrap();
        assert_eq!(value, [1, 1]);
        assert_eq!(decode_label(&value).unwrap(), UNTRUSTED_TEXT);
        assert!(decode_label_key(&key[..16]).is_err());
        assert!(decode_label_key(&label_key(0, 3)).is_err());
    }

    #[test]
    fn unknown_flag_bits_are_corrupt() {
        for flags in [0u8, 2, 0x80, 0xFF] {
            assert!(matches!(decode_label(&[1, flags]), Err(CoreError::Corrupt(_))), "{flags}");
        }
        assert!(matches!(decode_label(&[2, 1]), Err(CoreError::Corrupt(_))));
        assert!(matches!(decode_label(&[1, 1, 0]), Err(CoreError::Corrupt(_))));
        assert!(matches!(decode_label(&[]), Err(CoreError::Corrupt(_))));
        assert!(encode_label(2).is_err());
    }

    #[test]
    fn kind_requires_task_catalog_bit() {
        assert_eq!(required_bits(&label_key(7, 3)).unwrap(), TASK_CATALOG);
        assert!(required_bits(&[LABEL_KIND]).is_err());
    }
}
