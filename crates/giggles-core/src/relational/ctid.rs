//! Heap row identity text encoding. Offsets are one-based PostgreSQL offsets.
use super::{R, SqlError};

pub(super) fn format(rowid: i64) -> R<String> {
    if !(0..65535_i64 * (1_i64 << 32)).contains(&rowid) {
        return Err(SqlError::Overflow);
    }
    Ok(format!("({},{})", rowid / 65535, rowid % 65535 + 1))
}

#[cfg_attr(not(test), allow(dead_code))] // Row-targeting integration is pending.
pub(super) fn parse(text: &str) -> R<i64> {
    let invalid = || SqlError::InvalidTextRepresentation;
    let inner = text
        .trim()
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(invalid)?;
    let (block, offset) = inner.split_once(',').ok_or_else(invalid)?;
    let number = |s: &str| -> R<u64> {
        let s = s.trim();
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid());
        }
        s.parse().map_err(|_| invalid())
    };
    let block = number(block)?;
    let offset = number(offset)?;
    if block > u32::MAX as u64 || !(1..=65535).contains(&offset) {
        return Err(invalid());
    }
    Ok((block * 65535 + offset - 1) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_trip_boundaries() {
        for value in [0, 65534, 65535, 65535_i64 * (1_i64 << 32) - 1] {
            assert_eq!(parse(&format(value).unwrap()).unwrap(), value);
        }
        assert_eq!(parse("( 0 , 1 )").unwrap(), 0);
        assert!(format(-1).is_err());
        assert!(format(65535_i64 * (1_i64 << 32)).is_err());
    }
    #[test]
    fn rejects_invalid_tids() {
        for value in [
            "",
            "0,1",
            "(0,0)",
            "(0,65536)",
            "(4294967296,1)",
            "(-1,1)",
            "(0,1,2)",
        ] {
            assert!(matches!(
                parse(value),
                Err(SqlError::InvalidTextRepresentation)
            ));
        }
    }
}
