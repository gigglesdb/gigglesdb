//! Default-backslash PostgreSQL Unicode string literals.
use super::{R, SqlError};
pub(super) fn decode(input: &str) -> R<String> {
    let mut chars = input.chars();
    let mut units = Vec::new();
    while let Some(c) = chars.next() {
        if c != '\\' {
            units.extend(c.encode_utf16(&mut [0; 2]).iter().copied());
            continue;
        }
        let first = chars.next().ok_or(SqlError::InvalidEscapeSequence)?;
        if first == '\\' {
            units.push(u16::from(b'\\'));
            continue;
        }
        let (mut value, remaining) = if first == '+' {
            (0u32, 6)
        } else {
            (
                first.to_digit(16).ok_or(SqlError::InvalidEscapeSequence)?,
                3,
            )
        };
        for _ in 0..remaining {
            value = value * 16
                + chars
                    .next()
                    .and_then(|c| c.to_digit(16))
                    .ok_or(SqlError::InvalidEscapeSequence)?;
        }
        if value == 0 {
            return Err(SqlError::CharacterNotInRepertoire);
        }
        if value <= 0xffff {
            units.push(value as u16);
        } else {
            units.extend(
                char::from_u32(value)
                    .ok_or(SqlError::InvalidEscapeSequence)?
                    .encode_utf16(&mut [0; 2])
                    .iter()
                    .copied(),
            );
        }
    }
    String::from_utf16(&units).map_err(|_| SqlError::InvalidEscapeSequence)
}
