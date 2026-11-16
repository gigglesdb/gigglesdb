//! PostgreSQL numeric token spelling, normalized before expression planning.
use crate::{Error, Numeric};

fn digit(b: u8, radix: u32) -> bool {
    (b as char).is_digit(radix)
}

// Separators belong between digits, except immediately after a base prefix.
fn digits(bytes: &[u8], pos: &mut usize, radix: u32, prefix: bool) -> Result<usize, Error> {
    let start = *pos;
    if prefix && bytes.get(*pos) == Some(&b'_') {
        *pos += 1;
    }
    let mut count = 0;
    while bytes.get(*pos).is_some_and(|b| digit(*b, radix)) {
        count += 1;
        *pos += 1;
        if bytes.get(*pos) == Some(&b'_') {
            *pos += 1;
            if !bytes.get(*pos).is_some_and(|b| digit(*b, radix)) {
                return Err(Error::Syntax);
            }
        }
    }
    if count == 0 && *pos != start {
        return Err(Error::Syntax);
    }
    Ok(count)
}

/// Scan a PostgreSQL numeric token starting at a UTF-8 byte offset.
///
/// Returns its canonical decimal spelling and the first byte after the token.
/// Signs are separate SQL tokens. Invalid spelling or trailing identifier junk
/// returns `Error::Syntax`; nondecimal conversion retains `Numeric`'s limits.
pub fn numeric_literal(text: &str, start: usize) -> Result<(String, usize), Error> {
    let bytes = text.as_bytes();
    let mut pos = start;
    let radix = if bytes.get(pos) == Some(&b'0') {
        match bytes.get(pos + 1) {
            Some(b'x' | b'X') => 16,
            Some(b'o' | b'O') => 8,
            Some(b'b' | b'B') => 2,
            _ => 10,
        }
    } else {
        10
    };
    if radix != 10 {
        pos += 2;
        if digits(bytes, &mut pos, radix, true)? == 0 {
            return Err(Error::Syntax);
        }
    } else {
        let mut count = digits(bytes, &mut pos, 10, false)?;
        if bytes.get(pos) == Some(&b'.') && bytes.get(pos + 1) != Some(&b'.') {
            pos += 1;
            count += digits(bytes, &mut pos, 10, false)?;
        }
        if count == 0 {
            return Err(Error::Syntax);
        }
        if matches!(bytes.get(pos), Some(b'e' | b'E')) {
            pos += 1;
            if matches!(bytes.get(pos), Some(b'+' | b'-')) {
                pos += 1;
            }
            if digits(bytes, &mut pos, 10, false)? == 0 {
                return Err(Error::Syntax);
            }
        }
    }
    if bytes
        .get(pos)
        .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'$' || *b >= 0x80)
    {
        return Err(Error::Syntax);
    }
    let number = if radix != 10 {
        // Reuse the arbitrary-precision PostgreSQL input parser and its bounds.
        Numeric::parse(&text[start..pos])?.to_string()
    } else {
        text[start..pos].replace('_', "")
    };
    Ok((number, pos))
}
