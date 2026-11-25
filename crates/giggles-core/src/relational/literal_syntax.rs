//! PostgreSQL string prefixes all produce the ordinary unknown string token.
use super::*;

pub(super) fn prefixed(sql: &str, start: usize) -> R<Option<(String, usize)>> {
    let b = sql.as_bytes();
    if b[start] == b'$' && !b.get(start + 1).is_some_and(u8::is_ascii_digit) {
        let mut end = start + 1;
        if b.get(end) != Some(&b'$') {
            if !b
                .get(end)
                .is_some_and(|c| c.is_ascii_alphabetic() || *c == b'_' || *c >= 128)
            {
                return Ok(None);
            }
            while b
                .get(end)
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c >= 128)
            {
                end += 1;
            }
        }
        if b.get(end) != Some(&b'$') {
            return Err(SqlError::Syntax);
        }
        let delimiter = &sql[start..=end];
        let body = end + 1;
        let close = sql[body..].find(delimiter).ok_or(SqlError::Syntax)? + body;
        if close - body > crate::relational::max_value_bytes() {
            return Err(SqlError::Limit);
        }
        return Ok(Some((sql[body..close].into(), close + delimiter.len())));
    }
    let escape = b[start].eq_ignore_ascii_case(&b'e') && b.get(start + 1) == Some(&b'\'');
    let unicode =
        b[start].eq_ignore_ascii_case(&b'u') && b.get(start + 1..start + 3) == Some(b"&'");
    if !escape && !unicode {
        return Ok(None);
    }
    let mut i = start + if escape { 2 } else { 3 };
    let mut raw = vec![];
    loop {
        let c = *b.get(i).ok_or(SqlError::Syntax)?;
        i += 1;
        if c == b'\'' {
            if b.get(i) == Some(&b'\'') {
                raw.push(c);
                i += 1;
            } else {
                break;
            }
        } else {
            raw.push(c);
            if escape && c == b'\\' {
                raw.push(*b.get(i).ok_or(SqlError::Syntax)?);
                i += 1;
            }
        }
    }
    let mut marker = '\\';
    if unicode {
        let mut j = i;
        while b.get(j).is_some_and(u8::is_ascii_whitespace) {
            j += 1;
        }
        if b.get(j..j + 7)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"uescape"))
            && !b
                .get(j + 7)
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
        {
            j += 7;
            while b.get(j).is_some_and(u8::is_ascii_whitespace) {
                j += 1;
            }
            if b.get(j) != Some(&b'\'') {
                return Err(SqlError::Syntax);
            }
            j += 1;
            let ch = sql[j..].chars().next().ok_or(SqlError::Syntax)?;
            j += ch.len_utf8();
            if b.get(j) != Some(&b'\'')
                || ch.is_ascii_hexdigit()
                || ch.is_whitespace()
                || ['+', '\'', '"'].contains(&ch)
            {
                return Err(SqlError::Syntax);
            }
            marker = ch;
            i = j + 1;
        }
    }
    let value = if escape {
        decode_escape(&raw)?
    } else {
        decode_unicode(&raw, marker)?
    };
    if value.len() > crate::relational::max_value_bytes() {
        return Err(SqlError::Limit);
    }
    Ok(Some((value, i)))
}

fn hex(bytes: &[u8]) -> Option<u32> {
    bytes
        .iter()
        .try_fold(0u32, |n, b| Some(n * 16 + (*b as char).to_digit(16)?))
}
fn character(n: u32, pending: &mut Option<u32>, out: &mut Vec<u8>) -> R<()> {
    if (0xD800..=0xDBFF).contains(&n) && pending.is_none() {
        *pending = Some(n);
        return Ok(());
    }
    let n = if let Some(high) = pending.take() {
        if !(0xDC00..=0xDFFF).contains(&n) {
            return Err(SqlError::Syntax);
        }
        0x10000 + ((high - 0xD800) << 10) + n - 0xDC00
    } else {
        n
    };
    let ch = char::from_u32(n)
        .filter(|c| *c != '\0')
        .ok_or(SqlError::Syntax)?;
    append(out, ch.encode_utf8(&mut [0; 4]).as_bytes())?;
    Ok(())
}
fn finish(out: Vec<u8>, pending: Option<u32>) -> R<String> {
    if pending.is_some() {
        return Err(SqlError::Syntax);
    }
    if out.contains(&0) {
        return Err(SqlError::CharacterNotInRepertoire);
    }
    String::from_utf8(out).map_err(|_| SqlError::CharacterNotInRepertoire)
}
fn decode_escape(raw: &[u8]) -> R<String> {
    let mut out = vec![];
    let mut pending = None;
    let mut i = 0;
    while i < raw.len() {
        let c = raw[i];
        i += 1;
        if c != b'\\' {
            if pending.is_some() {
                return Err(SqlError::Syntax);
            }
            append(&mut out, &[c])?;
            continue;
        }
        let c = *raw.get(i).ok_or(SqlError::Syntax)?;
        i += 1;
        if c == b'u' || c == b'U' {
            let n = if c == b'u' { 4 } else { 8 };
            let value = raw
                .get(i..i + n)
                .and_then(hex)
                .ok_or(SqlError::InvalidEscapeSequence)?;
            i += n;
            character(value, &mut pending, &mut out)?;
        } else {
            if pending.is_some() {
                return Err(SqlError::Syntax);
            }
            match c {
                b'0'..=b'7' => {
                    let mut n = (c - b'0') as u16;
                    for _ in 0..2 {
                        if let Some(c @ b'0'..=b'7') = raw.get(i) {
                            n = n * 8 + (c - b'0') as u16;
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    append(&mut out, &[n as u8])?;
                }
                b'x' => {
                    let start = i;
                    while i < start + 2 && raw.get(i).is_some_and(u8::is_ascii_hexdigit) {
                        i += 1;
                    }
                    if i == start {
                        append(&mut out, b"x")?;
                    } else {
                        append(&mut out, &[hex(&raw[start..i]).unwrap() as u8])?;
                    }
                }
                b'n' => append(&mut out, b"\n")?,
                b't' => append(&mut out, b"\t")?,
                b'r' => append(&mut out, b"\r")?,
                b'b' => append(&mut out, &[8])?,
                b'v' => append(&mut out, &[11])?,
                b'f' => append(&mut out, &[12])?,
                c => append(&mut out, &[c])?,
            }
        }
    }
    finish(out, pending)
}
fn decode_unicode(raw: &[u8], marker: char) -> R<String> {
    let text = std::str::from_utf8(raw).map_err(|_| SqlError::Syntax)?;
    let mut out = vec![];
    let mut pending = None;
    let mut i = 0;
    while i < text.len() {
        let c = text[i..].chars().next().unwrap();
        i += c.len_utf8();
        if c != marker {
            if pending.is_some() {
                return Err(SqlError::Syntax);
            }
            append(&mut out, c.encode_utf8(&mut [0; 4]).as_bytes())?;
            continue;
        }
        if text[i..].starts_with(marker) {
            if pending.is_some() {
                return Err(SqlError::Syntax);
            }
            append(&mut out, marker.encode_utf8(&mut [0; 4]).as_bytes())?;
            i += marker.len_utf8();
            continue;
        }
        let n = if raw.get(i) == Some(&b'+') {
            i += 1;
            6
        } else {
            4
        };
        let value = raw.get(i..i + n).and_then(hex).ok_or(SqlError::Syntax)?;
        i += n;
        character(value, &mut pending, &mut out)?;
    }
    finish(out, pending)
}


fn append(out: &mut Vec<u8>, bytes: &[u8]) -> R<()> {
    if out.len().checked_add(bytes.len()).is_none_or(|n| n > crate::relational::max_value_bytes()) { return Err(SqlError::Limit); }
    out.extend_from_slice(bytes);
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prefixes_are_unknown_strings() {
        for (sql, value) in [
            (r"E'a\'b'", "a'b"),
            (r"E'it''s\tvalue\\ok'", "it's\tvalue\\ok"),
            (r"E'\101\x42\u00e9\U0001f600'", "ABé😀"),
            (r"U&'caf\00e9'", "café"),
            (r"U&'!D83D!DE00!!' UESCAPE '!'", "😀!"),
            ("$tag$it's $$ fine$tag$", "it's $$ fine"),
            ("$$it's a label$$", "it's a label"),
        ] {
            assert_eq!(lex(sql), Ok(vec![Token::String(value.into())]), "{sql}");
        }
    }
    #[test]
    fn invalid_escapes_match_pg18_sqlstates() {
        for (sql, error) in [
            (r"E'\000'", SqlError::CharacterNotInRepertoire),
            (r"E'\x00'", SqlError::CharacterNotInRepertoire),
            (r"E'\400'", SqlError::CharacterNotInRepertoire),
            (r"E'\377'", SqlError::CharacterNotInRepertoire),
            (r"E'\uZZZZ'", SqlError::InvalidEscapeSequence),
            (r"E'\u12'", SqlError::InvalidEscapeSequence),
            (r"E'\uD800'", SqlError::Syntax),
            (r"E'\u0000'", SqlError::Syntax),
            (r"E'\xFF'", SqlError::CharacterNotInRepertoire),
            (r"U&'\12'", SqlError::Syntax),
            (r"U&'\D800'", SqlError::Syntax),
            (r"U&'\0000'", SqlError::Syntax),
            (r"U&'x' UESCAPE '0'", SqlError::Syntax),
            (r"U&'\+110000'", SqlError::Syntax),
            (r"E'\U00110000'", SqlError::Syntax),
        ] {
            assert_eq!(lex(sql), Err(error), "{sql}");
        }
        assert_eq!(lex(r"E'\q'"), Ok(vec![Token::String("q".into())]));
    }
}
