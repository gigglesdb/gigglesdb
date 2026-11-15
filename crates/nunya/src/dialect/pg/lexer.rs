//! Conservative SQL literal boundaries used by input rewriting.
use crate::ir::Span;

/// Find a supported literal's exact byte extent and decoded text.
pub fn literal_extent(sql: &str, start: usize) -> Option<(Span, String)> {
    let tail = sql.get(start..)?;
    let bytes = tail.as_bytes();
    if bytes.first() == Some(&b'\'') {
        let mut i = 1;
        let mut text = String::new();
        let mut segment = i;
        loop {
            match bytes.get(i)? {
                b'\'' => {
                    text.push_str(tail.get(segment..i)?);
                    if bytes.get(i + 1) == Some(&b'\'') {
                        text.push('\'');
                        i += 2;
                        segment = i;
                    } else {
                        i += 1;
                        let rest = tail.get(i..)?;
                        // PostgreSQL joins strings separated by whitespace containing a newline.
                        let ws = rest.len() - rest.trim_start().len();
                        if rest.get(..ws)?.contains(['\n', '\r'])
                            && rest.get(ws..)?.starts_with('\'')
                        {
                            return None;
                        }
                        // Comments between string segments are deliberately unsupported.
                        if rest.trim_start().starts_with("/*")
                            || rest.trim_start().starts_with("--")
                        {
                            return None;
                        }
                        return Some((
                            Span {
                                start,
                                end: start + i,
                            },
                            text,
                        ));
                    }
                }
                b'\\' => return None, // independent of standard_conforming_strings
                _ => i += 1,
            }
        }
    }
    if bytes.first() == Some(&b'$') {
        let end = tail.get(1..)?.find('$')? + 1;
        let tag = tail.get(1..end)?;
        if !tag.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
            || tag.as_bytes().first().is_some_and(u8::is_ascii_digit)
        {
            return None;
        }
        let delimiter = tail.get(..=end)?;
        let body = tail.get(end + 1..)?;
        let close = body.find(delimiter)?;
        return Some((
            Span {
                start,
                end: start + end + 1 + close + delimiter.len(),
            },
            body.get(..close)?.into(),
        ));
    }
    let mut i = 0;
    if matches!(bytes.first(), Some(b'+') | Some(b'-')) {
        i += 1;
    }
    let mut digits = 0;
    while bytes.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
        digits += 1;
    }
    if bytes.get(i) == Some(&b'.') {
        i += 1;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return None;
    }
    if matches!(bytes.get(i), Some(b'e') | Some(b'E')) {
        i += 1;
        if matches!(bytes.get(i), Some(b'+') | Some(b'-')) {
            i += 1;
        }
        let begin = i;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if begin == i {
            return None;
        }
    }
    if bytes
        .get(i)
        .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
    {
        return None;
    }
    Some((
        Span {
            start,
            end: start + i,
        },
        tail.get(..i)?.into(),
    ))
}
