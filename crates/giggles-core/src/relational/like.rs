//! Bounded Unicode LIKE matching; no recursion or locale-dependent folding.
use super::{MAX_RESULT_BYTES, R, SqlError};

#[derive(Clone, Copy)]
pub(super) struct Escape(Option<char>);
impl Escape {
    pub(super) fn parse(value: &str) -> R<Self> {
        let mut chars = value.chars();
        let first = chars.next();
        if chars.next().is_some() {
            return Err(SqlError::InvalidEscapeSequence);
        }
        Ok(Self(first))
    }
}
fn charge(work: &mut usize, amount: usize) -> R<()> {
    let total = work.checked_add(amount).ok_or(SqlError::Limit)?;
    if total > MAX_RESULT_BYTES {
        return Err(SqlError::Limit);
    }
    *work = total;
    Ok(())
}
// Preserve the one-character case folding used by the legacy ILIKE matcher.
pub(super) fn legacy_fold(c: char) -> char {
    let mut lower = c.to_lowercase();
    let first = lower.next().unwrap_or(c);
    if lower.next().is_none() { first } else { c }
}

pub(super) fn like(
    value: &str,
    pattern: &str,
    escape: Escape,
    fold: impl Fn(char) -> char,
    work: &mut usize,
) -> R<bool> {
    // Account for scanning before allocating character vectors. The same work
    // counter also bounds scalar evaluation's accumulated result bytes.
    charge(work, value.len())?;
    charge(work, pattern.len())?;
    let value: Vec<char> = value.chars().map(&fold).collect();
    let pattern: Vec<char> = pattern.chars().collect();
    let (mut v, mut p) = (0, 0);
    let mut star = None;
    while v < value.len() {
        charge(work, 1)?;
        let current = pattern.get(p).copied();
        let (literal, width) = if current.is_some() && current == escape.0 {
            // At ordinary literal positions, exhausted input does not inspect
            // an incomplete escape at the pattern's end.
            let next = pattern
                .get(p + 1)
                .copied()
                .ok_or(SqlError::InvalidEscapeSequence)?;
            (Some(fold(next)), 2)
        } else if current == Some('%') {
            // A wildcard run reserves its mandatory characters before choosing
            // a retry point. Inspect its following literal even if those
            // mandatory characters have just exhausted the input.
            p += 1;
            while let Some(c) = pattern.get(p).copied() {
                if escape.0 == Some(c) || !matches!(c, '%' | '_') {
                    break;
                }
                charge(work, 1)?;
                if c == '_' {
                    if v == value.len() {
                        return Ok(false);
                    }
                    v += 1;
                }
                p += 1;
            }
            if p == pattern.len() {
                return Ok(true);
            }
            if escape.0 == pattern.get(p).copied() && p + 1 == pattern.len() {
                return Err(SqlError::InvalidEscapeSequence);
            }
            star = Some((p, v));
            continue;
        } else if current == Some('_') {
            v += 1;
            p += 1;
            continue;
        } else {
            (current.map(&fold), 1)
        };
        if literal == Some(value[v]) {
            v += 1;
            p += width;
        } else {
            match star {
                Some((at, matched)) if matched < value.len() => {
                    v = matched + 1;
                    p = at;
                    star = Some((at, v));
                }
                _ => return Ok(false),
            }
        }
    }
    while pattern.get(p) == Some(&'%') && escape.0 != Some('%') {
        charge(work, 1)?;
        p += 1;
    }
    Ok(p == pattern.len())
}

/// PG18 nondeterministic LIKE: wildcards consume characters, literal runs
/// compare whole substrings under the collation (including ignorable chars).
pub(super) fn collated(value:&str,pattern:&str,escape:Escape,work:&mut usize,eq:impl Fn(&str,&str)->bool)->R<bool> {
    enum Part { Literal(String), One, Many }
    let mut parts=Vec::new(); let mut literal=String::new(); let mut chars=pattern.chars();
    while let Some(c)=chars.next() {
        if Some(c)==escape.0 {literal.push(chars.next().ok_or(SqlError::InvalidEscapeSequence)?);}
        else if matches!(c,'_'|'%') {
            if !literal.is_empty() {parts.push(Part::Literal(std::mem::take(&mut literal)));}
            parts.push(if c=='_' {Part::One} else {Part::Many});
        } else {literal.push(c);}
    }
    if !literal.is_empty() {parts.push(Part::Literal(literal));}
    let mut offsets:Vec<_>=value.char_indices().map(|(i,_)|i).collect(); offsets.push(value.len());
    charge(work,offsets.len().saturating_mul(2))?;
    let mut previous=vec![false;offsets.len()];previous[0]=true;
    for part in parts {
        let mut next=vec![false;offsets.len()];
        match part {
            Part::Many=>{let mut seen=false;for (n,p) in next.iter_mut().zip(&previous) {seen|=*p;*n=seen;}},
            Part::One=>next[1..].copy_from_slice(&previous[..previous.len()-1]),
            Part::Literal(p)=>for (start,matched) in previous.iter().enumerate() {if *matched {for end in start..offsets.len() {
                charge(work,1+offsets[end]-offsets[start])?;
                if eq(&value[offsets[start]..offsets[end]],&p) {next[end]=true;}
            }}},
        }
        previous=next;
    }
    Ok(previous.last().copied().unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn matches(value: &str, pattern: &str, escape: &str, insensitive: bool) -> R<bool> {
        like(value, pattern, Escape::parse(escape)?, |c| {
            if insensitive { legacy_fold(c) } else { c }
        }, &mut 0)
    }
    #[test]
    fn unicode_wildcards_and_escapes() {
        for (value, pattern, expected) in [
            ("", "%", true),
            ("\u{e9}\u{1f980}", "__", true),
            ("\u{e9}", "__", false),
            ("a%b", r"a\%b", true),
            ("abc", "%b%", true),
            ("abc", "a%d", false),
            ("abc", "%a%c", true),
        ] {
            assert_eq!(matches(value, pattern, "\\", false), Ok(expected));
        }
        assert_eq!(matches("a%b", "a!%b", "!", false), Ok(true));
        assert_eq!(matches("abc\\", "abc\\", "", false), Ok(true));
        // GigglesDB ILIKE uses locale-independent one-to-one folding.
        assert_eq!(matches("ABC\u{c4}", "abc\u{e4}", "", true), Ok(true));
        assert_eq!(matches("", "\\", "\\", false), Ok(false));
        assert!(matches!(
            Escape::parse("ab"),
            Err(SqlError::InvalidEscapeSequence)
        ));
        assert_eq!(matches("x", "!x", "!", false), Ok(true));
    }
    #[test]
    fn trailing_escape_is_checked_only_when_reached() {
        for (value, pattern) in [
            ("", "\\"),
            ("abc", "abc\\"),
            ("a", "a\\"),
            ("xyz", "a\\"),
            ("", "%\\"),
            ("ab", "x%\\"),
        ] {
            assert_eq!(
                matches(value, pattern, "\\", false),
                Ok(false),
                "{value:?} {pattern:?}"
            );
        }
        for (value, pattern) in [
            ("abc", "ab\\"),
            ("abc", "%\\"),
            ("ab", "a%\\"),
            ("a", "%_\\"),
        ] {
            assert_eq!(
                matches(value, pattern, "\\", false),
                Err(SqlError::InvalidEscapeSequence),
                "{value:?} {pattern:?}"
            );
        }
    }

    #[test]
    fn exhaustive_pg18_default_escape_oracle() {
        let mut counts = [0; 3];
        for line in include_str!("../../tests/fixtures/like_pg18.tsv")
            .lines()
            .filter(|line| !line.starts_with('#'))
        {
            let fields: Vec<_> = line.split('\t').collect();
            assert_eq!(fields.len(), 3);
            let value = fields[0].replace("\\\\", "\\");
            let pattern = fields[1].replace("\\\\", "\\");
            let expected = match fields[2] {
                "true" => {
                    counts[0] += 1;
                    Ok(true)
                }
                "false" => {
                    counts[1] += 1;
                    Ok(false)
                }
                "22025" => {
                    counts[2] += 1;
                    Err(SqlError::InvalidEscapeSequence)
                }
                other => panic!("unexpected oracle result {other}"),
            };
            assert_eq!(
                matches(&value, &pattern, "\\", false),
                expected,
                "value={value:?} pattern={pattern:?}"
            );
        }
        assert_eq!(counts, [460, 4867, 140]);
    }

    #[test]
    fn pg18_custom_escape_and_ascii_ilike_samples() {
        for (value, pattern, escape, insensitive, expected) in [
            ("a%b", "a!%b", "!", false, Ok(true)),
            ("a_b", "a!_b", "!", false, Ok(true)),
            ("a!b", "a!!b", "!", false, Ok(true)),
            ("", "!", "!", false, Ok(false)),
            ("ab", "a!", "!", false, Err(SqlError::InvalidEscapeSequence)),
            (
                "abc",
                "ab!",
                "!",
                false,
                Err(SqlError::InvalidEscapeSequence),
            ),
            (
                "abc",
                "%!",
                "!",
                false,
                Err(SqlError::InvalidEscapeSequence),
            ),
            ("ab", "x%!", "!", false, Ok(false)),
            ("a\\xyz", "a\\%", "", false, Ok(true)),
            ("abc", "a\\%", "", false, Ok(false)),
            ("abc\\", "abc\\", "", false, Ok(true)),
            ("", "\\", "", false, Ok(false)),
            ("Alpha", "al%", "\\", true, Ok(true)),
            ("BETA", "be_a", "\\", true, Ok(true)),
            ("abc", "A_D", "\\", true, Ok(false)),
            (
                "ABC",
                "ab\\",
                "\\",
                true,
                Err(SqlError::InvalidEscapeSequence),
            ),
            (
                "ABC",
                "AB\\",
                "\\",
                true,
                Err(SqlError::InvalidEscapeSequence),
            ),
            (
                "ABC",
                "%\\",
                "\\",
                true,
                Err(SqlError::InvalidEscapeSequence),
            ),
        ] {
            assert_eq!(
                matches(value, pattern, escape, insensitive),
                expected,
                "value={value:?} pattern={pattern:?} escape={escape:?} insensitive={insensitive}"
            );
        }
    }

    #[test]
    fn adversarial_is_bounded() {
        let mut work = 0;
        let result = like(
            &"a".repeat(2000),
            "%a%a%a%a%a%b",
            Escape(None),
            std::convert::identity,
            &mut work,
        );
        assert!(matches!(result, Ok(false) | Err(SqlError::Limit)));
        assert!(work <= MAX_RESULT_BYTES);
        let mut work = MAX_RESULT_BYTES;
        assert_eq!(
            like("a", "a", Escape(None), std::convert::identity, &mut work),
            Err(SqlError::Limit)
        );
    }
}
