// Character classes based on PostgreSQL 18.6 tsearch/ts_locale.c.
// Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group.
#[path = "locale_ranges.rs"]
mod ranges;

fn lookup(c: char) -> (u8, i32) {
    let cp = u32::from(c);
    let index = ranges::RANGES.partition_point(|range| range.1 < cp);
    ranges::RANGES
        .get(index)
        .filter(|range| range.0 <= cp)
        .map_or((0, 0), |range| (range.2, range.3))
}

pub(crate) fn alpha(c: char) -> bool {
    c.is_ascii_alphabetic() || lookup(c).0 == 1
}

pub(crate) fn special(c: char) -> bool {
    !c.is_ascii() && lookup(c).0 == 2
}

pub(crate) fn ascii_space(c: char) -> bool {
    matches!(c, ' ' | '\t'..='\r')
}

pub(crate) fn space(c: char) -> bool {
    ascii_space(c)
        || matches!(c, '\u{1680}' | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}'
            | '\u{2028}' | '\u{2029}' | '\u{205f}' | '\u{3000}')
}

pub(crate) fn lower(s: &str) -> String {
    if s.is_ascii() {
        return s.to_ascii_lowercase();
    }
    s.chars()
        .map(|c| {
            if c.is_ascii() {
                c.to_ascii_lowercase()
            } else {
                char::from_u32(u32::from(c).wrapping_add_signed(lookup(c).1)).unwrap_or(c)
            }
        })
        .collect()
}
