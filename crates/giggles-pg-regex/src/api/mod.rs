//! PostgreSQL 18.6's SQL-facing regular expression functions.
//!
//! Pass decoded UTF-8 text and the SQL expression's [RegexpContext]. SQL text
//! excludes NUL. Positions are one-based Unicode scalar positions, including
//! the character after the end of the string when a match ends there.
//! An omitted flags argument is represented by None. The SQL caller handles
//! strict NULL propagation. [similar_escape] models its own NULL arguments
//! because the legacy SQL function is non-strict.
//!
//! Results borrow matched text from the input. A missing scalar match is None;
//! an unmatched capture is a None element in a [CaptureRow]. The set-returning
//! functions return their rows in match order. Compiled expressions are cached
//! in a per-thread 32-entry LRU keyed by pattern, compile flags and locale.

mod matching;
mod replace;
mod similar;

use std::fmt;

use crate::engine::{
    Chr, REG_ADVANCED, REG_EXPANDED, REG_EXTENDED, REG_ICASE, REG_NEWLINE, REG_NLANCH, REG_NLSTOP,
    REG_NOSUB, REG_QUOTE, RegErrorCode, RegMatch,
};

pub use crate::engine::{CancelCheck, RegexLocale};

/// Locale and optional cancellation hook for a SQL regular expression call.
pub struct RegexpContext<'a> {
    pub locale: RegexLocale,
    pub cancel: Option<CancelCheck<'a>>,
}

impl RegexpContext<'_> {
    /// Uses the supplied locale without a cancellation hook.
    pub const fn new(locale: RegexLocale) -> Self {
        Self {
            locale,
            cancel: None,
        }
    }
}

use matching::{Text, compile, execute, scan};

/// SQL error diagnostics from the PostgreSQL regex layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegexError {
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<&'static str>,
    pub hint: Option<&'static str>,
}

impl RegexError {
    fn new(sqlstate: &'static str, message: impl Into<String>) -> Self {
        Self {
            sqlstate,
            message: message.into(),
            detail: None,
            hint: None,
        }
    }

    fn with_hint(mut self, hint: &'static str) -> Self {
        self.hint = Some(hint);
        self
    }

    fn parameter(name: &str, value: i32) -> Self {
        Self::new(
            "22023",
            format!("invalid value for parameter \"{name}\": {value}"),
        )
    }

    fn flag(flag: char) -> Self {
        Self::new(
            "22023",
            format!("invalid regular expression option: \"{flag}\""),
        )
    }

    fn engine(context: &str, code: RegErrorCode) -> Self {
        if code == RegErrorCode::Cancel {
            Self::new("57014", "canceling statement due to user request")
        } else {
            Self::new("2201B", format!("{context}: {}", code.message()))
        }
    }
}

impl fmt::Display for RegexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RegexError {}

/// Parsed PostgreSQL option letters. The global option is separate from engine flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReFlags {
    pub cflags: i32,
    pub global: bool,
}

/// Parses the flags accepted by at least one of PostgreSQL's regex functions.
/// Individual functions may reject the global option after this step.
pub fn parse_re_flags(
    options: Option<&str>,
    _ctx: &RegexpContext<'_>,
) -> Result<ReFlags, RegexError> {
    let mut flags = ReFlags {
        cflags: REG_ADVANCED,
        global: false,
    };
    for ch in options.unwrap_or_default().chars() {
        match ch {
            'g' => flags.global = true,
            'b' => flags.cflags &= !(REG_ADVANCED | REG_EXTENDED | REG_QUOTE),
            'c' => flags.cflags &= !REG_ICASE,
            'e' => {
                // Preserve the order in regexp.c, including the shared
                // REG_EXTENDED bit that is cleared by the second operation.
                flags.cflags |= REG_EXTENDED;
                flags.cflags &= !(REG_ADVANCED | REG_QUOTE);
            }
            'i' => flags.cflags |= REG_ICASE,
            'm' | 'n' => flags.cflags |= REG_NEWLINE,
            'p' => flags.cflags = (flags.cflags | REG_NLSTOP) & !REG_NLANCH,
            'q' => flags.cflags = (flags.cflags | REG_QUOTE) & !(REG_ADVANCED | REG_EXTENDED),
            's' => flags.cflags &= !REG_NEWLINE,
            't' => flags.cflags &= !REG_EXPANDED,
            'w' => flags.cflags = (flags.cflags & !REG_NLSTOP) | REG_NLANCH,
            'x' => flags.cflags |= REG_EXPANDED,
            _ => return Err(RegexError::flag(ch)),
        }
    }
    Ok(flags)
}

fn nonglobal(
    options: Option<&str>,
    function: &str,
    ctx: &RegexpContext<'_>,
) -> Result<ReFlags, RegexError> {
    let flags = parse_re_flags(options, ctx)?;
    if flags.global {
        let error = RegexError::new(
            "22023",
            format!("{function}() does not support the \"global\" option"),
        );
        return Err(if function == "regexp_match" {
            error.with_hint("Use the regexp_matches function instead.")
        } else {
            error
        });
    }
    Ok(flags)
}

fn is_match(
    source: &str,
    pattern: &str,
    cflags: i32,
    ctx: &RegexpContext<'_>,
) -> Result<bool, RegexError> {
    let regex = compile(pattern, cflags | REG_NOSUB, ctx)?;
    let wide: Vec<Chr> = source.chars().map(Chr::from).collect();
    execute(&regex, &wide, 0, &mut [], ctx)
}

/// Implements the case-sensitive POSIX match operator (~).
pub fn textregexeq(
    source: &str,
    pattern: &str,
    ctx: &RegexpContext<'_>,
) -> Result<bool, RegexError> {
    is_match(source, pattern, REG_ADVANCED, ctx)
}

/// Implements the negated case-sensitive POSIX match operator (!~).
pub fn textregexne(
    source: &str,
    pattern: &str,
    ctx: &RegexpContext<'_>,
) -> Result<bool, RegexError> {
    textregexeq(source, pattern, ctx).map(|matched| !matched)
}

/// Implements the case-insensitive POSIX match operator (~*).
pub fn texticregexeq(
    source: &str,
    pattern: &str,
    ctx: &RegexpContext<'_>,
) -> Result<bool, RegexError> {
    is_match(source, pattern, REG_ADVANCED | REG_ICASE, ctx)
}

/// Implements the negated case-insensitive POSIX match operator (!~*).
pub fn texticregexne(
    source: &str,
    pattern: &str,
    ctx: &RegexpContext<'_>,
) -> Result<bool, RegexError> {
    texticregexeq(source, pattern, ctx).map(|matched| !matched)
}

/// Implements substring(source FROM pattern). If the pattern has any
/// captures, this returns the first capture. Otherwise it returns the match.
pub fn textregexsubstr<'a>(
    source: &'a str,
    pattern: &str,
    ctx: &RegexpContext<'_>,
) -> Result<Option<&'a str>, RegexError> {
    let regex = compile(pattern, REG_ADVANCED, ctx)?;
    let text = Text::new(source);
    let mut positions = [RegMatch::default(); 2];
    if !execute(&regex, &text.wide, 0, &mut positions, ctx)? {
        return Ok(None);
    }
    Ok(text.capture(positions[usize::from(regex.nsub() > 0)]))
}

/// Selects a SQL regexp_replace overload. For an explicit occurrence, zero
/// means all matches and overrides the presence or absence of 'g'.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReplaceOptions<'a> {
    #[default]
    Default,
    Flags(&'a str),
    Start(i32),
    Occurrence {
        start: i32,
        n: i32,
        flags: Option<&'a str>,
    },
}

/// Implements the three-argument regexp_replace and each overload with flags,
/// a start position or an explicit occurrence.
pub fn regexp_replace(
    source: &str,
    pattern: &str,
    replacement: &str,
    options: ReplaceOptions<'_>,
    ctx: &RegexpContext<'_>,
) -> Result<String, RegexError> {
    let (start, n, options) = match options {
        ReplaceOptions::Default => (1, None, None),
        ReplaceOptions::Flags(flags) => {
            if let Some(first @ '0'..='9') = flags.chars().next() {
                return Err(RegexError::flag(first).with_hint(
                    "If you meant to use regexp_replace() with a start parameter, cast the fourth argument to integer explicitly.",
                ));
            }
            (1, None, Some(flags))
        }
        ReplaceOptions::Start(start) => (start, None, None),
        ReplaceOptions::Occurrence { start, n, flags } => (start, Some(n), flags),
    };
    if start <= 0 {
        return Err(RegexError::parameter("start", start));
    }
    if let Some(n) = n.filter(|n| *n < 0) {
        return Err(RegexError::parameter("n", n));
    }
    let flags = parse_re_flags(options, ctx)?;
    let n = n.unwrap_or(i32::from(!flags.global));
    replace::replace(
        source,
        pattern,
        replacement,
        flags.cflags,
        ctx,
        (start - 1) as usize,
        n,
    )
}

/// Counts matches at or after the supplied start position. Pass 1 for the
/// SQL overloads that omit start.
pub fn regexp_count(
    source: &str,
    pattern: &str,
    start: i32,
    options: Option<&str>,
    ctx: &RegexpContext<'_>,
) -> Result<i32, RegexError> {
    if start <= 0 {
        return Err(RegexError::parameter("start", start));
    }
    let mut flags = nonglobal(options, "regexp_count", ctx)?;
    flags.global = true;
    let matches = scan(
        &Text::new(source),
        pattern,
        flags,
        (start - 1) as usize,
        ctx,
        false,
        false,
    )?;
    Ok(matches.len() as i32)
}

/// Optional arguments for regexp_instr, in SQL argument order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstrOptions<'a> {
    pub start: i32,
    pub n: i32,
    pub endoption: i32,
    pub flags: Option<&'a str>,
    pub subexpr: i32,
}

impl Default for InstrOptions<'_> {
    fn default() -> Self {
        Self {
            start: 1,
            n: 1,
            endoption: 0,
            flags: None,
            subexpr: 0,
        }
    }
}

/// Returns a match or capture position. End option 1 returns the position
/// after the match. A missing match or capture returns zero.
pub fn regexp_instr(
    source: &str,
    pattern: &str,
    options: InstrOptions<'_>,
    ctx: &RegexpContext<'_>,
) -> Result<i32, RegexError> {
    let InstrOptions {
        start,
        n,
        endoption,
        flags,
        subexpr,
    } = options;
    if start <= 0 {
        return Err(RegexError::parameter("start", start));
    }
    if n <= 0 {
        return Err(RegexError::parameter("n", n));
    }
    if !matches!(endoption, 0 | 1) {
        return Err(RegexError::parameter("endoption", endoption));
    }
    if subexpr < 0 {
        return Err(RegexError::parameter("subexpr", subexpr));
    }
    let mut flags = nonglobal(flags, "regexp_instr", ctx)?;
    flags.global = true;
    let matches = scan(
        &Text::new(source),
        pattern,
        flags,
        (start - 1) as usize,
        ctx,
        subexpr > 0,
        false,
    )?;
    Ok(matches.select(n, subexpr).map_or(0, |position| {
        let index = if endoption == 1 {
            position.eo
        } else {
            position.so
        };
        if index < 0 { 0 } else { index as i32 + 1 }
    }))
}

/// Tests whether the pattern matches anywhere in the input.
pub fn regexp_like(
    source: &str,
    pattern: &str,
    options: Option<&str>,
    ctx: &RegexpContext<'_>,
) -> Result<bool, RegexError> {
    let flags = nonglobal(options, "regexp_like", ctx)?;
    is_match(source, pattern, flags.cflags, ctx)
}

/// One SQL text array. When the pattern has no captures, the sole element is the whole match.
pub type CaptureRow<'a> = Vec<Option<&'a str>>;

/// Returns the first match as a text array, or None if there is no match.
pub fn regexp_match<'a>(
    source: &'a str,
    pattern: &str,
    options: Option<&str>,
    ctx: &RegexpContext<'_>,
) -> Result<Option<CaptureRow<'a>>, RegexError> {
    let flags = nonglobal(options, "regexp_match", ctx)?;
    Ok(capture_rows(source, pattern, flags, ctx)?.pop())
}

/// Returns the first match, or all non-overlapping matches with flag 'g'.
/// A non-matching input yields zero rows.
pub fn regexp_matches<'a>(
    source: &'a str,
    pattern: &str,
    options: Option<&str>,
    ctx: &RegexpContext<'_>,
) -> Result<Vec<CaptureRow<'a>>, RegexError> {
    capture_rows(source, pattern, parse_re_flags(options, ctx)?, ctx)
}

fn capture_rows<'a>(
    source: &'a str,
    pattern: &str,
    flags: ReFlags,
    ctx: &RegexpContext<'_>,
) -> Result<Vec<CaptureRow<'a>>, RegexError> {
    let text = Text::new(source);
    let matches = scan(&text, pattern, flags, 0, ctx, true, false)?;
    Ok(matches
        .rows()
        .map(|row| row.iter().map(|&position| text.capture(position)).collect())
        .collect())
}

/// Splits the input into a SQL text array, ignoring degenerate boundary
/// matches as PostgreSQL does.
pub fn regexp_split_to_array<'a>(
    source: &'a str,
    pattern: &str,
    options: Option<&str>,
    ctx: &RegexpContext<'_>,
) -> Result<Vec<&'a str>, RegexError> {
    split(
        source,
        pattern,
        nonglobal(options, "regexp_split_to_array", ctx)?,
        ctx,
    )
}

/// Splits the input as regexp_split_to_table, returning each piece as a row.
pub fn regexp_split_to_table<'a>(
    source: &'a str,
    pattern: &str,
    options: Option<&str>,
    ctx: &RegexpContext<'_>,
) -> Result<Vec<&'a str>, RegexError> {
    split(
        source,
        pattern,
        nonglobal(options, "regexp_split_to_table", ctx)?,
        ctx,
    )
}

fn split<'a>(
    source: &'a str,
    pattern: &str,
    mut flags: ReFlags,
    ctx: &RegexpContext<'_>,
) -> Result<Vec<&'a str>, RegexError> {
    flags.global = true;
    let text = Text::new(source);
    let matches = scan(&text, pattern, flags, 0, ctx, false, true)?;
    let mut result = Vec::with_capacity(matches.len() + 1);
    let mut start = 0;
    for row in matches.rows() {
        let position = row[0];
        result.push(text.slice(start, position.so as usize));
        start = position.eo as usize;
    }
    result.push(text.slice(start, text.wide.len()));
    Ok(result)
}

/// Optional arguments for regexp_substr, in SQL argument order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubstrOptions<'a> {
    pub start: i32,
    pub n: i32,
    pub flags: Option<&'a str>,
    pub subexpr: i32,
}

impl Default for SubstrOptions<'_> {
    fn default() -> Self {
        Self {
            start: 1,
            n: 1,
            flags: None,
            subexpr: 0,
        }
    }
}

/// Returns the requested match or capture, or None if it is missing.
pub fn regexp_substr<'a>(
    source: &'a str,
    pattern: &str,
    options: SubstrOptions<'_>,
    ctx: &RegexpContext<'_>,
) -> Result<Option<&'a str>, RegexError> {
    let SubstrOptions {
        start,
        n,
        flags,
        subexpr,
    } = options;
    if start <= 0 {
        return Err(RegexError::parameter("start", start));
    }
    if n <= 0 {
        return Err(RegexError::parameter("n", n));
    }
    if subexpr < 0 {
        return Err(RegexError::parameter("subexpr", subexpr));
    }
    let mut flags = nonglobal(flags, "regexp_substr", ctx)?;
    flags.global = true;
    let text = Text::new(source);
    let matches = scan(
        &text,
        pattern,
        flags,
        (start - 1) as usize,
        ctx,
        subexpr > 0,
        false,
    )?;
    Ok(matches
        .select(n, subexpr)
        .and_then(|position| text.capture(position)))
}

/// Converts a SQL pattern to a POSIX pattern, using backslash for None and
/// disabling escapes for Some(""). This represents both strict SQL arities.
pub fn similar_to_escape(
    pattern: &str,
    escape: Option<&str>,
    _ctx: &RegexpContext<'_>,
) -> Result<String, RegexError> {
    similar::translate(pattern, escape)
}

/// Legacy non-strict translation. A NULL pattern returns NULL. A NULL escape
/// selects backslash.
pub fn similar_escape(
    pattern: Option<&str>,
    escape: Option<&str>,
    ctx: &RegexpContext<'_>,
) -> Result<Option<String>, RegexError> {
    pattern
        .map(|pattern| similar_to_escape(pattern, escape, ctx))
        .transpose()
}

/// Implements SIMILAR TO using PostgreSQL's anchored pattern translation.
pub fn similar_to(
    source: &str,
    pattern: &str,
    escape: Option<&str>,
    ctx: &RegexpContext<'_>,
) -> Result<bool, RegexError> {
    textregexeq(source, &similar_to_escape(pattern, escape, ctx)?, ctx)
}

/// Implements substring(source SIMILAR pattern ESCAPE escape) and the legacy
/// substring(source FROM pattern FOR escape) spelling.
pub fn substring_similar<'a>(
    source: &'a str,
    pattern: &str,
    escape: &str,
    ctx: &RegexpContext<'_>,
) -> Result<Option<&'a str>, RegexError> {
    textregexsubstr(source, &similar_to_escape(pattern, Some(escape), ctx)?, ctx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_bits_follow_postgres() {
        let ctx = RegexpContext::new(RegexLocale::C);
        let cases = [
            ("", REG_ADVANCED, false),
            ("g", REG_ADVANCED, true),
            ("b", 0, false),
            ("ic", REG_ADVANCED, false),
            ("e", 0, false),
            ("i", REG_ADVANCED | REG_ICASE, false),
            ("m", REG_ADVANCED | REG_NEWLINE, false),
            ("n", REG_ADVANCED | REG_NEWLINE, false),
            ("np", REG_ADVANCED | REG_NLSTOP, false),
            ("q", REG_QUOTE, false),
            ("ns", REG_ADVANCED, false),
            ("xt", REG_ADVANCED, false),
            ("nw", REG_ADVANCED | REG_NLANCH, false),
            ("x", REG_ADVANCED | REG_EXPANDED, false),
            ("qeig", REG_ICASE, true),
        ];
        for (options, cflags, global) in cases {
            assert_eq!(
                parse_re_flags(Some(options), &ctx),
                Ok(ReFlags { cflags, global }),
                "{options}"
            );
        }
        assert_eq!(parse_re_flags(None, &ctx), parse_re_flags(Some(""), &ctx));
    }

    #[test]
    fn cancel_maps_to_query_canceled() {
        for context in ["invalid regular expression", "regular expression failed"] {
            assert_eq!(
                RegexError::engine(context, RegErrorCode::Cancel),
                RegexError::new("57014", "canceling statement due to user request"),
            );
        }
    }
}
