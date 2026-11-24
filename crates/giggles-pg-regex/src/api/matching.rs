use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::engine::{Chr, REG_NOSUB, RegMatch, Regex, RegexLocale};

use super::{ReFlags, RegexError, RegexpContext};

const CACHE_SIZE: usize = 32;

struct CachedRegex {
    pattern: Box<str>,
    cflags: i32,
    locale: RegexLocale,
    regex: Arc<Regex>,
}

thread_local! {
    static CACHE: RefCell<VecDeque<CachedRegex>> =
        RefCell::new(VecDeque::with_capacity(CACHE_SIZE));
}

pub(super) fn compile(
    pattern: &str,
    cflags: i32,
    ctx: &RegexpContext<'_>,
) -> Result<Arc<Regex>, RegexError> {
    let locale = ctx.locale;
    if let Some(regex) = CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = cache.iter().position(|entry| {
            entry.cflags == cflags && entry.locale == locale && &*entry.pattern == pattern
        })?;
        let entry = cache.remove(index).expect("cache hit");
        let regex = Arc::clone(&entry.regex);
        cache.push_front(entry);
        Some(regex)
    }) {
        return Ok(regex);
    }

    // The callback may call the API again. Release the cache borrow before
    // entering the engine and check for a nested insertion on return.
    let wide: Vec<Chr> = pattern.chars().map(Chr::from).collect();
    let regex = Arc::new(
        Regex::compile_with_cancel(&wide, cflags, locale, ctx.cancel)
            .map_err(|error| RegexError::engine("invalid regular expression", error))?,
    );
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(index) = cache.iter().position(|entry| {
            entry.cflags == cflags && entry.locale == locale && &*entry.pattern == pattern
        }) {
            cache.remove(index);
        } else if cache.len() == CACHE_SIZE {
            cache.pop_back();
        }
        cache.push_front(CachedRegex {
            pattern: pattern.into(),
            cflags,
            locale,
            regex: Arc::clone(&regex),
        });
    });
    Ok(regex)
}

pub(super) fn execute(
    regex: &Regex,
    wide: &[Chr],
    start: usize,
    positions: &mut [RegMatch],
    ctx: &RegexpContext<'_>,
) -> Result<bool, RegexError> {
    regex
        .exec_with_cancel(wide, start, None, positions, 0, ctx.cancel)
        .map_err(|error| RegexError::engine("regular expression failed", error))
}

pub(super) struct Text<'a> {
    source: &'a str,
    pub wide: Vec<Chr>,
    byte_offsets: Vec<usize>,
}

impl<'a> Text<'a> {
    pub fn new(source: &'a str) -> Self {
        let mut wide = Vec::new();
        let mut byte_offsets = Vec::new();
        for (byte, ch) in source.char_indices() {
            wide.push(Chr::from(ch));
            byte_offsets.push(byte);
        }
        byte_offsets.push(source.len());
        Self {
            source,
            wide,
            byte_offsets,
        }
    }

    pub fn slice(&self, start: usize, end: usize) -> &'a str {
        &self.source[self.byte_offsets[start]..self.byte_offsets[end]]
    }

    pub fn capture(&self, position: RegMatch) -> Option<&'a str> {
        if position.so < 0 || position.eo < 0 {
            return None;
        }
        Some(self.slice(position.so as usize, position.eo as usize))
    }
}

pub(super) struct Matches {
    positions: Vec<RegMatch>,
    width: usize,
}

impl Matches {
    pub fn len(&self) -> usize {
        self.positions.len() / self.width
    }

    pub fn rows(&self) -> impl Iterator<Item = &[RegMatch]> {
        self.positions.chunks_exact(self.width)
    }

    pub fn select(&self, n: i32, subexpr: i32) -> Option<RegMatch> {
        // With no capturing groups PostgreSQL exposes the whole match as the
        // sole stored pattern. Subexpr 1 then selects it too.
        self.positions
            .chunks_exact(self.width)
            .nth((n - 1) as usize)
            .and_then(|row| row.get((subexpr.max(1) - 1) as usize))
            .copied()
    }
}

pub(super) fn next_start(overall: RegMatch) -> usize {
    overall.eo as usize + usize::from(overall.so == overall.eo)
}

pub(super) fn scan(
    text: &Text<'_>,
    pattern: &str,
    flags: ReFlags,
    start: usize,
    ctx: &RegexpContext<'_>,
    captures: bool,
    split: bool,
) -> Result<Matches, RegexError> {
    let cflags = if captures {
        flags.cflags
    } else {
        flags.cflags | REG_NOSUB
    };
    let regex = compile(pattern, cflags, ctx)?;
    let captures = if captures { regex.nsub() } else { 0 };
    let mut current = vec![RegMatch::default(); captures + 1];
    let mut matches = Matches {
        positions: Vec::new(),
        width: captures.max(1),
    };
    let mut previous_end = 0;
    let mut search_start = start;
    while search_start <= text.wide.len()
        && execute(&regex, &text.wide, search_start, &mut current, ctx)?
    {
        let overall = current[0];
        // For splits, discard zero-width matches at either end or adjacent to
        // the preceding match, including a preceding discarded match.
        if !split || ((overall.so as usize) < text.wide.len() && overall.eo > previous_end) {
            // PG allows at most 2^28 - 1 int offsets, with one slot reserved
            // for the end of the source string.
            const MAX_MATCH_PAIRS: usize = (1 << 27) - 1;
            if matches.positions.len() + matches.width > MAX_MATCH_PAIRS {
                return Err(RegexError::new(
                    "54000",
                    "too many regular expression matches",
                ));
            }
            matches
                .positions
                .extend_from_slice(&current[usize::from(captures > 0)..]);
        }

        if !flags.global {
            break;
        }
        previous_end = overall.eo;
        search_start = next_start(overall);
    }
    Ok(matches)
}
