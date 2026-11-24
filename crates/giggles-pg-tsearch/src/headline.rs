// Port of PostgreSQL 18.6 wparser_def.c headline selection, ts_parse.c
// headline parsing and formatting, and tsearchcmds.c option parsing.
// Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group.
use crate::{
    Config, Parser, SearchResult, TsError, TsQuery, locale,
    matching::{Hit, Phrase, execute, locations},
    query::{Node, Operand},
    vector::{MAX_POS, MAX_WORD},
};

struct Options {
    start: String,
    stop: String,
    delimiter: String,
    max: i32,
    min: i32,
    short: i32,
    fragments: i32,
    all: bool,
}

fn quoted(mut tail: &str, mark: char, slash: bool) -> Option<(String, &str)> {
    let mut value = String::new();
    while let Some(c) = tail.chars().next() {
        tail = &tail[c.len_utf8()..];
        if c == mark {
            if let Some(rest) = tail.strip_prefix(mark) {
                tail = rest;
            } else {
                return Some((value, tail));
            }
        } else if c == '\\'
            && slash
            && let Some(rest) = tail.strip_prefix('\\')
        {
            tail = rest;
        }
        value.push(c);
    }
    None
}

fn option_pairs(mut rest: &str) -> Result<Vec<(String, String)>, TsError> {
    let entire = rest;
    let invalid = || {
        TsError::new(
            "42601",
            format!("invalid parameter list format: \"{entire}\""),
        )
    };
    let mut pairs = Vec::new();
    loop {
        rest = rest.trim_start_matches(|c| c == ',' || locale::ascii_space(c));
        if rest.is_empty() {
            return Ok(pairs);
        }
        let (key, tail) = if let Some(tail) = rest.strip_prefix('"') {
            quoted(tail, '"', false).ok_or_else(invalid)?
        } else {
            let initial = rest.chars().next().unwrap().len_utf8();
            let end = rest[initial..]
                .find(|c| c == '=' || locale::ascii_space(c))
                .map_or(rest.len(), |n| n + initial);
            (rest[..end].to_owned(), &rest[end..])
        };
        let tail = tail
            .trim_start_matches(locale::ascii_space)
            .strip_prefix('=')
            .ok_or_else(invalid)?
            .trim_start_matches(locale::ascii_space);
        if tail.is_empty() {
            return Err(invalid());
        }
        let (value, tail) =
            if let Some(text) = tail.strip_prefix('\'').or_else(|| tail.strip_prefix("E'")) {
                quoted(text, '\'', true).ok_or_else(invalid)?
            } else if let Some(text) = tail.strip_prefix('"') {
                quoted(text, '"', false).ok_or_else(invalid)?
            } else {
                let initial = tail.chars().next().unwrap().len_utf8();
                let end = tail[initial..]
                    .find(|c| c == ',' || locale::ascii_space(c))
                    .map_or(tail.len(), |n| n + initial);
                let value = &tail[..end];
                let value = value
                    .parse::<i32>()
                    .map_or_else(|_| value.to_owned(), |v| v.to_string());
                (value, &tail[end..])
            };
        pairs.push((key, value));
        rest = tail;
    }
}

fn integer(input: &str) -> Result<i32, TsError> {
    let syntax = || {
        TsError::new(
            "22P02",
            format!("invalid input syntax for type integer: \"{input}\""),
        )
    };
    let range = || {
        TsError::new(
            "22003",
            format!("value \"{input}\" is out of range for type integer"),
        )
    };
    let text = input.trim_matches(locale::ascii_space);
    let (sign, text) = if let Some(text) = text.strip_prefix('-') {
        (-1, text)
    } else {
        (1, text.strip_prefix('+').unwrap_or(text))
    };
    let (radix, digits) = match text.as_bytes().get(..2) {
        Some([b'0', b'x' | b'X']) => (16, &text[2..]),
        Some([b'0', b'o' | b'O']) => (8, &text[2..]),
        Some([b'0', b'b' | b'B']) => (2, &text[2..]),
        _ => (10, text),
    };
    let mut total = 0i64;
    let mut previous_digit = false;
    for (i, c) in digits.chars().enumerate() {
        if c == '_' && (previous_digit || (i == 0 && radix != 10)) {
            previous_digit = false;
        } else if let Some(digit) = c.to_digit(radix) {
            previous_digit = true;
            total = total * i64::from(radix) + i64::from(digit);
            if total > i64::from(i32::MAX) + i64::from(sign == -1) {
                return Err(range());
            }
        } else {
            return Err(syntax());
        }
    }
    if !previous_digit {
        return Err(syntax());
    }
    Ok((total * sign) as i32)
}

impl Options {
    fn parse(text: &str) -> Result<Self, TsError> {
        let mut opts = Self {
            start: "<b>".into(),
            stop: "</b>".into(),
            delimiter: " ... ".into(),
            max: 35,
            min: 15,
            short: 3,
            fragments: 0,
            all: false,
        };
        for (key, value) in option_pairs(text)? {
            match key.to_ascii_lowercase().as_str() {
                "maxwords" => opts.max = integer(&value)?,
                "minwords" => opts.min = integer(&value)?,
                "shortword" => opts.short = integer(&value)?,
                "maxfragments" => opts.fragments = integer(&value)?,
                "startsel" => opts.start = value,
                "stopsel" => opts.stop = value,
                "fragmentdelimiter" => opts.delimiter = value,
                "highlightall" => {
                    opts.all = matches!(
                        value.to_ascii_lowercase().as_str(),
                        "1" | "on" | "true" | "t" | "y" | "yes"
                    )
                }
                _ => {
                    return Err(TsError::new(
                        "22023",
                        format!("unrecognized headline parameter: \"{key}\""),
                    ));
                }
            }
        }
        if !opts.all {
            let error = if opts.min >= opts.max {
                "MinWords must be less than MaxWords"
            } else if opts.min <= 0 {
                "MinWords must be positive"
            } else if opts.short < 0 {
                "ShortWord must be >= 0"
            } else if opts.fragments < 0 {
                "MaxFragments must be >= 0"
            } else {
                ""
            };
            if !error.is_empty() {
                return Err(TsError::new("22023", error));
            }
        }
        for (name, value) in [
            ("StartSel", &opts.start),
            ("StopSel", &opts.stop),
            ("FragmentDelimiter", &opts.delimiter),
        ] {
            if value.len() > i16::MAX as usize {
                return Err(TsError::new(
                    "22023",
                    format!("value for \"{name}\" is too long"),
                ));
            }
        }
        Ok(opts)
    }
}

#[derive(Clone, Copy)]
struct Word<'a> {
    raw: &'a str,
    id: u8,
    pos: u16,
    item: Option<*const Operand>,
    repeated: bool,
}

impl Word<'_> {
    fn skip(self) -> bool {
        matches!(self.id, 5 | 15..=17)
    }

    fn nonword(self) -> bool {
        matches!(self.id, 12 | 13) || self.skip()
    }

    fn interesting(self) -> bool {
        self.item.is_some() && !self.repeated
    }

    fn bad_endpoint(self, short: i32) -> bool {
        (self.nonword()
            || matches!(self.id, 7 | 8 | 14 | 20..=23)
            || self.raw.len() as i32 <= short)
            && !self.interesting()
    }
}

fn tokenize<'a>(
    config: Config,
    document: &'a str,
    root: Option<&Node>,
    notices: &mut Vec<TsError>,
) -> Vec<Word<'a>> {
    let mut words = Vec::new();
    let mut operands = Vec::new();
    if let Some(root) = root {
        root.visit_operands(&mut |operand| operands.push(operand));
    }
    let mut pos = 0;
    for token in Parser::new(document) {
        if token.text.len() >= MAX_WORD {
            notices.push(TsError::long_token());
            continue;
        }
        let mut word = Word {
            raw: token.text,
            id: token.kind.id,
            pos: 0,
            item: None,
            repeated: false,
        };
        if let Some(dict) = config.dictionary(token.kind.id) {
            pos = (pos + 1).min(MAX_POS);
            if let Some(lexeme) = dict.lexize(token.text) {
                word.pos = pos;
                for &operand in &operands {
                    let matches = if operand.prefix {
                        lexeme.starts_with(&operand.word)
                    } else {
                        lexeme == operand.word
                    };
                    if matches {
                        if word.item.is_some() {
                            words.push(word);
                            word.repeated = true;
                        }
                        word.item = Some(std::ptr::from_ref(operand));
                    }
                }
            }
        }
        words.push(word);
    }
    words
}

fn check(words: &[Word<'_>], operand: &Operand, positions: bool) -> Hit {
    let id = Some(std::ptr::from_ref(operand));
    let mut matches = Vec::new();
    for word in words.iter().filter(|word| word.item == id) {
        if !positions {
            return Hit::present();
        }
        if matches.last().is_none_or(|&previous| previous < word.pos) {
            matches.push(word.pos);
        }
    }
    if matches.is_empty() {
        Hit::No
    } else {
        Hit::Yes(Phrase {
            positions: matches,
            ..Phrase::default()
        })
    }
}

struct Covers<'a, 'doc> {
    words: &'a [Word<'doc>],
    root: &'a Node,
    locations: Vec<Phrase>,
    next: i32,
}

impl Iterator for Covers<'_, '_> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut end_pos = -1;
            for term in &self.locations {
                let first = term
                    .positions
                    .iter()
                    .map(|&p| i32::from(p))
                    .find(|&p| p >= self.next)?;
                end_pos = end_pos.max(first);
            }
            if end_pos < 0 {
                return None;
            }
            let begin_pos = self
                .locations
                .iter()
                .map(|term| {
                    term.positions
                        .iter()
                        .rev()
                        .map(|&p| i32::from(p) - term.width)
                        .find(|&p| p <= end_pos)
                        .unwrap_or(-1)
                })
                .min()?
                .max(self.next);
            self.next = begin_pos + 1;
            if begin_pos > end_pos {
                continue;
            }
            let mut begin = None;
            let mut end = None;
            for (i, word) in self.words.iter().enumerate() {
                if word.item.is_none() {
                    continue;
                }
                let pos = i32::from(word.pos);
                if pos >= begin_pos && begin.is_none() {
                    begin = Some(i);
                }
                if pos > end_pos {
                    break;
                }
                end = Some(i);
            }
            if let (Some(a), Some(b)) = (begin, end)
                && a <= b
                && execute(self.root, &|operand, positions| {
                    check(&self.words[a..=b], operand, positions)
                })
            {
                return Some((a, b));
            }
        }
    }
}

fn first_words(words: &[Word<'_>], minimum: i32, included: &mut [bool]) {
    let mut count = 0;
    for (word, included) in words.iter().zip(included) {
        if count >= minimum {
            break;
        }
        count += i32::from(!word.nonword());
        *included = !word.repeated;
    }
}

fn single(
    words: &[Word<'_>],
    covers: impl Iterator<Item = (usize, usize)>,
    opts: &Options,
    included: &mut [bool],
) {
    if opts.all {
        for (word, present) in words.iter().zip(included) {
            *present = !word.repeated;
        }
        return;
    }

    let bad = |i: usize| words[i].bad_endpoint(opts.short);
    let mut best: Option<((bool, i32, bool), usize, usize)> = None;
    for (p, q) in covers {
        let mut total = 0;
        let mut hits = 0;
        let mut left = p;
        let mut right = p;
        let mut i = p;
        while i <= q && total < opts.max {
            total += i32::from(!words[i].nonword());
            hits += i32::from(words[i].interesting());
            right = i;
            i += 1;
        }
        if total < opts.max {
            for (i, word) in words.iter().enumerate().skip(i.saturating_sub(1)) {
                if total >= opts.max {
                    break;
                }
                if i > q {
                    total += i32::from(!word.nonword());
                    hits += i32::from(word.interesting());
                }
                right = i;
                if !bad(i) && total >= opts.min {
                    break;
                }
            }
            if total < opts.min {
                for i in (0..p).rev() {
                    total += i32::from(!words[i].nonword());
                    hits += i32::from(words[i].interesting());
                    left = i;
                    if total >= opts.max || (!bad(i) && total >= opts.min) {
                        break;
                    }
                }
            }
        } else {
            for i in (0..=i.min(q)).rev() {
                if total <= opts.min || !bad(i) {
                    break;
                }
                total -= i32::from(!words[i].nonword());
                hits -= i32::from(words[i].interesting());
                right = i.saturating_sub(1);
            }
        }
        let key = (left <= p && right >= q, hits, !bad(right));
        if best.is_none_or(|(old, ..)| key > old) {
            best = Some((key, left, right));
        }
    }
    if let Some((_, a, b)) = best {
        mark(words, included, a, b);
    } else {
        first_words(words, opts.min, included);
    }
}

fn mark(words: &[Word<'_>], included: &mut [bool], start: usize, end: usize) {
    for i in start..=end {
        included[i] = !words[i].repeated;
    }
}

struct Fragment {
    start: usize,
    end: usize,
    total: i32,
    hits: i32,
    excluded: bool,
}

fn fragments(
    words: &[Word<'_>],
    covers: impl Iterator<Item = (usize, usize)>,
    opts: &Options,
    included: &mut [bool],
) {
    let bad = |i: usize| words[i].bad_endpoint(opts.short);
    let mut candidates = Vec::new();
    for (p, q) in covers {
        let mut start = p;
        while start <= q {
            start = (start..=q).find(|&i| words[i].interesting()).unwrap_or(q);
            let mut total = 0;
            let mut hits = 0;
            let mut i = start;
            while i <= q && total < opts.max {
                total += i32::from(!words[i].nonword());
                hits += i32::from(words[i].interesting());
                i += 1;
            }
            let mut end = q;
            if q > i {
                for i in (start..=i).rev() {
                    end = i;
                    if words[i].interesting() {
                        break;
                    }
                    total -= i32::from(!words[i].nonword());
                }
            }
            candidates.push(Fragment {
                start,
                end,
                total,
                hits,
                excluded: false,
            });
            start = end + 1;
        }
    }

    let mut chosen = false;
    for _ in 0..opts.fragments {
        let Some(best) = candidates
            .iter()
            .enumerate()
            .filter(|(_, f)| !f.excluded)
            .min_by_key(|(i, f)| (-f.hits, f.total, *i))
            .map(|(i, _)| i)
        else {
            break;
        };
        let Fragment {
            mut start,
            mut end,
            mut total,
            ..
        } = candidates[best];
        if total < opts.max {
            let max_stretch = (opts.max - total) / 2;
            let mut stretched = 0;
            let original_start = start;
            for i in (0..start).rev() {
                if stretched >= max_stretch || included[i] {
                    break;
                }
                let increment = i32::from(!words[i].nonword());
                total += increment;
                stretched += increment;
                start = i;
            }
            while start < original_start && bad(start) {
                total -= i32::from(!words[start].nonword());
                start += 1;
            }
            let original_end = end;
            for i in end + 1..words.len() {
                if total >= opts.max || included[i] {
                    break;
                }
                total += i32::from(!words[i].nonword());
                end = i;
            }
            while end > original_end && bad(end) {
                end -= 1;
            }
        }
        mark(words, included, start, end);
        chosen = true;
        for candidate in &mut candidates {
            if candidate.start <= end && candidate.end >= start {
                candidate.excluded = true;
            }
        }
    }
    if !chosen {
        first_words(words, opts.min, included);
    }
}

/// Select and highlight a document excerpt using PostgreSQL's headline options.
pub fn ts_headline(
    config: Config,
    document: &str,
    query: &TsQuery,
    options: Option<&str>,
) -> Result<SearchResult<String>, TsError> {
    let opts = Options::parse(options.unwrap_or(""))?;
    let mut notices = Vec::new();
    let root = query.root.as_deref();
    let words = tokenize(config, document, root, &mut notices);
    let covers = root.into_iter().flat_map(|root| Covers {
        words: &words,
        root,
        locations: locations(root, &|operand, positions| {
            check(&words, operand, positions)
        })
        .unwrap_or_default(),
        next: 0,
    });
    let mut included = vec![false; words.len()];
    if opts.fragments == 0 {
        single(&words, covers, &opts, &mut included);
    } else {
        fragments(&words, covers, &opts, &mut included);
    }
    let mut text = String::new();
    let mut fragment_count = 0;
    let mut in_fragment = false;
    for (word, included) in words.into_iter().zip(included) {
        if word.repeated {
            continue;
        }
        if !included {
            in_fragment = false;
            continue;
        }
        if !in_fragment {
            if fragment_count != 0 {
                text.push_str(&opts.delimiter);
            }
            fragment_count += 1;
            in_fragment = true;
        }
        if !opts.all && word.id == 13 {
            text.push(' ');
        } else if !word.skip() {
            if word.item.is_some() {
                text.push_str(&opts.start);
            }
            text.push_str(word.raw);
            if word.item.is_some() {
                text.push_str(&opts.stop);
            }
        }
    }
    Ok(SearchResult {
        value: text,
        notices,
    })
}
