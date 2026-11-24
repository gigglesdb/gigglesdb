// Port of PostgreSQL 18.6 tsvector.c, tsvector_parser.c, tsvector_op.c and
// the vector construction in to_tsany.c.
// Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group.
use std::{cmp::Ordering, collections::BTreeMap, fmt, str::FromStr};

use crate::{Config, Corrupt, SearchResult, TsError, Wire, config, locale, quote};

pub(crate) const MAX_POS: u16 = 16383;
pub(crate) const MAX_WORD: usize = 2047;
pub(crate) const MAX_DATA: usize = 1048575;
const MAX_POSITIONS: usize = 256;

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum Weight {
    #[default]
    D,
    C,
    B,
    A,
}

impl Weight {
    pub fn parse(value: u8) -> Result<Self, TsError> {
        match value.to_ascii_uppercase() {
            b'A' => Ok(Self::A),
            b'B' => Ok(Self::B),
            b'C' => Ok(Self::C),
            b'D' => Ok(Self::D),
            _ => {
                let printed = if (b' '..127).contains(&value) {
                    char::from(value).to_string()
                } else {
                    format!("\\{value:03o}")
                };
                Err(TsError::new(
                    "22023",
                    format!("unrecognized weight: \"{printed}\""),
                ))
            }
        }
    }

    pub(crate) const fn from_bits(bits: u16) -> Self {
        [Self::D, Self::C, Self::B, Self::A][(bits & 3) as usize]
    }

    pub const fn letter(self) -> char {
        ['D', 'C', 'B', 'A'][self as usize]
    }

    pub const fn mask(self) -> u8 {
        1 << self as u8
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Position {
    pub index: u16,
    pub weight: Weight,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lexeme {
    pub word: String,
    pub positions: Vec<Position>,
}

/// PostgreSQL's sorted lexeme array. Binary input can preserve duplicate
/// lexemes and positions of zero. Text input merges duplicates and rejects
/// zero positions.
#[derive(Clone, Debug, Default)]
pub struct TsVector {
    entries: Vec<Lexeme>,
    data_len: usize,
}

fn vector_size_error(size: usize) -> TsError {
    TsError::new(
        "54000",
        format!("string is too long for tsvector ({size} bytes, max {MAX_DATA} bytes)"),
    )
}

fn word_size_error(size: usize) -> TsError {
    TsError::new(
        "54000",
        format!(
            "word is too long ({size} bytes, max {} bytes)",
            MAX_WORD - 1
        ),
    )
}

fn entry_size(current: usize, word_bytes: usize, positions: usize) -> usize {
    let size = current + word_bytes;
    if positions == 0 {
        size
    } else {
        size.next_multiple_of(2) + 2 + 2 * positions
    }
}

fn packed_size(entries: &[Lexeme]) -> usize {
    entries.iter().fold(0, |size, lexeme| {
        entry_size(size, lexeme.word.len(), lexeme.positions.len())
    })
}

impl TsVector {
    fn new(entries: Vec<Lexeme>) -> Result<Self, TsError> {
        let data_len = packed_size(&entries);
        if data_len > MAX_DATA {
            Err(vector_size_error(data_len))
        } else {
            Ok(Self { entries, data_len })
        }
    }

    pub fn lexemes(&self) -> &[Lexeme] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn unnest(&self) -> impl Iterator<Item = &Lexeme> {
        self.entries.iter()
    }

    pub fn to_array(&self) -> Vec<&str> {
        self.entries
            .iter()
            .map(|lexeme| lexeme.word.as_str())
            .collect()
    }

    pub fn from_array(words: &[Option<&str>]) -> Result<Self, TsError> {
        let mut entries = Vec::with_capacity(words.len());
        for word in words {
            let word =
                word.ok_or_else(|| TsError::new("22004", "lexeme array may not contain nulls"))?;
            if word.is_empty() {
                return Err(TsError::new(
                    "2200F",
                    "lexeme array may not contain empty strings",
                ));
            }
            if word.len() >= MAX_WORD {
                return Err(word_size_error(word.len()));
            }
            entries.push(Lexeme {
                word: word.to_owned(),
                positions: Vec::new(),
            });
        }
        entries.sort_unstable_by(|a, b| a.word.cmp(&b.word));
        entries.dedup_by(|a, b| a.word == b.word);
        Self::new(entries)
    }

    pub fn strip(&self) -> Self {
        let mut out = self.clone();
        for lexeme in &mut out.entries {
            lexeme.positions.clear();
        }
        out.data_len = packed_size(&out.entries);
        out
    }

    pub fn delete_lexeme(&self, word: &str) -> Self {
        if self.search(word).is_ok() {
            self.delete(&[Some(word)])
        } else {
            self.clone()
        }
    }

    pub fn delete(&self, words: &[Option<&str>]) -> Self {
        let mut indices: Vec<_> = words
            .iter()
            .flatten()
            .filter_map(|word| self.search(word).ok())
            .collect();
        indices.sort_unstable();
        indices.dedup();
        let mut out = self.clone();
        for index in indices.into_iter().rev() {
            out.entries.remove(index);
        }
        out.data_len = packed_size(&out.entries);
        out
    }

    pub fn setweight(&self, weight: Weight, words: Option<&[Option<&str>]>) -> Self {
        let mut out = self.clone();
        if let Some(words) = words {
            for word in words.iter().flatten() {
                if let Ok(i) = self.search(word) {
                    for position in &mut out.entries[i].positions {
                        position.weight = weight;
                    }
                }
            }
        } else {
            for lexeme in &mut out.entries {
                for position in &mut lexeme.positions {
                    position.weight = weight;
                }
            }
        }
        out
    }

    pub fn filter(&self, weights: &[Option<u8>]) -> Result<Self, TsError> {
        let mut mask = 0;
        for weight in weights {
            let weight = weight
                .ok_or_else(|| TsError::new("22004", "weight array may not contain nulls"))?;
            mask |= Weight::parse(weight)?.mask();
        }
        let mut entries = self.entries.clone();
        entries.retain_mut(|lexeme| {
            lexeme.positions.retain(|pos| pos.weight.mask() & mask != 0);
            !lexeme.positions.is_empty()
        });
        Self::new(entries)
    }

    pub fn concat(&self, right: &Self) -> Result<Self, TsError> {
        let max = self
            .entries
            .iter()
            .filter_map(|lexeme| lexeme.positions.last())
            .map(|pos| pos.index)
            .max()
            .unwrap_or(0);
        let mut entries = Vec::with_capacity(self.len() + right.len());
        let mut lhs = self.entries.iter().peekable();
        let mut rhs = right.entries.iter().peekable();
        while let (Some(a), Some(b)) = (lhs.peek(), rhs.peek()) {
            match a.word.cmp(&b.word) {
                Ordering::Less => entries.push(lhs.next().unwrap().clone()),
                Ordering::Greater => entries.push(shift(rhs.next().unwrap().clone(), max)),
                Ordering::Equal => {
                    let mut lexeme = lhs.next().unwrap().clone();
                    append_shifted(&mut lexeme.positions, &rhs.next().unwrap().positions, max);
                    entries.push(lexeme);
                }
            }
        }
        entries.extend(lhs.cloned());
        entries.extend(rhs.cloned().map(|lexeme| shift(lexeme, max)));
        Self::new(entries)
    }

    // Use PostgreSQL's midpoint search. Exact matching selects one entry
    // even when binary input supplied multiple copies of a lexeme.
    fn search(&self, word: &str) -> Result<usize, usize> {
        let (mut low, mut high) = (0, self.entries.len());
        while low < high {
            let mid = low + (high - low) / 2;
            match word.cmp(self.entries[mid].word.as_str()) {
                Ordering::Less => high = mid,
                Ordering::Greater => low = mid + 1,
                Ordering::Equal => return Ok(mid),
            }
        }
        Err(low)
    }

    pub(crate) fn matching_entries<'a>(
        &'a self,
        word: &str,
        prefix: bool,
    ) -> impl Iterator<Item = (usize, &'a Lexeme)> {
        let end = self.entries.len();
        let (start, stop) = match self.search(word) {
            Ok(i) if !prefix => (i, i + 1),
            Ok(i) | Err(i) if prefix => {
                let stop =
                    i + self.entries[i..].partition_point(|entry| entry.word.starts_with(word));
                (i, stop)
            }
            _ => (end, end),
        };
        self.entries[start..stop]
            .iter()
            .enumerate()
            .map(move |(i, entry)| (start + i, entry))
    }

    pub(crate) fn matching(&self, word: &str, prefix: bool) -> impl Iterator<Item = &Lexeme> {
        self.matching_entries(word, prefix).map(|(_, entry)| entry)
    }

    pub fn send(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(&(self.len() as u32).to_be_bytes());
        for lexeme in &self.entries {
            output.extend_from_slice(lexeme.word.as_bytes());
            output.push(0);
            output.extend_from_slice(&(lexeme.positions.len() as u16).to_be_bytes());
            for pos in &lexeme.positions {
                output.extend_from_slice(&(pos.index | ((pos.weight as u16) << 14)).to_be_bytes());
            }
        }
        output
    }

    /// Storage version 1: packed data size (u32 BE), then the send payload.
    pub fn to_storage(&self) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&(self.data_len as u32).to_be_bytes());
        bytes.extend(self.send());
        bytes
    }

    /// Restore the original packed size and order of duplicate lexemes.
    pub fn from_storage(bytes: &[u8]) -> Result<Self, Corrupt> {
        let Some((1, bytes)) = bytes.split_first() else {
            return Err(Corrupt);
        };
        let (size, payload) = bytes.split_first_chunk::<4>().ok_or(Corrupt)?;
        let size = u32::from_be_bytes(*size) as usize;
        if size > MAX_DATA {
            return Err(Corrupt);
        }
        let mut value = Self::decode(payload, false).map_err(|_| Corrupt)?;
        if !value
            .entries
            .windows(2)
            .all(|pair| pair[0].word <= pair[1].word)
        {
            return Err(Corrupt);
        }
        // Padding inserts one byte when a positioned entry follows an odd
        // offset. Each odd-length unpositioned word can toggle one such
        // insertion. Any unused unpositioned words may appear at the end.
        let mut base = 0;
        let mut odd_plain = 0;
        let mut odd_positioned: usize = 0;
        let mut even_positioned = 0;
        for entry in &value.entries {
            let len = entry.word.len();
            base += len;
            if entry.positions.is_empty() {
                odd_plain += len % 2;
            } else {
                base += 2 + 2 * entry.positions.len();
                odd_positioned += len % 2;
                even_positioned += 1 - len % 2;
            }
        }
        let min = base + odd_positioned.saturating_sub(odd_plain);
        let max = base + odd_positioned + even_positioned.min(odd_plain);
        if !(min..=max).contains(&size) {
            return Err(Corrupt);
        }
        value.data_len = size;
        Ok(value)
    }

    pub fn recv(bytes: &[u8]) -> Result<Self, TsError> {
        Self::decode(bytes, true)
    }

    fn decode(bytes: &[u8], wire_input: bool) -> Result<Self, TsError> {
        let mut wire = Wire(bytes);
        let count = u32::from_be_bytes(wire.read()?) as usize;
        if count > MAX_DATA {
            return Err(TsError::internal("invalid size of tsvector"));
        }
        let mut entries = Vec::with_capacity(count.min(wire.0.len() / 4));
        let mut data_len = 0;
        for _ in 0..count {
            let word = wire.string()?;
            let count = u16::from_be_bytes(wire.read()?) as usize;
            if word.is_empty() {
                return Err(TsError::internal("invalid tsvector: empty lexeme"));
            }
            if word.len() > MAX_WORD {
                return Err(TsError::internal("invalid tsvector: lexeme too long"));
            }
            if wire_input && data_len > MAX_DATA {
                return Err(TsError::internal(
                    "invalid tsvector: maximum total lexeme length exceeded",
                ));
            }
            if count > MAX_POSITIONS {
                return Err(TsError::internal("unexpected number of tsvector positions"));
            }
            let mut positions: Vec<Position> = Vec::with_capacity(count.min(wire.0.len() / 2));
            for _ in 0..count {
                let bits = u16::from_be_bytes(wire.read()?);
                let pos = Position {
                    index: bits & MAX_POS,
                    weight: Weight::from_bits(bits >> 14),
                };
                if positions
                    .last()
                    .is_some_and(|previous| previous.index >= pos.index)
                {
                    return Err(TsError::internal("position information is misordered"));
                }
                positions.push(pos);
            }
            data_len = entry_size(data_len, word.len(), count);
            entries.push(Lexeme {
                word: word.to_owned(),
                positions,
            });
        }
        if wire_input && data_len > MAX_DATA {
            return Err(TsError::internal(
                "invalid tsvector: maximum total lexeme length exceeded",
            ));
        }
        wire.finish()?;
        if wire_input {
            crate::pg_sort::sort(&mut entries, &|a, b| a.word.cmp(&b.word));
        }
        Ok(Self { entries, data_len })
    }
}

fn append_shifted(out: &mut Vec<Position>, positions: &[Position], max: u16) {
    for &position in positions {
        if out.len() == MAX_POSITIONS || out.last().is_some_and(|p| p.index == MAX_POS) {
            break;
        }
        out.push(Position {
            index: (position.index + max).min(MAX_POS),
            ..position
        });
    }
}

fn shift(mut lexeme: Lexeme, max: u16) -> Lexeme {
    let original = std::mem::take(&mut lexeme.positions);
    append_shifted(&mut lexeme.positions, &original, max);
    lexeme
}

pub(crate) fn literal_word(
    input: &str,
    end_at_operator: bool,
    web: bool,
) -> Result<(String, &str), TsError> {
    let kind = if end_at_operator {
        "tsquery"
    } else {
        "tsvector"
    };
    let syntax = || TsError::syntax(kind, input);
    let mut chars = input.char_indices().peekable();
    let mut word = String::new();
    let quoted = !web && input.starts_with('\'');
    if quoted {
        chars.next();
    }
    while let Some((index, c)) = chars.next() {
        if quoted && c == '\'' {
            if chars.next_if(|&(_, next)| next == '\'').is_some() {
                word.push(c);
            } else if word.is_empty() {
                return Err(syntax());
            } else {
                return Ok((word, &input[index + 1..]));
            }
        } else if !quoted
            && (locale::ascii_space(c)
                || (end_at_operator && crate::query::is_operator(c))
                || (web && c == '"')
                || (c == ':' && !word.is_empty()))
        {
            return if word.is_empty() {
                Err(syntax())
            } else {
                Ok((word, &input[index..]))
            };
        } else if !web && c == '\\' {
            let (_, escaped) = chars.next().ok_or_else(|| {
                TsError::new(
                    "42601",
                    format!("there is no escaped character: \"{input}\""),
                )
            })?;
            word.push(escaped);
        } else {
            word.push(c);
        }
    }
    if quoted || word.is_empty() {
        Err(syntax())
    } else {
        Ok((word, ""))
    }
}

fn parse_positions<'a>(
    mut rest: &'a str,
    full: &str,
    out: &mut Vec<Position>,
) -> Result<&'a str, TsError> {
    let syntax = || TsError::syntax("tsvector", full);
    loop {
        let end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        if end == 0 {
            return Err(syntax());
        }
        // PostgreSQL uses atoi and stores the result in a 14-bit field.
        let value = rest[..end].parse::<i64>().unwrap_or(i64::MAX) as i32;
        let index = (value.min(i32::from(MAX_POS)) as u16) & MAX_POS;
        if index == 0 {
            return Err(TsError::new(
                "42601",
                format!("wrong position info in tsvector: \"{full}\""),
            ));
        }
        rest = &rest[end..];
        let mut weight = Weight::D;
        while let Some(&c) = rest.as_bytes().first() {
            if c == b',' || locale::ascii_space(char::from(c)) {
                break;
            }
            if !c.is_ascii_digit() {
                if weight != Weight::D {
                    return Err(syntax());
                }
                weight = if c == b'*' {
                    Weight::A
                } else {
                    Weight::parse(c).map_err(|_| syntax())?
                };
            }
            rest = &rest[1..];
        }
        out.push(Position { index, weight });
        if let Some(tail) = rest.strip_prefix(',') {
            rest = tail;
        } else {
            return Ok(rest);
        }
    }
}

fn unique_positions(positions: &mut Vec<Position>) {
    crate::pg_sort::sort(positions, &|a, b| a.index.cmp(&b.index));
    let mut kept = 0;
    for i in 0..positions.len() {
        let position = positions[i];
        if kept > 0 && positions[kept - 1].index == position.index {
            positions[kept - 1].weight = positions[kept - 1].weight.max(position.weight);
        } else {
            positions[kept] = position;
            kept += 1;
            if kept == MAX_POSITIONS || (kept > 1 && position.index == MAX_POS) {
                break;
            }
        }
    }
    positions.truncate(kept);
}

impl FromStr for TsVector {
    type Err = TsError;

    fn from_str(input: &str) -> Result<Self, TsError> {
        let mut rest = input;
        let mut bytes = 0;
        let mut entries: Vec<Lexeme> = Vec::new();
        loop {
            rest = rest.trim_start_matches(locale::ascii_space);
            if rest.is_empty() {
                break;
            }
            let (word, tail) = literal_word(rest, false, false).map_err(|mut err| {
                err.message = if err.message.starts_with("there is no escaped") {
                    format!("there is no escaped character: \"{input}\"")
                } else {
                    TsError::syntax("tsvector", input).message
                };
                err
            })?;
            let mut positions = Vec::new();
            rest = if let Some(tail) = tail.strip_prefix(':') {
                parse_positions(tail, input, &mut positions)?
            } else {
                tail
            };
            if word.len() >= MAX_WORD {
                return Err(word_size_error(word.len()));
            }
            if bytes > MAX_DATA {
                return Err(vector_size_error(bytes));
            }
            bytes += word.len();
            entries.push(Lexeme { word, positions });
        }
        crate::pg_sort::sort(&mut entries, &|a, b| a.word.cmp(&b.word));
        entries.dedup_by(|later, earlier| {
            if later.word != earlier.word {
                return false;
            }
            earlier.positions.append(&mut later.positions);
            true
        });
        for lexeme in &mut entries {
            unique_positions(&mut lexeme.positions);
        }
        Self::new(entries)
    }
}

impl fmt::Display for TsVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, lexeme) in self.entries.iter().enumerate() {
            if index > 0 {
                f.write_str(" ")?;
            }
            quote(f, &lexeme.word)?;
            for (index, pos) in lexeme.positions.iter().enumerate() {
                let separator = if index == 0 { ':' } else { ',' };
                write!(f, "{separator}{}", pos.index)?;
                if pos.weight != Weight::D {
                    write!(f, "{}", pos.weight.letter())?;
                }
            }
        }
        Ok(())
    }
}

impl Ord for TsVector {
    fn cmp(&self, other: &Self) -> Ordering {
        let size = |vector: &Self| vector.data_len + 4 * vector.len();
        size(self)
            .cmp(&size(other))
            .then_with(|| self.len().cmp(&other.len()))
            .then_with(|| {
                self.entries
                    .iter()
                    .zip(&other.entries)
                    .map(|(a, b)| {
                        a.positions
                            .is_empty()
                            .cmp(&b.positions.is_empty())
                            .then_with(|| a.word.cmp(&b.word))
                            .then_with(|| b.positions.len().cmp(&a.positions.len()))
                            .then_with(|| {
                                a.positions
                                    .iter()
                                    .zip(&b.positions)
                                    .map(|(a, b)| {
                                        b.index.cmp(&a.index).then(b.weight.cmp(&a.weight))
                                    })
                                    .find(|order| !order.is_eq())
                                    .unwrap_or(Ordering::Equal)
                            })
                    })
                    .find(|order| !order.is_eq())
                    .unwrap_or(Ordering::Equal)
            })
    }
}

impl PartialOrd for TsVector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TsVector {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for TsVector {}

pub fn to_tsvector(config: Config, text: &str) -> Result<SearchResult<TsVector>, TsError> {
    let mut notices = Vec::new();
    let mut entries: BTreeMap<String, Vec<Position>> = BTreeMap::new();
    for (index, word) in config::lexemes(config, text, &mut notices) {
        let index = index.min(usize::from(MAX_POS)) as u16;
        let positions = entries.entry(word).or_default();
        if positions.len() < MAX_POSITIONS - 1 && positions.last().is_none_or(|p| p.index < index) {
            positions.push(Position {
                index,
                weight: Weight::D,
            });
        }
    }
    let value = TsVector::new(
        entries
            .into_iter()
            .map(|(word, positions)| Lexeme { word, positions })
            .collect(),
    )?;
    Ok(SearchResult { value, notices })
}
