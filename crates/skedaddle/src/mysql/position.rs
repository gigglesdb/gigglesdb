//! Exact GTID interval algebra and versioned MySQL reconnect positions.
use crate::event::{EventError, PositionOrder};
use std::{cmp::Ordering, collections::BTreeMap, fmt, str::FromStr};

type Result<T> = std::result::Result<T, EventError>;
type Intervals = Vec<(u64, u64)>;
const INVALID: EventError = EventError::InvalidEncoding("invalid MySQL position or GTID set");

fn uuid(text: &str) -> Result<String> {
    if text.len() != 36
        || !text.bytes().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                c == b'-'
            } else {
                c.is_ascii_hexdigit()
            }
        })
    {
        return Err(INVALID);
    }
    Ok(text.to_ascii_lowercase())
}

fn tag(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 32
        && (text.as_bytes()[0].is_ascii_alphabetic() || text.starts_with('_'))
        && text.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
}

/// Canonical disjoint inclusive intervals keyed by lowercase UUID and tag.
/// Untagged transactions and each distinct tag have independent number spaces.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GtidSet(BTreeMap<(String, String), Intervals>);

impl FromStr for GtidSet {
    type Err = EventError;
    fn from_str(text: &str) -> Result<Self> {
        let mut set = Self::default();
        if text.trim().is_empty() {
            return Ok(set);
        }
        for group in text.split(',') {
            let mut parts = group.split(':').map(str::trim);
            let source = uuid(parts.next().ok_or(INVALID)?)?;
            let mut current_tag = String::new();
            let mut needs_interval = true;
            for part in parts {
                if tag(part) {
                    if needs_interval && !current_tag.is_empty() {
                        return Err(INVALID);
                    }
                    current_tag = part.to_ascii_lowercase();
                    needs_interval = true;
                    continue;
                }
                let number = |s: &str| -> Result<u64> {
                    if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
                        return Err(INVALID);
                    }
                    let n = s.parse::<u64>().map_err(|_| INVALID)?;
                    if n == 0 || n > i64::MAX as u64 {
                        return Err(INVALID);
                    }
                    Ok(n)
                };
                let (start, end) = part.split_once('-').unwrap_or((part, part));
                let (start, end) = (number(start.trim())?, number(end.trim())?);
                if start > end {
                    return Err(INVALID);
                }
                set.0
                    .entry((source.clone(), current_tag.clone()))
                    .or_default()
                    .push((start, end));
                needs_interval = false;
            }
            if needs_interval {
                return Err(INVALID);
            }
        }
        set.normalize();
        Ok(set)
    }
}

impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut last_uuid = "";
        for ((source, tag), intervals) in &self.0 {
            if source != last_uuid {
                if !last_uuid.is_empty() {
                    f.write_str(",")?;
                }
                f.write_str(source)?;
                last_uuid = source;
            }
            if !tag.is_empty() {
                write!(f, ":{tag}")?;
            }
            for (start, end) in intervals {
                write!(f, ":{start}")?;
                if start != end {
                    write!(f, "-{end}")?;
                }
            }
        }
        Ok(())
    }
}

impl GtidSet {
    fn normalize(&mut self) {
        for intervals in self.0.values_mut() {
            intervals.sort_unstable();
            let mut merged: Intervals = Vec::new();
            for &(start, end) in intervals.iter() {
                if let Some(last) = merged.last_mut().filter(|last| start <= last.1 + 1) {
                    last.1 = last.1.max(end);
                } else {
                    merged.push((start, end));
                }
            }
            *intervals = merged;
        }
    }

    /// Whether this set includes the single GTID specified in MySQL text form.
    pub fn contains(&self, gtid: &str) -> Result<bool> {
        Ok(Self::singleton(gtid)?.is_subset(self))
    }

    fn singleton(gtid: &str) -> Result<Self> {
        let one: Self = gtid.parse()?;
        if one.0.len() != 1 || !one.0.values().all(|v| v.len() == 1 && v[0].0 == v[0].1) {
            return Err(INVALID);
        }
        Ok(one)
    }

    pub fn add(&mut self, gtid: &str) -> Result<()> {
        *self = self.union(&Self::singleton(gtid)?);
        Ok(())
    }

    pub fn is_subset(&self, other: &Self) -> bool {
        self.0.iter().all(|(key, intervals)| {
            other.0.get(key).is_some_and(|superset| {
                let mut i = 0;
                intervals.iter().all(|&(start, end)| {
                    while i < superset.len() && superset[i].1 < start {
                        i += 1;
                    }
                    i < superset.len() && superset[i].0 <= start && superset[i].1 >= end
                })
            })
        })
    }

    pub fn union(&self, other: &Self) -> Self {
        let mut result = self.clone();
        for (key, intervals) in &other.0 {
            result.0.entry(key.clone()).or_default().extend(intervals);
        }
        result.normalize();
        result
    }

    pub fn difference(&self, other: &Self) -> Self {
        let mut result = Self::default();
        for (key, intervals) in &self.0 {
            let remove = other.0.get(key).map(Vec::as_slice).unwrap_or_default();
            let mut kept = Vec::new();
            let mut i = 0;
            for &(start, end) in intervals {
                let mut cursor = start;
                while i < remove.len() && remove[i].1 < cursor {
                    i += 1;
                }
                let mut j = i;
                while j < remove.len() && remove[j].0 <= end {
                    let (lo, hi) = remove[j];
                    if cursor < lo {
                        kept.push((cursor, lo - 1));
                    }
                    cursor = cursor.max(hi + 1);
                    if cursor > end {
                        break;
                    }
                    j += 1;
                }
                if cursor <= end {
                    kept.push((cursor, end));
                }
                i = j;
            }
            if !kept.is_empty() {
                result.0.insert(key.clone(), kept);
            }
        }
        result
    }

    pub fn compare(&self, other: &Self) -> Result<Ordering> {
        match (self.is_subset(other), other.is_subset(self)) {
            (true, true) => Ok(Ordering::Equal),
            (true, false) => Ok(Ordering::Less),
            (false, true) => Ok(Ordering::Greater),
            (false, false) => Err(EventError::InvalidEncoding("incomparable MySQL GTID sets")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MysqlPosition {
    Gtid {
        set: GtidSet,
        server_uuid: String,
        file: String,
        offset: u64,
    },
    File {
        server_uuid: String,
        file_seq: u64,
        offset: u64,
    },
}

impl MysqlPosition {
    /// Wire v1: G/F tag, version byte, lowercase UUID (36 ASCII bytes), then
    /// G: canonical set and file (u32 BE length + UTF-8), offset (u64 BE);
    /// F: file sequence and offset (both u64 BE). No legacy fallback.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let (kind, server) = match self {
            Self::Gtid { server_uuid, .. } => (b'G', server_uuid),
            Self::File { server_uuid, .. } => (b'F', server_uuid),
        };
        let mut out = vec![kind, 1];
        out.extend(uuid(server)?.bytes());
        match self {
            Self::Gtid {
                set, file, offset, ..
            } => {
                for text in [set.to_string().as_str(), file.as_str()] {
                    out.extend(
                        u32::try_from(text.len())
                            .map_err(|_| INVALID)?
                            .to_be_bytes(),
                    );
                    out.extend(text.bytes());
                }
                out.extend(offset.to_be_bytes());
            }
            Self::File {
                file_seq, offset, ..
            } => {
                out.extend(file_seq.to_be_bytes());
                out.extend(offset.to_be_bytes());
            }
        }
        Ok(out)
    }

    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
            let field = bytes.get(..n).ok_or(INVALID)?;
            *bytes = &bytes[n..];
            Ok(field)
        }
        fn number(bytes: &mut &[u8]) -> Result<u64> {
            Ok(u64::from_be_bytes(
                take(bytes, 8)?.try_into().map_err(|_| INVALID)?,
            ))
        }
        fn string(bytes: &mut &[u8]) -> Result<String> {
            let n = u32::from_be_bytes(take(bytes, 4)?.try_into().map_err(|_| INVALID)?) as usize;
            Ok(std::str::from_utf8(take(bytes, n)?)
                .map_err(|_| INVALID)?
                .into())
        }
        let kind = take(&mut bytes, 1)?[0];
        let version = take(&mut bytes, 1)?[0];
        if version != 1 {
            return Err(EventError::UnsupportedVersion(version.into()));
        }
        let server = std::str::from_utf8(take(&mut bytes, 36)?).map_err(|_| INVALID)?;
        let server_uuid = uuid(server)?;
        if server_uuid != server {
            return Err(INVALID);
        }
        let position = match kind {
            b'G' => {
                let text = string(&mut bytes)?;
                let set: GtidSet = text.parse()?;
                if set.to_string() != text {
                    return Err(INVALID);
                }
                Self::Gtid {
                    set,
                    server_uuid,
                    file: string(&mut bytes)?,
                    offset: number(&mut bytes)?,
                }
            }
            b'F' => Self::File {
                server_uuid,
                file_seq: number(&mut bytes)?,
                offset: number(&mut bytes)?,
            },
            _ => return Err(INVALID),
        };
        if !bytes.is_empty() {
            return Err(INVALID);
        }
        Ok(position)
    }
}

/// GTID domains use a stable source/cluster identity across failover. File domains
/// use the server UUID itself: file sequences cannot be compared across servers.
#[derive(Clone, Debug)]
pub struct MysqlOrder {
    domain: String,
    server: Option<String>,
}

impl MysqlOrder {
    pub fn gtid(source_id: &str) -> Result<Self> {
        if source_id.is_empty() {
            return Err(INVALID);
        }
        Ok(Self {
            domain: format!("mysql:{source_id}/gtid"),
            server: None,
        })
    }

    pub fn file(server_uuid: &str) -> Result<Self> {
        let server = uuid(server_uuid)?;
        Ok(Self {
            domain: format!("mysql:{server}/file"),
            server: Some(server),
        })
    }
}

impl PositionOrder for MysqlOrder {
    fn domain(&self) -> &str {
        &self.domain
    }
    fn compare(&self, left: &[u8], right: &[u8]) -> Result<Ordering> {
        match (
            MysqlPosition::decode(left)?,
            MysqlPosition::decode(right)?,
            &self.server,
        ) {
            (MysqlPosition::Gtid { set: a, .. }, MysqlPosition::Gtid { set: b, .. }, None) => {
                a.compare(&b)
            }
            (
                MysqlPosition::File {
                    server_uuid: a,
                    file_seq: af,
                    offset: ao,
                },
                MysqlPosition::File {
                    server_uuid: b,
                    file_seq: bf,
                    offset: bo,
                },
                Some(server),
            ) if a == b && &a == server => Ok((af, ao).cmp(&(bf, bo))),
            _ => Err(EventError::PositionDomainMismatch),
        }
    }
}

#[cfg(test)]
mod tests;
