//! D-A1 positions and D-A2 journal frontiers. Timestamps are signed i128
//! nanoseconds since Unix epoch, encoded big endian. Empty token denotes root;
//! actual partition tokens are nonempty opaque bytes. Finished records are kept.
use crate::event::{EventError, PositionOrder};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt,
};

pub type Timestamp = i128;
pub type Token = Vec<u8>;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum State {
    Registered,
    Reading,
    Finished,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Partition {
    pub token: Token,
    pub parents: BTreeSet<Token>,
    pub start_ts: Timestamp,
    pub state: State,
    pub processed_ts: Timestamp,
}
impl Partition {
    pub fn registered(token: Token, parents: BTreeSet<Token>, start_ts: Timestamp) -> Self {
        Self {
            token,
            parents,
            start_ts,
            state: State::Registered,
            processed_ts: start_ts,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Encoding,
    Version(u8),
    MissingPartition(Token),
    InvalidTransition,
    Regression,
    ParentsBehind,
    ConflictingPartition,
    Capacity,
    Incomparable,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Spanner frontier: {self:?}")
    }
}
impl std::error::Error for Error {}
type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frontier {
    records: BTreeMap<Token, Partition>,
}
impl Frontier {
    pub fn new(snapshot_ts: Timestamp) -> Self {
        let root = Partition::registered(vec![], BTreeSet::new(), snapshot_ts);
        Self {
            records: [(vec![], root)].into(),
        }
    }
    pub fn records(&self) -> &BTreeMap<Token, Partition> {
        &self.records
    }
    /// D-A3/D-A3.1: inclusive restart, retaining read-ahead lineage.
    pub fn clamped(&self, boundary: Timestamp) -> Result<Self> {
        self.validate()?;
        self.resolved_ts()?;
        let mut next = self.clone();
        for p in next.records.values_mut() {
            if p.state == State::Finished && p.processed_ts > boundary {
                p.state = State::Reading;
            }
            p.processed_ts = p.processed_ts.min(boundary.max(p.start_ts));
        }
        let mut visited = BTreeSet::new();
        while visited.len() < next.records.len() {
            let ready: Vec<_> = next
                .records
                .values()
                .filter(|p| !visited.contains(&p.token) && p.parents.is_subset(&visited))
                .map(|p| p.token.clone())
                .collect();
            if ready.is_empty() {
                return Err(Error::Encoding);
            }
            for token in ready {
                let p = &next.records[&token];
                if p.state != State::Registered
                    && p.parents
                        .iter()
                        .any(|t| next.records[t].processed_ts < p.start_ts)
                {
                    let p = next.records.get_mut(&token).unwrap();
                    p.state = State::Registered;
                    p.processed_ts = p.start_ts;
                }
                visited.insert(token);
            }
        }
        next.validate()?;
        Ok(next)
    }
    fn partition(&self, token: &[u8]) -> Result<&Partition> {
        self.records
            .get(token)
            .ok_or_else(|| Error::MissingPartition(token.to_vec()))
    }
    /// Atomic in memory; persist the whole frontier marker before finish().
    /// Replayed registration is idempotent and never resets a child's progress.
    pub fn register_children(&mut self, parent: &[u8], children: Vec<Partition>) -> Result<()> {
        let p = self.partition(parent)?;
        if p.state != State::Reading || children.is_empty() {
            return Err(Error::InvalidTransition);
        }
        let mut next = self.clone();
        for c in children {
            if c.token.is_empty()
                || !c.parents.contains(parent)
                || c.parents.contains(&c.token)
                || c.start_ts < p.start_ts
                || c.state != State::Registered
                || c.processed_ts != c.start_ts
            {
                return Err(Error::InvalidTransition);
            }
            if let Some(old) = next.records.get(&c.token) {
                if (old.start_ts, &old.parents) != (c.start_ts, &c.parents) {
                    return Err(Error::ConflictingPartition);
                }
            } else {
                next.records.insert(c.token.clone(), c);
            }
        }
        next.validate()?;
        if let (Ok(Some(old)), Ok(Some(new))) = (self.resolved_ts(), next.resolved_ts())
            && new < old
        {
            return Err(Error::Regression);
        }
        *self = next;
        Ok(())
    }
    pub fn mark_reading(&mut self, token: &[u8]) -> Result<()> {
        let p = self.partition(token)?;
        if p.state != State::Registered {
            return Err(Error::InvalidTransition);
        }
        for parent in &p.parents {
            if self.partition(parent)?.processed_ts < p.start_ts {
                return Err(Error::ParentsBehind);
            }
        }
        self.records.get_mut(token).unwrap().state = State::Reading;
        Ok(())
    }
    pub fn advance(&mut self, token: &[u8], ts: Timestamp) -> Result<()> {
        let p = self.partition(token)?;
        if p.state != State::Reading {
            return Err(Error::InvalidTransition);
        }
        if ts < p.processed_ts {
            return Err(Error::Regression);
        }
        self.records.get_mut(token).unwrap().processed_ts = ts;
        Ok(())
    }
    pub fn finish(&mut self, parent: &[u8]) -> Result<()> {
        let p = self.partition(parent)?;
        let children: Vec<_> = self
            .records
            .values()
            .filter(|c| c.parents.contains(parent))
            .collect();
        if p.state != State::Reading || children.is_empty() {
            return Err(Error::InvalidTransition);
        }
        if children.iter().any(|c| p.processed_ts < c.start_ts) {
            return Err(Error::ParentsBehind);
        }
        self.resolved_ts()?;
        self.records.get_mut(parent).unwrap().state = State::Finished;
        Ok(())
    }
    pub fn resolved_ts(&self) -> Result<Option<Timestamp>> {
        for p in self.records.values() {
            for parent in &p.parents {
                self.partition(parent)?;
            }
        }
        Ok(self
            .records
            .values()
            .filter(|p| p.state != State::Finished)
            .map(|p| p.processed_ts)
            .min())
    }
    fn validate(&self) -> Result<()> {
        if self.records.is_empty() {
            return Err(Error::Encoding);
        }
        for p in self.records.values() {
            if p.processed_ts < p.start_ts
                || (p.token.is_empty() != p.parents.is_empty())
                || p.parents.contains(&p.token)
                || (p.state == State::Registered && p.processed_ts != p.start_ts)
            {
                return Err(Error::Encoding);
            }
            // Enforce an acyclic lineage even for equal start timestamps.
            let mut pending: Vec<_> = p.parents.iter().collect();
            let mut visited = BTreeSet::new();
            while let Some(token) = pending.pop() {
                if token == &p.token {
                    return Err(Error::Encoding);
                }
                if visited.insert(token)
                    && let Some(parent) = self.records.get(token)
                {
                    if parent.start_ts > p.start_ts {
                        return Err(Error::Encoding);
                    }
                    pending.extend(&parent.parents);
                }
            }
            if p.state == State::Finished
                && (!self.records.values().any(|c| c.parents.contains(&p.token))
                    || self
                        .records
                        .values()
                        .any(|c| c.parents.contains(&p.token) && c.start_ts > p.processed_ts))
            {
                return Err(Error::Encoding);
            }
            if p.state != State::Registered
                && p.parents.iter().any(|t| {
                    self.records
                        .get(t)
                        .is_none_or(|parent| parent.processed_ts < p.start_ts)
                })
            {
                return Err(Error::Encoding);
            }
        }
        Ok(())
    }
    fn dominates(&self, old: &Self) -> bool {
        old.records.iter().all(|(t, p)| {
            self.records.get(t).is_some_and(|n| {
                n.parents == p.parents
                    && n.start_ts == p.start_ts
                    && n.state >= p.state
                    && n.processed_ts >= p.processed_ts
            })
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpannerPosition {
    Txn {
        commit_ts: Timestamp,
        server_transaction_id: Vec<u8>,
    },
    Frontier(Frontier),
}
impl SpannerPosition {
    /// tag:u8 (1 txn, 2 frontier), version:u8=1; counts/byte lengths:u32;
    /// frontier records: token, parent count/parents, start:i128, state:u8,
    /// processed:i128. Tokens and parents are strictly bytewise sorted.
    pub fn encode(&self) -> Vec<u8> {
        fn bytes(out: &mut Vec<u8>, b: &[u8]) {
            out.extend((b.len() as u32).to_be_bytes());
            out.extend(b);
        }
        let mut out = vec![
            if matches!(self, Self::Txn { .. }) {
                1
            } else {
                2
            },
            1,
        ];
        match self {
            Self::Txn {
                commit_ts,
                server_transaction_id,
            } => {
                out.extend(commit_ts.to_be_bytes());
                bytes(&mut out, server_transaction_id);
            }
            Self::Frontier(f) => {
                out.extend((f.records.len() as u32).to_be_bytes());
                for p in f.records.values() {
                    bytes(&mut out, &p.token);
                    out.extend((p.parents.len() as u32).to_be_bytes());
                    for parent in &p.parents {
                        bytes(&mut out, parent);
                    }
                    out.extend(p.start_ts.to_be_bytes());
                    out.push(p.state as u8);
                    out.extend(p.processed_ts.to_be_bytes());
                }
            }
        }
        out
    }
    pub fn decode(input: &[u8]) -> Result<Self> {
        struct Read<'a>(&'a [u8]);
        impl<'a> Read<'a> {
            fn take(&mut self, n: usize) -> Result<&'a [u8]> {
                let b = self.0.get(..n).ok_or(Error::Encoding)?;
                self.0 = &self.0[n..];
                Ok(b)
            }
            fn count(&mut self) -> Result<usize> {
                Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as usize)
            }
            fn bytes(&mut self) -> Result<Vec<u8>> {
                let n = self.count()?;
                Ok(self.take(n)?.to_vec())
            }
            fn ts(&mut self) -> Result<Timestamp> {
                Ok(i128::from_be_bytes(self.take(16)?.try_into().unwrap()))
            }
        }
        let mut r = Read(input);
        let tag = r.take(1)?[0];
        let version = r.take(1)?[0];
        if version != 1 {
            return Err(Error::Version(version));
        }
        let value = match tag {
            1 => Self::Txn {
                commit_ts: r.ts()?,
                server_transaction_id: r.bytes()?,
            },
            2 => {
                let mut records = BTreeMap::new();
                for _ in 0..r.count()? {
                    let token = r.bytes()?;
                    let mut parents = BTreeSet::new();
                    for _ in 0..r.count()? {
                        if !parents.insert(r.bytes()?) {
                            return Err(Error::Encoding);
                        }
                    }
                    let start_ts = r.ts()?;
                    let state = match r.take(1)?[0] {
                        0 => State::Registered,
                        1 => State::Reading,
                        2 => State::Finished,
                        _ => return Err(Error::Encoding),
                    };
                    let p = Partition {
                        token: token.clone(),
                        parents,
                        start_ts,
                        state,
                        processed_ts: r.ts()?,
                    };
                    if records.insert(token, p).is_some() {
                        return Err(Error::Encoding);
                    }
                }
                let frontier = Frontier { records };
                frontier.validate()?;
                Self::Frontier(frontier)
            }
            _ => return Err(Error::Encoding),
        };
        if !r.0.is_empty() || value.encode() != input {
            return Err(Error::Encoding);
        }
        Ok(value)
    }
}
pub struct SpannerOrder {
    domain: String,
}
impl SpannerOrder {
    pub fn new(
        project: &str,
        instance: &str,
        database: &str,
        stream: &str,
        snapshot_ts: Timestamp,
    ) -> Result<Self> {
        if [project, instance, database, stream]
            .iter()
            .any(|s| s.is_empty() || s.contains('/'))
        {
            return Err(Error::Encoding);
        }
        Ok(Self {
            domain: format!("spanner:{project}/{instance}/{database}/{stream}/{snapshot_ts}"),
        })
    }
    fn compare_positions(a: SpannerPosition, b: SpannerPosition) -> Result<Ordering> {
        use SpannerPosition::*;
        match (a, b) {
            (
                Txn {
                    commit_ts: a,
                    server_transaction_id: ai,
                },
                Txn {
                    commit_ts: b,
                    server_transaction_id: bi,
                },
            ) => Ok((a, ai).cmp(&(b, bi))),
            (Frontier(a), Frontier(b)) => {
                a.resolved_ts()?;
                b.resolved_ts()?;
                if a == b {
                    Ok(Ordering::Equal)
                } else if b.dominates(&a) {
                    Ok(Ordering::Less)
                } else if a.dominates(&b) {
                    Ok(Ordering::Greater)
                } else {
                    Err(Error::Incomparable)
                }
            }
            (Txn { commit_ts, .. }, Frontier(f)) => Ok(
                if commit_ts <= f.resolved_ts()?.ok_or(Error::InvalidTransition)? {
                    Ordering::Less
                } else {
                    Ordering::Greater
                },
            ),
            (Frontier(f), t @ Txn { .. }) => Ok(Self::compare_positions(t, Frontier(f))?.reverse()),
        }
    }
}
impl PositionOrder for SpannerOrder {
    fn domain(&self) -> &str {
        &self.domain
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
        Self::compare_positions(
            SpannerPosition::decode(a).map_err(event_error)?,
            SpannerPosition::decode(b).map_err(event_error)?,
        )
        .map_err(event_error)
    }
}
fn event_error(error: Error) -> EventError {
    match error {
        Error::Version(v) => EventError::UnsupportedVersion(v.into()),
        Error::MissingPartition(_) => EventError::InvalidEncoding("missing Spanner partition"),
        Error::Incomparable => EventError::InvalidEncoding("incomparable Spanner frontiers"),
        _ => EventError::InvalidEncoding("invalid Spanner position"),
    }
}

/// At most `capacity` identities, each at most `max_identity_bytes` bytes.
/// Overflow blocks admission, never evicts an unacknowledged identity. Rebuild
/// with restore() from buffered record identities above the durable frontier;
/// records at/below fully processed timestamps are already known duplicates.
/// Retain those manifests until the authoritative journal marker covers them.
pub struct DuplicateFilter {
    seen: BTreeSet<(Vec<u8>, Vec<u8>)>,
    capacity: usize,
    max_identity_bytes: usize,
    pub drops: u64,
}
impl DuplicateFilter {
    pub fn restore(
        capacity: usize,
        max_identity_bytes: usize,
        identities: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Result<Self> {
        let mut filter = Self {
            seen: BTreeSet::new(),
            capacity,
            max_identity_bytes,
            drops: 0,
        };
        for (txn, sequence) in identities {
            filter.admit(txn, sequence)?;
        }
        Ok(filter)
    }
    /// Identity-only admission for inclusive replay, including several records at
    /// the same processed timestamp. Restore buffered identities on restart.
    pub fn admit(&mut self, txn: Vec<u8>, sequence: Vec<u8>) -> Result<bool> {
        if txn.len().saturating_add(sequence.len()) > self.max_identity_bytes {
            return Err(Error::Capacity);
        }
        if self.seen.contains(&(txn.clone(), sequence.clone())) {
            self.drops = self.drops.saturating_add(1);
            return Ok(false);
        }
        if self.seen.len() == self.capacity {
            return Err(Error::Capacity);
        }
        self.seen.insert((txn, sequence));
        Ok(true)
    }
    pub fn observe(
        &mut self,
        durable: &Frontier,
        token: &[u8],
        ts: Timestamp,
        txn: Vec<u8>,
        sequence: Vec<u8>,
    ) -> Result<bool> {
        durable.resolved_ts()?;
        if ts <= durable.partition(token)?.processed_ts {
            self.drops = self.drops.saturating_add(1);
            return Ok(false);
        }
        self.admit(txn, sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn child(token: &[u8], parents: &[&[u8]], ts: Timestamp) -> Partition {
        Partition::registered(
            token.to_vec(),
            parents.iter().map(|p| p.to_vec()).collect(),
            ts,
        )
    }
    fn txn(ts: Timestamp, id: &[u8]) -> SpannerPosition {
        SpannerPosition::Txn {
            commit_ts: ts,
            server_transaction_id: id.to_vec(),
        }
    }
    fn compare(
        a: SpannerPosition,
        b: SpannerPosition,
    ) -> std::result::Result<Ordering, EventError> {
        SpannerOrder::new("p", "i", "d", "s", 0)
            .unwrap()
            .compare(&a.encode(), &b.encode())
    }
    fn split() -> Frontier {
        let mut f = Frontier::new(0);
        f.mark_reading(b"").unwrap();
        f.advance(b"", 10).unwrap();
        f.register_children(b"", vec![child(b"a", &[b""], 10), child(b"b", &[b""], 10)])
            .unwrap();
        f
    }
    #[test]
    fn encoding_roundtrip_and_rejections() {
        for p in [txn(-123, b"id"), SpannerPosition::Frontier(split())] {
            let bytes = p.encode();
            assert_eq!(SpannerPosition::decode(&bytes).unwrap(), p);
            for n in 0..bytes.len() {
                assert!(SpannerPosition::decode(&bytes[..n]).is_err());
            }
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert!(SpannerPosition::decode(&trailing).is_err());
            let mut bad = bytes.clone();
            bad[0] = 99;
            assert!(SpannerPosition::decode(&bad).is_err());
            bad = bytes.clone();
            bad[1] = 2;
            assert_eq!(SpannerPosition::decode(&bad), Err(Error::Version(2)));
        }
        let mut f = split();
        f.mark_reading(b"a").unwrap();
        f.advance(b"a", 20).unwrap();
        f.register_children(b"a", vec![child(b"m", &[b"a", b"b"], 20)])
            .unwrap();
        let bytes = SpannerPosition::Frontier(f).encode();
        // Swap sorted parent values without changing their lengths.
        let needle = [0, 0, 0, 2, 0, 0, 0, 1, b'a', 0, 0, 0, 1, b'b'];
        let at = bytes
            .windows(needle.len())
            .position(|w| w == needle)
            .unwrap();
        let mut bad = bytes;
        bad.swap(at + 8, at + 13);
        assert!(SpannerPosition::decode(&bad).is_err());
        let mut bytes = SpannerPosition::Frontier(split()).encode();
        // Root record is 41 bytes; both one-parent child records are 46 bytes.
        let start = 6 + 41;
        let (a, b) = (
            bytes[start..start + 46].to_vec(),
            bytes[start + 46..].to_vec(),
        );
        bytes.truncate(start);
        bytes.extend(b);
        bytes.extend(a);
        assert!(SpannerPosition::decode(&bytes).is_err());
        let mut bad = split();
        bad.finish(b"").unwrap();
        bad.records.get_mut(b"b".as_slice()).unwrap().start_ts = 11;
        bad.records.get_mut(b"b".as_slice()).unwrap().processed_ts = 11;
        assert!(SpannerPosition::decode(&SpannerPosition::Frontier(bad).encode()).is_err());
    }
    #[test]
    fn all_position_comparison_rules() {
        use Ordering::*;
        assert_eq!(compare(txn(1, b"z"), txn(2, b"a")).unwrap(), Less);
        assert_eq!(compare(txn(2, b"a"), txn(2, b"b")).unwrap(), Less);
        assert_eq!(compare(txn(2, b"a"), txn(2, b"a")).unwrap(), Equal);
        let f = split();
        let fp = SpannerPosition::Frontier(f.clone());
        assert_eq!(compare(fp.clone(), fp.clone()).unwrap(), Equal);
        for ts in [9, 10, 11] {
            let expected = if ts <= 10 { Less } else { Greater };
            assert_eq!(compare(txn(ts, b"x"), fp.clone()).unwrap(), expected);
            assert_eq!(
                compare(fp.clone(), txn(ts, b"x")).unwrap(),
                expected.reverse()
            );
        }
        let mut next = f.clone();
        next.finish(b"").unwrap();
        next.mark_reading(b"a").unwrap();
        assert_eq!(
            compare(fp.clone(), SpannerPosition::Frontier(next.clone())).unwrap(),
            Less
        );
        assert_eq!(
            compare(SpannerPosition::Frontier(next.clone()), fp).unwrap(),
            Greater
        );
        let mut other = f;
        other.mark_reading(b"b").unwrap();
        other.advance(b"b", 11).unwrap();
        assert!(
            compare(
                SpannerPosition::Frontier(next),
                SpannerPosition::Frontier(other)
            )
            .is_err()
        );
        let order = SpannerOrder::new("p", "i", "d", "s", 0).unwrap();
        assert_eq!(order.domain(), "spanner:p/i/d/s/0");
        assert!(SpannerOrder::new("p/i", "i", "d", "s", 0).is_err());
    }
    #[test]
    fn split_merge_idle_heartbeats_and_transition_errors() {
        let mut f = Frontier::new(0);
        assert_eq!(f.advance(b"", 1), Err(Error::InvalidTransition));
        assert!(f.finish(b"").is_err());
        f.mark_reading(b"").unwrap();
        assert!(f.mark_reading(b"").is_err());
        assert_eq!(f.advance(b"", -1), Err(Error::Regression));
        assert!(f.mark_reading(b"absent").is_err());
        let mut f = split();
        f.finish(b"").unwrap();
        assert!(f.advance(b"", 11).is_err());
        for t in [b"a", b"b"] {
            f.mark_reading(t).unwrap();
        }
        f.advance(b"a", 20).unwrap();
        assert_eq!(f.resolved_ts().unwrap(), Some(10));
        f.register_children(b"a", vec![child(b"m", &[b"a", b"b"], 20)])
            .unwrap();
        assert_eq!(f.mark_reading(b"m"), Err(Error::ParentsBehind));
        assert!(f.finish(b"b").is_err());
        f.advance(b"b", 20).unwrap();
        f.mark_reading(b"m").unwrap();
        f.advance(b"m", 21).unwrap();
        f.register_children(b"a", vec![child(b"m", &[b"a", b"b"], 20)])
            .unwrap();
        assert_eq!(f.records()[b"m".as_slice()].processed_ts, 21);
        assert_eq!(
            f.register_children(b"a", vec![child(b"m", &[b"a"], 20)]),
            Err(Error::ConflictingPartition)
        );
        f.finish(b"a").unwrap();
        f.finish(b"b").unwrap();
        assert_eq!(f.resolved_ts().unwrap(), Some(21));
        assert!(f.finish(b"a").is_err());
        assert!(f.register_children(b"a", vec![]).is_err());
    }
    #[test]
    fn missing_partition_blocks_and_registration_is_atomic() {
        let mut f = split();
        let before = f.clone();
        assert!(
            f.register_children(b"", vec![child(b"c", &[b""], 10), child(b"a", &[b""], 11)])
                .is_err()
        );
        assert_eq!(f, before);
        f.mark_reading(b"a").unwrap();
        f.register_children(b"a", vec![child(b"m", &[b"a", b"missing"], 10)])
            .unwrap();
        assert_eq!(
            f.resolved_ts(),
            Err(Error::MissingPartition(b"missing".to_vec()))
        );
        assert!(compare(txn(0, b"x"), SpannerPosition::Frontier(f.clone())).is_err());
        assert_eq!(
            f.mark_reading(b"m"),
            Err(Error::MissingPartition(b"missing".to_vec()))
        );
        assert!(f.finish(b"a").is_err());
    }
    #[test]
    fn clamp_read_ahead_lineage_needs_a_child_state_decision() {
        for finished in [false, true] {
            let mut f = Frontier::new(0);
            f.mark_reading(b"").unwrap();
            f.register_children(
                b"",
                vec![child(b"branch", &[b""], 0), child(b"slow", &[b""], 0)],
            )
            .unwrap();
            f.finish(b"").unwrap();
            f.mark_reading(b"branch").unwrap();
            f.advance(b"branch", 10).unwrap();
            f.register_children(b"branch", vec![child(b"a", &[b"branch"], 10)])
                .unwrap();
            f.finish(b"branch").unwrap();
            f.mark_reading(b"a").unwrap();
            f.advance(b"a", 30).unwrap();
            if finished {
                f.register_children(b"a", vec![child(b"next", &[b"a"], 30)])
                    .unwrap();
                f.finish(b"a").unwrap();
                f.mark_reading(b"next").unwrap();
                f.advance(b"next", 40).unwrap();
            }
            f.mark_reading(b"slow").unwrap();
            f.advance(b"slow", 5).unwrap();
            f.validate().unwrap();
            assert_eq!(f.resolved_ts().unwrap(), Some(5));

            let clamped = f.clamped(5).unwrap();
            assert_eq!(clamped.records.len(), f.records.len());
            assert_eq!(clamped.records[b"branch".as_slice()].state, State::Reading);
            assert_eq!(clamped.records[b"a".as_slice()].state, State::Registered);
            assert_eq!(clamped.records[b"a".as_slice()].processed_ts, 10);
            if finished {
                assert_eq!(clamped.records[b"next".as_slice()].state, State::Registered);
                assert_eq!(clamped.records[b"next".as_slice()].processed_ts, 30);
            }
            let encoded = SpannerPosition::Frontier(clamped.clone()).encode();
            assert_eq!(
                SpannerPosition::decode(&encoded).unwrap(),
                SpannerPosition::Frontier(clamped.clone())
            );
            for boundary in [5, 10, 20, 30] {
                let next = f.clamped(boundary).unwrap();
                assert!(matches!(
                    compare(
                        SpannerPosition::Frontier(clamped.clone()),
                        SpannerPosition::Frontier(next)
                    ),
                    Ok(Ordering::Less | Ordering::Equal)
                ));
            }
            assert_eq!(
                f.clamped(10).unwrap().records[b"branch".as_slice()].state,
                State::Finished
            );
        }
    }
    #[test]
    fn duplicate_count_restore_and_memory_bound() {
        let f = Frontier::new(10);
        let key = (b"id".to_vec(), b"seq".to_vec());
        let mut d = DuplicateFilter::restore(1, 8, []).unwrap();
        assert!(
            d.observe(&f, b"", 11, key.0.clone(), key.1.clone())
                .unwrap()
        );
        assert!(
            !d.observe(&f, b"", 11, key.0.clone(), key.1.clone())
                .unwrap()
        );
        assert_eq!(d.drops, 1);
        assert_eq!(
            d.observe(&f, b"", 12, b"new".to_vec(), vec![]),
            Err(Error::Capacity)
        );
        let mut d = DuplicateFilter::restore(1, 8, [key.clone()]).unwrap();
        assert!(!d.observe(&f, b"", 11, key.0, key.1).unwrap());
        assert!(!d.observe(&f, b"", 10, b"other".to_vec(), vec![]).unwrap());
        assert_eq!(d.drops, 2);
        assert!(DuplicateFilter::restore(1, 1, [(b"long".to_vec(), vec![])]).is_err());
    }
}
