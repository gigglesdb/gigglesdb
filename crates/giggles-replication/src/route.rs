//! Route cache of range descriptors and leader hints (GD-0075 R5-23). Entries are hints: a
//! range leader rejects a stale epoch, and the client refreshes from the system range.
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use giggles_core::range_api::{RangeDescriptor, RangeId};

/// Descriptor end in range-key order; `Max` (end = None) sorts last.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum End {
    Key(Vec<u8>),
    Max,
}
impl End {
    fn of(desc: &RangeDescriptor) -> Self {
        desc.end.clone().map_or(Self::Max, Self::Key)
    }
}

/// Non-overlapping cached descriptors, indexed by end key and by id, plus routing and leader hints.
/// Not synchronised; the routed client keeps it under a mutex.
#[derive(Debug, Default)]
pub struct RouteCache {
    by_end: BTreeMap<End, RangeDescriptor>,
    ends: HashMap<RangeId, End>,
    leaders: HashMap<RangeId, u64>,
    // Id-only hints: never consulted by key lookup or by_id. Clear the whole map at
    // 4096 entries rather than tracking retirement order; these are best-effort hints.
    retired: HashMap<RangeId, RangeDescriptor>,
}

impl RouteCache {
    /// Cached descriptor containing `range_key`.
    pub fn lookup(&self, range_key: &[u8]) -> Option<RangeDescriptor> {
        let after = Bound::Excluded(End::Key(range_key.to_vec()));
        let (_, desc) = self.by_end.range((after, Bound::Unbounded)).next()?;
        desc.contains(range_key).then(|| desc.clone())
    }

    /// Caches a fresher descriptor: ignored if a newer epoch of the same id is cached, else it
    /// replaces the same id and drops every cached descriptor overlapping its span (epochs of
    /// different ranges are not comparable; the inserted one is the newest information).
    /// Overlapping descriptors remain as bounded, id-only routing hints; the inserted id's
    /// retired hint is removed.
    pub fn insert(&mut self, desc: RangeDescriptor) {
        if self
            .by_id(desc.id)
            .is_some_and(|cached| cached.epoch > desc.epoch)
        {
            return;
        }
        self.remove(desc.id);
        self.retired.remove(&desc.id);
        let after = Bound::Excluded(End::Key(desc.start.clone()));
        let overlapping: Vec<RangeId> = self
            .by_end
            .range((after, Bound::Unbounded))
            .take_while(|(_, cached)| desc.end.as_ref().is_none_or(|end| cached.start < *end))
            .map(|(_, cached)| cached.id)
            .collect();
        for id in overlapping {
            self.retire(id);
        }
        self.ends.insert(desc.id, End::of(&desc));
        self.by_end.insert(End::of(&desc), desc);
    }

    /// Removes `stale` from key routing and `by_id` if the cached epoch matches.
    /// Keeps the removed descriptor as a bounded, id-only routing hint.
    pub fn invalidate(&mut self, stale: &RangeDescriptor) {
        if self
            .by_id(stale.id)
            .is_some_and(|cached| cached.epoch == stale.epoch)
        {
            self.retire(stale.id);
        }
    }

    /// Cached descriptor of a range.
    pub fn by_id(&self, range: RangeId) -> Option<&RangeDescriptor> {
        self.by_end.get(self.ends.get(&range)?)
    }

    /// Last known descriptor for id-addressed routing, including retired descriptors.
    /// Retired spans are not authoritative for key routing.
    pub fn route_hint(&self, range: RangeId) -> Option<&RangeDescriptor> {
        self.by_id(range).or_else(|| self.retired.get(&range))
    }

    /// Sets or clears the leader hint of a range.
    pub fn set_leader(&mut self, range: RangeId, node: Option<u64>) {
        match node {
            Some(node) => self.leaders.insert(range, node),
            None => self.leaders.remove(&range),
        };
    }

    /// Leader hint of a range.
    pub fn leader(&self, range: RangeId) -> Option<u64> {
        self.leaders.get(&range).copied()
    }

    fn retire(&mut self, range: RangeId) {
        if let Some(desc) = self.remove(range) {
            if self.retired.len() >= 4096 {
                self.retired.clear();
            }
            self.retired.insert(range, desc);
        }
    }

    fn remove(&mut self, range: RangeId) -> Option<RangeDescriptor> {
        let end = self.ends.remove(&range)?;
        self.by_end.remove(&end)
    }
}
