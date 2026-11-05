use crate::{MAX_GROUPS, Result, VOTERS};
use giggles_core::{Key, Row, RowId, TenantId, local::Reply, replica::Image};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const DIRECTORY: u64 = 1;
pub const LIMIT: usize = 4096;
pub fn minimum() -> Key {
    Key {
        tenant: TenantId(0),
        row: RowId(0),
    }
}
pub fn digest<T: Serialize>(value: &T) -> Result<String> {
    Ok(hex::encode(Sha256::digest(bincode::serialize(value)?)))
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bounds {
    pub start: Key,
    pub end: Option<Key>,
}
impl Bounds {
    pub fn valid(&self) -> bool {
        self.end.is_none_or(|end| self.start < end)
    }
    pub fn contains(&self, key: Key) -> bool {
        key >= self.start && self.end.is_none_or(|end| key < end)
    }
    pub fn contains_bounds(&self, other: &Self) -> bool {
        other.valid()
            && self.contains(other.start)
            && match (self.end, other.end) {
                (None, _) => true,
                (Some(_), None) => false,
                (Some(a), Some(b)) => b <= a,
            }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Descriptor {
    pub range: u64,
    pub epoch: u64,
    pub group: u64,
    pub bounds: Bounds,
    /// Fixed full-data voter placement, separate from ordering group identity.
    pub voters: Vec<u64>,
}
impl Descriptor {
    pub fn validate(&self) -> Result<()> {
        if self.range == 0
            || self.group <= DIRECTORY
            || self.epoch == 0
            || !self.bounds.valid()
            || self.voters != VOTERS
        {
            return Err("invalid descriptor/membership".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Split {
    pub id: u64,
    pub parent: Descriptor,
    pub key: Key,
    pub left: Descriptor,
    pub right: Descriptor,
}
impl Split {
    pub fn validate(&self) -> Result<()> {
        self.parent.validate()?;
        self.left.validate()?;
        self.right.validate()?;
        if self.id == 0
            || self.key <= self.parent.bounds.start
            || !self.parent.bounds.contains(self.key)
            || self.left.bounds
                != (Bounds {
                    start: self.parent.bounds.start,
                    end: Some(self.key),
                })
            || self.right.bounds
                != (Bounds {
                    start: self.key,
                    end: self.parent.bounds.end,
                })
            || self.left.epoch != self.parent.epoch.checked_add(1).ok_or("epoch overflow")?
            || self.right.epoch != self.left.epoch
            || BTreeSet::from([self.parent.range, self.left.range, self.right.range]).len() != 3
            || BTreeSet::from([self.parent.group, self.left.group, self.right.group]).len() != 3
        {
            return Err("invalid split".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intent {
    pub split: Split,
    pub published: bool,
    pub children: Option<[Ready; 2]>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Directory {
    pub revision: u64,
    pub routes: Vec<Descriptor>,
    #[serde(with = "map_pairs")]
    pub intents: BTreeMap<u64, Intent>,
}
impl Directory {
    pub fn validate(&self) -> Result<()> {
        if self.revision == 0
            || self.routes.is_empty()
            || self.routes.len() >= MAX_GROUPS
            || self.intents.len() >= MAX_GROUPS
            || self.routes[0].bounds.start != minimum()
            || self.routes.last().ok_or("empty")?.bounds.end.is_some()
        {
            return Err("directory bounds/limit".into());
        }
        let mut ranges = BTreeSet::new();
        let mut groups = BTreeSet::new();
        for (i, r) in self.routes.iter().enumerate() {
            r.validate()?;
            if !ranges.insert(r.range)
                || !groups.insert(r.group)
                || (i > 0 && self.routes[i - 1].bounds.end != Some(r.bounds.start))
            {
                return Err("directory overlap/gap/identity".into());
            }
        }
        for (id, intent) in &self.intents {
            intent.split.validate()?;
            if *id != intent.split.id || intent.published != intent.children.is_some() {
                return Err("invalid intent".into());
            }
            if let Some(ready) = &intent.children
                && (ready[0].child != intent.split.left
                    || ready[1].child != intent.split.right
                    || ready[0].fence != ready[1].fence
                    || ready[0].fence.split != intent.split
                    || ready[0].fence.index == 0
                    || ready.iter().any(|p| p.digest.len() != 64))
            {
                return Err("invalid publication provenance".into());
            }
        }
        Ok(())
    }
    pub fn route(&self, key: Key) -> Result<Descriptor> {
        self.routes
            .iter()
            .find(|r| r.bounds.contains(key))
            .cloned()
            .ok_or_else(|| "no current route".into())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fence {
    pub split: Split,
    pub index: u64,
    pub source_digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ready {
    pub child: Descriptor,
    pub fence: Fence,
    pub digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryRecord {
    pub key: Key,
    pub digest: String,
    pub version: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Data {
    pub descriptor: Descriptor,
    pub image: Image,
    pub fence: Option<Fence>,
    pub active: bool,
    pub provenance: Option<Ready>,
    /// Reserved durable guard. Future prepared participants must populate it;
    /// a split cannot omit unresolved locks or intents.
    pub prepared: BTreeSet<u64>,
    #[serde(with = "map_pairs")]
    pub retries: BTreeMap<u64, RetryRecord>,
}
mod map_pairs {
    use super::*;
    pub fn serialize<S: serde::Serializer, T: Serialize>(
        map: &BTreeMap<u64, T>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
        deserializer: D,
    ) -> std::result::Result<BTreeMap<u64, T>, D::Error> {
        let pairs = Vec::<(u64, T)>::deserialize(deserializer)?;
        let mut map = BTreeMap::new();
        for (key, value) in pairs {
            if map.insert(key, value).is_some() {
                return Err(serde::de::Error::custom("duplicate map id"));
            }
        }
        Ok(map)
    }
}
impl Data {
    pub fn check(&self, route: &Descriptor, bounds: &Bounds) -> Result<()> {
        if self.descriptor != *route || !route.bounds.contains_bounds(bounds) {
            return Err("stale epoch/group/predicate".into());
        }
        if !self.active {
            return Err("child not activated by publication".into());
        }
        if self.fence.is_some() {
            return Err("range fenced; refresh whole operation".into());
        }
        Ok(())
    }
    pub fn validate(&self, domain: u128) -> Result<()> {
        self.descriptor.validate()?;
        let rows = self.image.snapshot()?.rows;
        if self.image.domain()? != domain
            || self.image.row_partition(minimum(), None)? != self.image
            || self.image.snapshot()?.rows.len() > LIMIT
            || self.retries.len() > LIMIT
            || self.prepared.len() > LIMIT
            || self.retries.values().any(|r| {
                r.digest.len() != 64
                    || !self.descriptor.bounds.contains(r.key)
                    || rows.get(&r.key).is_none_or(|row| row.version < r.version)
            })
            || self
                .image
                .snapshot()?
                .rows
                .keys()
                .any(|key| !self.descriptor.bounds.contains(*key))
        {
            return Err("non-data namespace or out-of-bounds image".into());
        }
        if let Some(f) = &self.fence {
            f.split.validate()?;
            if f.split.parent != self.descriptor
                || f.index == 0
                || f.source_digest != digest(&(&self.image, &self.retries))?
            {
                return Err("invalid fence".into());
            }
        }
        if let Some(p) = &self.provenance {
            p.fence.split.validate()?;
            if p.child != self.descriptor
                || (p.child != p.fence.split.left && p.child != p.fence.split.right)
            {
                return Err("invalid provenance".into());
            }
        }
        Ok(())
    }
    pub fn children(&self) -> Result<[Data; 2]> {
        self.validate(self.image.domain()?)?;
        let fence = self.fence.clone().ok_or("parent not fenced")?;
        if !self.prepared.is_empty() {
            return Err("prepared work blocks split".into());
        }
        let make = |descriptor: Descriptor| -> Result<Data> {
            let image = self
                .image
                .row_partition(descriptor.bounds.start, descriptor.bounds.end)?;
            let retries = self
                .retries
                .iter()
                .filter(|(_, r)| descriptor.bounds.contains(r.key))
                .map(|(id, r)| (*id, r.clone()))
                .collect::<BTreeMap<_, _>>();
            let proof = Ready {
                child: descriptor.clone(),
                fence: fence.clone(),
                digest: digest(&(&image, &retries))?,
            };
            Ok(Data {
                descriptor,
                image,
                fence: None,
                active: false,
                provenance: Some(proof),
                prepared: BTreeSet::new(),
                retries,
            })
        };
        Ok([
            make(fence.split.left.clone())?,
            make(fence.split.right.clone())?,
        ])
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Machine {
    Vacant { domain: u128, group: u64 },
    Root { domain: u128, directory: Directory },
    Range { domain: u128, data: Box<Data> },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Command {
    pub domain: u128,
    pub group: u64,
    pub action: Action,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Action {
    Begin(Split),
    /// Trusted controller must obtain both proofs through current child barriers.
    Publish {
        id: u64,
        ready: Box<[Ready; 2]>,
    },
    Fence(Split),
    Bootstrap(Box<Data>),
    Put {
        route: Descriptor,
        operation: u64,
        key: Key,
        expected: Option<u64>,
        row: Row,
    },
    Activate {
        proof: Ready,
        revision: u64,
    },
    TaskWrite,
    Move,
}
impl Machine {
    pub fn domain(&self) -> Result<u128> {
        Ok(match self {
            Self::Vacant { domain, .. }
            | Self::Root { domain, .. }
            | Self::Range { domain, .. } => *domain,
        })
    }
    pub fn group(&self) -> u64 {
        match self {
            Self::Vacant { group, .. } => *group,
            Self::Root { .. } => DIRECTORY,
            Self::Range { data, .. } => data.descriptor.group,
        }
    }
    pub fn root(domain: u128, descriptor: Descriptor) -> Result<Self> {
        let m = Self::Root {
            domain,
            directory: Directory {
                revision: 1,
                routes: vec![descriptor],
                intents: BTreeMap::new(),
            },
        };
        m.validate(domain, DIRECTORY)?;
        Ok(m)
    }
    pub fn range(domain: u128, descriptor: Descriptor) -> Result<Self> {
        let m = Self::Range {
            domain,
            data: Box::new(Data {
                descriptor,
                image: Image::new(domain)?,
                fence: None,
                active: true,
                provenance: None,
                prepared: BTreeSet::new(),
                retries: BTreeMap::new(),
            }),
        };
        m.validate(domain, m.group())?;
        Ok(m)
    }
    pub fn validate(&self, domain: u128, group: u64) -> Result<()> {
        if self.domain()? != domain || self.group() != group || group == 0 {
            return Err("machine domain/group".into());
        }
        match self {
            Self::Root { directory, .. } => directory.validate(),
            Self::Range { data, .. } => data.validate(domain),
            Self::Vacant { group, .. } => {
                if *group <= DIRECTORY {
                    Err("invalid vacant group".into())
                } else {
                    Ok(())
                }
            }
        }
    }
    pub fn apply(&mut self, command: &Command, index: u64) -> Reply {
        let mut next = self.clone();
        match next.execute(command, index).and_then(|v| {
            next.validate(command.domain, command.group)?;
            Ok(v)
        }) {
            Ok(v) => {
                *self = next;
                Reply::ok(v)
            }
            Err(e) => Reply::error(400, e),
        }
    }
    fn execute(&mut self, c: &Command, index: u64) -> Result<serde_json::Value> {
        if c.domain != self.domain()? || c.group != self.group() {
            return Err("wrong command domain/group".into());
        }
        match (&mut *self, &c.action) {
            (Self::Root { directory, .. }, Action::Begin(split)) => {
                split.validate()?;
                if let Some(old) = directory.intents.get(&split.id) {
                    if old.split == *split {
                        return Ok(serde_json::json!({"id":split.id,"published":old.published}));
                    }
                    return Err("split id conflict".into());
                }
                if directory.route(split.key)? != split.parent
                    || directory
                        .intents
                        .values()
                        .any(|i| !i.published && i.split.parent.range == split.parent.range)
                    || directory.routes.len() + 2 >= MAX_GROUPS
                    || directory.intents.len() + 1 >= MAX_GROUPS
                {
                    return Err("split stale/conflict/limit".into());
                }
                let used = directory
                    .routes
                    .iter()
                    .map(|r| (r.range, r.group))
                    .chain(
                        directory
                            .intents
                            .values()
                            .flat_map(|i| [&i.split.parent, &i.split.left, &i.split.right])
                            .map(|r| (r.range, r.group)),
                    )
                    .collect::<Vec<_>>();
                if used.iter().any(|(r, g)| {
                    *r == split.left.range
                        || *r == split.right.range
                        || *g == split.left.group
                        || *g == split.right.group
                }) {
                    return Err("child identity reused".into());
                }
                directory.intents.insert(
                    split.id,
                    Intent {
                        split: split.clone(),
                        published: false,
                        children: None,
                    },
                );
            }
            (Self::Root { directory, .. }, Action::Publish { id, ready }) => {
                let intent = directory.intents.get(id).ok_or("unknown split")?.clone();
                if intent.published {
                    if intent.children.as_ref() == Some(ready.as_ref()) {
                        return Ok(serde_json::json!({"published":true}));
                    }
                    return Err("publication conflict".into());
                }
                if ready[0].child != intent.split.left
                    || ready[1].child != intent.split.right
                    || ready[0].fence != ready[1].fence
                    || ready[0].fence.split != intent.split
                    || ready[0].fence.index == 0
                    || ready.iter().any(|p| p.digest.len() != 64)
                {
                    return Err("children not matching fenced intent".into());
                }
                let at = directory
                    .routes
                    .iter()
                    .position(|r| *r == intent.split.parent)
                    .ok_or("parent route changed")?;
                directory
                    .routes
                    .splice(at..at + 1, [intent.split.left, intent.split.right]);
                directory.revision = directory
                    .revision
                    .checked_add(1)
                    .ok_or("revision overflow")?;
                let i = directory.intents.get_mut(id).ok_or("unknown split")?;
                i.published = true;
                i.children = Some(*ready.clone());
            }
            (Self::Range { data, .. }, Action::Fence(split)) => {
                split.validate()?;
                if let Some(f) = &data.fence {
                    if f.split == *split {
                        return Ok(serde_json::to_value(f)?);
                    }
                    return Err("parent already fenced".into());
                }
                if !data.active || data.descriptor != split.parent || !data.prepared.is_empty() {
                    return Err("parent epoch/prepared guard".into());
                }
                let f = Fence {
                    split: split.clone(),
                    index,
                    source_digest: digest(&(&data.image, &data.retries))?,
                };
                // Never fence an otherwise live parent into children that the
                // bounded proposal path cannot bootstrap. Validate the exact
                // fenced payload, including retained retry/provenance state.
                let mut candidate = data.as_ref().clone();
                candidate.fence = Some(f.clone());
                for child in candidate.children()? {
                    let bootstrap = Command {
                        domain: c.domain,
                        group: child.descriptor.group,
                        action: Action::Bootstrap(Box::new(child)),
                    };
                    if serde_json::to_vec(&bootstrap)?.len() > crate::MAX_COMMAND {
                        return Err(
                            "split bootstrap exceeds proposal capacity; parent stays active".into(),
                        );
                    }
                }
                data.fence = Some(f.clone());
                return Ok(serde_json::to_value(f)?);
            }
            (Self::Vacant { domain, group }, Action::Bootstrap(child)) => {
                child.validate(*domain)?;
                let p = child
                    .provenance
                    .as_ref()
                    .ok_or("bootstrap lacks provenance")?;
                if child.active
                    || child.descriptor.group != *group
                    || child.fence.is_some()
                    || !child.prepared.is_empty()
                    || p.digest != digest(&(&child.image, &child.retries))?
                    || p.fence.index == 0
                {
                    return Err("invalid child bootstrap".into());
                }
                *self = Self::Range {
                    domain: *domain,
                    data: child.clone(),
                };
            }
            (Self::Range { data, .. }, Action::Bootstrap(child)) => {
                if data.as_ref() != child.as_ref() {
                    return Err("bootstrap conflict or child already mutated".into());
                }
            }
            (Self::Range { data, .. }, Action::Activate { proof, revision }) => {
                if *revision < 2 || data.provenance.as_ref() != Some(proof) {
                    return Err("activation proof mismatch".into());
                }
                data.active = true;
            }
            (
                Self::Range { data, .. },
                Action::Put {
                    route,
                    operation,
                    key,
                    expected,
                    row,
                },
            ) => {
                data.check(
                    route,
                    &Bounds {
                        start: *key,
                        end: None,
                    }
                    .point(),
                )?;
                if *operation == 0 {
                    return Err("operation id required".into());
                }
                let hash = digest(&(key, expected, row))?;
                if let Some(old) = data.retries.get(operation) {
                    if old.key == *key && old.digest == hash {
                        return Ok(serde_json::json!({"version":old.version,"replay":true}));
                    }
                    return Err("operation id conflict".into());
                }
                let snapshot = data.image.snapshot()?;
                if snapshot.rows.get(key).map(|r| r.version) != *expected {
                    return Err("version conflict".into());
                }
                if data.retries.len() >= LIMIT
                    || (!snapshot.rows.contains_key(key) && snapshot.rows.len() >= LIMIT)
                {
                    return Err("range capacity".into());
                }
                let (image, reply) =
                    data.image.evaluate(&giggles_core::replica::Command::Admin(
                        serde_json::to_vec(&giggles_core::local::AdminCommand::Seed {
                            key: *key,
                            row: row.clone(),
                        })?,
                    ))?;
                if reply.code != 0 {
                    return Err("core mutation refused".into());
                }
                let version = image
                    .snapshot()?
                    .rows
                    .get(key)
                    .ok_or("core row missing")?
                    .version;
                data.image = image;
                data.retries.insert(
                    *operation,
                    RetryRecord {
                        key: *key,
                        digest: hash,
                        version,
                    },
                );
                return Ok(serde_json::json!({"version":version,"replay":false}));
            }
            (_, Action::TaskWrite) => {
                return Err("distributed task decision required; no effects".into());
            }
            (_, Action::Move) => return Err("safe membership move unsupported".into()),
            _ => return Err("command incompatible with group".into()),
        }
        Ok(serde_json::json!({"applied":true}))
    }
}
impl Bounds {
    fn point(mut self) -> Self {
        self.end = if self.start.row.0 < u64::MAX {
            Some(Key {
                tenant: self.start.tenant,
                row: RowId(self.start.row.0 + 1),
            })
        } else if self.start.tenant.0 < u64::MAX {
            Some(Key {
                tenant: TenantId(self.start.tenant.0 + 1),
                row: RowId(0),
            })
        } else {
            None
        };
        self
    }
}

/// Cache entries are hints only. Even a cached miss requires a current root read.
#[derive(Default)]
pub struct RouteCache {
    routes: Vec<Descriptor>,
    revision: u64,
}
impl RouteCache {
    pub fn refresh(&mut self, directory: &Directory) -> Result<()> {
        directory.validate()?;
        self.routes = directory.routes.clone();
        self.revision = directory.revision;
        Ok(())
    }
    pub fn hint(&self, key: Key) -> Option<Descriptor> {
        self.routes.iter().find(|r| r.bounds.contains(key)).cloned()
    }
    pub fn resolve(
        &mut self,
        key: Key,
        mut current: impl FnMut() -> Result<Directory>,
        mut accepts: impl FnMut(&Descriptor) -> bool,
    ) -> Result<Descriptor> {
        for _ in 0..3 {
            let d = current()?;
            self.refresh(&d)?;
            let route = d.route(key)?;
            if accepts(&route) {
                return Ok(route);
            }
        }
        Err("redirect limit; retry entire operation".into())
    }
}
