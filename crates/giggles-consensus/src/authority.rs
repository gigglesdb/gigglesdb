//! Internal authority primitives. Only trusted local provisioning may reload.
//! A listener must authenticate TLS itself, then use Node's session/decision/
//! release methods; it must never dispatch core Admin/Task/NativeTask directly.
use crate::{Result, TaskAction};
use giggles_core::{Principal, TaskId};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

pub const MAX_BINDINGS: usize = 256;
pub const MAX_INTERVAL: u64 = 125;
pub const DECISION_HORIZON: u64 = 5;
pub(crate) const PREFIX: &[u8] = b"GDAUTH02";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub fingerprint: String,
    #[serde(with = "domain_string")]
    pub domain: u128,
    pub task: TaskId,
    pub principal: Principal,
}
mod domain_string {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &u128, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<u128, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}
/// Opaque authenticated session. Not deserializable from network input.
#[derive(Clone, Debug)]
pub struct Session {
    pub(crate) binding: Binding,
    pub(crate) epoch: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authority {
    pub epoch: u64,
    pub bindings: BTreeMap<String, Binding>,
    pub watermark: u64,
    pub term: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Fence {
    pub binding: Binding,
    pub epoch: u64,
    pub term: u64,
    pub lower: u64,
    pub upper: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Entry {
    Reload {
        expected: u64,
        bindings: Vec<Binding>,
    },
    Time {
        term: u64,
        lower: u64,
        upper: u64,
    },
    Decide {
        fence: Fence,
        action: TaskAction,
    },
    Release {
        fence: Fence,
        decision: u64,
    },
}
impl Authority {
    pub fn validate(&self, domain: u128) -> Result<()> {
        let values = self.bindings.values().cloned().collect::<Vec<_>>();
        Self::validated(domain, &values)?;
        if self.bindings.iter().any(|(k, v)| k != &v.fingerprint) {
            return Err("binding key".into());
        }
        Ok(())
    }
    pub(crate) fn validated(
        domain: u128,
        bindings: &[Binding],
    ) -> Result<BTreeMap<String, Binding>> {
        if bindings.len() > MAX_BINDINGS {
            return Err("binding limit".into());
        }
        let mut map = BTreeMap::new();
        for b in bindings {
            if b.domain != domain
                || b.fingerprint.len() != 64
                || !b
                    .fingerprint
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                || map.insert(b.fingerprint.clone(), b.clone()).is_some()
            {
                return Err("invalid/duplicate credential binding".into());
            }
        }
        Ok(map)
    }
    pub(crate) fn session(&self, fingerprint: &str) -> Result<Session> {
        Ok(Session {
            binding: self
                .bindings
                .get(fingerprint)
                .ok_or("unbound credential")?
                .clone(),
            epoch: self.epoch,
        })
    }
    pub(crate) fn check(&self, f: &Fence, entry_term: u64) -> Result<()> {
        if f.epoch != self.epoch
            || self.bindings.get(&f.binding.fingerprint) != Some(&f.binding)
            || f.term != entry_term
            || self.term != f.term
            || f.lower > f.upper
            || f.upper - f.lower > MAX_INTERVAL
            || f.upper < self.watermark
        {
            return Err("stale authority/time/term".into());
        }
        Ok(())
    }
}

/// Inject both wall and monotonic elapsed time. OS accuracy and sample-to-quorum
/// decision delay <= DECISION_HORIZON are trusted operator assumptions, not a
/// guarantee of asynchronous Raft. Release always samples again.
pub trait ClockSource {
    fn sample(&mut self) -> Result<(u64, u64)>;
}
pub struct OsClock {
    start: Instant,
}
impl Default for OsClock {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}
impl ClockSource for OsClock {
    fn sample(&mut self) -> Result<(u64, u64)> {
        Ok((
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            self.start.elapsed().as_secs(),
        ))
    }
}
pub struct LeaderClock<C: ClockSource> {
    source: C,
    skew: u64,
    max_pause: u64,
    last: Option<(u64, u64)>,
    failed: bool,
}
impl<C: ClockSource> LeaderClock<C> {
    pub fn is_fenced(&self) -> bool {
        self.failed
    }
    pub fn new(source: C, skew: u64, max_pause: u64) -> Result<Self> {
        if skew > 60 || max_pause == 0 || max_pause > 3600 {
            return Err("clock bounds".into());
        }
        Ok(Self {
            source,
            skew,
            max_pause,
            last: None,
            failed: false,
        })
    }
    pub fn interval(&mut self, watermark: u64, horizon: u64) -> Result<(u64, u64)> {
        let result = (|| {
            if self.failed || horizon > DECISION_HORIZON {
                return Err("clock fenced".into());
            }
            let (wall, mono) = self.source.sample()?;
            if let Some((w, m)) = self.last {
                let elapsed = mono.checked_sub(m).ok_or("monotonic regression")?;
                let expected = w.checked_add(elapsed).ok_or("clock overflow")?;
                if wall < w || elapsed > self.max_pause || wall.abs_diff(expected) > self.skew + 1 {
                    return Err("clock discontinuity/pause".into());
                }
            }
            let lower = wall.checked_sub(self.skew).ok_or("clock underflow")?;
            let upper = wall
                .checked_add(self.skew)
                .and_then(|n| n.checked_add(horizon))
                .ok_or("clock overflow")?;
            // Do not manufacture a timestamp to catch up to a prior leader.
            if upper < watermark {
                return Err("clock behind durable watermark".into());
            }
            self.last = Some((wall, mono));
            Ok((lower, upper))
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}
