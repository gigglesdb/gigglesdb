use crate::{Error, Result};
use bincode::Options;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Hard caps bound complete scans and serialized state, including acked IDs.
pub const MAX_EFFECTS: usize = 128;
pub const MAX_STATE_BYTES: usize = 48 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub effects: usize,
    pub payload_bytes: usize,
    pub batch: usize,
    pub statements: usize,
    pub sql_bytes: usize,
    pub uncertainty: u64,
    pub max_ttl: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            effects: 128,
            payload_bytes: 16 * 1024,
            batch: 32,
            statements: 64,
            sql_bytes: 64 * 1024,
            uncertainty: 10,
            max_ttl: 86_400,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        if self.effects == 0
            || self.effects > MAX_EFFECTS
            || self.payload_bytes == 0
            || self.payload_bytes > 16 * 1024
            || self.batch == 0
            || self.batch > 32
            || self.statements == 0
            || self.statements > 64
            || self.sql_bytes == 0
            || self.sql_bytes > 64 * 1024
            || self.max_ttl == 0
        {
            return Err(Error::Limit);
        }
        Ok(())
    }
}

/// Caller-certified shared monotonic time interval in an application-defined epoch.
/// Certification must survive restart. It is not derived from a local wall clock.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedTime {
    pub lower: u64,
    pub upper: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct EnqueueIntent<'a> {
    pub stream: &'a str,
    pub effect_id: &'a str,
    pub destination_kind: &'a str,
    pub payload: &'a [u8],
}

/// Immutable enqueue identity. A value returned while staging is provisional;
/// database acceptance is established only by Committed or a later status read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnqueueReceipt {
    pub stream: String,
    pub effect_id: String,
    pub digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimToken {
    pub stream: String,
    pub effect_id: String,
    pub owner: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    Ready,
    Claimed { owner: String, deadline: u64 },
    Acked,
}

/// Payload-free status; this is not a delivery authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectStatus {
    pub receipt: EnqueueReceipt,
    pub phase: Phase,
    pub generation: u64,
    pub attempts: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delivery {
    pub token: ClaimToken,
    pub payload: Vec<u8>,
    pub destination_kind: String,
    pub deadline: u64,
    pub attempt: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reconciliation {
    pub ready: usize,
    pub claimed: usize,
    pub acked: usize,
    pub payload_bytes: usize,
    pub retained_ids: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Effect {
    pub receipt: EnqueueReceipt,
    pub destination: String,
    pub payload: Vec<u8>,
    pub phase: Phase,
    pub generation: u64,
    pub attempts: u64,
}
impl Effect {
    pub fn status(&self) -> EffectStatus {
        EffectStatus {
            receipt: self.receipt.clone(),
            phase: self.phase.clone(),
            generation: self.generation,
            attempts: self.attempts,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct State {
    pub format: u32,
    pub limits: Limits,
    pub watermark: u64,
    pub effects: Vec<Effect>,
}

pub(crate) fn identifier(s: &str) -> Result<()> {
    if s.is_empty()
        || s.len() > 64
        || !s
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-.:".contains(&c))
    {
        return Err(Error::Invalid);
    }
    Ok(())
}
pub(crate) fn digest(i: EnqueueIntent<'_>) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"giggles-outbox-v1\0");
    for field in [
        i.stream.as_bytes(),
        i.effect_id.as_bytes(),
        i.destination_kind.as_bytes(),
        i.payload,
    ] {
        hash.update((field.len() as u64).to_be_bytes());
        hash.update(field);
    }
    hash.finalize().into()
}
fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .with_limit(MAX_STATE_BYTES as u64)
        .reject_trailing_bytes()
}
impl State {
    pub fn new(limits: Limits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            format: 1,
            limits,
            watermark: 0,
            effects: vec![],
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        codec().serialize(self).map_err(|_| Error::Limit)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_STATE_BYTES {
            return Err(Error::Corrupt);
        }
        let state: Self = codec().deserialize(bytes).map_err(|_| Error::Corrupt)?;
        state.validate().map_err(|_| Error::Corrupt)?;
        if state.encode().map_err(|_| Error::Corrupt)? != bytes {
            return Err(Error::Corrupt);
        }
        Ok(state)
    }
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        if self.format != 1 || self.effects.len() > self.limits.effects {
            return Err(Error::Limit);
        }
        let mut total = 0usize;
        let mut prev = None;
        for e in &self.effects {
            identifier(&e.receipt.stream)?;
            identifier(&e.receipt.effect_id)?;
            identifier(&e.destination)?;
            if e.payload.len() > MAX_PAYLOAD_BYTES || e.generation != e.attempts {
                return Err(Error::Corrupt);
            }
            let key = (&e.receipt.stream, &e.receipt.effect_id);
            if prev.is_some_and(|p| p >= key) {
                return Err(Error::Corrupt);
            }
            prev = Some(key);
            total = total.checked_add(e.payload.len()).ok_or(Error::Overflow)?;
            let expected = digest(EnqueueIntent {
                stream: &e.receipt.stream,
                effect_id: &e.receipt.effect_id,
                destination_kind: &e.destination,
                payload: &e.payload,
            });
            if expected != e.receipt.digest {
                return Err(Error::Corrupt);
            }
            match &e.phase {
                Phase::Claimed { owner, deadline } => {
                    identifier(owner)?;
                    if e.generation == 0 || *deadline == 0 {
                        return Err(Error::Corrupt);
                    }
                }
                Phase::Acked if e.generation == 0 => return Err(Error::Corrupt),
                _ => (),
            }
        }
        if total > self.limits.payload_bytes {
            return Err(Error::Limit);
        }
        // Admission reserves the largest lifecycle representation for every ID.
        // Filling the queue cannot consume the bytes needed for claim/ack later.
        let mut largest = self.clone();
        for e in &mut largest.effects {
            e.phase = Phase::Claimed {
                owner: "x".repeat(64),
                deadline: u64::MAX,
            };
        }
        if codec()
            .serialized_size(&largest)
            .map_err(|_| Error::Limit)?
            > MAX_STATE_BYTES as u64
        {
            return Err(Error::Limit);
        }
        Ok(())
    }
    pub fn index(&self, stream: &str, id: &str) -> std::result::Result<usize, usize> {
        self.effects.binary_search_by(|e| {
            (e.receipt.stream.as_str(), e.receipt.effect_id.as_str()).cmp(&(stream, id))
        })
    }
    pub fn enqueue(&mut self, intents: &[EnqueueIntent<'_>]) -> Result<Vec<EnqueueReceipt>> {
        if intents.len() > self.limits.batch {
            return Err(Error::Limit);
        }
        // Preflight before cloning or copying payloads.
        for i in intents {
            identifier(i.stream)?;
            identifier(i.effect_id)?;
            identifier(i.destination_kind)?;
            if i.payload.len() > MAX_PAYLOAD_BYTES {
                return Err(Error::Limit);
            }
        }
        let mut next = self.clone();
        let mut receipts = Vec::with_capacity(intents.len());
        for &i in intents {
            let receipt = EnqueueReceipt {
                stream: i.stream.into(),
                effect_id: i.effect_id.into(),
                digest: digest(i),
            };
            match next.index(i.stream, i.effect_id) {
                Ok(n) => {
                    let old = &next.effects[n];
                    if old.receipt != receipt
                        || old.destination != i.destination_kind
                        || old.payload != i.payload
                    {
                        return Err(Error::Conflict);
                    }
                }
                Err(n) => {
                    if next.effects.len() == next.limits.effects {
                        return Err(Error::Limit);
                    }
                    next.effects.insert(
                        n,
                        Effect {
                            receipt: receipt.clone(),
                            destination: i.destination_kind.into(),
                            payload: i.payload.into(),
                            phase: Phase::Ready,
                            generation: 0,
                            attempts: 0,
                        },
                    );
                }
            }
            receipts.push(receipt);
        }
        next.encode()?;
        *self = next;
        Ok(receipts)
    }
    pub fn time(&mut self, now: TrustedTime) -> Result<()> {
        if now.lower > now.upper
            || now.lower < self.watermark
            || now.upper - now.lower > self.limits.uncertainty
        {
            return Err(Error::Time);
        }
        self.watermark = now.lower;
        Ok(())
    }
    pub fn deadline(&self, now: TrustedTime, ttl: u64) -> Result<u64> {
        if ttl == 0 || ttl > self.limits.max_ttl || ttl <= now.upper - now.lower {
            return Err(Error::Time);
        }
        now.upper.checked_add(ttl).ok_or(Error::Overflow)
    }
    pub fn claim(
        &mut self,
        stream: &str,
        owner: &str,
        now: TrustedTime,
        ttl: u64,
        limit: usize,
    ) -> Result<Vec<Delivery>> {
        identifier(stream)?;
        identifier(owner)?;
        if limit == 0 || limit > self.limits.batch {
            return Err(Error::Limit);
        }
        let mut next = self.clone();
        next.time(now)?;
        let deadline = next.deadline(now, ttl)?;
        let mut result = Vec::with_capacity(limit);
        for e in &mut next.effects {
            if e.receipt.stream != stream {
                continue;
            }
            let eligible = matches!(e.phase, Phase::Ready)
                || matches!(e.phase,Phase::Claimed{deadline,..} if deadline <= now.lower);
            if !eligible {
                continue;
            }
            e.generation = e.generation.checked_add(1).ok_or(Error::Overflow)?;
            e.attempts = e.attempts.checked_add(1).ok_or(Error::Overflow)?;
            e.phase = Phase::Claimed {
                owner: owner.into(),
                deadline,
            };
            result.push(Delivery {
                token: ClaimToken {
                    stream: stream.into(),
                    effect_id: e.receipt.effect_id.clone(),
                    owner: owner.into(),
                    generation: e.generation,
                },
                payload: e.payload.clone(),
                destination_kind: e.destination.clone(),
                deadline,
                attempt: e.attempts,
            });
            if result.len() == limit {
                break;
            }
        }
        next.encode()?;
        *self = next;
        Ok(result)
    }
    pub fn resolve(&mut self, token: &ClaimToken, now: TrustedTime, action: Action) -> Result<()> {
        identifier(&token.stream)?;
        identifier(&token.effect_id)?;
        identifier(&token.owner)?;
        let mut next = self.clone();
        next.time(now)?;
        let renewed = if let Action::Renew(ttl) = action {
            Some(next.deadline(now, ttl)?)
        } else {
            None
        };
        let n = next
            .index(&token.stream, &token.effect_id)
            .map_err(|_| Error::StaleClaim)?;
        let e = &mut next.effects[n];
        let Phase::Claimed { owner, deadline } = &e.phase else {
            return Err(Error::StaleClaim);
        };
        if owner != &token.owner || e.generation != token.generation || now.upper >= *deadline {
            return Err(Error::StaleClaim);
        }
        e.phase = match action {
            Action::Renew(_) => Phase::Claimed {
                owner: owner.clone(),
                deadline: renewed.unwrap().max(*deadline),
            },
            Action::Release => Phase::Ready,
            Action::Ack => Phase::Acked,
        };
        next.encode()?;
        *self = next;
        Ok(())
    }
    pub fn reconcile(&self) -> Reconciliation {
        let mut r = Reconciliation {
            ready: 0,
            claimed: 0,
            acked: 0,
            payload_bytes: 0,
            retained_ids: self.effects.len(),
        };
        for e in &self.effects {
            r.payload_bytes += e.payload.len();
            match e.phase {
                Phase::Ready => r.ready += 1,
                Phase::Claimed { .. } => r.claimed += 1,
                Phase::Acked => r.acked += 1,
            }
        }
        r
    }
}
#[derive(Clone, Copy)]
pub(crate) enum Action {
    Renew(u64),
    Release,
    Ack,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn intent<'a>(id: &'a str, payload: &'a [u8]) -> EnqueueIntent<'a> {
        EnqueueIntent {
            stream: "s",
            effect_id: id,
            destination_kind: "local",
            payload,
        }
    }
    #[test]
    fn limits_reject_zero_and_hard_max() {
        for effects in [0, MAX_EFFECTS + 1] {
            assert_eq!(
                State::new(Limits {
                    effects,
                    ..Limits::default()
                }),
                Err(Error::Limit)
            );
        }
    }
    #[test]
    fn digest_separates_fields() {
        let a = intent("a", b"bc");
        let b = intent("ab", b"c");
        assert_ne!(digest(a), digest(b));
        assert_eq!(digest(a), digest(a));
    }
    #[test]
    fn codec_rejects_trailing_corruption() {
        let mut s = State::new(Limits::default()).unwrap();
        s.enqueue(&[intent("a", b"b")]).unwrap();
        let bytes = s.encode().unwrap();
        assert_eq!(State::decode(&bytes).unwrap(), s);
        let mut bad = bytes.clone();
        bad.push(0);
        assert_eq!(State::decode(&bad), Err(Error::Corrupt));
        let mut bad = bytes;
        *bad.last_mut().unwrap() ^= 1;
        assert!(State::decode(&bad).is_err());
    }
    #[test]
    fn lifecycle_overflow_fails_closed() {
        let mut s = State::new(Limits::default()).unwrap();
        s.enqueue(&[intent("a", b"b")]).unwrap();
        s.effects[0].generation = u64::MAX;
        s.effects[0].attempts = u64::MAX;
        let before = s.clone();
        assert_eq!(
            s.claim("s", "o", TrustedTime { lower: 0, upper: 0 }, 1, 1),
            Err(Error::Overflow)
        );
        assert_eq!(s, before);
        s.effects[0].generation = 0;
        s.effects[0].attempts = 0;
        let before = s.clone();
        assert_eq!(
            s.claim(
                "s",
                "o",
                TrustedTime {
                    lower: u64::MAX,
                    upper: u64::MAX
                },
                1,
                1
            ),
            Err(Error::Overflow)
        );
        assert_eq!(s, before);
    }
    #[test]
    fn uncertain_time_refused() {
        let mut s = State::new(Limits::default()).unwrap();
        assert_eq!(
            s.claim(
                "s",
                "o",
                TrustedTime {
                    lower: 0,
                    upper: 11
                },
                20,
                1
            ),
            Err(Error::Time)
        );
        assert_eq!(
            s.claim("s", "o", TrustedTime { lower: 5, upper: 6 }, 1, 1),
            Err(Error::Time)
        );
        assert_eq!(s.watermark, 0);
    }
    #[test]
    fn lifecycle_headroom_reserved_at_admission() {
        let mut s = State::new(Limits::default()).unwrap();
        let stream = "s".repeat(64);
        let destination = "d".repeat(64);
        let payload = [1u8; 128];
        let mut admitted = 0;
        for n in 0..128 {
            let id = format!("{n:064}");
            match s.enqueue(&[EnqueueIntent {
                stream: &stream,
                effect_id: &id,
                destination_kind: &destination,
                payload: &payload,
            }]) {
                Ok(_) => admitted += 1,
                Err(Error::Limit) => break,
                other => panic!("{other:?}"),
            }
        }
        assert!(admitted > 0 && admitted < 128);
        let mut claimed = 0;
        while claimed < admitted {
            let ds = s
                .claim(
                    &stream,
                    &"o".repeat(64),
                    TrustedTime { lower: 1, upper: 1 },
                    10,
                    32,
                )
                .unwrap();
            assert!(!ds.is_empty());
            claimed += ds.len();
            for d in ds {
                s.resolve(&d.token, TrustedTime { lower: 1, upper: 1 }, Action::Ack)
                    .unwrap();
            }
        }
        assert_eq!(s.reconcile().acked, admitted);
    }
    #[test]
    fn codec_rejects_invalid_lifecycle_digest_duplicates_and_version() {
        let mut valid = State::new(Limits::default()).unwrap();
        valid.enqueue(&[intent("a", b"b")]).unwrap();
        let mut cases = vec![];
        let mut s = valid.clone();
        s.format = 2;
        cases.push(s);
        let mut s = valid.clone();
        s.effects[0].receipt.digest[0] ^= 1;
        cases.push(s);
        let mut s = valid.clone();
        s.effects.push(s.effects[0].clone());
        cases.push(s);
        let mut s = valid.clone();
        s.effects[0].phase = Phase::Acked;
        cases.push(s);
        let mut s = valid;
        s.effects[0].phase = Phase::Claimed {
            owner: "bad owner".into(),
            deadline: 1,
        };
        cases.push(s);
        for s in cases {
            let bytes = codec().serialize(&s).unwrap();
            assert_eq!(State::decode(&bytes), Err(Error::Corrupt));
        }
    }
}
