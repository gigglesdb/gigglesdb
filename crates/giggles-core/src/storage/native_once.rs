//! Private, versioned application retry storage. Repair receipts remain separate.
use super::*;
use serde::de::{MapAccess, SeqAccess, Visitor};
use std::{
    fmt,
    time::{Duration, Instant},
};

mod receipt;
mod v2;
mod v2_migrate;
#[cfg(feature = "engine-rocksdb")]
mod v2_publish;
#[cfg(feature = "engine-rocksdb")]
mod v2_compact;

struct LedgerView<'a> {
    domain: u128,
    ledger: Option<&'a NativeOutcomes>,
    /// A v2 store whose ledger header `26 00` exists: identities live in
    /// per-identity `22` records (GD-0061 R3-625) and `ledger` is unused.
    shared: Option<SharedLedger<'a>>,
}
struct SharedLedger<'a> {
    db: &'a crate::storage::v2::V2Db,
    header: crate::task::records::LedgerHeader,
}
impl LedgerView<'_> {
    fn borrow(state: &State) -> LedgerView<'_> {
        LedgerView { domain: state.view.domain, ledger: state.native_outcomes.as_ref(), shared: None }
    }
}
use receipt::certify_lease;
#[cfg(feature = "engine-rocksdb")]
use receipt::certify_watermark;

pub(crate) const MAX_NATIVE_IDENTITY_BYTES: usize = 512;
pub(crate) const MAX_NATIVE_RESULT_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_NATIVE_RECORDS: usize = 1024;
pub(crate) const MAX_NATIVE_LEDGER_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_NATIVE_RETENTION_SECONDS: u64 = 604800;
pub(crate) const MAX_NATIVE_CLOCK_LEASE_SECONDS: u64 = 30;
const LEDGER_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, serde::Deserialize)]
pub(crate) struct NativeOnceKey {
    pub database_domain: u128,
    #[serde(deserialize_with = "decode_identity")]
    pub identity: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeClockLease {
    pub lower_unix_seconds: u64,
    pub upper_unix_seconds: u64,
    pub valid_until: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
pub(crate) struct NativeOnceRecord {
    pub submitted_fingerprint: [u8; 32],
    pub accepted_fingerprint: [u8; 32],
    pub retry_until_seconds: u64,
    #[serde(deserialize_with = "decode_optional_result")]
    pub result: Option<Vec<u8>>,
}

#[derive(Debug)]
pub(crate) enum NativeOnceAdmission {
    Replay(NativeOnceRecord),
    Owner(NativeOnceOwner),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum NativeOnceLookup {
    Absent,
    InProgress,
    Retained(NativeOnceRecord),
    Expired { retry_until_seconds: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativePublishMode {
    Success,
    Terminal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum NativeOnceError {
    Core(CoreError),
    DomainMismatch,
    InProgress,
    IdempotencyConflict,
    ClockUncertain,
    RetentionUnavailable,
}
impl From<CoreError> for NativeOnceError {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}
pub(crate) type NativeResult<T> = Result<T, NativeOnceError>;

#[derive(Clone, Serialize, serde::Deserialize)]
struct Entry {
    record: NativeOnceRecord,
    retention_seconds: u64,
    decided_lower_unix_seconds: u64,
    decided_upper_unix_seconds: u64,
}

#[derive(Clone, Serialize, serde::Deserialize)]
pub(crate) struct NativeOutcomes {
    version: u16,
    upper_watermark_unix_seconds: u64,
    #[serde(deserialize_with = "decode_entries")]
    entries: BTreeMap<NativeOnceKey, Entry>,
}
impl Default for NativeOutcomes {
    fn default() -> Self {
        Self {
            version: LEDGER_VERSION,
            upper_watermark_unix_seconds: 0,
            entries: BTreeMap::new(),
        }
    }
}

fn bad() -> CoreError {
    CoreError::Corrupt("native outcome invariant".into())
}
impl NativeOutcomes {
    pub(super) fn contains_temporal(&self) -> Result<bool, CoreError> {
        for e in self.entries.values() {
            if let Some(b) = &e.record.result
                && crate::operations::once::outcome::contains_temporal(b)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    /// Any retained GDOU version 2 receipt (feature bit WIDE_ONCE_RECEIPTS).
    pub(crate) fn contains_wide(&self) -> bool {
        self.entries.values().any(|e| {
            e.record
                .result
                .as_deref()
                .is_some_and(crate::operations::once::outcome::is_wide)
        })
    }
    pub(super) fn contains_typed(&self) -> Result<bool, CoreError> {
        for e in self.entries.values() {
            if let Some(b) = &e.record.result
                && crate::operations::once::outcome::contains_typed(b)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn validate(&self) -> Result<(), CoreError> {
        if self.version != LEDGER_VERSION {
            return Err(CoreError::Unsupported);
        }
        if self.entries.len() > MAX_NATIVE_RECORDS {
            return Err(CoreError::Limit);
        }
        for (key, entry) in &self.entries {
            if key.identity.is_empty()
                || key.identity.len() > MAX_NATIVE_IDENTITY_BYTES
                || !(1..=MAX_NATIVE_RETENTION_SECONDS).contains(&entry.retention_seconds)
                || entry.decided_lower_unix_seconds > entry.decided_upper_unix_seconds
                || entry.decided_upper_unix_seconds > self.upper_watermark_unix_seconds
                || entry
                    .decided_upper_unix_seconds
                    .checked_add(entry.retention_seconds)
                    != Some(entry.record.retry_until_seconds)
                || entry
                    .record
                    .result
                    .as_ref()
                    .is_some_and(|b| b.len() > MAX_NATIVE_RESULT_BYTES)
            {
                return Err(bad());
            }
        }
        if codec().serialized_size(self).map_err(|_| bad())? > MAX_NATIVE_LEDGER_BYTES as u64 {
            return Err(CoreError::Limit);
        }
        for entry in self.entries.values() {
            if let Some(payload) = &entry.record.result {
                crate::operations::once::outcome::validate(payload)?;
            }
        }
        Ok(())
    }
    pub(super) fn from_bytes(bytes: &[u8]) -> Result<Self, CoreError> {
        if bytes.len() > MAX_NATIVE_LEDGER_BYTES {
            return Err(CoreError::Limit);
        }
        let version = bytes.get(..2).ok_or_else(bad)?;
        if u16::from_be_bytes(version.try_into().expect("length")) != LEDGER_VERSION {
            return Err(CoreError::Unsupported);
        }
        let ledger: Self = decode(bytes)?;
        ledger.validate()?;
        if encode(&ledger)? != bytes {
            return Err(bad());
        }
        Ok(ledger)
    }
    pub(super) fn to_bytes(&self) -> Result<Vec<u8>, CoreError> {
        self.validate()?;
        encode(self)
    }
}

fn decode_bytes<'de, D: serde::Deserializer<'de>, const N: usize>(
    d: D,
) -> Result<Vec<u8>, D::Error> {
    struct Bytes<const N: usize>;
    impl<'de, const N: usize> Visitor<'de> for Bytes<N> {
        type Value = Vec<u8>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "at most {N} bytes")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            if seq.size_hint().is_some_and(|n| n > N) {
                return Err(serde::de::Error::custom("native byte limit"));
            }
            let mut result = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(N));
            while let Some(byte) = seq.next_element()? {
                if result.len() == N {
                    return Err(serde::de::Error::custom("native byte limit"));
                }
                result.push(byte);
            }
            Ok(result)
        }
    }
    d.deserialize_seq(Bytes::<N>)
}
fn decode_identity<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    decode_bytes::<D, MAX_NATIVE_IDENTITY_BYTES>(d)
}
fn decode_optional_result<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<Vec<u8>>, D::Error> {
    struct Optional;
    impl<'de> Visitor<'de> for Optional {
        type Value = Option<Vec<u8>>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("bounded native result or tombstone")
        }
        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            decode_bytes::<D, MAX_NATIVE_RESULT_BYTES>(d).map(Some)
        }
    }
    d.deserialize_option(Optional)
}
fn decode_entries<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<NativeOnceKey, Entry>, D::Error> {
    struct Entries;
    impl<'de> Visitor<'de> for Entries {
        type Value = BTreeMap<NativeOnceKey, Entry>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("bounded unique native identities")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            if map.size_hint().is_some_and(|n| n > MAX_NATIVE_RECORDS) {
                return Err(serde::de::Error::custom("native record limit"));
            }
            let mut result = BTreeMap::new();
            while let Some(key) = map.next_key()? {
                if result.len() == MAX_NATIVE_RECORDS || result.contains_key(&key) {
                    return Err(serde::de::Error::custom("native record limit or duplicate"));
                }
                result.insert(key, map.next_value()?);
            }
            Ok(result)
        }
    }
    d.deserialize_map(Entries)
}

pub(super) struct InFlight {
    submitted: [u8; 32],
    token: Arc<()>,
}
pub(crate) struct NativeOnceOwner {
    inner: Arc<Inner>,
    key: NativeOnceKey,
    submitted: [u8; 32],
    token: Arc<()>,
}
impl fmt::Debug for NativeOnceOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeOnceOwner").finish_non_exhaustive()
    }
}
impl Drop for NativeOnceOwner {
    fn drop(&mut self) {
        // Cleanup must work after cancellation. No publication gate is acquired.
        let mut owners = self
            .inner
            .native_once_owners
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if owners
            .get(&self.key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.token, &self.token))
        {
            owners.remove(&self.key);
        }
    }
}

fn check_key(state: &LedgerView<'_>, key: &NativeOnceKey) -> NativeResult<()> {
    if state.domain != key.database_domain {
        return Err(NativeOnceError::DomainMismatch);
    }
    if key.identity.is_empty() || key.identity.len() > MAX_NATIVE_IDENTITY_BYTES {
        return Err(NativeOnceError::RetentionUnavailable);
    }
    // On the shared ledger a once identity must not take a task or undo
    // identity's tag, so the two kinds can never share a `22` key.
    if state.shared.is_some() && v2_migrate::reserved_identity(&key.identity) {
        return Err(NativeOnceError::RetentionUnavailable);
    }
    Ok(())
}
fn retained(state: &LedgerView<'_>, key: &NativeOnceKey) -> NativeResult<Option<NativeOnceRecord>> {
    match &state.shared {
        Some(shared) => Ok(v2_migrate::shared_record(shared.db, key)?),
        None => Ok(state.ledger.and_then(|ledger| ledger.retained(key)).cloned()),
    }
}
/// Identities counted against [`MAX_NATIVE_RECORDS`]; blanked ones included.
fn identities(state: &LedgerView<'_>) -> usize {
    match &state.shared {
        Some(shared) => usize::try_from(shared.header.once_identities).unwrap_or(usize::MAX),
        None => state.ledger.map_or(0, |ledger| ledger.entries.len()),
    }
}
fn replay(record: &NativeOnceRecord, submitted: [u8; 32]) -> NativeResult<NativeOnceRecord> {
    if record.submitted_fingerprint != submitted {
        return Err(NativeOnceError::IdempotencyConflict);
    }
    Ok(record.clone())
}
fn certify(
    state: &State,
    clock: &mut dyn FnMut() -> NativeResult<NativeClockLease>,
) -> NativeResult<NativeClockLease> {
    certify_lease(state.native_outcomes.as_ref(), clock()?)
}

fn persistence(error: GuardedPersistError) -> NativeOnceError {
    match error {
        GuardedPersistError::Core(CoreError::Limit) => NativeOnceError::RetentionUnavailable,
        GuardedPersistError::Core(error) => NativeOnceError::Core(error),
        GuardedPersistError::ClockExpired => NativeOnceError::ClockUncertain,
    }
}
impl Inner {
    fn native_once_locked<T>(
        &self,
        work: impl FnOnce(&mut State) -> NativeResult<T>,
    ) -> NativeResult<T> {
        let _ticket = self.enter()?;
        let _guard = crate::cancellation::lock_notified(&self.gate, || {
            self.decision_point(DecisionPoint::WaitingLock)
        })?;
        if self.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced.into());
        }
        let mut state = self.read().inspect_err(|_| {
            self.fenced.store(true, Ordering::Release);
        })?;
        work(&mut state)
    }
}

impl Admin {
    pub(crate) fn admit_native_once_engine(
        &self,
        key: NativeOnceKey,
        submitted_fingerprint: [u8; 32],
    ) -> NativeResult<NativeOnceAdmission> {
        self.inner.native_once_view(|state| {
            check_key(state, &key)?;
            if let Some(record) = retained(state, &key)? {
                return replay(&record, submitted_fingerprint).map(NativeOnceAdmission::Replay);
            }
            let mut owners = crate::cancellation::lock(&self.inner.native_once_owners)?;
            if let Some(owner) = owners.get(&key) {
                return Err(if owner.submitted == submitted_fingerprint {
                    NativeOnceError::InProgress
                } else {
                    NativeOnceError::IdempotencyConflict
                });
            }
            if owners.len() >= self.inner.limits.admitted {
                return Err(CoreError::Busy.into());
            }
            if identities(state).saturating_add(owners.len()) >= MAX_NATIVE_RECORDS {
                return Err(NativeOnceError::RetentionUnavailable);
            }
            let token = Arc::new(());
            owners.insert(
                key.clone(),
                InFlight {
                    submitted: submitted_fingerprint,
                    token: token.clone(),
                },
            );
            Ok(NativeOnceAdmission::Owner(NativeOnceOwner {
                inner: self.inner.clone(),
                key,
                submitted: submitted_fingerprint,
                token,
            }))
        })
    }

    pub(crate) fn lookup_native_once_engine(
        &self,
        key: &NativeOnceKey,
    ) -> NativeResult<NativeOnceLookup> {
        self.inner.native_once_view(|state| {
            check_key(state, key)?;
            if let Some(record) = retained(state, key)? {
                return Ok(if record.result.is_some() {
                    NativeOnceLookup::Retained(record)
                } else {
                    NativeOnceLookup::Expired {
                        retry_until_seconds: record.retry_until_seconds,
                    }
                });
            }
            Ok(
                if crate::cancellation::lock(&self.inner.native_once_owners)?.contains_key(key) {
                    NativeOnceLookup::InProgress
                } else {
                    NativeOnceLookup::Absent
                },
            )
        })
    }

    pub(crate) fn compact_native_once_engine(
        &self,
        clock: &mut dyn FnMut() -> NativeResult<NativeClockLease>,
    ) -> NativeResult<usize> {
        #[cfg(feature = "engine-rocksdb")]
        if matches!(&self.inner.backend, Backend::V2(_)) { return self.compact_native_once_v2(clock); }
        self.inner.native_once_locked(|state| {
            if state.native_outcomes.is_none() {
                return Ok(0);
            }
            let lease = certify(state, clock)?;
            let ledger = state.native_outcomes.as_mut().expect("checked");
            let count = ledger.compact(lease);
            if count == 0 {
                return Ok(0);
            }
            // Compaction changes only the outcome ledger, never the catalog.
            self.inner
                .persist_checked(state, None, true, &mut || {
                    Instant::now() < lease.valid_until
                })
                .map_err(persistence)?;
            Ok(count)
        })
    }
}

impl SqlSession {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_native_once_engine(
        mut self,
        setup: Option<once_session::OnceSetup>,
        owner: NativeOnceOwner,
        accepted_fingerprint: [u8; 32],
        result: Vec<u8>,
        retention_seconds: u64,
        mode: NativePublishMode,
        clock: &mut dyn FnMut() -> NativeResult<NativeClockLease>,
    ) -> NativeResult<(NativeOnceRecord, bool)> {
        if !Arc::ptr_eq(&self.admin.inner, &owner.inner) {
            return Err(NativeOnceError::DomainMismatch);
        }
        if !(1..=MAX_NATIVE_RETENTION_SECONDS).contains(&retention_seconds)
            || result.len() > MAX_NATIVE_RESULT_BYTES
        {
            return Err(NativeOnceError::RetentionUnavailable);
        }
        #[cfg(feature = "engine-rocksdb")]
        if self.is_v2_engine() {
            let setup = setup.ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
            return self.publish_native_once_v2(setup, owner, accepted_fingerprint, result, retention_seconds, mode, clock);
        }
        let _ = setup;
        if self.staged.is_none() || self.transaction_id.is_none() {
            return Err(CoreError::Sql(relational::SqlError::Transaction).into());
        }
        if mode == NativePublishMode::Success && self.aborted {
            return Err(CoreError::Sql(relational::SqlError::Aborted).into());
        }
        let inner = self.admin.inner.clone();
        inner.native_once_locked(|state| {
            check_key(&LedgerView::borrow(state), &owner.key)?;
            if let Some(record) = retained(&LedgerView::borrow(state), &owner.key)? {
                return replay(&record, owner.submitted).map(|r| (r, true));
            }
            if !crate::cancellation::lock(&inner.native_once_owners)?
                .get(&owner.key)
                .is_some_and(|entry| Arc::ptr_eq(&entry.token, &owner.token))
            {
                return Err(CoreError::Fenced.into());
            }
            let staged = self.staged.as_mut().expect("active transaction");
            if state.view.domain != staged.view.domain
                || state.sql.generation != staged.sql.generation
            {
                return Err(CoreError::Sql(relational::SqlError::Conflict).into());
            }
            let lease = certify(state, clock)?;
            let previous_relations = relation_names(state);
            // Keep the old snapshot in self for normal slot cleanup. The terminal
            // branch keeps current state and discards all earlier staged effects.
            if mode == NativePublishMode::Success {
                std::mem::swap(state, staged);
            }
            let ledger = state
                .native_outcomes
                .get_or_insert_with(NativeOutcomes::default);
            ledger.insert_outcome(
                owner.key.clone(), owner.submitted, accepted_fingerprint,
                result, retention_seconds, lease,
            )?;
            // Result validation checks the stream without materializing the
            // application DTO. Finish response allocation before persistence.
            let response = retained(&LedgerView::borrow(state), &owner.key)?
                .expect("accepted record");
            inner
                .persist_checked(state, Some(&previous_relations), true, &mut || {
                    Instant::now() < lease.valid_until
                })
                .map_err(persistence)?;
            Ok((response, false))
        })
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
mod v2_publish_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
mod v2_ledger_tests;
