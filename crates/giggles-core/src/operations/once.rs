//! Versioned, trusted embedded durable retry envelopes.
//!
//! A caller must persist the original identity, including database domain, before
//! its first attempt. These labels partition retry identity; they confer no
//! tenant or task authority. Transport adapters need their own authorization.

use super::*;
use crate::storage::native_once::{
    NativeClockLease, NativeOnceAdmission, NativeOnceError, NativeOnceKey, NativeOnceLookup,
    NativeOnceRecord, NativePublishMode,
};
use std::time::Instant;

mod intent;
pub(crate) mod outcome;

pub const ONCE_CONTRACT_VERSION: u16 = 1;
pub const MAX_ONCE_KEY_BYTES: usize = 256;
pub const MAX_ONCE_NAMESPACE_BYTES: usize = 63;
pub const MAX_ONCE_INTENT_BYTES: usize = 256 * 1024;
pub const MAX_ONCE_RESULT_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_ONCE_RECORDS: usize = 1024;
pub const MAX_ONCE_LEDGER_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ONCE_RETENTION_SECONDS: u64 = 7 * 24 * 60 * 60;
pub const MAX_ONCE_CLOCK_LEASE_SECONDS: u64 = 30;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnceIdentity {
    pub database_domain: u128,
    pub tenant: u64,
    pub namespace: String,
    pub key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnceBody {
    Operation(Request),
    Transaction(Transaction),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnceRequest {
    /// Persist this selector with the original request. Initially only 1 exists.
    pub contract_version: u16,
    pub identity: OnceIdentity,
    pub retention_seconds: u64,
    pub body: OnceBody,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnceOutcome {
    Operation(Outcome),
    Transaction(TransactionOutcome),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnceSchema {
    Operation(Vec<ColumnDef>),
    Transaction(Vec<EmitDescription>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnceDescription {
    /// Catalog epoch of the accepting snapshot, never the replay's live catalog.
    pub catalog_epoch: u64,
    pub output: OnceSchema,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnceReceipt {
    pub accepted_fingerprint: [u8; 32],
    pub description: OnceDescription,
    /// Guaranteed retention from the logical publication decision, in Unix seconds.
    pub retry_until_seconds: u64,
    pub replayed: bool,
    pub outcome: std::result::Result<OnceOutcome, Error>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnceStatus {
    Absent,
    InProgress,
    Retained(OnceReceipt),
    Expired { retry_until_seconds: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnceError {
    Operation(Error),
    DomainMismatch,
    InProgress,
    IdempotencyConflict,
    KeyExpired {
        retry_until_seconds: u64,
    },
    ClockUncertain,
    /// The requested durable receipt does not fit the finite retention bounds.
    RetentionUnavailable,
    /// An internal result cannot be represented by the versioned receipt codec.
    InvalidOutcome,
    /// Publication may have committed. Resolve an execution's original identity
    /// after recovery. Compaction can be repeated, but its original count is unknown.
    Unknown,
}

impl std::fmt::Display for OnceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for OnceError {}
impl From<Error> for OnceError {
    fn from(error: Error) -> Self {
        Self::Operation(error)
    }
}

pub type OnceResult<T> = std::result::Result<T, OnceError>;

/// A trusted interval valid throughout its remaining monotonic lease.
///
/// The provider promises real Unix time stays within [lower, upper] until
/// `valid_until`, including the publication decision. A mere wall-clock sample
/// does not establish this promise. Seconds here do not reuse repair clock state.
#[derive(Clone, Copy, Debug)]
pub struct OnceTime {
    pub lower_unix_seconds: u64,
    pub upper_unix_seconds: u64,
    pub valid_until: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OnceClockUnavailable;

/// Trusted, bounded, non-reentrant clock certification. There is no default.
///
/// Called after evaluation while database publication is serialized. This must
/// not call back into this database, including `Admin::certify_server_time`.
pub trait OnceClock {
    fn certify(&self) -> std::result::Result<OnceTime, OnceClockUnavailable>;
}

impl<F> OnceClock for F
where
    F: Fn() -> std::result::Result<OnceTime, OnceClockUnavailable>,
{
    fn certify(&self) -> std::result::Result<OnceTime, OnceClockUnavailable> {
        self()
    }
}

pub(super) fn terminal(error: &Error) -> bool {
    matches!(
        error,
        Error::NotFound
            | Error::AmbiguousMatch
            | Error::ConditionFailed
            | Error::TooManyMatches
            | Error::RequirementFailed
    )
}

fn key(identity: &OnceIdentity) -> OnceResult<NativeOnceKey> {
    if identity.namespace.is_empty()
        || identity.namespace.len() > MAX_ONCE_NAMESPACE_BYTES
        || identity.key.is_empty()
        || identity.key.len() > MAX_ONCE_KEY_BYTES
    {
        return Err(Error::Bind("once identity".into()).into());
    }
    let mut bytes = Vec::with_capacity(12 + identity.namespace.len() + identity.key.len());
    bytes.push(1); // Scope encoding version, independent of intent/result codecs.
    bytes.extend_from_slice(&identity.tenant.to_be_bytes());
    bytes.push(identity.namespace.len() as u8);
    bytes.extend_from_slice(identity.namespace.as_bytes());
    bytes.extend_from_slice(&(identity.key.len() as u16).to_be_bytes());
    bytes.extend_from_slice(&identity.key);
    Ok(NativeOnceKey {
        database_domain: identity.database_domain,
        identity: bytes,
    })
}

fn storage_error(error: NativeOnceError) -> OnceError {
    match error {
        NativeOnceError::Core(CoreError::Unsupported) => OnceError::Operation(Error::Unsupported),
        NativeOnceError::Core(error) => OnceError::Operation(core(error)),
        NativeOnceError::DomainMismatch => OnceError::DomainMismatch,
        NativeOnceError::InProgress => OnceError::InProgress,
        NativeOnceError::IdempotencyConflict => OnceError::IdempotencyConflict,
        NativeOnceError::ClockUncertain => OnceError::ClockUncertain,
        NativeOnceError::RetentionUnavailable => OnceError::RetentionUnavailable,
    }
}

fn publication_error(error: NativeOnceError) -> OnceError {
    match error {
        NativeOnceError::Core(CoreError::Storage(_)) => OnceError::Unknown,
        other => storage_error(other),
    }
}

fn lease(clock: &dyn OnceClock) -> std::result::Result<NativeClockLease, NativeOnceError> {
    let time = clock
        .certify()
        .map_err(|_| NativeOnceError::ClockUncertain)?;
    Ok(NativeClockLease {
        lower_unix_seconds: time.lower_unix_seconds,
        upper_unix_seconds: time.upper_unix_seconds,
        valid_until: time.valid_until,
    })
}

fn replay(record: NativeOnceRecord) -> OnceResult<OnceReceipt> {
    let bytes = record.result.ok_or(OnceError::KeyExpired {
        retry_until_seconds: record.retry_until_seconds,
    })?;
    let (description, outcome) =
        outcome::decode(&bytes).map_err(|error| OnceError::Operation(core(error)))?;
    Ok(OnceReceipt {
        accepted_fingerprint: record.accepted_fingerprint,
        description,
        retry_until_seconds: record.retry_until_seconds,
        replayed: true,
        outcome,
    })
}

impl Engine {
    /// Execute a V1 write once within its explicit, caller-persisted identity.
    ///
    /// A retained matching submission returns the original receipt before live
    /// catalog binding or predicate evaluation. Failed admission and nonterminal
    /// execution errors do not consume a key. The five terminal business errors
    /// are successful durable decisions, represented in `OnceReceipt::outcome`.
    /// An Unknown response must be resolved using this original identity.
    pub fn execute_once(
        &self,
        request: &OnceRequest,
        bindings: &Bindings,
        clock: &dyn OnceClock,
    ) -> OnceResult<OnceReceipt> {
        let identity = key(&request.identity)?;
        if request.contract_version != ONCE_CONTRACT_VERSION {
            return Err(Error::Unsupported.into());
        }
        if request.retention_seconds == 0 || request.retention_seconds > MAX_ONCE_RETENTION_SECONDS
        {
            return Err(OnceError::RetentionUnavailable);
        }
        let submitted = intent::submission_identity(request, bindings)?;
        let owner = match self
            .admin
            .admit_native_once_engine(identity, submitted)
            .map_err(storage_error)?
        {
            NativeOnceAdmission::Replay(record) => return replay(record),
            NativeOnceAdmission::Owner(owner) => owner,
        };
        let mut session = self.admin.sql_session();
        #[cfg(feature = "engine-rocksdb")]
        let setup = if session.is_v2_engine() {
            Some(session.begin_once_engine().map_err(core)?)
        } else {
            session.begin_engine().map_err(core)?;
            None
        };
        #[cfg(not(feature = "engine-rocksdb"))]
        let setup = {
            session.begin_engine().map_err(core)?;
            None
        };
        let (database_domain, catalog_epoch) = session.snapshot_identity_engine().map_err(core)?;
        if database_domain != request.identity.database_domain {
            return Err(OnceError::DomainMismatch);
        }
        // Both the accepted identity and evaluation use this exact snapshot.
        // The owner/session guards release all transient resources on early return.
        let (accepted, description, result) = match &request.body {
            OnceBody::Operation(request) => {
                let (table, operation) = bind_request(&mut session, request, bindings)?;
                let columns = operation.columns(&table);
                let accepted = intent::accepted_identity(
                    &submitted,
                    std::slice::from_ref(&table),
                    intent::AcceptedOutput::Operation(&columns),
                )?;
                let result = run_complete(&mut session, &table, operation, None);
                let description = OnceDescription {
                    catalog_epoch,
                    output: OnceSchema::Operation(columns),
                };
                (accepted, description, result.map(OnceOutcome::Operation))
            }
            OnceBody::Transaction(transaction) => {
                let prepared = transaction::bind_transaction(&mut session, transaction, bindings)?;
                let description = prepared.description();
                let accepted = intent::accepted_identity(
                    &submitted,
                    &description.tables,
                    intent::AcceptedOutput::Transaction(&description.emitted),
                )?;
                let description = OnceDescription {
                    catalog_epoch: description.catalog_epoch,
                    output: OnceSchema::Transaction(description.emitted.clone()),
                };
                let result = transaction::run_transaction(&mut session, &prepared);
                (accepted, description, result.map(OnceOutcome::Transaction))
            }
        };
        let mode = match &result {
            Ok(_) => NativePublishMode::Success,
            Err(error) if terminal(error) => NativePublishMode::Terminal,
            Err(error) => return Err(error.clone().into()),
        };
        crate::cancellation::check().map_err(core)?;
        // Admission includes all encoded descriptors and zero-payload cells.
        // Everything above is speculative until data and receipt commit together.
        let encoded = outcome::encode(&description, &result)?;
        let (record, replayed) = session
            .publish_native_once_engine(
                setup,
                owner,
                accepted,
                encoded,
                request.retention_seconds,
                mode,
                &mut || lease(clock),
            )
            .map_err(publication_error)?;
        if replayed {
            return replay(record);
        }
        // Keep the already-materialized result; decoding after a known commit
        // would introduce a new allocation failure before delivering its receipt.
        Ok(OnceReceipt {
            accepted_fingerprint: record.accepted_fingerprint,
            description,
            retry_until_seconds: record.retry_until_seconds,
            replayed: false,
            outcome: result,
        })
    }

    /// Look up an original identity without rebinding or re-executing its body.
    /// Retained outcomes can remain available beyond their guaranteed deadline.
    pub fn outcome(&self, identity: &OnceIdentity) -> OnceResult<OnceStatus> {
        match self
            .admin
            .lookup_native_once_engine(&key(identity)?)
            .map_err(storage_error)?
        {
            NativeOnceLookup::Absent => Ok(OnceStatus::Absent),
            NativeOnceLookup::InProgress => Ok(OnceStatus::InProgress),
            NativeOnceLookup::Retained(record) => replay(record).map(OnceStatus::Retained),
            NativeOnceLookup::Expired {
                retry_until_seconds,
            } => Ok(OnceStatus::Expired {
                retry_until_seconds,
            }),
        }
    }

    /// Replace provably expired receipt payloads with permanent tombstones.
    /// Identities are never reused; reaching the key limit refuses new admissions.
    /// Unknown requires reopening and repeating compaction; its earlier count
    /// cannot be inferred from the count returned by a later attempt.
    pub fn compact_outcomes(&self, clock: &dyn OnceClock) -> OnceResult<usize> {
        self.admin
            .compact_native_once_engine(&mut || lease(clock))
            .map_err(publication_error)
    }
}
