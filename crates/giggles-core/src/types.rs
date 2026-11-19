use std::collections::{BTreeMap, BTreeSet};

macro_rules! id {
    ($($name:ident),*) => {$ (
        #[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u64);
    )*};
}
id!(TenantId, RowId, TaskId, OperationId, Principal, ApprovalId);

#[derive(
    serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord,
)]
#[repr(u8)]
pub enum Status {
    Failed = 0,
    Queued = 1,
    Succeeded = 2,
}
#[derive(
    serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord,
)]
#[repr(u8)]
pub enum Column {
    Status = 0,
    FailedAt = 1,
    AttemptCount = 2,
}
#[derive(
    serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct Key {
    pub tenant: TenantId,
    pub row: RowId,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub status: Status,
    pub failed_at: u64,
    pub attempt_count: u64,
    pub version: u64,
}

/// Typed grammar. Bounds are half-open; transition and column sets have no wildcards.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub tenant: TenantId,
    pub columns: BTreeSet<Column>,
    pub transitions: BTreeSet<(Status, Status)>,
    pub incident: (u64, u64),
    pub distinct_limit: u64,
    pub transition_limit: u64,
    pub approval_above: u64,
    pub valid_from: u64,
    pub expires: u64,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    pub task: TaskId,
    pub issuer: Principal,
    pub executor: Principal,
    pub approver: Principal,
    pub policy: Policy,
}
/// Opaque local capability, not a cryptographic token. Only trusted grant issues one.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg(feature = "legacy-task")]
pub(crate) struct Credential {
    pub(crate) domain: u128,
    pub(crate) task: TaskId,
    pub(crate) executor: Principal,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TaskState {
    pub grant: Grant,
    pub revoked: bool,
    pub touched: BTreeSet<Key>,
    pub transitions: u64,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Time {
    Certified(u64),
    Uncertain,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Selection {
    Keys(Vec<Key>),
    /// Complete qualifying set, including phantom protection in exact previews.
    Predicate {
        tenant: TenantId,
        status: Status,
        from: u64,
        until: u64,
    },
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub operation: OperationId,
    pub selection: Selection,
    pub column: Column,
    pub after: Status,
}
#[cfg(any(test, feature = "legacy-task"))]
impl Request {
    /// Versioned, length-delimited binary payload. Key-list order is immaterial;
    /// multiplicity is preserved and duplicate keys are rejected by validation.
    pub fn canonical(&self) -> Vec<u8> {
        let mut out = b"GD-P0-REPAIR\x01".to_vec();
        fn num(out: &mut Vec<u8>, n: u64) {
            out.extend(n.to_be_bytes());
        }
        match &self.selection {
            Selection::Keys(keys) => {
                out.push(0);
                let mut keys = keys.clone();
                keys.sort();
                num(&mut out, keys.len() as u64);
                for k in keys {
                    num(&mut out, k.tenant.0);
                    num(&mut out, k.row.0);
                }
            }
            Selection::Predicate {
                tenant,
                status,
                from,
                until,
            } => {
                out.push(1);
                num(&mut out, tenant.0);
                out.push(*status as u8);
                num(&mut out, *from);
                num(&mut out, *until);
            }
        }
        out.push(self.column as u8);
        out.push(self.after as u8);
        out
    }
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Effect {
    pub key: Key,
    pub before: Row,
    pub after: Row,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Preview {
    pub domain: u128,
    pub task: TaskId,
    pub request: Request,
    pub schema: u64,
    pub policy: Policy,
    pub dependencies: BTreeMap<Key, Row>,
    pub effects: Vec<Effect>,
    pub expires: u64,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Approval {
    pub id: ApprovalId,
    pub approver: Principal,
    pub preview: Preview,
    pub revoked: bool,
    pub consumed: bool,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub domain: u128,
    pub task: TaskId,
    pub operation: OperationId,
    pub payload: Vec<u8>,
    pub issuer: Principal,
    pub executor: Principal,
    pub approver: Option<Principal>,
    pub schema: u64,
    pub decided_at: u64,
    pub effects: Vec<Effect>,
    pub charged_transitions: u64,
    pub lifetime_transitions: u64,
    pub lifetime_distinct: u64,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Authentication,
    InvalidGrant,
    TaskExists,
    UnknownTask,
    Revoked,
    Expired,
    UncertainTime,
    TimeRegression,
    Scope,
    MissingRow,
    DuplicateKey,
    InvalidPredicate,
    Budget,
    ApprovalRequired,
    InvalidApproval,
    StalePreview,
    IdConflict,
    Overflow,
}
pub type Decision = Result<Receipt, Error>;
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg(feature = "legacy-task")]
pub enum Outcome {
    Known(Decision),
    Unknown,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg(feature = "legacy-task")]
pub enum OperationStatus {
    Absent,
    Decided(Decision),
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BoundDecision {
    pub payload: Vec<u8>,
    pub decision: Decision,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg(feature = "legacy-task")]
pub enum Event {
    Put(Key, Row),
    Grant(Grant),
    Revoke(TaskId),
    Schema(u64),
    Approve(Approval),
    RevokeApproval(ApprovalId),
    Decision {
        task: TaskId,
        request: Request,
        time: Time,
        approval: Option<ApprovalId>,
        result: Decision,
    },
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub domain: u128,
    pub schema: u64,
    pub rows: BTreeMap<Key, Row>,
    pub tasks: BTreeMap<TaskId, TaskState>,
    pub operations: BTreeMap<(TaskId, OperationId), BoundDecision>,
    pub approvals: BTreeMap<ApprovalId, Approval>,
}
