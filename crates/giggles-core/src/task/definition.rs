//! Task definition types (GD-0061 design section 1, sections 2, 6 and 7).
use crate::{relational::Value, roles::CatalogRole};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

/// At most this many task records (live or revoked) exist at once.
pub const MAX_TASKS: usize = 4096;
/// Longest task name, in bytes.
pub const MAX_TASK_NAME: usize = 63;

/// Load-limit defaults used when CREATE TASK omits a `WITH` entry.
pub const DEFAULT_STATEMENT_MS: u64 = 2000;
pub const DEFAULT_ROWS_SCANNED: u64 = 100_000;
pub const DEFAULT_CONCURRENT_STATEMENTS: u32 = 2;
pub const DEFAULT_LOCK_WAIT_MS: u64 = 1000;
pub const DEFAULT_PENDING_WRITE_BYTES: u64 = 1_048_576;
pub const DEFAULT_STORED_PREVIEWS: u32 = 8;
pub const DEFAULT_PREVIEW_TTL_S: u64 = 3600;

/// A database role name.
pub type RoleName = String;
/// A column name of the task's table.
pub type ColumnName = String;
/// A catalog table id.
pub type TableId = u64;

/// Catalog object id of a task. Defined once, next to its key codec, in
/// `task/records.rs`.
pub use super::records::TaskObjectId;

/// An immutable task name: 1..=63 bytes over `[A-Za-z0-9_.:-]`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskName(String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidTaskName;

impl fmt::Display for InvalidTaskName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid task name")
    }
}

impl std::error::Error for InvalidTaskName {}

impl TaskName {
    pub fn new(name: &str) -> Result<Self, InvalidTaskName> {
        let valid = !name.is_empty()
            && name.len() <= MAX_TASK_NAME
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'));
        if valid {
            Ok(Self(name.to_owned()))
        } else {
            Err(InvalidTaskName)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Canonical, deterministic SQL predicate text (design section 4).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PredicateText(pub String);

/// Unix milliseconds, UTC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WallClockMs(pub i64);

/// How a column of the task's table appears to the executor. Unlisted
/// columns are Hidden. A masked constant is Int, Text or Bool in record
/// version 1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnMode {
    Readable,
    Masked { constant: Option<Value> },
    Hidden,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllowedWrite {
    pub columns: BTreeSet<ColumnName>,
    pub old_row: PredicateText,
    pub new_row: PredicateText,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MutationCaps {
    pub distinct_rows: u64,
    pub transitions: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DisclosureCaps {
    pub rows_read: u64,
    pub rows_returned: u64,
    pub bytes_returned: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoadLimits {
    pub statement_ms: u64,
    pub rows_scanned: u64,
    pub concurrent_statements: u32,
    pub lock_wait_ms: u64,
    pub pending_write_bytes: u64,
    pub stored_previews: u32,
    pub preview_ttl_s: u64,
}

impl Default for LoadLimits {
    fn default() -> Self {
        Self {
            statement_ms: DEFAULT_STATEMENT_MS,
            rows_scanned: DEFAULT_ROWS_SCANNED,
            concurrent_statements: DEFAULT_CONCURRENT_STATEMENTS,
            lock_wait_ms: DEFAULT_LOCK_WAIT_MS,
            pending_write_bytes: DEFAULT_PENDING_WRITE_BYTES,
            stored_previews: DEFAULT_STORED_PREVIEWS,
            preview_ttl_s: DEFAULT_PREVIEW_TTL_S,
        }
    }
}

/// Per-task privileges of a role (design section 6).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TaskPrivileges(u8);

impl TaskPrivileges {
    pub const NONE: Self = Self(0);
    pub const APPROVE: Self = Self(1);
    pub const UNDO: Self = Self(2);
    pub const INSPECT: Self = Self(4);
    pub const ALL: Self = Self(7);

    /// Known bits only; `None` for an unknown bit.
    pub fn from_bits(bits: u8) -> Option<Self> {
        (bits & !Self::ALL.0 == 0).then_some(Self(bits))
    }

    pub fn bits(self) -> u8 {
        self.0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for TaskPrivileges {
    type Output = Self;
    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::ops::BitOrAssign for TaskPrivileges {
    fn bitor_assign(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskDef {
    pub id: TaskObjectId,
    pub name: TaskName,
    pub def_version: u64,
    pub issuer: RoleName,
    pub executor: RoleName,
    pub approver: Option<RoleName>,
    pub table: TableId,
    pub visible_rows: PredicateText,
    /// Unlisted columns are Hidden.
    pub columns: BTreeMap<ColumnName, ColumnMode>,
    pub untrusted_allowed: BTreeSet<ColumnName>,
    /// `None` is a read-only task.
    pub allowed_write: Option<AllowedWrite>,
    pub mutation_caps: MutationCaps,
    pub disclosure_caps: DisclosureCaps,
    pub limits: LoadLimits,
    /// Rows; 0 = every write is proposed.
    pub approval_threshold: u64,
    pub expires_at: WallClockMs,
    pub revoked: bool,
    pub grants: BTreeMap<RoleName, TaskPrivileges>,
}

impl TaskDef {
    /// The executor holds nothing, even if granted. The named approver holds
    /// APPROVE, UNDO and INSPECT; the issuer UNDO and INSPECT; a superuser
    /// INSPECT; grants are OR-ed with these.
    pub fn privileges_of(&self, role: &CatalogRole) -> TaskPrivileges {
        if role.name == self.executor {
            return TaskPrivileges::NONE;
        }
        let mut held = TaskPrivileges::NONE;
        if self.approver.as_deref() == Some(role.name.as_str()) {
            held |= TaskPrivileges::ALL;
        }
        if role.name == self.issuer {
            held |= TaskPrivileges::UNDO | TaskPrivileges::INSPECT;
        }
        if role.superuser {
            held |= TaskPrivileges::INSPECT;
        }
        if let Some(granted) = self.grants.get(&role.name) {
            held |= *granted;
        }
        held
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskOnlyViolation {
    TaskOnlyRoleOutsideTask,
    OrdinaryRoleInTask,
}

/// A task-only role may act only inside a task session, and a task session
/// only as a task-only role.
pub fn check_task_only(role: &CatalogRole, in_task_session: bool) -> Result<(), TaskOnlyViolation> {
    match (role.task_only, in_task_session) {
        (true, false) => Err(TaskOnlyViolation::TaskOnlyRoleOutsideTask),
        (false, true) => Err(TaskOnlyViolation::OrdinaryRoleInTask),
        _ => Ok(()),
    }
}

/// A parsed CREATE TASK (design-1 section 5), shared by the SQL parser
/// (`task::statements`) and the catalog writer (`TaskCatalog::create`).
/// Names are folded (unquoted) or exact (quoted); predicates are raw SQL text,
/// canonicalised by the catalog; the EXPIRES literal is kept as written so the
/// catalog can require an explicit offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewTask {
    pub name: String,
    pub executor: RoleName,
    pub approver: Option<RoleName>,
    /// `None` is the unqualified (public) table.
    pub schema: Option<String>,
    pub table: String,
    pub visible_rows: PredicateText,
    pub readable: Vec<ColumnName>,
    pub masked: Vec<(ColumnName, Option<Value>)>,
    pub untrusted_text: Vec<ColumnName>,
    /// `None` is a read-only task.
    pub allowed_write: Option<AllowedWrite>,
    pub mutation_caps: MutationCaps,
    pub disclosure_caps: DisclosureCaps,
    pub limits: LoadLimits,
    /// Rows; equals `mutation_caps.distinct_rows` when APPROVE ABOVE is omitted.
    pub approval_threshold: u64,
    /// The EXPIRES literal as written.
    pub expires: String,
}
