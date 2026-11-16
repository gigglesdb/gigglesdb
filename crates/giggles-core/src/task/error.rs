//! Task-session errors and their SQLSTATE identities (GD-0061 design section 6).
//!
//! Custom codes sit inside standard classes with subclass `Tnn`, which PostgreSQL does not use.
//! Messages are fixed literals: no values, keys, counts, remaining budget or object names, and
//! identical on SQL and native.
use super::admit::StatementClass;
use crate::relational::SqlError;
use crate::CoreError;

/// Why a task statement was denied. Persisted in outcome records by `code()`: codes are stable,
/// new variants take the next code, and codes are never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DenyRule {
    RolePrivilege,
    HiddenColumn,
    MaskedColumnPosition,
    UntrustedColumn,
    OtherTable,
    ColumnNotWritable,
    OutsideAllowedWrite,
    FunctionNotAllowed,
    SequenceFunction,
    ForeignKeyReach,
    UnmaterializedValue,
    DefinitionChanged,
    ColumnNotAvailable,
    Join,
    Subquery,
    Sequence,
    UserType,
    Explain,
    OutsideVisibleRows,
    PrimaryKeyChange,
    NotMaterializable,
    PreviewTooLarge,
    UndoHasConflicts,
}

impl DenyRule {
    pub const ALL: [DenyRule; 23] = [
        DenyRule::RolePrivilege,
        DenyRule::HiddenColumn,
        DenyRule::MaskedColumnPosition,
        DenyRule::UntrustedColumn,
        DenyRule::OtherTable,
        DenyRule::ColumnNotWritable,
        DenyRule::OutsideAllowedWrite,
        DenyRule::FunctionNotAllowed,
        DenyRule::SequenceFunction,
        DenyRule::ForeignKeyReach,
        DenyRule::UnmaterializedValue,
        DenyRule::DefinitionChanged,
        DenyRule::ColumnNotAvailable,
        DenyRule::Join,
        DenyRule::Subquery,
        DenyRule::Sequence,
        DenyRule::UserType,
        DenyRule::Explain,
        DenyRule::OutsideVisibleRows,
        DenyRule::PrimaryKeyChange,
        DenyRule::NotMaterializable,
        DenyRule::PreviewTooLarge,
        DenyRule::UndoHasConflicts,
    ];

    /// Stable persisted code.
    pub fn code(self) -> u16 {
        match self {
            DenyRule::RolePrivilege => 1,
            DenyRule::HiddenColumn => 2,
            DenyRule::MaskedColumnPosition => 3,
            DenyRule::UntrustedColumn => 4,
            DenyRule::OtherTable => 5,
            DenyRule::ColumnNotWritable => 6,
            DenyRule::OutsideAllowedWrite => 7,
            DenyRule::FunctionNotAllowed => 8,
            DenyRule::SequenceFunction => 9,
            DenyRule::ForeignKeyReach => 10,
            DenyRule::UnmaterializedValue => 11,
            DenyRule::DefinitionChanged => 12,
            DenyRule::ColumnNotAvailable => 13,
            DenyRule::Join => 14,
            DenyRule::Subquery => 15,
            DenyRule::Sequence => 16,
            DenyRule::UserType => 17,
            DenyRule::Explain => 18,
            DenyRule::OutsideVisibleRows => 19,
            DenyRule::PrimaryKeyChange => 20,
            DenyRule::NotMaterializable => 21,
            DenyRule::PreviewTooLarge => 22,
            DenyRule::UndoHasConflicts => 23,
        }
    }

    pub fn from_code(code: u16) -> Option<Self> {
        Self::ALL.into_iter().find(|rule| rule.code() == code)
    }

    pub fn message(self) -> &'static str {
        match self {
            DenyRule::RolePrivilege => "denied: the task role lacks the privilege this statement needs",
            DenyRule::HiddenColumn => "denied: column is hidden for this task",
            DenyRule::MaskedColumnPosition => {
                "denied: a masked column may only be selected as a plain output column"
            }
            DenyRule::UntrustedColumn => "denied: column is not trusted for this use by this task",
            DenyRule::OtherTable => "denied: statement references a table outside this task",
            DenyRule::ColumnNotWritable => "denied: column is not writable by this task",
            DenyRule::OutsideAllowedWrite => "denied: change is outside the allowed write of this task",
            DenyRule::FunctionNotAllowed => "denied: function is not allowed in a task statement",
            DenyRule::SequenceFunction => {
                "denied: sequence functions are not allowed in a task statement"
            }
            DenyRule::ForeignKeyReach => {
                "denied: change would reach other rows through a foreign key"
            }
            DenyRule::UnmaterializedValue => "denied: value cannot be materialized for this task",
            DenyRule::DefinitionChanged => "denied: task definition changed; reconnect",
            DenyRule::ColumnNotAvailable => "denied: column is not available to this task",
            DenyRule::Join => "denied: joins are not allowed in a task statement",
            DenyRule::Subquery => "denied: subqueries are not allowed in a task statement",
            DenyRule::Sequence => "denied: sequences are not allowed in a task statement",
            DenyRule::UserType => "denied: user-defined types are not allowed in a task statement",
            DenyRule::Explain => "denied: EXPLAIN is not allowed in a task statement",
            DenyRule::OutsideVisibleRows => "denied: row is outside the visible rows of this task",
            DenyRule::PrimaryKeyChange => "denied: a task may not change a primary key",
            DenyRule::NotMaterializable => "denied: change cannot be materialized for preview",
            DenyRule::PreviewTooLarge => "denied: preview is too large to store",
            DenyRule::UndoHasConflicts => {
                "denied: complete undo is not possible because later changes conflict"
            }
        }
    }
}

/// Which task budget or limit was exceeded. Persisted in outcome records by `code()`: codes are
/// stable, new variants take the next code, and codes are never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Budget {
    DistinctRows,
    Transitions,
    RowsRead,
    RowsReturned,
    BytesReturned,
    RowsScanned,
    PendingWrite,
    ConcurrentStatements,
    AllAgents,
    StoredPreviews,
}

impl Budget {
    pub const ALL: [Budget; 10] = [
        Budget::DistinctRows,
        Budget::Transitions,
        Budget::RowsRead,
        Budget::RowsReturned,
        Budget::BytesReturned,
        Budget::RowsScanned,
        Budget::PendingWrite,
        Budget::ConcurrentStatements,
        Budget::AllAgents,
        Budget::StoredPreviews,
    ];

    /// Stable persisted code.
    pub fn code(self) -> u16 {
        match self {
            Budget::DistinctRows => 1,
            Budget::Transitions => 2,
            Budget::RowsRead => 3,
            Budget::RowsReturned => 4,
            Budget::BytesReturned => 5,
            Budget::RowsScanned => 6,
            Budget::PendingWrite => 7,
            Budget::ConcurrentStatements => 8,
            Budget::AllAgents => 9,
            Budget::StoredPreviews => 10,
        }
    }

    pub fn from_code(code: u16) -> Option<Self> {
        Self::ALL.into_iter().find(|budget| budget.code() == code)
    }

    pub fn message(self) -> &'static str {
        match self {
            Budget::DistinctRows => "budget exceeded: distinct rows changed by this task",
            Budget::Transitions => "budget exceeded: row transitions made by this task",
            Budget::RowsRead => "budget exceeded: rows read by this task",
            Budget::RowsReturned => "budget exceeded: rows returned to this task",
            Budget::BytesReturned => "budget exceeded: bytes returned to this task",
            Budget::RowsScanned => "budget exceeded: rows scanned by this statement",
            Budget::PendingWrite => "budget exceeded: pending write size of this statement",
            Budget::ConcurrentStatements => "budget exceeded: concurrent statements of this task",
            Budget::AllAgents => "budget exceeded: load of all agent sessions",
            Budget::StoredPreviews => "budget exceeded: stored previews of this task",
        }
    }
}

/// An error from a task session. `Sql` and `Core` pass an underlying error through unchanged.
///
/// `Core` is boxed because `CoreError::Task` holds a `TaskError` inline. Convert with `From`
/// in either direction; the conversions never nest one wrapper inside the other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskError {
    Denied(DenyRule),
    ApprovalRequired,
    ApprovalStale,
    Revoked,
    Expired,
    BudgetExceeded(Budget),
    OperationConflict,
    OperationIdRequired,
    NotAllowedInTaskSession(StatementClass),
    /// 40001 serialization_failure, after the bounded internal retry.
    Retry,
    Sql(SqlError),
    Core(Box<CoreError>),
}

/// Message sent for a pass-through error that reaches a task message site.
const PASS_THROUGH: &str = "request rejected; consult documented code";

impl TaskError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            TaskError::Denied(_) => "42501",
            TaskError::ApprovalRequired => "42T01",
            TaskError::OperationConflict => "42T03",
            TaskError::OperationIdRequired => "42T04",
            TaskError::ApprovalStale => "55T01",
            TaskError::Revoked => "28T01",
            TaskError::Expired => "28T02",
            TaskError::BudgetExceeded(_) => "54T01",
            TaskError::NotAllowedInTaskSession(_) => "0A000",
            TaskError::Retry => "40001",
            TaskError::Sql(e) => crate::sqlstate(&CoreError::Sql(e.clone())),
            TaskError::Core(e) => crate::sqlstate(e),
        }
    }

    /// Name on the wire (`task_error`). `None` for pass-through errors.
    pub fn name(&self) -> Option<&'static str> {
        Some(match self {
            TaskError::Denied(_) => "denied",
            TaskError::ApprovalRequired => "approval_required",
            TaskError::OperationConflict => "operation_conflict",
            TaskError::OperationIdRequired => "operation_id_required",
            TaskError::ApprovalStale => "approval_stale",
            TaskError::Revoked => "revoked",
            TaskError::Expired => "expired",
            TaskError::BudgetExceeded(_) => "budget_exceeded",
            TaskError::NotAllowedInTaskSession(_) => "not_allowed",
            TaskError::Retry => "retry",
            TaskError::Sql(_) | TaskError::Core(_) => return None,
        })
    }

    pub fn message(&self) -> &'static str {
        match self {
            TaskError::Denied(rule) => rule.message(),
            TaskError::BudgetExceeded(budget) => budget.message(),
            TaskError::NotAllowedInTaskSession(class) => not_allowed_message(*class),
            TaskError::Retry => "could not serialize access due to concurrent update; retry",
            TaskError::Sql(_) | TaskError::Core(_) => PASS_THROUGH,
            TaskError::ApprovalRequired
            | TaskError::ApprovalStale
            | TaskError::Revoked
            | TaskError::Expired
            | TaskError::OperationConflict
            | TaskError::OperationIdRequired => Self::message_for_sqlstate(self.sqlstate())
                .unwrap_or(PASS_THROUGH),
        }
    }

    /// Default message for a task-specific SQLSTATE, for sites that only have the code.
    pub fn message_for_sqlstate(code: &str) -> Option<&'static str> {
        Some(match code {
            "42T01" => "approval required: preview the change and have it approved",
            "42T03" => "operation id already used for a different request",
            "42T04" => "operation id required for this statement",
            "55T01" => "approval is stale: preview the change again, do not retry blindly",
            "28T01" => "task authorization revoked",
            "28T02" => "task authorization expired",
            "54T01" => "task budget exceeded",
            _ => return None,
        })
    }
}

fn not_allowed_message(class: StatementClass) -> &'static str {
    use StatementClass as C;
    match class {
        C::Select | C::Update | C::Preview | C::Apply | C::OutcomeLookup | C::Show | C::Set => {
            "statement form is not allowed in a task session"
        }
        C::Insert => "INSERT is not allowed in a task session",
        C::Delete => "DELETE is not allowed in a task session",
        C::Ddl => "schema changes are not allowed in a task session",
        C::Copy => "COPY is not allowed in a task session",
        C::TransactionControl => "explicit transactions are not allowed in a task session",
        C::Savepoint => "savepoints are not allowed in a task session",
        C::RoleStatement => "role statements are not allowed in a task session",
        C::Explain => "EXPLAIN is not allowed in a task session",
        C::Cursor => "cursors are not allowed in a task session",
        C::Lock => "explicit locks are not allowed in a task session",
        C::Catalog => "catalog queries are not allowed in a task session",
        C::Other => "statement is not allowed in a task session",
    }
}

impl From<CoreError> for TaskError {
    fn from(e: CoreError) -> Self {
        match e {
            CoreError::Task(t) => t,
            other => TaskError::Core(Box::new(other)),
        }
    }
}

impl From<TaskError> for CoreError {
    fn from(e: TaskError) -> Self {
        match e {
            TaskError::Core(c) => *c,
            other => CoreError::Task(other),
        }
    }
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for TaskError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;

    const CLASSES: [StatementClass; 19] = [
        StatementClass::Select,
        StatementClass::Update,
        StatementClass::Preview,
        StatementClass::Apply,
        StatementClass::OutcomeLookup,
        StatementClass::Show,
        StatementClass::Set,
        StatementClass::Insert,
        StatementClass::Delete,
        StatementClass::Ddl,
        StatementClass::Copy,
        StatementClass::TransactionControl,
        StatementClass::Savepoint,
        StatementClass::RoleStatement,
        StatementClass::Explain,
        StatementClass::Cursor,
        StatementClass::Lock,
        StatementClass::Catalog,
        StatementClass::Other,
    ];

    fn every_error() -> Vec<TaskError> {
        let mut all = vec![
            TaskError::ApprovalRequired,
            TaskError::ApprovalStale,
            TaskError::Revoked,
            TaskError::Expired,
            TaskError::OperationConflict,
            TaskError::OperationIdRequired,
            TaskError::Retry,
            TaskError::Sql(SqlError::Syntax),
            TaskError::Core(Box::new(CoreError::Busy)),
        ];
        all.extend(DenyRule::ALL.map(TaskError::Denied));
        all.extend(Budget::ALL.map(TaskError::BudgetExceeded));
        all.extend(CLASSES.map(TaskError::NotAllowedInTaskSession));
        all
    }

    #[test]
    fn each_error_has_one_sqlstate() {
        let table = [
            (TaskError::Denied(DenyRule::HiddenColumn), "denied", "42501"),
            (TaskError::ApprovalRequired, "approval_required", "42T01"),
            (TaskError::OperationConflict, "operation_conflict", "42T03"),
            (TaskError::OperationIdRequired, "operation_id_required", "42T04"),
            (TaskError::ApprovalStale, "approval_stale", "55T01"),
            (TaskError::Revoked, "revoked", "28T01"),
            (TaskError::Expired, "expired", "28T02"),
            (TaskError::BudgetExceeded(Budget::RowsRead), "budget_exceeded", "54T01"),
            (
                TaskError::NotAllowedInTaskSession(StatementClass::Ddl),
                "not_allowed",
                "0A000",
            ),
            (TaskError::Retry, "retry", "40001"),
        ];
        for (error, name, code) in &table {
            assert_eq!(error.sqlstate(), *code, "{error:?}");
            assert_eq!(error.name(), Some(*name), "{error:?}");
            assert_eq!(crate::sqlstate(&CoreError::Task(error.clone())), *code);
        }
        // A variant's code does not depend on its rule, budget or class.
        for rule in DenyRule::ALL {
            assert_eq!(TaskError::Denied(rule).sqlstate(), "42501");
        }
        for budget in Budget::ALL {
            assert_eq!(TaskError::BudgetExceeded(budget).sqlstate(), "54T01");
        }
        for class in CLASSES {
            assert_eq!(TaskError::NotAllowedInTaskSession(class).sqlstate(), "0A000");
        }
        // The custom T codes are pairwise distinct and each has a code default message.
        let custom: Vec<_> = table
            .iter()
            .map(|(e, _, _)| e.sqlstate())
            .filter(|c| c.as_bytes()[2] == b'T')
            .collect();
        assert_eq!(custom.len(), 7);
        for (i, a) in custom.iter().enumerate() {
            assert!(TaskError::message_for_sqlstate(a).is_some(), "{a}");
            for b in &custom[i + 1..] {
                assert_ne!(a, b);
            }
        }
        // Pass-through errors keep their own code and have no wire name.
        let sql = TaskError::Sql(SqlError::Syntax);
        assert_eq!(sql.sqlstate(), "42601");
        assert_eq!(sql.name(), None);
        let core = TaskError::Core(Box::new(CoreError::Busy));
        assert_eq!(core.sqlstate(), "53300");
        assert_eq!(core.name(), None);
        // Conversions never nest.
        assert_eq!(TaskError::from(CoreError::Busy), core);
        assert_eq!(CoreError::from(core), CoreError::Busy);
        assert_eq!(
            TaskError::from(CoreError::Task(TaskError::Revoked)),
            TaskError::Revoked
        );
        assert_eq!(
            CoreError::from(TaskError::Expired),
            CoreError::Task(TaskError::Expired)
        );
    }

    #[test]
    fn messages_are_static_and_value_free() {
        let mut messages: Vec<&'static str> = every_error().iter().map(TaskError::message).collect();
        messages.extend(
            ["42T01", "42T03", "42T04", "55T01", "28T01", "28T02", "54T01"]
                .iter()
                .filter_map(|c| TaskError::message_for_sqlstate(c)),
        );
        for message in messages {
            assert!(!message.is_empty());
            assert!(
                !message
                    .chars()
                    .any(|c| c.is_ascii_digit() || matches!(c, '"' | '\'' | '`' | '=')),
                "{message}"
            );
        }
        for error in every_error() {
            assert_eq!(error.message() == PASS_THROUGH, error.name().is_none(), "{error:?}");
        }
        for rule in DenyRule::ALL {
            assert!(rule.message().starts_with("denied: "), "{rule:?}");
        }
        assert_eq!(
            TaskError::Denied(DenyRule::HiddenColumn).message(),
            "denied: column is hidden for this task"
        );
        assert_eq!(
            TaskError::NotAllowedInTaskSession(StatementClass::TransactionControl).message(),
            "explicit transactions are not allowed in a task session"
        );
    }

    #[test]
    fn persisted_codes_are_stable_and_round_trip() {
        for (i, rule) in DenyRule::ALL.into_iter().enumerate() {
            assert_eq!(usize::from(rule.code()), i + 1);
            assert_eq!(DenyRule::from_code(rule.code()), Some(rule));
        }
        for (i, budget) in Budget::ALL.into_iter().enumerate() {
            assert_eq!(usize::from(budget.code()), i + 1);
            assert_eq!(Budget::from_code(budget.code()), Some(budget));
        }
        assert_eq!(DenyRule::from_code(0), None);
        assert_eq!(Budget::from_code(0), None);
    }

    #[test]
    fn legacy_gd_codes_unchanged() {
        for (error, code) in [
            (Error::ApprovalRequired, "GD001"),
            (Error::InvalidApproval, "GD001"),
            (Error::StalePreview, "GD001"),
            (Error::IdConflict, "GD002"),
            (Error::Revoked, "42501"),
            (Error::Expired, "42501"),
            (Error::Budget, "54000"),
        ] {
            assert_eq!(crate::sqlstate(&CoreError::Policy(error)), code, "{error:?}");
        }
        // No task error maps to a legacy GD code.
        for error in every_error() {
            assert!(!error.sqlstate().starts_with("GD"), "{error:?}");
        }
    }
}
