//! Statement admission for task sessions (GD-0061 R3-202, design-2 section 4).
//!
//! `admit` is a pure table. `ENABLED` is the staging switch: phases 3, 6 and 7 extend it in
//! their last item, so a class is never admitted before its checks exist.

use super::context::TaskContext;
use super::error::TaskError;

/// The class of a statement, as task-session admission sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatementClass {
    Select,
    Update,
    Preview,
    Apply,
    OutcomeLookup,
    Show,
    Set,
    // Everything else:
    Insert,
    Delete,
    Ddl,
    Copy,
    TransactionControl,
    Savepoint,
    RoleStatement,
    Explain,
    Cursor,
    Lock,
    Catalog,
    Other,
}

impl StatementClass {
    /// Every class, for exhaustive tests.
    pub const ALL: [StatementClass; 19] = [
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
}

/// Classes a task session may run in this build. Phase 3 adds `Select`; phase 6 (its last
/// item, R3-626) has added `Update` and `OutcomeLookup`; phase 7 (R3-707, the item that
/// wires the SQL statements once preview, approval and apply exist) has added `Preview`
/// and `Apply`.
pub const ENABLED: &[StatementClass] = &[
    StatementClass::Show,
    StatementClass::Set,
    StatementClass::Update,
    StatementClass::OutcomeLookup,
    StatementClass::Preview,
    StatementClass::Apply,
];

/// `None` is not a task session and is always admitted.
pub fn admit(ctx: Option<&TaskContext>, class: StatementClass) -> Result<(), TaskError> {
    match ctx {
        None => Ok(()),
        Some(_) if ENABLED.contains(&class) => Ok(()),
        Some(_) => Err(TaskError::NotAllowedInTaskSession(class)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::definition::{TaskName, TaskObjectId};

    fn ctx() -> TaskContext {
        TaskContext {
            task: TaskObjectId(1),
            name: TaskName::new("inc-436").unwrap(),
            executor: "agent_exec".to_string(),
            bound_def_version: 1,
            credential_fingerprint: [7; 32],
        }
    }

    #[test]
    fn admit_none_is_always_ok() {
        for class in StatementClass::ALL {
            assert!(admit(None, class).is_ok(), "{class:?}");
        }
    }

    #[test]
    fn default_refuse_every_class_not_enabled() {
        let c = ctx();
        for class in StatementClass::ALL {
            let got = admit(Some(&c), class);
            if ENABLED.contains(&class) {
                assert!(got.is_ok(), "{class:?}");
            } else {
                assert!(
                    matches!(got, Err(TaskError::NotAllowedInTaskSession(k)) if k == class),
                    "{class:?}: {got:?}"
                );
            }
        }
        assert_eq!(
            ENABLED,
            &[
                StatementClass::Show,
                StatementClass::Set,
                StatementClass::Update,
                StatementClass::OutcomeLookup,
                StatementClass::Preview,
                StatementClass::Apply,
            ]
        );
    }
}
