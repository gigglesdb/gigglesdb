//! Internal application reducer. Only trusted owner commands provision authority;
//! application claims never create it. The caller must persist returned Image and
//! applied index atomically. There is no public listener or task budget authority.
use super::CoreError;
use crate::operations::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
mod durable;
pub mod partition;
pub(crate) use durable::Ledger;
pub use durable::{
    Error, Failure, MAX_AUTHORITIES, MAX_LEDGER_BYTES, MAX_OUTCOMES, Outcome, OwnerCommand, Success,
};

pub const CONTRACT_VERSION: u32 = 1;
pub const MAX_TABLES: usize = 64;
pub const MAX_KEY_VALUES: usize = 64;
pub const MAX_ASSIGNMENTS: usize = 64;
pub const MAX_VALUE_BYTES: usize = crate::operations::MAX_BINDING_BYTES;

/// Trusted runtime-owned authority, separate from task grants and remote input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authority {
    pub application: u128,
    pub generation: u64,
    pub domain: u128,
    pub catalog_epoch: u64,
    pub read_tables: BTreeSet<u64>,
    pub write_tables: BTreeSet<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Identity {
    pub application: u128,
    pub authority_generation: u64,
    pub operation: u128,
}

/// Closed reserved grammar: complete primary key, bounded literal values only.
/// Catalog binding must later verify key arity/types and immutable table IDs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    Read {
        key: Vec<Value>,
    },
    GuardedWrite {
        key: Vec<Value>,
        expected_version: u64,
        /// Stable column IDs; primary-key changes are unavailable.
        assignments: Vec<(u64, Value)>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Body {
    pub contract_version: u32,
    pub domain: u128,
    pub catalog_epoch: u64,
    pub table: u64,
    pub operation: Operation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub identity: Identity,
    pub body: Body,
}

impl Command {
    /// Structural bounds only. This does not authenticate or authorize a caller.
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.body.contract_version != CONTRACT_VERSION {
            return Err(CoreError::Unsupported);
        }
        let (key, assignments) = match &self.body.operation {
            Operation::Read { key } => (key, &[][..]),
            Operation::GuardedWrite {
                key,
                expected_version,
                assignments,
            } => {
                if *expected_version == 0 || assignments.is_empty() {
                    return Err(CoreError::Unsupported);
                }
                (key, assignments.as_slice())
            }
        };
        if key.is_empty() || key.len() > MAX_KEY_VALUES || assignments.len() > MAX_ASSIGNMENTS {
            return Err(CoreError::Limit);
        }
        let mut ids = BTreeSet::new();
        let mut remaining = MAX_VALUE_BYTES;
        for value in key.iter().chain(assignments.iter().map(|(_, value)| value)) {
            remaining = remaining
                .checked_sub(crate::relational::value_bytes(value))
                .ok_or(CoreError::Limit)?;
        }
        for (id, _) in assignments {
            if !ids.insert(id) {
                return Err(CoreError::Unsupported);
            }
        }
        Ok(())
    }

    /// Admit against independently selected authority, never remote claims.
    pub fn admit(&self, authority: &Authority) -> Result<(), CoreError> {
        self.validate()?;
        if authority.read_tables.len() > MAX_TABLES || authority.write_tables.len() > MAX_TABLES {
            return Err(CoreError::Limit);
        }
        let allowed = match self.body.operation {
            Operation::Read { .. } => authority.read_tables.contains(&self.body.table),
            Operation::GuardedWrite { .. } => {
                authority.read_tables.contains(&self.body.table)
                    && authority.write_tables.contains(&self.body.table)
            }
        };
        if self.identity.application != authority.application
            || self.identity.authority_generation != authority.generation
            || self.body.domain != authority.domain
            || self.body.catalog_epoch != authority.catalog_epoch
            || !allowed
        {
            return Err(CoreError::Unsupported);
        }
        Ok(())
    }

    /// Exact identity/body equality only; this is not a durable retry lookup.
    pub fn same_submission(&self, other: &Self) -> bool {
        self == other
    }
}

#[cfg(test)]
mod tests;
