//! Effects collector for protected statements (GD-0061 design section 6, D-D).
//!
//! `SqlRows::apply` records every row change of a protected run here. One
//! entry per physical key (`table_id BE || pk`): the first `old` and the last
//! `new` survive. The commit cross-checks the collected keys against the
//! transaction's buffered row writes before recording anything.

use crate::relational::SqlRow;
use crate::CoreError;
use std::collections::{BTreeMap, BTreeSet};

pub type TableId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowEffect {
    pub table: TableId,
    pub pk: Vec<u8>,
    pub old: Option<SqlRow>,
    pub new: Option<SqlRow>,
}

#[derive(Debug, Default, Clone)]
pub struct Effects {
    rows: BTreeMap<Vec<u8>, RowEffect>,
}

impl Effects {
    /// Merges one row change. A later change's `old` must equal the stored
    /// `new` (values and version), else the chain is corrupt.
    pub(crate) fn record(
        &mut self,
        table: u64,
        physical: &[u8],
        old: Option<&SqlRow>,
        new: Option<&SqlRow>,
    ) -> Result<(), CoreError> {
        if physical.len() < 8 || physical[..8] != table.to_be_bytes() {
            return Err(CoreError::Corrupt("effects key".into()));
        }
        match self.rows.get_mut(physical) {
            Some(entry) => {
                if entry.new.as_ref() != old {
                    return Err(CoreError::Corrupt("effects chain".into()));
                }
                entry.new = new.cloned();
                if entry.old.is_none() && entry.new.is_none() {
                    self.rows.remove(physical);
                }
            }
            None => {
                if old.is_none() && new.is_none() {
                    return Ok(());
                }
                self.rows.insert(
                    physical.to_vec(),
                    RowEffect {
                        table,
                        pk: physical[8..].to_vec(),
                        old: old.cloned(),
                        new: new.cloned(),
                    },
                );
            }
        }
        Ok(())
    }

    /// The set of written row keys must equal the set of effect keys.
    /// Deviation: takes the written keys rather than `&Transaction`, because
    /// `storage::engine` is private to `storage`; the storage-side wrapper
    /// passes `txn.written_keys(Keyspace::Rows)`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn cross_check<'a>(
        &self,
        written: impl IntoIterator<Item = &'a Vec<u8>>,
    ) -> Result<(), CoreError> {
        let written = written.into_iter().collect::<BTreeSet<_>>();
        let ours = self.rows.keys().collect::<BTreeSet<_>>();
        if written != ours {
            return Err(CoreError::Corrupt("protected effects incomplete".into()));
        }
        Ok(())
    }

    /// Rows in physical-key order.
    pub fn iter(&self) -> impl Iterator<Item = &RowEffect> {
        self.rows.values()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// A no-op still carries a new row version; it is decided by values only.
    pub fn is_noop(e: &RowEffect) -> bool {
        matches!((&e.old, &e.new), (Some(o), Some(n)) if o.values == n.values)
    }
}

#[cfg(test)]
#[path = "effects_tests.rs"]
mod tests;
