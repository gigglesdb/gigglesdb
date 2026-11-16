//! Conservative cell accounting, including primary keys and index maintenance.
use std::collections::{BTreeMap, BTreeSet};

/// Pinned 2026-11-15: https://cloud.google.com/spanner/quotas#limits_for_creating_reading_updating_and_deleting_data
pub const DEFAULT_MUTATION_LIMIT: u64 = 80_000;
pub const REPLAY_CELLS: u64 = 6;
pub const DEFAULT_REPLAY_TABLE: &str = "skedaddle_apply";

#[derive(Clone, Debug)]
pub struct Config {
    pub mutation_limit: u64,
    /// Skedaddle-owned replay table in the default schema (D-A5).
    pub replay_table: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            mutation_limit: DEFAULT_MUTATION_LIMIT,
            replay_table: DEFAULT_REPLAY_TABLE.into(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitExceeded {
    pub estimated: u64,
    pub limit: u64,
}
impl std::fmt::Display for LimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Spanner mutation admission: {} exceeds {}",
            self.estimated, self.limit
        )
    }
}
impl std::error::Error for LimitExceeded {}

#[derive(Clone, Debug, Default)]
pub struct Table {
    pub primary_key: Vec<String>,
    /// Includes key and stored columns, including Spanner's hidden FK indexes.
    pub indexes: BTreeMap<String, BTreeSet<String>>,
    pub columns: BTreeSet<String>,
    pub array_columns: BTreeSet<String>,
    pub generated_columns: BTreeSet<String>,
    pub default_columns: BTreeSet<String>,
    pub cascade_children: bool,
}
pub type Catalog = BTreeMap<String, Table>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Insert,
    Update,
    Delete,
}

/// Updates charge both old and new index cells whenever a key/stored column
/// is touched. Null-filtered indexes are charged even when no entry is emitted.
/// This deliberately overestimates; ranges, cascades and array indexes require
/// cardinality information and are rejected by the apply adapter.
pub fn cells(table: &Table, kind: Kind, written: &BTreeSet<String>) -> u64 {
    let affected: BTreeSet<_> = written
        .iter()
        .chain(table.primary_key.iter())
        .chain(table.generated_columns.iter())
        .chain(table.default_columns.iter())
        .collect();
    // Quotas note 10 charges generated/default columns and primary keys in
    // addition to explicitly written cells, including explicitly written keys.
    let base = if kind == Kind::Delete {
        1
    } else if !table.generated_columns.is_empty() || !table.default_columns.is_empty() {
        (written.len()
            + table.generated_columns.len()
            + table.default_columns.len()
            + table.primary_key.len()) as u64
    } else {
        affected.len() as u64
    };
    table.indexes.values().fold(base, |count, columns| {
        let extra = match kind {
            Kind::Delete => 1,
            Kind::Insert => columns.len() as u64,
            Kind::Update if columns.iter().any(|c| affected.contains(c)) => {
                (columns.len() as u64).saturating_mul(2)
            }
            Kind::Update => 0,
        };
        count.saturating_add(extra)
    })
}
pub fn admit(config: &Config, counts: impl IntoIterator<Item = u64>) -> Result<u64, LimitExceeded> {
    let estimated = counts.into_iter().fold(0u64, u64::saturating_add);
    if estimated > config.mutation_limit {
        Err(LimitExceeded {
            estimated,
            limit: config.mutation_limit,
        })
    } else {
        Ok(estimated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn names(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|s| (*s).into()).collect()
    }
    #[test]
    fn keys_indexes_stored_columns_and_deletes() {
        let table = Table {
            primary_key: vec!["id".into()],
            indexes: [("ix".into(), names(&["id", "value", "stored"]))].into(),
            ..Table::default()
        };
        assert_eq!(
            cells(&table, Kind::Insert, &names(&["id", "value", "stored"])),
            6
        );
        assert_eq!(cells(&table, Kind::Update, &names(&["value"])), 8);
        assert_eq!(cells(&table, Kind::Delete, &names(&[])), 2);
    }
    #[test]
    fn documented_default_and_replay_boundary() {
        let config = Config::default();
        assert_eq!(config.mutation_limit, 80_000);
        assert_eq!(admit(&config, [79_994, REPLAY_CELLS]), Ok(80_000));
        assert_eq!(
            admit(&config, [79_995, REPLAY_CELLS]),
            Err(LimitExceeded {
                estimated: 80_001,
                limit: 80_000
            })
        );
        assert!(admit(&config, [u64::MAX, REPLAY_CELLS]).is_err());
    }
    #[test]
    fn documented_generated_and_default_column_accounting() {
        let mut table = Table {
            primary_key: vec!["a".into(), "b".into()],
            generated_columns: names(&["g"]),
            ..Default::default()
        };
        assert_eq!(cells(&table, Kind::Insert, &names(&["a", "b"])), 5);
        table.default_columns = names(&["d"]);
        assert_eq!(cells(&table, Kind::Insert, &names(&["a", "b"])), 6);
        assert_eq!(cells(&table, Kind::Delete, &names(&["a", "b"])), 1);
    }
}
