//! Expected state depends exclusively on acknowledged workload history.
//! No decoder, mapper, event, merge, or applier is used here.
use super::workload::{HistoryEntry, Operation, Outcome, Row, State, Toast};
use std::collections::{BTreeMap, BTreeSet};

type Key = (String, i64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Anomaly {
    MissingTransaction {
        txn_id: u64,
    },
    DuplicatedEffect {
        txn_id: u64,
        table: String,
        key: i64,
    },
    PartialTransaction {
        txn_id: u64,
    },
    ResurrectedDelete {
        txn_id: u64,
        table: String,
        key: i64,
    },
    StaleOverwrite {
        txn_id: u64,
        table: String,
        key: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowDiff {
    pub table: String,
    pub key: i64,
    pub expected: Option<Row>,
    pub actual: Option<Row>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Diff {
    pub rows: Vec<RowDiff>,
    /// Evidence consistent with each anomaly, not a unique causal diagnosis.
    /// Final-state reads cannot detect a duplicate idempotent write, or a lost
    /// transaction whose effects were completely overwritten by later commits.
    pub anomalies: Vec<Anomaly>,
}

impl Diff {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

struct Commit {
    id: u64,
    before: State,
    after: State,
    deltas: BTreeMap<Key, i64>,
}

pub struct Oracle {
    pub expected: State,
    commits: Vec<Commit>,
    versions: BTreeMap<Key, Vec<Row>>,
    owners: BTreeMap<Key, usize>,
}

impl Oracle {
    pub fn replay(history: &[HistoryEntry]) -> Self {
        let mut committed: Vec<_> = history
            .iter()
            .filter(|entry| entry.outcome == Outcome::Committed)
            .collect();
        committed.sort_by_key(|entry| entry.commit_lsn);
        let mut oracle = Self {
            expected: State::new(),
            commits: Vec::new(),
            versions: BTreeMap::new(),
            owners: BTreeMap::new(),
        };
        for entry in committed {
            let mut before = oracle.expected.clone();
            let mut touched = BTreeSet::new();
            let mut deltas = BTreeMap::new();
            for statement in &entry.statements {
                match &statement.operation {
                    Operation::Insert { table, row } => {
                        let key = (table.clone(), row.key);
                        touched.insert(key.clone());
                        deltas.remove(&key);
                        oracle.expected.insert(key, row.clone());
                    }
                    Operation::Update {
                        table,
                        key,
                        new_key,
                        delta,
                        toast,
                    } => {
                        let old = (table.clone(), *key);
                        if let Some(mut row) = oracle.expected.remove(&old) {
                            touched.insert(old.clone());
                            row.counter += delta;
                            if let Some(toast) = toast {
                                row.toast = toast.clone();
                            }
                            row.key = new_key.unwrap_or(*key);
                            let new = (table.clone(), row.key);
                            touched.insert(new.clone());
                            let prior = deltas.remove(&old).unwrap_or(0);
                            deltas.insert(new.clone(), prior + delta);
                            oracle.expected.insert(new, row);
                        }
                    }
                    Operation::Delete { table, key } => {
                        let key = (table.clone(), *key);
                        touched.insert(key.clone());
                        deltas.remove(&key);
                        oracle.expected.remove(&key);
                    }
                    Operation::Truncate { tables } => {
                        let keys: Vec<_> = oracle
                            .expected
                            .keys()
                            .filter(|(table, _)| tables.contains(table))
                            .cloned()
                            .collect();
                        for key in keys {
                            touched.insert(key.clone());
                            deltas.remove(&key);
                            oracle.expected.remove(&key);
                        }
                    }
                }
                // Retain intermediate versions too: an old value can leak from
                // any statement in a transaction (not just its final image).
                for key in &touched {
                    if let Some(row) = oracle.expected.get(key) {
                        let versions = oracle.versions.entry(key.clone()).or_default();
                        if versions.last() != Some(row) {
                            versions.push(row.clone());
                        }
                    }
                }
            }
            for key in &touched {
                oracle.owners.insert(key.clone(), oracle.commits.len());
            }
            before.retain(|key, _| touched.contains(key));
            let after = touched
                .iter()
                .filter_map(|key| {
                    oracle
                        .expected
                        .get(key)
                        .map(|row| (key.clone(), row.clone()))
                })
                .collect();
            oracle.commits.push(Commit {
                id: entry.txn_id,
                before,
                after,
                deltas,
            });
        }
        oracle
    }

    pub fn compare(&self, actual: &State) -> Diff {
        let mut diff = Diff::default();
        let keys: BTreeSet<_> = self.expected.keys().chain(actual.keys()).cloned().collect();
        for (table, key) in keys {
            let identity = (table.clone(), key);
            let expected = self.expected.get(&identity);
            let observed = actual.get(&identity);
            if expected == observed {
                continue;
            }
            diff.rows.push(RowDiff {
                table: table.clone(),
                key,
                expected: expected.cloned(),
                actual: observed.cloned(),
            });
            if let Some(owner) = self.owners.get(&identity) {
                let commit = &self.commits[*owner];
                if expected.is_none() && observed.is_some() {
                    diff.anomalies.push(Anomaly::ResurrectedDelete {
                        txn_id: commit.id,
                        table: table.clone(),
                        key,
                    });
                }
                if let (Some(want), Some(got)) = (expected, observed) {
                    if self
                        .versions
                        .get(&identity)
                        .is_some_and(|versions| versions.contains(got))
                    {
                        diff.anomalies.push(Anomaly::StaleOverwrite {
                            txn_id: commit.id,
                            table: table.clone(),
                            key,
                        });
                    }
                    if let Some(delta) = commit.deltas.get(&identity).filter(|delta| **delta != 0) {
                        let mut duplicate = want.clone();
                        if let Some(counter) = duplicate.counter.checked_add(*delta) {
                            duplicate.counter = counter;
                            if &duplicate == got {
                                diff.anomalies.push(Anomaly::DuplicatedEffect {
                                    txn_id: commit.id,
                                    table: table.clone(),
                                    key,
                                });
                            }
                        }
                    }
                }
            }
        }
        for (index, commit) in self.commits.iter().enumerate() {
            let terminal: Vec<_> = self
                .owners
                .iter()
                .filter(|(_, owner)| **owner == index)
                .map(|(key, _)| key)
                .filter(|key| commit.before.get(*key) != commit.after.get(*key))
                .collect();
            if terminal.is_empty() {
                continue;
            }
            let matching = terminal
                .iter()
                .filter(|key| actual.get(*key) == commit.after.get(*key))
                .count();
            if matching == 0
                && terminal
                    .iter()
                    .all(|key| actual.get(*key) == commit.before.get(*key))
            {
                diff.anomalies
                    .push(Anomaly::MissingTransaction { txn_id: commit.id });
            } else if matching > 0 && matching < terminal.len() {
                diff.anomalies
                    .push(Anomaly::PartialTransaction { txn_id: commit.id });
            }
        }
        diff
    }
}

/// A separate plain SQL read, deliberately not the migration engine's reader.
/// UTC formatting avoids session timezone/DateStyle differences. Numeric scale
/// is preserved, as are NaN and infinity spellings.
pub async fn read_destination(
    client: &mut tokio_postgres::Client,
    tables: &[&str],
) -> Result<State, Box<dyn std::error::Error>> {
    let transaction = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    transaction
        .batch_execute("SET LOCAL TIME ZONE 'UTC'")
        .await?;
    let mut state = State::new();
    for table in tables {
        if table.is_empty()
            || !table
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err("unsafe table identifier".into());
        }
        let sql = format!(
            "SELECT id, counter, amount::text, CASE WHEN isfinite(at_time) THEN to_char(at_time, 'YYYY-MM-DD HH24:MI:SS.US') || '+00' ELSE at_time::text END, body, encode(payload, 'hex') FROM \"{table}\" ORDER BY id"
        );
        for record in transaction.query(&sql, &[]).await? {
            let mut timestamp: String = record.get(3);
            // The generator emits seconds without a fractional part at year 1.
            timestamp = timestamp.replace(".000000+00", "+00");
            let row = Row {
                key: record.get(0),
                counter: record.get(1),
                numeric: record.get(2),
                timestamp,
                toast: Toast {
                    text: record.get(4),
                    bytes: record.get(5),
                },
            };
            state.insert(((*table).into(), row.key), row);
        }
    }
    transaction.commit().await?;
    Ok(state)
}
