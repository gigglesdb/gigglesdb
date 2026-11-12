//! Seeded SQL traffic and its independently persisted, acknowledged history.
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    ops::RangeInclusive,
    path::Path,
};

pub const TABLES: [&str; 3] = ["campaign_accounts", "campaign_orders", "campaign_notes"];

#[derive(Clone, Debug)]
pub struct Config {
    pub seed: u64,
    pub transactions: usize,
    pub hot_range: RangeInclusive<i64>,
    pub hot_percent: u8,
    pub toast_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            seed: 1,
            transactions: 48,
            hot_range: 10..=19,
            hot_percent: 85,
            toast_bytes: 16_384,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Toast {
    pub text: String,
    /// Hexadecimal bytea, so the history and SQL reads are lossless plain text.
    pub bytes: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub key: i64,
    pub counter: i64,
    pub numeric: String,
    /// UTC text, including PostgreSQL's infinity spellings.
    pub timestamp: String,
    pub toast: Toast,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    Insert {
        table: String,
        row: Row,
    },
    Update {
        table: String,
        key: i64,
        new_key: Option<i64>,
        delta: i64,
        toast: Option<Toast>,
    },
    Delete {
        table: String,
        key: i64,
    },
    Truncate {
        tables: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Statement {
    pub sql: String,
    pub operation: Operation,
}

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

impl Statement {
    pub fn new(operation: Operation) -> Self {
        let sql = match &operation {
            Operation::Insert { table, row } => format!(
                "INSERT INTO {table} (id, counter, amount, at_time, body, payload) VALUES ({}, {}, {}::numeric, {}::timestamptz, {}, decode({}, 'hex')) ON CONFLICT (id) DO UPDATE SET counter=EXCLUDED.counter, amount=EXCLUDED.amount, at_time=EXCLUDED.at_time, body=EXCLUDED.body, payload=EXCLUDED.payload",
                row.key,
                row.counter,
                quoted(&row.numeric),
                quoted(&row.timestamp),
                quoted(&row.toast.text),
                quoted(&row.toast.bytes)
            ),
            Operation::Update {
                table,
                key,
                new_key,
                delta,
                toast,
            } => {
                let mut sets = vec![format!("counter=counter+({delta})")];
                if let Some(key) = new_key {
                    sets.push(format!("id={key}"));
                }
                if let Some(toast) = toast {
                    sets.push(format!("body={}", quoted(&toast.text)));
                    sets.push(format!("payload=decode({}, 'hex')", quoted(&toast.bytes)));
                }
                format!("UPDATE {table} SET {} WHERE id={key}", sets.join(", "))
            }
            Operation::Delete { table, key } => format!("DELETE FROM {table} WHERE id={key}"),
            Operation::Truncate { tables } => format!("TRUNCATE {}", tables.join(", ")),
        };
        Self { sql, operation }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Committed,
    RolledBack,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    pub txn_id: u64,
    pub statements: Vec<Statement>,
    pub outcome: Outcome,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub txn_id: u64,
    pub statements: Vec<Statement>,
    pub outcome: Outcome,
    /// pg_current_wal_lsn(), sampled inside the transaction just before COMMIT
    /// (or ROLLBACK). This is the campaign's upper-bound ordering convention,
    /// not the commit-record LSN: physically the sample precedes that record.
    /// The executor is serial; concurrent writers require a true commit-order
    /// source. Equal samples retain history-file order in the oracle.
    pub commit_lsn: u64,
}

struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut n = self.0;
        n = (n ^ (n >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        n = (n ^ (n >> 27)).wrapping_mul(0x94d049bb133111eb);
        n ^ (n >> 31)
    }
    fn key(&mut self, cfg: &Config) -> i64 {
        let (low, high) = if self.next() % 100 < u64::from(cfg.hot_percent) {
            (*cfg.hot_range.start(), *cfg.hot_range.end())
        } else {
            (0, 99_999)
        };
        (i128::from(low) + i128::from(self.next()) % (i128::from(high) - i128::from(low) + 1))
            as i64
    }
    fn toast(&mut self, bytes: usize) -> Toast {
        let mut text = String::new();
        while text.len() < bytes {
            text.push_str(&format!("{:016x}", self.next()));
        }
        text.truncate(bytes);
        let payload = text.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
        Toast {
            text,
            bytes: payload,
        }
    }
}

/// Every 12 transactions exercise all classes, even when starting with no rows.
/// Inserts are explicit upserts so small hot ranges cannot exhaust vacant keys.
pub fn generate(cfg: &Config) -> Vec<Transaction> {
    assert!(cfg.hot_percent <= 100);
    assert!(cfg.hot_range.start() < cfg.hot_range.end());
    let mut random = Random(cfg.seed);
    let mut result = Vec::new();
    let mut known = BTreeSet::<(String, i64)>::new();
    for index in 0..cfg.transactions {
        let table = TABLES[index % TABLES.len()].to_owned();
        let mut key = random.key(cfg);
        let modifies_existing = matches!(index % 12, 1..=4 | 11);
        if modifies_existing {
            let hot = cfg.hot_range.contains(&key);
            let candidates: Vec<_> = known
                .iter()
                .filter(|(t, k)| t == &table && (!hot || cfg.hot_range.contains(k)))
                .collect();
            if !candidates.is_empty() {
                key = candidates[random.next() as usize % candidates.len()].1;
            }
        }
        let row = Row {
            key,
            counter: (random.next() % 1000) as i64,
            numeric: [
                "-999999999999999999999999999999.0000000001",
                "0",
                "999999999999999999999999999999.9999999999",
                "NaN",
            ][index % 4]
                .into(),
            timestamp: [
                "-infinity",
                "infinity",
                "0001-01-01 00:00:00+00",
                "9999-12-31 23:59:59.999999+00",
            ][index % 4]
                .into(),
            toast: random.toast(cfg.toast_bytes),
        };
        let mut operations = Vec::new();
        if !modifies_existing || !known.contains(&(table.clone(), key)) {
            operations.push(Operation::Insert {
                table: table.clone(),
                row: row.clone(),
            });
        }
        match index % 12 {
            1 | 11 => {
                for _ in 0..if index % 12 == 11 { 3 } else { 1 } {
                    operations.push(Operation::Update {
                        table: table.clone(),
                        key,
                        new_key: None,
                        delta: 1,
                        toast: None,
                    });
                }
            }
            2 => operations.push(Operation::Update {
                table,
                key,
                new_key: None,
                delta: -2,
                toast: Some(random.toast(cfg.toast_bytes)),
            }),
            3 => {
                let mut new_key = random.key(cfg);
                if new_key == key {
                    new_key = if key == *cfg.hot_range.start() {
                        *cfg.hot_range.end()
                    } else {
                        *cfg.hot_range.start()
                    };
                }
                operations.push(Operation::Delete {
                    table: table.clone(),
                    key: new_key,
                });
                operations.push(Operation::Update {
                    table,
                    key,
                    new_key: Some(new_key),
                    delta: 3,
                    toast: None,
                });
            }
            4 => operations.push(Operation::Delete { table, key }),
            5 | 6 => operations.push(Operation::Insert {
                table: TABLES[(index + 1) % 3].into(),
                row,
            }),
            9 => operations.push(Operation::Truncate {
                tables: vec![table],
            }),
            10 => operations.push(Operation::Truncate {
                tables: TABLES.iter().map(|s| (*s).into()).collect(),
            }),
            _ => {}
        }
        let outcome = if index % 12 == 6 {
            Outcome::RolledBack
        } else {
            Outcome::Committed
        };
        // Track identities only to select effective updates. Expected values
        // are exclusively the oracle's responsibility, never generated here.
        if outcome == Outcome::Committed {
            for operation in &operations {
                match operation {
                    Operation::Insert { table, row } => {
                        known.insert((table.clone(), row.key));
                    }
                    Operation::Update {
                        table,
                        key,
                        new_key,
                        ..
                    } => {
                        if let Some(new_key) = new_key {
                            known.remove(&(table.clone(), *key));
                            known.insert((table.clone(), *new_key));
                        }
                    }
                    Operation::Delete { table, key } => {
                        known.remove(&(table.clone(), *key));
                    }
                    Operation::Truncate { tables } => {
                        known.retain(|(table, _)| !tables.contains(table))
                    }
                }
            }
        }
        result.push(Transaction {
            txn_id: index as u64 + 1,
            statements: operations.into_iter().map(Statement::new).collect(),
            outcome,
        });
    }
    result
}

pub fn read_history(path: &Path) -> Result<Vec<HistoryEntry>, Box<dyn std::error::Error>> {
    std::fs::read_to_string(path)?
        .lines()
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

pub fn parse_lsn(value: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let (high, low) = value.split_once('/').ok_or("invalid WAL LSN")?;
    let high = u32::from_str_radix(high, 16)?;
    let low = u32::from_str_radix(low, 16)?;
    Ok((u64::from(high) << 32) | u64::from(low))
}

/// Refuse database access unless invoked by the harness with an explicit URL.
pub fn harness_url(variable: &str) -> Option<String> {
    if std::env::var_os("SKEDADDLE_TEST_CONFIG").is_none() {
        let _ = writeln!(
            std::io::stderr(),
            "SKIP campaign live round trip: harness/run.ps1 configuration is required"
        );
        return None;
    }
    match std::env::var(variable) {
        Ok(value) if !value.is_empty() => Some(value),
        _ => {
            let _ = writeln!(
                std::io::stderr(),
                "SKIP campaign live round trip: harness must set {variable}"
            );
            None
        }
    }
}

pub async fn create_tables(client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    for table in TABLES {
        client.batch_execute(&format!("CREATE TABLE {table} (id bigint PRIMARY KEY, counter bigint NOT NULL, amount numeric NOT NULL, at_time timestamptz NOT NULL, body text NOT NULL, payload bytea NOT NULL); ALTER TABLE {table} ALTER COLUMN body SET STORAGE EXTERNAL; ALTER TABLE {table} ALTER COLUMN payload SET STORAGE EXTERNAL")).await?;
    }
    Ok(())
}

/// Append only acknowledged outcomes, synchronizing each JSON line after the
/// database acknowledgment. A lost connection/ack is an error, never a guessed
/// commit. A crash between acknowledgment and append is outside this helper's
/// contract; the fault-injecting campaign must retain that uncertainty.
pub async fn execute(
    client: &mut tokio_postgres::Client,
    transactions: &[Transaction],
    path: &Path,
) -> Result<Vec<HistoryEntry>, Box<dyn std::error::Error>> {
    assert!(
        std::env::var_os("SKEDADDLE_TEST_CONFIG").is_some(),
        "live execution requires harness/run.ps1"
    );
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    let mut history = Vec::new();
    for planned in transactions {
        let transaction = client.transaction().await?;
        for statement in &planned.statements {
            transaction.batch_execute(&statement.sql).await?;
        }
        let sampled: String = transaction
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await?
            .get(0);
        let commit_lsn = parse_lsn(&sampled)?;
        match planned.outcome {
            Outcome::Committed => transaction.commit().await?,
            Outcome::RolledBack => transaction.rollback().await?,
        }
        let entry = HistoryEntry {
            txn_id: planned.txn_id,
            statements: planned.statements.clone(),
            outcome: planned.outcome,
            commit_lsn,
        };
        serde_json::to_writer(&mut file, &entry)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        history.push(entry);
    }
    Ok(history)
}

pub type State = BTreeMap<(String, i64), Row>;
