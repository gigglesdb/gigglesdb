//! Both dialects use Mutation API writes, so one commit owns data and replay.
//! Values are already qualified/mapped by the caller (as in PgApplier's planner).
//! No source timestamp is replaced with a pending commit timestamp.
use super::limits::{self, Catalog, Config, Kind};
use crate::{definition::SpannerDialect, event::AssembledTxn};
use google_cloud_spanner::{
    client::{DatabaseClient, Spanner},
    key::{Key, KeySet},
    mutation::Mutation,
    statement::Statement,
    value::{Timestamp, Value},
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::Duration};

fn replay_identifier(name: &str, dialect: SpannerDialect) -> Result<String> {
    let max_len = if dialect == SpannerDialect::GoogleSql {
        128
    } else {
        63
    };
    if name.is_empty()
        || name.len() > max_len
        || !name.as_bytes()[0].is_ascii_alphabetic()
        || !name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
    {
        return Err(Error::Invalid("invalid replay table name"));
    }
    let quote = if dialect == SpannerDialect::GoogleSql {
        '`'
    } else {
        '"'
    };
    Ok(format!("{quote}{name}{quote}"))
}

/// Disclosed installation step: creates the configured Skedaddle-owned table.
/// S10 preflight must reject an existing user table with a different shape.
pub fn replay_ddl(config: &Config, dialect: SpannerDialect) -> Result<String> {
    let table = replay_identifier(&config.replay_table, dialect)?;
    Ok(if dialect == SpannerDialect::GoogleSql {
        format!(
            "CREATE TABLE {table} (source_txn_id STRING(64) NOT NULL, source_txn_digest STRING(64) NOT NULL, source_identity STRING(MAX) NOT NULL, txn_identity STRING(MAX) NOT NULL, source_position STRING(MAX) NOT NULL, applied_at TIMESTAMP NOT NULL) PRIMARY KEY (source_txn_id)"
        )
    } else {
        format!(
            "CREATE TABLE {table} (source_txn_id varchar(64) PRIMARY KEY, source_txn_digest varchar(64) NOT NULL, source_identity varchar NOT NULL, txn_identity varchar NOT NULL, source_position varchar NOT NULL, applied_at timestamptz NOT NULL)"
        )
    })
}

#[derive(Debug)]
pub enum Error {
    Limit(limits::LimitExceeded),
    Conflict,
    Invalid(&'static str),
    Database,
    UnknownOutcome,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limit(e) => e.fmt(f),
            other => write!(f, "Spanner apply: {other:?}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<google_cloud_spanner::Error> for Error {
    fn from(_: google_cloud_spanner::Error) -> Self {
        Self::Database
    }
}
pub type Result<T> = std::result::Result<T, Error>;

/// Single-row writes only. All primary-key columns must be supplied. The
/// planner omits generated/unchanged columns and maps values before this seam.
#[derive(Clone, Debug)]
pub struct Write {
    pub table: String,
    pub kind: Kind,
    pub values: BTreeMap<String, Value>,
}
impl Write {
    fn mutation(&self, catalog: &Catalog, replay_table: &str) -> Result<(Mutation, u64)> {
        if self.table.eq_ignore_ascii_case(replay_table) {
            return Err(Error::Invalid("reserved table"));
        }
        let table = catalog
            .get(&self.table)
            .ok_or(Error::Invalid("unqualified table"))?;
        if table.primary_key.is_empty()
            || table
                .primary_key
                .iter()
                .any(|k| self.values.get(k).is_none_or(Value::is_null))
        {
            return Err(Error::Invalid("missing primary key"));
        }
        if self.values.keys().any(|k| !table.columns.contains(k)) {
            return Err(Error::Invalid("unknown column"));
        }
        if table
            .indexes
            .values()
            .any(|index| !index.is_disjoint(&table.array_columns))
        {
            return Err(Error::Invalid(
                "array indexes require cardinality qualification",
            ));
        }
        if self
            .values
            .keys()
            .any(|k| table.generated_columns.contains(k))
        {
            return Err(Error::Invalid("generated columns must be omitted"));
        }
        if self.kind == Kind::Delete
            && (table.cascade_children
                || self.values.keys().any(|k| !table.primary_key.contains(k)))
        {
            return Err(Error::Invalid(
                "delete requires exact key and no cascading children",
            ));
        }
        let count = limits::cells(table, self.kind, &self.values.keys().cloned().collect());
        let mutation = if self.kind == Kind::Delete {
            Mutation::delete(
                &self.table,
                KeySet::from(Key::new(
                    table
                        .primary_key
                        .iter()
                        .map(|k| self.values[k].clone())
                        .collect(),
                )),
            )
        } else {
            let builder = if self.kind == Kind::Insert {
                Mutation::new_insert_builder(&self.table)
            } else {
                Mutation::new_update_builder(&self.table)
            };
            self.values
                .iter()
                .fold(builder, |b, (k, v)| b.set(k).to(v.clone()))
                .build()
        };
        Ok((mutation, count))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Applied,
    Replayed,
}
/// Fault injection only: the official SDK commits successfully, then this
/// wrapper discards its reply. Never enabled from process environment.
#[derive(Clone, Copy, Debug, Default)]
pub enum CommitFault {
    #[default]
    None,
    LoseReplyOnce,
}

pub struct Destination {
    pub database: DatabaseClient,
    pub dialect: SpannerDialect,
    pub config: Config,
    pub fault: CommitFault,
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
struct Replay {
    key: String,
    digest: String,
    source: String,
    txn: String,
    position: String,
}
impl Replay {
    fn new(t: &AssembledTxn) -> Self {
        let source = serde_json::to_string(&(
            &t.source.engine,
            &t.source.system_id,
            &t.source.lineage,
            &t.source.db_name,
            &t.source.stream,
        ))
        .expect("strings");
        let txn = hex(&t.txn.0);
        let identity = serde_json::to_vec(&(&source, &txn)).expect("strings");
        Self {
            key: hex(&Sha256::digest(identity)),
            digest: hex(&t.digest),
            source,
            txn,
            position: serde_json::to_string(&(
                t.commit_position.order_domain(),
                hex(t.commit_position.bytes()),
            ))
            .expect("strings"),
        }
    }
    fn query(&self, dialect: SpannerDialect, table: &str) -> Statement {
        let param = if dialect == SpannerDialect::GoogleSql {
            "@p1"
        } else {
            "$1"
        };
        Statement::builder(format!(
            "SELECT source_txn_digest FROM {table} WHERE source_txn_id={param}"
        ))
        .add_param("p1", self.key.clone())
        .build()
    }
    fn mutation(&self, table: &str) -> Mutation {
        // applied_at is an observation time, not the source commit timestamp.
        // No allow_commit_timestamp option is required by this schema.
        Mutation::new_insert_builder(table)
            .set("source_txn_id")
            .to(self.key.clone())
            .set("source_txn_digest")
            .to(self.digest.clone())
            .set("source_identity")
            .to(self.source.clone())
            .set("txn_identity")
            .to(self.txn.clone())
            .set("source_position")
            .to(self.position.clone())
            .set("applied_at")
            .to(Timestamp::try_from(std::time::SystemTime::now()).expect("current time"))
            .build()
    }
}
impl Destination {
    /// Caller discloses/journals this Skedaddle-owned table's DDL before install.
    /// S10 preflight owns checking an existing table's shape; never alters it.
    pub async fn install(&self, spanner: &Spanner, database_name: &str) -> Result<()> {
        let ddl = replay_ddl(&self.config, self.dialect)?;
        let catalog = self.catalog().await?;
        if catalog.contains_key(&self.config.replay_table) {
            return Ok(());
        }
        let admin = spanner
            .database_admin_builder()
            .build()
            .await
            .map_err(|_| Error::Database)?;
        let mut op = admin
            .update_database_ddl()
            .set_database(database_name)
            .set_statements([ddl])
            .send()
            .await?;
        for _ in 0..600 {
            if op.done {
                return if op.error().is_some() {
                    Err(Error::Database)
                } else {
                    Ok(())
                };
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            op = admin.get_operation().set_name(&op.name).send().await?;
        }
        Err(Error::UnknownOutcome)
    }
    /// Refresh metadata per source transaction. Schema must remain stable from
    /// qualification through commit, as required by connector preflight.
    pub async fn catalog(&self) -> Result<Catalog> {
        let schema = if self.dialect == SpannerDialect::GoogleSql {
            ""
        } else {
            "public"
        };
        let mut catalog = Catalog::new();
        let mut rows = self.database.single_use().build().execute_query(format!(
            "SELECT TABLE_NAME,COLUMN_NAME,SPANNER_TYPE,IS_GENERATED,COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{schema}'")).await?;
        while let Some(row) = rows.next().await {
            let row = row?;
            let table: String = row.get(0).map_err(|_| Error::Database)?;
            let column: String = row.get(1).map_err(|_| Error::Database)?;
            let ty: String = row.get(2).map_err(|_| Error::Database)?;
            let generated: String = row.get(3).map_err(|_| Error::Database)?;
            let default: Option<String> = row.get(4).map_err(|_| Error::Database)?;
            let table = catalog.entry(table).or_default();
            if ty.starts_with("ARRAY<") || ty.ends_with("[]") {
                table.array_columns.insert(column.clone());
            }
            if generated == "ALWAYS" {
                table.generated_columns.insert(column.clone());
            }
            if default.is_some() {
                table.default_columns.insert(column.clone());
            }
            table.columns.insert(column);
        }
        let mut rows = self.database.single_use().build().execute_query(format!(
            "SELECT TABLE_NAME,INDEX_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_SCHEMA='{schema}' ORDER BY TABLE_NAME,INDEX_NAME,ORDINAL_POSITION")).await?;
        while let Some(row) = rows.next().await {
            let row = row?;
            let table: String = row.get(0).map_err(|_| Error::Database)?;
            let index: String = row.get(1).map_err(|_| Error::Database)?;
            let column: String = row.get(2).map_err(|_| Error::Database)?;
            let table = catalog.entry(table).or_default();
            if index == "PRIMARY_KEY" {
                table.primary_key.push(column);
            } else {
                table.indexes.entry(index).or_default().insert(column);
            }
        }
        for table in catalog.values_mut() {
            for index in table.indexes.values_mut() {
                index.extend(table.primary_key.iter().cloned());
            }
        }
        let mut rows = self.database.single_use().build().execute_query(format!(
            "SELECT PARENT_TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{schema}' AND ON_DELETE_ACTION='CASCADE'")).await?;
        while let Some(row) = rows.next().await {
            let parent: String = row?.get(0).map_err(|_| Error::Database)?;
            catalog.entry(parent).or_default().cascade_children = true;
        }
        let mut rows = self.database.single_use().build().execute_query(format!(
            "SELECT t.TABLE_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS r JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS t ON r.UNIQUE_CONSTRAINT_NAME=t.CONSTRAINT_NAME AND r.UNIQUE_CONSTRAINT_SCHEMA=t.CONSTRAINT_SCHEMA WHERE t.TABLE_SCHEMA='{schema}' AND r.DELETE_RULE='CASCADE'")).await?;
        while let Some(row) = rows.next().await {
            let parent: String = row?.get(0).map_err(|_| Error::Database)?;
            catalog.entry(parent).or_default().cascade_children = true;
        }
        Ok(catalog)
    }
    async fn resolve(&self, replay: &Replay) -> Result<Option<Decision>> {
        let table = replay_identifier(&self.config.replay_table, self.dialect)?;
        let mut rows = self
            .database
            .single_use()
            .build()
            .execute_query(replay.query(self.dialect, &table))
            .await?;
        if let Some(row) = rows.next().await {
            let digest: String = row?.get(0).map_err(|_| Error::Database)?;
            if digest != replay.digest {
                return Err(Error::Conflict);
            }
            Ok(Some(Decision::Replayed))
        } else {
            Ok(None)
        }
    }
    pub async fn apply(
        &mut self,
        transaction: &AssembledTxn,
        writes: &[Write],
    ) -> Result<Decision> {
        let replay = Replay::new(transaction);
        if let Some(decision) = self.resolve(&replay).await? {
            return Ok(decision);
        }
        let catalog = self.catalog().await?;
        let mut mutations = Vec::new();
        let mut counts = Vec::new();
        for write in writes {
            let (mutation, count) = write.mutation(&catalog, &self.config.replay_table)?;
            mutations.push(mutation);
            counts.push(count);
        }
        let receipt = catalog
            .get(&self.config.replay_table)
            .ok_or(Error::Invalid("install replay table first"))?;
        counts.push(limits::cells(receipt, Kind::Insert, &receipt.columns));
        limits::admit(&self.config, counts).map_err(Error::Limit)?;
        mutations.push(replay.mutation(&self.config.replay_table));
        let table = replay_identifier(&self.config.replay_table, self.dialect)?;
        for attempt in 0..4 {
            // Recheck under the transaction's locks on EVERY SDK abort retry.
            // A competing writer cannot commit the same identity twice.
            let outcome = self
                .database
                .read_write_transaction()
                .build()
                .run(async |tx| {
                    let mut rows = tx.execute_query(replay.query(self.dialect, &table)).await?;
                    if let Some(row) = rows.next().await {
                        let row = row?;
                        let digest = row.raw_values().first().and_then(Value::as_str);
                        return Ok(if digest == Some(&replay.digest) { 1 } else { 2 });
                    }
                    tx.buffer(mutations.clone())?;
                    Ok(0)
                })
                .await;
            let outcome = match outcome {
                Ok(result) => match result.into_inner() {
                    2 => return Err(Error::Conflict),
                    1 => return Ok(Decision::Replayed),
                    _ if matches!(self.fault, CommitFault::LoseReplyOnce) => {
                        self.fault = CommitFault::None;
                        Err(Error::UnknownOutcome)
                    }
                    _ => return Ok(Decision::Applied),
                },
                Err(error) => {
                    // Read back even permanent errors: a racing insert may
                    // report ALREADY_EXISTS after another writer committed.
                    if let Some(decision) = self.resolve(&replay).await? {
                        return Ok(decision);
                    }
                    let unknown = error.status().is_none_or(|s| {
                        matches!(
                            s.code.name(),
                            "DEADLINE_EXCEEDED"
                                | "UNKNOWN"
                                | "UNAVAILABLE"
                                | "INTERNAL"
                                | "NOT_FOUND"
                                | "ABORTED"
                        )
                    });
                    if !unknown {
                        return Err(Error::Database);
                    }
                    Err(Error::UnknownOutcome)
                }
            };
            if let Some(decision) = self.resolve(&replay).await? {
                return Ok(decision);
            }
            if attempt == 3 {
                return outcome;
            }
            tokio::time::sleep(Duration::from_millis(25 * (attempt + 1))).await;
        }
        Err(Error::UnknownOutcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Position, SourceIdentity, TxnId};
    #[test]
    fn replay_key_excludes_digest_and_includes_source() {
        let mut txn = AssembledTxn {
            source: SourceIdentity {
                engine: "pg".into(),
                system_id: "1".into(),
                lineage: "1".into(),
                db_name: "db".into(),
                stream: "s".into(),
            },
            txn: TxnId(vec![1]),
            commit_position: Position::new("pg", vec![1]),
            commit_time: None,
            source_origin: None,
            events: vec![],
            digest: [1; 32],
        };
        let first = Replay::new(&txn);
        txn.digest = [2; 32];
        let changed = Replay::new(&txn);
        assert_eq!(first.key, changed.key);
        assert_ne!(first.digest, changed.digest);
        txn.source.lineage = "2".into();
        assert_ne!(first.key, Replay::new(&txn).key);
    }
    #[test]
    fn missing_keys_and_unqualified_tables_block() {
        let write = Write {
            table: "t".into(),
            kind: Kind::Insert,
            values: BTreeMap::new(),
        };
        assert!(matches!(
            write.mutation(&Catalog::new(), limits::DEFAULT_REPLAY_TABLE),
            Err(Error::Invalid(_))
        ));
        let catalog = [(
            "t".into(),
            limits::Table {
                primary_key: vec!["id".into()],
                columns: ["id".into()].into(),
                ..Default::default()
            },
        )]
        .into();
        assert!(matches!(
            write.mutation(&catalog, limits::DEFAULT_REPLAY_TABLE),
            Err(Error::Invalid("missing primary key"))
        ));
    }
    #[test]
    fn configurable_replay_name_is_validated_quoted_and_reserved() {
        let mut config = Config::default();
        assert_eq!(config.replay_table, "skedaddle_apply");
        for dialect in [SpannerDialect::GoogleSql, SpannerDialect::Postgresql] {
            assert!(
                replay_ddl(&config, dialect)
                    .unwrap()
                    .contains(limits::DEFAULT_REPLAY_TABLE)
            );
            config.replay_table = "S11_Replay".into();
            let quoted = replay_identifier(&config.replay_table, dialect).unwrap();
            assert!(
                replay_ddl(&config, dialect)
                    .unwrap()
                    .starts_with(&format!("CREATE TABLE {quoted} "))
            );
            config.replay_table = limits::DEFAULT_REPLAY_TABLE.into();
        }
        for invalid in [
            "",
            "_skedaddle_apply",
            "a.b",
            "a;DROP TABLE x",
            "a\"b",
            &"a".repeat(129),
        ] {
            config.replay_table = invalid.into();
            assert!(matches!(
                replay_ddl(&config, SpannerDialect::GoogleSql),
                Err(Error::Invalid(_))
            ));
            assert!(matches!(
                replay_ddl(&config, SpannerDialect::Postgresql),
                Err(Error::Invalid(_))
            ));
        }
        assert!(replay_identifier(&"a".repeat(128), SpannerDialect::GoogleSql).is_ok());
        assert!(replay_identifier(&"a".repeat(63), SpannerDialect::Postgresql).is_ok());
        assert!(replay_identifier(&"a".repeat(64), SpannerDialect::Postgresql).is_err());
        let write = Write {
            table: "S11_Replay".into(),
            kind: Kind::Insert,
            values: BTreeMap::new(),
        };
        assert!(matches!(
            write.mutation(&Catalog::new(), "s11_replay"),
            Err(Error::Invalid("reserved table"))
        ));
    }
}
