//! Independent InnoDB snapshot reads in capture's mapped PostgreSQL value domain.
//! KeyOrder must be qualified for BOTH endpoints; canonical bytes are not a
//! MySQL collation comparator. Durable reports remain refused by the V1 boundary.
pub mod checkpoint;
pub mod evidence;
pub mod input;
pub mod publication;
use std::{cmp::Ordering, collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use mysql_async::{Params, prelude::Queryable};

use super::{
    BoundarySource, ChangeEvidence, Key, KeyOrder, Projection, RangePlan, Result, Row, RowSource,
    SourcePositionOrder, SourceRead, VerifyError,
    sql::{RangeBudget, SectionDifferences, SqlChangeEvidence},
};
use crate::{
    event::Position,
    mapping::Value,
    mysql::{
        GtidSet, MysqlOrder, MysqlPosition,
        chunk::{ident, map_row},
        chunk_key::MysqlTableKey,
        client::{MysqlClient, MysqlConfig},
        dest_schema,
        schema::{ColumnDescriptor, TableDescriptor},
    },
};

/// Neither mode is an atomic snapshot/replication-coordinate pair. Waiting for
/// this position alone never establishes an online comparable boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotBoundary {
    UpperBound(MysqlPosition),
    /// GTID externalization is non-atomic with commit, so this sample may also
    /// omit a just-visible transaction. It has no snapshot-order guarantee.
    UncertifiedGtidSample(MysqlPosition),
}
impl SnapshotBoundary {
    pub fn native(&self) -> &MysqlPosition {
        match self {
            Self::UpperBound(position) | Self::UncertifiedGtidSample(position) => position,
        }
    }
    pub fn durable_report(&self) -> Result<()> {
        Err(VerifyError::NonPgVerificationReportUnsupported)
    }
}

/// An owned dedicated connection: dropping it (including cancellation) closes
/// the connection and rolls back the read-only transaction. finish rolls back
/// explicitly. No transaction-bearing connection is returned to a pool.
pub struct MysqlSnapshot {
    client: MysqlClient,
    pub boundary: SnapshotBoundary,
    positions: SourcePositionOrder,
}
impl MysqlSnapshot {
    async fn begin(config: &MysqlConfig, source_id: &str) -> Result<Self> {
        let mut client = config.connect().await.map_err(read_error)?;
        client
            .conn
            .query_drop("SET SESSION time_zone='+00:00'")
            .await
            .map_err(read_error)?;
        client
            .conn
            .query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .map_err(read_error)?;
        client
            .conn
            .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            .await
            .map_err(read_error)?;
        let (mode, uuid, executed): (String, String, String) = client
            .conn
            .query_first("SELECT @@gtid_mode, @@server_uuid, @@gtid_executed")
            .await
            .map_err(read_error)?
            .ok_or(VerifyError::Invalid("missing MySQL boundary variables"))?;
        let row: mysql_async::Row = client
            .conn
            .query_first(client.version.status_sql())
            .await
            .map_err(read_error)?
            .ok_or(VerifyError::Invalid("binary logging disabled"))?;
        let file = row
            .get::<String, _>(0)
            .ok_or(VerifyError::Invalid("binlog filename"))?;
        let offset = row
            .get::<u64, _>(1)
            .ok_or(VerifyError::Invalid("binlog offset"))?;
        let boundary = sampled_boundary(&mode, uuid, &executed, file, offset)?;
        let positions: SourcePositionOrder = Arc::new(
            match boundary.native() {
                MysqlPosition::Gtid { .. } => MysqlOrder::gtid(source_id),
                MysqlPosition::File { server_uuid, .. } => MysqlOrder::file(server_uuid),
            }
            .map_err(VerifyError::Position)?,
        );
        Ok(Self {
            client,
            boundary,
            positions,
        })
    }
    pub async fn finish(mut self) -> Result<()> {
        self.client
            .conn
            .query_drop("ROLLBACK")
            .await
            .map_err(read_error)?;
        self.client.conn.disconnect().await.map_err(read_error)
    }
    fn position(&self) -> Result<Position> {
        let position = Position::new(
            self.positions.domain(),
            self.boundary
                .native()
                .encode()
                .map_err(VerifyError::Position)?,
        );
        super::compare_positions(self.positions.as_ref(), &position, &position)?;
        Ok(position)
    }
}

// START WITH CONSISTENT SNAPSHOT establishes an InnoDB read view before these
// global observations. Under normal binlogged InnoDB commits, visible commits
// precede the sampled binlog coordinate, but concurrent commits can also be
// included while absent from the view: file mode is an upper bound, not exact.
// GTID externalization is non-atomic after commit; @@gtid_executed can additionally
// omit just-visible commits. It is therefore uncertified, not even an upper bound
// (MySQL 8.0/8.4 manual, GTID Life Cycle). GTID/file are separate samples;
// file/offset in the GTID variant is diagnostic, not an atomic GTID resume proof.
// Requires stable server lineage/mode, no RESET BINARY LOGS/GTID mutation, ordinary
// binlogged InnoDB transactions and no concurrent DDL. Complete evidence armed
// BEFORE begin must retain in-flight changes regardless of the sampled position.
fn sampled_boundary(
    mode: &str,
    uuid: String,
    executed: &str,
    file: String,
    offset: u64,
) -> Result<SnapshotBoundary> {
    if !(4..=u64::from(u32::MAX)).contains(&offset) {
        return Err(VerifyError::Invalid(
            "binlog offset outside supported range",
        ));
    }
    let file_seq = file
        .rsplit_once('.')
        .and_then(|(_, n)| n.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .ok_or(VerifyError::Invalid("binlog filename sequence"))?;
    let position = match mode {
        "ON" => MysqlPosition::Gtid {
            set: executed.parse::<GtidSet>().map_err(VerifyError::Position)?,
            server_uuid: uuid,
            file,
            offset,
        },
        "OFF" => MysqlPosition::File {
            server_uuid: uuid,
            file_seq,
            offset,
        },
        _ => return Err(VerifyError::Invalid("transitional MySQL GTID mode")),
    };
    position.encode().map_err(VerifyError::Position)?;
    Ok(if mode == "ON" {
        SnapshotBoundary::UncertifiedGtidSample(position)
    } else {
        SnapshotBoundary::UpperBound(position)
    })
}

pub struct MysqlRowSource<O> {
    config: MysqlConfig,
    source_id: String,
    key: MysqlTableKey,
    table: TableDescriptor,
    order: O,
    budget: RangeBudget,
    page_rows: u64,
    evidence: Option<Box<dyn SqlChangeEvidence>>,
    last_boundary: Option<SnapshotBoundary>,
}
impl<O: KeyOrder + Send> MysqlRowSource<O> {
    pub fn new(
        config: MysqlConfig,
        source_id: String,
        table: TableDescriptor,
        key: MysqlTableKey,
        order: O,
        budget: RangeBudget,
        page_rows: u64,
    ) -> Result<Self> {
        if source_id.is_empty()
            || page_rows == 0
            || page_rows > i64::MAX as u64
            || budget.max_rows == 0
            || budget.max_bytes == 0
        {
            return Err(VerifyError::Invalid(
                "MySQL verification budget/source identity",
            ));
        }
        key.validate().map_err(read_error)?;
        if key.schema != table.schema
            || key.table != table.table
            || key.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
                != table.primary_key.iter().collect::<Vec<_>>()
        {
            return Err(VerifyError::Invalid("MySQL descriptor/key mismatch"));
        }
        let names: BTreeSet<_> = table.columns.iter().map(|c| &c.name).collect();
        if names.len() != table.columns.len() {
            return Err(VerifyError::Invalid("duplicate MySQL descriptor column"));
        }
        for k in &key.columns {
            let c = table
                .columns
                .iter()
                .find(|c| c.name == k.name)
                .ok_or(VerifyError::Invalid("missing key column"))?;
            let mapped = crate::mapping::mysql::plan_column(c)?;
            if c.nullable
                || c.data_type != k.data_type
                || c.column_type != k.column_type
                || c.collation != k.collation
                || c.character_set != k.character_set
                || mapped.type_id != k.type_id
            {
                return Err(VerifyError::Invalid("MySQL key mapping mismatch"));
            }
        }
        Ok(Self {
            config,
            source_id,
            key,
            table,
            order,
            budget,
            page_rows,
            evidence: None,
            last_boundary: None,
        })
    }
    /// Provider must cover the entire armed interval, including in-flight and
    /// reverted changes regardless of the sampled position. Item 12 supplies the
    /// real MySQL marker provider; no status-query substitute is provided here.
    pub fn with_change_evidence(mut self, evidence: impl SqlChangeEvidence + 'static) -> Self {
        self.evidence = Some(Box::new(evidence));
        self
    }
    pub fn last_boundary(&self) -> Option<&SnapshotBoundary> {
        self.last_boundary.as_ref()
    }
    pub async fn begin_snapshot(&mut self) -> Result<MysqlSnapshot> {
        MysqlSnapshot::begin(&self.config, &self.source_id).await
    }
    /// Reuse one snapshot for every page/range which must share a read view.
    /// Overflow fails without returning partial rows; caller must narrow scope.
    pub async fn read_snapshot(
        &mut self,
        snapshot: &mut MysqlSnapshot,
        range: &RangePlan,
        projection: &Projection,
    ) -> Result<SourceRead> {
        super::compare::validate_range(self, range)?;
        let names: BTreeSet<_> = self
            .key
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .chain(projection.columns().iter().map(String::as_str))
            .collect();
        let columns: Vec<ColumnDescriptor> = names
            .iter()
            .map(|name| {
                self.table
                    .columns
                    .iter()
                    .find(|c| c.name == *name)
                    .cloned()
                    .ok_or(VerifyError::Invalid("column outside verification scope"))
            })
            .collect::<Result<_>>()?;
        for column in &columns {
            crate::mapping::mysql::plan_column(column)?;
        }
        // Hold a metadata lock for this table until snapshot finish, then check
        // engine. Nontransactional tables cannot satisfy the snapshot contract.
        let relation = format!(
            "{}.{}",
            ident(&self.key.schema).map_err(read_error)?,
            ident(&self.key.table).map_err(read_error)?
        );
        snapshot
            .client
            .conn
            .query_drop(format!("SELECT 1 FROM {relation} LIMIT 0"))
            .await
            .map_err(read_error)?;
        let engine: Option<String> = snapshot.client.conn.exec_first("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", (&self.key.schema, &self.key.table)).await.map_err(read_error)?;
        if engine.as_deref() != Some("InnoDB") {
            return Err(VerifyError::Invalid("verification requires InnoDB"));
        }
        let size_expr = columns
            .iter()
            .map(|c| {
                Ok(format!(
                    "16 + COALESCE(OCTET_LENGTH(CAST({} AS BINARY)),0)",
                    ident(&c.name).map_err(read_error)?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(" + ");
        let mut rows: Vec<Row> = Vec::new();
        let mut bytes = 0u64;
        loop {
            let query = self
                .key
                .page_query_columns(
                    range,
                    &columns,
                    rows.last().map(|r| &r.key),
                    self.page_rows.min(
                        self.budget
                            .max_rows
                            .saturating_sub(rows.len() as u64)
                            .saturating_add(1),
                    ),
                )
                .map_err(read_error)?;
            let marker = format!(" FROM {relation} WHERE ");
            let (_, tail) = query
                .sql
                .split_once(&marker)
                .ok_or(VerifyError::Invalid("MySQL page query shape"))?;
            let sizes = format!(
                "SELECT CAST(COUNT(*) AS CHAR), CAST(COALESCE(SUM(`__bytes`),0) AS CHAR) FROM (SELECT {size_expr} AS `__bytes` FROM {relation} WHERE {tail}) AS `__page`"
            );
            let (count, raw_bytes): (String, String) = snapshot
                .client
                .conn
                .exec_first(sizes, Params::Positional(query.parameters.clone()))
                .await
                .map_err(read_error)?
                .ok_or(VerifyError::Invalid("MySQL page size missing"))?;
            let count: u64 = count.parse().map_err(read_error)?;
            let raw_bytes: u64 = raw_bytes.parse().map_err(read_error)?;
            if !self.budget.accepts(
                (rows.len() as u64).saturating_add(count),
                bytes.saturating_add(raw_bytes),
            ) {
                return Err(VerifyError::Invalid(
                    "MySQL verification range exceeds budget; narrow scope",
                ));
            }
            if count == 0 {
                break;
            }
            let page: Vec<mysql_async::Row> = snapshot
                .client
                .conn
                .exec(query.sql, Params::Positional(query.parameters))
                .await
                .map_err(read_error)?;
            if page.len() as u64 != count {
                return Err(VerifyError::Invalid("MySQL snapshot page count changed"));
            }
            for raw in page {
                let mapped = map_row(&columns, raw).map_err(read_error)?;
                let decoded = mapped
                    .iter()
                    .map(|(name, v)| {
                        let value = v
                            .bytes
                            .as_deref()
                            .map(Value::from_canonical_bytes)
                            .transpose()?
                            .unwrap_or(Value::Null);
                        Ok((name.clone(), value))
                    })
                    .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
                let key: Key = self
                    .key
                    .columns
                    .iter()
                    .map(|c| decoded[&c.name].clone())
                    .collect();
                super::compare::validate_key(self, range, &key)?;
                if let Some(previous) = rows.last()
                    && super::compare::compare_key(self, &previous.key, &key)? != Ordering::Less
                {
                    return Err(VerifyError::Invalid(
                        "MySQL page keys not strictly increasing",
                    ));
                }
                for v in decoded.values() {
                    bytes = bytes
                        .checked_add(16 + v.canonical_bytes()?.len() as u64)
                        .ok_or(VerifyError::Invalid("MySQL byte budget overflow"))?;
                }
                if bytes > self.budget.max_bytes {
                    return Err(VerifyError::Invalid("mapped MySQL rows exceed byte budget"));
                }
                rows.push(Row {
                    key,
                    columns: projection
                        .columns()
                        .iter()
                        .map(|n| (n.clone(), decoded[n].clone()))
                        .collect(),
                });
            }
        }
        Ok(SourceRead {
            rows,
            position: snapshot.position()?,
        })
    }
    pub async fn read_with_boundary(
        &mut self,
        range: &RangePlan,
        projection: &Projection,
    ) -> Result<(SourceRead, SnapshotBoundary)> {
        self.last_boundary = None;
        let mut snapshot = self.begin_snapshot().await?;
        let boundary = snapshot.boundary.clone();
        let result = self.read_snapshot(&mut snapshot, range, projection).await;
        let finish = snapshot.finish().await;
        let read = result?;
        finish?;
        self.last_boundary = Some(boundary.clone());
        Ok((read, boundary))
    }
}
impl<O: KeyOrder> KeyOrder for MysqlRowSource<O> {
    fn compare_keys(&self, left: &Key, right: &Key) -> Result<Ordering> {
        self.order.compare_keys(left, right)
    }
}
#[async_trait]
impl<O: KeyOrder + Send> RowSource for MysqlRowSource<O> {
    async fn fetch(&mut self, range: &RangePlan, projection: &Projection) -> Result<Vec<Row>> {
        Ok(self.read_with_boundary(range, projection).await?.0.rows)
    }
}
#[async_trait]
impl<O: KeyOrder + Send> BoundarySource for MysqlRowSource<O> {
    fn position_order(&self) -> SourcePositionOrder {
        self.evidence
            .as_ref()
            .expect("read_source requires evidence")
            .position_order()
    }
    async fn read_source(
        &mut self,
        range: &RangePlan,
        projection: &Projection,
    ) -> Result<SourceRead> {
        self.evidence
            .as_mut()
            .ok_or(VerifyError::Invalid(
                "complete MySQL change evidence required",
            ))?
            .arm(range)
            .await?;
        let (read, _) = self.read_with_boundary(range, projection).await?;
        super::compare_positions(
            self.position_order().as_ref(),
            &read.position,
            &read.position,
        )?;
        Ok(read)
    }
    async fn changed_since(
        &mut self,
        range: &RangePlan,
        position: Position,
    ) -> Result<ChangeEvidence> {
        self.evidence
            .as_mut()
            .ok_or(VerifyError::Invalid(
                "complete MySQL change evidence required",
            ))?
            .changes(range, position)
            .await
    }
}

/// Mapped-schema findings use the same result sections as PostgreSQL. This
/// checks M7's mapped columns/PK/checks/collations, not raw MySQL DDL strings.
/// Sequence/auto-increment equivalence and secondary indexes remain unqualified;
/// empty vectors here must not be promoted into those readiness proofs.
pub async fn mapped_sections(
    client: &mut tokio_postgres::Client,
    table: &TableDescriptor,
) -> Result<SectionDifferences> {
    let schema = dest_schema::build(std::slice::from_ref(table)).map_err(read_error)?;
    let mismatches = dest_schema::verify_destination(client, &schema)
        .await
        .map_err(read_error)?;
    let mut sections = SectionDifferences::default();
    for m in mismatches {
        let detail = format!("mapped MySQL schema differs: {:?}", m.kind);
        match m.kind {
            dest_schema::MismatchKind::PrimaryKey | dest_schema::MismatchKind::Check => {
                sections.constraints.push(super::ConstraintFinding {
                    constraint: m.object,
                    detail,
                })
            }
            _ => sections.schema.push(super::SchemaFinding {
                object: m.object,
                detail,
            }),
        }
    }
    Ok(sections)
}
fn read_error(_: impl std::fmt::Display) -> VerifyError {
    VerifyError::Read("MySQL verification operation failed (details redacted)".into())
}

#[cfg(test)]
mod tests;
