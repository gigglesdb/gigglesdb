//! Independent, bounded PostgreSQL reads. Catalog preflight supplies the key
//! comparator: canonical encodings do not define PostgreSQL index ordering.
mod ranges;
mod repair;
mod sections;

pub use ranges::{RangeBudget, split_range};
pub use repair::*;
pub use sections::*;

use std::{cmp::Ordering, collections::BTreeMap};

use async_trait::async_trait;
use tokio_postgres::{Client, IsolationLevel, types::ToSql};

use super::{
    BoundarySource, ChangeEvidence, Key, KeyOrder, Projection, RangePlan, Result, Row, RowSource,
    SourceRead, VerifyError,
};
use crate::{
    event::Position,
    mapping::{PgType, Value},
    pg::chunk::{ChunkQuery, TableKey},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Source,
    Destination,
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub pg_type: PgType,
}

/// Independent source change tracking, never inferred from current row values.
/// `arm` runs BEFORE snapshot acquisition and must retain in-flight transactions,
/// both identities of key changes, deletes, inserts and reverted writes. Its
/// evidence must extend through the destination read when `changes` is called.
#[async_trait]
pub trait SqlChangeEvidence: Send {
    fn position_order(&self) -> super::SourcePositionOrder;
    async fn arm(&mut self, range: &RangePlan) -> Result<()>;
    async fn changes(&mut self, range: &RangePlan, source_lsn: Position) -> Result<ChangeEvidence>;
}

pub struct PgRowSource<O> {
    client: Client,
    side: Side,
    key: TableKey,
    columns: BTreeMap<String, PgType>,
    order: O,
    budget: RangeBudget,
    evidence: Option<Box<dyn SqlChangeEvidence>>,
    transport: crate::pg::tls::TransportEvidence,
    source_positions: Option<PgSourcePosition>,
}

impl<O: KeyOrder + Send> PgRowSource<O> {
    /// Each call opens a new connection owned exclusively by this endpoint.
    /// Requires an explicit CA file and verified TLS, then sets a read-only session.
    pub async fn connect(
        config: &crate::pg::tls::Config,
        side: Side,
        key: TableKey,
        columns: Vec<Column>,
        order: O,
        budget: RangeBudget,
    ) -> Result<Self> {
        key.validate().map_err(chunk_error)?;
        budget.validate()?;
        let mut scoped = BTreeMap::new();
        for column in columns {
            ident(&column.name)?;
            if scoped.insert(column.name, column.pg_type).is_some() {
                return Err(VerifyError::Invalid("duplicate scoped column"));
            }
        }
        let (client, connection, transport) =
            crate::pg::tls::connect(config, &crate::security::TlsPolicy::Required)
                .await
                .map_err(|_| {
                    VerifyError::Read("verified PostgreSQL TLS connection failed".into())
                })?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.batch_execute("SET default_transaction_read_only = on; SET DateStyle = 'ISO'; SET IntervalStyle = 'postgres'; SET extra_float_digits = 3; SET TimeZone = 'UTC'; SET client_encoding = 'UTF8'; SET bytea_output = 'hex'; SET search_path = pg_catalog").await.map_err(pg_error)?;
        Ok(Self {
            client,
            side,
            key,
            columns: scoped,
            order,
            budget,
            evidence: None,
            transport,
            source_positions: None,
        })
    }

    /// Explicit source lineage for reads made without online evidence.
    pub fn with_source_positions(mut self, positions: PgSourcePosition) -> Self {
        self.source_positions = Some(positions);
        self
    }

    pub fn transport_evidence(&self) -> &crate::pg::tls::TransportEvidence {
        &self.transport
    }

    pub fn with_change_evidence(mut self, evidence: impl SqlChangeEvidence + 'static) -> Self {
        self.source_positions = Some(PgSourcePosition::new(evidence.position_order().domain()));
        self.evidence = Some(Box::new(evidence));
        self
    }

    /// Records a source position in the same transaction as the range read.
    /// A position alone is not comparable-boundary evidence; use BoundarySource
    /// with a complete SqlChangeEvidence provider for online verification.
    pub async fn read_with_lsn(
        &mut self,
        range: &RangePlan,
        projection: &Projection,
    ) -> Result<SourceRead> {
        if self.side != Side::Source {
            return Err(VerifyError::Invalid("source read on destination"));
        }
        let (rows, position) = self.read(range, projection).await?;
        Ok(SourceRead {
            rows,
            position: position.ok_or(VerifyError::Invalid("source position order is not bound"))?,
        })
    }

    async fn read(
        &mut self,
        range: &RangePlan,
        projection: &Projection,
    ) -> Result<(Vec<Row>, Option<Position>)> {
        super::compare::validate_range(self, range)?;
        let query = self.key.read_query(range).map_err(chunk_error)?;
        let (select, types) = self.select(projection)?;
        let tail = query
            .sql
            .strip_prefix("SELECT *")
            .ok_or(VerifyError::Invalid("chunk query shape"))?;
        let raw_fields = self
            .key
            .columns
            .iter()
            .map(|c| c.name.clone())
            .chain(projection.columns().iter().cloned())
            .collect::<std::collections::BTreeSet<_>>()
            .iter()
            .map(|n| ident(n))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let from = format!(" FROM (SELECT {raw_fields}{tail}) AS v");
        let sql = format!(
            "SELECT {}{from} ORDER BY {}",
            select.join(", "),
            key_order(&self.key, Some("v"))?
        );
        let params = parameters(&query);
        let tx = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(pg_error)?;
        // This first query acquires the RR snapshot. The independently armed
        // evidence provider must cover transactions not visible to it.
        let lsn_sql = if self.side == Side::Source {
            "SELECT pg_current_wal_lsn()::text"
        } else {
            "SELECT '0/0'::text"
        };
        let lsn_text: String = tx.query_one(lsn_sql, &[]).await.map_err(pg_error)?.get(0);
        let lsn = parse_lsn(&lsn_text)?;
        // Destination 0/0 is a query placeholder, never a source boundary.
        let position = if self.side == Side::Source {
            self.source_positions
                .as_ref()
                .map(|adapter| adapter.position(lsn))
        } else {
            None
        };
        let bytes = (0..select.len())
            .map(|i| format!("8::numeric + COALESCE(octet_length(\"v{i}\"), 0)"))
            .collect::<Vec<_>>()
            .join(" + ");
        let sized_select = select
            .iter()
            .enumerate()
            .map(|(i, expr)| format!("{expr} AS \"v{i}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let size_sql = format!(
            "SELECT count(*)::text, COALESCE(sum({bytes}), 0)::text FROM (SELECT {sized_select}{from}) AS sized"
        );
        let size = tx.query_one(&size_sql, &params).await.map_err(pg_error)?;
        let count = parse_count(size.get(0))?;
        let bytes = parse_count(size.get(1))?;
        if !self.budget.accepts(count, bytes) {
            if count <= 1 {
                return Err(VerifyError::Invalid(
                    "single row exceeds verification byte cap",
                ));
            }
            // The same exact server-side row-number stride used by chunk.rs,
            // restricted to this range. Only one key crosses the connection.
            let aliases = self
                .key
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    Ok(format!(
                        "{} AS \"k{i}\"",
                        text_expression(&format!("v.{}", ident(&c.name)?), &c.pg_type)
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            let ordering = key_order(&self.key, Some("v"))?;
            let key_bytes = self
                .key
                .columns
                .iter()
                .enumerate()
                .map(|(i, _)| format!("8::numeric + COALESCE(octet_length(\"k{i}\"),0)"))
                .collect::<Vec<_>>()
                .join(" + ");
            let key_aliases = self
                .key
                .columns
                .iter()
                .enumerate()
                .map(|(i, _)| format!("\"k{i}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let sample_sql = format!(
                "WITH candidate AS MATERIALIZED (SELECT {key_aliases} FROM (SELECT {aliases}, row_number() OVER (ORDER BY {ordering}) AS rn{from}) AS samples WHERE rn > 1 AND (rn - 1) % {} = 0 ORDER BY rn LIMIT 1) SELECT {key_aliases} FROM candidate WHERE {key_bytes} <= {}",
                count / 2,
                self.budget.max_bytes
            );
            let sampled = tx
                .query_opt(&sample_sql, &params)
                .await
                .map_err(pg_error)?
                .ok_or(VerifyError::Invalid(
                    "sample key exceeds verification byte cap",
                ))?;
            let boundary = decode_key(&sampled, &self.key)?;
            let splits = split_range(&self.order, range, boundary)?;
            tx.commit().await.map_err(pg_error)?;
            return Err(VerifyError::SplitRequired(splits));
        }
        let raw = tx.query(&sql, &params).await.map_err(pg_error)?;
        let rows = raw
            .iter()
            .map(|r| {
                let key = decode_key(r, &self.key)?;
                let columns = projection
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        let text: Option<&str> = r.get(self.key.columns.len() + i);
                        Ok((
                            name.clone(),
                            Value::parse_pg_text(&types[i], text.map(str::as_bytes))
                                .map_err(decode_error)?,
                        ))
                    })
                    .collect::<Result<_>>()?;
                Ok(Row { key, columns })
            })
            .collect::<Result<_>>()?;
        tx.commit().await.map_err(pg_error)?;
        Ok((rows, position))
    }

    fn select(&self, projection: &Projection) -> Result<(Vec<String>, Vec<PgType>)> {
        let mut select = self
            .key
            .columns
            .iter()
            .map(|c| {
                Ok(text_expression(
                    &format!("v.{}", ident(&c.name)?),
                    &c.pg_type,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut types = Vec::new();
        for name in projection.columns() {
            types.push(
                self.columns
                    .get(name)
                    .ok_or(VerifyError::Invalid("column outside verification scope"))?
                    .clone(),
            );
            select.push(text_expression(
                &format!("v.{}", ident(name)?),
                types.last().expect("type inserted"),
            ));
        }
        Ok((select, types))
    }
}

impl<O: KeyOrder> KeyOrder for PgRowSource<O> {
    fn compare_keys(&self, left: &Key, right: &Key) -> Result<Ordering> {
        self.order.compare_keys(left, right)
    }
}

#[async_trait]
impl<O: KeyOrder + Send> RowSource for PgRowSource<O> {
    async fn fetch(&mut self, range: &RangePlan, projection: &Projection) -> Result<Vec<Row>> {
        Ok(self.read(range, projection).await?.0)
    }
}

#[async_trait]
impl<O: KeyOrder + Send> BoundarySource for PgRowSource<O> {
    fn position_order(&self) -> super::SourcePositionOrder {
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
                "complete source change evidence required",
            ))?
            .arm(range)
            .await?;
        self.read_with_lsn(range, projection).await
    }
    async fn changed_since(
        &mut self,
        range: &RangePlan,
        source_lsn: Position,
    ) -> Result<ChangeEvidence> {
        self.evidence
            .as_mut()
            .ok_or(VerifyError::Invalid(
                "complete source change evidence required",
            ))?
            .changes(range, source_lsn)
            .await
    }
}

fn decode_key(row: &tokio_postgres::Row, key: &TableKey) -> Result<Key> {
    key.columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let text: Option<&str> = row.get(i);
            let value =
                Value::parse_pg_text(&c.pg_type, text.map(str::as_bytes)).map_err(decode_error)?;
            if value == Value::Null {
                return Err(VerifyError::Invalid("NULL verification key"));
            }
            Ok(value)
        })
        .collect()
}

fn parameters(query: &ChunkQuery) -> Vec<&(dyn ToSql + Sync)> {
    query
        .parameters
        .iter()
        .map(|p| p as &(dyn ToSql + Sync))
        .collect()
}
fn text_expression(expr: &str, pg_type: &PgType) -> String {
    match pg_type {
        // A bpchar -> text cast strips padding; its output function does not.
        PgType::Char { .. } => format!("pg_catalog.bpcharout({expr})::text"),
        PgType::Domain { base, .. } => text_expression(expr, base),
        _ => format!("{expr}::text"),
    }
}
fn key_order(key: &TableKey, alias: Option<&str>) -> Result<String> {
    key.columns
        .iter()
        .map(|c| {
            let mut expr = format!(
                "{}{}",
                alias.map_or(String::new(), |a| format!("{a}.")),
                ident(&c.name)?
            );
            if let Some(collation) = &c.collation {
                expr = format!("({expr} COLLATE {})", collation.sql().map_err(chunk_error)?);
            }
            Ok(format!(
                "{expr} {}",
                match c.direction {
                    crate::pg::chunk::Direction::Asc => "ASC",
                    crate::pg::chunk::Direction::Desc => "DESC",
                }
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(|v| v.join(", "))
}
fn ident(name: &str) -> Result<String> {
    if name.is_empty() || name.contains('\0') {
        return Err(VerifyError::Invalid("SQL identifier"));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}
fn pg_error(_: tokio_postgres::Error) -> VerifyError {
    VerifyError::Read("PostgreSQL operation failed (details redacted)".into())
}
fn decode_error(_: crate::mapping::MappingError) -> VerifyError {
    VerifyError::Read("PostgreSQL value decoding failed (details redacted)".into())
}
fn chunk_error(_: crate::pg::chunk::ChunkError) -> VerifyError {
    VerifyError::Invalid("invalid PostgreSQL range/key descriptor")
}
fn parse_count(text: &str) -> Result<u64> {
    text.parse()
        .map_err(|_| VerifyError::Invalid("range size overflow"))
}
fn parse_lsn(text: &str) -> Result<u64> {
    let (hi, lo) = text
        .split_once('/')
        .ok_or(VerifyError::Invalid("PostgreSQL LSN"))?;
    let hi = u32::from_str_radix(hi, 16).map_err(|_| VerifyError::Invalid("PostgreSQL LSN"))?;
    let lo = u32::from_str_radix(lo, 16).map_err(|_| VerifyError::Invalid("PostgreSQL LSN"))?;
    Ok((u64::from(hi) << 32) | u64::from(lo))
}

#[cfg(test)]
mod tests;

/// Process-local conversion between PostgreSQL WAL LSNs and a bound source domain.
#[derive(Clone, Debug)]
pub struct PgSourcePosition(crate::pg::apply::NativePgOrder);
impl PgSourcePosition {
    pub fn new(domain: impl Into<String>) -> Self {
        Self(crate::pg::apply::NativePgOrder(domain.into()))
    }
    pub fn position(&self, lsn: u64) -> Position {
        Position::new(self.0.0.clone(), lsn.to_be_bytes())
    }
    pub fn lsn(&self, position: &Position) -> Result<u64> {
        super::compare_positions(&self.0, position, position)?;
        Ok(u64::from_be_bytes(
            position.bytes().try_into().expect("validated PG LSN"),
        ))
    }
}
