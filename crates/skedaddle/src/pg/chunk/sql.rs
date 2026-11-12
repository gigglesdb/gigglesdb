use super::{ChunkError, ChunkRange, Key, Result};
use crate::mapping::PgType;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tokio_postgres::{Client, IsolationLevel, Row, types::ToSql};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualifiedName {
    pub schema: String,
    pub name: String,
}
impl QualifiedName {
    pub fn sql(&self) -> Result<String> {
        Ok(format!("{}.{}", ident(&self.schema)?, ident(&self.name)?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyColumn {
    pub name: String,
    pub pg_type: PgType,
    /// Exact catalog type, including enum/domain; never raw SQL.
    pub sql_type: QualifiedName,
    pub collation: Option<QualifiedName>,
    pub direction: Direction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableKey {
    /// Engine-neutral Event.table identity, not parsed as SQL.
    pub table: String,
    pub relation: QualifiedName,
    pub columns: Vec<KeyColumn>,
}
impl TableKey {
    pub fn validate(&self) -> Result<()> {
        self.relation.sql()?;
        if self.table.is_empty() || self.columns.is_empty() {
            return Err(ChunkError::Invalid("table/key identity"));
        }
        let mut names = BTreeSet::new();
        for c in &self.columns {
            ident(&c.name)?;
            c.sql_type.sql()?;
            if let Some(collation) = &c.collation {
                collation.sql()?;
            }
            if !names.insert(&c.name) {
                return Err(ChunkError::Invalid("duplicate key column"));
            }
        }
        Ok(())
    }
    pub(crate) fn validate_key(&self, key: &[crate::mapping::Value]) -> Result<()> {
        self.parameters(key).map(|_| ())
    }
    fn parameters(&self, key: &[crate::mapping::Value]) -> Result<Vec<String>> {
        if key.len() != self.columns.len() {
            return Err(ChunkError::Invalid("key arity"));
        }
        key.iter()
            .zip(&self.columns)
            .map(|(v, c)| {
                let bytes = v
                    .format_pg_text(&c.pg_type)?
                    .ok_or(ChunkError::Invalid("NULL key"))?;
                String::from_utf8(bytes).map_err(|_| {
                    ChunkError::Invalid("key SQL bindings require UTF8 client encoding")
                })
            })
            .collect()
    }
    fn expression(&self, i: usize, alias: bool) -> Result<String> {
        self.collate(
            i,
            if alias {
                ident(&format!("__k{i}"))?
            } else {
                ident(&self.columns[i].name)?
            },
        )
    }
    fn collate(&self, i: usize, expr: String) -> Result<String> {
        Ok(match &self.columns[i].collation {
            Some(c) => format!("({expr} COLLATE {})", c.sql()?),
            None => expr,
        })
    }
    fn order(&self, alias: bool) -> Result<String> {
        (0..self.columns.len())
            .map(|i| {
                Ok(format!(
                    "{} {}",
                    self.expression(i, alias)?,
                    match self.columns[i].direction {
                        Direction::Asc => "ASC",
                        Direction::Desc => "DESC",
                    }
                ))
            })
            .collect::<Result<Vec<_>>>()
            .map(|v| v.join(", "))
    }
    /// Exact server-side stride sampling, not approximate histogram estimates.
    pub fn sampling_query(&self, size: usize) -> Result<ChunkQuery> {
        self.validate()?;
        let size = i64::try_from(size)
            .ok()
            .filter(|v| *v > 0)
            .ok_or(ChunkError::Invalid("chunk size"))?;
        let projections = (0..self.columns.len())
            .map(|i| {
                Ok(format!(
                    "{} AS {}",
                    self.expression(i, false)?,
                    ident(&format!("__k{i}"))?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let aliases = (0..self.columns.len())
            .map(|i| format!("\"__k{i}\""))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(ChunkQuery {
            sql: format!(
                "SELECT {aliases} FROM (SELECT {projections}, row_number() OVER (ORDER BY {}) AS \"__rn\" FROM {}) AS \"__sample\" WHERE \"__rn\" > 1 AND (\"__rn\" - 1) % $1::text::bigint = 0 ORDER BY {}",
                self.order(false)?,
                self.relation.sql()?,
                self.order(true)?
            ),
            parameters: vec![size.to_string()],
        })
    }
    pub fn read_query(&self, chunk: &ChunkRange) -> Result<ChunkQuery> {
        self.read_projection(chunk, "*")
    }
    /// Qualified partial-table path. Keys must remain in the projection.
    pub fn read_query_columns(&self, chunk: &ChunkRange, columns: &[String]) -> Result<ChunkQuery> {
        let names: BTreeSet<_> = columns.iter().collect();
        if names.is_empty()
            || names.len() != columns.len()
            || self.columns.iter().any(|key| !names.contains(&key.name))
        {
            return Err(ChunkError::Invalid(
                "projection must include every key exactly once",
            ));
        }
        let projection = columns
            .iter()
            .map(|name| ident(name))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        self.read_projection(chunk, &projection)
    }
    fn read_projection(&self, chunk: &ChunkRange, projection: &str) -> Result<ChunkQuery> {
        self.validate()?;
        chunk.id.validate()?;
        if chunk.id.table != self.table {
            return Err(ChunkError::Invalid("chunk table mismatch"));
        }
        let mut parameters = Vec::new();
        let mut predicates = Vec::new();
        for (bound, lower) in [(&chunk.lo, true), (&chunk.hi, false)] {
            if let Some(key) = bound {
                let values = self.parameters(key)?;
                let first = parameters.len() + 1;
                parameters.extend(values);
                predicates.push(self.bound_predicate(first, lower)?);
            }
        }
        Ok(ChunkQuery {
            sql: format!(
                "SELECT {projection} FROM {} WHERE {} ORDER BY {}",
                self.relation.sql()?,
                if predicates.is_empty() {
                    "TRUE".into()
                } else {
                    predicates.join(" AND ")
                },
                self.order(false)?
            ),
            parameters,
        })
    }
    fn bound_predicate(&self, first: usize, lower: bool) -> Result<String> {
        let left = (0..self.columns.len())
            .map(|i| self.expression(i, false))
            .collect::<Result<Vec<_>>>()?;
        let right = (0..self.columns.len())
            .map(|i| {
                self.collate(
                    i,
                    format!("${}::text::{}", first + i, self.columns[i].sql_type.sql()?),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if self
            .columns
            .iter()
            .all(|c| c.direction == self.columns[0].direction)
        {
            let greater = lower == (self.columns[0].direction == Direction::Asc);
            let op = match (greater, lower) {
                (true, true) => ">=",
                (false, true) => "<=",
                (true, false) => ">",
                (false, false) => "<",
            };
            return Ok(format!(
                "(({}) {op} ({}))",
                left.join(", "),
                right.join(", ")
            ));
        }
        // PostgreSQL row comparisons cannot express mixed ASC/DESC; expand
        // lexicographically, retaining source collation on every operand.
        let mut alternatives = Vec::new();
        for i in 0..left.len() {
            let mut terms = (0..i)
                .map(|j| format!("{} = {}", left[j], right[j]))
                .collect::<Vec<_>>();
            let greater = lower == (self.columns[i].direction == Direction::Asc);
            let inclusive = lower && i + 1 == left.len();
            let op = match (greater, inclusive) {
                (true, true) => ">=",
                (false, true) => "<=",
                (true, false) => ">",
                (false, false) => "<",
            };
            terms.push(format!("{} {op} {}", left[i], right[i]));
            alternatives.push(format!("({})", terms.join(" AND ")));
        }
        Ok(format!("({})", alternatives.join(" OR ")))
    }
    /// Decoder uses preflight's wire/mapping descriptors. Commit before return;
    /// no snapshot/xmin survives this call.
    pub async fn sample(
        &self,
        client: &mut Client,
        size: usize,
        mut decode: impl FnMut(Row) -> Result<Key>,
    ) -> Result<Vec<Key>> {
        self.sampling_query(size)?
            .execute(client)
            .await?
            .into_iter()
            .map(&mut decode)
            .collect()
    }
    pub async fn read(
        &self,
        client: &mut Client,
        chunk: &ChunkRange,
        mut decode: impl FnMut(Row) -> Result<crate::event::Row>,
    ) -> Result<Vec<crate::event::Row>> {
        self.read_query(chunk)?
            .execute(client)
            .await?
            .into_iter()
            .map(&mut decode)
            .collect()
    }
}

/// Bind values are text explicitly cast to catalog types. All identifiers are
/// separately quoted. Caller must use mapping-compatible session settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkQuery {
    pub sql: String,
    pub parameters: Vec<String>,
}
impl ChunkQuery {
    pub async fn execute(&self, client: &mut Client) -> Result<Vec<Row>> {
        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .read_only(true)
            .start()
            .await?;
        let params = self
            .parameters
            .iter()
            .map(|p| p as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        let rows = tx.query(&self.sql, &params).await?;
        tx.commit().await?;
        Ok(rows)
    }
}
fn ident(name: &str) -> Result<String> {
    if name.is_empty() || name.contains('\0') {
        return Err(ChunkError::Invalid("SQL identifier"));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}
