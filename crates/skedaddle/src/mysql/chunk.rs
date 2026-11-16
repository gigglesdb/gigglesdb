//! MySQL source ordering and short snapshot transactions. Mapped bytes are identities only.
use super::{
    binlog::MysqlValue,
    chunk_key::{MysqlKeyColumn, MysqlKeyError, MysqlTableKey},
    client::{MysqlConfig, MysqlError},
    schema::{ColumnDescriptor, TableDescriptor},
    value,
};
use crate::{
    coordinator::chunk::{ChunkPlan, SourceTableKey},
    engine::ChunkDatabase,
    event::Row,
    mapping::{
        self,
        mysql::{self, MysqlPgColumn},
    },
    pg::chunk::{ChunkRange, Key, Watermark},
};
use mysql_async::{Conn, Params, Value as Parameter, prelude::Queryable};
use std::collections::BTreeSet;

#[derive(Debug)]
pub enum MysqlChunkError {
    Invalid(&'static str),
    UnsupportedSource,
    NonIncreasingSamples,
    Key(MysqlKeyError),
    Mysql(MysqlError),
    Driver(mysql_async::Error),
    Mapping(mapping::MappingError),
    Plan(crate::coordinator::Error),
}
impl std::fmt::Display for MysqlChunkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MySQL chunks: {self:?}")
    }
}
impl std::error::Error for MysqlChunkError {}
macro_rules! conversion {
    ($ty:ty, $variant:ident) => {
        impl From<$ty> for MysqlChunkError {
            fn from(e: $ty) -> Self {
                Self::$variant(e)
            }
        }
    };
}
conversion!(MysqlKeyError, Key);
conversion!(MysqlError, Mysql);
conversion!(mysql_async::Error, Driver);
conversion!(mapping::MappingError, Mapping);
conversion!(crate::coordinator::Error, Plan);
pub type Result<T> = std::result::Result<T, MysqlChunkError>;
fn invalid(s: &'static str) -> MysqlChunkError {
    MysqlChunkError::Invalid(s)
}
pub fn ident(s: &str) -> Result<String> {
    if s.is_empty() || s.contains('\0') {
        return Err(invalid("identifier"));
    }
    Ok(format!("`{}`", s.replace('`', "``")))
}
fn descriptor(c: &MysqlKeyColumn, ordinal: usize) -> ColumnDescriptor {
    ColumnDescriptor {
        name: c.name.clone(),
        ordinal: ordinal as u64 + 1,
        data_type: c.data_type.clone(),
        column_type: c.column_type.clone(),
        character_set: c.character_set.clone(),
        collation: c.collation.clone(),
        nullable: false,
        generated: false,
    }
}
fn mappings(key: &MysqlTableKey) -> Result<Vec<MysqlPgColumn>> {
    key.validate()?;
    key.columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let p = mysql::plan_column(&descriptor(c, i))?;
            if p.type_id != c.type_id {
                return Err(invalid("key mapping identity"));
            }
            parameter_type(&p)?;
            Ok(p)
        })
        .collect()
}
fn collate(c: &MysqlKeyColumn, expression: String) -> Result<String> {
    Ok(match &c.collation {
        Some(co) => format!("({expression} COLLATE {})", ident(&co.name)?),
        None => expression,
    })
}
fn expressions(key: &MysqlTableKey, aliases: bool) -> Result<Vec<String>> {
    key.columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            collate(
                c,
                ident(&if aliases {
                    format!("__k{i}")
                } else {
                    c.name.clone()
                })?,
            )
        })
        .collect()
}
fn order(key: &MysqlTableKey, aliases: bool) -> Result<String> {
    Ok(expressions(key, aliases)?
        .iter()
        .map(|e| format!("{e} ASC"))
        .collect::<Vec<_>>()
        .join(", "))
}
fn relation(key: &MysqlTableKey) -> Result<String> {
    Ok(format!("{}.{}", ident(&key.schema)?, ident(&key.table)?))
}
fn comparison(left: &[String], op: &str, right: &[String]) -> String {
    // A scalar is necessary for single-column keys; multi-column tuples use MySQL row comparison.
    if left.len() == 1 {
        format!("{} {op} {}", left[0], right[0])
    } else {
        format!("({}) {op} ({})", left.join(", "), right.join(", "))
    }
}
fn parameter_type(p: &MysqlPgColumn) -> Result<String> {
    let c = &p.source;
    Ok(match c.data_type.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => format!(
            "CAST(? AS {})",
            if c.column_type.contains("unsigned") {
                "UNSIGNED"
            } else {
                "SIGNED"
            }
        ),
        "decimal" | "numeric" => {
            let args = c
                .column_type
                .split_once('(')
                .and_then(|(_, s)| s.split_once(')'))
                .map(|(s, _)| s)
                .unwrap_or("10,0");
            if !args
                .bytes()
                .all(|b| b.is_ascii_digit() || b == b',' || b == b' ')
            {
                return Err(invalid("decimal type"));
            }
            format!("CAST(? AS DECIMAL({args}))")
        }
        "date" => "CAST(? AS DATE)".into(),
        "datetime" | "timestamp" => format!("CAST(? AS DATETIME({}))", p.type_modifier),
        "binary" | "varbinary" => "CAST(? AS BINARY)".into(),
        "char" | "varchar" => format!(
            "CONVERT(? USING {})",
            ident(c.character_set.as_deref().ok_or(invalid("charset"))?)?
        ),
        _ => return Err(invalid("unsupported key parameter type")),
    })
}
fn parameters(key: &MysqlTableKey, values: &Key) -> Result<(Vec<String>, Vec<Parameter>)> {
    key.validate_key(values)?;
    let plans = mappings(key)?;
    let mut sql = Vec::new();
    let mut bindings = Vec::new();
    for ((c, p), v) in key.columns.iter().zip(plans).zip(values) {
        let source = mysql::inverse(&p, v)?;
        let binding = match source {
            MysqlValue::Signed(n) => Parameter::Int(n),
            MysqlValue::Unsigned(n) => Parameter::UInt(n),
            MysqlValue::Binary(b) => Parameter::Bytes(b),
            MysqlValue::Text { bytes, .. } => Parameter::Bytes(bytes),
            MysqlValue::Decimal(s) | MysqlValue::Year(s) => Parameter::Bytes(s.into_bytes()),
            MysqlValue::DateTime(y, m, d, h, min, s, us) => Parameter::Date(y, m, d, h, min, s, us),
            MysqlValue::Timestamp(_) => Parameter::Bytes(
                value::to_opaque(&p.source, &source)?
                    .bytes
                    .ok_or(invalid("timestamp"))?,
            ),
            _ => return Err(invalid("unsupported key parameter")),
        };
        sql.push(collate(c, parameter_type(&p)?)?);
        bindings.push(binding);
    }
    Ok((sql, bindings))
}
fn range_predicate(
    key: &MysqlTableKey,
    range: &ChunkRange,
    aliases: bool,
) -> Result<(String, Vec<Parameter>)> {
    if range.id.table != key.event_table {
        return Err(invalid("chunk table"));
    }
    let mut bindings = Vec::new();
    let mut predicates = Vec::new();
    for (bound, op) in [(&range.lo, ">="), (&range.hi, "<")] {
        if let Some(bound) = bound {
            let (sql, p) = parameters(key, bound)?;
            predicates.push(comparison(&expressions(key, aliases)?, op, &sql));
            bindings.extend(p);
        }
    }
    Ok((
        if predicates.is_empty() {
            "TRUE".into()
        } else {
            predicates.join(" AND ")
        },
        bindings,
    ))
}
#[derive(Debug)]
pub struct MysqlChunkQuery {
    pub sql: String,
    pub parameters: Vec<Parameter>,
}
impl MysqlTableKey {
    pub fn sampling_query(&self, size: usize) -> Result<MysqlChunkQuery> {
        mappings(self)?;
        let size = u64::try_from(size)
            .ok()
            .filter(|n| *n > 0 && *n <= i64::MAX as u64)
            .ok_or(invalid("chunk size"))?;
        let projections = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| Ok(format!("{} AS `__k{i}`", ident(&c.name)?)))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        // Binary projections preserve column charset bytes, even on a UTF8 connection.
        let aliases = (0..self.columns.len())
            .map(|i| format!("CAST(`__k{i}` AS BINARY)"))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(MysqlChunkQuery {
            sql: format!(
                "SELECT {aliases} FROM (SELECT {projections}, ROW_NUMBER() OVER (ORDER BY {}) AS `__rn` FROM {}) AS `__sample` WHERE `__rn` > 1 AND (`__rn` - 1) % ? = 0 ORDER BY {}",
                order(self, false)?,
                relation(self)?,
                order(self, true)?
            ),
            parameters: vec![Parameter::UInt(size)],
        })
    }
    pub fn read_query_columns(
        &self,
        range: &ChunkRange,
        columns: &[ColumnDescriptor],
    ) -> Result<MysqlChunkQuery> {
        mappings(self)?;
        let names: BTreeSet<_> = columns.iter().map(|c| &c.name).collect();
        if names.is_empty()
            || names.len() != columns.len()
            || self.columns.iter().any(|c| !names.contains(&c.name))
        {
            return Err(invalid("projection must include every key exactly once"));
        }
        let (predicate, parameters_out) = range_predicate(self, range, false)?;
        let projection = columns
            .iter()
            .map(|c| {
                let name = ident(&c.name)?;
                // Prepared binary-protocol floats retain every bit; text formatting need not.
                Ok(
                    if matches!(c.data_type.as_str(), "float" | "double" | "real") {
                        name
                    } else {
                        format!("CAST({name} AS BINARY)")
                    },
                )
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        Ok(MysqlChunkQuery {
            sql: format!(
                "SELECT {projection} FROM {} WHERE {} ORDER BY {}",
                relation(self)?,
                predicate,
                order(self, false)?
            ),
            parameters: parameters_out,
        })
    }
    /// Source-side comparison, also usable for keys whose rows no longer exist.
    pub fn adjacent_query(&self, left: &Key, right: &Key) -> Result<MysqlChunkQuery> {
        let (l, mut p) = parameters(self, left)?;
        let (r, rp) = parameters(self, right)?;
        p.extend(rp);
        Ok(MysqlChunkQuery {
            sql: format!("SELECT {}", comparison(&l, "<", &r)),
            parameters: p,
        })
    }
    /// One row per supplied key, one boolean per range. No source row need exist.
    /// The caller bounds key count; the prepared-statement parameter cap is also enforced here.
    pub fn membership_query(&self, ranges: &[ChunkRange], keys: &[Key]) -> Result<MysqlChunkQuery> {
        mappings(self)?;
        if ranges.is_empty() || keys.is_empty() {
            return Err(invalid("empty membership query"));
        }
        let mut bindings = Vec::new();
        let mut predicates = Vec::new();
        for range in ranges {
            let (sql, p) = range_predicate(self, range, true)?;
            predicates.push(format!("({sql})"));
            bindings.extend(p);
        }
        let mut rows = Vec::new();
        for (ordinal, key) in keys.iter().enumerate() {
            let (sql, p) = parameters(self, key)?;
            let mut cells = vec!["? AS `__ordinal`".to_string()];
            cells.extend(
                sql.iter()
                    .enumerate()
                    .map(|(i, s)| format!("{s} AS `__k{i}`")),
            );
            bindings.push(Parameter::UInt(ordinal as u64));
            bindings.extend(p);
            rows.push(format!("SELECT {}", cells.join(", ")));
        }
        if bindings.len() > 65535 {
            return Err(invalid("membership parameter limit"));
        }
        Ok(MysqlChunkQuery {
            sql: format!(
                "SELECT `__ordinal`, {} FROM ({}) AS `__keys`",
                predicates.join(", "),
                rows.join(" UNION ALL ")
            ),
            parameters: bindings,
        })
    }
}
/// Decode a binary-projection row through capture's mysql.v1 -> skva.v1 path.
pub fn map_row(columns: &[ColumnDescriptor], row: mysql_async::Row) -> Result<Row> {
    if row.len() != columns.len() {
        return Err(invalid("row width"));
    }
    columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let raw = row.as_ref(i).ok_or(invalid("row column"))?;
            let opaque = match raw {
                Parameter::NULL => {
                    let mut nullable = c.clone();
                    nullable.nullable = true;
                    value::to_opaque(&nullable, &MysqlValue::Null)?
                }
                Parameter::Float(f) => value::to_opaque(c, &MysqlValue::Float(*f))?,
                Parameter::Double(f) => value::to_opaque(c, &MysqlValue::Double(*f))?,
                Parameter::Bytes(b) => {
                    // Derive the exact wire identity without duplicating its codec.
                    let mut nullable = c.clone();
                    nullable.nullable = true;
                    let mut opaque = value::to_opaque(&nullable, &MysqlValue::Null)?;
                    let mut b = b.clone();
                    match c.data_type.as_str() {
                        "date" => b.extend(b" 00:00:00"),
                        "year" => {
                            let s = std::str::from_utf8(&b).map_err(|_| invalid("year"))?;
                            b = format!("{:04}", s.parse::<u16>().map_err(|_| invalid("year"))?)
                                .into_bytes();
                        }
                        "json" => {
                            b = serde_json::to_vec(
                                &serde_json::from_slice::<serde_json::Value>(&b)
                                    .map_err(|_| invalid("JSON"))?,
                            )
                            .map_err(|_| invalid("JSON"))?
                        }
                        _ => {}
                    }
                    opaque.bytes = Some(b);
                    opaque
                }
                _ => return Err(invalid("expected binary projection")),
            };
            Ok((
                c.name.clone(),
                mysql::to_pg_opaque(&mysql::plan_column(c)?, &opaque)?,
            ))
        })
        .collect()
}
async fn begin(conn: &mut Conn) -> Result<()> {
    conn.query_drop("SET SESSION time_zone='+00:00'").await?;
    conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .await?;
    conn.query_drop("START TRANSACTION READ ONLY").await?;
    Ok(())
}
async fn finish<T>(conn: &mut Conn, result: Result<T>) -> Result<T> {
    match result {
        Ok(value) => {
            conn.query_drop("COMMIT").await?;
            Ok(value)
        }
        Err(error) => {
            let _ = conn.query_drop("ROLLBACK").await;
            Err(error)
        }
    }
}
pub async fn sample_plan(conn: &mut Conn, key: &MysqlTableKey, size: usize) -> Result<ChunkPlan> {
    let query = key.sampling_query(size)?;
    let columns = key
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| descriptor(c, i))
        .collect::<Vec<_>>();
    begin(conn).await?;
    let result = async {
        let rows: Vec<mysql_async::Row> = conn
            .exec(query.sql, Params::Positional(query.parameters))
            .await?;
        let mut samples = Vec::<Key>::new();
        for row in rows {
            let mapped = map_row(&columns, row)?;
            let sample = key
                .columns
                .iter()
                .map(|c| {
                    mapping::Value::from_canonical_bytes(
                        mapped[&c.name]
                            .bytes
                            .as_deref()
                            .ok_or(invalid("NULL key"))?,
                    )
                    .map_err(Into::into)
                })
                .collect::<Result<Key>>()?;
            if let Some(previous) = samples.last() {
                let q = key.adjacent_query(previous, &sample)?;
                if conn
                    .exec_first::<u8, _, _>(q.sql, Params::Positional(q.parameters))
                    .await?
                    != Some(1)
                {
                    return Err(MysqlChunkError::NonIncreasingSamples);
                }
            }
            samples.push(sample);
        }
        Ok(ChunkPlan::new(key.clone(), size, samples)?)
    }
    .await;
    finish(conn, result).await
}
pub struct MysqlChunks {
    admin: Conn,
    reader: Conn,
    tables: Vec<TableDescriptor>,
}
impl MysqlChunks {
    /// Two dedicated connections. Installing the control table is a separate disclosed step.
    pub async fn connect(config: &MysqlConfig, tables: Vec<TableDescriptor>) -> Result<Self> {
        let mut admin = config.connect().await?.conn;
        admin.query_drop("SET autocommit=1").await?;
        let reader = config.connect().await?.conn;
        Ok(Self {
            admin,
            reader,
            tables,
        })
    }
    pub async fn emit(&mut self, marker: Watermark) -> Result<()> {
        super::control::emit_watermark(&mut self.admin, &marker).await?;
        Ok(())
    }
    pub async fn read(&mut self, plan: &ChunkPlan, chunk: &ChunkRange) -> Result<Vec<Row>> {
        let SourceTableKey::Mysql(key) = &plan.table else {
            return Err(MysqlChunkError::UnsupportedSource);
        };
        if ChunkPlan::new(key.clone(), plan.chunk_size, plan.samples.clone())? != *plan {
            return Err(invalid("samples/plan mismatch"));
        }
        if plan
            .ranges
            .get(usize::try_from(chunk.id.ordinal).map_err(|_| invalid("ordinal"))?)
            != Some(chunk)
        {
            return Err(invalid("foreign chunk"));
        }
        let table = self
            .tables
            .iter()
            .find(|t| t.schema == key.schema && t.table == key.table)
            .ok_or(invalid("unqualified table"))?;
        if table.primary_key
            != key
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
        {
            return Err(invalid("primary key drift"));
        }
        for c in &key.columns {
            let actual = table
                .columns
                .iter()
                .find(|a| a.name == c.name)
                .ok_or(invalid("key column"))?;
            let expected = descriptor(c, 0);
            if actual.data_type != expected.data_type
                || actual.column_type != expected.column_type
                || actual.character_set != expected.character_set
                || actual.collation != expected.collation
                || actual.nullable
                || actual.generated
            {
                return Err(invalid("key metadata drift"));
            }
        }
        let q = key.read_query_columns(chunk, &table.columns)?;
        begin(&mut self.reader).await?;
        let result = async {
            let rows: Vec<mysql_async::Row> = self
                .reader
                .exec(q.sql, Params::Positional(q.parameters))
                .await?;
            rows.into_iter()
                .map(|r| map_row(&table.columns, r))
                .collect()
        }
        .await;
        finish(&mut self.reader, result).await
    }
    pub async fn disconnect(self) -> Result<()> {
        self.admin.disconnect().await?;
        self.reader.disconnect().await?;
        Ok(())
    }
}
fn engine_error(error: MysqlChunkError) -> crate::coordinator::Error {
    crate::coordinator::Error::RecoveryRequired(error.to_string())
}
impl ChunkDatabase for MysqlChunks {
    async fn emit(&mut self, marker: Watermark) -> crate::engine::Result<()> {
        MysqlChunks::emit(self, marker).await.map_err(engine_error)
    }
    async fn read(
        &mut self,
        plan: &ChunkPlan,
        chunk: &ChunkRange,
    ) -> crate::engine::Result<Vec<Row>> {
        MysqlChunks::read(self, plan, chunk)
            .await
            .map_err(engine_error)
    }
}

#[cfg(test)]
mod tests;
