//! Bounded, trusted-owner logical relational copies. See the crate README for exclusions.
//!
//! ```
//! use giggles_export::{Archive, Bounds, decode, encode};
//! let empty = Archive { tables: vec![], rows: vec![] };
//! let bytes = encode(&empty, Bounds::default())?;
//! assert_eq!(decode(&bytes, Bounds::default())?, empty);
//! # Ok::<(), giggles_export::Error>(())
//! ```
mod format;
mod io;

pub use format::{decode, encode};
use giggles_core::relational::{self, DataType, Scan, TableDef, Value};
use giggles_core::{Admin, CoreError, Limits, SqlSession};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Failures never authorize overwriting a destination or removing retained artifacts.
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Core(CoreError),
    Invalid(String),
    Limit,
    Integrity,
    Exists,
    Injected,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<CoreError> for Error {
    fn from(e: CoreError) -> Self {
        Self::Core(e)
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            Self::Exists
        } else {
            Self::Io(e)
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;
fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

/// Caller limits can narrow, never enlarge, the version-one profile.
#[derive(Clone, Copy, Debug)]
pub struct Bounds {
    pub file_bytes: usize,
    pub line_bytes: usize,
    pub tables: usize,
    pub rows_per_table: usize,
    pub total_rows: usize,
}
impl Default for Bounds {
    fn default() -> Self {
        Self {
            file_bytes: 16 * 1024 * 1024,
            line_bytes: 512 * 1024,
            tables: 128,
            rows_per_table: 1024,
            total_rows: 131072,
        }
    }
}
impl Bounds {
    fn check(self) -> Result<()> {
        let hard = Self::default();
        for (v, max) in [
            (self.file_bytes, hard.file_bytes),
            (self.line_bytes, hard.line_bytes),
            (self.tables, hard.tables),
            (self.rows_per_table, hard.rows_per_table),
            (self.total_rows, hard.total_rows),
        ] {
            if v == 0 || v > max {
                return Err(Error::Limit);
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub not_null: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<usize>,
}
/// Integers are decimal strings and byte strings are lower-case hexadecimal on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Cell {
    Null,
    Bool(bool),
    Int(String),
    Text(String),
    Bytes(String),
    Numeric(String),
    Float4(u32),
    Float8(u64),
    Date(String),
    Time(String),
    TimeTz(String),
    Timestamp(String),
    TimestampTz(String),
    Interval(String),
}
impl Cell {
    pub fn from_value(v: Value) -> Self {
        match v {
            Value::Date(v) => Self::Date(format::hex(&v.to_binary())),
            Value::Time(v) => Self::Time(format::hex(&v.to_binary())),
            Value::TimeTz(v) => Self::TimeTz(format::hex(&v.to_binary())),
            Value::Timestamp(v) => Self::Timestamp(format::hex(&v.to_binary())),
            Value::TimestampTz(v) => Self::TimestampTz(format::hex(&v.to_binary())),
            Value::Interval(v) => Self::Interval(format::hex(&v.to_binary())),

            Value::Numeric(v) => Self::Numeric(v.to_string()),
            Value::Float4(v) => Self::Float4(v.to_bits()),
            Value::Float8(v) => Self::Float8(v.to_bits()),
            Value::Null => Self::Null,
            Value::Bool(v) => Self::Bool(v),
            Value::Int(v) => Self::Int(v.to_string()),
            Value::Text(v) => Self::Text(v),
            Value::Bytes(v) => Self::Bytes(format::hex(&v)),
        }
    }
    pub fn to_value(&self) -> Result<Value> {
        Ok(match self {
            Self::Date(v) => Value::Date(
                relational::Date::from_binary(&format::unhex(v)?)
                    .map_err(|_| invalid("Date binary"))?,
            ),
            Self::Time(v) => Value::Time(
                relational::PgTime::from_binary(&format::unhex(v)?)
                    .map_err(|_| invalid("Time binary"))?,
            ),
            Self::TimeTz(v) => Value::TimeTz(
                relational::TimeTz::from_binary(&format::unhex(v)?)
                    .map_err(|_| invalid("TimeTz binary"))?,
            ),
            Self::Timestamp(v) => Value::Timestamp(
                relational::Timestamp::from_binary(&format::unhex(v)?)
                    .map_err(|_| invalid("Timestamp binary"))?,
            ),
            Self::TimestampTz(v) => Value::TimestampTz(
                relational::TimestampTz::from_binary(&format::unhex(v)?)
                    .map_err(|_| invalid("TimestampTz binary"))?,
            ),
            Self::Interval(v) => Value::Interval(
                relational::Interval::from_binary(&format::unhex(v)?)
                    .map_err(|_| invalid("Interval binary"))?,
            ),

            Self::Numeric(v) => {
                let n = relational::Numeric::parse(v).map_err(|_| invalid("numeric"))?;
                if n.to_string() != *v {
                    return Err(invalid("noncanonical numeric"));
                }
                Value::Numeric(n)
            }
            Self::Float4(v) => Value::Float4(relational::Float4::from_bits(*v)),
            Self::Float8(v) => Value::Float8(relational::Float8::from_bits(*v)),
            Self::Null => Value::Null,
            Self::Bool(v) => Value::Bool(*v),
            Self::Text(v) => Value::Text(v.clone()),
            Self::Int(v) => {
                let n = v.parse::<i64>().map_err(|_| invalid("integer"))?;
                if n.to_string() != *v {
                    return Err(invalid("noncanonical integer"));
                }
                Value::Int(n)
            }
            Self::Bytes(v) => Value::Bytes(format::unhex(v)?),
        })
    }
}
/// Table rows are in ascending core memcomparable composite primary-key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Archive {
    pub tables: Vec<Table>,
    pub rows: Vec<Vec<Vec<Cell>>>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    pub profile: String,
    pub sha256: String,
    pub tables: usize,
    pub rows: usize,
}
impl Archive {
    fn report(&self, bytes: &[u8]) -> Report {
        Report {
            profile: format::profile(&self.tables).1.into(),
            sha256: format::hash(bytes),
            tables: self.tables.len(),
            rows: self.rows.iter().map(Vec::len).sum(),
        }
    }
}

/// Explicit trusted test/embedding checkpoints. No environment variables inject failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Point {
    CatalogBefore,
    Snapshot,
    Captured,
    StageSynced,
    ExportPublished,
    ImportCreated,
    ImportStaged,
    ImportCommitted,
    ImportVerified,
}
type Hook<'a> = &'a mut dyn FnMut(Point) -> Result<()>;
struct Transaction(SqlSession);
impl Drop for Transaction {
    fn drop(&mut self) {
        self.0.rollback();
    }
}
fn table(t: TableDef) -> Result<Table> {
    if t.hidden_rowid().is_some() {
        return Err(Error::Core(CoreError::Sql(relational::SqlError::Unsupported)));
    }
    // The logical archive has no varchar/char identity or typmod (E2a′). Refuse rather
    // than silently exporting those columns as TEXT.
    if t
        .columns
        .iter()
        .any(|c| matches!(c.data_type, DataType::Varchar | DataType::Bpchar))
    {
        return Err(Error::Core(CoreError::Sql(relational::SqlError::Unsupported)));
    }
    Ok(Table {
        name: t.name,
        columns: t
            .columns
            .into_iter()
            .map(|c| Column {
                name: c.name,
                data_type: c.data_type,
                not_null: c.not_null,
            })
            .collect(),
        primary_key: t.primary_key,
    })
}

pub fn capture(admin: &Admin, bounds: Bounds) -> Result<Archive> {
    capture_with_hook(admin, bounds, &mut |_| Ok(()))
}
/// The catalog bracket certifies that the full-state BEGIN snapshot has this schema.
pub fn capture_with_hook(admin: &Admin, bounds: Bounds, hook: Hook<'_>) -> Result<Archive> {
    bounds.check()?;
    let before = admin.catalog()?;
    hook(Point::CatalogBefore)?;
    let mut tx = Transaction(admin.sql_session());
    tx.0.execute("BEGIN", &[], None)?;
    let after = admin.catalog()?;
    if before != after {
        return Err(invalid("catalog changed across BEGIN"));
    }
    // This profile refuses known legacy/task authority rather than silently
    // presenting an incomplete operational copy. Nothing from Snapshot is copied.
    let legacy = admin.snapshot()?;
    if !legacy.rows.is_empty()
        || !legacy.tasks.is_empty()
        || !legacy.operations.is_empty()
        || !legacy.approvals.is_empty()
    {
        return Err(invalid(
            "nonrelational data or task authority is outside logical profile",
        ));
    }
    drop(legacy);
    hook(Point::Snapshot)?;
    let tables = before.1.into_iter().map(table).collect::<Result<Vec<_>>>()?;
    let mut archive = Archive {
        tables,
        rows: Vec::new(),
    };
    format::schema(&archive.tables, bounds)?;
    let mut encoded_bytes = format::header_charge(&archive.tables, bounds)?;
    let mut total = 0usize;
    for t in &archive.tables {
        // Core supports at most MAX_ROWS; use the full limit so a narrower caller
        // bound cannot silently turn a complete scan into a prefix.
        let result = tx.0.scan(Scan {
            table: t.name.clone(),
            projection: t.columns.iter().map(|c| c.name.clone()).collect(),
            prefix: vec![],
            lower: None,
            upper: None,
            reverse: false,
            limit: relational::MAX_ROWS,
        })?;
        // A composite-key successor is the union of these disjoint suffix
        // ranges. Probe each in the same snapshot to detect silent truncation.
        if result.rows.len() == relational::MAX_ROWS {
            let last = &result.rows[result.rows.len() - 1].values;
            let key: Vec<_> = t.primary_key.iter().map(|i| last[*i].clone()).collect();
            for i in (0..key.len()).rev() {
                let more = tx.0.scan(Scan {
                    table: t.name.clone(),
                    projection: vec![t.columns[t.primary_key[0]].name.clone()],
                    prefix: key[..i].to_vec(),
                    lower: Some((key[i].clone(), false)),
                    upper: None,
                    reverse: false,
                    limit: 1,
                })?;
                if !more.rows.is_empty() {
                    return Err(Error::Limit);
                }
            }
        }
        total = total.checked_add(result.rows.len()).ok_or(Error::Limit)?;
        if result.rows.len() > bounds.rows_per_table || total > bounds.total_rows {
            return Err(Error::Limit);
        }
        let mut rows = Vec::new();
        for r in result.rows {
            let row = r
                .values
                .into_iter()
                .map(Cell::from_value)
                .collect::<Vec<_>>();
            format::row_charge(archive.rows.len(), &row, &mut encoded_bytes, bounds)?;
            rows.push(row);
        }
        archive.rows.push(rows);
    }
    format::validate(&archive, bounds)?;
    hook(Point::Captured)?;
    Ok(archive)
}
pub fn export(admin: &Admin, path: impl AsRef<Path>, bounds: Bounds) -> Result<Report> {
    export_with_hook(admin, path, bounds, &mut |_| Ok(()))
}
pub fn export_with_hook(
    admin: &Admin,
    path: impl AsRef<Path>,
    bounds: Bounds,
    hook: Hook<'_>,
) -> Result<Report> {
    let archive = capture_with_hook(admin, bounds, hook)?;
    let bytes = encode(&archive, bounds)?;
    io::publish(path.as_ref(), &bytes, bounds, hook)?;
    Ok(archive.report(&bytes))
}
pub fn verify(path: impl AsRef<Path>, bounds: Bounds) -> Result<Report> {
    let bytes = io::read(path.as_ref(), bounds)?;
    let a = decode(&bytes, bounds)?;
    Ok(a.report(&bytes))
}
pub fn import(input: impl AsRef<Path>, target: impl AsRef<Path>, bounds: Bounds) -> Result<Report> {
    import_with_hook(input, target, bounds, &mut |_| Ok(()))
}
pub fn import_with_hook(
    input: impl AsRef<Path>,
    target: impl AsRef<Path>,
    bounds: Bounds,
    hook: Hook<'_>,
) -> Result<Report> {
    let bytes = io::read(input.as_ref(), bounds)?;
    let archive = decode(&bytes, bounds)?;
    let admin = Admin::create(target.as_ref(), Limits::default())?;
    hook(Point::ImportCreated)?;
    let mut tx = Transaction(admin.sql_session());
    tx.0.execute("BEGIN", &[], None)?;
    for (t, rows) in archive.tables.iter().zip(&archive.rows) {
        tx.0.execute(&format::create_sql(t), &[], None)?;
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            t.name,
            t.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(","),
            (1..=t.columns.len())
                .map(|i| format!("${i}"))
                .collect::<Vec<_>>()
                .join(",")
        );
        for row in rows {
            let values = row.iter().map(Cell::to_value).collect::<Result<Vec<_>>>()?;
            tx.0.execute(&sql, &values, None)?;
        }
    }
    hook(Point::ImportStaged)?;
    tx.0.execute("COMMIT", &[], None)?;
    hook(Point::ImportCommitted)?;
    drop(tx);
    drop(admin);
    let reopened = Admin::open(target.as_ref(), Limits::default())?;
    if capture(&reopened, bounds)? != archive {
        return Err(Error::Integrity);
    }
    hook(Point::ImportVerified)?;
    let report = archive.report(&bytes);
    io::marker(target.as_ref(), &report)?;
    drop(reopened);
    Ok(report)
}
