//! Independent durable result format: `GDOU`, version 1 or 2, explicit numeric tags.
//!
//! Integers are big endian. Counts/indices are u16 in version 1 and u32 in
//! version 2 (whose NO_ROW sentinel is u32::MAX), except columns, summaries,
//! emissions and step indices (u8). Version 1 is written whenever every count
//! fits its 1024-row rules; version 2 only for a V2-engine result that does
//! not, and a version 2 record whose counts all fit version 1 is rejected.
//! Text/byte payload lengths are u32; names
//! have a u8 byte length. Row arity comes from its enclosing column list.
//! The header is followed by the accepting catalog epoch (u64), output schema
//! (operation tag 0 / transaction tag 1), then result tag 0 / 1 / terminal 2.
//! Schema scalar tag 0 carries optional type and nullability; row/rows tags
//! 1/2 carry columns. This schema remains present for terminal business errors.
//! No Rust enum discriminant, usize, serde representation or debug string is
//! part of this format. Both encoding and decoding validate before allocating
//! a second owned representation of a result.

use super::{
    MAX_ONCE_RESULT_BYTES, OnceDescription, OnceError, OnceOutcome, OnceResult, OnceSchema,
};
use crate::{
    CoreError,
    operations::{
        ConflictStatus, ConflictTupleOutcome, EmitDescription, EmittedValue, Error,
        MAX_EMIT_VALUES, MAX_TRANSACTION_ITEMS, Outcome, StepSummary, TransactionOutcome,
    },
    relational::{ColumnDef, DataType, MAX_COLUMNS, MAX_RESULT_BYTES, MAX_ROWS, MAX_TARGET_COLUMNS, SqlRow, Value},
};

const MAGIC: &[u8; 4] = b"GDOU";
const VERSION: u8 = 1;
const WIDE_VERSION: u8 = 2;
type BusinessResult = std::result::Result<OnceOutcome, Error>;
type Checked<T> = std::result::Result<T, Fault>;

#[derive(Clone, Copy, Debug)]
enum Fault {
    Invalid,
    Bound,
}
fn invalid(condition: bool) -> Checked<()> {
    if condition {
        Ok(())
    } else {
        Err(Fault::Invalid)
    }
}
fn encode_error(error: Fault) -> OnceError {
    match error {
        Fault::Invalid => OnceError::InvalidOutcome,
        Fault::Bound => OnceError::RetentionUnavailable,
    }
}
fn corrupt(_: Fault) -> CoreError {
    CoreError::Corrupt("once outcome encoding".into())
}

/// Admit the entire borrowed result before reserving the encoded buffer.
/// Version 1 only: the legacy engine keeps its 1024-row receipts.
pub(crate) fn encode(
    description: &OnceDescription,
    result: &BusinessResult,
) -> OnceResult<Vec<u8>> {
    encode_version(description, result, false)
}

/// V2 engine: version 1 whenever it admits the result (byte-identical to
/// [`encode`]), otherwise version 2 with u32 counts and no row-count cap.
pub(crate) fn encode_wide(
    description: &OnceDescription,
    result: &BusinessResult,
) -> OnceResult<Vec<u8>> {
    match encode_version(description, result, false) {
        Err(OnceError::InvalidOutcome) => encode_version(description, result, true),
        other => other,
    }
}

fn encode_version(
    description: &OnceDescription,
    result: &BusinessResult,
    wide: bool,
) -> OnceResult<Vec<u8>> {
    let mut counter = Writer::new(Counter { len: 0 }, wide);
    counter.result(description, result).map_err(encode_error)?;
    invalid(!wide || counter.widest > MAX_ROWS).map_err(encode_error)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(counter.sink.len)
        .map_err(|_| OnceError::RetentionUnavailable)?;
    let mut writer = Writer::new(Buffer { bytes }, wide);
    writer.result(description, result).map_err(encode_error)?;
    Ok(writer.sink.bytes)
}

/// True for a version 2 (u32 count) record; callers validate it separately.
pub(crate) fn is_wide(bytes: &[u8]) -> bool {
    bytes.get(..5).is_some_and(|h| h[..4] == *MAGIC && h[4] == WIDE_VERSION)
}

/// Validate without constructing strings, vectors, rows or public result DTOs.
pub(crate) fn validate(bytes: &[u8]) -> std::result::Result<(), CoreError> {
    Reader::new(bytes, false)
        .result()
        .map(|_| ())
        .map_err(corrupt)
}

/// The first pass rejects malformed/oversized data before result allocation.
pub(crate) fn decode(
    bytes: &[u8],
) -> std::result::Result<(OnceDescription, BusinessResult), CoreError> {
    validate(bytes)?;
    Reader::new(bytes, true)
        .result()
        .map_err(corrupt)?
        .ok_or_else(|| corrupt(Fault::Invalid))
}

#[derive(Clone, Copy)]
struct Counts {
    matched: usize,
    affected: usize,
    changed: usize,
}
impl Counts {
    fn check(self, wide: bool) -> Checked<()> {
        invalid(
            (wide || self.matched <= MAX_ROWS)
                && self.changed <= self.affected
                && self.affected <= self.matched,
        )
    }
}
fn check_transaction_indices(count: usize, last: Option<usize>, emits: usize) -> Checked<()> {
    invalid(count <= MAX_TRANSACTION_ITEMS && emits <= MAX_EMIT_VALUES)?;
    let emit_item = usize::from(emits != 0);
    invalid(count + emit_item <= MAX_TRANSACTION_ITEMS)?;
    if let Some(last) = last {
        invalid(last < MAX_TRANSACTION_ITEMS - emit_item)?;
    }
    Ok(())
}
fn name_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.as_bytes()[0].is_ascii_lowercase()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}
fn check_names<'a>(count: usize, names: impl Iterator<Item = &'a str>) -> Checked<()> {
    invalid(count <= MAX_TARGET_COLUMNS)?;
    let mut seen = Vec::new();
    seen.try_reserve_exact(count).map_err(|_| Fault::Bound)?;
    for name in names {
        invalid(seen.len() < count && name_valid(name) && !seen.contains(&name))?;
        seen.push(name);
    }
    Ok(())
}

// These values are permanent wire tags, not casts of the public DataType enum.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Text,
    Bytes,
    Numeric,
    Real,
    DoublePrecision,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Interval,
}
impl Kind {
    fn tag(self) -> u8 {
        match self {
            Self::Boolean => 0,
            Self::SmallInt => 1,
            Self::Integer => 2,
            Self::BigInt => 3,
            Self::Text => 4,
            Self::Bytes => 5,
            Self::Numeric => 6,
            Self::Real => 7,
            Self::DoublePrecision => 8,
            Self::Date => 9,
            Self::Time => 10,
            Self::TimeTz => 11,
            Self::Timestamp => 12,
            Self::TimestampTz => 13,
            Self::Interval => 14,
        }
    }
    fn parse(tag: u8) -> Checked<Self> {
        Ok(match tag {
            0 => Self::Boolean,
            1 => Self::SmallInt,
            2 => Self::Integer,
            3 => Self::BigInt,
            4 => Self::Text,
            5 => Self::Bytes,
            6 => Self::Numeric,
            7 => Self::Real,
            8 => Self::DoublePrecision,
            9 => Self::Date,
            10 => Self::Time,
            11 => Self::TimeTz,
            12 => Self::Timestamp,
            13 => Self::TimestampTz,
            14 => Self::Interval,

            _ => return Err(Fault::Invalid),
        })
    }
    fn from_type(ty: &DataType) -> Self {
        match ty {
            DataType::Boolean => Self::Boolean,
            DataType::SmallInt => Self::SmallInt,
            DataType::Integer => Self::Integer,
            DataType::BigInt => Self::BigInt,
            DataType::RegType
            | DataType::ExpressionArray(_)
            | DataType::Uuid
            | DataType::Jsonb
            | DataType::IntArray
            | DataType::BigIntArray
            | DataType::TextArray
            | DataType::UuidArray
            | DataType::TsVector | DataType::TsQuery | DataType::RegConfig | DataType::RegDictionary
            | DataType::Oid | DataType::PgChar | DataType::RealArray | DataType::PgCharArray | DataType::SmallIntArray | DataType::RegDictionaryArray
            | DataType::Text
            | DataType::Varchar
            | DataType::Bpchar => Self::Text,
            DataType::Bytes => Self::Bytes,
            DataType::Numeric => Self::Numeric,
            DataType::Real => Self::Real,
            DataType::DoublePrecision => Self::DoublePrecision,
            DataType::Date => Self::Date,
            DataType::Time => Self::Time,
            DataType::TimeTz => Self::TimeTz,
            DataType::Timestamp => Self::Timestamp,
            DataType::TimestampTz => Self::TimestampTz,
            DataType::Interval => Self::Interval,
        }
    }
    fn data_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::SmallInt => DataType::SmallInt,
            Self::Integer => DataType::Integer,
            Self::BigInt => DataType::BigInt,
            Self::Text => DataType::Text,
            Self::Bytes => DataType::Bytes,
            Self::Numeric => DataType::Numeric,
            Self::Real => DataType::Real,
            Self::DoublePrecision => DataType::DoublePrecision,
            Self::Date => DataType::Date,
            Self::Time => DataType::Time,
            Self::TimeTz => DataType::TimeTz,
            Self::Timestamp => DataType::Timestamp,
            Self::TimestampTz => DataType::TimestampTz,
            Self::Interval => DataType::Interval,
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
struct Column<'a> {
    id: u64,
    name: &'a str,
    kind: Kind,
    not_null: bool,
}
#[derive(Clone, Copy)]
enum Shape {
    Null,
    Boolean,
    Int(i64),
    Text,
    Bytes,
    Numeric,
    Real,
    DoublePrecision,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Interval,
}
fn check_type(shape: Shape, column: Option<Column<'_>>) -> Checked<()> {
    let Some(column) = column else {
        return Ok(());
    };
    invalid(match (shape, column.kind) {
        (Shape::Null, _) => !column.not_null,
        (Shape::Date, Kind::Date)
        | (Shape::Time, Kind::Time)
        | (Shape::TimeTz, Kind::TimeTz)
        | (Shape::Timestamp, Kind::Timestamp)
        | (Shape::TimestampTz, Kind::TimestampTz)
        | (Shape::Interval, Kind::Interval)
        | (Shape::Numeric, Kind::Numeric)
        | (Shape::Real, Kind::Real)
        | (Shape::DoublePrecision, Kind::DoublePrecision)
        | (Shape::Boolean, Kind::Boolean)
        | (Shape::Text, Kind::Text)
        | (Shape::Bytes, Kind::Bytes) => true,
        (Shape::Int(n), Kind::SmallInt) => i16::try_from(n).is_ok(),
        (Shape::Int(n), Kind::Integer) => i32::try_from(n).is_ok(),
        (Shape::Int(_), Kind::BigInt) => true,
        _ => false,
    })
}
fn scalar_column(kind: Kind, nullable: bool) -> Column<'static> {
    Column {
        id: 0,
        name: "",
        kind,
        not_null: !nullable,
    }
}
fn charge(remaining: &mut usize, bytes: usize) -> Checked<()> {
    *remaining = remaining.checked_sub(bytes).ok_or(Fault::Bound)?;
    Ok(())
}

// Returned-row indices are below affected <= matched == tuple count. Version 1
// keeps a fixed stack bitset; only a version 2 count above it allocates one.
enum Seen {
    Fixed([u64; MAX_ROWS / 64]),
    Wide(Vec<u64>),
}
impl Seen {
    fn new(count: usize) -> Checked<Self> {
        if count <= MAX_ROWS {
            return Ok(Self::Fixed([0; MAX_ROWS / 64]));
        }
        let mut words = Vec::new();
        words
            .try_reserve_exact(count.div_ceil(64))
            .map_err(|_| Fault::Bound)?;
        words.resize(count.div_ceil(64), 0);
        Ok(Self::Wide(words))
    }
    /// Mark `row`; false when it was already marked or is out of range.
    fn mark(&mut self, row: usize) -> bool {
        let words = match self {
            Self::Fixed(words) => words.as_mut_slice(),
            Self::Wide(words) => words.as_mut_slice(),
        };
        words.get_mut(row / 64).is_some_and(|word| {
            let bit = 1u64 << (row % 64);
            let fresh = *word & bit == 0;
            *word |= bit;
            fresh
        })
    }
}
struct ConflictCheck {
    counts: Counts,
    row_count: Option<usize>,
    seen: Seen,
    affected: usize,
    changed: usize,
    returned: usize,
}
impl ConflictCheck {
    fn new(counts: Counts, count: usize, row_count: Option<usize>, wide: bool) -> Checked<Self> {
        invalid((wide || count <= MAX_ROWS) && count == counts.matched)?;
        Ok(Self {
            counts,
            row_count,
            seen: Seen::new(count)?,
            affected: 0,
            changed: 0,
            returned: 0,
        })
    }
    fn tuple(&mut self, index: usize, value: &ConflictTupleOutcome) -> Checked<()> {
        invalid(value.input_index == index)?;
        match value.status {
            ConflictStatus::Skipped => invalid(!value.changed && value.returned_row.is_none())?,
            ConflictStatus::Success => {
                self.affected += 1;
                self.changed += usize::from(value.changed);
                if let Some(row) = value.returned_row {
                    invalid(row < self.counts.affected && self.seen.mark(row))?;
                    self.returned += 1;
                }
            }
        }
        Ok(())
    }
    fn finish(self) -> Checked<()> {
        invalid(self.affected == self.counts.affected && self.changed == self.counts.changed)?;
        // RETURN covers every non-skipped input or is absent. Summary indices
        // refer to that operation's rows, not to transaction emitted values.
        invalid(self.returned == 0 || self.returned == self.affected)?;
        if let Some(rows) = self.row_count {
            invalid(rows == self.returned)?;
        }
        Ok(())
    }
}

trait Sink {
    fn write(&mut self, bytes: &[u8]) -> Checked<()>;
}
struct Counter {
    len: usize,
}
impl Sink for Counter {
    fn write(&mut self, bytes: &[u8]) -> Checked<()> {
        self.len = self.len.checked_add(bytes.len()).ok_or(Fault::Bound)?;
        if self.len > MAX_ONCE_RESULT_BYTES {
            return Err(Fault::Bound);
        }
        Ok(())
    }
}
struct Buffer {
    bytes: Vec<u8>,
}
impl Sink for Buffer {
    fn write(&mut self, bytes: &[u8]) -> Checked<()> {
        let len = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or(Fault::Bound)?;
        // The counting pass reserved the entire buffer; this pass never grows it.
        if len > MAX_ONCE_RESULT_BYTES || len > self.bytes.capacity() {
            return Err(Fault::Bound);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}
struct Writer<S> {
    sink: S,
    remaining_values: usize,
    wide: bool,
    widest: usize,
}
impl<S: Sink> Writer<S> {
    fn new(sink: S, wide: bool) -> Self {
        Self {
            sink,
            remaining_values: MAX_RESULT_BYTES,
            wide,
            widest: 0,
        }
    }
    fn u8(&mut self, n: u8) -> Checked<()> {
        self.sink.write(&[n])
    }
    /// A count or index: u16 in version 1, u32 in version 2.
    fn count(&mut self, n: usize) -> Checked<()> {
        self.widest = self.widest.max(n);
        if self.wide {
            self.sink
                .write(&u32::try_from(n).map_err(|_| Fault::Invalid)?.to_be_bytes())
        } else {
            self.sink
                .write(&u16::try_from(n).map_err(|_| Fault::Invalid)?.to_be_bytes())
        }
    }
    /// The version's NO_ROW sentinel; it is not a count, so `widest` skips it.
    fn no_row(&mut self) -> Checked<()> {
        if self.wide {
            self.sink.write(&u32::MAX.to_be_bytes())
        } else {
            self.sink.write(&u16::MAX.to_be_bytes())
        }
    }
    fn u32(&mut self, n: usize) -> Checked<()> {
        self.sink
            .write(&u32::try_from(n).map_err(|_| Fault::Bound)?.to_be_bytes())
    }
    fn u64(&mut self, n: u64) -> Checked<()> {
        self.sink.write(&n.to_be_bytes())
    }
    fn boolean(&mut self, b: bool) -> Checked<()> {
        self.u8(u8::from(b))
    }
    fn name(&mut self, name: &str) -> Checked<()> {
        invalid(name_valid(name))?;
        self.u8(name.len() as u8)?;
        self.sink.write(name.as_bytes())
    }
    fn counts(&mut self, counts: Counts, complete: bool) -> Checked<()> {
        counts.check(self.wide)?;
        self.count(counts.matched)?;
        self.count(counts.affected)?;
        self.count(counts.changed)?;
        self.boolean(complete)
    }
    // Keep the original one-byte width through 64. The formerly invalid
    // 0xff byte introduces a two-byte, big-endian width for wider results.
    fn column_count(&mut self, len: usize) -> Checked<()> {
        invalid(len <= MAX_TARGET_COLUMNS)?;
        if len <= MAX_COLUMNS {
            self.u8(len as u8)
        } else {
            self.u8(u8::MAX)?;
            self.sink.write(&(len as u16).to_be_bytes())
        }
    }
    fn columns(&mut self, columns: &[ColumnDef], allow_empty: bool) -> Checked<()> {
        check_names(columns.len(), columns.iter().map(|c| c.name.as_str()))?;
        invalid(allow_empty || !columns.is_empty())?;
        self.column_count(columns.len())?;
        for column in columns {
            self.u64(column.id)?;
            self.name(&column.name)?;
            self.u8(Kind::from_type(&column.data_type).tag())?;
            self.boolean(column.not_null)?;
        }
        Ok(())
    }
    fn description(&mut self, description: &OnceDescription) -> Checked<()> {
        self.u64(description.catalog_epoch)?;
        match &description.output {
            OnceSchema::Operation(columns) => {
                self.u8(0)?;
                self.columns(columns, true)
            }
            OnceSchema::Transaction(emitted) => {
                invalid(emitted.len() <= MAX_EMIT_VALUES)?;
                self.u8(1)?;
                self.u8(emitted.len() as u8)?;
                for description in emitted {
                    match description {
                        EmitDescription::Scalar {
                            data_type,
                            nullable,
                        } => {
                            invalid(data_type.is_some() || *nullable)?;
                            self.u8(0)?;
                            self.boolean(data_type.is_some())?;
                            if let Some(data_type) = data_type {
                                self.u8(Kind::from_type(data_type).tag())?;
                            }
                            self.boolean(*nullable)?;
                        }
                        EmitDescription::Row { columns } => {
                            self.u8(1)?;
                            self.columns(columns, false)?;
                        }
                        EmitDescription::Rows { columns } => {
                            self.u8(2)?;
                            self.columns(columns, false)?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
    fn value(&mut self, value: &Value, column: Option<Column<'_>>) -> Checked<()> {
        let (shape, bytes) = match value {
            Value::Null => (Shape::Null, 9),
            Value::Bool(_) => (Shape::Boolean, 9),
            Value::Int(n) => (Shape::Int(*n), 9),
            Value::Text(text) => (Shape::Text, text.len()),
            Value::Bytes(bytes) => (Shape::Bytes, bytes.len()),
            Value::Numeric(n) => (Shape::Numeric, n.to_string().len()),
            Value::Float4(_) => (Shape::Real, 4),
            Value::Float8(_) => (Shape::DoublePrecision, 8),
            Value::Date(_) => (Shape::Date, 4),
            Value::Time(_) => (Shape::Time, 8),
            Value::TimeTz(_) => (Shape::TimeTz, 12),
            Value::Timestamp(_) => (Shape::Timestamp, 8),
            Value::TimestampTz(_) => (Shape::TimestampTz, 8),
            Value::Interval(_) => (Shape::Interval, 16),
        };
        check_type(shape, column)?;
        charge(&mut self.remaining_values, bytes)?;
        match value {
            Value::Date(n) => {
                self.u8(8)?;
                self.sink.write(&n.to_binary())
            }
            Value::Time(n) => {
                self.u8(9)?;
                self.sink.write(&n.to_binary())
            }
            Value::TimeTz(n) => {
                self.u8(10)?;
                self.sink.write(&n.to_binary())
            }
            Value::Timestamp(n) => {
                self.u8(11)?;
                self.sink.write(&n.to_binary())
            }
            Value::TimestampTz(n) => {
                self.u8(12)?;
                self.sink.write(&n.to_binary())
            }
            Value::Interval(n) => {
                self.u8(13)?;
                self.sink.write(&n.to_binary())
            }
            Value::Numeric(n) => {
                let t = n.to_string();
                self.u8(5)?;
                self.u32(t.len())?;
                self.sink.write(t.as_bytes())
            }
            Value::Float4(n) => {
                self.u8(6)?;
                self.sink.write(&n.to_bits().to_be_bytes())
            }
            Value::Float8(n) => {
                self.u8(7)?;
                self.sink.write(&n.to_bits().to_be_bytes())
            }
            Value::Null => self.u8(0),
            Value::Bool(b) => {
                self.u8(1)?;
                self.boolean(*b)
            }
            Value::Int(n) => {
                self.u8(2)?;
                self.sink.write(&n.to_be_bytes())
            }
            Value::Text(text) => {
                self.u8(3)?;
                self.u32(text.len())?;
                self.sink.write(text.as_bytes())
            }
            Value::Bytes(bytes) => {
                self.u8(4)?;
                self.u32(bytes.len())?;
                self.sink.write(bytes)
            }
        }
    }
    fn row(&mut self, row: &SqlRow, arity: usize, columns: Option<&[ColumnDef]>) -> Checked<()> {
        invalid(arity != 0 && arity <= MAX_TARGET_COLUMNS && row.values.len() == arity)?;
        self.u64(row.version)?;
        for (index, value) in row.values.iter().enumerate() {
            let column = columns.map(|columns| {
                let c = &columns[index];
                Column {
                    id: c.id,
                    name: &c.name,
                    kind: Kind::from_type(&c.data_type),
                    not_null: c.not_null,
                }
            });
            self.value(value, column)?;
        }
        Ok(())
    }
    fn conflicts(
        &mut self,
        results: &Option<Vec<ConflictTupleOutcome>>,
        counts: Counts,
        rows: Option<usize>,
    ) -> Checked<()> {
        let Some(results) = results else {
            return self.u8(0);
        };
        let mut check = ConflictCheck::new(counts, results.len(), rows, self.wide)?;
        self.u8(1)?;
        self.count(results.len())?;
        for (index, result) in results.iter().enumerate() {
            check.tuple(index, result)?;
            self.count(result.input_index)?;
            self.u8(match result.status {
                ConflictStatus::Success => 0,
                ConflictStatus::Skipped => 1,
            })?;
            self.boolean(result.changed)?;
            match result.returned_row {
                Some(row) => self.count(row)?,
                None => self.no_row()?,
            }
        }
        check.finish()
    }
    fn operation(&mut self, out: &Outcome, columns: &[ColumnDef]) -> Checked<()> {
        let counts = Counts {
            matched: out.matched,
            affected: out.affected,
            changed: out.changed,
        };
        invalid((self.wide || out.rows.len() <= MAX_ROWS) && out.rows.len() <= out.matched)?;
        check_names(out.columns.len(), out.columns.iter().map(String::as_str))?;
        invalid(out.columns.len() == columns.len())?;
        invalid(
            out.columns
                .iter()
                .zip(columns)
                .all(|(name, column)| *name == column.name),
        )?;
        self.counts(counts, out.complete)?;
        self.column_count(out.columns.len())?;
        for name in &out.columns {
            self.name(name)?;
        }
        self.count(out.rows.len())?;
        for row in &out.rows {
            self.row(row, out.columns.len(), Some(columns))?;
        }
        self.conflicts(&out.conflict_results, counts, Some(out.rows.len()))
    }
    fn transaction(
        &mut self,
        out: &TransactionOutcome,
        descriptions: &[EmitDescription],
    ) -> Checked<()> {
        invalid(out.emitted.len() == descriptions.len())?;
        check_transaction_indices(
            out.summaries.len(),
            out.summaries.last().map(|s| s.item_index),
            out.emitted.len(),
        )?;
        self.u8(out.summaries.len() as u8)?;
        let mut last = None;
        for summary in &out.summaries {
            invalid(
                summary.item_index < MAX_TRANSACTION_ITEMS
                    && last.is_none_or(|last| summary.item_index > last),
            )?;
            last = Some(summary.item_index);
            self.u8(summary.item_index as u8)?;
            let counts = Counts {
                matched: summary.matched,
                affected: summary.affected,
                changed: summary.changed,
            };
            self.counts(counts, summary.complete)?;
            self.conflicts(&summary.conflict_results, counts, None)?;
        }
        self.u8(out.emitted.len() as u8)?;
        for (emitted, description) in out.emitted.iter().zip(descriptions) {
            match (emitted, description) {
                (
                    EmittedValue::Scalar(value),
                    EmitDescription::Scalar {
                        data_type,
                        nullable,
                    },
                ) => {
                    self.u8(0)?;
                    if data_type.is_none() {
                        invalid(*nullable && *value == Value::Null)?;
                    }
                    self.value(
                        value,
                        data_type
                            .as_ref()
                            .map(|ty| scalar_column(Kind::from_type(ty), *nullable)),
                    )?;
                }
                (
                    EmittedValue::Row { columns, row },
                    EmitDescription::Row { columns: expected },
                ) => {
                    invalid(columns == expected)?;
                    self.u8(1)?;
                    self.columns(columns, false)?;
                    self.row(row, columns.len(), Some(columns))?;
                }
                (
                    EmittedValue::Rows {
                        columns,
                        rows,
                        complete,
                    },
                    EmitDescription::Rows { columns: expected },
                ) => {
                    invalid(columns == expected)?;
                    invalid(self.wide || rows.len() <= MAX_ROWS)?;
                    self.u8(2)?;
                    self.columns(columns, false)?;
                    self.count(rows.len())?;
                    self.boolean(*complete)?;
                    for row in rows {
                        self.row(row, columns.len(), Some(columns))?;
                    }
                }
                _ => return Err(Fault::Invalid),
            }
        }
        Ok(())
    }
    fn result(&mut self, description: &OnceDescription, result: &BusinessResult) -> Checked<()> {
        self.sink.write(MAGIC)?;
        self.u8(if self.wide { WIDE_VERSION } else { VERSION })?;
        self.description(description)?;
        match result {
            Ok(OnceOutcome::Operation(out)) => {
                let OnceSchema::Operation(columns) = &description.output else {
                    return Err(Fault::Invalid);
                };
                self.u8(0)?;
                self.operation(out, columns)
            }
            Ok(OnceOutcome::Transaction(out)) => {
                let OnceSchema::Transaction(descriptions) = &description.output else {
                    return Err(Fault::Invalid);
                };
                self.u8(1)?;
                self.transaction(out, descriptions)
            }
            Err(error) => {
                self.u8(2)?;
                self.u8(match error {
                    Error::NotFound => 0,
                    Error::AmbiguousMatch => 1,
                    Error::ConditionFailed => 2,
                    Error::TooManyMatches => 3,
                    Error::RequirementFailed => 4,
                    _ => return Err(Fault::Invalid),
                })
            }
        }
    }
}

#[derive(PartialEq, Eq)]
struct Columns<'a>(Vec<Column<'a>>);
impl Columns<'_> {
    fn owned(&self, build: bool) -> Checked<Option<Vec<ColumnDef>>> {
        let mut out = vector(build, self.0.len())?;
        if let Some(out) = &mut out {
            for column in &self.0 {
                out.push(ColumnDef {
                    id: column.id,
                    name: owned_text(column.name)?,
                    data_type: column.kind.data_type(),
                    not_null: column.not_null,
                });
            }
        }
        Ok(out)
    }
}
fn vector<T>(build: bool, len: usize) -> Checked<Option<Vec<T>>> {
    if !build {
        return Ok(None);
    }
    let mut out = Vec::new();
    out.try_reserve_exact(len).map_err(|_| Fault::Bound)?;
    Ok(Some(out))
}
fn owned_text(text: &str) -> Checked<String> {
    let mut out = String::new();
    out.try_reserve_exact(text.len())
        .map_err(|_| Fault::Bound)?;
    out.push_str(text);
    Ok(out)
}
fn owned_bytes(bytes: &[u8]) -> Checked<Vec<u8>> {
    let mut out = Vec::new();
    out.try_reserve_exact(bytes.len())
        .map_err(|_| Fault::Bound)?;
    out.extend_from_slice(bytes);
    Ok(out)
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
    remaining_values: usize,
    wide: bool,
    widest: usize,
    build: bool,
    typed: bool,
    temporal: bool,
}
// Keep only slices into the validated input between schema and value parsing.
// A transaction never needs a 64-by-64 descriptor array on the stack.
struct SchemaView<'a> {
    tag: u8,
    bytes: &'a [u8],
}
// Boxing the fixed 64-column member would allocate during the validation pass.
// Only one emitted descriptor is retained at a time.
#[allow(clippy::large_enum_variant)]
enum EmitShape<'a> {
    Scalar { kind: Option<Kind>, nullable: bool },
    Row(Columns<'a>),
    Rows(Columns<'a>),
}
impl EmitShape<'_> {
    fn owned(&self, build: bool) -> Checked<Option<EmitDescription>> {
        if !build {
            return Ok(None);
        }
        Ok(Some(match self {
            Self::Scalar { kind, nullable } => EmitDescription::Scalar {
                data_type: kind.map(Kind::data_type),
                nullable: *nullable,
            },
            Self::Row(columns) => EmitDescription::Row {
                columns: columns.owned(true)?.ok_or(Fault::Invalid)?,
            },
            Self::Rows(columns) => EmitDescription::Rows {
                columns: columns.owned(true)?.ok_or(Fault::Invalid)?,
            },
        }))
    }
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], build: bool) -> Self {
        Self {
            bytes,
            offset: 0,
            remaining_values: MAX_RESULT_BYTES,
            wide: false,
            widest: 0,
            typed: false,
            temporal: false,
            build,
        }
    }
    fn kind(&mut self) -> Checked<Kind> {
        let tag = self.u8()?;
        self.typed |= (6..=8).contains(&tag);
        self.temporal |= (9..=14).contains(&tag);
        Kind::parse(tag)
    }
    fn take(&mut self, len: usize) -> Checked<&'a [u8]> {
        let end = self.offset.checked_add(len).ok_or(Fault::Invalid)?;
        let bytes = self.bytes.get(self.offset..end).ok_or(Fault::Invalid)?;
        self.offset = end;
        Ok(bytes)
    }
    fn u8(&mut self) -> Checked<u8> {
        Ok(self.take(1)?[0])
    }
    /// A count or index (u16 in version 1, u32 in version 2), or None for
    /// the version's NO_ROW sentinel when `sentinel` allows one.
    fn count_or_none(&mut self, sentinel: bool) -> Checked<Option<usize>> {
        let (n, no_row) = if self.wide {
            let n = u32::from_be_bytes(self.take(4)?.try_into().map_err(|_| Fault::Invalid)?);
            (usize::try_from(n).map_err(|_| Fault::Bound)?, n == u32::MAX)
        } else {
            let n = u16::from_be_bytes(self.take(2)?.try_into().map_err(|_| Fault::Invalid)?);
            (usize::from(n), n == u16::MAX)
        };
        if sentinel && no_row {
            return Ok(None);
        }
        self.widest = self.widest.max(n);
        Ok(Some(n))
    }
    fn count(&mut self) -> Checked<usize> {
        self.count_or_none(false)?.ok_or(Fault::Invalid)
    }
    /// Bound a row-vector reservation by the bytes actually present: every
    /// row is a u64 version plus at least one value byte (arity >= 1).
    fn rows_present(&self, rows: usize) -> Checked<()> {
        invalid(rows.saturating_mul(9) <= self.bytes.len() - self.offset)
    }
    fn u32(&mut self) -> Checked<usize> {
        usize::try_from(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| Fault::Invalid)?,
        ))
        .map_err(|_| Fault::Bound)
    }
    fn u64(&mut self) -> Checked<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| Fault::Invalid)?,
        ))
    }
    fn boolean(&mut self) -> Checked<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Fault::Invalid),
        }
    }
    fn name(&mut self) -> Checked<&'a str> {
        let len = usize::from(self.u8()?);
        let name = std::str::from_utf8(self.take(len)?).map_err(|_| Fault::Invalid)?;
        invalid(name_valid(name))?;
        Ok(name)
    }
    fn counts(&mut self) -> Checked<(Counts, bool)> {
        let counts = Counts {
            matched: self.count()?,
            affected: self.count()?,
            changed: self.count()?,
        };
        counts.check(self.wide)?;
        Ok((counts, self.boolean()?))
    }
    fn column_count(&mut self) -> Checked<usize> {
        let len = self.u8()?;
        if len == u8::MAX {
            let len = usize::from(u16::from_be_bytes(
                self.take(2)?.try_into().map_err(|_| Fault::Invalid)?,
            ));
            invalid((MAX_COLUMNS + 1..=MAX_TARGET_COLUMNS).contains(&len))?;
            Ok(len)
        } else {
            let len = usize::from(len);
            invalid(len <= MAX_COLUMNS)?;
            Ok(len)
        }
    }
    fn columns(&mut self, allow_empty: bool) -> Checked<Columns<'a>> {
        let len = self.column_count()?;
        invalid((allow_empty || len != 0) && len <= (self.bytes.len() - self.offset) / 12)?;
        let mut values: Vec<Column<'a>> = Vec::new();
        values.try_reserve_exact(len).map_err(|_| Fault::Bound)?;
        for _ in 0..len {
            let id = self.u64()?;
            let name = self.name()?;
            invalid(!values.iter().any(|column| column.name == name))?;
            values.push(Column {
                id,
                name,
                kind: self.kind()?,
                not_null: self.boolean()?,
            });
        }
        Ok(Columns(values))
    }
    fn emit_shape(&mut self) -> Checked<EmitShape<'a>> {
        Ok(match self.u8()? {
            0 => {
                let kind = if self.boolean()? {
                    Some(self.kind()?)
                } else {
                    None
                };
                let nullable = self.boolean()?;
                invalid(kind.is_some() || nullable)?;
                EmitShape::Scalar { kind, nullable }
            }
            1 => EmitShape::Row(self.columns(false)?),
            2 => EmitShape::Rows(self.columns(false)?),
            _ => return Err(Fault::Invalid),
        })
    }
    fn description(&mut self) -> Checked<(SchemaView<'a>, Option<OnceDescription>)> {
        let catalog_epoch = self.u64()?;
        let tag = self.u8()?;
        let start = self.offset;
        let output = match tag {
            0 => self
                .columns(true)?
                .owned(self.build)?
                .map(OnceSchema::Operation),
            1 => {
                let len = usize::from(self.u8()?);
                invalid(len <= MAX_EMIT_VALUES)?;
                let mut descriptions = vector(self.build, len)?;
                for _ in 0..len {
                    let shape = self.emit_shape()?;
                    if let Some(descriptions) = &mut descriptions {
                        descriptions.push(shape.owned(true)?.ok_or(Fault::Invalid)?);
                    }
                }
                descriptions.map(OnceSchema::Transaction)
            }
            _ => return Err(Fault::Invalid),
        };
        Ok((
            SchemaView {
                tag,
                bytes: &self.bytes[start..self.offset],
            },
            output.map(|output| OnceDescription {
                catalog_epoch,
                output,
            }),
        ))
    }
    fn value(&mut self, column: Option<Column<'_>>) -> Checked<Option<Value>> {
        let tag = self.u8()?;
        self.typed |= (5..=7).contains(&tag);
        self.temporal |= (8..=13).contains(&tag);
        match tag {
            0 => {
                check_type(Shape::Null, column)?;
                charge(&mut self.remaining_values, 9)?;
                Ok(self.build.then_some(Value::Null))
            }
            1 => {
                let b = self.boolean()?;
                check_type(Shape::Boolean, column)?;
                charge(&mut self.remaining_values, 9)?;
                Ok(self.build.then_some(Value::Bool(b)))
            }
            2 => {
                let n = i64::from_be_bytes(self.take(8)?.try_into().map_err(|_| Fault::Invalid)?);
                check_type(Shape::Int(n), column)?;
                charge(&mut self.remaining_values, 9)?;
                Ok(self.build.then_some(Value::Int(n)))
            }
            3 => {
                let len = self.u32()?;
                charge(&mut self.remaining_values, len)?;
                let text = std::str::from_utf8(self.take(len)?).map_err(|_| Fault::Invalid)?;
                check_type(Shape::Text, column)?;
                Ok(if self.build {
                    Some(Value::Text(owned_text(text)?))
                } else {
                    None
                })
            }
            4 => {
                let len = self.u32()?;
                charge(&mut self.remaining_values, len)?;
                let bytes = self.take(len)?;
                check_type(Shape::Bytes, column)?;
                Ok(if self.build {
                    Some(Value::Bytes(owned_bytes(bytes)?))
                } else {
                    None
                })
            }
            5 => {
                let len = self.u32()?;
                invalid(len <= giggles_pg_types::MAX_TEXT)?;
                charge(&mut self.remaining_values, len)?;
                let t = std::str::from_utf8(self.take(len)?).map_err(|_| Fault::Invalid)?;
                invalid(crate::pg_serde::canonical_numeric_text(t))?;
                check_type(Shape::Numeric, column)?;
                Ok(if self.build {
                    Some(Value::Numeric(
                        crate::relational::Numeric::parse(t).map_err(|_| Fault::Invalid)?,
                    ))
                } else {
                    None
                })
            }
            6 => {
                charge(&mut self.remaining_values, 4)?;
                let b = u32::from_be_bytes(self.take(4)?.try_into().map_err(|_| Fault::Invalid)?);
                check_type(Shape::Real, column)?;
                Ok(self
                    .build
                    .then_some(Value::Float4(crate::relational::Float4::from_bits(b))))
            }
            7 => {
                charge(&mut self.remaining_values, 8)?;
                let b = u64::from_be_bytes(self.take(8)?.try_into().map_err(|_| Fault::Invalid)?);
                check_type(Shape::DoublePrecision, column)?;
                Ok(self
                    .build
                    .then_some(Value::Float8(crate::relational::Float8::from_bits(b))))
            }
            8 => {
                charge(&mut self.remaining_values, 4)?;
                check_type(Shape::Date, column)?;
                let value = crate::relational::Date::from_binary(self.take(4)?)
                    .map_err(|_| Fault::Invalid)?;
                Ok(self.build.then_some(Value::Date(value)))
            }
            9 => {
                charge(&mut self.remaining_values, 8)?;
                check_type(Shape::Time, column)?;
                let value = crate::relational::Time::from_binary(self.take(8)?)
                    .map_err(|_| Fault::Invalid)?;
                Ok(self.build.then_some(Value::Time(value)))
            }
            10 => {
                charge(&mut self.remaining_values, 12)?;
                check_type(Shape::TimeTz, column)?;
                let value = crate::relational::TimeTz::from_binary(self.take(12)?)
                    .map_err(|_| Fault::Invalid)?;
                Ok(self.build.then_some(Value::TimeTz(value)))
            }
            11 => {
                charge(&mut self.remaining_values, 8)?;
                check_type(Shape::Timestamp, column)?;
                let value = crate::relational::Timestamp::from_binary(self.take(8)?)
                    .map_err(|_| Fault::Invalid)?;
                Ok(self.build.then_some(Value::Timestamp(value)))
            }
            12 => {
                charge(&mut self.remaining_values, 8)?;
                check_type(Shape::TimestampTz, column)?;
                let value = crate::relational::TimestampTz::from_binary(self.take(8)?)
                    .map_err(|_| Fault::Invalid)?;
                Ok(self.build.then_some(Value::TimestampTz(value)))
            }
            13 => {
                charge(&mut self.remaining_values, 16)?;
                check_type(Shape::Interval, column)?;
                let value = crate::relational::Interval::from_binary(self.take(16)?)
                    .map_err(|_| Fault::Invalid)?;
                Ok(self.build.then_some(Value::Interval(value)))
            }
            _ => Err(Fault::Invalid),
        }
    }
    fn row(&mut self, arity: usize, columns: Option<&Columns<'a>>) -> Checked<Option<SqlRow>> {
        invalid(arity != 0 && arity <= MAX_TARGET_COLUMNS)?;
        let version = self.u64()?;
        let mut values = vector(self.build, arity)?;
        for index in 0..arity {
            let value = self.value(columns.and_then(|c| c.0.get(index)).copied())?;
            if let Some(values) = &mut values {
                values.push(value.ok_or(Fault::Invalid)?);
            }
        }
        Ok(values.map(|values| SqlRow { values, version }))
    }
    fn conflicts(
        &mut self,
        counts: Counts,
        rows: Option<usize>,
    ) -> Checked<Option<Vec<ConflictTupleOutcome>>> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let len = self.count()?;
                // Bound the version 2 bitset by the tuples actually present.
                let tuple = if self.wide { 10 } else { 6 };
                invalid(len.saturating_mul(tuple) <= self.bytes.len() - self.offset)?;
                let mut check = ConflictCheck::new(counts, len, rows, self.wide)?;
                let mut out = vector(self.build, len)?;
                for index in 0..len {
                    let input_index = self.count()?;
                    let status = match self.u8()? {
                        0 => ConflictStatus::Success,
                        1 => ConflictStatus::Skipped,
                        _ => return Err(Fault::Invalid),
                    };
                    let changed = self.boolean()?;
                    let returned_row = self.count_or_none(true)?;
                    let result = ConflictTupleOutcome {
                        input_index,
                        status,
                        changed,
                        returned_row,
                    };
                    check.tuple(index, &result)?;
                    if let Some(out) = &mut out {
                        out.push(result);
                    }
                }
                check.finish()?;
                Ok(out)
            }
            _ => Err(Fault::Invalid),
        }
    }
    fn operation(&mut self, schema: &SchemaView<'a>) -> Checked<Option<OnceOutcome>> {
        invalid(schema.tag == 0)?;
        let expected = Reader::new(schema.bytes, false).columns(true)?;
        let (counts, complete) = self.counts()?;
        let len = self.column_count()?;
        invalid(len == expected.0.len())?;
        let mut columns = vector(self.build, len)?;
        for expected in &expected.0 {
            let name = self.name()?;
            invalid(name == expected.name)?;
            if let Some(columns) = &mut columns {
                columns.push(owned_text(name)?);
            }
        }
        let row_count = self.count()?;
        invalid((self.wide || row_count <= MAX_ROWS) && row_count <= counts.matched)?;
        self.rows_present(row_count)?;
        let mut rows = vector(self.build, row_count)?;
        for _ in 0..row_count {
            let row = self.row(len, Some(&expected))?;
            if let Some(rows) = &mut rows {
                rows.push(row.ok_or(Fault::Invalid)?);
            }
        }
        let conflict_results = self.conflicts(counts, Some(row_count))?;
        Ok(if self.build {
            Some(OnceOutcome::Operation(Outcome {
                columns: columns.ok_or(Fault::Invalid)?,
                rows: rows.ok_or(Fault::Invalid)?,
                matched: counts.matched,
                affected: counts.affected,
                changed: counts.changed,
                complete,
                conflict_results,
            }))
        } else {
            None
        })
    }
    fn transaction(&mut self, schema: &SchemaView<'a>) -> Checked<Option<OnceOutcome>> {
        invalid(schema.tag == 1)?;
        let mut expected = Reader::new(schema.bytes, false);
        let expected_count = usize::from(expected.u8()?);
        let len = usize::from(self.u8()?);
        invalid(len <= MAX_TRANSACTION_ITEMS)?;
        let mut summaries = vector(self.build, len)?;
        let mut last = None;
        for _ in 0..len {
            let item_index = usize::from(self.u8()?);
            invalid(
                item_index < MAX_TRANSACTION_ITEMS && last.is_none_or(|last| item_index > last),
            )?;
            last = Some(item_index);
            let (counts, complete) = self.counts()?;
            let conflict_results = self.conflicts(counts, None)?;
            if let Some(summaries) = &mut summaries {
                summaries.push(StepSummary {
                    item_index,
                    matched: counts.matched,
                    affected: counts.affected,
                    changed: counts.changed,
                    complete,
                    conflict_results,
                });
            }
        }
        let emits = usize::from(self.u8()?);
        check_transaction_indices(len, last, emits)?;
        invalid(emits == expected_count)?;
        let mut emitted = vector(self.build, emits)?;
        for _ in 0..emits {
            let shape = expected.emit_shape()?;
            let value = match (self.u8()?, shape) {
                (0, EmitShape::Scalar { kind, nullable }) => {
                    if kind.is_none() {
                        invalid(nullable && self.bytes.get(self.offset) == Some(&0))?;
                    }
                    self.value(kind.map(|kind| scalar_column(kind, nullable)))?
                        .map(EmittedValue::Scalar)
                }
                (1, EmitShape::Row(expected_columns)) => {
                    let columns = self.columns(false)?;
                    invalid(columns == expected_columns)?;
                    let row = self.row(columns.0.len(), Some(&columns))?;
                    if self.build {
                        Some(EmittedValue::Row {
                            columns: columns.owned(true)?.ok_or(Fault::Invalid)?,
                            row: row.ok_or(Fault::Invalid)?,
                        })
                    } else {
                        None
                    }
                }
                (2, EmitShape::Rows(expected_columns)) => {
                    let columns = self.columns(false)?;
                    invalid(columns == expected_columns)?;
                    let len = self.count()?;
                    invalid(self.wide || len <= MAX_ROWS)?;
                    let complete = self.boolean()?;
                    self.rows_present(len)?;
                    let mut rows = vector(self.build, len)?;
                    for _ in 0..len {
                        let row = self.row(columns.0.len(), Some(&columns))?;
                        if let Some(rows) = &mut rows {
                            rows.push(row.ok_or(Fault::Invalid)?);
                        }
                    }
                    if self.build {
                        Some(EmittedValue::Rows {
                            columns: columns.owned(true)?.ok_or(Fault::Invalid)?,
                            rows: rows.ok_or(Fault::Invalid)?,
                            complete,
                        })
                    } else {
                        None
                    }
                }
                _ => return Err(Fault::Invalid),
            };
            if let Some(emitted) = &mut emitted {
                emitted.push(value.ok_or(Fault::Invalid)?);
            }
        }
        Ok(if self.build {
            Some(OnceOutcome::Transaction(TransactionOutcome {
                summaries: summaries.ok_or(Fault::Invalid)?,
                emitted: emitted.ok_or(Fault::Invalid)?,
            }))
        } else {
            None
        })
    }
    fn result(&mut self) -> Checked<Option<(OnceDescription, BusinessResult)>> {
        if self.bytes.len() > MAX_ONCE_RESULT_BYTES {
            return Err(Fault::Bound);
        }
        invalid(self.take(4)? == MAGIC)?;
        self.wide = match self.u8()? {
            VERSION => false,
            WIDE_VERSION => true,
            _ => return Err(Fault::Invalid),
        };
        let (schema, description) = self.description()?;
        let result = match self.u8()? {
            0 => self.operation(&schema)?.map(Ok),
            1 => self.transaction(&schema)?.map(Ok),
            2 => {
                let error = match self.u8()? {
                    0 => Error::NotFound,
                    1 => Error::AmbiguousMatch,
                    2 => Error::ConditionFailed,
                    3 => Error::TooManyMatches,
                    4 => Error::RequirementFailed,
                    _ => return Err(Fault::Invalid),
                };
                self.build.then_some(Err(error))
            }
            _ => return Err(Fault::Invalid),
        };
        invalid(self.offset == self.bytes.len())?;
        // Canonical: version 2 only when some count does not fit version 1.
        invalid(!self.wide || self.widest > MAX_ROWS)?;
        Ok(if self.build {
            Some((
                description.ok_or(Fault::Invalid)?,
                result.ok_or(Fault::Invalid)?,
            ))
        } else {
            None
        })
    }
}

pub(crate) fn contains_temporal(bytes: &[u8]) -> Result<bool, CoreError> {
    let mut reader = Reader::new(bytes, false);
    reader.result().map_err(corrupt)?;
    Ok(reader.temporal)
}
pub(crate) fn contains_typed(bytes: &[u8]) -> Result<bool, CoreError> {
    let mut reader = Reader::new(bytes, false);
    reader.result().map_err(corrupt)?;
    Ok(reader.typed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transaction(emitted: Vec<EmittedValue>) -> BusinessResult {
        Ok(OnceOutcome::Transaction(TransactionOutcome {
            summaries: vec![],
            emitted,
        }))
    }
    fn scalar(value: Value) -> BusinessResult {
        transaction(vec![EmittedValue::Scalar(value)])
    }
    fn operation(out: Outcome) -> BusinessResult {
        Ok(OnceOutcome::Operation(out))
    }
    fn column(name: &str, ty: DataType, not_null: bool) -> ColumnDef {
        ColumnDef {
            id: 0,
            name: name.into(),
            data_type: ty,
            not_null,
        }
    }
    // Convenience descriptions for legacy behavioral fixtures only. Dedicated
    // schema tests below supply exact declared types independently of values.
    fn fixture_type(value: &Value) -> Option<DataType> {
        match value {
            Value::Null => None,
            Value::Bool(_) => Some(DataType::Boolean),
            Value::Int(_) => Some(DataType::BigInt),
            Value::Text(_) => Some(DataType::Text),
            Value::Bytes(_) => Some(DataType::Bytes),
            Value::Numeric(_) => Some(DataType::Numeric),
            Value::Float4(_) => Some(DataType::Real),
            Value::Float8(_) => Some(DataType::DoublePrecision),
            Value::Date(_) => Some(DataType::Date),
            Value::Time(_) => Some(DataType::Time),
            Value::TimeTz(_) => Some(DataType::TimeTz),
            Value::Timestamp(_) => Some(DataType::Timestamp),
            Value::TimestampTz(_) => Some(DataType::TimestampTz),
            Value::Interval(_) => Some(DataType::Interval),
        }
    }
    fn fixture_description(result: &BusinessResult) -> OnceDescription {
        let output = match result {
            Ok(OnceOutcome::Operation(out)) => OnceSchema::Operation(
                out.columns
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        let ty = out
                            .rows
                            .iter()
                            .filter_map(|row| row.values.get(index))
                            .find_map(fixture_type)
                            .unwrap_or(DataType::Text);
                        column(name, ty, false)
                    })
                    .collect(),
            ),
            Ok(OnceOutcome::Transaction(out)) => OnceSchema::Transaction(
                out.emitted
                    .iter()
                    .map(|value| match value {
                        EmittedValue::Scalar(value) => EmitDescription::Scalar {
                            data_type: fixture_type(value),
                            nullable: *value == Value::Null,
                        },
                        EmittedValue::Row { columns, .. } => EmitDescription::Row {
                            columns: columns.clone(),
                        },
                        EmittedValue::Rows { columns, .. } => EmitDescription::Rows {
                            columns: columns.clone(),
                        },
                    })
                    .collect(),
            ),
            Err(_) => OnceSchema::Operation(vec![]),
        };
        OnceDescription {
            catalog_epoch: 0,
            output,
        }
    }
    fn encode(result: &BusinessResult) -> OnceResult<Vec<u8>> {
        super::encode(&fixture_description(result), result)
    }
    fn roundtrip(value: &BusinessResult) -> Vec<u8> {
        let bytes = encode(value).unwrap();
        validate(&bytes).unwrap();
        assert_eq!(
            decode(&bytes).unwrap(),
            (fixture_description(value), value.clone())
        );
        bytes
    }
    fn golden_prefix(schema: &[u8]) -> Vec<u8> {
        let mut expected = b"GDOU\x01".to_vec();
        expected.extend([0; 8]); // accepting epoch 0 in these fixtures
        expected.extend(schema);
        expected
    }
    fn bad(bytes: &[u8]) {
        assert!(matches!(validate(bytes), Err(CoreError::Corrupt(_))));
        assert!(matches!(decode(bytes), Err(CoreError::Corrupt(_))));
    }

    #[test]
    fn sql_overflow_is_nonterminal_and_rejected_by_receipt_codec() {
        let overflow = crate::operations::core(CoreError::Sql(crate::relational::SqlError::Overflow));
        assert_eq!(overflow, Error::Overflow);
        for error in [overflow, Error::Core("Sql(Overflow)".into())] {
            assert!(!super::super::terminal(&error));
            assert_eq!(encode(&Err(error)), Err(OnceError::InvalidOutcome));
        }
    }

    #[test]
    fn sql_error_is_nonterminal_and_rejected_by_receipt_codec() {
        let error = crate::operations::core(CoreError::Sql(crate::relational::SqlError::CheckViolation));
        assert_eq!(error, Error::Sql { sqlstate: "23514".into() });
        assert!(!super::super::terminal(&error));
        assert_eq!(encode(&Err(error)), Err(OnceError::InvalidOutcome));
    }

    #[test]
    fn duplicate_is_not_terminal() {
        assert!(!super::super::terminal(&Error::Duplicate));
        assert!(!super::super::terminal(&Error::Core(
            "Sql(Duplicate)".into()
        )));
    }

    #[test]
    fn golden_result_and_terminal_tags_do_not_follow_rust_enum_layout() {
        for (tag, error) in [
            Error::NotFound,
            Error::AmbiguousMatch,
            Error::ConditionFailed,
            Error::TooManyMatches,
            Error::RequirementFailed,
        ]
        .into_iter()
        .enumerate()
        {
            let mut expected = golden_prefix(&[0, 0]);
            expected.extend([2, tag as u8]);
            assert_eq!(roundtrip(&Err(error)), expected);
        }
        for error in [
            Error::Limit,
            Error::Unknown,
            Error::Conflict,
            Error::DuplicateTarget,
            Error::ClaimNotConsumed,
            Error::Type,
            Error::Overflow,
            Error::DivisionByZero,
            Error::Unsupported,
            Error::Bind("column".into()),
            Error::Core("Sql(Duplicate)".into()),
            Error::Duplicate,
        ] {
            assert_eq!(encode(&Err(error)), Err(OnceError::InvalidOutcome));
        }
        let mut expected = golden_prefix(&[0, 0]);
        expected.extend([0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]);
        assert_eq!(roundtrip(&operation(Outcome::default())), expected);
        let out = operation(Outcome {
            columns: vec!["x".into()],
            rows: vec![SqlRow {
                version: 0x0102030405060708,
                values: vec![Value::Int(-1)],
            }],
            matched: 1,
            complete: false,
            ..Outcome::default()
        });
        let mut expected = golden_prefix(&[0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, b'x', 3, 0]);
        expected.extend([
            0, 0, 1, 0, 0, 0, 0, 0, 1, 1, b'x', 0, 1, 1, 2, 3, 4, 5, 6, 7, 8, 2, 255, 255, 255,
            255, 255, 255, 255, 255, 0,
        ]);
        assert_eq!(roundtrip(&out), expected);
        for (value, schema, suffix) in [
            (Value::Null, vec![1, 1, 0, 0, 1], vec![0]),
            (Value::Bool(false), vec![1, 1, 0, 1, 0, 0], vec![1, 0]),
            (Value::Bool(true), vec![1, 1, 0, 1, 0, 0], vec![1, 1]),
            (
                Value::Int(i64::MIN),
                vec![1, 1, 0, 1, 3, 0],
                vec![2, 128, 0, 0, 0, 0, 0, 0, 0],
            ),
            (
                Value::Text("é\0".into()),
                vec![1, 1, 0, 1, 4, 0],
                vec![3, 0, 0, 0, 3, 195, 169, 0],
            ),
            (
                Value::Bytes(vec![0, 255]),
                vec![1, 1, 0, 1, 5, 0],
                vec![4, 0, 0, 0, 2, 0, 255],
            ),
        ] {
            let mut expected = golden_prefix(&schema);
            expected.extend([1, 0, 1, 0]);
            expected.extend(suffix);
            assert_eq!(roundtrip(&scalar(value)), expected);
        }
    }

    #[test]
    fn wide_row_descriptor_roundtrips() {
        let columns = (0..MAX_TARGET_COLUMNS).map(|i| format!("c{i}")).collect();
        let result = operation(Outcome {
            columns,
            rows: vec![SqlRow {
                values: vec![Value::Int(42); MAX_TARGET_COLUMNS],
                version: 1,
            }],
            matched: 1,
            affected: 1,
            changed: 1,
            complete: true,
            conflict_results: None,
        });
        let encoded = roundtrip(&result);
        assert!(encoded.starts_with(&golden_prefix(&[0, 0xff, 0x06, 0x80])));
    }

    #[test]
    fn exact_rows_descriptors_versions_summaries_and_optional_conflicts_roundtrip() {
        let mut columns = vec![
            column("b", DataType::Boolean, true),
            column("s", DataType::SmallInt, true),
            column("i", DataType::Integer, true),
            column("n", DataType::BigInt, true),
            column("t", DataType::Text, false),
            column("x", DataType::Bytes, false),
            column("nullable", DataType::Text, false),
        ];
        for (i, column) in columns.iter_mut().enumerate() {
            column.id = if i == 0 { u64::MAX } else { i as u64 };
        }
        let row = SqlRow {
            version: u64::MAX,
            values: vec![
                Value::Bool(true),
                Value::Int(i64::from(i16::MIN)),
                Value::Int(i64::from(i32::MAX)),
                Value::Int(i64::MIN),
                Value::Text("é\0e\u{301}".into()),
                Value::Bytes(vec![0, 255, 0]),
                Value::Null,
            ],
        };
        let out = TransactionOutcome {
            summaries: vec![
                StepSummary {
                    item_index: 0,
                    matched: 1,
                    affected: 1,
                    changed: 0,
                    complete: true,
                    conflict_results: None,
                },
                StepSummary {
                    item_index: 2,
                    matched: 2,
                    affected: 1,
                    changed: 1,
                    complete: true,
                    conflict_results: Some(vec![
                        ConflictTupleOutcome {
                            input_index: 0,
                            status: ConflictStatus::Skipped,
                            changed: false,
                            returned_row: None,
                        },
                        ConflictTupleOutcome {
                            input_index: 1,
                            status: ConflictStatus::Success,
                            changed: true,
                            returned_row: Some(0),
                        },
                    ]),
                },
            ],
            emitted: vec![
                EmittedValue::Scalar(Value::Text(String::new())),
                EmittedValue::Scalar(Value::Bytes(vec![])),
                EmittedValue::Row {
                    columns: columns.clone(),
                    row: row.clone(),
                },
                EmittedValue::Rows {
                    columns,
                    rows: vec![row],
                    complete: false,
                },
            ],
        };
        let bytes = roundtrip(&Ok(OnceOutcome::Transaction(out.clone())));
        let Ok(OnceOutcome::Transaction(decoded)) = decode(&bytes).unwrap().1 else {
            panic!("transaction expected");
        };
        assert_eq!(
            serde_json::to_vec(&decoded).unwrap(),
            serde_json::to_vec(&out).unwrap()
        );
        let none = roundtrip(&operation(Outcome::default()));
        let some = roundtrip(&operation(Outcome {
            conflict_results: Some(vec![]),
            ..Outcome::default()
        }));
        assert_ne!(none, some);
        assert_eq!(&some[some.len() - 3..], &[1, 0, 0]);
        let skipped = Outcome {
            columns: vec!["id".into()],
            matched: 1,
            conflict_results: Some(vec![ConflictTupleOutcome {
                input_index: 0,
                status: ConflictStatus::Skipped,
                changed: false,
                returned_row: None,
            }]),
            ..Outcome::default()
        };
        roundtrip(&operation(skipped));
        let out = Outcome {
            columns: vec!["id".into()],
            rows: vec![
                SqlRow {
                    version: 19,
                    values: vec![Value::Int(1)],
                },
                SqlRow {
                    version: 9,
                    values: vec![Value::Int(2)],
                },
            ],
            matched: 3,
            affected: 2,
            changed: 1,
            complete: true,
            conflict_results: Some(vec![
                ConflictTupleOutcome {
                    input_index: 0,
                    status: ConflictStatus::Success,
                    changed: false,
                    returned_row: Some(1),
                },
                ConflictTupleOutcome {
                    input_index: 1,
                    status: ConflictStatus::Skipped,
                    changed: false,
                    returned_row: None,
                },
                ConflictTupleOutcome {
                    input_index: 2,
                    status: ConflictStatus::Success,
                    changed: true,
                    returned_row: Some(0),
                },
            ]),
        };
        let bytes = roundtrip(&operation(out.clone()));
        let Ok(OnceOutcome::Operation(decoded)) = decode(&bytes).unwrap().1 else {
            panic!("operation expected");
        };
        assert_eq!(
            serde_json::to_vec(&decoded).unwrap(),
            serde_json::to_vec(&out).unwrap()
        );
    }

    #[test]
    fn golden_column_and_conflict_tags_preserve_current_metadata() {
        for (tag, data_type) in [
            DataType::Boolean,
            DataType::SmallInt,
            DataType::Integer,
            DataType::BigInt,
            DataType::Text,
            DataType::Bytes,
        ]
        .into_iter()
        .enumerate()
        {
            let result = transaction(vec![EmittedValue::Row {
                columns: vec![column("x", data_type, false)],
                row: SqlRow {
                    version: 7,
                    values: vec![Value::Null],
                },
            }]);
            let mut expected =
                golden_prefix(&[1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, b'x', tag as u8, 0]);
            expected.extend([
                1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, b'x', tag as u8, 0, 0, 0, 0, 0, 0, 0, 0,
                7, 0,
            ]);
            assert_eq!(roundtrip(&result), expected);
        }
        for (status, affected, tail) in [
            (ConflictStatus::Success, 1, [0, 0, 0, 0, 255, 255]),
            (ConflictStatus::Skipped, 0, [0, 0, 1, 0, 255, 255]),
        ] {
            let bytes = roundtrip(&operation(Outcome {
                matched: 1,
                affected,
                conflict_results: Some(vec![ConflictTupleOutcome {
                    input_index: 0,
                    status,
                    changed: false,
                    returned_row: None,
                }]),
                ..Outcome::default()
            }));
            assert_eq!(&bytes[bytes.len() - 6..], &tail);
        }
    }

    #[test]
    fn declared_schema_survives_nulls_empty_results_and_terminal_errors() {
        for data_type in [
            DataType::Boolean,
            DataType::SmallInt,
            DataType::Integer,
            DataType::BigInt,
            DataType::Text,
            DataType::Bytes,
        ] {
            let mut columns = vec![column("original_alias", data_type.clone(), false)];
            columns[0].id = 912;
            let description = OnceDescription {
                catalog_epoch: 0x0102030405060708,
                output: OnceSchema::Operation(columns),
            };
            for rows in [
                vec![],
                vec![SqlRow {
                    version: 42,
                    values: vec![Value::Null],
                }],
            ] {
                let result = operation(Outcome {
                    columns: vec!["original_alias".into()],
                    matched: rows.len(),
                    rows,
                    ..Outcome::default()
                });
                let bytes = super::encode(&description, &result).unwrap();
                assert_eq!(&bytes[5..13], &[1, 2, 3, 4, 5, 6, 7, 8]);
                assert_eq!(decode(&bytes).unwrap(), (description.clone(), result));
            }
            for error in [
                Error::NotFound,
                Error::AmbiguousMatch,
                Error::ConditionFailed,
                Error::TooManyMatches,
                Error::RequirementFailed,
            ] {
                let result = Err(error);
                let bytes = super::encode(&description, &result).unwrap();
                assert_eq!(decode(&bytes).unwrap(), (description.clone(), result));
            }
            let description = OnceDescription {
                catalog_epoch: 88,
                output: OnceSchema::Transaction(vec![EmitDescription::Scalar {
                    data_type: Some(data_type),
                    nullable: true,
                }]),
            };
            for result in [scalar(Value::Null), Err(Error::RequirementFailed)] {
                let bytes = super::encode(&description, &result).unwrap();
                assert_eq!(decode(&bytes).unwrap(), (description.clone(), result));
            }
        }
        let mut distinct = std::collections::BTreeSet::new();
        for data_type in [DataType::SmallInt, DataType::Integer, DataType::BigInt] {
            let description = OnceDescription {
                catalog_epoch: 1,
                output: OnceSchema::Transaction(vec![EmitDescription::Scalar {
                    data_type: Some(data_type),
                    nullable: false,
                }]),
            };
            let result = scalar(Value::Int(7));
            let bytes = super::encode(&description, &result).unwrap();
            assert_eq!(decode(&bytes).unwrap(), (description, result));
            distinct.insert(bytes);
        }
        assert_eq!(distinct.len(), 3);
        let untyped = fixture_description(&scalar(Value::Null));
        let typed = OnceDescription {
            catalog_epoch: 0,
            output: OnceSchema::Transaction(vec![EmitDescription::Scalar {
                data_type: Some(DataType::BigInt),
                nullable: true,
            }]),
        };
        assert_ne!(
            super::encode(&untyped, &scalar(Value::Null)).unwrap(),
            super::encode(&typed, &scalar(Value::Null)).unwrap()
        );
    }

    #[test]
    fn schema_and_successful_value_mismatches_are_rejected_before_encoding() {
        let result = operation(Outcome {
            columns: vec!["x".into()],
            rows: vec![SqlRow {
                version: 5,
                values: vec![Value::Null],
            }],
            matched: 1,
            ..Outcome::default()
        });
        for output in [
            OnceSchema::Transaction(vec![]),
            OnceSchema::Operation(vec![]),
            OnceSchema::Operation(vec![column("other", DataType::Text, false)]),
            OnceSchema::Operation(vec![column("x", DataType::Text, true)]),
            OnceSchema::Operation(vec![column("x", DataType::Text, false); 2]),
            OnceSchema::Operation(vec![column("x", DataType::Text, false); 65]),
        ] {
            let description = OnceDescription {
                catalog_epoch: 0,
                output,
            };
            assert_eq!(
                super::encode(&description, &result),
                Err(OnceError::InvalidOutcome)
            );
        }
        for (data_type, nullable, value) in [
            (None, false, Value::Null),
            (None, true, Value::Int(1)),
            (Some(DataType::Text), false, Value::Null),
            (Some(DataType::Text), true, Value::Bytes(vec![])),
            (Some(DataType::SmallInt), false, Value::Int(32768)),
            (Some(DataType::Integer), false, Value::Int(i64::MAX)),
            (Some(DataType::Boolean), false, Value::Int(1)),
        ] {
            let description = OnceDescription {
                catalog_epoch: 0,
                output: OnceSchema::Transaction(vec![EmitDescription::Scalar {
                    data_type,
                    nullable,
                }]),
            };
            assert_eq!(
                super::encode(&description, &scalar(value)),
                Err(OnceError::InvalidOutcome)
            );
        }
        let columns = vec![column("x", DataType::Text, false)];
        let result = transaction(vec![EmittedValue::Row {
            columns: columns.clone(),
            row: SqlRow {
                version: 5,
                values: vec![Value::Null],
            },
        }]);
        for mutation in 0..7 {
            let mut changed = columns.clone();
            match mutation {
                0 => changed[0].id += 1,
                1 => changed[0].name = "other".into(),
                2 => changed[0].data_type = DataType::BigInt,
                3 => changed[0].not_null = true,
                _ => (),
            }
            let emitted = match mutation {
                4 => vec![EmitDescription::Rows { columns: changed }],
                5 => vec![],
                6 => vec![EmitDescription::Row { columns: changed }; 65],
                _ => vec![EmitDescription::Row { columns: changed }],
            };
            let description = OnceDescription {
                catalog_epoch: 0,
                output: OnceSchema::Transaction(emitted),
            };
            assert_eq!(
                super::encode(&description, &result),
                Err(OnceError::InvalidOutcome)
            );
        }
        // A malformed schema is rejected even when no result values exist.
        let malformed = OnceDescription {
            catalog_epoch: 0,
            output: OnceSchema::Transaction(vec![EmitDescription::Scalar {
                data_type: None,
                nullable: false,
            }]),
        };
        assert_eq!(
            super::encode(&malformed, &Err(Error::NotFound)),
            Err(OnceError::InvalidOutcome)
        );
    }

    #[test]
    fn schema_corruption_and_value_mismatches_fail_the_allocation_free_pass() {
        let result = operation(Outcome {
            columns: vec!["x".into()],
            rows: vec![SqlRow {
                version: 0,
                values: vec![Value::Null],
            }],
            matched: 1,
            ..Outcome::default()
        });
        let encoded = roundtrip(&result);
        // Independent single-column wire offsets: descriptor begins at 15;
        // result begins at 27, followed by counts, names, row count and row.
        for (index, value) in [(14, 65), (23, 64), (24, b'y'), (25, 255), (26, 1), (27, 1)] {
            let mut bytes = encoded.clone();
            bytes[index] = value;
            bad(&bytes);
        }
        let declared_small = OnceDescription {
            catalog_epoch: 0,
            output: OnceSchema::Operation(vec![column("x", DataType::SmallInt, true)]),
        };
        let result = operation(Outcome {
            columns: vec!["x".into()],
            rows: vec![SqlRow {
                version: 0,
                values: vec![Value::Int(32767)],
            }],
            matched: 1,
            ..Outcome::default()
        });
        let mut bytes = super::encode(&declared_small, &result).unwrap();
        bytes[49..57].copy_from_slice(&32768i64.to_be_bytes());
        bad(&bytes);
        let result = transaction(vec![EmittedValue::Row {
            columns: vec![column("x", DataType::Text, false)],
            row: SqlRow {
                version: 7,
                values: vec![Value::Null],
            },
        }]);
        let encoded = roundtrip(&result);
        for (index, value) in [(15, 2), (24, 1), (26, b'y'), (27, 3), (28, 1)] {
            let mut bytes = encoded.clone();
            bytes[index] = value;
            bad(&bytes);
        }
        let mut untyped = roundtrip(&scalar(Value::Null));
        // Untyped schema occupies one fewer byte than a known scalar type.
        untyped[22] = 1;
        untyped.push(1);
        bad(&untyped); // valid Bool value cannot inhabit unknown/NULL type
    }

    #[test]
    fn malformed_wire_is_rejected_before_materialization() {
        let encoded = roundtrip(&scalar(Value::Text("hello".into())));
        for len in 0..encoded.len() {
            bad(&encoded[..len]);
        }
        let mut trailing = encoded.clone();
        trailing.push(0);
        bad(&trailing);
        for (index, value) in [
            (0, b'X'),
            (4, 2),
            (13, 255), // schema kind
            (14, 65),  // schema emission count
            (15, 3),   // schema emission kind
            (16, 2),   // type presence
            (17, 255), // data type
            (18, 2),   // nullability
            (19, 0),   // operation result paired with transaction schema
            (20, 65),  // summary count
            (21, 65),  // value emission count
            (22, 3),   // value emission kind
            (23, 255), // value type
        ] {
            let mut bytes = encoded.clone();
            bytes[index] = value;
            bad(&bytes);
        }
        let mut bytes = encoded.clone();
        bytes[24..28].copy_from_slice(&u32::MAX.to_be_bytes());
        bad(&bytes);
        let mut bytes = encoded;
        bytes[28] = 255;
        bad(&bytes);
        let mut boolean = roundtrip(&scalar(Value::Bool(true)));
        boolean[24] = 2;
        bad(&boolean);
        let mut terminal = roundtrip(&Err(Error::NotFound));
        terminal[16] = 5;
        bad(&terminal);
        let mut count = roundtrip(&operation(Outcome::default()));
        count[23] = 65;
        bad(&count);
        let mut count = roundtrip(&operation(Outcome::default()));
        count[24..26].copy_from_slice(&1025u16.to_be_bytes());
        bad(&count);
        let mut presence = roundtrip(&operation(Outcome::default()));
        presence[26] = 2;
        bad(&presence);
        bad(&vec![0; MAX_ONCE_RESULT_BYTES + 1]);
    }

    #[test]
    fn invalid_public_shapes_and_typed_rows_do_not_enter_the_codec() {
        let valid = Outcome {
            columns: vec!["id".into()],
            rows: vec![SqlRow {
                version: 1,
                values: vec![Value::Int(1)],
            }],
            matched: 1,
            ..Outcome::default()
        };
        let mut cases = vec![
            Outcome {
                matched: usize::MAX,
                ..Outcome::default()
            },
            Outcome {
                matched: 1,
                affected: 2,
                ..Outcome::default()
            },
            Outcome {
                matched: 1,
                changed: 1,
                ..Outcome::default()
            },
            Outcome {
                columns: vec!["x".into(); 65],
                ..Outcome::default()
            },
            Outcome {
                columns: vec!["x".into(), "x".into()],
                ..Outcome::default()
            },
            Outcome {
                columns: vec!["bad.name".into()],
                ..Outcome::default()
            },
            Outcome {
                columns: vec!["x".repeat(64)],
                ..Outcome::default()
            },
        ];
        let mut bad_arity = valid.clone();
        bad_arity.rows[0].values.clear();
        cases.push(bad_arity);
        let mut no_columns = valid;
        no_columns.columns.clear();
        cases.push(no_columns);
        for out in cases {
            assert_eq!(encode(&operation(out)), Err(OnceError::InvalidOutcome));
        }
        for (column, value) in [
            (column("n", DataType::SmallInt, true), Value::Int(32768)),
            (column("n", DataType::Integer, true), Value::Int(i64::MAX)),
            (column("n", DataType::Text, true), Value::Null),
            (
                column("n", DataType::Bytes, false),
                Value::Text("wrong".into()),
            ),
        ] {
            assert_eq!(
                encode(&transaction(vec![EmittedValue::Row {
                    columns: vec![column],
                    row: SqlRow {
                        version: 1,
                        values: vec![value]
                    }
                }])),
                Err(OnceError::InvalidOutcome)
            );
        }
        for emitted in [
            vec![EmittedValue::Scalar(Value::Null); 65],
            vec![EmittedValue::Rows {
                columns: vec![],
                rows: vec![],
                complete: true,
            }],
            vec![EmittedValue::Rows {
                columns: vec![column("n", DataType::BigInt, false)],
                rows: vec![
                    SqlRow {
                        version: 0,
                        values: vec![Value::Null]
                    };
                    1025
                ],
                complete: true,
            }],
        ] {
            assert_eq!(
                encode(&transaction(emitted)),
                Err(OnceError::InvalidOutcome)
            );
        }
        let summary = StepSummary {
            item_index: 0,
            matched: 0,
            affected: 0,
            changed: 0,
            complete: true,
            conflict_results: None,
        };
        for summaries in [
            vec![summary.clone(), summary.clone()],
            vec![StepSummary {
                item_index: 64,
                ..summary.clone()
            }],
            vec![summary; 65],
        ] {
            assert_eq!(
                encode(&Ok(OnceOutcome::Transaction(TransactionOutcome {
                    summaries,
                    emitted: vec![]
                }))),
                Err(OnceError::InvalidOutcome)
            );
        }
        let full = TransactionOutcome {
            summaries: vec![StepSummary {
                item_index: 63,
                matched: 0,
                affected: 0,
                changed: 0,
                complete: false,
                conflict_results: None,
            }],
            emitted: vec![],
        };
        roundtrip(&Ok(OnceOutcome::Transaction(full.clone())));
        let mut too_many = full;
        too_many.emitted.push(EmittedValue::Scalar(Value::Null));
        assert_eq!(
            encode(&Ok(OnceOutcome::Transaction(too_many))),
            Err(OnceError::InvalidOutcome)
        );
    }

    #[test]
    fn conflict_metadata_indices_status_and_counts_are_validated() {
        let success = ConflictTupleOutcome {
            input_index: 0,
            status: ConflictStatus::Success,
            changed: false,
            returned_row: Some(0),
        };
        let base = Outcome {
            columns: vec!["id".into()],
            rows: vec![SqlRow {
                version: 1,
                values: vec![Value::Int(1)],
            }],
            matched: 1,
            affected: 1,
            conflict_results: Some(vec![success.clone()]),
            ..Outcome::default()
        };
        let encoded = roundtrip(&operation(base.clone()));
        for result in [
            ConflictTupleOutcome {
                input_index: 1,
                ..success.clone()
            },
            ConflictTupleOutcome {
                status: ConflictStatus::Skipped,
                ..success.clone()
            },
            ConflictTupleOutcome {
                changed: true,
                ..success.clone()
            },
            ConflictTupleOutcome {
                returned_row: None,
                ..success.clone()
            },
            ConflictTupleOutcome {
                returned_row: Some(1),
                ..success.clone()
            },
        ] {
            let mut out = base.clone();
            out.conflict_results = Some(vec![result]);
            assert_eq!(encode(&operation(out)), Err(OnceError::InvalidOutcome));
        }
        let mut out = base.clone();
        out.conflict_results = Some(vec![]);
        assert_eq!(encode(&operation(out)), Err(OnceError::InvalidOutcome));
        let mut duplicate = base;
        duplicate.matched = 2;
        duplicate.affected = 2;
        duplicate.rows.push(duplicate.rows[0].clone());
        duplicate
            .conflict_results
            .as_mut()
            .unwrap()
            .push(ConflictTupleOutcome {
                input_index: 1,
                ..success
            });
        assert_eq!(
            encode(&operation(duplicate)),
            Err(OnceError::InvalidOutcome)
        );
        for (offset, value) in [(5, 1), (4, 2), (3, 2), (1, 4)] {
            let mut bytes = encoded.clone();
            let index = bytes.len() - offset;
            bytes[index] = value;
            bad(&bytes);
        }
    }

    // Six repeated rows emissions have substantial metadata even though empty
    // Text values consume zero ordinary value-payload allowance. Their exact
    // wire-size arithmetic below is independent of the encoder's count pass.
    fn dense_result(delta: isize) -> BusinessResult {
        let columns = (0..64)
            .map(|i| column(&format!("c{i}"), DataType::Text, false))
            .collect::<Vec<_>>();
        let descriptor_bytes: usize = columns.iter().map(|c| 8 + 1 + c.name.len() + 1 + 1).sum();
        let rows_frame = 1 + 1 + descriptor_bytes + 2 + 1 + 1024 * (8 + 64 * (1 + 4));
        let empty_scalar_frame = 1 + 1 + 4;
        let schema_bytes = 8 + 1 + 1 + 6 * (1 + 1 + descriptor_bytes) + 4;
        let overhead = 4 + 1 + schema_bytes + 1 + 1 + 1 + 6 * rows_frame + empty_scalar_frame;
        let payload = (MAX_ONCE_RESULT_BYTES as isize + delta - overhead as isize) as usize;
        assert!(payload < MAX_RESULT_BYTES);
        let mut rows = vec![
            SqlRow {
                version: 1,
                values: vec![Value::Text(String::new()); 64]
            };
            1024
        ];
        rows[0].values[0] = Value::Text("p".repeat(payload / 6));
        let mut emitted = (0..6)
            .map(|_| EmittedValue::Rows {
                columns: columns.clone(),
                rows: rows.clone(),
                complete: true,
            })
            .collect::<Vec<_>>();
        emitted.push(EmittedValue::Scalar(Value::Text("q".repeat(payload % 6))));
        transaction(emitted)
    }
    #[test]
    fn exact_encoded_limit_includes_empty_values_and_all_descriptors() {
        for delta in [-1, 0] {
            let result = dense_result(delta);
            let bytes = roundtrip(&result);
            assert_eq!(
                bytes.len(),
                (MAX_ONCE_RESULT_BYTES as isize + delta) as usize
            );
        }
        assert_eq!(
            encode(&dense_result(1)),
            Err(OnceError::RetentionUnavailable)
        );
        let mut oversized = encode(&dense_result(0)).unwrap();
        oversized.push(0);
        bad(&oversized);
    }

    #[test]
    fn aggregate_raw_value_budget_is_independent_of_encoded_budget() {
        let value = transaction(vec![EmittedValue::Scalar(Value::Bytes(vec![7; 65536])); 16]);
        let bytes = roundtrip(&value);
        assert!(bytes.len() < MAX_ONCE_RESULT_BYTES);
        let mut over = value;
        let Ok(OnceOutcome::Transaction(out)) = &mut over else {
            unreachable!();
        };
        let EmittedValue::Scalar(Value::Bytes(payload)) = &mut out.emitted[15] else {
            unreachable!();
        };
        payload.push(8);
        assert_eq!(encode(&over), Err(OnceError::RetentionUnavailable));
        // Mutate the independently pinned scalar frame: header/epoch/schema
        // 5+8+1+1+16*4, then result/counts3 and each
        // frame is emission tag1 + value tag1 + length4 +65536 payload bytes.
        let mut malformed = bytes;
        let length = (5 + 8 + 1 + 1 + 16 * 4 + 3) + 15 * (1 + 1 + 4 + 65536) + 2;
        malformed[length..length + 4].copy_from_slice(&65537u32.to_be_bytes());
        malformed.push(8);
        bad(&malformed);
        let zeros = transaction(vec![EmittedValue::Scalar(Value::Null); 64]);
        roundtrip(&zeros);
    }

    fn update(count: usize) -> BusinessResult {
        operation(Outcome { matched: count, affected: count, changed: count, ..Outcome::default() })
    }
    // Every odd tuple is skipped; with RETURN each success returns its own row.
    fn upsert(count: usize, returning: bool) -> BusinessResult {
        let affected = count.div_ceil(2);
        let conflicts = (0..count)
            .map(|i| ConflictTupleOutcome {
                input_index: i,
                status: if i % 2 == 0 { ConflictStatus::Success } else { ConflictStatus::Skipped },
                changed: i % 2 == 0,
                returned_row: (returning && i % 2 == 0).then_some(i / 2),
            })
            .collect();
        let row = SqlRow { version: 1, values: vec![Value::Int(7)] };
        operation(Outcome {
            columns: if returning { vec!["n".into()] } else { vec![] },
            rows: if returning { vec![row; affected] } else { vec![] },
            matched: count,
            affected,
            changed: affected,
            complete: true,
            conflict_results: Some(conflicts),
        })
    }
    fn wide(result: &BusinessResult) -> Vec<u8> {
        super::encode_wide(&fixture_description(result), result).unwrap()
    }

    #[test]
    fn version_one_is_kept_whenever_counts_fit_it() {
        let summary = |matched| StepSummary {
            item_index: 0,
            matched,
            affected: matched,
            changed: 0,
            complete: true,
            conflict_results: None,
        };
        let rows = |n| EmittedValue::Rows {
            columns: vec![column("n", DataType::BigInt, false)],
            rows: vec![SqlRow { version: 0, values: vec![Value::Int(1)] }; n],
            complete: true,
        };
        for result in [
            operation(Outcome::default()),
            update(1024),
            upsert(1024, true),
            upsert(1024, false),
            dense_result(0),
            Err(Error::NotFound),
            transaction(vec![rows(1024)]),
            Ok(OnceOutcome::Transaction(TransactionOutcome {
                summaries: vec![summary(1024)],
                emitted: vec![],
            })),
        ] {
            let narrow = roundtrip(&result);
            assert_eq!(wide(&result), narrow);
            assert!(narrow[4] == 1 && !is_wide(&narrow));
        }
        // The legacy encoder keeps the 1,024-row cap; V2 widens only past it.
        for result in [
            update(5000),
            upsert(5000, true),
            upsert(2049, false),
            transaction(vec![rows(1500)]),
            Ok(OnceOutcome::Transaction(TransactionOutcome {
                summaries: vec![summary(70_000)],
                emitted: vec![],
            })),
        ] {
            assert_eq!(encode(&result), Err(OnceError::InvalidOutcome));
            let bytes = wide(&result);
            assert!(is_wide(&bytes));
            validate(&bytes).unwrap();
            assert_eq!(decode(&bytes).unwrap(), (fixture_description(&result), result.clone()));
        }
        // Golden: u32 counts after version byte 2.
        let mut expected = golden_prefix(&[0, 0]);
        expected[4] = 2;
        expected.extend([0, 0, 0, 0x13, 0x88, 0, 0, 0x13, 0x88, 0, 0, 0x13, 0x88, 1, 0]);
        expected.extend([0, 0, 0, 0, 0]);
        assert_eq!(wide(&update(5000)), expected);
        // A skipped tuple's NO_ROW is u32::MAX in version 2.
        let bytes = wide(&upsert(2049, false));
        assert_eq!(bytes[bytes.len() - 4..], [255; 4]);
        // Non-canonical: version 2 whose counts all fit version 1.
        for result in [update(1024), upsert(3, true)] {
            let mut writer = Writer::new(Buffer { bytes: Vec::with_capacity(MAX_ONCE_RESULT_BYTES) }, true);
            writer.result(&fixture_description(&result), &result).unwrap();
            assert_eq!(writer.sink.bytes[4], 2);
            bad(&writer.sink.bytes);
        }
        // Truncation and duplicate returned rows fail before materialisation.
        let bytes = wide(&upsert(5000, true));
        for cut in [5, 30, bytes.len() / 2, bytes.len() - 1] {
            bad(&bytes[..cut]);
        }
        // Tuple 4998 (10 bytes, second to last) returns row 2499; make it row 0.
        let mut duplicate = bytes.clone();
        let row = duplicate.len() - 14;
        assert_eq!(duplicate[row..row + 4], 2499u32.to_be_bytes());
        duplicate[row..row + 4].copy_from_slice(&0u32.to_be_bytes());
        bad(&duplicate);
        bad(&[b"GDOU\x03".as_slice(), &bytes[5..]].concat());
    }

    #[test]
    fn row_counts_beyond_the_present_bytes_fail_before_reservation() {
        // Emitted Rows: count 1500 then the complete flag. Claim u32::MAX - 1
        // rows: the building reader refuses it as Invalid by the bytes left,
        // instead of first trying to reserve ~4G rows (Bound).
        let rows = EmittedValue::Rows {
            columns: vec![column("n", DataType::BigInt, false)],
            rows: vec![SqlRow { version: 0, values: vec![Value::Int(1)] }; 1500],
            complete: true,
        };
        let bytes = wide(&transaction(vec![rows]));
        let at = bytes.windows(5).position(|w| w == [0, 0, 5, 0xdc, 1]).unwrap();
        let mut corrupt = bytes[..at + 5].to_vec();
        corrupt[at..at + 4].copy_from_slice(&(u32::MAX - 1).to_be_bytes());
        bad(&corrupt);
        assert!(matches!(Reader::new(&corrupt, true).result(), Err(Fault::Invalid)));
        // Operation rows: 5000 rows claimed with no row bytes present.
        let bytes = wide(&update(5000));
        let mut corrupt = bytes[..bytes.len() - 1].to_vec();
        let count = corrupt.len() - 4;
        corrupt[count..].copy_from_slice(&5000u32.to_be_bytes());
        bad(&corrupt);
        assert!(matches!(Reader::new(&corrupt, true).result(), Err(Fault::Invalid)));
    }
}
