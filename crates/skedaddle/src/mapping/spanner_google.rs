//! Checked GoogleSQL -> PostgreSQL mappings (safety, independent of emulator qualification).
//!
//! | Source -> destination | Classification | Domain / reason |
//! |---|---|---|
//! | INT64 -> int8; FLOAT32/64 -> float4/8; BOOL -> bool | Supported | Exact bits |
//! | NUMERIC -> numeric(38,9) | Supported | Finite, 29 integral / 9 fractional digits |
//! | STRING(n/MAX) -> varchar(n)/text; BYTES(n/MAX) -> bytea | Supported | UTF8 / size checked |
//! | DATE -> date | Supported | Years 1..9999 |
//! | TIMESTAMP / commit timestamp -> timestamptz | Supported | Only exact microseconds |
//! | JSON -> jsonb | Supported | Canonical common subset; incompatible values block |
//! | ARRAY<T> -> T[] | Supported | One dimension, lower bound 1; checked elements |
//! | Nested arrays / invalid descriptors | Unsupported | No reversible representation |
//!
//! Source timestamps use `Value::Text` containing exactly
//! `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`, so nanoseconds reach the check intact.
//! Source JSON uses `Value::Json` in the existing Jsonb semantic normal form.
//! GoogleSQL preserves the FIRST duplicate key, PG the LAST: noncanonical JSON
//! (including duplicates), NUL, and numbers outside the common domain block.
//! This deliberately rejects some safe but noncanonical spellings.
//! GoogleSQL NUMERIC source values use normalized coefficients (no display-only
//! fractional zeros); PG numeric(38,9) values have scale 9. Conversion accounts
//! for the typmod's mandatory padding so rollback restores the source normal form.
//! Source arrays carry these source storage types, destination arrays PG types.
//!
//! Domains: https://cloud.google.com/spanner/docs/reference/standard-sql/data-types
//! Cell limits: https://cloud.google.com/spanner/quotas (10 MiB; 2,621,440 characters).

use super::{
    Array, Classification, Date, EncodedBytes, Jsonb, Mapping, MappingError, Numeric, PgType,
    Timestamp, Value,
};

pub(super) const CHARS: usize = 2_621_440;
const CELL: usize = 10 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GoogleType {
    Int64,
    Float32,
    Float64,
    Numeric,
    String(Option<usize>),
    Bytes(Option<usize>),
    Bool,
    Date,
    Timestamp { allow_commit_timestamp: bool },
    Json,
    Array(Box<GoogleType>),
}

/// SQL carries typmods absent from PgType; callers must use it when emitting DDL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Destination {
    pub pg_type: PgType,
    pub sql: String,
}

#[derive(Clone, Debug)]
pub struct GoogleMapping {
    pub source: GoogleType,
    pub(super) postgres: bool,
}

pub fn destination(source: &GoogleType) -> Result<Destination, MappingError> {
    GoogleMapping::new(source.clone()).destination()
}

impl GoogleMapping {
    pub fn new(source: GoogleType) -> Self {
        Self {
            source,
            postgres: false,
        }
    }
    pub fn allow_commit_timestamp(&self) -> bool {
        matches!(
            self.source,
            GoogleType::Timestamp {
                allow_commit_timestamp: true
            }
        )
    }
    pub fn destination(&self) -> Result<Destination, MappingError> {
        use GoogleType::*;
        let (pg_type, sql) = match &self.source {
            Int64 => (PgType::Int8, "int8".into()),
            Float32 => (PgType::Float4, "float4".into()),
            Float64 => (PgType::Float8, "float8".into()),
            Numeric => (
                PgType::Numeric,
                if self.postgres {
                    "numeric"
                } else {
                    "numeric(38,9)"
                }
                .into(),
            ),
            String(n) if n.is_none_or(|n| (1..=CHARS).contains(&n)) => match n {
                Some(n) => (
                    PgType::Varchar {
                        encoding: "UTF8".into(),
                    },
                    format!("varchar({n})"),
                ),
                None => (
                    PgType::Text {
                        encoding: "UTF8".into(),
                    },
                    "text".into(),
                ),
            },
            Bytes(n) if n.is_none_or(|n| (1..=CELL).contains(&n)) => {
                (PgType::Bytea, "bytea".into())
            }
            Bool => (PgType::Bool, "bool".into()),
            Date => (PgType::Date, "date".into()),
            Timestamp { .. } => (PgType::Timestamptz, "timestamptz".into()),
            Json => (PgType::Jsonb, "jsonb".into()),
            Array(t)
                if !matches!(
                    **t,
                    Array(_)
                        | Timestamp {
                            allow_commit_timestamp: true
                        }
                ) =>
            {
                let d = self.element(t).destination()?;
                (PgType::Array(Box::new(d.pg_type)), format!("{}[]", d.sql))
            }
            _ => return Err(MappingError::Unsupported("Spanner type descriptor".into())),
        };
        Ok(Destination { pg_type, sql })
    }
    fn element(&self, t: &GoogleType) -> Self {
        Self {
            source: t.clone(),
            postgres: self.postgres,
        }
    }
    pub fn source_storage_type(&self) -> Result<PgType, MappingError> {
        match &self.source {
            GoogleType::Timestamp { .. } => Ok(PgType::Text {
                encoding: "UTF8".into(),
            }),
            GoogleType::Json if !self.postgres => Ok(PgType::Json),
            GoogleType::Array(t) => {
                self.destination()?;
                Ok(PgType::Array(Box::new(
                    self.element(t).source_storage_type()?,
                )))
            }
            _ => Ok(self.destination()?.pg_type),
        }
    }
    fn convert(&self, value: &Value, inverse: bool) -> Result<Value, MappingError> {
        let destination = self.destination()?;
        let storage = if inverse {
            destination.pg_type
        } else {
            self.source_storage_type()?
        };
        storage.check(value, None, 0)?;
        if value == &Value::Null {
            return Ok(Value::Null);
        }
        match (&self.source, value) {
            (GoogleType::Numeric, Value::Numeric(n)) => {
                // https://cloud.google.com/spanner/docs/working-with-numerics
                match n.finite_parts() {
                    Some((_, digits, scale))
                        if self.postgres
                            && digits.len().max(scale as usize) <= 147_455
                            && scale <= 16_383 => {}
                    Some((_, digits, scale))
                        if !self.postgres
                            && scale <= 9
                            && digits.len().saturating_sub(scale as usize) <= 29 => {}
                    None if self.postgres && n == &Numeric::NAN => {}
                    _ => return Err(out("Spanner numeric precision/scale or special value")),
                }
                if !self.postgres {
                    if inverse {
                        if n.scale() != Some(9) {
                            return Err(out("numeric(38,9) destination scale"));
                        }
                        return Ok(Value::Numeric(n.normalized_json()));
                    }
                    if n != &n.normalized_json() {
                        return Err(out("canonical GoogleSQL numeric required"));
                    }
                    let (negative, digits, scale) =
                        n.finite_parts().expect("finite numeric checked above");
                    let padded = format!("{digits}{}", "0".repeat((9 - scale) as usize));
                    return Ok(Value::Numeric(Numeric::finite(negative, &padded, 9)?));
                }
            }
            (GoogleType::String(n), Value::Text(t) | Value::Varchar(t)) => {
                let s = std::str::from_utf8(&t.bytes).map_err(|_| out("UTF8"))?;
                if s.chars().count() > n.unwrap_or(CHARS) || t.bytes.len() > CELL {
                    return Err(out("Spanner string length"));
                }
            }
            (GoogleType::Bytes(n), Value::Bytea(b)) if b.len() > n.unwrap_or(CELL) => {
                return Err(out("Spanner bytes length"));
            }
            (GoogleType::Date, Value::Date(d)) => {
                check_date(*d)?;
            }
            (GoogleType::Timestamp { .. }, _) => {
                if inverse {
                    let Value::Timestamptz(t) = value else {
                        unreachable!()
                    };
                    check_timestamp(*t)?;
                    let s = super::temporal::format_timestamp(*t, true)
                        .replace(' ', "T")
                        .replace("+00:00", "000Z");
                    return Ok(Value::Text(EncodedBytes {
                        bytes: s.into_bytes(),
                        encoding: "UTF8".into(),
                    }));
                }
                let Value::Text(t) = value else {
                    unreachable!()
                };
                let s = std::str::from_utf8(&t.bytes).map_err(|_| out("timestamp UTF8"))?;
                if s.len() != 30
                    || !s.is_ascii()
                    || &s[19..20] != "."
                    || !s.ends_with('Z')
                    || !s[20..29].bytes().all(|b| b.is_ascii_digit())
                {
                    return Err(out("canonical nanosecond UTC timestamp"));
                }
                if &s[26..29] != "000" {
                    return Err(MappingError::Unsupported(
                        "sub-microsecond timestamp".into(),
                    ));
                }
                let t = super::temporal::parse_timestamp(&format!("{}Z", &s[..26]), true)?;
                check_timestamp(t)?;
                let pg = Value::Timestamptz(t);
                if self.convert(&pg, true)? != *value {
                    return Err(out("noncanonical source timestamp"));
                }
                return Ok(pg);
            }
            (GoogleType::Json, _) => {
                let j = match value {
                    Value::Json(s) => {
                        let j = Jsonb::parse(s)?;
                        if j.as_str() != s {
                            return Err(out("GoogleSQL JSON normalisation"));
                        }
                        j
                    }
                    Value::Jsonb(j) => j.clone(),
                    _ => unreachable!(),
                };
                if j.as_str().len() > CELL {
                    return Err(out("Spanner JSON cell size"));
                }
                if !self.postgres {
                    check_json_numbers(j.as_str())?;
                }
                return Ok(if inverse && !self.postgres {
                    Value::Json(j.as_str().into())
                } else {
                    Value::Jsonb(j)
                });
            }
            (GoogleType::Array(t), Value::Array(a)) => {
                if a.dimensions.len() > 1 || a.dimensions.iter().any(|d| d.lower_bound != 1) {
                    return Err(out("Spanner array shape"));
                }
                a.elements.iter().try_fold(0usize, |sum, v| {
                    let size = match (&**t, v) {
                        (_, Value::Null) => 0,
                        (GoogleType::Timestamp { .. }, _) => 12,
                        (_, Value::Text(t) | Value::Varchar(t)) => t.bytes.len(),
                        (_, Value::Bytea(b)) => b.len(),
                        (_, Value::Json(s)) => s.len(),
                        (_, Value::Jsonb(j)) => j.as_str().len(),
                        (_, Value::Bool(_)) => 1,
                        (_, Value::Float4(_) | Value::Date(_)) => 4,
                        (_, Value::Int8(_) | Value::Float8(_)) => 8,
                        _ => 22, // conservative maximum NUMERIC storage, from data-types docs
                    };
                    sum.checked_add(8 + size)
                        .filter(|n| *n <= CELL)
                        .ok_or_else(|| out("Spanner array cell size"))
                })?;
                let element = self.element(t);
                let elements = a
                    .elements
                    .iter()
                    .map(|v| element.convert(v, inverse))
                    .collect::<Result<_, _>>()?;
                let element_type = if inverse {
                    element.source_storage_type()?
                } else {
                    element.destination()?.pg_type
                };
                return Ok(Value::Array(Array {
                    element_type: Box::new(element_type),
                    dimensions: a.dimensions.clone(),
                    elements,
                }));
            }
            _ => {}
        }
        Ok(value.clone())
    }
}
impl Mapping for GoogleMapping {
    fn forward(&self, value: &Value) -> Result<Value, MappingError> {
        self.convert(value, false)
    }
    fn inverse(&self, value: &Value) -> Result<Value, MappingError> {
        self.convert(value, true)
    }
    fn domain_check(&self, value: &Value) -> Result<(), MappingError> {
        self.convert(value, false).map(|_| ())
    }
    fn classification(&self) -> Classification {
        if self.destination().is_ok() {
            Classification::Supported
        } else {
            Classification::Unsupported
        }
    }
    fn is_reversible(&self) -> bool {
        self.classification() == Classification::Supported
    }
}
fn out(s: &str) -> MappingError {
    MappingError::OutOfDomain(s.into())
}
fn check_date(d: Date) -> Result<(), MappingError> {
    let min = super::temporal::parse_date("0001-01-01")?;
    let max = super::temporal::parse_date("9999-12-31")?;
    match (d, min, max) {
        (Date::Finite(d), Date::Finite(min), Date::Finite(max)) if (min..=max).contains(&d) => {
            Ok(())
        }
        _ => Err(out("Spanner date range")),
    }
}
fn check_timestamp(t: Timestamp) -> Result<(), MappingError> {
    match t {
        Timestamp::Finite(t) => {
            check_date(Date::Finite((t.div_euclid(super::temporal::DAY)) as i32))
        }
        _ => Err(out("Spanner timestamp range")),
    }
}
fn check_json_numbers(s: &str) -> Result<(), MappingError> {
    // Jsonb is already validated/canonical: tokenize only outside quoted strings.
    let (mut quoted, mut escaped, mut i) = (false, false, 0);
    let b = s.as_bytes();
    while i < b.len() {
        let c = b[i];
        if quoted {
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                quoted = false;
            }
        } else if c == b'"' {
            quoted = true;
        } else if c == b'-' || c.is_ascii_digit() {
            let start = i;
            i += 1;
            while i < b.len() && matches!(b[i], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-') {
                i += 1;
            }
            let token = &s[start..i];
            if token.parse::<i64>().is_err() && token.parse::<u64>().is_err() {
                let f: f64 = token.parse().map_err(|_| out("JSON number"))?;
                if !f.is_finite()
                    || Numeric::parse(token)? != Numeric::parse(&f.to_string())?.normalized_json()
                {
                    return Err(out("GoogleSQL JSON numeric round trip"));
                }
            }
            continue;
        }
        i += 1;
    }
    Ok(())
}
#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::mapping::ArrayDimension;
    pub(in crate::mapping) fn text(s: &str) -> Value {
        Value::Text(EncodedBytes {
            bytes: s.as_bytes().into(),
            encoding: "UTF8".into(),
        })
    }
    fn varchar(s: &str) -> Value {
        Value::Varchar(EncodedBytes {
            bytes: s.as_bytes().into(),
            encoding: "UTF8".into(),
        })
    }
    pub(in crate::mapping) fn numeric(s: &str) -> Value {
        Value::Numeric(Numeric::parse(s).unwrap())
    }
    pub(in crate::mapping) fn rows() -> Vec<(GoogleType, Vec<Value>, Vec<Value>)> {
        use GoogleType::*;
        let dates = ["0001-01-01", "2000-01-01", "9999-12-31"]
            .into_iter()
            .map(|s| Value::Date(super::super::temporal::parse_date(s).unwrap()))
            .collect();
        vec![
            (
                Int64,
                [i64::MIN, -1, 0, i64::MAX]
                    .into_iter()
                    .map(Value::Int8)
                    .collect(),
                vec![Value::Int4(0)],
            ),
            (
                Float32,
                [
                    f32::MIN,
                    f32::MAX,
                    f32::MIN_POSITIVE,
                    f32::from_bits(1),
                    0.,
                    -0.,
                    f32::NAN,
                    f32::from_bits(0x7fc00001),
                    f32::INFINITY,
                    f32::NEG_INFINITY,
                ]
                .into_iter()
                .map(|f| Value::Float4(f.to_bits()))
                .collect(),
                vec![Value::Float8(0)],
            ),
            (
                Float64,
                [
                    f64::MIN,
                    f64::MAX,
                    f64::MIN_POSITIVE,
                    f64::from_bits(1),
                    0.,
                    -0.,
                    f64::NAN,
                    f64::from_bits(0x7ff8000000000001),
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                ]
                .into_iter()
                .map(|f| Value::Float8(f.to_bits()))
                .collect(),
                vec![Value::Float4(0)],
            ),
            (
                Numeric,
                [
                    "-99999999999999999999999999999.999999999",
                    "0",
                    "0.000000001",
                    "99999999999999999999999999999.999999999",
                ]
                .into_iter()
                .map(numeric)
                .collect(),
                ["1e29", "1e-10", "NaN", "Infinity", "-Infinity"]
                    .into_iter()
                    .map(numeric)
                    .collect(),
            ),
            (
                String(Some(2)),
                vec![varchar(""), varchar("a"), varchar("é🦀")],
                vec![
                    varchar("abc"),
                    Value::Varchar(EncodedBytes {
                        bytes: vec![255],
                        encoding: "UTF8".into(),
                    }),
                    varchar("\0"),
                ],
            ),
            (
                String(None),
                vec![text(""), text("é🦀"), text(&"a".repeat(CHARS))],
                vec![text(&"a".repeat(CHARS + 1))],
            ),
            (
                Bytes(Some(2)),
                vec![Value::Bytea(vec![]), Value::Bytea(vec![0, 255])],
                vec![Value::Bytea(vec![0; 3])],
            ),
            (
                Bytes(None),
                vec![Value::Bytea(vec![]), Value::Bytea(vec![0; CELL])],
                vec![Value::Bytea(vec![0; CELL + 1])],
            ),
            (
                Bool,
                vec![Value::Bool(false), Value::Bool(true)],
                vec![Value::Int8(1)],
            ),
            (
                Date,
                dates,
                vec![
                    Value::Date(crate::mapping::Date::Finite(-730120)),
                    Value::Date(crate::mapping::Date::Finite(2921940)),
                    Value::Date(crate::mapping::Date::PositiveInfinity),
                    Value::Date(crate::mapping::Date::NegativeInfinity),
                ],
            ),
            (
                Timestamp {
                    allow_commit_timestamp: false,
                },
                [
                    "0001-01-01T00:00:00.000000000Z",
                    "1999-12-31T23:59:59.999999000Z",
                    "2000-01-01T00:00:00.000000000Z",
                    "9999-12-31T23:59:59.999999000Z",
                ]
                .into_iter()
                .map(text)
                .collect(),
                vec![
                    Value::Timestamptz(crate::mapping::Timestamp::Finite(-63_082_281_600_000_001)),
                    Value::Timestamptz(crate::mapping::Timestamp::Finite(252_455_616_000_000_000)),
                    Value::Timestamptz(crate::mapping::Timestamp::PositiveInfinity),
                ],
            ),
            (
                Json,
                [
                    "null",
                    "true",
                    "\"é🦀\"",
                    "{\"a\":[1,0.1,null],\"b\":-9223372036854775808}",
                    "18446744073709551615",
                ]
                .into_iter()
                .map(|s| Value::Json(s.into()))
                .collect(),
                vec![Value::Jsonb(Jsonb::parse("0.10000000000000001").unwrap())],
            ),
        ]
    }
    pub(in crate::mapping) fn roundtrips(m: &dyn Mapping, good: &[Value], bad: &[Value]) {
        assert_eq!(m.classification(), Classification::Supported);
        assert!(m.is_reversible());
        for v in good.iter().chain([&Value::Null]) {
            m.domain_check(v).unwrap();
            let pg = m.forward(v).unwrap();
            assert_eq!(m.inverse(&pg).unwrap(), *v);
            assert_eq!(m.forward(&m.inverse(&pg).unwrap()).unwrap(), pg);
        }
        for v in bad {
            assert!(m.inverse(v).is_err(), "inverse accepted {v:?}");
            assert!(m.forward(v).is_err(), "forward accepted {v:?}");
        }
    }
    #[test]
    fn matrix_boundaries_and_classifications() {
        for (t, good, bad) in rows() {
            let m = GoogleMapping::new(t);
            roundtrips(&m, &good, &bad);
        }
        assert_eq!(
            destination(&GoogleType::Numeric).unwrap().sql,
            "numeric(38,9)"
        );
        assert_eq!(
            destination(&GoogleType::String(Some(2))).unwrap().sql,
            "varchar(2)"
        );
    }
    pub(in crate::mapping) fn timestamp_blocks(m: &dyn Mapping) {
        for s in [
            "2000-01-01T00:00:00.000000001Z",
            "9999-12-31T23:59:59.999999999Z",
            "2000-01-01T00:00:00.123456789Z",
            "10000-01-01T00:00:00.000000000Z",
            "2000-01-01T24:00:00.000000000Z",
        ] {
            assert!(m.forward(&text(s)).is_err());
            assert!(m.domain_check(&text(s)).is_err());
        }
    }
    #[test]
    fn numeric_typmod_and_source_normal_form() {
        let m = GoogleMapping::new(GoogleType::Numeric);
        assert_eq!(m.forward(&numeric("0")).unwrap(), numeric("0.000000000"));
        assert_eq!(m.inverse(&numeric("1.200000000")).unwrap(), numeric("1.2"));
        assert!(m.forward(&numeric("1.20")).is_err());
        assert!(m.inverse(&numeric("1.2")).is_err());
    }
    #[test]
    fn timestamp_loss_and_commit_flag() {
        for allow_commit_timestamp in [false, true] {
            let m = GoogleMapping::new(GoogleType::Timestamp {
                allow_commit_timestamp,
            });
            timestamp_blocks(&m);
            assert_eq!(m.allow_commit_timestamp(), allow_commit_timestamp);
            roundtrips(&m, &[text("2000-01-01T00:00:00.000000000Z")], &[]);
        }
    }
    #[test]
    fn json_normalization_blocks() {
        let m = GoogleMapping::new(GoogleType::Json);
        for s in [
            "{\"a\":1,\"a\":2}",
            "{\"a\":2,\"a\":1}",
            "{\"\\u0061\":1,\"a\":2}",
            "[ 1 ]",
            "-0",
            "\"\\u0000\"",
            "1e400",
            "0.10000000000000001",
        ] {
            assert!(m.forward(&Value::Json(s.into())).is_err(), "{s}");
        }
    }
    pub(in crate::mapping) fn arrays(element: &GoogleMapping, good: &Value) {
        let m = GoogleMapping {
            source: GoogleType::Array(Box::new(element.source.clone())),
            postgres: element.postgres,
        };
        let a = Value::Array(Array {
            element_type: Box::new(element.source_storage_type().unwrap()),
            dimensions: vec![ArrayDimension {
                length: 2,
                lower_bound: 1,
            }],
            elements: vec![good.clone(), Value::Null],
        });
        roundtrips(&m, &[a], &[]);
        let mut bad = Array {
            element_type: Box::new(element.destination().unwrap().pg_type),
            dimensions: vec![ArrayDimension {
                length: 1,
                lower_bound: 0,
            }],
            elements: vec![Value::Null],
        };
        assert!(m.inverse(&Value::Array(bad.clone())).is_err());
        bad.dimensions = vec![
            ArrayDimension {
                length: 1,
                lower_bound: 1
            };
            2
        ];
        assert!(m.inverse(&Value::Array(bad.clone())).is_err());
        bad.dimensions.clear();
        bad.elements.clear();
        let empty = Value::Array(bad);
        assert_eq!(m.forward(&m.inverse(&empty).unwrap()).unwrap(), empty);
    }
    #[test]
    fn arrays_and_invalid_descriptors() {
        for (t, good, _) in rows() {
            arrays(&GoogleMapping::new(t), &good[0]);
        }
        for t in [
            GoogleType::Array(Box::new(GoogleType::Array(Box::new(GoogleType::Int64)))),
            GoogleType::String(Some(0)),
            GoogleType::String(Some(CHARS + 1)),
            GoogleType::Bytes(Some(CELL + 1)),
        ] {
            let m = GoogleMapping::new(t);
            assert_eq!(m.classification(), Classification::Unsupported);
            assert!(m.forward(&Value::Null).is_err());
            assert!(!m.is_reversible());
        }
        let bytes = GoogleMapping::new(GoogleType::Array(Box::new(GoogleType::Bytes(None))));
        let too_big = Value::Array(Array {
            element_type: Box::new(PgType::Bytea),
            dimensions: vec![ArrayDimension {
                length: 1,
                lower_bound: 1,
            }],
            elements: vec![Value::Bytea(vec![0; CELL])],
        });
        assert!(bytes.forward(&too_big).is_err());
        assert!(bytes.inverse(&too_big).is_err());
        let m = GoogleMapping::new(GoogleType::Array(Box::new(GoogleType::Numeric)));
        let a = Value::Array(Array {
            element_type: Box::new(PgType::Numeric),
            dimensions: vec![ArrayDimension {
                length: 1,
                lower_bound: 1,
            }],
            elements: vec![numeric("1e29")],
        });
        assert!(m.forward(&a).is_err());
        assert!(m.inverse(&a).is_err());
    }
}
