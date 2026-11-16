//! PostgreSQL 16-18 -> Spanner, using S9's checked conversions in reverse.
//!
//! | PostgreSQL | GoogleSQL / PostgreSQL dialect | Classification |
//! |---|---|---|
//! | int2/int4/int8 | INT64 / bigint | Supported; rollback checks width |
//! | float4/float8/bool/bytea | FLOAT32/FLOAT64/BOOL/BYTES / real/double/boolean/bytea | Supported |
//! | numeric(p,s) | NUMERIC / numeric (PG.NUMERIC wire type) | Supported within checked limits |
//! | unconstrained numeric | blocked / numeric | Unsupported / Supported per value |
//! | text/varchar(n)/char(n) | STRING(MAX/n) / varchar | Supported within checked limits |
//! | date/timestamptz | DATE/TIMESTAMP / date/timestamptz | Supported within years 1..9999 |
//! | json/jsonb | JSON / jsonb | Supported only for the exact common normal form |
//! | uuid | STRING(36) / varchar(36) | Supported; canonical lowercase text |
//! | scalar arrays | ARRAY<T> / T[] | Supported; one dimension, lower bound 1 |
//! | timestamp without zone, other types, nested arrays | blocked | Unsupported |
//!
//! `char(n)` retains all n characters, including padding; short/unpadded values
//! block. Consumers must not trim trailing spaces. JSON text must already equal
//! the Jsonb normal form: duplicates, whitespace and alternative spellings block.
//! Numeric typmods are source metadata, never emitted as Spanner PG DDL typmods.
//! Typmods on arrays describe their scalar elements. Missing char length blocks.
//! UUID deliberately uses the requested text representation. Both current
//! dialect references document native UUID, so this conversion is Supported;
//! this does not qualify deployment on older, unverified Spanner versions.
//!
//! Domains and UUID availability checked against official references:
//! https://cloud.google.com/spanner/docs/reference/standard-sql/data-types
//! https://cloud.google.com/spanner/docs/reference/postgresql/data-types
//! https://cloud.google.com/spanner/docs/working-with-numerics

use super::spanner_google::{GoogleMapping, GoogleType};
use super::spanner_pg::SpannerPgType;
use super::{
    Array, Classification, EncodedBytes, Jsonb, Mapping, MappingError, Numeric, PgType, Value,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    GoogleSql,
    PostgreSql,
}

/// Source catalog metadata; precision 1..1000, scale -1000..1000 (PG 16-18).
#[derive(Clone, Debug)]
pub struct Source {
    pub pg_type: PgType,
    pub numeric: Option<(u16, i16)>,
    pub length: Option<usize>,
}
impl Source {
    pub fn new(pg_type: PgType) -> Self {
        Self {
            pg_type,
            numeric: None,
            length: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DestinationType {
    Google(GoogleType),
    PostgreSql(SpannerPgType),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Destination {
    pub column_type: DestinationType,
    pub sql: String,
    /// Same source-storage contract as S9, including nanosecond UTC text.
    pub storage_type: PgType,
}
#[derive(Clone, Debug)]
pub struct PgSpannerMapping {
    pub source: Source,
    pub dialect: Dialect,
}

pub fn destination(source: &Source, dialect: Dialect) -> Result<Destination, MappingError> {
    PgSpannerMapping::new(source.clone(), dialect).destination()
}
fn unsupported(s: impl Into<String>) -> MappingError {
    MappingError::Unsupported(s.into())
}
fn out(s: &str) -> MappingError {
    MappingError::OutOfDomain(s.into())
}

impl PgSpannerMapping {
    pub fn new(source: Source, dialect: Dialect) -> Self {
        Self { source, dialect }
    }
    fn element(&self, pg_type: &PgType) -> Self {
        Self::new(
            Source {
                pg_type: pg_type.clone(),
                ..self.source.clone()
            },
            self.dialect,
        )
    }
    fn engine(&self) -> Result<GoogleMapping, MappingError> {
        use PgType::*;
        let t = match &self.source.pg_type {
            Array(t) if !matches!(**t, Array(_)) => {
                GoogleType::Array(Box::new(self.element(t).engine()?.source))
            }
            Int2 | Int4 | Int8 => GoogleType::Int64,
            Float4 => GoogleType::Float32,
            Float8 => GoogleType::Float64,
            Bool => GoogleType::Bool,
            Bytea => GoogleType::Bytes(None),
            Date => GoogleType::Date,
            Timestamptz => GoogleType::Timestamp {
                allow_commit_timestamp: false,
            },
            Numeric => {
                if let Some((p, s)) = self.source.numeric {
                    if !(1..=1000).contains(&p) || !(-1000..=1000).contains(&s) {
                        return Err(unsupported("invalid PostgreSQL numeric typmod"));
                    }
                    if self.dialect == Dialect::GoogleSql
                        && (i32::from(p) - i32::from(s) > 29 || s > 9)
                    {
                        return Err(unsupported(
                            "numeric typmod exceeds GoogleSQL's 29 integral / 9 fractional digits",
                        ));
                    }
                } else if self.dialect == Dialect::GoogleSql {
                    return Err(unsupported(
                        "unconstrained numeric has no GoogleSQL NUMERIC inverse",
                    ));
                }
                GoogleType::Numeric
            }
            Text { encoding } | Varchar { encoding } | Char { encoding } => {
                if encoding != "UTF8" {
                    return Err(unsupported("Spanner requires UTF8 text"));
                }
                if matches!(self.source.pg_type, Char { .. }) && self.source.length.is_none() {
                    return Err(unsupported("char requires its padding length"));
                }
                GoogleType::String(self.source.length)
            }
            Uuid => GoogleType::String(Some(36)),
            Json | Jsonb => GoogleType::Json,
            Timestamp => {
                return Err(unsupported(
                    "timestamp without time zone requires a declared zone",
                ));
            }
            t => {
                return Err(unsupported(format!(
                    "no reversible Spanner mapping for {t:?}"
                )));
            }
        };
        let m = GoogleMapping {
            source: t,
            postgres: self.dialect == Dialect::PostgreSql,
        };
        m.destination()?;
        Ok(m)
    }
    pub fn destination(&self) -> Result<Destination, MappingError> {
        fn describe(t: &GoogleType, pg: bool) -> (SpannerPgType, String) {
            use GoogleType::*;
            let (p, g, s) = match t {
                Int64 => (SpannerPgType::Bigint, "INT64".into(), "bigint".into()),
                Float32 => (SpannerPgType::Real, "FLOAT32".into(), "real".into()),
                Float64 => (
                    SpannerPgType::DoublePrecision,
                    "FLOAT64".into(),
                    "double precision".into(),
                ),
                Numeric => (SpannerPgType::Numeric, "NUMERIC".into(), "numeric".into()),
                String(n) => (
                    SpannerPgType::Varchar(*n),
                    format!("STRING({})", n.map_or("MAX".into(), |n| n.to_string())),
                    n.map_or("varchar".into(), |n| format!("varchar({n})")),
                ),
                Bytes(_) => (SpannerPgType::Bytea, "BYTES(MAX)".into(), "bytea".into()),
                Bool => (SpannerPgType::Boolean, "BOOL".into(), "boolean".into()),
                Date => (SpannerPgType::Date, "DATE".into(), "date".into()),
                Timestamp { .. } => (
                    SpannerPgType::Timestamptz,
                    "TIMESTAMP".into(),
                    "timestamptz".into(),
                ),
                Json => (SpannerPgType::Jsonb, "JSON".into(), "jsonb".into()),
                Array(t) => {
                    let (p, s) = describe(t, pg);
                    return (
                        SpannerPgType::Array(Box::new(p)),
                        if pg {
                            format!("{s}[]")
                        } else {
                            format!("ARRAY<{s}>")
                        },
                    );
                }
            };
            (p, if pg { s } else { g })
        }
        let m = self.engine()?;
        let (p, sql) = describe(&m.source, m.postgres);
        Ok(Destination {
            column_type: if m.postgres {
                DestinationType::PostgreSql(p)
            } else {
                DestinationType::Google(m.source.clone())
            },
            sql,
            storage_type: m.source_storage_type()?,
        })
    }
    /// Adapt PG representation to/from the PG-facing side of S9's engine.
    fn adapt(&self, v: &Value, inverse: bool) -> Result<Value, MappingError> {
        if v == &Value::Null {
            return Ok(Value::Null);
        }
        Ok(match (&self.source.pg_type, v) {
            (PgType::Array(t), Value::Array(a)) => {
                let element = self.element(t);
                Value::Array(Array {
                    element_type: Box::new(if inverse {
                        *t.clone()
                    } else {
                        element.engine()?.destination()?.pg_type
                    }),
                    dimensions: a.dimensions.clone(),
                    elements: a
                        .elements
                        .iter()
                        .map(|v| element.adapt(v, inverse))
                        .collect::<Result<_, _>>()?,
                })
            }
            (PgType::Int2, Value::Int2(n)) => Value::Int8(i64::from(*n)),
            (PgType::Int4, Value::Int4(n)) => Value::Int8(i64::from(*n)),
            (PgType::Int2, Value::Int8(n)) if inverse => {
                Value::Int2((*n).try_into().map_err(|_| out("int2 rollback range"))?)
            }
            (PgType::Int4, Value::Int8(n)) if inverse => {
                Value::Int4((*n).try_into().map_err(|_| out("int4 rollback range"))?)
            }
            (
                PgType::Text { .. } | PgType::Varchar { .. } | PgType::Char { .. },
                Value::Text(b) | Value::Varchar(b) | Value::Char(b),
            ) => {
                let s = std::str::from_utf8(&b.bytes).map_err(|_| out("UTF8"))?;
                if self.source.length.is_some_and(|n| {
                    s.chars().count() > n
                        || matches!(self.source.pg_type, PgType::Char { .. })
                            && s.chars().count() != n
                }) {
                    return Err(out("source character length/padding"));
                }
                if inverse {
                    match self.source.pg_type {
                        PgType::Char { .. } => Value::Char(b.clone()),
                        PgType::Varchar { .. } => Value::Varchar(b.clone()),
                        _ => Value::Text(b.clone()),
                    }
                } else if self.source.length.is_some() {
                    Value::Varchar(b.clone())
                } else {
                    Value::Text(b.clone())
                }
            }
            (PgType::Uuid, Value::Uuid(u)) => Value::Varchar(EncodedBytes {
                bytes: uuid::Uuid::from_bytes(*u).to_string().into_bytes(),
                encoding: "UTF8".into(),
            }),
            (PgType::Uuid, Value::Varchar(b)) if inverse => {
                let s = std::str::from_utf8(&b.bytes).map_err(|_| out("UUID UTF8"))?;
                let u = uuid::Uuid::parse_str(s).map_err(|_| out("UUID text"))?;
                if u.to_string() != s {
                    return Err(out("canonical UUID text"));
                }
                Value::Uuid(*u.as_bytes())
            }
            (PgType::Json, Value::Json(s)) => {
                let j = Jsonb::parse(s)?;
                if j.as_str() != s {
                    return Err(out("JSON text cannot round-trip after normalisation"));
                }
                Value::Jsonb(j)
            }
            (PgType::Json, Value::Jsonb(j)) if inverse => Value::Json(j.as_str().into()),
            (PgType::Numeric, Value::Numeric(n)) => {
                if let Some((p, s)) = self.source.numeric {
                    let scale = s.max(0) as u32;
                    let source = if inverse {
                        rescale(n, scale)?
                    } else {
                        n.clone()
                    };
                    if let Some((_, digits, actual)) = source.finite_parts() {
                        if actual != scale
                            || digits != "0"
                                && (digits.len() as i32 > i32::from(p) + i32::from((-s).max(0))
                                    || s < 0 && !digits.ends_with(&"0".repeat((-s) as usize)))
                        {
                            return Err(out("source numeric typmod would round or overflow"));
                        }
                    } else if n != &Numeric::NAN {
                        return Err(out("constrained numeric infinity"));
                    }
                    Value::Numeric(if !inverse && self.dialect == Dialect::GoogleSql {
                        rescale(&source, 9)?
                    } else {
                        source
                    })
                } else {
                    v.clone()
                }
            }
            _ => v.clone(),
        })
    }
}
/// Exact rescaling only: a typmod must never round a rollback value.
fn rescale(n: &Numeric, scale: u32) -> Result<Numeric, MappingError> {
    let Some((negative, digits, actual)) = n.finite_parts() else {
        return Ok(n.clone());
    };
    let digits = if actual > scale {
        let remove = (actual - scale) as usize;
        if digits == "0" {
            "0".into()
        } else if remove < digits.len() && digits.ends_with(&"0".repeat(remove)) {
            digits[..digits.len() - remove].into()
        } else {
            return Err(out("numeric rescale would round"));
        }
    } else {
        format!("{digits}{}", "0".repeat((scale - actual) as usize))
    };
    Numeric::finite(negative, &digits, scale)
}
impl Mapping for PgSpannerMapping {
    fn forward(&self, v: &Value) -> Result<Value, MappingError> {
        let m = self.engine()?;
        self.source.pg_type.check(v, None, 0)?;
        m.inverse(&self.adapt(v, false)?)
    }
    fn inverse(&self, v: &Value) -> Result<Value, MappingError> {
        let m = self.engine()?;
        let pg = self.adapt(&m.forward(v)?, true)?;
        self.source.pg_type.check(&pg, None, 0)?;
        if self.forward(&pg)? != *v {
            return Err(out("Spanner value has no exact PostgreSQL inverse"));
        }
        Ok(pg)
    }
    fn domain_check(&self, v: &Value) -> Result<(), MappingError> {
        self.forward(v).map(|_| ())
    }
    fn classification(&self) -> Classification {
        if self.engine().is_ok() {
            Classification::Supported
        } else {
            Classification::Unsupported
        }
    }
    fn is_reversible(&self) -> bool {
        self.classification() == Classification::Supported
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::spanner_google::tests::{numeric, rows};
    use crate::mapping::{ArrayDimension, Date, Timestamp, TypeIdentity};
    fn mapping(t: PgType, d: Dialect) -> PgSpannerMapping {
        PgSpannerMapping::new(Source::new(t), d)
    }
    fn string(s: &str) -> EncodedBytes {
        EncodedBytes {
            bytes: s.as_bytes().into(),
            encoding: "UTF8".into(),
        }
    }
    fn roundtrip(m: &PgSpannerMapping, values: &[Value]) {
        assert_eq!(m.classification(), Classification::Supported);
        assert!(m.is_reversible());
        for v in values.iter().chain([&Value::Null]) {
            m.domain_check(v).unwrap();
            let spanner = m.forward(v).unwrap();
            assert_eq!(m.inverse(&spanner).unwrap(), *v, "{:?}", m.source);
            assert_eq!(m.forward(&m.inverse(&spanner).unwrap()).unwrap(), spanner);
        }
    }
    fn blocked(m: &PgSpannerMapping, values: &[Value]) {
        for v in values {
            assert!(m.domain_check(v).is_err(), "domain accepted {v:?}");
            assert!(m.forward(v).is_err(), "forward accepted {v:?}");
        }
    }
    fn array(m: &PgSpannerMapping, value: &Value) {
        let a = PgSpannerMapping::new(
            Source {
                pg_type: PgType::Array(Box::new(m.source.pg_type.clone())),
                ..m.source.clone()
            },
            m.dialect,
        );
        let good = Value::Array(Array {
            element_type: Box::new(m.source.pg_type.clone()),
            dimensions: vec![ArrayDimension {
                length: 2,
                lower_bound: 1,
            }],
            elements: vec![value.clone(), Value::Null],
        });
        roundtrip(
            &a,
            &[
                good.clone(),
                Value::Array(Array {
                    element_type: Box::new(m.source.pg_type.clone()),
                    dimensions: vec![],
                    elements: vec![],
                }),
            ],
        );
        let Value::Array(mut bad) = good else {
            unreachable!()
        };
        bad.dimensions[0].lower_bound = 0;
        blocked(&a, &[Value::Array(bad.clone())]);
        bad.dimensions = vec![
            ArrayDimension {
                length: 1,
                lower_bound: 1,
            },
            ArrayDimension {
                length: 2,
                lower_bound: 1,
            },
        ];
        blocked(&a, &[Value::Array(bad.clone())]);
        bad.dimensions = vec![ArrayDimension {
            length: 2,
            lower_bound: 0,
        }];
        bad.element_type = match a.destination().unwrap().storage_type {
            PgType::Array(t) => t,
            _ => unreachable!(),
        };
        bad.elements = vec![Value::Null; 2];
        assert!(a.inverse(&Value::Array(bad.clone())).is_err());
        bad.dimensions = vec![
            ArrayDimension {
                length: 1,
                lower_bound: 1,
            },
            ArrayDimension {
                length: 2,
                lower_bound: 1,
            },
        ];
        assert!(a.inverse(&Value::Array(bad)).is_err());
        let nested = mapping(PgType::Array(Box::new(a.source.pg_type.clone())), m.dialect);
        assert_eq!(nested.classification(), Classification::Unsupported);
        assert!(nested.forward(&Value::Null).is_err());
    }
    fn matrix(d: Dialect) {
        // S9 supplies min/max, IEEE NaNs/payloads, infinities, negative zero,
        // Unicode/astral, empty/max strings and bytes, date/timestamp boundaries.
        for (t, good, bad) in rows() {
            if matches!(t, GoogleType::Bytes(Some(_))) {
                continue;
            }
            let s9 = GoogleMapping::new(t.clone());
            let mut source = Source::new(s9.destination().unwrap().pg_type);
            if let GoogleType::String(n) = t {
                source.length = n;
            }
            if matches!(t, GoogleType::Numeric) {
                source.numeric = Some((38, 9));
            }
            let m = PgSpannerMapping::new(source, d);
            let values: Vec<_> = good.iter().map(|v| s9.forward(v).unwrap()).collect();
            roundtrip(&m, &values);
            array(&m, &values[0]);
            if matches!(
                t,
                GoogleType::Date
                    | GoogleType::Timestamp { .. }
                    | GoogleType::Bytes(None)
                    | GoogleType::String(None)
            ) {
                blocked(&m, &bad);
            }
        }
        for (t, values, limits) in [
            (
                PgType::Int2,
                vec![Value::Int2(i16::MIN), Value::Int2(0), Value::Int2(i16::MAX)],
                (i64::from(i16::MIN) - 1, i64::from(i16::MAX) + 1),
            ),
            (
                PgType::Int4,
                vec![Value::Int4(i32::MIN), Value::Int4(0), Value::Int4(i32::MAX)],
                (i64::from(i32::MIN) - 1, i64::from(i32::MAX) + 1),
            ),
        ] {
            let m = mapping(t, d);
            roundtrip(&m, &values);
            array(&m, &values[0]);
            for n in [limits.0, limits.1] {
                assert!(m.inverse(&Value::Int8(n)).is_err());
            }
            blocked(&m, &[Value::Int8(0)]);
        }
        for t in [
            PgType::Char {
                encoding: "UTF8".into(),
            },
            PgType::Varchar {
                encoding: "UTF8".into(),
            },
        ] {
            let is_char = matches!(t, PgType::Char { .. });
            let m = PgSpannerMapping::new(
                Source {
                    pg_type: t,
                    length: Some(2),
                    numeric: None,
                },
                d,
            );
            let wrap = |s: &str| {
                if is_char {
                    Value::Char(string(s))
                } else {
                    Value::Varchar(string(s))
                }
            };
            roundtrip(&m, &[wrap("é🦀"), wrap("a "), wrap("  ")]);
            array(&m, &wrap("a "));
            blocked(&m, &[wrap("abc"), wrap("\0 ")]);
            if is_char {
                blocked(&m, &[wrap("a")]);
                assert!(m.inverse(&Value::Varchar(string("a"))).is_err());
            }
            let mut invalid = string("a");
            invalid.bytes = vec![255];
            blocked(
                &m,
                &[if is_char {
                    Value::Char(invalid)
                } else {
                    Value::Varchar(invalid)
                }],
            );
        }
        let m = mapping(PgType::Uuid, d);
        roundtrip(&m, &[Value::Uuid([0; 16]), Value::Uuid([255; 16])]);
        array(&m, &Value::Uuid([0; 16]));
        for s in [
            "bad",
            "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
            "00000000000000000000000000000000",
        ] {
            assert!(m.inverse(&Value::Varchar(string(s))).is_err());
        }
        assert_eq!(
            m.destination().unwrap().sql,
            if d == Dialect::GoogleSql {
                "STRING(36)"
            } else {
                "varchar(36)"
            }
        );
        for t in [PgType::Json, PgType::Jsonb] {
            let m = mapping(t.clone(), d);
            let wrap = |s: &str| {
                if t == PgType::Json {
                    Value::Json(s.into())
                } else {
                    Value::Jsonb(Jsonb::parse(s).unwrap())
                }
            };
            let good: Vec<_> = [
                "null",
                "true",
                "\"é🦀\"",
                "{\"a\":[1,0.1,null]}",
                "18446744073709551615",
            ]
            .iter()
            .map(|s| wrap(s))
            .collect();
            roundtrip(&m, &good);
            array(&m, &good[0]);
            if t == PgType::Json {
                for s in [
                    "[ 1 ]",
                    "{\"a\":1,\"a\":2}",
                    "{\"a\":2,\"a\":1}",
                    "-0",
                    "1.00",
                    "\"\\u0000\"",
                ] {
                    blocked(&m, &[Value::Json(s.into())]);
                }
            }
            if d == Dialect::GoogleSql {
                blocked(&m, &[wrap("0.10000000000000001"), wrap("1e400")]);
            } else {
                roundtrip(&m, &[wrap("0.10000000000000001")]);
            }
        }
        blocked(
            &mapping(PgType::Timestamptz, d),
            &[Value::Timestamptz(Timestamp::NegativeInfinity)],
        );
        blocked(
            &mapping(PgType::Date, d),
            &[Value::Date(Date::PositiveInfinity)],
        );
        let timestamps = mapping(PgType::Timestamptz, d);
        for s in [
            "2000-01-01T00:00:00.000000001Z",
            "9999-12-31T23:59:59.999999999Z",
        ] {
            assert!(timestamps.inverse(&Value::Text(string(s))).is_err());
        }
    }
    fn numeric_cases(d: Dialect) {
        let typed = |p, s| {
            PgSpannerMapping::new(
                Source {
                    pg_type: PgType::Numeric,
                    numeric: Some((p, s)),
                    length: None,
                },
                d,
            )
        };
        let m = typed(5, 2);
        roundtrip(
            &m,
            &[numeric("-999.99"), numeric("0.00"), numeric("999.99")],
        );
        blocked(
            &m,
            &[
                numeric("1000.00"),
                numeric("1.001"),
                numeric("1.0"),
                numeric("Infinity"),
                numeric("-Infinity"),
            ],
        );
        assert!(m.inverse(&numeric("1.001")).is_err());
        let m = typed(2, -2);
        roundtrip(&m, &[numeric("-9900"), numeric("0"), numeric("9900")]);
        blocked(&m, &[numeric("9901"), numeric("10000")]);
        let m = typed(2, 4);
        roundtrip(&m, &[numeric("0.0000"), numeric("0.0099")]);
        blocked(&m, &[numeric("0.0100")]);
        let unconstrained = mapping(PgType::Numeric, d);
        if d == Dialect::GoogleSql {
            for m in [
                unconstrained,
                typed(39, 9),
                typed(38, 10),
                typed(29, -1),
                typed(0, 0),
                typed(1001, 0),
            ] {
                assert_eq!(m.classification(), Classification::Unsupported);
                assert!(m.forward(&Value::Null).is_err());
                assert!(m.inverse(&Value::Null).is_err());
            }
            blocked(&typed(38, 9), &[numeric("NaN")]);
        } else {
            roundtrip(
                &unconstrained,
                &[
                    numeric("NaN"),
                    numeric("1.2300"),
                    numeric("1e-16383"),
                    Value::Numeric(Numeric::finite(false, &"9".repeat(147_455), 16_383).unwrap()),
                ],
            );
            roundtrip(&typed(5, 2), &[numeric("NaN")]);
            blocked(&unconstrained, &[numeric("Infinity"), numeric("-Infinity")]);
            let malformed = serde_json::from_value(
                serde_json::json!({"Finite":{"negative":false,"digits":"1","scale":16384}}),
            )
            .unwrap();
            blocked(&unconstrained, &[Value::Numeric(malformed)]);
        }
        assert_eq!(
            typed(5, 2).destination().unwrap().sql,
            if d == Dialect::GoogleSql {
                "NUMERIC"
            } else {
                "numeric"
            }
        );
    }
    fn unsupported_cases(d: Dialect) {
        let id = TypeIdentity {
            schema: "public".into(),
            name: "custom".into(),
            definition_digest: [0; 32],
        };
        let mut types = vec![
            PgType::Timestamp,
            PgType::Time,
            PgType::Timetz,
            PgType::Interval,
            PgType::Enum {
                type_id: id.clone(),
                labels: vec![],
                encoding: "UTF8".into(),
            },
            PgType::Domain {
                type_id: id,
                base: Box::new(PgType::Interval),
                constraint_ref: "check".into(),
            },
            PgType::Text {
                encoding: "LATIN1".into(),
            },
            PgType::Char {
                encoding: "UTF8".into(),
            },
        ];
        types.extend(
            [
                "money",
                "inet",
                "cidr",
                "point",
                "int4range",
                "composite",
                "xml",
                "tsvector",
            ]
            .iter()
            .map(|name| PgType::Other {
                name: (*name).into(),
                known_lossy: false,
            }),
        );
        for t in types {
            let m = mapping(t, d);
            assert_eq!(m.classification(), Classification::Unsupported);
            assert!(!m.is_reversible());
            assert!(
                matches!(m.forward(&Value::Null), Err(MappingError::Unsupported(s)) if !s.is_empty())
            );
            assert!(m.inverse(&Value::Null).is_err());
        }
        for n in [0, super::super::spanner_google::CHARS + 1] {
            let m = PgSpannerMapping::new(
                Source {
                    pg_type: PgType::Varchar {
                        encoding: "UTF8".into(),
                    },
                    length: Some(n),
                    numeric: None,
                },
                d,
            );
            assert_eq!(m.classification(), Classification::Unsupported);
        }
    }
    #[test]
    fn google_matrix() {
        matrix(Dialect::GoogleSql);
    }
    #[test]
    fn pg_matrix() {
        matrix(Dialect::PostgreSql);
    }
    #[test]
    fn google_numeric() {
        numeric_cases(Dialect::GoogleSql);
    }
    #[test]
    fn pg_numeric() {
        numeric_cases(Dialect::PostgreSql);
    }
    #[test]
    fn google_unsupported() {
        unsupported_cases(Dialect::GoogleSql);
    }
    #[test]
    fn pg_unsupported() {
        unsupported_cases(Dialect::PostgreSql);
    }
}
