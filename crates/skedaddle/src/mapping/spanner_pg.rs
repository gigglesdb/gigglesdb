//! Spanner PostgreSQL dialect -> PostgreSQL; safety classification only.
//!
//! | Source -> destination | Classification | Reason |
//! |---|---|---|
//! | bigint / real / double / boolean -> int8 / float4 / float8 / bool | Supported | Exact bits |
//! | numeric -> numeric; jsonb -> jsonb | Supported | Checked Spanner limits; NaN supported, numeric infinities block |
//! | varchar(n) / text / bytea -> same | Supported | Character / cell limits checked |
//! | date / timestamptz / spanner.commit_timestamp -> date / timestamptz | Supported | Checked range and microsecond precision |
//! | T[] -> T[] | Supported | NULL elements retained, one dimension / bound 1 |
//! | Nested arrays / invalid descriptors | Unsupported | No inverse |
//!
//! Timestamp source storage follows spanner_google's canonical nanosecond text
//! contract; it must never be truncated by the connector before mapping.
//! https://cloud.google.com/spanner/docs/reference/postgresql/data-types
//! https://cloud.google.com/spanner/docs/working-with-numerics

use super::spanner_google::{Destination, GoogleMapping, GoogleType};
use super::{Classification, Mapping, MappingError, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpannerPgType {
    Bigint,
    Real,
    DoublePrecision,
    Numeric,
    Varchar(Option<usize>),
    Text,
    Bytea,
    Boolean,
    Date,
    Timestamptz,
    CommitTimestamp,
    Jsonb,
    Array(Box<SpannerPgType>),
}
#[derive(Clone, Debug)]
pub struct PgMapping(GoogleMapping);

pub fn destination(source: &SpannerPgType) -> Result<Destination, MappingError> {
    PgMapping::new(source.clone()).destination()
}
impl PgMapping {
    pub fn new(source: SpannerPgType) -> Self {
        fn google(t: SpannerPgType) -> GoogleType {
            match t {
                SpannerPgType::Bigint => GoogleType::Int64,
                SpannerPgType::Real => GoogleType::Float32,
                SpannerPgType::DoublePrecision => GoogleType::Float64,
                SpannerPgType::Numeric => GoogleType::Numeric,
                SpannerPgType::Varchar(n) => GoogleType::String(n),
                SpannerPgType::Text => GoogleType::String(None),
                SpannerPgType::Bytea => GoogleType::Bytes(None),
                SpannerPgType::Boolean => GoogleType::Bool,
                SpannerPgType::Date => GoogleType::Date,
                SpannerPgType::Timestamptz => GoogleType::Timestamp {
                    allow_commit_timestamp: false,
                },
                SpannerPgType::CommitTimestamp => GoogleType::Timestamp {
                    allow_commit_timestamp: true,
                },
                SpannerPgType::Jsonb => GoogleType::Json,
                SpannerPgType::Array(t) => GoogleType::Array(Box::new(google(*t))),
            }
        }
        Self(GoogleMapping {
            source: google(source),
            postgres: true,
        })
    }
    pub fn destination(&self) -> Result<Destination, MappingError> {
        self.0.destination()
    }
    pub fn source_storage_type(&self) -> Result<super::PgType, MappingError> {
        self.0.source_storage_type()
    }
    pub fn allow_commit_timestamp(&self) -> bool {
        self.0.allow_commit_timestamp()
    }
}
impl Mapping for PgMapping {
    fn forward(&self, value: &Value) -> Result<Value, MappingError> {
        self.0.forward(value)
    }
    fn inverse(&self, value: &Value) -> Result<Value, MappingError> {
        self.0.inverse(value)
    }
    fn domain_check(&self, value: &Value) -> Result<(), MappingError> {
        self.0.domain_check(value)
    }
    fn classification(&self) -> Classification {
        self.0.classification()
    }
    fn is_reversible(&self) -> bool {
        self.0.is_reversible()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::spanner_google::{self, GoogleType};
    use crate::mapping::{Numeric, PgType};
    use spanner_google::tests::{arrays, numeric, roundtrips, rows, text, timestamp_blocks};
    fn pg(t: GoogleType) -> SpannerPgType {
        match t {
            GoogleType::Int64 => SpannerPgType::Bigint,
            GoogleType::Float32 => SpannerPgType::Real,
            GoogleType::Float64 => SpannerPgType::DoublePrecision,
            GoogleType::Numeric => SpannerPgType::Numeric,
            GoogleType::String(Some(n)) => SpannerPgType::Varchar(Some(n)),
            GoogleType::String(None) => SpannerPgType::Text,
            GoogleType::Bytes(_) => SpannerPgType::Bytea,
            GoogleType::Bool => SpannerPgType::Boolean,
            GoogleType::Date => SpannerPgType::Date,
            GoogleType::Timestamp { .. } => SpannerPgType::Timestamptz,
            GoogleType::Json => SpannerPgType::Jsonb,
            GoogleType::Array(t) => SpannerPgType::Array(Box::new(pg(*t))),
        }
    }
    #[test]
    fn matrix_boundaries_and_classifications() {
        for (t, mut good, mut bad) in rows() {
            match &t {
                GoogleType::Numeric => {
                    good.extend([
                        numeric("1e29"),
                        numeric("1e-16383"),
                        Value::Numeric(Numeric::NAN),
                        Value::Numeric(
                            Numeric::finite(false, &"9".repeat(147_455), 16_383).unwrap(),
                        ),
                        Value::Numeric(
                            Numeric::finite(true, &"9".repeat(147_455), 16_383).unwrap(),
                        ),
                    ]);
                    bad = vec![
                        Value::Numeric(Numeric::POSITIVE_INFINITY),
                        Value::Numeric(Numeric::NEGATIVE_INFINITY),
                    ];
                }
                GoogleType::Json => {
                    good = good
                        .iter()
                        .map(|v| {
                            let Value::Json(s) = v else { unreachable!() };
                            Value::Jsonb(crate::mapping::Jsonb::parse(s).unwrap())
                        })
                        .collect();
                    good.push(Value::Jsonb(
                        crate::mapping::Jsonb::parse("0.10000000000000001").unwrap(),
                    ));
                    bad.clear();
                }
                GoogleType::Bytes(Some(_)) => continue,
                _ => {}
            }
            let m = PgMapping::new(pg(t));
            roundtrips(&m, &good, &bad);
            arrays(&m.0, &good[0]);
        }
        assert_eq!(destination(&SpannerPgType::Numeric).unwrap().sql, "numeric");
        assert_eq!(
            destination(&SpannerPgType::Varchar(None)).unwrap().pg_type,
            PgType::Text {
                encoding: "UTF8".into()
            }
        );
    }
    #[test]
    fn inverse_numeric_overflow_blocks() {
        let m = PgMapping::new(SpannerPgType::Numeric);
        for (digits, scale) in [("9".repeat(147_456), 16_383), ("1".into(), 16_384)] {
            let n: Numeric = serde_json::from_value(serde_json::json!({"Finite": {"negative": false, "digits": digits, "scale": scale}})).unwrap();
            assert!(m.inverse(&Value::Numeric(n)).is_err());
        }
    }
    #[test]
    fn timestamps_and_nested_arrays_block() {
        for t in [SpannerPgType::Timestamptz, SpannerPgType::CommitTimestamp] {
            let m = PgMapping::new(t.clone());
            timestamp_blocks(&m);
            assert_eq!(
                m.allow_commit_timestamp(),
                t == SpannerPgType::CommitTimestamp
            );
            roundtrips(&m, &[text("9999-12-31T23:59:59.999999000Z")], &[]);
        }
        let m = PgMapping::new(SpannerPgType::Array(Box::new(SpannerPgType::Array(
            Box::new(SpannerPgType::Bigint),
        ))));
        assert_eq!(m.classification(), Classification::Unsupported);
        assert!(m.inverse(&Value::Null).is_err());
    }
}
