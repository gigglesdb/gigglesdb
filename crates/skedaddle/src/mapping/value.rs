use super::temporal::{Date, Timestamp};
use super::{Classification, DomainConstraints, Jsonb, MappingError, Numeric, invalid};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Definition fingerprint is supplied by catalog preflight, not an OID (OIDs
/// differ across databases). Includes enum labels or domain definition as needed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeIdentity {
    pub schema: String,
    pub name: String,
    pub definition_digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncodedBytes {
    pub bytes: Vec<u8>,
    /// Catalog encoding identity, e.g. UTF8, LATIN1 or SQL_ASCII.
    pub encoding: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgType {
    Bool,
    Int2,
    Int4,
    Int8,
    Numeric,
    Float4,
    Float8,
    Text {
        encoding: String,
    },
    Varchar {
        encoding: String,
    },
    Char {
        encoding: String,
    },
    Bytea,
    Uuid,
    Date,
    Time,
    Timetz,
    Timestamp,
    Timestamptz,
    Interval,
    Json,
    Jsonb,
    Array(Box<PgType>),
    Enum {
        type_id: TypeIdentity,
        labels: Vec<Vec<u8>>,
        encoding: String,
    },
    Domain {
        type_id: TypeIdentity,
        base: Box<PgType>,
        constraint_ref: String,
    },
    /// Unknown extensions are unverified; known-lossy mappings are unsupported.
    Other {
        name: String,
        known_lossy: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrayDimension {
    pub length: u32,
    pub lower_bound: i32,
}

/// Row-major elements. PostgreSQL arrays have at most six dimensions. Empty
/// arrays have no dimensions; array NULL elements remain explicit Values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Array {
    pub element_type: Box<PgType>,
    pub dimensions: Vec<ArrayDimension>,
    pub elements: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Numeric(Numeric),
    /// IEEE bits, never host float equality (NaN payloads and -0 survive).
    Float4(u32),
    Float8(u64),
    Text(EncodedBytes),
    Varchar(EncodedBytes),
    Char(EncodedBytes),
    Bytea(Vec<u8>),
    Uuid([u8; 16]),
    Date(Date),
    Time(i64),
    Timetz {
        micros: i64,
        offset_east: i32,
    },
    Timestamp(Timestamp),
    Timestamptz(Timestamp),
    Interval {
        months: i32,
        days: i32,
        micros: i64,
    },
    /// Original UTF-8 JSON text, including whitespace and duplicate keys.
    Json(String),
    Jsonb(Jsonb),
    Array(Array),
    Enum {
        type_id: TypeIdentity,
        label: EncodedBytes,
    },
    Domain {
        type_id: TypeIdentity,
        constraint_ref: String,
        base: Box<Value>,
    },
}

impl Value {
    pub fn parse_pg_text(pg_type: &PgType, text: Option<&[u8]>) -> Result<Self, MappingError> {
        super::text::parse(pg_type, text, 0)
    }

    pub fn format_pg_text(&self, pg_type: &PgType) -> Result<Option<Vec<u8>>, MappingError> {
        super::text::format(self, pg_type, 0)
    }

    /// Version 1: `SKVA\0\x01` followed by compact serde JSON. Enum tags,
    /// declaration-order fields, integer decimal spelling, UTF-8 strings and
    /// serde JSON escaping are fixed for this version; no host floats occur.
    /// Jsonb is normalized before storage. Maps are ordered. This is a typed
    /// verification format, not PG text/binary or the event envelope format.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, MappingError> {
        self.validate(0)?;
        let mut out = b"SKVA\0\x01".to_vec();
        serde_json::to_writer(&mut out, self).map_err(|e| invalid(e.to_string()))?;
        Ok(out)
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, MappingError> {
        let body = bytes
            .strip_prefix(b"SKVA\0\x01")
            .ok_or_else(|| invalid("canonical version"))?;
        let value: Self = serde_json::from_slice(body).map_err(|e| invalid(e.to_string()))?;
        // Reject whitespace, duplicate/unknown fields and noncanonical numbers.
        if value.canonical_bytes()? != bytes {
            return Err(invalid("noncanonical value encoding"));
        }
        Ok(value)
    }

    pub fn digest(&self) -> Result<[u8; 32], MappingError> {
        Ok(Sha256::digest(self.canonical_bytes()?).into())
    }

    pub(crate) fn validate(&self, depth: usize) -> Result<(), MappingError> {
        if depth > 32 {
            return Err(invalid("value nesting exceeds 32"));
        }
        match self {
            Self::Numeric(n) => n.validate()?,
            Self::Text(t) | Self::Varchar(t) | Self::Char(t) | Self::Enum { label: t, .. } => {
                if t.encoding.is_empty() || t.bytes.contains(&0) {
                    return Err(invalid("text encoding/NUL"));
                }
                if t.encoding == "UTF8" && std::str::from_utf8(&t.bytes).is_err() {
                    return Err(invalid("UTF8 text"));
                }
            }
            Self::Time(t) | Self::Timetz { micros: t, .. } => {
                if !(0..=super::temporal::DAY).contains(t) {
                    return Err(invalid("time range"));
                }
                if let Self::Timetz { offset_east, .. } = self
                    && offset_east.unsigned_abs() >= 57_600
                {
                    return Err(invalid("timezone range"));
                }
            }
            Self::Date(d) => d.validate()?,
            Self::Timestamp(t) | Self::Timestamptz(t) => t.validate()?,
            Self::Json(s) => {
                super::json::validate_json(s)?;
            }
            Self::Jsonb(j) => j.validate()?,
            Self::Array(a) => {
                if a.dimensions.len() > 6 {
                    return Err(invalid("array dimensions exceed PG limit"));
                }
                let mut count = usize::from(!a.dimensions.is_empty());
                for d in &a.dimensions {
                    if d.length == 0
                        || i64::from(d.lower_bound) + i64::from(d.length) - 1 > i64::from(i32::MAX)
                    {
                        return Err(invalid("array bounds"));
                    }
                    count = count
                        .checked_mul(d.length as usize)
                        .ok_or_else(|| invalid("array size"))?;
                }
                if count != a.elements.len() {
                    return Err(invalid("array shape"));
                }
                for e in &a.elements {
                    e.validate(depth + 1)?;
                    a.element_type.check(e, None, depth + 1).or_else(|error| {
                        // Constraint evaluation is deferred to Mapping, not serialization.
                        if matches!(error, MappingError::Unverified(_)) {
                            Ok(())
                        } else {
                            Err(error)
                        }
                    })?;
                }
            }
            Self::Domain {
                base,
                constraint_ref,
                ..
            } => {
                if constraint_ref.is_empty() {
                    return Err(invalid("empty constraint reference"));
                }
                if base.as_ref() == &Self::Null {
                    return Err(invalid("use Value::Null for a NULL domain"));
                }
                base.validate(depth + 1)?;
            }
            _ => {}
        }
        Ok(())
    }
}

impl PgType {
    pub fn classification(&self, constraints_available: bool) -> Classification {
        match self {
            Self::Other {
                known_lossy: true, ..
            } => Classification::Unsupported,
            Self::Other { .. } => Classification::Unverified,
            Self::Array(t) => t.classification(constraints_available),
            Self::Domain { base, .. } => {
                let c = base.classification(constraints_available);
                if c == Classification::Supported && !constraints_available {
                    Classification::Unverified
                } else {
                    c
                }
            }
            _ => Classification::Supported,
        }
    }

    pub(crate) fn check(
        &self,
        value: &Value,
        constraints: Option<&dyn DomainConstraints>,
        depth: usize,
    ) -> Result<(), MappingError> {
        if depth > 32 {
            return Err(invalid("type nesting exceeds 32"));
        }
        value.validate(depth)?;
        if let Self::Other { name, known_lossy } = self {
            return Err(if *known_lossy {
                MappingError::Unsupported(name.clone())
            } else {
                MappingError::Unverified(name.clone())
            });
        }
        if let Self::Domain {
            type_id,
            base,
            constraint_ref,
        } = self
        {
            let inner = match value {
                Value::Null => value,
                Value::Domain {
                    type_id: id,
                    constraint_ref: r,
                    base: v,
                } if id == type_id && r == constraint_ref => v,
                _ => return Err(MappingError::OutOfDomain("domain identity".into())),
            };
            base.check(inner, constraints, depth + 1)?;
            return constraints
                .ok_or_else(|| MappingError::Unverified("domain evaluator missing".into()))?
                .check(type_id, constraint_ref, inner);
        }
        if value == &Value::Null {
            return Ok(());
        }
        let matches = match (self, value) {
            (Self::Bool, Value::Bool(_))
            | (Self::Int2, Value::Int2(_))
            | (Self::Int4, Value::Int4(_))
            | (Self::Int8, Value::Int8(_))
            | (Self::Numeric, Value::Numeric(_))
            | (Self::Float4, Value::Float4(_))
            | (Self::Float8, Value::Float8(_))
            | (Self::Bytea, Value::Bytea(_))
            | (Self::Uuid, Value::Uuid(_))
            | (Self::Date, Value::Date(_))
            | (Self::Time, Value::Time(_))
            | (Self::Timetz, Value::Timetz { .. })
            | (Self::Timestamp, Value::Timestamp(_))
            | (Self::Timestamptz, Value::Timestamptz(_))
            | (Self::Interval, Value::Interval { .. })
            | (Self::Json, Value::Json(_))
            | (Self::Jsonb, Value::Jsonb(_)) => true,
            (Self::Text { encoding }, Value::Text(t))
            | (Self::Varchar { encoding }, Value::Varchar(t))
            | (Self::Char { encoding }, Value::Char(t)) => encoding == &t.encoding,
            (
                Self::Enum {
                    type_id,
                    labels,
                    encoding,
                },
                Value::Enum { type_id: id, label },
            ) => id == type_id && encoding == &label.encoding && labels.contains(&label.bytes),
            (Self::Array(t), Value::Array(a)) if t == &a.element_type => {
                for v in &a.elements {
                    t.check(v, constraints, depth + 1)?;
                }
                true
            }
            _ => false,
        };
        if matches {
            Ok(())
        } else {
            Err(MappingError::OutOfDomain(
                "type, encoding, enum label or definition mismatch".into(),
            ))
        }
    }
}
