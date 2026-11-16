//! Lossless in-memory PostgreSQL values and checked identity mappings.
//!
//! Text codecs accept server output with `DateStyle = ISO`,
//! `IntervalStyle = postgres`, numeric timezone offsets and array delimiter `,`.
//! Floats require `extra_float_digits >= 1`; JSON/JSONB text is UTF-8.
//! They do not guess session settings. Canonical records are independent of
//! these settings and preserve float bits which PostgreSQL text cannot carry.

mod array;
mod json;
pub mod mysql;
mod numeric;
pub mod pg_spanner;
pub mod spanner_google;
pub mod spanner_pg;
mod temporal;
mod text;
mod value;

pub use json::Jsonb;
pub use numeric::Numeric;
pub use temporal::{Date, Timestamp};
pub use value::{Array, ArrayDimension, EncodedBytes, PgType, TypeIdentity, Value};

use std::{error::Error, fmt};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MappingError {
    Invalid(String),
    OutOfDomain(String),
    Unverified(String),
    Unsupported(String),
    LossyText(String),
}

impl fmt::Display for MappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(s) => write!(f, "invalid value: {s}"),
            Self::OutOfDomain(s) => write!(f, "out of domain: {s}"),
            Self::Unverified(s) => write!(f, "unverified mapping: {s}"),
            Self::Unsupported(s) => write!(f, "unsupported mapping: {s}"),
            Self::LossyText(s) => write!(f, "text would lose information: {s}"),
        }
    }
}
impl Error for MappingError {}

pub(crate) fn invalid(s: impl Into<String>) -> MappingError {
    MappingError::Invalid(s.into())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Classification {
    Supported,
    Unverified,
    Unsupported,
}

/// Implementations must reject an out-of-domain value before converting it.
pub trait Mapping {
    fn forward(&self, value: &Value) -> Result<Value, MappingError>;
    fn inverse(&self, value: &Value) -> Result<Value, MappingError>;
    fn domain_check(&self, value: &Value) -> Result<(), MappingError>;
    fn is_reversible(&self) -> bool;
    fn classification(&self) -> Classification;
}

/// The caller supplies actual constraint evaluation, including NULL checks.
/// Merely carrying a constraint reference is never proof of its satisfaction.
pub trait DomainConstraints {
    fn check(
        &self,
        type_id: &TypeIdentity,
        constraint_ref: &str,
        base: &Value,
    ) -> Result<(), MappingError>;
}

/// Identity requires matching storage type descriptors, including enum/domain
/// definitions. Column typmods and collation/provider/version preflight belong
/// to P4b and are not implied here. `new` assumes the same descriptor at both ends.
pub struct IdentityMapping<'a> {
    pg_type: PgType,
    constraints: Option<&'a dyn DomainConstraints>,
}

impl<'a> IdentityMapping<'a> {
    pub fn new(pg_type: PgType) -> Self {
        Self {
            pg_type,
            constraints: None,
        }
    }

    pub fn between(source: PgType, destination: &PgType) -> Result<Self, MappingError> {
        if &source != destination {
            return Err(MappingError::Unsupported("type definitions differ".into()));
        }
        Ok(Self::new(source))
    }

    pub fn with_constraints(mut self, constraints: &'a dyn DomainConstraints) -> Self {
        self.constraints = Some(constraints);
        self
    }

    pub fn pg_type(&self) -> &PgType {
        &self.pg_type
    }
}

impl Mapping for IdentityMapping<'_> {
    fn forward(&self, value: &Value) -> Result<Value, MappingError> {
        self.domain_check(value)?;
        Ok(value.clone())
    }
    fn inverse(&self, value: &Value) -> Result<Value, MappingError> {
        self.forward(value)
    }
    fn domain_check(&self, value: &Value) -> Result<(), MappingError> {
        match self.classification() {
            Classification::Unverified => {
                return Err(MappingError::Unverified(
                    "type or domain evaluator unverified".into(),
                ));
            }
            Classification::Unsupported => {
                return Err(MappingError::Unsupported("known-lossy type".into()));
            }
            Classification::Supported => {}
        }
        self.pg_type.check(value, self.constraints, 0)
    }
    fn is_reversible(&self) -> bool {
        self.classification() == Classification::Supported
    }
    fn classification(&self) -> Classification {
        self.pg_type.classification(self.constraints.is_some())
    }
}
