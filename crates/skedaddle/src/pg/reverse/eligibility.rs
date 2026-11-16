use std::{cell::RefCell, rc::Rc};

use serde::{Deserialize, Serialize};

use crate::{
    mapping::{Classification, Mapping, MappingError, Value},
    pg::{
        pgoutput::{Relation, RelationColumn},
        source::{self, ColumnMapping, TypeResolver},
    },
};

/// Stable, typed journal/status reasons. Values themselves are never recorded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Violation {
    Value {
        reason: MappingFailure,
    },
    UnsupportedDdl {
        reason: String,
    },
    NewType {
        relation: String,
        column: String,
    },
    Extension {
        name: String,
    },
    IrreversibleMapping,
    #[serde(rename = "destination-ddl")]
    DestinationDdl,
    #[serde(rename = "destination-catalog-drift")]
    DestinationCatalogDrift,
    #[serde(rename = "activation-baseline-unavailable")]
    ActivationBaselineUnavailable,
    #[serde(rename = "reverse-capture-unverifiable")]
    ReverseCaptureUnverifiable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MappingFailure {
    Invalid,
    OutOfDomain,
    Unverified,
    Unsupported,
    LossyText,
}
impl From<&MappingError> for MappingFailure {
    fn from(error: &MappingError) -> Self {
        match error {
            MappingError::Invalid(_) => Self::Invalid,
            MappingError::OutOfDomain(_) => Self::OutOfDomain,
            MappingError::Unverified(_) => Self::Unverified,
            MappingError::Unsupported(_) => Self::Unsupported,
            MappingError::LossyText(_) => Self::LossyText,
        }
    }
}

type Journal = Box<dyn FnMut(&Violation) -> Result<(), String>>;
struct State {
    first: Option<Violation>,
    journaled: bool,
    journal: Journal,
}

/// Shared by all column mappings and capture. The latch closes BEFORE trying
/// the journal; a journal failure cannot reopen eligibility. Retry `flush` and
/// persist/restore the first reason before resuming any work after a restart.
#[derive(Clone)]
pub struct Eligibility(Rc<RefCell<State>>);
impl Eligibility {
    pub fn new(journal: impl FnMut(&Violation) -> Result<(), String> + 'static) -> Self {
        Self::restore(None, journal)
    }
    pub fn restore(
        first: Option<Violation>,
        journal: impl FnMut(&Violation) -> Result<(), String> + 'static,
    ) -> Self {
        Self(Rc::new(RefCell::new(State {
            journaled: first.is_some(),
            first,
            journal: Box::new(journal),
        })))
    }
    pub fn first_violation(&self) -> Option<Violation> {
        self.0.borrow().first.clone()
    }
    pub fn eligible(&self) -> bool {
        self.0.borrow().first.is_none()
    }
    pub fn flush(&self) -> Result<(), MappingError> {
        let mut state = self.0.borrow_mut();
        if !state.journaled
            && let Some(reason) = state.first.clone()
        {
            (state.journal)(&reason)
                .map_err(|_| MappingError::Unverified("eligibility journal unavailable".into()))?;
            state.journaled = true;
        }
        Ok(())
    }
    pub fn end(&self, reason: Violation) -> Result<(), MappingError> {
        {
            let mut state = self.0.borrow_mut();
            if state.first.is_none() {
                state.first = Some(reason);
            }
        }
        self.flush()
    }
    pub fn ensure(&self) -> Result<(), MappingError> {
        self.flush()?;
        if !self.eligible() {
            return Err(MappingError::OutOfDomain(
                "rollback eligibility permanently ended".into(),
            ));
        }
        Ok(())
    }
    pub fn check(&self, inverse: &dyn Mapping, value: &Value) -> Result<(), MappingError> {
        self.ensure()?;
        if !inverse.is_reversible() {
            self.end(Violation::IrreversibleMapping)?;
            return Err(MappingError::Unsupported(
                "inverse mapping is not reversible".into(),
            ));
        }
        if let Err(reason) = inverse.domain_check(value) {
            self.end(Violation::Value {
                reason: (&reason).into(),
            })?;
            return Err(reason);
        }
        Ok(())
    }
    /// For consumers of buffered reverse DDL. New type/extension operations
    /// fail independently of whether a subsequent row happens to use them.
    pub fn check_ddl(
        &self,
        _schemas: &mut crate::pg::ddl::SchemaVersions,
        records: &[crate::pg::ddl::DdlRecord],
    ) -> Result<(), MappingError> {
        self.ensure()?;
        if !records.is_empty() {
            self.end(Violation::DestinationDdl)?;
            return self.ensure();
        }
        Ok(())
    }
}

/// The supplied mapping's domain is the destination (reverse input) domain.
/// Its inverse maps destination values back to the old source. It must be
/// qualified in this direction; a forward-only domain predicate is insufficient.
pub struct InverseMapping {
    mapping: Box<dyn Mapping>,
    eligibility: Eligibility,
}
impl InverseMapping {
    pub fn new(mapping: Box<dyn Mapping>, eligibility: Eligibility) -> Self {
        Self {
            mapping,
            eligibility,
        }
    }
}
impl Mapping for InverseMapping {
    fn forward(&self, value: &Value) -> Result<Value, MappingError> {
        self.domain_check(value)?;
        match self.mapping.inverse(value) {
            Ok(v) => Ok(v),
            Err(reason) => {
                self.eligibility.end(Violation::Value {
                    reason: (&reason).into(),
                })?;
                Err(reason)
            }
        }
    }
    fn inverse(&self, value: &Value) -> Result<Value, MappingError> {
        self.domain_check(value)?;
        self.mapping.forward(value)
    }
    fn domain_check(&self, value: &Value) -> Result<(), MappingError> {
        self.eligibility.check(self.mapping.as_ref(), value)
    }
    fn is_reversible(&self) -> bool {
        self.mapping.is_reversible() && self.eligibility.eligible()
    }
    fn classification(&self) -> Classification {
        self.mapping.classification()
    }
}

/// Constructs a capture-facing mapping using the capture's shared latch.
pub type MappingConstructor = fn(Box<dyn Mapping>, Eligibility) -> Box<dyn Mapping>;

/// Opt-in for a qualified resolver whose forward method already produces the
/// desired reverse event value. Unlike InverseMapping this never inverts it.
pub struct ValidatedMapping {
    mapping: Box<dyn Mapping>,
    eligibility: Eligibility,
}
impl ValidatedMapping {
    pub fn boxed(mapping: Box<dyn Mapping>, eligibility: Eligibility) -> Box<dyn Mapping> {
        Box::new(Self {
            mapping,
            eligibility,
        })
    }
}
impl Mapping for ValidatedMapping {
    fn forward(&self, value: &Value) -> Result<Value, MappingError> {
        self.domain_check(value)?;
        self.mapping.forward(value).or_else(|reason| {
            self.eligibility.end(Violation::Value {
                reason: (&reason).into(),
            })?;
            Err(reason)
        })
    }
    fn inverse(&self, value: &Value) -> Result<Value, MappingError> {
        self.domain_check(value)?;
        self.mapping.inverse(value)
    }
    fn domain_check(&self, value: &Value) -> Result<(), MappingError> {
        self.eligibility.check(self.mapping.as_ref(), value)
    }
    fn is_reversible(&self) -> bool {
        self.mapping.is_reversible() && self.eligibility.eligible()
    }
    fn classification(&self) -> Classification {
        self.mapping.classification()
    }
}
fn inverse_mapping(mapping: Box<dyn Mapping>, eligibility: Eligibility) -> Box<dyn Mapping> {
    Box::new(InverseMapping::new(mapping, eligibility))
}

pub struct ReverseResolver<R> {
    resolver: R,
    eligibility: Eligibility,
    construct: MappingConstructor,
}
impl<R> ReverseResolver<R> {
    pub fn new(resolver: R, eligibility: Eligibility) -> Self {
        Self::with_mapping(resolver, eligibility, inverse_mapping)
    }
    pub fn with_mapping(
        resolver: R,
        eligibility: Eligibility,
        construct: MappingConstructor,
    ) -> Self {
        Self {
            resolver,
            eligibility,
            construct,
        }
    }
}
impl<R: TypeResolver> TypeResolver for ReverseResolver<R> {
    fn included_columns(
        &mut self,
        relation: &Relation<'_>,
    ) -> source::Result<Option<std::collections::BTreeSet<String>>> {
        self.eligibility.ensure().map_err(source::Error::Mapping)?;
        self.resolver.included_columns(relation)
    }
    fn resolve(
        &mut self,
        relation: &Relation<'_>,
        column: &RelationColumn<'_>,
    ) -> source::Result<ColumnMapping> {
        self.eligibility.ensure().map_err(source::Error::Mapping)?;
        match self.resolver.resolve(relation, column) {
            Ok(value) => Ok(ColumnMapping {
                pg_type: value.pg_type,
                mapping: (self.construct)(value.mapping, self.eligibility.clone()),
            }),
            Err(e) => {
                self.eligibility
                    .end(Violation::NewType {
                        relation: format!("{}.{}", relation.namespace, relation.name),
                        column: column.name.into(),
                    })
                    .map_err(source::Error::Mapping)?;
                Err(e)
            }
        }
    }
}
