//! Strict migration definitions and declarative capability reports.

pub mod capability;
pub mod coordinator;
pub mod definition;

pub use capability::{CapabilityItem, CapabilityReport, Criticality, Evidence, ItemStatus};
pub use definition::{DefinitionError, MigrationDefinition, ValidationError};
