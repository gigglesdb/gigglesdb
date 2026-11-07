//! Strict migration definitions and declarative capability reports.

pub mod buffer;
pub mod capability;
pub mod coordinator;
pub mod definition;
pub mod event;

pub use capability::{CapabilityItem, CapabilityReport, Criticality, Evidence, ItemStatus};
pub use definition::{DefinitionError, MigrationDefinition, ValidationError};
