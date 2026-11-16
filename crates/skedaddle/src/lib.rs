//! Strict migration definitions and declarative capability reports.
//!
//! The coordinator commits transitions to object storage before local apply or effects.

pub mod budget;
pub mod buffer;
pub mod capability;
pub mod cli;
pub mod coordinator;
pub mod cutover;
pub mod definition;
pub mod engine;
pub mod event;
pub mod fsconfine;
pub mod hooks;
pub mod mapping;
pub mod mysql;
pub mod pg;
pub mod rollback;
pub mod runner;
pub mod security;
pub mod spanner;
pub mod spanner_dest;
pub mod status;
pub mod verify;

pub use capability::{CapabilityItem, CapabilityReport, Criticality, Evidence, ItemStatus};
pub use definition::{DefinitionError, MigrationDefinition, ValidationError};
