//! PostgreSQL connectors (plan P5/P6). P5a owns pgoutput, frame and assembly.

pub mod activate;
pub mod apply;
pub mod assembly;
pub mod catalog;
pub mod chunk;
pub mod ddl;
pub(crate) mod exclusion;
pub mod fence;
pub mod frame;
pub mod pgoutput;
pub mod preflight;
pub mod replication;
pub mod reverse;
pub mod source;
pub mod tls;
