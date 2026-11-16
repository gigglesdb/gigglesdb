//! Shared column policy data model.
use crate::schema::ColumnRef;
use std::{collections::BTreeMap, path::PathBuf};
/// File.
pub mod file;
/// Validate.
pub mod validate;
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
/// Logical token domain name shared by columns with compatible policies.
pub struct DomainName(
    /// Logical domain identifier.
    pub String,
);
/// Treatment.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Treatment {
    /// Hide values using tokens from this domain.
    Blind {
        /// Domain.
        domain: DomainName,
    },
    /// Substitute the placeholder, with None meaning SQL NULL.
    Masked {
        /// Placeholder.
        placeholder: Option<String>,
    },
}
/// Tokenize setting.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum TokenizeSetting {
    #[default]
    /// Off.
    Off,
    /// Enable explicit tokenization with session and principal limits.
    On {
        /// Maximum explicit tokenizations in one session.
        limit_per_session: u32,
        /// Maximum explicit tokenizations per principal window.
        limit_per_principal: u32,
        /// Fixed window length for the principal limit.
        principal_window: std::time::Duration,
    },
}
/// Inclusive format-preserving encryption range in the units of SqlValue.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TypeRange {
    /// Inclusive lower bound.
    pub min: i64,
    /// Inclusive upper bound.
    pub max: i64,
}
/// Domain settings.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DomainSettings {
    /// Tokenize.
    pub tokenize: TokenizeSetting,
    /// Range.
    pub range: Option<TypeRange>,
}
/// Read log sink.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReadLogSink {
    /// Stdout.
    Stdout,
    /// Append read records to a bounded file.
    File {
        /// Path.
        path: PathBuf,
        /// Max bytes.
        max_bytes: u64,
    },
}
/// Policy.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Policy {
    /// Columns.
    pub columns: BTreeMap<ColumnRef, Treatment>,
    /// Domains.
    pub domains: BTreeMap<DomainName, DomainSettings>,
    /// Maximum permitted schema snapshot age in milliseconds.
    pub snapshot_max_age_ms: u64,
    /// Readlog.
    pub readlog: ReadLogSink,
}
impl Policy {
    /// Return the treatment of a fully qualified column.
    pub fn treatment(&self, c: &ColumnRef) -> Option<&Treatment> {
        self.columns.get(c)
    }
    /// Return the blind domain of a column, if any.
    pub fn domain_of(&self, c: &ColumnRef) -> Option<&DomainName> {
        match self.treatment(c) {
            Some(Treatment::Blind { domain }) => Some(domain),
            Some(Treatment::Masked { .. }) | None => None,
        }
    }
}
