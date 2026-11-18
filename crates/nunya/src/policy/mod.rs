//! Shared column policy data model.
use crate::schema::ColumnRef;
use std::{collections::BTreeMap, path::PathBuf};
/// Persisted identity-bound private domains.
pub mod bindings;
/// Compile effective column treatments.
pub mod effective;
/// File.
pub mod file;
/// JSON path policy syntax and types.
pub mod json_path;
/// Normalized JSON path declarations and leaf types.
pub use json_path::{JsonLeafType, JsonPathRule, JsonPathTreatment, JsonTokenType, PathSegment};
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
    /// Explicitly permit plaintext values.
    Show,
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
/// Service-level defaults resolved with policy file settings.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServicePolicySettings {
    /// Optional audit destination.
    pub readlog: Option<ReadLogSink>,
    /// Optional maximum schema snapshot age in milliseconds.
    pub snapshot_max_age_ms: Option<u64>,
}
/// Policy.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Policy {
    /// Generated domains retired on this route; their tokens are refused on input.
    #[serde(skip)]
    pub retired_domains: crate::token::RetiredDomains,
    /// Columns released only through a JSON path filter; their treatment stays Masked.
    #[serde(skip)]
    pub json_filters: BTreeMap<ColumnRef, JsonColumnFilter>,
    /// Columns.
    pub columns: BTreeMap<ColumnRef, Treatment>,
    /// Domains.
    pub domains: BTreeMap<DomainName, DomainSettings>,
    /// Maximum permitted schema snapshot age in milliseconds.
    pub snapshot_max_age_ms: u64,
    /// Readlog.
    pub readlog: ReadLogSink,
}
/// Rules and default mode for sanitizing one JSON column.
#[derive(Clone, PartialEq, Eq)]
pub struct JsonColumnFilter {
    /// Path rules in declaration order.
    pub rules: Vec<JsonPathRule>,
    /// Treatment of unlisted members.
    pub mode: crate::json::filter::JsonMode,
}
impl std::fmt::Debug for JsonColumnFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = match self.mode {
            crate::json::filter::JsonMode::Show => "Show",
            crate::json::filter::JsonMode::Tokenize => "Tokenize",
        };
        f.debug_struct("JsonColumnFilter")
            .field("rules", &self.rules)
            .field("mode", &mode)
            .finish()
    }
}

impl Policy {
    /// Return the required JSON output filter for a column.
    pub fn json_filter(&self, c: &ColumnRef) -> Option<&JsonColumnFilter> {
        self.json_filters.get(c)
    }

    /// Return the treatment of a fully qualified column.
    pub fn treatment(&self, c: &ColumnRef) -> Option<&Treatment> {
        self.columns.get(c)
    }
    /// Return the blind domain of a column, if any.
    pub fn domain_of(&self, c: &ColumnRef) -> Option<&DomainName> {
        match self.treatment(c) {
            Some(Treatment::Blind { domain }) => Some(domain),
            Some(Treatment::Masked { .. }) | Some(Treatment::Show) | None => None,
        }
    }
}

/// Default treatment for columns without an explicit rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteDefault {
    /// Tokenize unlisted columns.
    Tokenize,
    /// Show unlisted columns.
    Show,
}
/// Policy file format generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyVersion {
    /// Original policy format.
    Legacy,
    /// Explicit defaults and JSON path rules.
    V2,
}
/// Columns sharing a linked token domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkRule {
    /// Fully qualified linked columns.
    pub fields: Vec<ColumnRef>,
    /// Optional declared domain shared by the fields.
    pub domain: Option<DomainName>,
}
/// Parsed policy together with versioned routing metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyDocument {
    /// Audit destination explicitly declared in the policy file.
    pub file_readlog: Option<ReadLogSink>,
    /// Snapshot age explicitly declared in the policy file.
    pub file_snapshot_max_age_ms: Option<u64>,
    /// Existing column and domain policy.
    pub policy: Policy,
    /// Policy format generation.
    pub version: PolicyVersion,
    /// Treatment of unlisted columns.
    pub route_default: RouteDefault,
    /// Validated JSON path declarations, not yet enforced.
    pub json_paths: Vec<JsonPathRule>,
    /// Linked columns, not yet enforced.
    pub links: Vec<LinkRule>,
}

/// Durable policy revision storage.
pub mod store;
pub use store::{LocalRevisionStore, NewRevision, Owner, PolicyRevision, RevisionKind, RevisionStore, RevisionStoreError, RevisionSummary, WriteOutcome};

/// Value-free policy previews.
pub mod diff;
