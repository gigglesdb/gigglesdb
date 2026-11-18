//! Strict policy file parsing with data-free diagnostics.
use super::{
    DomainName, DomainSettings, JsonLeafType, JsonPathRule, JsonPathTreatment, JsonTokenType,
    LinkRule, Policy, PolicyDocument, PolicyVersion, ReadLogSink, RouteDefault,
    ServicePolicySettings, TokenizeSetting, Treatment, TypeRange,
};
use crate::schema::ColumnRef;
use serde::{Deserialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::{Path, PathBuf},
};
use toml::Value;

/// A policy problem containing only object names and a fixed explanation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyError {
    /// Human-readable, sanitized explanation.
    pub message: String,
}
impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for PolicyError {}

pub(super) fn error(errors: &mut Vec<PolicyError>, context: &str, problem: &str) {
    errors.push(PolicyError {
        message: format!("{context}: {problem}"),
    });
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawPolicy {
    version: Option<Value>,
    default: Option<Value>,
    json_path: Option<Value>,
    link: Option<Value>,
    snapshot_max_age_ms: Option<Value>,
    readlog: Option<Value>,
    domains: Option<Value>,
    column: Option<Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLog {
    sink: Option<Value>,
    path: Option<Value>,
    max_bytes: Option<Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDomain {
    tokenize: Option<Value>,
    tokenize_limit_per_session: Option<Value>,
    tokenize_limit_per_principal: Option<Value>,
    tokenize_principal_window: Option<Value>,
    range: Option<Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRange {
    min: Option<Value>,
    max: Option<Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawColumn {
    show: Option<Value>,
    table: Option<Value>,
    name: Option<Value>,
    blind: Option<Value>,
    masked: Option<Value>,
    placeholder: Option<Value>,
}

// Validate each table separately so one malformed section cannot hide other errors.
fn table<T: DeserializeOwned>(
    value: Value,
    allowed: &[&str],
    context: &str,
    errors: &mut Vec<PolicyError>,
) -> Option<T> {
    let Value::Table(mut fields) = value else {
        error(errors, context, "expected a table");
        return None;
    };
    fields.retain(|key, _| {
        let known = allowed.contains(&key);
        if !known {
            error(errors, context, "unknown key");
        }
        known
    });
    match Value::Table(fields).try_into() {
        Ok(value) => Some(value),
        Err(_) => {
            error(errors, context, "invalid table");
            None
        }
    }
}
fn field<T: DeserializeOwned>(
    value: Option<Value>,
    context: &str,
    problem: &str,
    errors: &mut Vec<PolicyError>,
) -> Option<T> {
    value.and_then(|value| match value.try_into() {
        Ok(value) => Some(value),
        Err(_) => {
            error(errors, context, problem);
            None
        }
    })
}
fn required_string(
    value: Option<Value>,
    context: &str,
    problem: &str,
    errors: &mut Vec<PolicyError>,
) -> Option<String> {
    if value.is_none() {
        error(errors, context, problem);
        return None;
    }
    field(value, context, problem, errors)
}
fn domain_name_valid(name: &str) -> bool {
    (1..=31).contains(&name.len())
        && name.as_bytes()[0].is_ascii_lowercase()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}
pub(crate) fn parse_log(value: Value, errors: &mut Vec<PolicyError>) -> ReadLogSink {
    let Some(raw): Option<RawLog> = table(value, &["sink", "path", "max_bytes"], "readlog", errors)
    else {
        return ReadLogSink::Stdout;
    };
    let sink = required_string(raw.sink, "readlog", "sink must be stdout or file", errors);
    let path: Option<String> = field(raw.path, "readlog", "path must be a string", errors);
    let max_bytes: Option<u64> = field(
        raw.max_bytes,
        "readlog",
        "max_bytes must be a positive integer",
        errors,
    );
    let max_bytes = max_bytes.unwrap_or(100 * 1024 * 1024);
    if max_bytes == 0 {
        error(errors, "readlog", "max_bytes must be positive");
    }
    match sink.as_deref() {
        Some("stdout") => ReadLogSink::Stdout,
        Some("file") => match path {
            Some(path) if !path.is_empty() => ReadLogSink::File {
                path: PathBuf::from(path),
                max_bytes,
            },
            _ => {
                error(errors, "readlog", "file sink requires path");
                ReadLogSink::Stdout
            }
        },
        Some(_) => {
            error(errors, "readlog", "sink must be stdout or file");
            ReadLogSink::Stdout
        }
        None => ReadLogSink::Stdout,
    }
}
fn tokenize_limit(
    value: Option<Value>,
    max: u32,
    context: &str,
    missing: &str,
    invalid: &str,
    errors: &mut Vec<PolicyError>,
) -> Option<u32> {
    let Some(value) = value else {
        error(errors, context, missing);
        return None;
    };
    let limit = value
        .as_integer()
        .and_then(|n| u32::try_from(n).ok())
        .filter(|n| (1..=max).contains(n));
    if limit.is_none() {
        error(errors, context, invalid);
    }
    limit
}
fn parse_domains(
    value: Option<Value>,
    errors: &mut Vec<PolicyError>,
) -> BTreeMap<DomainName, DomainSettings> {
    let mut result = BTreeMap::new();
    let Some(value) = value else {
        return result;
    };
    let Value::Table(domains) = value else {
        error(errors, "domains", "expected a table");
        return result;
    };
    for (name, value) in domains {
        let context = format!("domain {name}");
        if !domain_name_valid(&name) {
            error(errors, &context, "invalid domain name");
        }
        let Some(raw): Option<RawDomain> = table(
            value,
            &[
                "tokenize",
                "tokenize_limit_per_session",
                "tokenize_limit_per_principal",
                "tokenize_principal_window",
                "range",
            ],
            &context,
            errors,
        ) else {
            continue;
        };
        let tokenize: Option<String> =
            field(raw.tokenize, &context, "tokenize must be off or on", errors);
        let tokenize = match tokenize.as_deref() {
            None | Some("off") => {
                for present in [
                    raw.tokenize_limit_per_session.is_some(),
                    raw.tokenize_limit_per_principal.is_some(),
                    raw.tokenize_principal_window.is_some(),
                ] {
                    if present {
                        error(errors, &context, "tokenize limits require tokenize on");
                    }
                }
                TokenizeSetting::Off
            }
            Some("on") => {
                let session = tokenize_limit(
                    raw.tokenize_limit_per_session,
                    100000,
                    &context,
                    "missing tokenize_limit_per_session",
                    "tokenize session limit must be in 1..=100000",
                    errors,
                );
                let principal = tokenize_limit(
                    raw.tokenize_limit_per_principal,
                    100000000,
                    &context,
                    "missing tokenize_limit_per_principal",
                    "tokenize principal limit must be in 1..=100000000",
                    errors,
                );
                let window = match raw.tokenize_principal_window {
                    None => {
                        error(errors, &context, "missing tokenize_principal_window");
                        None
                    }
                    Some(value) => {
                        let window = value
                            .as_str()
                            .and_then(crate::config::parse_duration)
                            .filter(|window| (60..=31 * 86400).contains(&window.as_secs()));
                        if window.is_none() {
                            error(
                                errors,
                                &context,
                                "tokenize principal window must be a duration in 1m..=31d",
                            );
                        }
                        window
                    }
                };
                match (session, principal, window) {
                    (Some(session), Some(principal), _) if principal < session => {
                        error(
                            errors,
                            &context,
                            "tokenize principal limit must be at least session limit",
                        );
                        TokenizeSetting::Off
                    }
                    (
                        Some(limit_per_session),
                        Some(limit_per_principal),
                        Some(principal_window),
                    ) => TokenizeSetting::On {
                        limit_per_session,
                        limit_per_principal,
                        principal_window,
                    },
                    _ => TokenizeSetting::Off,
                }
            }
            Some(_) => {
                error(errors, &context, "tokenize must be off or on");
                TokenizeSetting::Off
            }
        };
        let range = raw.range.and_then(|value| {
            let raw: RawRange = table(value, &["min", "max"], &context, errors)?;
            if raw.min.is_none() {
                error(errors, &context, "range requires min");
            }
            if raw.max.is_none() {
                error(errors, &context, "range requires max");
            }
            let min: Option<i64> = field(raw.min, &context, "range min must be an integer", errors);
            let max: Option<i64> = field(raw.max, &context, "range max must be an integer", errors);
            match (min, max) {
                (Some(min), Some(max)) => {
                    if min > max {
                        error(errors, &context, "range min exceeds max");
                    }
                    Some(TypeRange { min, max })
                }
                _ => None,
            }
        });
        result.insert(DomainName(name), DomainSettings { tokenize, range });
    }
    result
}

/// Parse TOML while collecting independent policy problems without echoing values.
pub fn parse_policy(toml_text: &str) -> Result<Policy, Vec<PolicyError>> {
    parse_policy_document(toml_text)
        .and_then(|doc| resolve(doc, &ServicePolicySettings::default()))
        .map(|document| document.policy)
}

/// Parse legacy or version 2 policy declarations with sanitized diagnostics.
pub fn parse_policy_document(toml_text: &str) -> Result<PolicyDocument, Vec<PolicyError>> {
    let value: Value = toml::from_str(toml_text).map_err(|_| {
        vec![PolicyError {
            message: "policy: invalid TOML syntax".into(),
        }]
    })?;
    let v2 = match value.get("version") {
        None => false,
        Some(Value::Integer(2)) => true,
        Some(_) => {
            return Err(vec![PolicyError {
                message: "policy: version must be 2".into(),
            }]);
        }
    };
    let mut errors = vec![];
    if !v2 && value.get("link").is_some() {
        error(&mut errors, "link", "link requires version = 2");
    }
    let allowed: &[&str] = if v2 {
        &[
            "version",
            "default",
            "json_path",
            "link",
            "snapshot_max_age_ms",
            "readlog",
            "domains",
            "column",
        ]
    } else {
        &["snapshot_max_age_ms", "readlog", "domains", "column"]
    };
    let Some(raw): Option<RawPolicy> = table(value, allowed, "policy", &mut errors) else {
        return Err(errors);
    };
    let route_default = if v2 {
        match raw.default.as_ref().and_then(Value::as_str) {
            Some("tokenize") => RouteDefault::Tokenize,
            Some("show") => RouteDefault::Show,
            _ => {
                error(&mut errors, "policy", "default must be tokenize or show");
                RouteDefault::Tokenize
            }
        }
    } else {
        RouteDefault::Show
    };
    // Kept in the raw shape to distinguish a forbidden key from an unknown key.
    let _ = raw.version;
    if v2 {
        if raw.readlog.is_some() {
            error(&mut errors, "readlog", "moved to the service config");
        }
        if raw.snapshot_max_age_ms.is_some() {
            error(
                &mut errors,
                "snapshot_max_age_ms",
                "moved to the service config",
            );
        }
    }
    let age: Option<u64> = if v2 {
        None
    } else {
        field(
            raw.snapshot_max_age_ms,
            "policy",
            "snapshot_max_age_ms must be a nonnegative integer",
            &mut errors,
        )
    };
    let readlog = if v2 { None } else {
        raw.readlog.map(|value| parse_log(value, &mut errors))
    };
    let domains = parse_domains(raw.domains, &mut errors);
    let mut columns = BTreeMap::new();
    let mut seen = BTreeSet::new();
    let values = match raw.column {
        None => vec![],
        Some(Value::Array(values)) => values,
        Some(_) => {
            error(&mut errors, "columns", "expected an array of tables");
            vec![]
        }
    };
    for value in values {
        let initial_context = match &value {
            Value::Table(fields) => format!(
                "column {}.{}",
                fields.get("table").and_then(Value::as_str).unwrap_or("?"),
                fields.get("name").and_then(Value::as_str).unwrap_or("?")
            ),
            _ => "column".into(),
        };
        let Some(raw): Option<RawColumn> = table(
            value,
            if v2 {
                &["table", "name", "blind", "masked", "placeholder", "show"]
            } else {
                &["table", "name", "blind", "masked", "placeholder"]
            },
            &initial_context,
            &mut errors,
        ) else {
            continue;
        };
        let table = required_string(
            raw.table,
            &initial_context,
            "table must be schema.table",
            &mut errors,
        );
        let name = required_string(
            raw.name,
            &initial_context,
            "name must be a string",
            &mut errors,
        );
        let context = format!(
            "column {}.{}",
            table.as_deref().unwrap_or("?"),
            name.as_deref().unwrap_or("?")
        );
        let column = table.as_deref().and_then(|table| {
            let parts: Vec<_> = table.split('.').collect();
            if parts.len() != 2 || parts[1].is_empty() {
                error(&mut errors, &context, "table must be schema.table");
                return None;
            }
            name.as_ref().map(|name| ColumnRef {
                schema: parts[0].into(),
                table: parts[1].into(),
                column: name.clone(),
            })
        });
        if let Some(column) = &column
            && !seen.insert(column.clone())
        {
            error(&mut errors, &context, "column listed twice");
        }
        let has_show = raw.show.is_some();
        let show: Option<bool> = field(raw.show, &context, "show must be true", &mut errors);
        if has_show && show != Some(true) {
            error(&mut errors, &context, "show must be true");
        }
        let has_blind = raw.blind.is_some();
        let has_masked = raw.masked.is_some();
        let blind: Option<String> =
            field(raw.blind, &context, "blind must name a domain", &mut errors);
        let masked: Option<bool> = field(raw.masked, &context, "masked must be true", &mut errors);
        let has_placeholder = raw.placeholder.is_some();
        let placeholder: Option<String> = field(
            raw.placeholder,
            &context,
            "placeholder must be a string",
            &mut errors,
        );
        if v2 && has_placeholder && !has_masked {
            error(
                &mut errors,
                &context,
                "placeholder requires masked treatment",
            );
        }
        if if v2 {
            usize::from(has_blind) + usize::from(has_masked) + usize::from(has_show) != 1
        } else {
            has_blind == has_masked
        } {
            error(
                &mut errors,
                &context,
                if v2 {
                    "requires exactly one of blind, masked and show"
                } else {
                    "requires exactly one of blind and masked"
                },
            );
        }
        let treatment = if has_blind {
            blind.map(|name| {
                let domain = DomainName(name);
                if !domains.contains_key(&domain) {
                    error(
                        &mut errors,
                        &context,
                        &format!("domain {} is not declared", domain.0),
                    );
                }
                if !v2 && placeholder.is_some() {
                    error(
                        &mut errors,
                        &context,
                        "placeholder requires masked treatment",
                    );
                }
                Treatment::Blind { domain }
            })
        } else if show == Some(true) {
            Some(Treatment::Show)
        } else if masked == Some(true) {
            Some(Treatment::Masked { placeholder })
        } else {
            if has_masked {
                error(&mut errors, &context, "masked must be true");
            }
            None
        };
        if let (Some(column), Some(treatment)) = (column, treatment) {
            columns.insert(column, treatment);
        }
    }
    let links = parse_links(raw.link, &domains, &mut errors);
    let json_paths = parse_json_paths(raw.json_path, &domains, &columns, &mut errors);
    if errors.is_empty() {
        Ok(PolicyDocument {
            file_readlog: readlog.clone(),
            file_snapshot_max_age_ms: age,
            policy: Policy {
                json_filters: Default::default(),
                columns,
                domains,
                snapshot_max_age_ms: age.unwrap_or(60000),
                readlog: readlog.unwrap_or(ReadLogSink::Stdout),
            },
            version: if v2 {
                PolicyVersion::V2
            } else {
                PolicyVersion::Legacy
            },
            route_default,
            json_paths,
            links,
        })
    } else {
        Err(errors)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLink {
    fields: Option<Value>,
    domain: Option<Value>,
}

fn parse_links(
    value: Option<Value>,
    domains: &BTreeMap<DomainName, DomainSettings>,
    errors: &mut Vec<PolicyError>,
) -> Vec<LinkRule> {
    let values = match value {
        None => return vec![],
        Some(Value::Array(values)) => values,
        Some(_) => {
            error(errors, "link", "expected an array of tables");
            return vec![];
        }
    };
    let mut links = vec![];
    let mut linked_fields = BTreeSet::new();
    let mut linked_domains = BTreeSet::new();
    for value in values {
        let Some(raw): Option<RawLink> = table(value, &["fields", "domain"], "link", errors) else {
            continue;
        };
        let names: Vec<String> = match raw.fields {
            None => {
                error(
                    errors,
                    "link",
                    "fields is required and must be an array of strings",
                );
                continue;
            }
            Some(value) => match value.try_into() {
                Ok(names) => names,
                Err(_) => {
                    error(errors, "link", "fields must be an array of strings");
                    continue;
                }
            },
        };
        if names.len() < 2 {
            error(errors, "link", "at least two fields are required");
        }
        let mut fields = vec![];
        let mut seen = BTreeSet::new();
        for name in names {
            let parts: Vec<_> = name.split('.').collect();
            if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
                error(errors, "link", "field must be schema.table.column");
                continue;
            }
            let column = ColumnRef {
                schema: parts[0].into(),
                table: parts[1].into(),
                column: parts[2].into(),
            };
            if !seen.insert(column.clone()) {
                error(errors, "link", "duplicate field within a link");
            } else if !linked_fields.insert(column.clone()) {
                error(errors, "link", "field appears in more than one link");
            }
            fields.push(column);
        }
        let domain: Option<String> = field(raw.domain, "link", "domain must be a string", errors);
        let domain = domain.map(DomainName);
        if let Some(domain) = &domain {
            if !domain_name_valid(&domain.0) || !domains.contains_key(domain) {
                error(errors, "link", "domain must name a declared domain");
            }
            if !linked_domains.insert(domain.clone()) {
                error(
                    errors,
                    "link",
                    "declared domain appears in more than one link",
                );
            }
        }
        links.push(LinkRule { fields, domain });
    }
    links
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawJsonPath {
    table: Option<Value>,
    name: Option<Value>,
    path: Option<Value>,
    show: Option<Value>,
    blind: Option<Value>,
    masked: Option<Value>,
    #[serde(rename = "type")]
    ty: Option<Value>,
}

fn parse_json_paths(
    value: Option<Value>,
    domains: &BTreeMap<DomainName, DomainSettings>,
    columns: &BTreeMap<ColumnRef, Treatment>,
    errors: &mut Vec<PolicyError>,
) -> Vec<JsonPathRule> {
    let values = match value {
        None => return vec![],
        Some(Value::Array(values)) => values,
        Some(_) => {
            error(errors, "json_path", "expected an array of tables");
            return vec![];
        }
    };
    let mut rules: Vec<JsonPathRule> = Vec::new();
    for value in values {
        let context = match &value {
            Value::Table(fields) => format!(
                "json_path {}.{}",
                fields.get("table").and_then(Value::as_str).unwrap_or("?"),
                fields.get("name").and_then(Value::as_str).unwrap_or("?")
            ),
            _ => "json_path".into(),
        };
        let Some(raw): Option<RawJsonPath> = table(
            value,
            &["table", "name", "path", "show", "blind", "masked", "type"],
            &context,
            errors,
        ) else {
            continue;
        };
        let table_name = required_string(raw.table, &context, "table must be schema.table", errors);
        let name = required_string(raw.name, &context, "name must be a string", errors);
        let column = table_name.and_then(|table| {
            let parts: Vec<_> = table.split('.').collect();
            if parts.len() != 2 || parts[1].is_empty() {
                error(errors, &context, "table must be schema.table");
                return None;
            }
            name.map(|column| ColumnRef {
                schema: parts[0].into(),
                table: parts[1].into(),
                column,
            })
        });
        let path =
            required_string(raw.path, &context, "path must be a string", errors).and_then(|path| {
                match super::json_path::parse_path(&path) {
                    Ok(path) => Some(path),
                    Err(_) => {
                        error(errors, &context, "invalid path");
                        None
                    }
                }
            });
        let count = usize::from(raw.show.is_some())
            + usize::from(raw.blind.is_some())
            + usize::from(raw.masked.is_some());
        if count != 1 {
            error(
                errors,
                &context,
                "requires exactly one of blind, masked and show",
            );
        }
        let has_show = raw.show.is_some();
        let has_blind = raw.blind.is_some();
        let has_masked = raw.masked.is_some();
        let has_type = raw.ty.is_some();
        let show: Option<bool> = field(raw.show, &context, "show must be true", errors);
        let masked: Option<bool> = field(raw.masked, &context, "masked must be true", errors);
        if has_show && show != Some(true) {
            error(errors, &context, "show must be true");
        }
        if has_masked && masked != Some(true) {
            error(errors, &context, "masked must be true");
        }
        let blind: Option<String> = field(raw.blind, &context, "blind must name a domain", errors);
        let ty: Option<String> = field(raw.ty, &context, "invalid type", errors);
        let treatment = if has_show {
            let ty = match ty.as_deref() {
                Some("string") => Some(JsonLeafType::String),
                Some("number") => Some(JsonLeafType::Number),
                Some("boolean") => Some(JsonLeafType::Boolean),
                _ => {
                    error(
                        errors,
                        &context,
                        "show requires type string, number or boolean",
                    );
                    None
                }
            };
            ty.map(|ty| JsonPathTreatment::Show { ty })
        } else if has_blind {
            let ty = match ty.as_deref() {
                Some("text") => Some(JsonTokenType::Text),
                Some("int4") => Some(JsonTokenType::Int4),
                Some("int8") => Some(JsonTokenType::Int8),
                Some("uuid") => Some(JsonTokenType::Uuid),
                Some("date") => Some(JsonTokenType::Date),
                Some("timestamp") => Some(JsonTokenType::Timestamp),
                Some("timestamptz") => Some(JsonTokenType::Timestamptz),
                _ => {
                    error(errors, &context, "blind requires a supported token type");
                    None
                }
            };
            let domain = blind.map(DomainName);
            if domain
                .as_ref()
                .is_some_and(|domain| !domains.contains_key(domain))
            {
                error(errors, &context, "domain is not declared");
            }
            domain
                .zip(ty)
                .map(|(domain, ty)| JsonPathTreatment::Blind { domain, ty })
        } else if has_masked {
            if has_type {
                error(errors, &context, "type is not allowed with masked");
            }
            Some(JsonPathTreatment::Masked)
        } else {
            None
        };
        if let (Some(column), Some(path), Some(treatment)) = (column, path, treatment) {
            if matches!(
                columns.get(&column),
                Some(Treatment::Blind { .. } | Treatment::Masked { .. })
            ) {
                error(
                    errors,
                    &context,
                    "column blind or masked treatment conflicts with json_path",
                );
            }
            for rule in &rules {
                if rule.column == column
                    && (rule.path.starts_with(&path) || path.starts_with(&rule.path))
                {
                    error(
                        errors,
                        &context,
                        if rule.path == path {
                            "duplicate path"
                        } else {
                            "overlapping paths"
                        },
                    );
                }
            }
            rules.push(JsonPathRule {
                column,
                path,
                treatment,
            });
        }
    }
    rules
}

/// Load and parse a policy file without exposing its path or contents on failure.
pub fn load_policy(path: &Path) -> Result<Policy, Vec<PolicyError>> {
    let text = std::fs::read_to_string(path).map_err(|_| {
        vec![PolicyError {
            message: "policy: unable to read file".into(),
        }]
    })?;
    parse_policy(&text)
}

/// Normalize an audit destination for startup and reload comparisons.
pub(crate) fn readlog_key(sink: &crate::policy::ReadLogSink) -> crate::policy::ReadLogSink {
    use crate::policy::ReadLogSink;
    match sink {
        ReadLogSink::Stdout => ReadLogSink::Stdout,
        ReadLogSink::File { path, max_bytes } => {
            let canonical = std::fs::canonicalize(path)
                .or_else(|error| {
                    let parent = path
                        .parent()
                        .filter(|p| !p.as_os_str().is_empty())
                        .unwrap_or(Path::new("."));
                    match path.file_name() {
                        Some(name) => std::fs::canonicalize(parent).map(|p| p.join(name)),
                        None => Err(error),
                    }
                })
                .unwrap_or_else(|_| path.clone());
            ReadLogSink::File {
                path: canonical,
                max_bytes: *max_bytes,
            }
        }
    }
}

/// Resolve settings with source-relative paths already expanded, refusing disagreements.
pub fn resolve(mut doc: PolicyDocument, service: &ServicePolicySettings) -> Result<PolicyDocument, Vec<PolicyError>> {
    let mut errors = Vec::new();
    doc.policy.readlog = match (&doc.file_readlog, &service.readlog) {
        (Some(file), Some(service)) if readlog_key(file) != readlog_key(service) => {
            error(&mut errors, "readlog", "policy file and service config disagree");
            file.clone()
        }
        (Some(file), _) => file.clone(),
        (None, Some(service)) => service.clone(),
        (None, None) => {
            error(&mut errors, "readlog", "missing table");
            ReadLogSink::Stdout
        }
    };
    doc.policy.snapshot_max_age_ms = match (doc.file_snapshot_max_age_ms, service.snapshot_max_age_ms) {
        (Some(file), Some(service)) if file != service => {
            error(&mut errors, "snapshot_max_age_ms", "policy file and service config disagree");
            file
        }
        (Some(file), _) => file,
        (None, Some(service)) => service,
        (None, None) => 60000,
    };
    // Links are dropped by the Policy projection, so refuse them until a later
    // stack enforces shared domains (fail closed rather than serve them as Show).
    if !doc.links.is_empty() {
        error(&mut errors, "link", "link rules are not served yet");
    }
    if errors.is_empty() { Ok(doc) } else { Err(errors) }
}

#[cfg(test)]
mod tests {
    use super::*;
    const EXAMPLE: &str = r#"
[readlog]
sink = "file"
path = "/var/log/nunya/read.log"
[domains.email]
tokenize = "on"
tokenize_limit_per_session = 20
tokenize_limit_per_principal = 200
tokenize_principal_window = "24h"
[[column]]
table = "public.customers"
name = "email"
blind = "email"
[[column]]
table = "public.customers"
name = "country"
masked = true
placeholder = "**"
"#;
    const LIMIT_LINES: [&str; 3] = [
        "tokenize_limit_per_session = 20\n",
        "tokenize_limit_per_principal = 200\n",
        "tokenize_principal_window = \"24h\"\n",
    ];
    fn without_limits() -> String {
        LIMIT_LINES
            .iter()
            .fold(EXAMPLE.to_owned(), |text, line| text.replace(line, ""))
    }
    fn assert_problem(text: &str, problem: &str) {
        let errors = parse_policy(text).unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].message, format!("domain email: {problem}"));
    }
    #[test]
    fn each_tokenize_limit_is_required() {
        for (line, problem) in LIMIT_LINES.into_iter().zip([
            "missing tokenize_limit_per_session",
            "missing tokenize_limit_per_principal",
            "missing tokenize_principal_window",
        ]) {
            assert_problem(&EXAMPLE.replace(line, ""), problem);
        }
        assert_eq!(parse_policy(&without_limits()).unwrap_err().len(), 3);
    }
    #[test]
    fn each_tokenize_limit_requires_on() {
        for line in LIMIT_LINES {
            for setting in ["tokenize = \"off\"", ""] {
                let text =
                    without_limits().replace("tokenize = \"on\"", &format!("{setting}\n{line}"));
                assert_problem(&text, "tokenize limits require tokenize on");
            }
        }
        assert_eq!(
            parse_policy(&EXAMPLE.replace("tokenize = \"on\"", "tokenize = \"off\""))
                .unwrap_err()
                .len(),
            3
        );
    }
    #[test]
    fn obsolete_tokenize_key_is_unknown() {
        let key = concat!("tokenize_limit_per_", "task");
        let extra = format!("tokenize = \"on\"\n{key} = 987654321");
        assert_problem(&EXAMPLE.replace("tokenize = \"on\"", &extra), "unknown key");
        let errors =
            parse_policy(&without_limits().replace("tokenize = \"on\"", &extra)).unwrap_err();
        assert_eq!(errors.len(), 4);
        assert!(
            errors
                .iter()
                .any(|e| e.message == "domain email: unknown key")
        );
        assert!(
            errors
                .iter()
                .all(|e| !e.message.contains(key) && !e.message.contains("987654321"))
        );
    }
    #[test]
    fn principal_limit_must_cover_session() {
        assert_problem(
            &EXAMPLE.replace(
                "tokenize_limit_per_principal = 200",
                "tokenize_limit_per_principal = 19",
            ),
            "tokenize principal limit must be at least session limit",
        );
        assert!(
            parse_policy(&EXAMPLE.replace(
                "tokenize_limit_per_principal = 200",
                "tokenize_limit_per_principal = 20"
            ))
            .is_ok()
        );
    }
    #[test]
    fn tokenize_window_bounds_and_types() {
        for value in [
            "\"24\"",
            "\"0h\"",
            "\"32d\"",
            "\"1.5h\"",
            "86400",
            "\"59s\"",
            "\"private_window\"",
            "true",
        ] {
            assert_problem(
                &EXAMPLE.replace(
                    "tokenize_principal_window = \"24h\"",
                    &format!("tokenize_principal_window = {value}"),
                ),
                "tokenize principal window must be a duration in 1m..=31d",
            );
        }
        for value in ["1m", "60s", "31d"] {
            assert!(
                parse_policy(&EXAMPLE.replace(
                    "tokenize_principal_window = \"24h\"",
                    &format!("tokenize_principal_window = \"{value}\"")
                ))
                .is_ok()
            );
        }
    }
    #[test]
    fn tokenize_limit_bounds_and_types() {
        for (key, original, problem) in [
            (
                "tokenize_limit_per_session",
                "20",
                "tokenize session limit must be in 1..=100000",
            ),
            (
                "tokenize_limit_per_principal",
                "200",
                "tokenize principal limit must be in 1..=100000000",
            ),
        ] {
            for value in ["0", "-1", "4294967296", "\"private_limit\"", "1.5", "true"] {
                assert_problem(
                    &EXAMPLE.replace(
                        &format!("{key} = {original}\n"),
                        &format!("{key} = {value}\n"),
                    ),
                    problem,
                );
            }
        }
        assert_problem(
            &EXAMPLE.replace(
                "tokenize_limit_per_session = 20",
                "tokenize_limit_per_session = 100001",
            ),
            "tokenize session limit must be in 1..=100000",
        );
        assert_problem(
            &EXAMPLE.replace(
                "tokenize_limit_per_principal = 200",
                "tokenize_limit_per_principal = 100000001",
            ),
            "tokenize principal limit must be in 1..=100000000",
        );
        for (session, principal) in [(1, 1), (100000, 100000000)] {
            assert!(
                parse_policy(
                    &EXAMPLE
                        .replace(
                            "tokenize_limit_per_session = 20",
                            &format!("tokenize_limit_per_session = {session}")
                        )
                        .replace(
                            "tokenize_limit_per_principal = 200",
                            &format!("tokenize_limit_per_principal = {principal}")
                        )
                )
                .is_ok()
            );
        }
    }
    #[test]
    fn empty_schema_requires_leading_dot() {
        let policy = parse_policy(&EXAMPLE.replace("public.customers", ".Customers")).unwrap();
        assert!(
            policy
                .columns
                .keys()
                .all(|c| c.schema.is_empty() && c.table == "Customers")
        );
        assert!(parse_policy(&EXAMPLE.replace("public.customers", "Customers")).is_err());
    }
    #[test]
    fn example_and_defaults() {
        let policy = parse_policy(EXAMPLE).unwrap();
        assert_eq!(policy.snapshot_max_age_ms, 60000);
        assert_eq!(
            policy.readlog,
            ReadLogSink::File {
                path: "/var/log/nunya/read.log".into(),
                max_bytes: 104857600
            }
        );
        assert_eq!(
            policy.domains[&DomainName("email".into())].tokenize,
            TokenizeSetting::On {
                limit_per_session: 20,
                limit_per_principal: 200,
                principal_window: std::time::Duration::from_secs(86400)
            }
        );
        assert_eq!(policy.columns.len(), 2);
        let c = ColumnRef {
            schema: "public".into(),
            table: "customers".into(),
            column: "country".into(),
        };
        assert_eq!(
            policy.treatment(&c),
            Some(&Treatment::Masked {
                placeholder: Some("**".into())
            })
        );
        let off =
            parse_policy(&without_limits().replace("tokenize = \"on\"", "tokenize = \"off\""))
                .unwrap();
        assert_eq!(
            off.domains[&DomainName("email".into())].tokenize,
            TokenizeSetting::Off
        );
    }
    #[test]
    fn all_required_error_cases() {
        let cases = [
            EXAMPLE.replace("sink = \"file\"", "sink = \"file\"\nunknown = \"hidden\""),
            EXAMPLE.replace("blind = \"email\"", "blind = \"email\"\nmasked = true"),
            EXAMPLE.replace("blind = \"email\"", ""),
            EXAMPLE.replace("public.customers", "customers"),
            EXAMPLE.replace("email", "BadDomain"),
            EXAMPLE.replace("blind = \"email\"", "blind = \"missing\""),
            format!(
                "{EXAMPLE}\n[[column]]\ntable = \"public.customers\"\nname = \"email\"\nmasked = true"
            ),
            EXAMPLE.replace("tokenize_limit_per_session = 20", ""),
            EXAMPLE.replace(
                "tokenize_limit_per_session = 20",
                "tokenize_limit_per_session = 0",
            ),
            EXAMPLE.replace(
                "tokenize_limit_per_session = 20",
                "tokenize_limit_per_session = -1",
            ),
            EXAMPLE.replace(
                "tokenize_limit_per_session = 20",
                "tokenize_limit_per_session = 4294967296",
            ),
            EXAMPLE.replace(
                "tokenize_limit_per_session = 20",
                "tokenize_limit_per_session = 100001",
            ),
            EXAMPLE.replace(
                "tokenize_limit_per_session = 20",
                "tokenize_limit_per_session = 20\nrange = { min = 2, max = 1 }",
            ),
            EXAMPLE.replace("path = \"/var/log/nunya/read.log\"", "max_bytes = 0"),
            EXAMPLE.replace("path = \"/var/log/nunya/read.log\"", ""),
        ];
        for (index, text) in cases.into_iter().enumerate() {
            assert!(parse_policy(&text).is_err(), "case {index}");
        }
    }
    #[test]
    fn collects_errors_and_redacts_arbitrary_values() {
        let text = EXAMPLE
            .replace(
                "tokenize_limit_per_session = 20",
                "tokenize_limit_per_session = 0",
            )
            .replace("path = \"/var/log/nunya/read.log\"", "max_bytes = 0")
            .replace("placeholder = \"**\"", "placeholder = 1987654321");
        let errors = parse_policy(&text).unwrap_err();
        assert!(errors.len() >= 3);
        let displayed = errors.iter().map(ToString::to_string).collect::<String>();
        assert!(!displayed.contains("1987654321"));
        assert!(!displayed.contains("/var/log"));
        let syntax = parse_policy("secret = [\"secret-value\"").unwrap_err();
        assert!(!syntax[0].message.contains("secret"));
    }
    #[test]
    fn duplicate_is_detected_even_with_invalid_treatment() {
        let text = format!("{EXAMPLE}\n[[column]]\ntable = \"public.customers\"\nname = \"email\"");
        assert!(
            parse_policy(&text)
                .unwrap_err()
                .iter()
                .any(|e| e.message.contains("listed twice"))
        );
    }
    #[test]
    fn unknown_column_keys_name_the_column_without_echoing_the_key() {
        let text = EXAMPLE.replace(
            "blind = \"email\"",
            "blind = \"email\"\narbitrary_secret_key = \"arbitrary_secret_value\"",
        );
        let errors = parse_policy(&text).unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0].message,
            "column public.customers.email: unknown key"
        );
    }

    const V2: &str = "version = 2\ndefault = 'tokenize'\n[domains.email]\n";
    fn column_rule(rule: &str) -> String {
        format!("{V2}[[column]]\ntable = 'public.customers'\nname = 'data'\n{rule}\n")
    }
    fn path_rule(path: &str, treatment: &str) -> String {
        format!(
            "[[json_path]]\ntable = 'public.customers'\nname = 'data'\npath = '{path}'\n{treatment}\n"
        )
    }
    #[test]
    fn legacy_documents_preserve_policy_and_defaults() {
        for text in [
            EXAMPLE,
            include_str!("../../examples/policy.toml"),
            include_str!("../../harness/policy.toml"),
            include_str!("../../harness/spanner/policy_gsql.toml"),
            &without_limits().replace("tokenize = \"on\"", "tokenize = \"off\""),
            "[readlog]\nsink = 'stdout'",
        ] {
            let document = parse_policy_document(text).unwrap();
            assert_eq!(document.policy, parse_policy(text).unwrap());
            assert_eq!(document.version, PolicyVersion::Legacy);
            assert_eq!(document.route_default, RouteDefault::Show);
            assert!(document.json_paths.is_empty());
        }
    }
    #[test]
    fn v2_happy_documents_cover_all_treatments_and_types() {
        for default in ["tokenize", "show"] {
            let mut text = column_rule("show = true")
                .replace("default = 'tokenize'", &format!("default = '{default}'"));
            for (path, treatment) in [
                ("$.s", "show = true\ntype = 'string'"),
                ("$.n", "show = true\ntype = 'number'"),
                ("$.b", "show = true\ntype = 'boolean'"),
                ("$.hidden", "masked = true"),
            ] {
                text.push_str(&path_rule(path, treatment));
            }
            for ty in [
                "text",
                "int4",
                "int8",
                "uuid",
                "date",
                "timestamp",
                "timestamptz",
            ] {
                text.push_str(&path_rule(
                    &format!("$.{ty}"),
                    &format!("blind = 'email'\ntype = '{ty}'"),
                ));
            }
            text.push_str("[[column]]\ntable = 'public.customers'\nname = 'email'\nblind = 'email'\n[[column]]\ntable = 'public.customers'\nname = 'secret'\nmasked = true\nplaceholder = '**'\n");
            let document = parse_policy_document(&text).unwrap();
            assert_eq!(document.version, PolicyVersion::V2);
            assert_eq!(
                document.route_default,
                if default == "show" {
                    RouteDefault::Show
                } else {
                    RouteDefault::Tokenize
                }
            );
            assert_eq!(document.json_paths.len(), 11);
            assert_eq!(document.policy.columns.len(), 3);
            let show = document
                .policy
                .columns
                .iter()
                .find(|(col, _)| col.column == "data")
                .unwrap();
            assert_eq!(*show.1, Treatment::Show);
            assert_eq!(document.policy.domain_of(show.0), None);
        }
    }
    #[test]
    fn v2_strict_headers_and_service_settings() {
        for text in [
            "version = 2",
            "version = 2\ndefault = 'bad'",
            "version = 2\ndefault = true",
            "version = 1",
            "version = 3",
            "version = '2'",
            "version = true",
            "version = 2\ndefault = 'show'\nprivate_key = 'private_value'",
        ] {
            assert!(parse_policy_document(text).is_err());
        }
        for setting in ["readlog = {}", "snapshot_max_age_ms = 42"] {
            let text = format!("version = 2\ndefault = 'show'\n{setting}");
            let errors = parse_policy_document(&text).unwrap_err();
            assert_eq!(errors.len(), 1);
            assert!(errors[0].message.ends_with("moved to the service config"));
        }
    }
    #[test]
    fn v2_strict_column_treatments() {
        for rule in [
            "",
            "blind = 'email'\nshow = true",
            "blind = 'email'\nmasked = true",
            "masked = true\nshow = true",
            "show = false",
            "show = 'true'",
            "masked = false",
            "show = true\nplaceholder = 'x'",
            "placeholder = 'x'",
            "show = true\nprivate_key = 'private_value'",
        ] {
            assert!(parse_policy_document(&column_rule(rule)).is_err());
        }
    }
    #[test]
    fn v2_strict_json_treatments_and_column_names() {
        for rule in [
            "",
            "show = true",
            "blind = 'email'",
            "masked = true\ntype = 'text'",
            "show = true\ntype = 'text'",
            "blind = 'email'\ntype = 'string'",
            "blind = 'missing'\ntype = 'text'",
            "show = false\ntype = 'string'",
            "masked = false",
            "masked = true\nshow = true\ntype = 'string'",
            "masked = true\nprivate_key = 'private_value'",
        ] {
            assert!(parse_policy_document(&format!("{V2}{}", path_rule("$.a", rule))).is_err());
        }
        let valid = format!("{V2}{}", path_rule("$.a", "masked = true"));
        for text in [
            valid.replace("public.customers", "customers"),
            valid.replace("name = 'data'", "name = 123"),
            valid.replace("path = '$.a'", "path = 123"),
        ] {
            assert!(parse_policy_document(&text).is_err());
        }
        assert!(parse_policy_document(&valid.replace("public.customers", ".Customers")).is_ok());
    }
    #[test]
    fn v2_json_paths_conflict_only_on_the_same_column() {
        for (a, b, treatment) in [
            ("$.a", "$.a", "masked = true"),
            ("$.a", r#"$["a"]"#, "masked = true"),
            ("$.a", "$.a.b", "masked = true"),
            ("$.a.b", "$.a", "show = true\ntype = 'string'"),
            ("$.a", "$.a.b", "show = true\ntype = 'string'"),
        ] {
            let text = format!(
                "{V2}{}{}",
                path_rule(a, treatment),
                path_rule(b, "masked = true")
            );
            assert!(parse_policy_document(&text).is_err());
            assert!(
                parse_policy_document(&text.replacen("name = 'data'", "name = 'other'", 1)).is_ok()
            );
        }
        for treatment in ["masked = true", "blind = 'email'"] {
            assert!(
                parse_policy_document(&format!(
                    "{}{}",
                    column_rule(treatment),
                    path_rule("$.a", "masked = true")
                ))
                .is_err()
            );
        }
        assert!(
            parse_policy_document(&format!(
                "{}{}",
                column_rule("show = true"),
                path_rule("$.a", "masked = true")
            ))
            .is_ok()
        );
    }
    #[test]
    fn unknown_keys_remain_redacted_in_both_versions() {
        for text in [
            "default = 'show'\n[readlog]\nsink = 'stdout'".to_owned(),
            "show = true\n[readlog]\nsink = 'stdout'".to_owned(),
            EXAMPLE.replace("blind = \"email\"", "show = true"),
            format!("{EXAMPLE}{}", path_rule("$.a", "masked = true")),
        ] {
            assert!(
                parse_policy_document(&text)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.message.ends_with("unknown key"))
            );
        }
        for text in [
            "version = 2\ndefault = 'show'\nprivate_key = 'private_value'".to_owned(),
            column_rule("show = true\nprivate_key = 'private_value'"),
            format!(
                "{V2}{}",
                path_rule("$.a", "masked = true\nprivate_key = 'private_value'")
            ),
        ] {
            let errors = parse_policy_document(&text).unwrap_err();
            assert_eq!(errors.len(), 1);
            assert!(errors[0].message.ends_with("unknown key"));
            assert!(!errors[0].message.contains("private"));
        }
    }

    #[test]
    fn resolution_checks_both_sources() {
        let doc = || parse_policy_document("").unwrap();
        let stdout = ReadLogSink::Stdout;
        let file = ReadLogSink::File { path: PathBuf::from("other.log"), max_bytes: 1 };
        for (declared, service, expected) in [
            (Some(stdout.clone()), None, stdout.clone()),
            (None, Some(stdout.clone()), stdout.clone()),
            (Some(stdout.clone()), Some(stdout.clone()), stdout.clone()),
        ] {
            let mut d = doc(); d.file_readlog = declared;
            assert_eq!(resolve(d, &ServicePolicySettings { readlog: service, ..Default::default() }).unwrap().policy.readlog, expected);
        }
        assert_eq!(resolve(doc(), &ServicePolicySettings::default()).unwrap_err()[0].message, "readlog: missing table");
        let mut d = doc(); d.file_readlog = Some(stdout.clone());
        assert_eq!(resolve(d.clone(), &ServicePolicySettings { readlog: Some(file), ..Default::default() }).unwrap_err()[0].message, "readlog: policy file and service config disagree");
        for (declared, service, expected) in [(Some(0), None, 0), (None, Some(42), 42), (Some(42), Some(42), 42), (None, None, 60000)] {
            d.file_snapshot_max_age_ms = declared;
            assert_eq!(resolve(d.clone(), &ServicePolicySettings { snapshot_max_age_ms: service, ..Default::default() }).unwrap().policy.snapshot_max_age_ms, expected);
        }
        d.file_snapshot_max_age_ms = Some(42);
        assert_eq!(resolve(d, &ServicePolicySettings { snapshot_max_age_ms: Some(43), ..Default::default() }).unwrap_err()[0].message, "snapshot_max_age_ms: policy file and service config disagree");
        let base = std::env::temp_dir();
        let mut d = doc();
        d.file_readlog = Some(ReadLogSink::File { path: base.join(".").join("resolution.log"), max_bytes: 12 });
        let service = ServicePolicySettings { readlog: Some(ReadLogSink::File { path: std::fs::canonicalize(base).unwrap().join("resolution.log"), max_bytes: 12 }), ..Default::default() };
        assert!(resolve(d, &service).is_ok());
    }
    fn link_error(body: &str, expected: &str) {
        let errors = parse_policy_document(&format!("{V2}{body}")).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("link:") && e.message.contains(expected))
        );
        for e in errors {
            assert!(!e.message.contains("private_value"));
            assert!(!e.message.contains("private_key"));
        }
    }

    #[test]
    fn links_are_refused_when_resolved_for_serving() {
        let text = format!("{V2}[[link]]
fields = ['public.a.x', 'public.b.y']
");
        let doc = parse_policy_document(&text).unwrap();
        let errors = resolve(doc, &ServicePolicySettings::default()).unwrap_err();
        assert!(errors.iter().any(|e| e.message == "link: link rules are not served yet"));
        assert!(parse_policy(&text).is_err());
    }

    #[test]
    fn links_parse_with_and_without_domain() {
        for domain in ["", "domain = 'email'\n"] {
            let text = format!("{V2}[[link]]\nfields = ['public.a.x', 'public.b.y']\n{domain}");
            let doc = parse_policy_document(&text).unwrap();
            assert_eq!(doc.links.len(), 1);
            assert_eq!(
                doc.links[0].fields,
                vec![
                    ColumnRef {
                        schema: "public".into(),
                        table: "a".into(),
                        column: "x".into()
                    },
                    ColumnRef {
                        schema: "public".into(),
                        table: "b".into(),
                        column: "y".into()
                    },
                ]
            );
            assert_eq!(
                doc.links[0].domain,
                if domain.is_empty() {
                    None
                } else {
                    Some(DomainName("email".into()))
                }
            );
            let fields: Vec<_> = doc.links[0]
                .fields
                .iter()
                .map(|field| format!("{}.{}.{}", field.schema, field.table, field.column))
                .collect();
            let domain = doc.links[0]
                .domain
                .as_ref()
                .map(|domain| format!("domain = {:?}\n", domain.0))
                .unwrap_or_default();
            let rebuilt = format!("{V2}[[link]]\nfields = {fields:?}\n{domain}");
            assert_eq!(parse_policy_document(&rebuilt).unwrap(), doc);
        }
    }
    #[test]
    fn link_requires_fields() {
        link_error("[[link]]\ndomain = 'email'", "fields is required");
    }
    #[test]
    fn link_requires_array_of_strings() {
        for fields in ["'private_value'", "[1, 2]", "['public.a.x', 2]"] {
            link_error(&format!("[[link]]\nfields = {fields}"), "array of strings");
        }
    }
    #[test]
    fn link_requires_two_fields() {
        for fields in ["[]", "['public.a.x']"] {
            link_error(&format!("[[link]]\nfields = {fields}"), "at least two");
        }
    }
    #[test]
    fn link_requires_qualified_nonempty_fields() {
        for field in ["private_value", "a.b", "a.b.c.d", ".b.c", "a..c", "a.b."] {
            link_error(
                &format!("[[link]]\nfields = ['{field}', 'public.b.y']"),
                "schema.table.column",
            );
        }
    }
    #[test]
    fn link_rejects_duplicate_fields() {
        link_error(
            "[[link]]\nfields = ['public.a.x', 'public.a.x']",
            "duplicate field",
        );
    }
    #[test]
    fn link_rejects_overlapping_fields() {
        link_error(
            "[[link]]\nfields = ['public.a.x', 'public.b.y']\n[[link]]\nfields = ['public.a.x', 'public.c.z']",
            "more than one link",
        );
    }
    #[test]
    fn link_rejects_reused_domain() {
        link_error(
            "[[link]]\nfields = ['public.a.x', 'public.b.y']\ndomain = 'email'\n[[link]]\nfields = ['public.c.z', 'public.d.w']\ndomain = 'email'",
            "declared domain appears",
        );
    }
    #[test]
    fn link_requires_declared_valid_domain() {
        for domain in ["private_value", "Bad", ""] {
            link_error(
                &format!("[[link]]\nfields = ['public.a.x', 'public.b.y']\ndomain = '{domain}'"),
                "declared domain",
            );
        }
    }
    #[test]
    fn link_requires_string_domain() {
        link_error(
            "[[link]]\nfields = ['public.a.x', 'public.b.y']\ndomain = 42",
            "domain must be a string",
        );
    }
    #[test]
    fn link_rejects_unknown_key() {
        link_error(
            "[[link]]\nfields = ['public.a.x', 'public.b.y']\nprivate_key = 'private_value'",
            "unknown key",
        );
    }
    #[test]
    fn link_requires_array_of_tables() {
        // Root settings must precede the domains table in V2.
        let errors = parse_policy_document("version = 2\ndefault = 'show'\nlink = 'private_value'")
            .unwrap_err();
        assert_eq!(errors[0].message, "link: expected an array of tables");
    }
    #[test]
    fn link_requires_table_entries() {
        let errors =
            parse_policy_document("version = 2\ndefault = 'show'\nlink = ['private_value']")
                .unwrap_err();
        assert_eq!(errors[0].message, "link: expected a table");
    }
    #[test]
    fn legacy_link_requires_version_two() {
        let errors =
            parse_policy_document("[[link]]\nfields = ['public.a.x', 'public.b.y']").unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.message == "link: link requires version = 2")
        );
    }
}
