//! Strict policy file parsing with data-free diagnostics.
use super::{
    DomainName, DomainSettings, Policy, ReadLogSink, TokenizeSetting, Treatment, TypeRange,
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
    tokenize_limit_per_task: Option<Value>,
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
fn parse_log(value: Option<Value>, errors: &mut Vec<PolicyError>) -> ReadLogSink {
    let Some(value) = value else {
        error(errors, "readlog", "missing table");
        return ReadLogSink::Stdout;
    };
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
            &["tokenize", "tokenize_limit_per_task", "range"],
            &context,
            errors,
        ) else {
            continue;
        };
        let tokenize: Option<String> =
            field(raw.tokenize, &context, "tokenize must be off or on", errors);
        let limit: Option<u32> = field(
            raw.tokenize_limit_per_task,
            &context,
            "tokenize limit must be in 1..=100000",
            errors,
        );
        let tokenize = match tokenize.as_deref() {
            None | Some("off") => TokenizeSetting::Off,
            Some("on") => match limit {
                Some(limit) if (1..=100000).contains(&limit) => TokenizeSetting::On {
                    limit_per_task: limit,
                },
                _ => {
                    error(
                        errors,
                        &context,
                        "tokenize on requires a limit in 1..=100000",
                    );
                    TokenizeSetting::Off
                }
            },
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
    let value: Value = toml::from_str(toml_text).map_err(|_| {
        vec![PolicyError {
            message: "policy: invalid TOML syntax".into(),
        }]
    })?;
    let mut errors = vec![];
    let Some(raw): Option<RawPolicy> = table(
        value,
        &["snapshot_max_age_ms", "readlog", "domains", "column"],
        "policy",
        &mut errors,
    ) else {
        return Err(errors);
    };
    let age: Option<u64> = field(
        raw.snapshot_max_age_ms,
        "policy",
        "snapshot_max_age_ms must be a nonnegative integer",
        &mut errors,
    );
    let readlog = parse_log(raw.readlog, &mut errors);
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
            &["table", "name", "blind", "masked", "placeholder"],
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
        let has_blind = raw.blind.is_some();
        let has_masked = raw.masked.is_some();
        let blind: Option<String> =
            field(raw.blind, &context, "blind must name a domain", &mut errors);
        let masked: Option<bool> = field(raw.masked, &context, "masked must be true", &mut errors);
        let placeholder: Option<String> = field(
            raw.placeholder,
            &context,
            "placeholder must be a string",
            &mut errors,
        );
        if has_blind == has_masked {
            error(
                &mut errors,
                &context,
                "requires exactly one of blind and masked",
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
                if placeholder.is_some() {
                    error(
                        &mut errors,
                        &context,
                        "placeholder requires masked treatment",
                    );
                }
                Treatment::Blind { domain }
            })
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
    if errors.is_empty() {
        Ok(Policy {
            columns,
            domains,
            snapshot_max_age_ms: age.unwrap_or(60000),
            readlog,
        })
    } else {
        Err(errors)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    const EXAMPLE: &str = r#"
[readlog]
sink = "file"
path = "/var/log/nunya/read.log"
[domains.email]
tokenize = "on"
tokenize_limit_per_task = 20
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
            TokenizeSetting::On { limit_per_task: 20 }
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
            parse_policy(&EXAMPLE.replace("tokenize = \"on\"", "tokenize = \"off\"")).unwrap();
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
            EXAMPLE.replace("tokenize_limit_per_task = 20", ""),
            EXAMPLE.replace(
                "tokenize_limit_per_task = 20",
                "tokenize_limit_per_task = 0",
            ),
            EXAMPLE.replace(
                "tokenize_limit_per_task = 20",
                "tokenize_limit_per_task = -1",
            ),
            EXAMPLE.replace(
                "tokenize_limit_per_task = 20",
                "tokenize_limit_per_task = 4294967296",
            ),
            EXAMPLE.replace(
                "tokenize_limit_per_task = 20",
                "tokenize_limit_per_task = 100001",
            ),
            EXAMPLE.replace(
                "tokenize_limit_per_task = 20",
                "tokenize_limit_per_task = 20\nrange = { min = 2, max = 1 }",
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
                "tokenize_limit_per_task = 20",
                "tokenize_limit_per_task = 0",
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
}
