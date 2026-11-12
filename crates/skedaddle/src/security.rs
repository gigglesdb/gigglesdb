//! Runtime secrets and diagnostic policy (plan P12a, section 3.15).
//!
//! Callers must send every diagnostic through [`Redactor::redact`], including
//! database error details. This module does not install a global logger or alter
//! connector behavior. Register secrets before using them with a connector.

use std::{fmt, fs};

use serde::{Serialize, Serializer};

pub use crate::definition::SecretRef;
use crate::pg::{
    fence::{Catalog, Grantee, Table, reachable_roles},
    preflight::{Check, Snapshot},
};

pub const REDACTED: &str = "[REDACTED]";

/// Plaintext is available only through an explicit exposure at the use site.
/// Formatting and serialization always produce a fixed marker, including when
/// nested in another diagnostic value. This is not a memory-zeroization type.
pub struct Secret(String);

impl Secret {
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl Serialize for Secret {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(REDACTED)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecurityError {
    SecretUnavailable,
    EmptySecret,
    InvalidTlsPolicy,
    PlaintextRefused,
}

impl fmt::Display for SecurityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::SecretUnavailable => {
                "secret unavailable; check the referenced environment variable or UTF-8 file"
            }
            Self::EmptySecret => "empty secret refused; supply a non-empty referenced secret",
            Self::InvalidTlsPolicy => "invalid TLS policy; use require or disable-for-local-test",
            Self::PlaintextRefused => {
                "TLS REQUIRED: refuse plaintext; configure and verify TLS on the connection"
            }
        })
    }
}

impl std::error::Error for SecurityError {}

/// Resolve only a validated reference, at runtime. File contents are preserved
/// exactly (including trailing newlines). Errors never include paths or values.
pub fn resolve(reference: &SecretRef) -> Result<Secret, SecurityError> {
    let value = if let Some(name) = reference.as_str().strip_prefix("env:") {
        std::env::var(name).map_err(|_| SecurityError::SecretUnavailable)?
    } else if let Some(path) = reference.as_str().strip_prefix("file:") {
        fs::read_to_string(path).map_err(|_| SecurityError::SecretUnavailable)?
    } else {
        return Err(SecurityError::SecretUnavailable);
    };
    if value.is_empty() {
        return Err(SecurityError::EmptySecret);
    }
    Ok(Secret(value))
}

/// Reusable diagnostic boundary. Debug does not expose the registered values.
#[derive(Clone, Default)]
pub struct Redactor {
    values: Vec<String>,
}

impl fmt::Debug for Redactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Redactor([REDACTED])")
    }
}

impl Redactor {
    pub fn register(&mut self, secret: &Secret) {
        self.register_value(secret.expose_secret());
    }

    /// Register an already exposed value, e.g. one supplied by a connector.
    pub fn register_value(&mut self, value: &str) {
        self.register_literal(value);
        // A connection reference often resolves to a whole DSN. SQL errors
        // can echo just its password, so register decoded password components
        // before handing the connection string to the driver.
        for (start, end) in password_ranges(value) {
            let password = &value[start..end];
            self.register_literal(password);
            self.register_literal(&percent_decode(password));
            self.register_literal(
                &password
                    .replace("''", "'")
                    .replace("\\'", "'")
                    .replace("\\\"", "\"")
                    .replace("\\\\", "\\")
                    .replace("}}", "}"),
            );
            if let Ok(decoded) = serde_json::from_str::<String>(&format!("\"{password}\"")) {
                self.register_literal(&decoded);
            }
        }
    }

    fn register_literal(&mut self, value: &str) {
        if value.is_empty() {
            return;
        }
        let json = serde_json::to_string(value).expect("serializing a string cannot fail");
        for candidate in [
            value.to_owned(),
            value.replace('\'', "''"),
            value.replace('\\', "\\\\").replace('\'', "\\'"),
            json[1..json.len() - 1].to_owned(),
            percent_encode(value, false),
            percent_encode(value, true),
        ] {
            if !self.values.contains(&candidate) {
                self.values.push(candidate);
            }
        }
        self.values
            .sort_by_key(|value| std::cmp::Reverse(value.len()));
    }

    /// Redact known values (also SQL/JSON/URI escaped) and password fields in
    /// URI/libpq/ADO connection strings, even if not registered beforehand.
    pub fn redact(&self, diagnostic: &str) -> String {
        let mut ranges = password_ranges(diagnostic);
        // Match all variants against the original input. Sequential replacement
        // could reveal suffixes of overlapping secrets or corrupt the marker.
        for value in &self.values {
            ranges.extend(
                diagnostic
                    .match_indices(value)
                    .map(|(i, v)| (i, i + v.len())),
            );
        }
        ranges.sort_unstable();
        let mut merged: Vec<(usize, usize)> = Vec::new();
        for (start, end) in ranges {
            if let Some(last) = merged.last_mut()
                && start <= last.1
            {
                last.1 = last.1.max(end);
                continue;
            }
            merged.push((start, end));
        }
        let mut output = String::new();
        let mut cursor = 0;
        for (start, end) in merged {
            output.push_str(&diagnostic[cursor..start]);
            output.push_str(REDACTED);
            cursor = end;
        }
        output.push_str(&diagnostic[cursor..]);
        output
    }
}

fn percent_encode(value: &str, lowercase: bool) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            out.push(char::from(byte));
        } else {
            use fmt::Write;
            if lowercase {
                write!(out, "%{byte:02x}").expect("writing to String cannot fail");
            } else {
                write!(out, "%{byte:02X}").expect("writing to String cannot fail");
            }
        }
    }
    out
}

fn percent_decode(input: &str) -> String {
    let mut decoded = Vec::new();
    let mut bytes = input.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let pair: Vec<_> = bytes.by_ref().take(2).collect();
            if pair.len() == 2
                && let (Some(high), Some(low)) = (
                    (pair[0] as char).to_digit(16),
                    (pair[1] as char).to_digit(16),
                )
            {
                decoded.push((high * 16 + low) as u8);
            } else {
                decoded.push(byte);
                decoded.extend(pair);
            }
        } else {
            decoded.push(byte);
        }
    }
    String::from_utf8(decoded).unwrap_or_else(|_| input.to_owned())
}

fn password_ranges(input: &str) -> Vec<(usize, usize)> {
    let bytes = input.as_bytes();
    let mut ranges = Vec::new();
    for (scheme, _) in input.match_indices("://") {
        let start = scheme + 3;
        let end = bytes[start..]
            .iter()
            .position(|b| b.is_ascii_whitespace() || b"/?#\"'".contains(b))
            .map_or(bytes.len(), |offset| start + offset);
        if let Some(at) = input[start..end].rfind('@')
            && let Some(colon) = input[start..start + at].find(':')
        {
            ranges.push((start + colon + 1, start + at));
        }
    }
    let mut i = 0;
    while i < bytes.len() {
        let length = ["password", "pwd"].iter().find_map(|key| {
            bytes
                .get(i..i + key.len())
                .filter(|part| part.eq_ignore_ascii_case(key.as_bytes()))
                .map(|_| key.len())
        });
        if let Some(length) = length
            && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_'))
        {
            let mut pos = i + length;
            while bytes.get(pos).is_some_and(u8::is_ascii_whitespace) {
                pos += 1;
            }
            // Also handle JSON-style password fields in error context.
            if matches!(bytes.get(pos), Some(b'\'' | b'"')) {
                pos += 1;
            }
            if matches!(bytes.get(pos), Some(b'=' | b':')) {
                pos += 1;
                while bytes.get(pos).is_some_and(u8::is_ascii_whitespace) {
                    pos += 1;
                }
                let quote = bytes.get(pos).copied().filter(|b| b"'\"{".contains(b));
                let start = pos + usize::from(quote.is_some());
                pos = start;
                while pos < bytes.len() {
                    if bytes[pos] == b'\\' {
                        pos = (pos + 2).min(bytes.len());
                    } else if let Some(quote) = quote {
                        let closing = if quote == b'{' { b'}' } else { quote };
                        if bytes[pos] == closing {
                            if bytes.get(pos + 1) == Some(&closing) {
                                pos += 2;
                            } else {
                                break;
                            }
                        } else {
                            pos += 1;
                        }
                    } else if bytes[pos].is_ascii_whitespace() || b";&,\"'<>".contains(&bytes[pos])
                    {
                        break;
                    } else {
                        pos += 1;
                    }
                }
                ranges.push((start, pos));
                i = pos;
            }
        }
        i += 1;
    }
    ranges
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
pub enum TlsPolicy {
    #[default]
    #[serde(rename = "require")]
    Required,
    #[serde(rename = "disable-for-local-test")]
    DisableForLocalTest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct WarningItem {
    pub code: &'static str,
    pub message: &'static str,
}

impl TlsPolicy {
    /// The value must come from the definition, never a connection string.
    pub fn from_definition(value: Option<&str>) -> Result<Self, SecurityError> {
        match value {
            None | Some("require") => Ok(Self::Required),
            Some("disable-for-local-test") => Ok(Self::DisableForLocalTest),
            Some(_) => Err(SecurityError::InvalidTlsPolicy),
        }
    }

    /// `encrypted` is verified transport evidence supplied by the connector,
    /// not its requested sslmode. A policy check does not establish TLS itself.
    pub fn check(self, encrypted: bool) -> Result<Vec<WarningItem>, SecurityError> {
        match self {
            Self::Required if !encrypted => Err(SecurityError::PlaintextRefused),
            Self::Required => Ok(Vec::new()),
            Self::DisableForLocalTest => Ok(vec![WarningItem {
                code: "plaintext_local_test_only",
                message: "WARNING: TLS DISABLED FOR LOCAL TEST; credentials and data may travel in plaintext. Enable TLS before any real migration.",
            }]),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RolePurpose {
    Capture,
    Fence,
    Apply,
    Verify,
}

/// Classify the existing facts without SQL or side effects. `snapshot` must be
/// collected as `role`; `catalog` includes memberships and write grants. This
/// detects excess *observed* authority, not missing grants or unobserved ACLs.
/// Preflight does not enumerate global SELECT, CREATEROLE, or publication ACLs;
/// a clean result cannot certify completeness of least privilege.
pub fn classify_role(
    role: &str,
    purpose: RolePurpose,
    snapshot: &Snapshot,
    catalog: &Catalog,
    in_scope: &[Table],
) -> Vec<WarningItem> {
    let mut warnings = Vec::new();
    let mut warn = |code, message| {
        if !warnings.iter().any(|item: &WarningItem| item.code == code) {
            warnings.push(WarningItem { code, message });
        }
    };
    let reached = reachable_roles(role, &catalog.memberships);
    if !catalog.roles.iter().any(|r| r.name == role) {
        warn(
            "role_facts_missing",
            "Role facts are missing; collect the role catalog before assessing least privilege.",
        );
    }
    for fact in &catalog.roles {
        if reached.contains(&fact.name) {
            if fact.superuser {
                warn(
                    "role_superuser",
                    "SUPERUSER exceeds every migration role purpose; use a dedicated non-superuser role.",
                );
            }
            if fact.bypass_rls {
                warn(
                    "role_bypass_rls",
                    "BYPASSRLS exceeds the declared purpose; remove it and explicitly qualify row security.",
                );
            }
        }
    }
    for name in &reached {
        let allowed =
            name == role || (name == "pg_signal_backend" && purpose == RolePurpose::Fence);
        if !allowed && name.starts_with("pg_") {
            warn(
                "broad_predefined_role",
                "A reachable predefined PostgreSQL role exceeds the declared purpose; replace it with scoped grants.",
            );
        }
    }
    let scoped = |table: &Table| {
        in_scope
            .iter()
            .any(|t| t.schema == table.schema && t.name == table.name)
    };
    for table in &catalog.tables {
        if reached.contains(&table.owner)
            && (!scoped(table) || matches!(purpose, RolePurpose::Capture | RolePurpose::Verify))
        {
            warn(
                "table_ownership",
                "Table ownership exceeds the declared purpose or scope; use scoped grants and a separate owner.",
            );
        }
    }
    for grant in &catalog.grants {
        let effective = match &grant.grantee {
            Grantee::Public => true,
            Grantee::Role(name) => reached.contains(name),
        };
        if !effective {
            continue;
        }
        if !scoped(&grant.table) {
            warn(
                "out_of_scope_write",
                "Write privileges extend outside the migration scope; revoke the excess grants, including PUBLIC and inherited grants.",
            );
        } else if matches!(purpose, RolePurpose::Capture | RolePurpose::Verify)
            || (purpose == RolePurpose::Fence && !grant.grant_option)
        {
            warn(
                "unneeded_write",
                "Write authority exceeds this role's purpose; use read-only capture/verify roles and scoped fence grant authority.",
            );
        }
        if grant.grant_option && purpose != RolePurpose::Fence {
            warn(
                "unneeded_grant_option",
                "Grant authority exceeds this role's purpose; revoke WITH GRANT OPTION.",
            );
        }
    }
    for observation in &snapshot.observations {
        if !matches!(
            observation.check,
            Check::ReplicationPrivilege
                | Check::EventTriggerPrivilege
                | Check::DatabaseCreate
                | Check::Grants
        ) {
            continue;
        }
        if observation.error.is_some() {
            warn(
                "role_facts_missing",
                "Role facts are missing; collect the role catalog before assessing least privilege.",
            );
            continue;
        }
        for row in &observation.rows {
            if row["superuser"] == true {
                warn(
                    "role_superuser",
                    "SUPERUSER exceeds every migration role purpose; use a dedicated non-superuser role.",
                );
            }
            if observation.check == Check::ReplicationPrivilege
                && row["replication"] == true
                && purpose != RolePurpose::Capture
            {
                warn(
                    "unneeded_replication",
                    "REPLICATION exceeds this role's purpose; reserve it for source capture.",
                );
            }
            if observation.check == Check::DatabaseCreate && row["create"] == true {
                warn(
                    "unneeded_database_create",
                    "Database CREATE exceeds the steady-state role purpose; use a separate scoped installer.",
                );
            }
            if observation.check == Check::Grants
                && row["owner"] == true
                && matches!(purpose, RolePurpose::Capture | RolePurpose::Verify)
            {
                warn(
                    "table_ownership",
                    "Table ownership exceeds the declared purpose or scope; use scoped grants and a separate owner.",
                );
            }
        }
    }
    warnings
}
