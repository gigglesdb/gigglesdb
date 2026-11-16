//! Version 1 JSON definition. Parsing validates structure and all semantic rules.

use std::{collections::BTreeMap, fmt, fs, path::Path};

use serde::{Deserialize, Deserializer, Serialize, de};
use sha2::{Digest, Sha256};

pub const DEFINITION_VERSION: u32 = 1;

/// An opaque reference, never a secret value. Parsing does not read the target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent, deny_unknown_fields)]
pub struct SecretRef(String);

impl SecretRef {
    pub fn parse(value: &str) -> Result<Self, &'static str> {
        let valid = if let Some(name) = value.strip_prefix("env:") {
            valid_env_name(name)
        } else if let Some(path) = value.strip_prefix("file:") {
            let bytes = path.as_bytes();
            // Accept POSIX, UNC, and drive-qualified Windows absolute paths.
            !path.trim().is_empty()
                && !path.chars().any(char::is_control)
                && (path.starts_with('/')
                    || path.starts_with("\\\\")
                    || (bytes.len() >= 3
                        && bytes[0].is_ascii_alphabetic()
                        && bytes[1] == b':'
                        && matches!(bytes[2], b'/' | b'\\')))
        } else {
            false
        };
        if valid {
            Ok(Self(value.to_owned()))
        } else {
            Err(
                "inline secret values are forbidden; use env:NAME or file:/absolute/path (non-empty path and valid environment name required)",
            )
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for SecretRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(de::Error::custom)
    }
}

fn valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationDefinition {
    pub definition_version: u32,
    pub source: Connection,
    pub destination: Connection,
    pub selected_objects: Vec<ObjectSelector>,
    pub excluded_objects: Vec<ExcludedObject>,
    pub mapping_version: String,
    pub connector_versions: ConnectorVersions,
    pub buffer: BufferConfig,
    pub budgets: Budgets,
    pub readiness_policy: ReadinessPolicy,
    pub buffer_retention: BufferRetention,
    pub fence_plan: FencePlan,
    pub hooks: Vec<Hook>,
    pub rollback_contract: RollbackContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_ref: Option<SecretRef>,
    pub engine: Engine,
    #[serde(default)]
    pub tls: crate::security::TlsPolicy,
    /// Reference to a UTF-8 PEM bundle path, not PEM text. Resolution reads the
    /// env/file value as a path for pg::tls::Config::ca_pem, avoiding temporary
    /// certificate files. No DSN, ambient environment, or system-root fallback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca_ref: Option<SecretRef>,
    /// Server version to qualify, e.g. "17.2". This is a declaration, not evidence.
    /// Spanner endpoints do not require a declared server version.
    /// GigglesDB uses the exact version() string the operator qualified against.
    #[serde(default)]
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spanner: Option<SpannerEndpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mysql: Option<MysqlEndpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gigglesdb: Option<GigglesdbEndpoint>,
}

/// Persisted endpoint: credentials remain references; TLS/version use Connection policy.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlEndpoint {
    pub host: String,
    pub port: u16,
    pub user: SecretRef,
    pub password: SecretRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gtid_mode: Option<MysqlGtidMode>,
}
impl fmt::Debug for MysqlEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MysqlEndpoint([REDACTED])")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MysqlGtidMode {
    On,
    Off,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GigglesdbEndpoint {
    pub deployment: GigglesdbDeployment,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_path: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub accept_forward_only: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GigglesdbDeployment {
    Server,
    Embedded,
    Distributed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpannerEndpoint {
    pub project: String,
    pub instance: String,
    pub database: String,
    pub dialect: SpannerDialect,
    pub change_stream: String,
    pub credentials: SecretRef,
    pub emulator: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SpannerDialect {
    GoogleSql,
    Postgresql,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Engine {
    Postgresql,
    Mysql,
    Mariadb,
    Sqlite,
    SqlServer,
    Oracle,
    Mongodb,
    Spanner,
    Gigglesdb,
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Postgresql => "postgresql",
            Self::Mysql => "mysql",
            Self::Mariadb => "mariadb",
            Self::Sqlite => "sqlite",
            Self::SqlServer => "sql-server",
            Self::Oracle => "oracle",
            Self::Mongodb => "mongodb",
            Self::Spanner => "spanner",
            Self::Gigglesdb => "gigglesdb",
        };
        f.write_str(name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectSelector {
    pub schema: String,
    pub name: String,
    pub kind: ObjectKind,
    /// Explicit source column exclusions; operational keys remain mandatory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude_columns: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectKind {
    Table,
    Sequence,
    View,
    MaterializedView,
    Function,
    Type,
    Extension,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExcludedObject {
    pub object: ObjectSelector,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorVersions {
    pub source: String,
    pub destination: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferConfig {
    pub backend: BufferBackend,
    pub manifest_put: ManifestPut,
}

/// Options are backend-specific and reject unknown keys, including credentials
/// passed to a local filesystem backend.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "options",
    rename_all = "kebab-case",
    deny_unknown_fields
)]
pub enum BufferBackend {
    S3Compatible(S3Options),
    Gcs(GcsOptions),
    LocalFs(LocalFsOptions),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ManifestPut {
    CreateOnly,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Options {
    /// Public HTTP(S) endpoint without userinfo, query parameters, or fragments.
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub credentials_ref: SecretRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcsOptions {
    pub bucket: String,
    pub prefix: String,
    pub credentials_ref: SecretRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalFsOptions {
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budgets {
    pub max_snapshot_bytes: u64,
    pub max_buffer_bytes: u64,
    pub max_downtime_seconds: u64,
    pub max_replication_lag_seconds: u64,
    pub max_parallel_tables: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessPolicy {
    pub max_lag_seconds: u64,
    pub required_consecutive_checks: u32,
    pub require_full_validation: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferRetention {
    pub min_seconds: u64,
    pub max_seconds: u64,
    pub retain_on_failure: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FencePlan {
    pub application_roles: Vec<String>,
    pub acknowledged_residual_risks: Vec<String>,
    /// Exact destination inventory IDs, independently reviewed for rollback.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acknowledged_destination_residual_risks: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hook {
    pub name: String,
    pub phase: HookPhase,
    pub timeout_seconds: u64,
    /// Executable and arguments. P1 records this contract but never executes it.
    /// Credentials must be supplied through secret_env, not embedded here.
    pub command: Vec<String>,
    pub secret_env: BTreeMap<String, SecretRef>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum HookPhase {
    BeforeSnapshot,
    PreFence,
    PostActivate,
    PreRollback,
    PostRollback,
}

impl<'de> Deserialize<'de> for HookPhase {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        match value.as_str() {
            "before-snapshot" => Ok(Self::BeforeSnapshot),
            "pre-fence" | "before-fence" => Ok(Self::PreFence),
            "post-activate" | "after-cutover" => Ok(Self::PostActivate),
            "pre-rollback" => Ok(Self::PreRollback),
            "post-rollback" => Ok(Self::PostRollback),
            "on-rollback" => Err(de::Error::custom(
                "on-rollback is ambiguous; choose pre-rollback or post-rollback",
            )),
            _ => Err(de::Error::unknown_variant(
                &value,
                &[
                    "before-snapshot",
                    "pre-fence",
                    "post-activate",
                    "pre-rollback",
                    "post-rollback",
                    "before-fence",
                    "after-cutover",
                ],
            )),
        }
    }
}

impl TryFrom<&Hook> for crate::hooks::OperatorHook {
    type Error = ValidationError;

    fn try_from(hook: &Hook) -> Result<Self, Self::Error> {
        let phase = match hook.phase {
            HookPhase::PreFence => crate::hooks::HookPhase::PreFence,
            HookPhase::PostActivate => crate::hooks::HookPhase::PostActivate,
            HookPhase::PreRollback => crate::hooks::HookPhase::PreRollback,
            HookPhase::PostRollback => crate::hooks::HookPhase::PostRollback,
            HookPhase::BeforeSnapshot => {
                return Err(ValidationError {
                    path: "phase".to_owned(),
                    message: "before-snapshot has no operator hook execution phase".to_owned(),
                });
            }
        };
        if hook.timeout_seconds == 0 {
            return Err(ValidationError {
                path: "timeout_seconds".to_owned(),
                message: "must be greater than zero".to_owned(),
            });
        }
        Ok(Self {
            name: hook.name.clone(),
            phase,
            command: hook.command.clone(),
            secret_env: hook.secret_env.clone(),
            timeout: std::time::Duration::from_secs(hook.timeout_seconds),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackContract {
    pub strategy: RollbackStrategy,
    pub source_retention_seconds: u64,
    pub procedure: String,
    pub acknowledged_data_loss_window_seconds: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RollbackStrategy {
    RestartFromSource,
    ExternalProcedure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationError {
    pub path: String,
    pub message: String,
}

#[derive(Debug)]
pub enum DefinitionError {
    Io(std::io::Error),
    Json(serde_json::Error),
    Validation(Vec<ValidationError>),
}

impl fmt::Display for DefinitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "cannot read definition: {error}"),
            Self::Json(error) => write!(f, "invalid definition JSON: {error}"),
            Self::Validation(errors) => {
                write!(f, "definition validation failed ({} errors)", errors.len())?;
                for error in errors {
                    write!(f, "\n{}: {}", error.path, error.message)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for DefinitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Validation(_) => None,
        }
    }
}

impl MigrationDefinition {
    pub fn from_json(input: &str) -> Result<Self, DefinitionError> {
        let definition: Self = serde_json::from_str(input).map_err(DefinitionError::Json)?;
        definition.ensure_valid()?;
        Ok(definition)
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, DefinitionError> {
        let bytes = fs::read(path).map_err(DefinitionError::Io)?;
        let input = std::str::from_utf8(&bytes).map_err(|_| {
            DefinitionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "definition must be UTF-8",
            ))
        })?;
        Self::from_json(input.strip_prefix('\u{feff}').unwrap_or(input))
    }

    pub(crate) fn ensure_valid(&self) -> Result<(), DefinitionError> {
        let errors = self.validate();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(DefinitionError::Validation(errors))
        }
    }

    /// Return every semantic error. Structural/type errors are reported by serde
    /// during parsing. No network access or secret resolution is performed.
    pub fn validate(&self) -> Vec<ValidationError> {
        let mut errors = Vec::new();
        if self.definition_version != DEFINITION_VERSION {
            push_error(
                &mut errors,
                "definition_version",
                "only version 1 is understood",
            );
        }
        for (path, connection) in [("source", &self.source), ("destination", &self.destination)] {
            if connection.engine != Engine::Spanner {
                nonempty(&mut errors, &format!("{path}.version"), &connection.version);
            }
        }
        for (path, value) in [
            ("mapping_version", &self.mapping_version),
            ("connector_versions.source", &self.connector_versions.source),
            (
                "connector_versions.destination",
                &self.connector_versions.destination,
            ),
            (
                "rollback_contract.procedure",
                &self.rollback_contract.procedure,
            ),
        ] {
            nonempty(&mut errors, path, value);
        }
        for (path, connection) in [("source", &self.source), ("destination", &self.destination)] {
            if connection.engine == Engine::Mysql {
                if connection.connection_ref.is_some() || connection.spanner.is_some() {
                    push_error(
                        &mut errors,
                        path,
                        "MySQL requires only its mysql endpoint block, without connection_ref or spanner",
                    );
                }
                if !matches!(connection.version.as_str(), "8.0" | "8.4") {
                    push_error(
                        &mut errors,
                        &format!("{path}.version"),
                        "expected MySQL version family 8.0 or 8.4",
                    );
                }
                match &connection.mysql {
                    Some(endpoint) => {
                        if endpoint.host.is_empty()
                            || endpoint.host.trim() != endpoint.host
                            || endpoint.host.chars().any(char::is_control)
                        {
                            push_error(
                                &mut errors,
                                &format!("{path}.mysql.host"),
                                "host must be non-empty, trimmed, and free of control characters",
                            );
                        }
                        if endpoint.port == 0 {
                            push_error(
                                &mut errors,
                                &format!("{path}.mysql.port"),
                                "port must be in 1..=65535",
                            );
                        }
                    }
                    None => push_error(
                        &mut errors,
                        &format!("{path}.mysql"),
                        "MySQL engine requires a mysql endpoint block",
                    ),
                }
            } else if connection.mysql.is_some() {
                push_error(
                    &mut errors,
                    &format!("{path}.mysql"),
                    "mysql endpoint block requires the MySQL engine",
                );
            }
            if path == "source"
                && connection
                    .gigglesdb
                    .as_ref()
                    .is_some_and(|e| e.accept_forward_only)
            {
                push_error(
                    &mut errors,
                    "source.gigglesdb.accept_forward_only",
                    "accept_forward_only is meaningful only on the destination",
                );
            }
            if connection.engine != Engine::Gigglesdb && connection.gigglesdb.is_some() {
                push_error(
                    &mut errors,
                    &format!("{path}.gigglesdb"),
                    "gigglesdb endpoint block requires the GigglesDB engine",
                );
            }
            if connection.engine == Engine::Spanner {
                match &connection.spanner {
                    Some(endpoint) => {
                        for (field, value) in [
                            ("project", &endpoint.project),
                            ("instance", &endpoint.instance),
                            ("database", &endpoint.database),
                            ("change_stream", &endpoint.change_stream),
                        ] {
                            if value.is_empty()
                                || value.trim() != value
                                || value.chars().any(char::is_control)
                            {
                                push_error(
                                    &mut errors,
                                    &format!("{path}.spanner.{field}"),
                                    "must be non-empty, trimmed, and free of control characters",
                                );
                            }
                        }
                    }
                    None => push_error(
                        &mut errors,
                        &format!("{path}.spanner"),
                        "Spanner engine requires a spanner endpoint block",
                    ),
                }
                continue;
            }
            if connection.spanner.is_some() {
                push_error(
                    &mut errors,
                    &format!("{path}.spanner"),
                    "spanner endpoint block requires the Spanner engine",
                );
            }
            if connection.engine == Engine::Gigglesdb {
                match &connection.gigglesdb {
                    None => push_error(
                        &mut errors,
                        &format!("{path}.gigglesdb"),
                        "GigglesDB engine requires a gigglesdb endpoint block",
                    ),
                    Some(endpoint) => match endpoint.deployment {
                        GigglesdbDeployment::Server => {
                            if endpoint.database_path.is_some() {
                                push_error(
                                    &mut errors,
                                    &format!("{path}.gigglesdb.database_path"),
                                    "server GigglesDB deployments must not declare database_path",
                                );
                            }
                        }
                        GigglesdbDeployment::Embedded => {
                            let valid_path = endpoint.database_path.as_ref().is_some_and(|value| {
                                let bytes = value.as_bytes();
                                !value.is_empty()
                                    && value.trim() == value
                                    && !value.chars().any(char::is_control)
                                    && (value.starts_with('/')
                                        || value.strip_prefix("\\\\").is_some_and(|rest| {
                                            let mut parts = rest.split(['\\', '/']);
                                            parts.next().is_some_and(|part| !part.is_empty())
                                                && parts.next().is_some_and(|part| !part.is_empty())
                                        })
                                        || (bytes.len() >= 3
                                            && bytes[0].is_ascii_alphabetic()
                                            && bytes[1] == b':'
                                            && matches!(bytes[2], b'/' | b'\\')))
                            });
                            if !valid_path {
                                push_error(
                                    &mut errors,
                                    &format!("{path}.gigglesdb.database_path"),
                                    "embedded GigglesDB deployments require a non-empty, trimmed, control-free absolute database_path",
                                );
                            }
                            if connection.connection_ref.is_some() {
                                push_error(
                                    &mut errors,
                                    &format!("{path}.connection_ref"),
                                    "connection_ref is not applicable to embedded GigglesDB deployments",
                                );
                            }
                            if connection.tls != crate::security::TlsPolicy::default() {
                                push_error(
                                    &mut errors,
                                    &format!("{path}.tls"),
                                    "TLS policy is not applicable to embedded GigglesDB deployments; leave the default",
                                );
                            }
                            if connection.tls_ca_ref.is_some() {
                                push_error(
                                    &mut errors,
                                    &format!("{path}.tls_ca_ref"),
                                    "TLS CA trust is not applicable to embedded GigglesDB deployments",
                                );
                            }
                            continue;
                        }
                        GigglesdbDeployment::Distributed => {
                            push_error(
                                &mut errors,
                                &format!("{path}.gigglesdb.deployment"),
                                "distributed GigglesDB deployments are out of scope (GD-0065 Q3)",
                            );
                            continue;
                        }
                    },
                }
            }
            if connection.connection_ref.is_none() && connection.engine != Engine::Mysql {
                push_error(
                    &mut errors,
                    &format!("{path}.connection_ref"),
                    "connection_ref is required for non-Spanner endpoints",
                );
            }
            match (&connection.tls, &connection.tls_ca_ref) {
                (crate::security::TlsPolicy::Required, None) => push_error(
                    &mut errors,
                    &format!("{path}.tls_ca_ref"),
                    "TLS require needs explicit CA trust; set tls_ca_ref to env:NAME or file:/absolute/path whose resolved UTF-8 value is a PEM bundle path",
                ),
                (crate::security::TlsPolicy::DisableForLocalTest, Some(_)) => push_error(
                    &mut errors,
                    &format!("{path}.tls_ca_ref"),
                    "CA trust contradicts disable-for-local-test; remove tls_ca_ref or select tls require",
                ),
                _ => {}
            }
            if connection.engine == Engine::Postgresql
                && postgres_major(&connection.version).is_none()
            {
                push_error(
                    &mut errors,
                    &format!("{path}.version"),
                    "expected a numeric PostgreSQL version such as 17 or 17.2",
                );
            }
        }
        if self.selected_objects.is_empty() {
            push_error(
                &mut errors,
                "selected_objects",
                "select at least one object",
            );
        }
        let mut selected = std::collections::BTreeSet::new();
        for (index, object) in self.selected_objects.iter().enumerate() {
            validate_object(&mut errors, &format!("selected_objects[{index}]"), object);
            // PG folds unquoted names; our reserved schema is always quoted.
            // Compare exactly, so distinct quoted names remain selectable.
            if object.schema == "_skedaddle" {
                push_error(
                    &mut errors,
                    &format!("selected_objects[{index}].schema"),
                    "_skedaddle is reserved for tool metadata and cannot be selected",
                );
            }
            if !selected.insert((&object.schema, &object.name, object.kind)) {
                push_error(
                    &mut errors,
                    &format!("selected_objects[{index}]"),
                    "duplicate selected object",
                );
            }
        }
        let mut excluded = std::collections::BTreeSet::new();
        for (index, exclusion) in self.excluded_objects.iter().enumerate() {
            let path = format!("excluded_objects[{index}]");
            validate_object(&mut errors, &format!("{path}.object"), &exclusion.object);
            nonempty(&mut errors, &format!("{path}.reason"), &exclusion.reason);
            if !excluded.insert((
                &exclusion.object.schema,
                &exclusion.object.name,
                exclusion.object.kind,
            )) {
                push_error(&mut errors, &path, "duplicate excluded object");
            }
            if selected.contains(&(
                &exclusion.object.schema,
                &exclusion.object.name,
                exclusion.object.kind,
            )) {
                push_error(
                    &mut errors,
                    &path,
                    "an object cannot be both selected and excluded",
                );
            }
        }
        match &self.buffer.backend {
            BufferBackend::S3Compatible(options) => {
                for (key, value) in [
                    ("endpoint", &options.endpoint),
                    ("bucket", &options.bucket),
                    ("region", &options.region),
                ] {
                    nonempty(&mut errors, &format!("buffer.backend.options.{key}"), value);
                }
                if !public_http_endpoint(&options.endpoint) {
                    push_error(
                        &mut errors,
                        "buffer.backend.options.endpoint",
                        "use a public HTTP(S) endpoint without inline credentials, query parameters, or fragments; put secrets in credentials_ref",
                    );
                }
            }
            BufferBackend::Gcs(options) => nonempty(
                &mut errors,
                "buffer.backend.options.bucket",
                &options.bucket,
            ),
            BufferBackend::LocalFs(options) => {
                nonempty(&mut errors, "buffer.backend.options.path", &options.path)
            }
        }
        for (path, value) in [
            (
                "budgets.max_snapshot_bytes",
                self.budgets.max_snapshot_bytes,
            ),
            ("budgets.max_buffer_bytes", self.budgets.max_buffer_bytes),
            (
                "budgets.max_parallel_tables",
                u64::from(self.budgets.max_parallel_tables),
            ),
            (
                "readiness_policy.required_consecutive_checks",
                u64::from(self.readiness_policy.required_consecutive_checks),
            ),
            (
                "buffer_retention.min_seconds",
                self.buffer_retention.min_seconds,
            ),
            (
                "rollback_contract.source_retention_seconds",
                self.rollback_contract.source_retention_seconds,
            ),
        ] {
            if value == 0 {
                push_error(&mut errors, path, "must be greater than zero");
            }
        }
        if self.readiness_policy.max_lag_seconds > self.budgets.max_replication_lag_seconds {
            push_error(
                &mut errors,
                "readiness_policy.max_lag_seconds",
                "must not exceed budgets.max_replication_lag_seconds",
            );
        }
        if self.buffer_retention.max_seconds < self.buffer_retention.min_seconds {
            push_error(
                &mut errors,
                "buffer_retention.max_seconds",
                "must be at least buffer_retention.min_seconds",
            );
        }
        if self.fence_plan.application_roles.is_empty() {
            push_error(
                &mut errors,
                "fence_plan.application_roles",
                "name at least one application role to fence",
            );
        }
        for (field, values) in [
            ("application_roles", &self.fence_plan.application_roles),
            (
                "acknowledged_residual_risks",
                &self.fence_plan.acknowledged_residual_risks,
            ),
            (
                "acknowledged_destination_residual_risks",
                &self.fence_plan.acknowledged_destination_residual_risks,
            ),
        ] {
            for (index, value) in values.iter().enumerate() {
                nonempty(&mut errors, &format!("fence_plan.{field}[{index}]"), value);
            }
        }
        let mut hook_names = std::collections::BTreeSet::new();
        for (index, hook) in self.hooks.iter().enumerate() {
            let path = format!("hooks[{index}]");
            nonempty(&mut errors, &format!("{path}.name"), &hook.name);
            if hook.timeout_seconds == 0 {
                push_error(
                    &mut errors,
                    &format!("{path}.timeout_seconds"),
                    "must be greater than zero",
                );
            }
            if !hook_names.insert(&hook.name) {
                push_error(
                    &mut errors,
                    &format!("{path}.name"),
                    "hook names must be unique",
                );
            }
            if hook.command.is_empty() {
                push_error(
                    &mut errors,
                    &format!("{path}.command"),
                    "specify an executable and optional arguments",
                );
            } else {
                nonempty(&mut errors, &format!("{path}.command[0]"), &hook.command[0]);
            }
            for name in hook.secret_env.keys() {
                if !valid_env_name(name) {
                    push_error(
                        &mut errors,
                        &format!("{path}.secret_env"),
                        "keys must be valid environment variable names",
                    );
                }
            }
        }
        errors
    }

    /// SHA-256 of compact JSON with recursively lexicographically sorted keys.
    /// Arrays preserve their declared order. References are hashed as references;
    /// secret contents and formatting are never part of this digest.
    pub fn canonical_digest(&self) -> Result<String, serde_json::Error> {
        fn canonical(value: serde_json::Value) -> serde_json::Value {
            match value {
                serde_json::Value::Object(map) => {
                    let sorted: BTreeMap<_, _> =
                        map.into_iter().map(|(k, v)| (k, canonical(v))).collect();
                    serde_json::Value::Object(sorted.into_iter().collect())
                }
                serde_json::Value::Array(values) => {
                    serde_json::Value::Array(values.into_iter().map(canonical).collect())
                }
                scalar => scalar,
            }
        }
        let bytes = serde_json::to_vec(&canonical(serde_json::to_value(self)?))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

pub(crate) fn postgres_major(version: &str) -> Option<u32> {
    let mut parts = version.split('.');
    let major = parts.next()?;
    if major.is_empty() || !major.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    for part in parts {
        if part.is_empty() || !part.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
    }
    major.parse().ok()
}

fn push_error(errors: &mut Vec<ValidationError>, path: &str, message: &str) {
    errors.push(ValidationError {
        path: path.to_owned(),
        message: message.to_owned(),
    });
}

fn nonempty(errors: &mut Vec<ValidationError>, path: &str, value: &str) {
    if value.trim().is_empty() {
        push_error(errors, path, "must not be empty or whitespace");
    }
}

fn validate_object(errors: &mut Vec<ValidationError>, path: &str, object: &ObjectSelector) {
    nonempty(errors, &format!("{path}.schema"), &object.schema);
    nonempty(errors, &format!("{path}.name"), &object.name);
    if !object.exclude_columns.is_empty() && object.kind != ObjectKind::Table {
        push_error(
            errors,
            &format!("{path}.exclude_columns"),
            "column exclusions are allowed only for tables",
        );
    }
    let mut columns = std::collections::BTreeSet::new();
    for (index, column) in object.exclude_columns.iter().enumerate() {
        let field = format!("{path}.exclude_columns[{index}]");
        nonempty(errors, &field, column);
        if !columns.insert(column) {
            push_error(errors, &field, "duplicate excluded column");
        }
    }
}

fn public_http_endpoint(endpoint: &str) -> bool {
    let rest = endpoint
        .strip_prefix("https://")
        .or_else(|| endpoint.strip_prefix("http://"));
    rest.is_some_and(|rest| {
        !rest.split('/').next().unwrap_or_default().is_empty()
            && !rest.contains(['@', '?', '#', '\\'])
            && !rest.chars().any(char::is_whitespace)
            && !rest.chars().any(char::is_control)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mysql_endpoint_references_are_strict_and_debug_is_redacted() {
        let endpoint: MysqlEndpoint = serde_json::from_str(r#"{"host":"localhost","port":3306,"user":"env:PRIVATE_USER","password":"env:PRIVATE_PASSWORD","gtid_mode":"OFF"}"#).unwrap();
        assert_eq!(format!("{endpoint:?}"), "MysqlEndpoint([REDACTED])");
        assert_eq!(endpoint.gtid_mode, Some(MysqlGtidMode::Off));
        assert!(
            serde_json::from_str::<MysqlEndpoint>(
                r#"{"host":"localhost","port":3306,"user":"env:USER","password":"inline-value"}"#
            )
            .is_err()
        );
    }
}
