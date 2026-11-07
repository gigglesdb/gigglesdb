# Skedaddle

Skedaddle is a database migration tool. Milestone 1 targets PostgreSQL 16/17/18
to PostgreSQL, logical replication through `pgoutput`, and a buffer on
S3-compatible storage, GCS, or the local filesystem. The M1 qualification matrix
is PostgreSQL 16/17/18 on both endpoints. Other engines are declared in the
definition model but have no M1 connector.

P1 provides a standalone Rust 2024 crate, strict version 1 JSON definitions,
canonical digests, an offline capability report, and `skedaddle check <definition>`.
The checker does not connect, load secrets, inspect catalogs, move data, write manifests,
run hooks, fence applications, or implement rollback. Connector versions in a
definition are requested pins, not installed or verified connectors. This crate
is not publishable yet.

The checker prints a JSON report. Exit status is 0 for a report with no blockers,
2 for capability blockers, and 1 for invalid input or an I/O error. Valid
PostgreSQL definitions still block with `Unverified` items until live preflight
is implemented. Exclusion entries are advisory, retain their mandatory reason,
and do not claim the excluded object was inspected.

Definitions reject unknown fields at every level. `definition_version` must be
1. `validate()` collects all semantic errors after structural parsing; malformed
JSON, unknown fields, wrong types, and malformed secret references fail parsing.
Every secret-bearing field (`connection_ref`, `credentials_ref`, hook
`secret_env` values) accepts only `env:NAME` or `file:/absolute/path`; Windows
drive and UNC paths are also accepted. Secret references are never resolved in
P1. Public S3 endpoints cannot contain userinfo, query parameters, or fragments.
Hook commands must use `secret_env` for credentials rather than embed secrets
in their executable or arguments.

The digest is lowercase SHA-256 over compact JSON of the typed definition with
recursively sorted object keys and preserved array order. It is independent of
input key order, whitespace, and equivalent JSON string escapes. It hashes
reference strings, not secret contents. This is the version 1 canonicalization
contract, not a general-purpose JSON canonicalization standard.

Buffers require `manifest_put: "create-only"`; overwriting manifests is not a
representable policy. Backend `type` is `local-fs`, `s3-compatible`, or `gcs`.
Local options have `path`; S3 options have `endpoint`, `bucket`, `prefix`,
`region`, `credentials_ref`; GCS options have `bucket`, `prefix`,
`credentials_ref`. Prefixes may be empty. Backend-specific options reject other
fields. Create-only behavior is a contract for future connectors; P1 performs
no puts. All time budgets and retention fields use seconds, storage budgets use
bytes. Zero downtime and replication lag budgets are allowed. Retention minimum,
snapshot/buffer capacities, parallelism, readiness check count, and source
retention must be positive. Readiness lag must fit the replication lag budget.

The five report statuses are `Supported`, `SupportedWithTransformation`,
`RequiresExternalChange`, `Unsupported`, and `Unverified`. Critical items block
unless their status is one of the first two. Every item carries explicit
`sampled`, `criticality`, `remediation`, and `evidence` fields. P1 never samples
data, so its generated items have `sampled: false`. An unsupported engine on
either endpoint yields exactly one critical `Unsupported` item, plus any
advisory exclusions.

## Definition example

Every `json` fence in this README is a complete definition tested by
`definition_examples_in_readme_parse`. Save this example as a JSON file outside
the source checkout, then pass its path to `skedaddle check`.

```json
{
  "definition_version": 1,
  "source": {
    "connection_ref": "env:SKEDADDLE_SOURCE",
    "engine": "postgresql",
    "version": "17.2"
  },
  "destination": {
    "connection_ref": "file:/run/secrets/skedaddle-destination",
    "engine": "postgresql",
    "version": "18.0"
  },
  "selected_objects": [
    { "schema": "public", "name": "accounts", "kind": "table" }
  ],
  "excluded_objects": [
    {
      "object": { "schema": "public", "name": "scratch", "kind": "table" },
      "reason": "Disposable staging data is rebuilt by the application."
    }
  ],
  "mapping_version": "1",
  "connector_versions": { "source": "0.1.0", "destination": "0.1.0" },
  "buffer": {
    "backend": {
      "type": "local-fs",
      "options": { "path": "J:/gigglesdb-work/example-migration/buffer" }
    },
    "manifest_put": "create-only"
  },
  "budgets": {
    "max_snapshot_bytes": 1073741824,
    "max_buffer_bytes": 536870912,
    "max_downtime_seconds": 30,
    "max_replication_lag_seconds": 5,
    "max_parallel_tables": 4
  },
  "readiness_policy": {
    "max_lag_seconds": 1,
    "required_consecutive_checks": 3,
    "require_full_validation": true
  },
  "buffer_retention": {
    "min_seconds": 86400,
    "max_seconds": 604800,
    "retain_on_failure": true
  },
  "fence_plan": {
    "application_roles": ["app_writer"],
    "acknowledged_residual_risks": [
      "Privileged operators can bypass the application role fence."
    ]
  },
  "hooks": [
    {
      "name": "prepare-application-fence",
      "phase": "before-fence",
      "command": ["application-admin", "prepare-fence"],
      "secret_env": { "ADMIN_TOKEN": "env:SKEDADDLE_ADMIN_TOKEN" }
    }
  ],
  "rollback_contract": {
    "strategy": "restart-from-source",
    "source_retention_seconds": 604800,
    "procedure": "Keep source fenced and retained; operator decides whether to restore source writes before destination writes are accepted.",
    "acknowledged_data_loss_window_seconds": 0
  }
}
```

## Development

Keep all build outputs, temporary files, fixtures, and logs under a unique task
folder in `J:/gigglesdb-work`. Set `CARGO_TARGET_DIR`, `TEMP`, and `TMP` for every
Cargo invocation, and set `SKEDADDLE_TEST_ROOT` to that task's fixture directory.
Tests write file-based cases there without deleting them. Existing `artifacts/`
and `target/` are historical P0 material, excluded from packaging and untouched.
