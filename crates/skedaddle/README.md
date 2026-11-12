# Skedaddle

Skedaddle is a database migration tool with an engine-neutral coordinator and
PostgreSQL-to-PostgreSQL migration components. The milestone target is PostgreSQL
16/17/18 on both endpoints, using logical replication through `pgoutput`.
S3-compatible storage, GCS and local filesystem are buffer backend choices.
The operational CLI runner currently assembles only the local-filesystem backend.

This README describes implementation boundaries and incomplete qualification.
R3d reports the foreground local-fs runner implemented and ordinary gates passing;
live PostgreSQL qualification was not run. Production readiness and WAL-health
collectors remain disconnected, so the default engine cannot claim verified-ready.
A command being present does not establish a safe live migration.

## Docker harness quickstart

Run the existing harness from the repository root. Use a unique task directory
under `J:/gigglesdb-work`; keep data at or below 1 GB. Docker-backed qualification
requires Docker and the harness's pinned images. Run the disk guard first.
These commands are operator instructions, not evidence of a completed run:

```powershell
$taskRoot = 'J:/gigglesdb-work/skedaddle-rehearsal-<unique-run-id>'
crates/skedaddle/harness/run.ps1 guard -TaskRoot $taskRoot
crates/skedaddle/harness/run.ps1 p2 -TaskRoot $taskRoot
crates/skedaddle/harness/cleanup.ps1 -Check -TaskRoot $taskRoot
```

The `p2` tier exercises coordinator/backend infrastructure, not a complete migration.
The `pg-matrix` phase accepts `-PgVersion 16`, `17` or `18`. Other accepted phases
are `p2-backends`, `p3`, `linux-fs` and `mutants`; their detailed coverage is not
requalified here. Mutation runs need exclusive access to the source they mutate.

The harness checks host/Docker disk headroom, uses ephemeral loopback ports and
labels resources `skedaddle.test=<run-id>` in project `skedaddle-<run-id>`.
Normal teardown is scoped to the run. After interruption, use its exact recorded
run ID and the same task root, then check for leftovers:

```powershell
crates/skedaddle/harness/cleanup.ps1 -RunId <run-id> -TaskRoot $taskRoot
crates/skedaddle/harness/cleanup.ps1 -Check -TaskRoot $taskRoot
```

Unscoped teardown is rejected. Never prune unrelated Docker resources or remove
another run's files. Preserve small logs/reports and remove only your task's
regenerable bulk after its processes stop.

Both PowerShell headers set `CARGO_BUILD_JOBS=4`, matching project policy.
The run wrapper uses `<TaskRoot>/target`, except `pg-matrix`, which uses
`J:/gigglesdb-work/gd49-target-b`. These override inherited settings.

For direct Cargo work, explicitly set these variables. Use a stable target per
track and hold the project's exclusive heavy-Cargo lock during a build/test.
Never share a target directory between concurrent builds.

```powershell
$env:CARGO_TARGET_DIR = 'J:/gigglesdb-work/gd49-target-<track>'
$env:TEMP = "$taskRoot/temp"
$env:TMP = "$taskRoot/temp"
$env:SKEDADDLE_TEST_ROOT = "$taskRoot/test-root"
$env:RUSTC_WRAPPER = 'J:/gigglesdb-work/tools/bin/sccache.exe'
$env:SCCACHE_DIR = 'J:/gigglesdb-work/sccache-cache'
$env:SCCACHE_CACHE_SIZE = '100G'
$env:CARGO_INCREMENTAL = '0'
$env:CARGO_BUILD_JOBS = '4'
```

## Definition reference

Definitions are strict version-1 JSON. Unknown fields are rejected at every level.
Structural errors fail parsing; semantic validation collects errors. Durations
use seconds and storage capacities use bytes. Every `json` fence in this file is
consumed as a full definition by definition/local-sizing tests. Save the complete
example outside the source checkout before using `skedaddle check`.

```json
{
  "definition_version": 1,
  "source": {
    "connection_ref": "env:SKEDADDLE_SOURCE",
    "tls_ca_ref": "env:SKEDADDLE_SOURCE_CA_PATH",
    "engine": "postgresql",
    "version": "17.2"
  },
  "destination": {
    "connection_ref": "file:/run/secrets/skedaddle-destination",
    "tls_ca_ref": "file:/run/secrets/skedaddle-destination-ca-path",
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
      "timeout_seconds": 30,
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

### Top-level fields and objects

| Field | Meaning |
|---|---|
| `definition_version` | Must be `1`. |
| `source`, `destination` | Connection definitions below. |
| `selected_objects` | Objects explicitly selected for migration. |
| `excluded_objects` | Advisory exclusions, each with `object` and mandatory nonblank `reason`. |
| `mapping_version` | Nonempty requested mapping version. |
| `connector_versions` | Requested `source` and `destination` pins; not proof of installed connectors. |
| `buffer` | Backend and create-only manifest policy. |
| `budgets` | Capacity, lag, interruption and parallelism limits. |
| `readiness_policy` | Lag, consecutive checks and validation requirements. |
| `buffer_retention` | Minimum/maximum retention and failure policy. |
| `fence_plan` | Application roles and acknowledged residual risks. |
| `hooks` | Named commands and their declared secret environment. |
| `rollback_contract` | Strategy, source retention, procedure and acknowledged loss window. |

Object references have `schema`, `name` and `kind`; the example selects a `table`.
An exclusion reason records intent, not proof an object was inspected. Do not
select and exclude the same object. Additional object-kind and engine enum
spellings are not independently audited here.

### Connections and references

Both endpoints have `engine`, `version`, `connection_ref`, `tls` and `tls_ca_ref`.
PostgreSQL 16/17/18 are the intended connector versions; a declared version is
not a live observation. Other engines have no qualified M1 connector.

`tls` defaults to `"require"`, which requires `tls_ca_ref`.
`"disable-for-local-test"` is the explicit local-only exception and rejects a
simultaneous CA reference. Plain `"disable"` is not accepted. Do not use the
local-test exception as a production policy.

`connection_ref`, `tls_ca_ref`, backend `credentials_ref`, and hook `secret_env`
values accept `env:NAME` or `file:/absolute/path`; Windows drive and UNC forms
are accepted. Inline credentials are not definition values. Parsing is offline;
references are resolved by runtime consumers. The resolved CA value is a UTF-8
PEM-bundle **path**, not PEM contents. There is no implicit DSN, ambient-root or
system-root trust inference. Real trust remains unverified until connection.

The digest is lowercase SHA-256 of compact typed JSON with recursively sorted
keys and preserved array order. It binds references, including CA references,
not secret contents. Whitespace/input key order do not change it. This is the
version-1 contract, not general JSON canonicalization.

### Buffer fields

`buffer.manifest_put` must be `"create-only"`; overwrite is not a policy option.
`buffer.backend` has `type` and matching `options`; unrelated options are rejected.

| Backend `type` | `options` fields |
|---|---|
| `local-fs` | `path` |
| `s3-compatible` | `endpoint`, `bucket`, `prefix`, `region`, `credentials_ref` |
| `gcs` | `bucket`, `prefix`, `credentials_ref` |

Prefixes may be empty. Public S3 endpoints cannot contain userinfo, query
parameters or fragments. Definitions alone do not qualify remote runner assembly.

### Budgets, readiness, retention and fencing

| Field | Meaning |
|---|---|
| `budgets.max_snapshot_bytes` | Positive snapshot capacity. |
| `budgets.max_buffer_bytes` | Positive buffer capacity. |
| `budgets.max_downtime_seconds` | Interruption budget; zero is representable, not guaranteed. |
| `budgets.max_replication_lag_seconds` | Lag budget; zero is representable. |
| `budgets.max_parallel_tables` | Positive table parallelism. |
| `readiness_policy.max_lag_seconds` | Must fit the replication-lag budget. |
| `readiness_policy.required_consecutive_checks` | Positive check count. |
| `readiness_policy.require_full_validation` | Require full validation for readiness. |
| `buffer_retention.min_seconds` | Positive retention minimum. |
| `buffer_retention.max_seconds` | Retention upper bound, consistent with the minimum. |
| `buffer_retention.retain_on_failure` | Requested failure-retention policy. |
| `fence_plan.application_roles` | Nonempty application principals to fence. |
| `fence_plan.acknowledged_residual_risks` | Acknowledgments, not mitigation by themselves. |

### Hooks and rollback

The rollback journal contract (`coordinator::rollback`) records admission and
`rolling-back` atomically, then requires a current-epoch intent before each step
checkpoint. It retains the request identity, absolute deadline and final marker
behind coordinator metadata. Its conservative point of no return is publication
of the reverse-apply intent, before any reverse transaction may commit. A deadline
abort before that point keeps `rolling-back` until destination restoration is
checkpointed; only then does it return to `rollback-window`. Eligibility loss
blocks new transfer and restoration intents, retaining the fences for recovery.
The post-activation owner admits eligible requests and drives the shared rollback
steps under exclusive migration authority. It retains live reverse capture on a
dedicated thread, publishing the exact durable stop receipt before reverse apply.
Native destination fence receipts and the original source snapshot reconstruct
adapters on retry; restart/takeover resumes the journaled step without extending
the deadline. Before reverse-apply intent, a deadline restores the destination
and restarts capture before returning to `rollback-window`. After that intent,
rollback completes or retains fences for operator action. Infrastructure faults
retry; an operator-action result is durably latched for the admitted request.

Each hook has `name`, `phase`, positive `timeout_seconds`, `command` and
`secret_env`. `command` is an executable/argument array. `secret_env` keys name
the supplied environment variables; its values are references. Do not embed
credentials in arguments. The example uses `before-fence`; the full accepted
phase enum is unverified in the permitted evidence. Parsing a phase does not
prove a CLI operation invokes that hook.

`rollback_contract` has `strategy`, positive `source_retention_seconds`, nonblank
`procedure`, and `acknowledged_data_loss_window_seconds`. The example uses
`restart-from-source`; other strategies are not independently audited here.
A declared procedure is not evidence of qualified reverse replication.

## CLI and JSON outcomes

P12b reports the command tree; R3d subsequently wires the local-fs foreground
runner. Use an explicit absolute `--state-root`. `--definition` must validate
and match the committed definition (or the positional check/init definition).
Init uses the first 24 hex characters of the definition digest as migration ID.

| Command | Current boundary |
|---|---|
| `check <definition.json>` | Wired offline validation/capability report. |
| `init <definition.json>` | Wired definition commit through the coordinator. |
| `assess <id>` | Wired offline report; live assessment unqualified. |
| `assess <id> --sample N` | Sampling not wired: exit 9; offline report/request retained, `sampling_performed=false`. |
| `start <id>` | Wired local-fs foreground runner; default `--until catching-up`. |
| `resume <id>` | Wired continuation, including Resume from Paused. |
| `pause <id>` | Wired active-owner request; no owner succeeds without lifecycle mutation. |
| `status <id> [--watch]` | Wired observer; watch is flushed NDJSON. |
| `verify <id> [--tables ...]` | Wired active-owner request; exit 6 with no owner. |
| `cutover <id> --deadline <dur>` | Wired active-owner request to `request_cutover`; exits 0 completed, 2 not ready or deadline, 3 invalid, 5 uncertain or breach, 6 no owner or infrastructure, 8 lost ownership. |
| `rollback <id> [--deadline <dur>]` | Generation-scoped owner request (default: configured downtime budget). Exits 0 after source activation, 2 after a pre-PONR deadline restores destination and capture, 3 for invalid state/eligibility/deadline, 5 for uncertainty or operator action, 6 for no owner/infrastructure, and 8 for lost ownership/stale generation. Admission is enabled. Status exposes the durable step, intent, fixed deadline and outcome; restart/takeover resumes before probes or finalize. |
| `finalize <id> [--drop-objects]` | Active-owner request from rollback-window, or destination-active after eligibility ended. Stops reverse capture; explicit `--drop-objects` removes installed objects on both endpoints. Exits 0 completed, 3 invalid, 6 no owner/infrastructure, 8 lost ownership. |
| `abort <id>` | Wired coordinator-only; exit 9 if journal history records any external effect; no database restoration. |
| `report <id>` | Wired safe trust/status reporting. |
| `buffer gc <id>` | Request one bounded retention pass from the active owner; reports deleted objects/bytes and retained manifest cursor. Exit 0 on success, 3 for invalid arguments, 6 for no/stopped owner or infrastructure failure, 8 for lost ownership/stale generation. |

Start/resume report `--until` choices `catching-up`,
`verified-ready` and `paused`. Ctrl-C/SIGINT and Unix SIGTERM request graceful stop.

All stdout outcomes use schema-version-1 JSON envelopes, including usage errors,
help and version. Status watch emits one envelope per line. The reported
`schema_version` is 1; capability results retain structured report data.
Problems/blockers provide `what`, `why`, `next_step`. The complete envelope schema
and other exact field names are unverified in the permitted excerpts.

| Exit | Outcome / interpretation |
|---|---|
| 0 | `success` |
| 1 | Undocumented in current P12b table; do not rely on the old input-error meaning. |
| 2 | `blocked` |
| 3 | `invalid-input` |
| 4 | `illegal-transition` |
| 5 | `recovery-required` |
| 6 | `infrastructure` |
| 7 | `internal` |
| 8 | `non-owner` |
| 9 | `not-wired` |

## Lifecycle and readiness

The intended lifecycle is `defined` -> `assessed` -> `copying` -> `catching-up`
-> `verified-ready` -> `source-fenced` -> `final-boundary-applied`
-> `destination-active` -> `rollback-window` -> `rolling-back`
-> `source-active-after-rollback`. Finalization ends in `finalized`;
a preactivation abort can end in `aborted-to-source`.

`paused`, `blocked`, `failed` and `recovery-required` retain write authority and
progress markers. This is a safety model, not proof all transitions are qualified.
Readiness regression returns verified-ready to catching-up, not a sticky ready flag.

Readiness is derived from fresh facts. C3b reports readiness assembly, current
capture-health gating, WAL enforcement and durable coverage guards implemented
and unit-qualified. Production readiness/WAL collectors are not connected by the
existing PG constructors. The default engine fails closed and cannot claim
verified-ready, including when `--until verified-ready` is requested. An offline
check does not establish freshness, retention health or full validation.
Confirmed conditions include policy lag, consecutive-check count, required full
validation, fresh capture health and durable coverage. The full ten-fact checklist
is unavailable in the permitted excerpts.

## Buffer durability, retention and failure domains

The create-only object-store journal is coordinator authority; embedded
`giggles-core` is its rebuildable current-state projection. Conditional-put
failure revokes ownership; unresolved ambiguous writes prohibit further effects.
Corruption requires recovery. Do not delete journal/checkpoint objects to proceed.

| Backend | Failure-domain contract / qualification boundary |
|---|---|
| Local filesystem | Depends on configured filesystem/host. Runner uses durable fsync/startup staging audit. Host/disk loss can lose buffer and journal together; not a replicated remote failure domain. |
| S3-compatible | Requires conditional create and durable readable objects. Emulator tests do not qualify provider durability, consistency or failover. Current operational runner assembly is unqualified. |
| GCS | Requires generation-conditional create and generation-bound reads; existing JSON adapter uses these contracts. Emulator fidelity and current operational runner assembly remain unqualified. |

Inspect unfinished local staging objects; do not sweep an active writer's files.
Checkpoint recovery does not scrub every older object. Old corruption below a
valid checkpoint requires explicit full-history scrub. Retain authoritative history.

E4b reports epoch-bound retention with contiguous applied prefix and both durable
checkpoints before collection, one candidate per cadence and restart proof.
Completed ChunkRows remain pinned for authentication. Outages stop joined workers;
a stale/replaced owner must not collect. An ownership-revoking journal outage
joins workers, but the pause can remain unwritten while the journal is unavailable.
Resume requires fresh-epoch handling and rejects a replacement-owner conflict;
do not infer durable Paused from unavailable journal writes.
E4b's final acceptance was blocked by a
shared runner edit, and its live PG receipt-retention qualification was not run.
R3d subsequently reports retention integrated, not a new live retention campaign.

WAL retention is separate from buffer GC. A stalled slot can retain source WAL.
Configured budgets do not prove healthy retention or server headroom. Production
WAL observations remain disconnected today; no live automatic WAL-safety guarantee
is claimed. Do not advance acknowledgments or discard required coverage merely
to relieve pressure. Coverage loss requires an explicit blocked/recovery decision.
Exact WAL-health/retention thresholds and server stop/ack policy are unverified
in the permitted evidence.

## Current PostgreSQL types and keys

K1 qualifies catalog adapters for built-in bool, integers, numeric, text, varchar,
bpchar, bytea, uuid, date, time, timetz, timestamp, timestamptz, interval, json,
jsonb and their built-in arrays. The decoder preserves numeric scale, temporal
infinities, array dimensions/lower bounds and NULLs. Both endpoints need checked
UTF8 server/client encoding; their type identities are qualified independently.
Enum, domain, extension and unsupported built-in types block. Floats and their
arrays block because text capture cannot qualify arbitrary NaN payload preservation.
No lossy fallback is claimed. K4 blocks selected generated columns; exclude and
recompute them rather than assume generated-value equivalence.

Keys must be non-null, unique, immediate, valid and ready, with old tuples available
through DEFAULT primary identity, USING INDEX identity or FULL. Only qualified
built-in default btree opclasses/families are supported. Typed range ordering
supports bool, int2/int4/int8, bytea, uuid, date, time, timestamp and timestamptz,
including infinities, composite keys and mixed direction. Text/collation-sensitive,
numeric, float, array, interval, timetz, JSON and JSONB ordering blocks. Supported
value types are not automatically supported key types. Keyless tables need a key
or explicit exclusion.

K3 provides partial-column adapters; K4 adds checked null/default/generated/identity
evidence, resolving the earlier missing-evidence collection gap. Excluded required
destination columns need a default, generation or identity. Publication column lists
and end-to-end excluded-column unreadability remain unqualified in available evidence.
Filtering decoded output does not prove excluded bytes never crossed the connection.

## Security and operator responsibilities

Require TLS and an explicit CA reference on every connection. Trust errors stop
work. Trust summaries expose configuration/reference kind, not CA identifiers or
certificate verification. R3b reports recursive CA-reference redaction in CLI
output; committed journal references remain digest-bound. Secret **values** must
never enter journal, local state or diagnostics.

The plan assigns separate least-privilege roles: source capture needs REPLICATION,
SELECT on in-scope tables and publication ownership; fencing needs ALTER ROLE,
scoped GRANT/REVOKE and `pg_signal_backend`; destination apply needs its required
writes; verification is read-only on both endpoints. Exact deployment grants,
over-broad-role warnings, hook isolation and whole-run secret sweeps need live
qualification. This README does not certify all planned security gates exercised.

Superusers, owners and out-of-scope principals can bypass role fencing.
Acknowledgment does not prevent writes. DDL drift, source identity and retained
history changes are safety boundaries. Do not restore source writes after
destination activation without a qualified rollback procedure/authority decision.

## Connection poolers: planned, unqualified

The design expects PgBouncer or similar to retain server connections and reconnect
after `pg_terminate_backend`; reconnects should fail under NOLOGIN. Clients see
errors, counting as application interruption. The fence must be proven in both
session and transaction pooling modes. These are planned requirements;
pooler behavior was not qualified by this work.

## Explicit bounds and residual risks

The local profile is limited to at most 1 GB; no larger-scale qualification is
claimed. Coordinator core limits are a 16 MiB envelope and 64 KiB row. Admission
allows 12 MiB estimated state and 60 KiB charged row payload. Current-state budgets
are 12,000 chunks/4 MiB, 12,000 verifier leaves/6 MiB, 1,024 exceptions/512 KiB,
64 active effects/512 KiB, 32 progress streams/64 KiB, and 512 KiB each for definition
and saved privileges. Definitions/privileges use 24 KiB fragments. These are not
throughput guarantees.

Residual risks include privileged fence bypass, pooler/client retry behavior,
managed-PG privileges, emulator/provider differences, object-store latency during
cutover, DDL drift, WAL exhaustion and unqualified live retention/recovery
interactions. Ordinary local tests do not substitute for real-app, chaos and
independent-history gates.

## What is not claimed

- Zero downtime: the design targets measured minimal interruption.
- Qualification beyond the local profile or above 1 GB.
- MySQL, Spanner or GigglesDB connectors; these are later phases.
- Production cloud object-store, managed-PG or cloud-failover qualification.
- A completed live PG16/17/18 migration, cutover, rollback or pooler campaign.
- Live PG qualification by this docs work or the cited runner/catalog tasks;
  gated live cases were not run.
- Verified-ready while default production collectors are absent.
- Source-side unreadability of excluded columns from adapter filtering alone.
- Safe side effects merely because a command parses or a definition validates.

## Schema changes during a migration

Milestone 1 prohibits `CREATE INDEX CONCURRENTLY`, `DROP INDEX CONCURRENTLY`,
`REINDEX CONCURRENTLY`, and `ALTER TABLE ... DETACH PARTITION CONCURRENTLY` on
in-scope tables. Quiesce schema changes before starting and throughout migration.

Source preflight reports named invalid/unready indexes, active index builds and
pending partition detaches. Slot setup fingerprints scoped table catalogs
immediately before and after slot creation; any difference or hazard fails
closed. The accepted 32-hex baseline is journaled durably. Readiness watermarks
and the final cutover marker repeat the fingerprint and hazard probe. Drift,
hazards or invalid fingerprints set a durable sticky blocker with the reason, observed
value and poll point. It survives restart and prevents apply admission,
readiness and cutover; `skedaddle status` exposes it and recovery instructions.

Failed probes fail the current attempt without persisting a blocker. A migration
started before this version has no stored baseline and refuses to resume; restart
it from a fresh snapshot.

Recovery: quiesce schema changes, resolve the named hazards, and start a new
migration from a fresh snapshot. Returning catalogs to the baseline does not
clear a blocker. There is no re-baselining. Any scoped drift currently blocks,
including transactional DDL captured by the end trigger (limitation F10).
Polling can miss transient changes that leave no catalog difference between
polls; it cannot identify the first affected commit. A `ddl_command_start`
guard is a planned follow-up, and is not installed in milestone 1.

The operational fingerprint excludes roles, ownership, ACLs, publications and
Skedaddle administration. Relation OIDs intentionally bind table identity so
that drop/recreate blocks; storage rewrites alone do not change the hash.
