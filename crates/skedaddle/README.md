# Skedaddle

The coordinator pins giggles-core to commit edc519b05357a6b240c313c0af3c0ced1ed090d5 via this repository's local Git URL.

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

## P2 coordinator

`coordinator` supplies immutable create-only journal entries, SHA-256 predecessor
validation, checked checkpoints, a rebuildable giggles-core projection, ownership
epochs, a total lifecycle function, and readiness from all ten required facts.
Journal integrity errors return `RecoveryRequired`; local state never repairs or
replaces journal history. A failed conditional put revokes ownership. Ambiguous
puts re-read the exact key, and unresolved outcomes prohibit effects.

External effects use `Coordinator::effect`: the intent is committed and applied
locally first; the callback receives its `(epoch, seq)` idempotency key. The PG
authority helper performs the epoch update, SQL effect, and idempotency-ledger
write in one transaction. Connectors must initialize `_skedaddle.authority` in
both databases during explicit setup and execute effects through that gate.
This is coordinator infrastructure; the migration CLI and connectors arrive in
later phases.

`Coordinator` publishes a full checkpoint every 1,000 transitions by default;
`with_checkpoint_interval` changes this positive interval. Its explicit
`checkpoint()` method also checks ownership. Full checkpoints embed a core
export, using core's public export/restore APIs. On a new host,
`Local::recover` restores that export and applies only the uncheckpointed tail.
No private core formats are constructed and no core source is changed.

Checkpoint objects are create-only and self-digested, and record sequence N and
the digest of journal object N. A present anchor must match. Corruption or a
missing published checkpoint stops recovery; it never falls back to an older
checkpoint. A separate create-only discovery index at
`<prefix>/checkpoint-index/<id>/<ordinal:020>.json` uses dense ordinals and
exponential/binary GETs. Publishing the index record makes a checkpoint
discoverable. An interrupted publication may leave an unindexed derived object;
the full scrub checks these as well. Recovery never lists the journal prefix:
it trusts the validated snapshot prefix and reads N+1 upward by key until
NotFound, requiring every tail entry to link to the recorded head digest.

Use `journal.load(true)` for checkpoint recovery, and `journal.scrub()` (or
`load(false)`) for an explicit full-history integrity check. Old-object
corruption below a valid checkpoint does not block checkpoint recovery, and is
detected by the scrub. Both paths have separate real-filesystem tests. The
100k test asserts zero checkpoint-path LIST calls, bounded GETs, and recovery
time below half the full-scrub time. Work is bounded by the materialized snapshot
and its uncheckpointed tail, rather than fetching every historical object.

The local tables include the §3.4 columns. Singleton and composite records add
stable primary-key columns (`id`); the chunk column `table` is `table_name` in SQL.
Audit stores complete transitions, while timestamp/epoch/chain metadata remains
in the journal. Collection reads use keyset paging in a core read transaction,
with a reverse-scan boundary proof and explicit errors on core result limits.

From the repository root, run the real-backend, PG18, Toxiproxy, crash and long-run
tiers with one command (all generated files and retained scratch go to J:):

```powershell
crates/skedaddle/harness/run.ps1 p2
```

Plain `cargo test` reports Docker and long-run cases as **ignored with an explicit
reason**. Enable `--features test-crash` to run subprocess seams independently;
the fixture executable and seam controls do not exist in normal builds. No
environment variable can select a production crash seam. Harness endpoint JSON
is passed only to integration tests via `SKEDADDLE_TEST_CONFIG`.

The harness checks host and Docker disk headroom before startup, uses pinned
P0 image digests, ephemeral loopback ports, bounded logging, and resources labeled
`skedaddle.test=<run-id>` in `skedaddle-<run-id>`. Its `finally`/signal teardown
revalidates both labels before touching resources. It preserves every host file
and image. After an interrupted run, tear down only its exact label:

```powershell
crates/skedaddle/harness/cleanup.ps1 -RunId <run-id> -TaskRoot <same-J-task-folder>
crates/skedaddle/harness/cleanup.ps1 -Check
crates/skedaddle/harness/run.ps1 mutants
```

Mutation runs require P0's installed cargo-mutants 27.1.0, operate in place, and
restore source after each mutation. Do not edit the crate while they run. Logs
and scratch are retained under a fresh J: output directory. Five disjoint source
scopes run the relevant reference-model, real-store, PG and subprocess tests;
the 100k fixture and local paging stress are omitted from repeated iterations.
Use `-MutationSuite state|epoch|journal-chain|journal-io|gcs` to rerun one scope.
Survivors or mutant timeouts keep a nonzero exit status and require review;
the harness continues the other scopes and preserves every outcome.

The pinned fake-gcs-server does not implement object_store's XML upload path.
`coordinator::gcs::GcsJsonStore` supplies an append-only ObjectStore adapter over
the real GCS JSON API: media uploads use `ifGenerationMatch=0`, GETs bind to the
metadata generation, and listing follows every page token. It reuses
object_store's GCS credential provider. The same real emulator runs the journal
race and range/paging tests; no fake storage implementation is used. HTTP is
available only through the explicit emulator constructor. Overwrite, deletion,
copy, rename, multipart, tags and attributes return unsupported errors.
The local backend uses unmodified crates.io `object_store =0.14.2`, including
upstream staging cleanup. Configure local stores through
`coordinator::journal::local_store`: it enables fsync and reports unfinished
`<object>#<number>` uploads at startup. Inspect reported files before retrying;
they may belong to an active writer, so startup never deletes them.

The core dependency is pinned to committed revision
`edc519b05357a6b240c313c0af3c0ced1ed090d5`, independent of working-tree edits.
Checkpoint export and restore use only its public APIs. Skedaddle confines
archives and core staging files to `<database>.skedaddle-scratch` beside the
configured database directory, then removes its own scratch files and directory
on success or error. That sibling path is reserved for Skedaddle. A crash leaves
identifiable files there; the next create, open, checkpoint or recovery reports
the path and stops for inspection. Existing scratch is never adopted or swept.
The harness audits retained test roots for staging and archive leaks; only the
explicit crash-orphan fixtures may retain the files their assertions identify.

`retry_effect` retries within an epoch. After failover, `resume_effect` journals
an adoption under the new owner before calling a peer. Tokens retain the original
`(epoch, seq)` idempotency key and carry a separate `authority_epoch()` for
fencing. The PG helper updates authority and deduplicates the original key in
the same transaction as the SQL effect. Both records also carry the durable
journal namespace, preventing cross-migration deduplication or fencing. The
namespace must identify one migration consistently against a database. P10 owns
installation policy for overlapping migration scopes. Coordinator labels get
internal UUID identities, so reusing a display label cannot create two owners.
Subprocess PG tests crash before and after the effect commit and prove that
adoption produces one business write. A real Toxiproxy test drops the COMMIT
reply, checks ownership revocation, and resumes without duplicating that write.

The 100k recovery fixture uses bounded concurrent atomic creates with fsync
disabled only for fixture setup; crash tests use fsync. Its timings describe
journal recovery, not sequential durable-append throughput or 100k-row core
materialization. A separate 1,100-row real-core test asserts export/restore is
faster than rebuilding and compares every current chunk row. The embedded core
has a 16 MiB envelope and a 64 KiB row limit. The local profile admits at most
12 MiB of conservatively estimated encoded state and 60 KiB of charged row
payload, leaving headroom below both core limits. Budget rejection is typed
(`BudgetExceeded`) and carries a stable `local-budget.*` blocker and remediation;
coordinator admission runs before the authoritative journal put.

Local audit rows and `Materialized.audit` contain only active original/current
intents and the latest replay anchor. Completed effects and historical status
must use `Journal::history_page_through(after, limit, verified_head)` to detect
gaps against a stable head (at most 128 objects and a 1 MiB page target; a first large definition can exceed the target). `Journal::entry`
reads a single historical record. Full audit history remains in the object-store
journal. Never discard journal objects to reduce local projection size.

Current-state budgets are 12,000 chunks / 4 MiB; 12,000 verifier leaf ranges /
6 MiB; 1,024 verifier exceptions / 512 KiB; 64 active effects / 512 KiB of
entries; 32 progress streams / 64 KiB; and 512 KiB each for the definition and
saved privileges. Range deletion and exception resolution are explicit journal
transitions. Completed split parents must be retired; retaining an entire
bisection tree consumes the range budget. Definitions and privileges are split
into UTF-8-safe 24 KiB fragments, avoiding a single oversized core row. `check`
reports an oversized definition as a Critical capability blocker. A clone of
Materialized contains only this bounded current state, capped at 16 MiB of JSON.
The RW4 sizing test uses D2's exact row arithmetic with 10,000 chunks, 10,000
current verifier ranges and the final markers from 14,400 ticks; a separate
simulation applies all 43,200 marker events and proves history does not grow.

The compact checkpoint format adds current verifier state and active-intent
roots. Default recovery remains strict: both `load(true)` and `load(false)` /
`scrub()` reject a P2 full-history checkpoint. `load(false)` is a full integrity
scrub, not a legacy-format bypass. Do not remove checkpoint or index objects to
make recovery proceed.

An explicit maintenance rebuild is available when the complete raw journal is
retained. This changes the physical store, so first stop every coordinator and
arrange that the old journal/discovery prefixes cannot accept further writes.
Preserve those objects and the old local cache as read-only evidence. Configure a replacement
store with empty journal/discovery prefixes and the **same prefix and migration
ID**; the unchanged journal namespace preserves peer effect idempotency keys.
Then, using `old` and `replacement` Journal handles and a new local path:

```rust,ignore
let state = old.copy_entries_for_rebuild(&replacement).await?;
let local = Local::rebuild(new_local_path, &state)?;
local.checkpoint(&replacement).await?;
```

This explicitly ignores derived checkpoint contents and copies the gap-free,
digest/ownership-validated raw entries byte-for-byte with create-only writes.
It checks that raw history reaches the published discovery head. Missing raw
history must be restored before rebuilding. A failed copy leaves the original
unchanged and may leave an unactivated partial destination; retry with another
empty store. Only after success should all coordinators be redirected to the
replacement store and new local cache, followed by normal ownership acquisition.
Never allow journal writers against both stores. This copies journal metadata
only: it does not move buffer/data objects or rewrite definitions/references;
those objects and their existing connector contracts must remain available.
Ordinary checkpoint recovery and full
scrub of the replacement are strict again; no automatic fallback is introduced.

An open cache must match its supplied verified snapshot; `Local::open_journal`
reopens a lagging current-format cache using paged prefix verification.
`Local::recover` restores a current-format checkpoint and updates its rows to
the verified journal head. The dev profile retains
unoptimized code and debug assertions with debug symbols disabled, reducing
mutation linking cost. No production or larger-scale qualification is claimed.
