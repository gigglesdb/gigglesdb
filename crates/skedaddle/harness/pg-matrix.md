# PostgreSQL TLS/SCRAM matrix

Run only after the coordinator assigns the Docker lane. This phase starts
`pg-source`, `pg-destination`, and a separate `pg-replica-wal` negative control.
`-PgVersion 16`, `17`, or `18` selects the existing digest-pinned S4 image lock.
Source and destination always use `wal_level=logical`. The negative control
uses `replica`, as required by `wal_level_replica_blocks`.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File crates/skedaddle/harness/run.ps1 -Phase pg-matrix -PgVersion 16 -TaskRoot J:/gigglesdb-work/gd49-h1-FRESH-pg16
```

Use a fresh task directory per invocation and serialize versions. Set
`CARGO_TARGET_DIR` to the assigned stable lane target; otherwise it defaults to
`<TaskRoot>/target`. The phase sets the shared sccache variables,
four build jobs, and J: TEMP/TMP/scratch. It runs the disk guard before any
resource creation or image pull. The fixture has no host data binds; harness
scripts are mounted read-only. Each labeled data volume has a hard 256 MiB
tmpfs cap (768 MiB total, including WAL, TLS and extracted tools). WAL retention
is capped at 64 MiB per slot and `logical_decoding_work_mem` is 64 KiB.

`pg-start.sh` follows S1's Alpine method: record the repository package URL and
index entry, extract only `usr/bin/openssl`, and generate the CA/server private
keys inside the labeled volume before the PostgreSQL entrypoint starts. Only
public CA PEMs (plus their public bundle) and package provenance reach J:.
Socket administration is local trust; every TCP allow rule is hostssl SCRAM;
hostnossl is rejected. Startup probes validate hostname/CA, TLS, SCRAM catalog
policy, PostgreSQL major version, WAL mode, and actual plaintext rejection.

The file named by `SKEDADDLE_TEST_CONFIG` is created exclusively under the run's
task folder. If unset, the phase names it `config-<run-id>.json`. Legacy gates
are stored under `env` in that JSON and exported to every cargo child. DSNs use
`sslmode=require`, which tokio-postgres accepts; TLS adapters must use the
corresponding public `ca_pem` and validate `localhost`. libpq receives
`PGSSLMODE=verify-full` and `PGSSLROOTCERT` pointing at the public CA bundle.

## Gated test key coverage

`ROOT` below means `SKEDADDLE_TEST_ROOT`, provided to every test. `CONFIG` means
`SKEDADDLE_TEST_CONFIG`. All rows use ROOT, either directly or for test scratch.

| Gated test file | Keys / environment gates read | Fixture coverage |
| --- | --- | --- |
| `tests/pg_replication.rs` | CONFIG; `pg`; `replication.{host,port,server_name,user,password,database,ca_pem}` | All provided; TLS enabled |
| `tests/pg_source.rs` | CONFIG; `pg`; same seven `replication` fields | All provided; TLS enabled |
| `tests/pg_apply.rs` | CONFIG; `pg_apply` | Provided, explicit destination admin; TLS enabled |
| `tests/pg_preflight.rs` | CONFIG; `pg`; `pg_replica_wal` | Both provided, separate logical/replica servers; TLS enabled |
| `tests/pg_chunk.rs` | CONFIG; `pg_chunk`, fallback `pg` | Both provided; capture role; TLS enabled |
| `tests/pg_ddl.rs` | `SKEDADDLE_PG_DDL_URL` | Provided in `env` and exported; `pg_ddl` JSON alias; TLS enabled |
| `tests/pg_fence.rs` | `SKEDADDLE_PG_FENCE_DSN`; optional `SKEDADDLE_PSQL` (default `psql` on PATH) | DSN provided/exported, `pg_fence` alias; pinned host psql precheck; TLS enabled |
| `tests/pg_activate.rs` | `SKEDADDLE_PG_ACTIVATE_DSN` | Provided/exported, `pg_activate` alias; TLS enabled |
| `tests/pg_reverse.rs` | `SKEDADDLE_PG_REVERSE_SOURCE_URL`, `SKEDADDLE_PG_REVERSE_DESTINATION_URL` | Both provided/exported; `pg_reverse.{source,destination}` aliases; TLS enabled |
| `tests/verify_pg.rs` | CONFIG; recursive source/destination DSN lookup, or `SKEDADDLE_VERIFY_SOURCE_URL`, `SKEDADDLE_VERIFY_DESTINATION_URL` | Explicit env overrides and `source`/`destination` plus `verify_pg` aliases provided; TLS enabled |
| `tests/cutover.rs` | `SKEDADDLE_CUTOVER_SOURCE_URL`, `SKEDADDLE_CUTOVER_DESTINATION_URL` | Both provided/exported; `cutover.{source,destination}` aliases; TLS enabled |
| `tests/rollback.rs` | `SKEDADDLE_ROLLBACK_PG_SOURCE_URL`, `SKEDADDLE_ROLLBACK_PG_DESTINATION_URL` | Dedicated fresh database pair; env and `rollback.{source,destination}` aliases; TLS enabled |
| `tests/engine.rs` | CONFIG; `engine.{source,destination}` | Writable setup endpoints and TLS CA supplied |
| `src/pg/source/tests.rs` | ROOT; no PostgreSQL config lookups in grep results | Provided; source unit tests included separately |

## Privilege assignments

Both servers initialize separate non-superuser roles:

| Role | Scope / privileges |
| --- | --- |
| `skedaddle_capture` | REPLICATION, schema/database CREATE for test publication ownership, SELECT on in-scope fixture tables; owns `harness_capture` |
| `skedaddle_fence` | CREATEROLE, ADMIN OPTION over only `harness_app`, `pg_signal_backend`, grant options over `harness_scope.items` |
| `skedaddle_apply` | DML on `harness_scope.items`; SET on `session_replication_role`; no CREATE/REPLICATION/CREATEROLE |
| `skedaddle_verify` | SELECT on in-scope fixture tables and schema USAGE; default transactions read-only; no CREATE/TEMP/DML/role administration |
| `skedaddle_fixture` | Writable test setup owner with REPLICATION for the shared `pg` gate; no superuser |
| `skedaddle_setup` | Writable source/destination setup for verify tests, which create and mutate their own fixtures; no superuser/REPLICATION/CREATEROLE |

`roles.<pg-source|pg-destination>.{capture,fence,apply,verify}` exposes the four
operational DSNs separately. The verify tests currently share one connection
for fixture writes and verification; they therefore receive the writable setup
role. A future test change must separate setup and read-only verify connections
to qualify the operational verify role itself.

The following live test files explicitly request superuser in their gate or
fixture documentation and receive `postgres` only for those gates:
`pg_apply` (all four ignored live tests), `pg_ddl` (all four live tests, including
event-trigger installation), `pg_fence` (all live fixtures, including its
explicit rolsuper assertion), `pg_activate` (all three live tests, including
database creation), `pg_reverse`, `cutover`, and the real-PG module in `rollback`.
No superuser is assigned to `pg`, replication capture, pg_chunk, preflight,
or verify_pg. `engine` uses the writable `skedaddle_setup` endpoints.

| Superuser gate | Live tests using it |
| --- | --- |
| `pg_apply` | `exactly_one_effect_after_crash_matrix`, `ambiguous_commit_resolved_by_progress`, `digest_mismatch_on_skip_is_error`, `completed_range_missing_row_is_fault` |
| `pg_ddl` | `supported_ddl_follows_boundary`, `unsupported_ddl_blocks_at_boundary`, `rolled_back_ddl_emits_nothing`, `no_event_trigger_profile_blocks_on_drift` |
| `pg_fence` | `stale_pooled_session_cannot_commit_after_fence`, `in_flight_txn_drained_or_blocks`, `prepared_xact_blocks`, `residual_superuser_reported_and_detected`, `abort_before_activation_restores_source`, `authority_is_migration_scoped_monotonic_and_atomic_with_final_fence` |
| `pg_activate` | `destination_app_roles_cannot_write_before_activation`, `sequence_sync_exact`, `invalid_index_blocks` |
| `pg_reverse` | `reverse_slot_covers_first_destination_write`, `no_replication_loop_either_direction`, `non_reversible_value_ends_eligibility`, `unsupported_destination_ddl_ends_eligibility` |
| `cutover` | `abort_before_activation_restores_source`, `measured_interruption_reported` |
| `rollback::real_pg` | `rollback_preserves_destination_writes`, `rollback_restores_source_privileges_exactly`, `crash_during_rollback_recovers`, `finalize_drops_all_installed_objects` |

## Native Windows client prerequisite

Provision once with `powershell -NoProfile -ExecutionPolicy Bypass -File
crates/skedaddle/harness/provision-pg-client.ps1 -TaskRoot
J:/gigglesdb-work/<fresh-provision-task>`. `pg-client.lock` pins the official
EDB Windows x64 zip URL, PostgreSQL 16.15-1 version and SHA-256. The script
verifies the archive before extracting psql.exe and its bundled DLLs under
`J:/gigglesdb-work/tools/pg16-client/bin`; it never changes PATH. The checksum
is an archive-content pin obtained over official HTTPS, not a signed checksum.
Both entrypoints set `SKEDADDLE_PSQL` to that absolute executable. Python runs
`psql --version` before guard/startup/database mutation and gives a provisioning
instruction if the executable, DLLs, or expected version are unavailable.
TLS remains verify-full with the exported public CA bundle.

## Isolation and WAL privileges

`pg-init.sql` creates `skedaddle_rollback` on each server, accessible only to
its explicit admin. Both rollback legacy gates and JSON aliases select these
fresh databases, so earlier binaries' `_skedaddle` tables cannot contaminate
rollback. Its protective assertion remains intact; no shared schema is dropped.

Preflight's allocated-WAL measurement requires `EXECUTE ON FUNCTION
pg_catalog.pg_ls_waldir()` in each endpoint database. The fixture grants it to
`skedaddle_fixture` (the preflight gate) and `skedaddle_setup` (engine endpoints).
No `pg_monitor` membership is granted. This measures allocated WAL, not free disk.

## Remaining qualification boundaries

The current live adapters support the fixture's TLS-only endpoints; the old
H1 NoTls and missing-engine blockers no longer apply. Product issues tracked
by the PG16 triage (DDL JSON decoding, aborted-origin apply progress, Windows
buffer filenames and combined reverse-readiness probing) are separate from
these harness repairs. Consult each run's qualification report for exact results.

The phase runs each integration binary twice, ordinary env-gated tests
and `--ignored`, with `--no-fail-fast --nocapture --test-threads=1`, preserving
each cargo transcript and its libtest totals. It includes the source unit
module and engine when present. Test failures do not suppress later binaries.
On all exits, bounded container logs precede teardown of only exact matching
run labels and Compose project labels; PowerShell finally invokes
`cleanup.ps1 -Check`. No cross-run sweeper is invoked.
