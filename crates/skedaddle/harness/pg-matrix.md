# PostgreSQL TLS/SCRAM matrix

Run only after the coordinator assigns the Docker lane. This phase starts
`pg-source`, `pg-destination`, and a separate `pg-replica-wal` negative control.
`-PgVersion 16`, `17`, or `18` selects the existing digest-pinned S4 image lock.
Source and destination always use `wal_level=logical`. The negative control
uses `replica`, as required by `wal_level_replica_blocks`.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File crates/skedaddle/harness/run.ps1 -Phase pg-matrix -PgVersion 16 -TaskRoot J:/gigglesdb-work/gd49-h1-FRESH-pg16
```

Use a fresh task directory per invocation and serialize versions: they share
`J:/gigglesdb-work/gd49-target-b`. The phase sets the shared sccache variables,
two build jobs, and J: TEMP/TMP/scratch. It runs the disk guard before any
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
| `tests/pg_replication.rs` | CONFIG; `pg`; `replication.{host,port,server_name,user,password,database,ca_pem}` | All provided; TLS client blocker below |
| `tests/pg_source.rs` | CONFIG; `pg`; same seven `replication` fields | All provided; TLS client blocker |
| `tests/pg_apply.rs` | CONFIG; `pg_apply` | Provided, explicit destination admin; TLS client blocker |
| `tests/pg_preflight.rs` | CONFIG; `pg`; `pg_replica_wal` | Both provided, separate logical/replica servers; TLS client blocker |
| `tests/pg_chunk.rs` | CONFIG; `pg_chunk`, fallback `pg` | Both provided; capture role; TLS client blocker |
| `tests/pg_ddl.rs` | `SKEDADDLE_PG_DDL_URL` | Provided in `env` and exported; `pg_ddl` JSON alias; TLS client blocker |
| `tests/pg_fence.rs` | `SKEDADDLE_PG_FENCE_DSN`; optional `SKEDADDLE_PSQL` (default `psql` on PATH) | DSN provided/exported, `pg_fence` alias; host psql prerequisite; TLS client blocker |
| `tests/pg_activate.rs` | `SKEDADDLE_PG_ACTIVATE_DSN` | Provided/exported, `pg_activate` alias; TLS client blocker |
| `tests/pg_reverse.rs` | `SKEDADDLE_PG_REVERSE_SOURCE_URL`, `SKEDADDLE_PG_REVERSE_DESTINATION_URL` | Both provided/exported; `pg_reverse.{source,destination}` aliases; TLS client blocker |
| `tests/verify_pg.rs` | CONFIG; recursive source/destination DSN lookup, or `SKEDADDLE_VERIFY_SOURCE_URL`, `SKEDADDLE_VERIFY_DESTINATION_URL` | Explicit env overrides and `source`/`destination` plus `verify_pg` aliases provided; TLS client blocker |
| `tests/cutover.rs` | `SKEDADDLE_CUTOVER_SOURCE_URL`, `SKEDADDLE_CUTOVER_DESTINATION_URL` | Both provided/exported; `cutover.{source,destination}` aliases; TLS client blocker |
| `tests/rollback.rs` | `SKEDADDLE_ROLLBACK_PG_SOURCE_URL`, `SKEDADDLE_ROLLBACK_PG_DESTINATION_URL` | Both provided/exported; `rollback.{source,destination}` aliases; TLS client blocker |
| `tests/engine.rs` | File absent at H1 inspection; contract cannot be collected | GAP: speculative `engine.{source,destination}` aliases supplied; not qualified; phase fails explicitly if absent |
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
or verify_pg. `engine` privileges cannot be determined without its file.

| Superuser gate | Live tests using it |
| --- | --- |
| `pg_apply` | `exactly_one_effect_after_crash_matrix`, `ambiguous_commit_resolved_by_progress`, `digest_mismatch_on_skip_is_error`, `completed_range_missing_row_is_fault` |
| `pg_ddl` | `supported_ddl_follows_boundary`, `unsupported_ddl_blocks_at_boundary`, `rolled_back_ddl_emits_nothing`, `no_event_trigger_profile_blocks_on_drift` |
| `pg_fence` | `stale_pooled_session_cannot_commit_after_fence`, `in_flight_txn_drained_or_blocks`, `prepared_xact_blocks`, `residual_superuser_reported_and_detected`, `abort_before_activation_restores_source`, `authority_is_migration_scoped_monotonic_and_atomic_with_final_fence` |
| `pg_activate` | `destination_app_roles_cannot_write_before_activation`, `sequence_sync_exact`, `invalid_index_blocks` |
| `pg_reverse` | `reverse_slot_covers_first_destination_write`, `no_replication_loop_either_direction`, `non_reversible_value_ends_eligibility`, `unsupported_destination_ddl_ends_eligibility` |
| `cutover` | `abort_before_activation_restores_source`, `measured_interruption_reported` |
| `rollback::real_pg` | `rollback_preserves_destination_writes`, `rollback_restores_source_privileges_exactly`, `crash_during_rollback_recovers`, `finalize_drops_all_installed_objects` |

## Known qualification blockers

Every inspected live SQL setup/driver currently uses `tokio_postgres::NoTls`,
including `pg_replication`/`pg_source` administration (their replication stream
already uses TLS). `NoTls` cannot connect to this fixture's TLS-only TCP
endpoints. Supplying a TLS DSN does not add a connector. These clients need TLS
adapters loading the supplied CA, outside H1's harness-only edit boundary.
The fixture deliberately retains plaintext rejection; no passing live suite is
claimed. The source unit tests are not a replacement for live qualification.
`tests/engine.rs` is also absent, and the future phase records this gap and
returns failure. A host `psql` executable is required by pg_fence.

The future phase runs each integration binary twice, ordinary env-gated tests
and `--ignored`, with `--no-fail-fast --nocapture --test-threads=1`, preserving
each cargo transcript and its libtest totals. It includes the source unit
module and engine when present. Test failures do not suppress later binaries.
On all exits, bounded container logs precede teardown of only exact matching
run labels and Compose project labels; PowerShell finally invokes
`cleanup.ps1 -Check`. No cross-run sweeper is invoked.
