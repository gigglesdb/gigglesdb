S2 / GD-0049 D11 source write fencing

All implementation is confined to this folder; generated source reuses S1's
connection, TLS/SCRAM, pgoutput v1 decoder and explicit flush controls. Run outputs
are artifacts/s2-<run-id>. No Archdocs/Linear writes, commits or pushes. If ever
writing GigglesDB Archdocs, the mandatory space is 5714972c-8f62-4f17-8ebc-b62fbf4420b9.

PowerShell, cwd K:/Projects/gigglesdb/crates/skedaddle:
  python -B artifacts/spikes/s2-fencing/build-source.py
  python -B artifacts/spikes/s2-fencing/make-runner.py
  $env:CARGO_BUILD_JOBS='2'
  $env:CARGO_TARGET_DIR='K:/Projects/gigglesdb/crates/skedaddle/target/s2'
  $env:CARGO_HOME='K:/Projects/gigglesdb/crates/skedaddle/artifacts/spikes/s1-replication/cargo-home'
  $env:TEMP=$env:CARGO_TARGET_DIR
  $env:TMP=$env:TEMP
  cargo build --offline --locked --manifest-path artifacts/spikes/s2-fencing/Cargo.toml
  python -B artifacts/spikes/s2-fencing/run.py 16 <fresh-run-id>
  python -B artifacts/spikes/s2-fencing/run.py 17 <different-fresh-run-id>
  python -B artifacts/spikes/s2-fencing/run.py 18 <different-fresh-run-id>

Versions execute one at a time. run.py adapts S1's TLS setup and S4's pinned
images, disk guard, Compose lifecycle, export, down, and run-label checks. It
NEVER invokes sweep or unscoped cleanup. New poolsession service and the
transaction-mode pgbouncer service use dynamic loopback ports. Only their
frontend test clients use plaintext; PG backend/replication uses TLS/SCRAM.
S2 uses wal_sender_timeout=60s; S1's 4s keepalive torture setting can expire
while a multi-client S2 workload is running under parallel-spike contention.

Fixture: nine application login roles, two intermediary group roles, two tables,
an out-of-scope owner, a superuser, an out-of-scope ACL writer and an ACL writer
with BYPASSRLS. Recursive pg_auth_members closure includes NOINHERIT/SET paths;
table ACL grantees reached through that closure are revoked. Grantors and grant
options are saved and restored, as are original LOGIN flags. The fixture verifies
grant-option restoration. Residual roles are reported, not silently fenced.

One scenario trial, five idle trials and five loaded trials per PG version.
Load is twelve independent clients with 10ms pacing for 1.5s before fence. A
scheduled-job simulation is a background session issuing acknowledged writes.
Pause is wall time just before step 3 through the acknowledged step-8 COMMIT,
including SQL/evidence overhead. The scenario trial is excluded from pause
statistics. Report p95 by nearest rank; with five samples it equals max.

NOLOGIN commits first, then recursive ACL revoke, session termination and a
bounded no-app-backend / no-prepared-transaction drain. Step 8 repeats the last
REVOKE, writes authority/epoch and emits a transactional final marker in ONE SQL
transaction. Records retain marker LSN separately from decoded commit LSN.
The final marker's commit must be decoded before the oracle is evaluated.

The logical slot is created before workload; the S1-derived origin='none' stream
opens after drain and replays that slot to reconcile the complete experiment.
A second origin='none' detector starts exactly at the drain LSN before the four
positive residual writes, and independently verifies those post-drain commits.
An application breach is a decoded commit_lsn > drain_lsn, bounded by the WAL
position before unfence. Every completed fence window is rechecked at the end.
Positive residual writes deliberately breach and must be caught by the detector.
Client ACKs, DB row IDs and decoded insert IDs are compared, with any committed
but unacknowledged in-flight writes reported explicitly. No WAL flush ACK is
advanced until the final checkpoint is fsynced.

Prepared-transaction negative test uses only s2_owned_prepared_blocker in this
run's own ephemeral DB. Fence must refuse authority advance. Only that fixture-
owned GID is rolled back by its creator; no other's prepared xact is resolved.

events.jsonl records SQL, timings, output/error rows, every decoded record and
assertion. summary.json contains pause samples, trial oracles and client errors.
checkpoint-*.json persists full replay before feedback. Compressed S4 export
retains server/PgBouncer logs and inspect evidence. leftovers.json is scoped to
this run ID and must list empty containers, volumes and networks.

Author's absolute deletion ban is honored. All files and directories, including
failed-attempt evidence, empty temp folders and Cargo output, are retained.
