S1: PostgreSQL replication subset / GD-0049

Read ../s4-harness/README.md first. No commits, external-document writes, host
file deletion, image removal, or pruning. All host outputs stay under skedaddle.
Only run-created Docker resources are removed, through S4's down/check functions.
The S1 adapter deliberately does not invoke the S4 cross-run sweeper.

PowerShell, from K:\Projects\gigglesdb:
  $env:CARGO_BUILD_JOBS='2'
  $env:CARGO_TARGET_DIR='K:/Projects/gigglesdb/crates/skedaddle/target'
  $env:CARGO_HOME='K:/Projects/gigglesdb/crates/skedaddle/artifacts/spikes/s1-replication/cargo-home'
  $env:TEMP=$env:CARGO_TARGET_DIR
  $env:TMP=$env:TEMP
  cargo build --locked --manifest-path crates/skedaddle/artifacts/spikes/s1-replication/Cargo.toml
  cargo test --locked --manifest-path crates/skedaddle/artifacts/spikes/s1-replication/Cargo.toml
  python -B crates/skedaddle/artifacts/spikes/s1-replication/run.py 16
  python -B crates/skedaddle/artifacts/spikes/s1-replication/run.py 17
  python -B crates/skedaddle/artifacts/spikes/s1-replication/run.py 18
  powershell -NoProfile -File crates/skedaddle/artifacts/spikes/s4-harness/cleanup.ps1 -Check
  cargo check --locked --manifest-path crates/skedaddle/artifacts/spikes/s1-replication/survey-compile/Cargo.toml

run.py accepts an optional fresh run ID; it exclusively creates artifacts/s1-<id>.
Never reuse IDs. Disk guard runs before container startup. S4's digest-pinned
Compose definition creates and labels every service, network and volume. The
override publishes PG and Toxiproxy on dynamic loopback ports, without host data
binds. PG16/17/18 execute one at a time and teardown precedes the next version.

The PG Alpine image includes libssl but lacks the CLI. run.py reads its Alpine
repository index and extracts only usr/bin/openssl into a new container directory
using tar -k. It records the package URL and index entry. The CLI generates its
private key/certificate inside the labeled PG volume. The host retains only the
public certificate for rustls trust-anchor + hostname validation. New pg_hba rules
require hostssl scram-sha-256 and reject hostnossl; local socket trust permits S4
administration. Only this labeled PG container is restarted to apply hba_file;
Docker Desktop, its daemon, and WSL are never restarted or maintained.

The probe intentionally implements ordinary SQL over the same protocol subset;
tokio-postgres is not needed. It uses postgres-protocol 0.6.12 frontend/SCRAM
helpers plus a bounded, cancellation-safe frame buffer and a pgoutput v1 decoder.
No automatic flush advancement occurs. Marker transactions and their checkpoints
are sync_all/fsynced before feedback advances. Keepalive replies preserve that
durable position. Tests cover ten record types, independent flush/write, clean
and Toxiproxy replay, LSN seek behavior, origin privileges/filtering, TOAST and
commit-only spilled transactions with all row IDs, lengths and hashes verified.
All payloads used by this spike are text; production arbitrary-byte Message
handling, fuzzing, long soak, channel binding and crash recovery remain separate.

verify.py / capture-checks.py / summarize.py accept an optional campaign suffix
and intentionally refuse output collisions; use a fresh suffix for a new matrix.
survey.py / survey-extra.py retain fixed primary-source snapshots. Use run.py
with fresh IDs for individual repeat runs. Survey sources are registry archives
and immutable GitHub commit snapshots, retained as JSON; none are executed.
survey-compile checks native default TLS features for pgwire/pg_walstream and
the pgoutput parser as edition-2024 dependencies. No alternative crate was live
qualified against the matrix; the live assertions exercise our minimal subset.
