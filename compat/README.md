# Compatibility harness

This runner compares real client output from a pinned PostgreSQL 18.6 oracle
and a frozen GigglesDB development binary. A passing check qualifies only the
specific SQL and client exercised; it does not imply general PostgreSQL compatibility.

Prerequisites: Python 3.11+, Docker with Linux containers, and Rust/Cargo for commit builds, or a prebuilt
`giggles-server` binary supporting `--dev`. The pinned image must already be local,
or pass `--pull` to permit Docker to fetch its exact digest:
`postgres@sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42`.
No third-party Python packages are used.
Run from the repository root:

```powershell
python -I -B compat/harness.py run --server-bin J:/gigglesdb-work/r0-harness/cargo-target/debug/giggles-server.exe --commit 0e2b54ac18e300c6bdee487e75265891ed1df351
# Equivalent wrapper:
powershell -NoProfile -File scripts/compat.ps1 run --server-bin J:/gigglesdb-work/r0-harness/cargo-target/debug/giggles-server.exe
```

`--clients psql` selects comma-separated registered clients (default: all).
`--checks 'connect*'` selects check IDs or filenames by glob. `--client-timeout 60`
sets the deadline for each client invocation. `--pull` permits fetching the exact
pinned digest when absent; image inspection always happens first. `--keep` retains
and prints this run's Docker objects. `--strict` returns 1 on FAIL or HARNESS_ERROR.
Ordinary completed runs return 0 even with failures; infrastructure failures return 2.
With `--server-bin`, the optional commit is an operator label; without a label,
reports mark the supplied binary unverified. Without `--server-bin`, `--commit`
defaults to HEAD and the harness resolves it to a full SHA and builds the committed
source with `cargo build --locked --manifest-path <src>/crates/giggles-server/Cargo.toml --bin giggles-server` before starting Docker.
`--build-root` defaults to `J:/gigglesdb-work/r0-harness/builds` and
`--cargo-target` to `J:/gigglesdb-work/r0-harness/cargo-target`; K: is refused.
A per-commit cache verifies binary size and SHA-256 before reuse. An exclusive
lock serializes builds, recovers stale owners, and times out on a live owner.
Reports record resolved commit, cache hit/miss, build root and duration; build
failures include the final 30 build-log lines. Task environment and generated
source/build artifacts remain under J:/gigglesdb-work. Every report
includes the copied executable's SHA-256 and size, plus harness HEAD/dirty state.

Output defaults to `J:/gigglesdb-work/r0-harness/runs`; `--out-root` must resolve
outside K:. Each run uses a unique `r0-YYYYMMDDTHHMMSSZ-6hex` directory:

```text
bin/                        frozen executable
cwd/ data/ tmp/ target/      isolated server state and child environment
server.stdout.log           drained readiness and output (redacted)
server.stderr.log           drained server diagnostics (redacted)
checks/psql/connect/
  pg.stdout.txt pg.stderr.txt giggles.stdout.txt giggles.stderr.txt
  result.json diff.txt       diff.txt exists on FAIL
report.json report.md
```

Reports use schema 1, with per-client counts in `summary.clients`, aggregate
counts in `summary.totals`, and aggregate rate in `summary.giggles_pass_rate`.
PASS means GigglesDB exits zero and its normalized stdout
exactly matches a successful oracle invocation. Checks with
`-- compare: stdout+stderr` require both normalized streams to match; default
comparison is stdout. Diffs label each compared stream. FAIL means a GigglesDB error,
timeout, or mismatch. HARNESS_ERROR means the oracle invocation failed or timed
out. UNTESTED means an unavailable client or an explicit check status. Pass rate
is PASS / (PASS + FAIL), or N/A when no qualifying comparison exists. Diff excerpts
contain at most 40 lines; the full diff is retained alongside output artifacts.

Normalization applies registry rules in order: `crlf` converts CRLF to LF;
`trailing-ws` removes trailing whitespace per line and trailing blank lines.
No values, row order, error messages, timing text, or other SQL output are rewritten.

Docker uses the exact `postgres@sha256:...` digest in `pins.json` for both server
and psql. The oracle initializes UTF8 / C.UTF-8 and UTC, with unpublished ports on
a per-run network. All containers use tmpfs over `/var/lib/postgresql` to suppress
the image's anonymous volume; no Docker volumes are created. Client connections
to GigglesDB use `host.docker.internal` with the host-gateway mapping. Docker Desktop
must forward host loopback listeners for this path to work. Oracle readiness checks
TCP `pg_isready`, then authenticated `SELECT 1`, before recording version, database
collation, timezone and encoding.

Per-run passwords enter child processes through inherited environment variables,
never command arguments. Both server output streams are drained in threads and
redacted, including the one-time credential URL. Reports and client artifacts are
also redacted. On completion, startup error or Ctrl-C, the runner sends `shutdown`
to the server, waits 30 seconds, then kills if necessary. Docker cleanup targets
only explicitly tracked names beginning with the exact run prefix; no global prune
or unrelated object removal occurs. Timed-out clients are forcibly removed.

Add a check under `compat/checks/<client>/*.sql` with unique safe ID and headers:

```sql
-- id: example
-- title: Describe the behavior being exercised
-- owner: A
SELECT 1 AS one;
```

Owners are A, B, C, D, E1, E2, E3, F, G or H. An optional
`-- status: untested explanation` records a deliberately unexecuted check.
Repeatable `-- env: KEY=VALUE` headers pass client environment to both databases
for that check only. Allowed keys are PGTZ, PGOPTIONS, PGAPPNAME,
PGCLIENTENCODING and PGDATESTYLE; values after `=` are preserved. Other keys,
malformed entries and duplicate keys are rejected. Clients receive no default
PGTZ override; the oracle server retains its pinned UTC configuration.
`-- compare: stdout+stderr` enables diagnostics comparison; unsupported modes
are rejected.
Client entries in `psql_client.REGISTRY` expose `run` and `version` callbacks;
add their pin and checks when registering another client. Checks run against PostgreSQL first, then GigglesDB.

Run the Docker-free unit suite:

```powershell
$env:PYTHONDONTWRITEBYTECODE = '1'
python -I -B -m unittest discover -s compat/tests -t compat -v
```

The requested POSIX form is
`PYTHONDONTWRITEBYTECODE=1 python -I -m unittest discover -s compat/tests -t compat -v`.
Isolated mode ignores that environment variable, so add `-B` as above to prevent
bytecode in the checkout. `compat/tests/__init__.py` is an empty package marker.

The startup-timezone smoke check explicitly opts into PGTZ=UTC to capture the
client startup parameter behavior separately from the remaining checks.
