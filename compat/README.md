# Compatibility harness

This runner compares real client output from a pinned PostgreSQL 18.6 oracle
and a frozen GigglesDB development binary. A passing check qualifies only the
specific SQL and client exercised; it does not imply general PostgreSQL compatibility.

Prerequisites: Python 3.11+, Docker with Linux containers, and Rust/Cargo for commit builds, or a prebuilt
`giggles-server` binary supporting `--dev`. The pinned image must already be local,
or pass `--pull` to permit Docker to fetch its exact digest:
`postgres@sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42`.
The host harness uses only the Python standard library; probe dependencies are installed inside Docker images.
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
and prints this run's Docker objects. `--strict` returns 1 on client FAIL/HARNESS_ERROR or differential
UNSUPPORTED/DIVERGENT/HARNESS_ERROR; INTENTIONAL is excluded.
Ordinary completed runs return 0 even with failures; infrastructure failures return 2.
To run an `engine-rocksdb` server binary against a fresh v2 database, add
`--server-arg=--engine --server-arg=v2`. Repeat `--server-arg` to pass other
individual arguments to the server's development-mode command.

With `--server-bin`, the optional commit is an operator label; without a label,
reports mark the supplied binary unverified. Without `--server-bin`, `--commit`
defaults to HEAD and the harness resolves it to a full SHA and builds the committed
source with `cargo build --locked --manifest-path <build-worktree>/crates/giggles-server/Cargo.toml --bin giggles-server` before starting Docker.
`--build-root` defaults to `J:/gigglesdb-work/r0-harness/builds` and
`--cargo-target` to `J:/gigglesdb-work/r0-harness/cargo-target`; K: is refused.
`--build-worktree` defaults to `J:/gigglesdb-work/wt/r0-build`. It must resolve
strictly beneath `J:/gigglesdb-work/wt/`, including when reusing a cached binary.
This stable, harness-owned Git worktree is verified as registered to the source
repository before use. The harness may force-check out the requested commit and
run `git clean -ffdx` there: do not keep personal changes or artifacts in it.
Reuse the same worktree and cargo target to preserve sccache path identity.
A shared worktree lock serializes users even when their cargo targets differ;
the target build lock also prevents concurrent builds sharing one cargo target.
Each Cargo child sets `RUSTC_WRAPPER=J:/gigglesdb-work/tools/bin/sccache.exe`,
`SCCACHE_DIR=J:/gigglesdb-work/sccache-cache`, `SCCACHE_CACHE_SIZE=100G`, and
`CARGO_INCREMENTAL=0`. If sccache is unavailable, the build records that status
and falls back to Cargo without a wrapper. Build metadata records sccache stats
before and after the build and their delta when available.
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

Owners are A, B, C, D, E1, E2, E3, F, G, H, DD-0068 or unowned.
Unowned failures appear under `Unowned (needs an owner)` in both report lanes. An optional
`-- status: untested explanation` records a deliberately unexecuted check.
Repeatable `-- env: KEY=VALUE` headers pass client environment to both databases
for that check only. Allowed keys are PGTZ, PGOPTIONS, PGAPPNAME,
PGCLIENTENCODING and PGDATESTYLE; values after `=` are preserved. Other keys,
malformed entries and duplicate keys are rejected. Clients receive no default
PGTZ override; the oracle server retains its pinned UTC configuration.
`-- compare: stdout+stderr` enables diagnostics comparison; unsupported modes
are rejected.
SQL client entries in `psql_client.REGISTRY` expose `run` and `version` callbacks;
driver entries declare `kind: probe`, driver name, expected version and probe directory.
Registering another driver does not require a custom SQL loop. Checks run against PostgreSQL first, then GigglesDB.

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

## Differential SQL corpus

After client checks, the runner executes `compat/corpus/<area>/<case>.sql` in
one psql session per case per server, sequentially against the same databases.
The pinned PostgreSQL image and network are reused. Each server session has a
120-second deadline. `--corpus '*'` selects case IDs, areas, or area/case paths
by glob; several globs may be given separated by commas (`--corpus
'case_a,case_b,pg_types/interval_*'`), and a glob starting with `!` excludes
(`--corpus '!pg_types,!pg_ddl'` runs every other area). IDs are checked
globally for duplicates before filtering. `--no-corpus` skips this stage; `--no-checks` runs the corpus
without client checks.

Cases require `-- case: safe_unique_id`, `-- title: ...`, and `-- owner: A`
headers. Optional `-- expect: compatible` is the default; `-- expect: intentional
reason` documents an accepted difference, and `-- expect: untested reason`
records metadata without invoking either server. Place `-- @error` immediately
before a statement expected to fail on PostgreSQL. Unexpected PostgreSQL errors
or an expected error that succeeds are HARNESS_ERROR.

The SQL splitter terminates a statement only when a line ends with a semicolon
outside a quoted string or identifier. Put each statement's final semicolon at
the end of its own line. Each line-terminated chunk must contain exactly one SQL
command; multiple top-level commands or trailing SQL after a semicolon are rejected. Comments
and semicolons inside single quotes, double quotes, or dollar quotes are handled;
trailing line comments are recognized.
psql metacommands in corpus source are forbidden.

Corpus `r0c_*` tables are dropped on both sides after each case. Cleanup output and status are recorded as artifacts and metadata, but are not compared.

Give corpus setup tables a primary key so setup succeeds before the intended
feature is exercised. Preserve feature-specific key definitions: the
`constraints/composite_primary_key.sql` case retains its two-column primary key
and its explanatory `primary-key exception` comment. The dedicated
`defaults/create_table_without_primary_key.sql` case belongs to owner F and
intentionally omits the key because that absence is the feature under test; its
`primary-key exception` comment records the approved exception. The corpus
primary-key lint rejects setup tables without a primary key unless they have an
approved exception.

Table names without the case ID warn.

Generated scripts disable ON_ERROR_STOP, use SQLSTATE diagnostics and CSV output,
and surround each statement with BEGIN/END markers containing SQLSTATE and
ROW_COUNT. psql runs without echo-all or quiet so command tags remain observable.
COMPATIBLE means all statement states and normalized output match. The first
GigglesDB-only error is UNSUPPORTED for 0A000, 42601, 42883, or 42704; XX000 is
unsupported only when its stderr says unsupported or not supported. Other first
differences are DIVERGENT. An intentional difference is INTENTIONAL; a matching
intentional case is COMPATIBLE with a stale_expectation warning. Missing GigglesDB
markers produce CONNECTION_LOST records. Invalid PostgreSQL transcripts are
HARNESS_ERROR. Existing normalization rules also apply to corpus output.

Artifacts live under `corpus/<area>/<id>/` in the run directory: `script.sql`,
`pg.stdout.txt`, `pg.stderr.txt`, `giggles.stdout.txt`, `giggles.stderr.txt`, and
`result.json` containing records, classification, and first divergence. Skipped
cases retain their script and result metadata. Report JSON adds `differential`
with the unsupported SQLSTATE registry, cases, and summary overall/by_area/by_owner.
Each summary counts all six outcomes; pass rate is COMPATIBLE /
(COMPATIBLE + UNSUPPORTED + DIVERGENT), or null/N/A for an empty denominator.
Markdown and console reports include both client and differential tables, with
first differences grouped by owning track and stale expectation warnings.

An empirical PostgreSQL 18.6 CSV probe confirms DDL/DML command tags are printed
(for example CREATE TABLE, INSERT 0 N, UPDATE N), while SELECT emits CSV headers
and rows without a SELECT command tag. The transcript therefore also retains a
canonical ROW_COUNT line derived from each END marker, so SELECT row counts are compared
without guessing tags from SQL text; actual DDL/DML tags remain compared too.

## Focused runs for one stack

A lead or worker checks only its own cases against a lane build:

```bash
cd <lane worktree>    # run the harness from the lane, not the shared checkout
python -I -B compat/harness.py run --server-bin <lane target>/debug/giggles-server.exe   --no-checks --keep-scratch --corpus 'case_a,case_b,case_c'   --out-root J:/gigglesdb-work/pgparity/gap2/runs/<lane>   --cargo-target <lane target>
```

- `--corpus` takes one glob or several separated by commas. Each glob is
  matched against the case id, the area, `area/id`, `area/file.sql` and the
  file path, so ids, whole areas (`pg_types`) and prefixes (`pg_ddl/view_*`)
  can be mixed; `!glob` excludes. Check the "Differential" table at the end of the output: it
  lists only the selected cases.
- `--no-checks` skips client, ORM and GUI tool checks; the Docker oracle still
  starts once per run.
- `--cargo-target` only names where the run lock `.gd-r0-run.lock` lives. Pass
  the lane's own target so a focused run never waits on, or blocks, another
  lane's run.
- Output: `<out-root>/r0-<UTC time>-<hex>/` holds `report.json`, `report.md`,
  `server.stdout.log`, `server.stderr.log`, `oracle.log` and
  `corpus/<area>/<id>/` with `script.sql`, `pg.stdout.txt`,
  `giggles.stdout.txt`, both stderr files and `result.json`. With
  `--keep-scratch` the server's `bin` (a copy of the binary, about 96 MB),
  `data`, `tmp`, `target` and `cwd` directories stay for debugging. Delete the
  run folder when the stack lands.
- Time: 3 cases took 25 seconds end to end (oracle start, server start, both
  sides, cleanup) with a built binary and the oracle image present. A whole
  area takes about 6 seconds per case.

## Nightly main measurement

```bash
bash K:/Projects/gigglesdb/scripts/pgparity-nightly.sh
```

Where one task may run at most two hours (agent background tasks), run the two
halves as two tasks, one after the other:

```bash
bash K:/Projects/gigglesdb/scripts/pgparity-nightly.sh --half a   # pg_types, pg_functions, pg_queries, pg_silent
bash K:/Projects/gigglesdb/scripts/pgparity-nightly.sh --half b   # every other area
```

The script resolves `main` once, builds a debug `giggles-server` through the
build-slot wrapper in the stable worktree `J:/gigglesdb-work/wt/pgp-nightly`
and target `J:/gigglesdb-work/pgparity/gap2/nightly-target` (sccache hits need
both to stay the same), runs the corpus with `--no-checks --keep-scratch` in two
halves split by area (`--half a|b|all`, default all) and writes
`J:/gigglesdb-work/pgparity/gap2/nightly/<UTC date>-<sha12>/`: `build.log`,
`timings`, `harness-<half>.log`, `part-<half>/r0-*/report.json` with the
per-case transcripts, `report.json` (the halves merged with `harness.py
merge`), `areas.md` (per-area outcome counts and the COMPATIBLE change),
`delta.md` (`harness.py delta` against the previous run) and `SUMMARY.md`.
A half that already has a report for this commit is skipped, and whichever
invocation completes the pair merges and reports.
`nightly/LATEST` names the newest report; the next run compares against it, or
against `--previous <report.json>`. A commit that already has a run is a no-op
unless `--force`; `--with-checks` adds the client, ORM and GUI tool checks
to half a;
`--keep-scratch-runs <n>` (default 2) keeps server scratch only for the newest
runs, while reports, transcripts and logs stay. Exit 0 done, 3 done with
regressions, 2 failure. A full corpus run takes about 110 minutes.

`watch` is not used for this: it builds without the build-slot wrapper, always
runs every client check, deletes scratch and writes no per-area table.

`harness.py areas REPORT... [--previous OLD...] [--out FILE]` prints the
per-area table for any report, merging several part reports on either side.
`harness.py merge PART... --out FILE` writes one report from parts of the same
commit's run; it refuses parts of different commits and a case present in two
parts.

## H2b source-derived coverage inventory

The corpus contains 78 cases: 56 ORM-shaped cases, 19 retained-gap
probes, and 3 seed cases. The per-area inventory is:

| Area | Count | Case IDs |
|---|---:|---|
| alter_table | 8 | `add_column`, `add_column_with_default`, `add_constraint_unique`, `alter_column_drop_not_null_and_set_not_null`, `alter_column_set_default`, `drop_column`, `rename_column`, `rename_table` |
| constraints | 8 | `check_violation`, `composite_primary_key`, `fk_delete_restrict`, `fk_on_delete_cascade`, `fk_violation`, `not_null_violation`, `unique_multi_column`, `unique_violation` |
| defaults | 7 | `bigserial_column`, `default_keyword_in_values`, `default_literal`, `identity_always`, `identity_by_default`, `sequence_basic`, `serial_column` |
| dml_predicates | 9 | `delete_with_in_subquery`, `update_multiple_columns`, `update_set_expression`, `update_where_between`, `where_in_list`, `where_is_null_and_is_not_null`, `where_like_and_ilike`, `where_not_equal`, `where_or` |
| identifiers | 4 | `quoted_identifier_with_space`, `quoted_mixed_case_table`, `quoted_reserved_word_columns`, `quoted_vs_unquoted_folding` |
| insert | 4 | `insert_column_subset_nulls`, `insert_select`, `insert_select_returning`, `multi_row_values` |
| known_gaps | 19 | `aggregate_filter`, `aggregate_over_app_table`, `array_column`, `composite_type`, `domain_type`, `enum_type`, `explicit_bytea_cast`, `immutable_overflow_error`, `information_schema_columns`, `jsonb_column`, `mixed_catalog_app_query`, `no_from_aggregate`, `numeric_precision`, `pg_get_userbyid_owner`, `public_schema_routine`, `search_path_custom`, `table_free_scalars`, `timestamptz_roundtrip`, `uuid_column` |
| on_conflict | 4 | `on_conflict_do_nothing`, `on_conflict_do_update_excluded`, `on_conflict_do_update_where`, `on_conflict_on_constraint` |
| returning | 5 | `delete_returning`, `insert_returning_cols`, `insert_returning_default_id`, `insert_returning_star`, `update_returning` |
| seed | 3 | `division_by_zero`, `error_sqlstate`, `select_literals` |
| session | 4 | `reset_all`, `set_application_name`, `set_show_search_path`, `set_show_timezone` |
| transactions | 3 | `savepoint_release`, `savepoint_rollback_to`, `set_transaction_isolation` |

The retained-gap probes come from the DD68 [client gap source inventory](J:/gigglesdb-work/dd68-client-gap-inventory-20261106/audit-01/client-gaps.md).
That read-only inventory identifies candidate boundaries; its historical evidence
is not a measurement of these new cases. All cases explicitly declare
`-- expect: compatible`: PostgreSQL behavior is the target, including errors
marked with `-- @error`. Enforced `-- expect-sqlstate: CODE` annotations pin
the GigglesDB code for the following statement. A mismatch is DIVERGENT even
for an intentional case. `-- @error` independently marks an expected PostgreSQL
error; omit it when PostgreSQL succeeds and GigglesDB deliberately rejects SQL.
See [the compatibility matrix](MATRIX.md) for pinned R0 evidence and the
additional intentional boundary cases.

The parameter-typing gap for `SELECT position($1 IN $2)` with declared bytea
parameters requires the H3 extended-protocol client lane. This simple-query SQL
corpus cannot exercise Parse-declared parameter types; `explicit_bytea_cast`
covers only literal casts and must not be cited as closing that gap.

At the H2 inventory checkpoint, measurement was pending (**DOCKER LANE HELD**). No new PostgreSQL/GigglesDB
classifications, pass rates, or compatibility claims are established by this
inventory or the Docker-free unit suite. The known_gaps directory names source
candidates, not measured failures. Every case is intended for later differential
measurement against the pinned oracle.

Unit tests share `tests/task_storage.py`: each module activates an owned UUID
root under `J:/gigglesdb-work/r0-harness/unit-tmp` in `setUpModule`, sets TEMP,
TMP and CARGO_TARGET_DIR there, and restores the environment and removes only
that root in `tearDownModule`. Prior task evidence is never cleaned up.

## H3a driver probes

`--clients jdbc` selects the pgjdbc 42.7.3 driver probe through the client
registry; `--clients psql,jdbc` includes both clients in the existing summary.
The JDBC entry uses the driver-probe framework rather than a SQL callback.
`--no-checks` skips driver probes too. Corpus execution still requires a selected
client with a session runner; JDBC alone records corpus cases UNTESTED.

The probe's `compat/probes/jdbc/cases.json` owns 19 ordered cases and their owners.
The program executes the entire sequence once against PostgreSQL, then once
against GigglesDB, with a 180-second deadline for each invocation. `--checks`
filters reported manifest IDs only: all cases still execute in their listed
order because later cases depend on tables created by earlier cases. The JDBC
lane covers extended-protocol parameter types and metadata, including the DD68
bytea `position` boundary that simple-query corpus cases cannot exercise.

The framework reuses the CASE/base64 framing and integrity-check design from
GD0048's `JdbcProbe.java` and Rust evaluator. This is design reuse, not reuse of
GD0048's mTLS transport or client authentication setup. Connections use the same
per-run password as other harness clients, passed through environment only.

Stdout starts with `VERSION<TAB>jdbc<TAB>42.7.3`, followed by
`CASE<TAB>id<TAB>OK|ERR<TAB>base64(JSON object)` records, and ends with
`DONE<TAB>count`. Unknown or duplicate IDs, malformed framing/JSON/base64,
incorrect version/count, unsolicited stdout, and absent DONE invalidate a side.
Missing manifest records are classified per case. ERR observations include
exception class and SQLSTATE; successful observations include deterministic
values, JDBC types, labels and counts. Observations must exclude timings,
backend PIDs, ports, server-version strings, stack traces and error-message text.
Full canonical JSON observations and statuses are compared with sorted keys;
no additional value normalization occurs. Unexpected PostgreSQL ERR or invalid
PostgreSQL output yields HARNESS_ERROR; an expected PostgreSQL ERR that returns
OK also yields HARNESS_ERROR. A GigglesDB mismatch yields FAIL.
Explicit manifest untested reasons yield UNTESTED.

The build uses `maven:3.9.9-eclipse-temurin-21` and
`eclipse-temurin:21-jre`, with pgjdbc exactly 42.7.3. Only the probe directory is
sent as the Docker build context. The cache tag is
`gd-r0-probe-jdbc:<first12-sha256>` over sorted relative file paths and bytes
(with framing to disambiguate boundaries). Existing images are inspected first;
a failed build or unavailable image records every selected manifest case as
UNTESTED with a reason, without making the run an infrastructure error.
Reports retain tag, image ID, digest-like local image ID, repository digests when
available, and the driver version observed from the connected VERSION record.
No separate unconnected VERSION invocation is made.

Artifacts are `checks/jdbc/{pg,giggles}.{stdout,stderr}.txt`, optional build logs,
and `checks/jdbc/<id>/result.json` with per-case observations and classification.
Failures retain a sorted-JSON diff and a short report excerpt; the first
GigglesDB ERR SQLSTATE appears as `first_error`. All artifacts are redacted.
Named per-run containers are tracked and removed on timeout. Cached probe images
survive ordinary cleanup and `--keep`; remove a specific unused image explicitly:

```powershell
docker image rm gd-r0-probe-jdbc:<first12-sha256>
```

At the H3a implementation checkpoint, Docker qualification was pending (**DOCKER LANE HELD**). Neither
host Java/javac nor probe image builds were run for the Docker-free unit checks;
these checks establish harness behavior, not measured JDBC compatibility.



## H3b Python and Node driver probes

`--clients psycopg,node-pg` selects psycopg 3.2.9 and node-postgres (`pg`)
8.16.3. The default client selection includes all registered clients. Each
probe runs its full ordered manifest against PostgreSQL and then GigglesDB;
`--checks` filters reported cases, preserving sequence dependencies.

| Driver | Runtime image | Cases | Database operations exercised |
|---|---|---:|---|
| JDBC 42.7.3 | `eclipse-temurin:21-jre` | 19 | Connection checks, prepared statements, transactions/savepoints, JDBC catalog metadata, scalar and type round trips |
| psycopg 3.2.9 | `python:3.13-slim` | 19 | Connection checks, prepared statements, transactions/savepoints, catalog metadata, scalar and type round trips |
| node-pg 8.16.3 | `node:22-slim` | 18 | Connection checks, parameterized and named prepared statements, transactions/savepoints, catalog metadata, scalar and type round trips |

The shared minimal application table is unqualified and uses exactly these
columns; each driver owns its prefix to avoid collisions:

| Column | Declaration |
|---|---|
| `id` | `integer PRIMARY KEY` |
| `name` | `text` |
| `score` | `bigint` |

These are the minimum database behaviors covered by these manifests, not a
claim of full driver support. PostgreSQL 18.6 remains the pinned reference;
no older server version has been qualified by this lane. Source fixtures live
under `compat/probes/<driver>/`; the harness sends only that directory as each
Docker build context. No host pip packages or native driver installation are
needed. Node dependencies use the committed npm lockfile and `npm ci`; changes
to the lockfile, probe, manifest or Dockerfile change the existing content-based
image cache tag. Psycopg is pinned exactly, including its binary runtime package.

JDBC and Node retain exactly three VERSION fields. Psycopg starts with
`VERSION<TAB>psycopg<TAB>3.2.9<TAB>libpq_version`, where the fourth field is the
canonical positive decimal integer returned by `psycopg.pq.version()`. The registry opts
into that single extra field; missing, repeated, noncanonical or unsolicited
fields invalidate the side. `clients.psycopg.libpq_version` records the observed
PostgreSQL-side runtime value alongside the driver version. It is not a
hardcoded libpq build pin.

Every probe-owned assertion includes a stable `reason` matching
`[a-z][a-z0-9_]{0,63}`, such as `missing_row`, `value_mismatch` or
`column_type_mismatch`. The parser enforces these reasons recursively, including
nested assertions inside otherwise successful CASE records. Server errors keep
their driver exception class and SQLSTATE without inventing an assertion reason
or retaining server message text. Reasons remain part of the canonical JSON
comparison. SQL prefixes are `r0d_jdbc`, `r0d_psycopg`, and `r0d_nodepg`.

Docker-free tests validate framing, metadata propagation, assertion reasons and
report classification. They do not establish PostgreSQL/GigglesDB compatibility;
end-to-end results must come from a separately recorded Docker run.

## Scratch retention

Runs write their reports before deleting only their own `bin`, `data`, `tmp`,
`target`, and `cwd` directories, then update the reports with `scratch_removed`
and `scratch_bytes_freed`. `--keep-scratch` retains those directories for debugging;
`--keep` independently retains Docker objects. A failed server shutdown retains
scratch. Cleanup refuses redirected parents, symlinks, junctions, and nested
reparse points. Successful commit builds retain the per-commit executable cache,
`build.json`, and `build.log`, while deleting their owned temporary directory.
The stable build worktree and cargo target survive for subsequent builds.
Probe client metadata records `image_build_cache` and `image_build_duration_s`.

## H3c compiled driver probes

`tokio-postgres` uses exact driver 0.7.15, Tokio 1.47.1 and postgres-types
0.2.11. Its standalone Cargo workspace builds with `rust:1.90.0-slim-bookworm`
and runs on `debian:bookworm-slim`. Cargo.lock is generated in that builder,
committed, and required by `cargo build --locked`; the Docker build also checks
the locked tokio-postgres version. Chrono 0.4.42, UUID 1.18.1 and rust_decimal
1.39.0 provide native timestamp, UUID and numeric conversions.

The 18 tokio-postgres cases include the shared 15 core cases plus `simple_query`
(message kinds and values), `statement_types` (prepared parameter and column
types), and `typed_prepare` (explicit INT8 parameter against an integer key).
Its application table is `r0d_tokio_items`. Each case owns a NoTls connection,
with a 30 second case deadline, and emits only VERSION, CASE and DONE records.
The no-FROM `max($1)` observation uses a typed INT4 parameter; type round trips
use independent primary-key tables with non-NULL and NULL point observations.
The other compiled H3c probes pin pgx 5.7.6 (19 cases), built with
`golang:1.23-bookworm` and run on `debian:bookworm-slim`, and Npgsql 9.0.4
(19 cases), built with `mcr.microsoft.com/dotnet/sdk:9.0.203` and run on
`mcr.microsoft.com/dotnet/runtime:9.0.4`. Go and Rust use committed `go.sum` and `Cargo.lock` files. Npgsql references
an exact NuGet package version. All compiled probes build from their own
probe directory without a host language toolchain.


## H3d selection and oracle diagnostics

`--owners E1,H` selects owners from A, B, C, D, E1, E2, E3, F, G, H, DD-0068, unowned.
The owner filter is ANDed with `--checks` and `--corpus`; invalid or empty
owner selections fail before starting infrastructure. Corpus filtering happens
before sessions run. Driver probes still execute and validate their full manifest,
including setup dependencies, while only matching cases enter the report.

The `npgsql-notypes` client uses the same Npgsql 9.0.4 image and manifest as
`npgsql`, with `R0_NPGSQL_PROFILE=notypes` and separate `r0d_npgsqlnt` tables.
Its profile skips startup type loading for diagnostics; the normal profile
retains the ordinary driver startup behavior. Both clients reuse the same
content-addressed image.

The PostgreSQL oracle enables all statement logging, error statement logging,
and `%m [%p] %a ` prefixes. Before container cleanup, even after readiness
failure, its merged Docker stdout/stderr is captured as `oracle.log` and linked
by top-level `oracle_log` in report JSON (also mirrored in `oracle.oracle_log`). Capture reads a bounded prefix, redacts
passwords and PostgreSQL URLs, and caps final stored bytes at 20 MiB, including
a truncation marker. Required application attribution overrides any check
`PGAPPNAME` header: `r0:psql:<check-id>`, `r0:corpus:<area>/<case-id>`, and
`r0:<registered-client>` for probes, including `r0:npgsql-notypes`.

## H5a ownership and shape comparisons

Corpus cases accept `-- compare: full` (default) or `-- compare: shape`,
and optional `-- source: ...` provenance metadata. Shape comparison requires
equal SQLSTATEs; successful statements compare the first logical CSV record
(column names, including quoted multiline names) and framed ROW_COUNT, ignoring
row values. Command-only statements compare their first output record (the
command tag); empty output has no header. Equal error SQLSTATEs compare equal.
Every case's comparison mode appears in JSON and the Markdown case table,
including compatible cases. Existing full comparison behavior is unchanged.

Parameterized update/delete probes and update_set_expression belong to E3.
No-FROM aggregate probes belong to DD-0068. Savepoints and COPY/binary import
probes are unowned; pgx simple execution remains D with the GD-0055 reference.

## DataGrip captured SQL corpus

Regenerate the 36 DataGrip cases with:

```powershell
python -I -B compat/tools/gen_datagrip_corpus.py
```

The default destination is the committed `compat/corpus/datagrip` source
directory. Use `--out J:/gigglesdb-work/<unique-task>/datagrip` to review an
independent export. Set TEMP, TMP and CARGO_TARGET_DIR to that task's J: paths
before running commands.

The generator reads two capture JSON files and `datagrip_*.sql` fixtures,
verifies JSON `sha256`/`expanded_sha256` identities, and globally deduplicates
the SQL UTF-8 bytes after stripping a leading BOM and normalizing CRLF to LF.
This applies to fixture SQL and embedded JSON SQL strings before checking the
stored `sha256`/`expanded_sha256` (which must describe the LF-normalized value),
deduplicating, naming, and writing cases. Generated files always use LF.
Stable IDs use the captured family/name, adding a hash suffix for collisions.
Each case creates its own primary-key fixture table and body index, declares
owner G, `compare: shape`, and source provenance. Captured SQL retains its
comments and literals; multiple statements become separate line-terminated
chunks. Only unexpanded `:schema_id` and `$N` parameters are replaced by literal
2200, the public namespace OID; captured literal OIDs and names stay intact.

PostgreSQL 18 qualification can identify obsolete captured SQL. Such cases
remain in the corpus with a reasoned `expect: untested` override keyed by their
source SHA-256 in `PG18_UNTESTED`, so regeneration preserves the decision.
Shape-compatible catalog results qualify column names and counts only.

## Offline report deltas

```powershell
python -I -B compat/harness.py delta OLD.json NEW.json --out J:/gigglesdb-work/<unique-task>/delta.md
```

`delta` reads existing report JSON without starting infrastructure, prints
Markdown, and optionally writes it with `--out`. It matches checks and corpus
cases by their exact stable identities, reports improvements, regressions,
added/removed records and other changes, and includes PASS-count and pass-rate
percentage-point deltas. Owner and error changes appear as other changes.
Exit status is 0 without regressions, 3 with regressions, or 2 for invalid input.

## One-iteration main watcher

```powershell
python -I -B compat/harness.py watch --seed-report J:/gigglesdb-work/<baseline>/report.json --seed-commit <full-baseline-SHA>
```

`watch` resolves `main` once and exits. Its default state is
`J:/gigglesdb-work/r0-harness/watch/state.json`, containing `last_run_commit`,
`last_run_at`, and `last_report`. A missing state requires both seed arguments;
the report's build commit must match, and its finished/start timestamp seeds the
interval. Existing state takes precedence over seed arguments. `--state`,
`--out-root`, `--build-root`, `--build-worktree`, and `--cargo-target` override
their J: defaults; output paths cannot be on K:. The resolved build worktree is
forwarded to both full and isolation runs and must remain beneath
`J:/gigglesdb-work/wt/`.

An unchanged commit, fewer than 60 minutes since the previous report, a diff
without changes under `crates/` (excluding `crates/giggles-pg-conformance/`), or
an active harness/build lock produces a successful no-op. Change the positive,
finite interval with `--min-interval-minutes`. Compatibility harness changes
alone do not trigger a run. All runs, including `--server-bin` runs, hold a
run-wide `.gd-r0-run.lock` in the cargo target; the watcher also honors
`.gd-r0-build.lock`. Locks are not removed by competing invocations.

A qualifying change runs all registered clients and corpus cases at the exact
resolved commit, saves the full report as the new baseline atomically, and
writes `delta-<old7>-<new7>.md` beside state. Regressed clients run once each in
isolation without corpus; regressed corpus cases run individually with psql and
checks disabled. Delta entries and `REGRESSION` console lines identify
`reproduces-in-isolation`, `state-dependent (passes alone)`, or
`isolation-inconclusive`, and link the isolated report evidence. Regression
output includes `git log --oneline <old>..<new> -- crates`. The original full
run determines exit 3 even if isolation passes; no regression/no-op exits 0,
and invalid state or infrastructure failure exits 2. Isolation never replaces
the full report baseline.

DataGrip's 36 generated header comparison cases compare column headers only.
Result row counts remain informational in `row_counts` and do not determine
header compatibility.

## pg_dump qualification

The registered `pg_dump` client runs seven checks: fixture application, schema
dump execution, schema restore into PostgreSQL, normalized schema equivalence,
data dump execution, full restore into PostgreSQL, and PostgreSQL-source dump
restore into GigglesDB. Dumps use pinned client binaries, plain SQL,
`--no-owner`, and `--no-privileges`. The independent source fixture uses `r0q_`
tables in `r0_src`, validates its dump by restoring into a separate PostgreSQL
database, then checks GigglesDB's restored rows against the source.

The fixture includes foreign keys referencing redundant standalone UNIQUE and primary keys.
Restore rebinding to the primary key is PostgreSQL 18 behavior; historical key binding is recorded separately and is not asserted by these checks.

Schema normalization drops comments, SET prologue lines, blank lines, trailing
whitespace, and PostgreSQL 18's random `\restrict`/`\unrestrict` guard tokens.
SQL definitions remain intact. Each check keeps raw artifacts and records its
PostgreSQL baseline: a failed baseline is `HARNESS_ERROR`; a failed GigglesDB
operation against a working baseline is `FAIL`.

## Mixed-version directory and export qualification

Build OLD and NEW `giggles` and `giggles-server` binaries separately. NEW must
include `--features engine-rocksdb` in both crates; OLD uses its legacy build.
Run this bounded matrix with Docker available and `postgres:18.6` installed:

```powershell
python -I compat/mixed_version.py --old-cli J:/gigglesdb-work/p1-mv-old/target/debug/giggles.exe --new-cli J:/gigglesdb-work/p1-mv/target/debug/giggles.exe --old-server J:/gigglesdb-work/p1-mv-old/target/debug/giggles-server.exe --new-server J:/gigglesdb-work/p1-mv/target/debug/giggles-server.exe --out J:/gigglesdb-work/p1-mv/out
```

The runner creates fresh per-run directories beneath `--out`, leaves both source
worktrees untouched, and writes `report.json` and `report.md`. Rows cover legacy
reopens and writes, v2 rejection, upgrade retention, both GDEXP001 directions,
and cross-version server startup. Every fixture includes PK and heap tables,
an index, 200 initial rows per table, an UPDATE and a DELETE; optional schema,
enum, sequence and role support is recorded. Refusals require a nonzero exit,
a format/version/feature diagnostic and unchanged directory contents. Hashes
include every filename, size and file content, including lock files. Silent
misreads, panics and mutation after refusal stop the run with a FINDING; later
cells are NOT RUN. Full command output and server logs remain in the evidence
directory. No product files are changed, and the temporary psql container is
removed on completion. The script does not build binaries or run upgrades on
operator data.

## v2 real-process crash qualification

Build `giggles-server` in `crates/giggles-server` and `giggles` in
`crates/giggles-core`, both with default features and the same external target
directory. With Docker and `postgres:18.6` available, run:

```powershell
python -I compat/v2_qualification.py --server-bin J:/gigglesdb-work/p1-pc/target/debug/giggles-server.exe
```

The default run uses seed 5412, 30 four-connection crash iterations, three DDL
crashes, and 5,000,000 rows with a PK and two secondary indexes, followed by v2
export/restore verification. `--seed`, `--iterations`, `--rows`, `--batch` and
`--work` configure the run; work must remain beneath `J:/gigglesdb-work`.
Each unique evidence directory contains `report.md`, server/client logs and
transaction intent/COMMIT acknowledgement journals. Failures stop immediately
with a nonzero exit and retain data for reproduction; successful large-load
and restored data are removed. The script abruptly kills its servers and
removes its temporary psql container before exiting. No product files change.

## Replicated cluster mode

`--cluster-mode replication` is the default and retains the three-node leader gateway.
`--cluster --cluster-mode ranges --cluster-nodes N` starts 3..6 range-cluster nodes (default 3).
`--gateway K` chooses the range gateway, 1..N (default 2); reports record mode, N, K and all ports.

`run --cluster` replaces the single `giggles-server --dev` process with three
replicated `giggles-server` processes started from the same frozen binary
(`compat_lib/cluster.py`) and runs every selected client and corpus case against
the elected leader only. `--server-arg` is refused with `--cluster`: config-file
mode has no `--dev` flags. Each node gets a JSON config under
`data/cluster/node-N/config.json` (database `db`, Raft `raftlog`, PostgreSQL on a
reserved `127.0.0.1` port with TLS disabled and `loopback_trust: false`, so the
owner authenticates with the per-run password exactly as in `--dev` mode), and
`replication.owner: postgres` bootstraps the `postgres` role from
`GIGGLES_OWNER_PASSWORD` on the first leader. The served database is `postgres`.

Peer mTLS material is generated per run in `data/cluster/pki/`: an ECDSA P-256 CA
and one leaf per node with `subjectAltName = DNS:localhost` and server+client
EKU (the peer transport verifies SNI `localhost` and pins each leaf's DER
SHA-256 fingerprint). The generator uses a host `openssl` (PATH, then Git for
Windows' `mingw64/bin/openssl.exe`) and falls back to the pinned PostgreSQL
image's `openssl` via `docker run -i --network none` without mounts. Keys are
test-only and are deleted with the other scratch directories.

Readiness waits up to 60 seconds for every node's `preview=replicated role=...`
stdout line and exactly one `role=leader`; the leader's `postgresql=` address
must match its configured port. Logs are `server-nodeN.stdout.log` and
`server-nodeN.stderr.log` (redacted). Report `build.server_mode` is
`replicated-3-leader` (single-node runs record `single-node-dev`), and
`build.cluster` records the PKI method, ports, fingerprints, leader node/term,
readiness time, per-node exit codes, and any role announcements between
readiness and shutdown (`leader_changes_after_ready`; non-zero means clients may
have reached a refusing follower). Shutdown sends `shutdown` on each node's stdin
and then closes stdin: a replicated process acts on the command but exits only
once stdin also delivers EOF.

Compare two reports record by record with:

```powershell
python -I -B compat/tools/compare_reports.py A/report.json B/report.json --label-a single --label-b cluster --out J:/gigglesdb-work/<unique-task>/comparison.md
```

It keys client checks by (client, check) and corpus cases by (area, case),
prints both runs' identities and outcome totals plus a table of every record
whose outcome differs or exists on one side only, and exits 0 when identical,
1 when different, 2 for invalid input.

## Application latency bound under agent load (R3-404)

`agent_load.py` measures whether AI-agent task sessions at their limits slow
the application down (GD-0061 design-3 section 7). It runs one real
`giggles-server` and drives it from separate client processes over the
PostgreSQL wire protocol (a small stdlib client; no Docker and no driver
install):

```powershell
python -I compat/agent_load.py --server-bin J:/gigglesdb-work/<task>/target/debug/giggles-server.exe
```

- **Setup** (`--dev --trust-local`): table `shipments(id bigint primary key,
  merchant_id bigint, status text, failed_at timestamptz, attempt_count int,
  customer_email text, card_token text, note text)`, 1,000,000 rows in
  1,000-row merchant blocks, an index on `(merchant_id, status)`, the task-only
  executor `agent_exec` and 36 tasks (each sees one merchant block).
- **Load server**: the same database in config-file mode, with a trusted
  loopback PostgreSQL listener and the agent listener (client certificates from
  a throwaway CA, `agent.max_concurrent_statements` 8). The shared request
  bucket is raised to 1,000,000/s so it does not close connections.
- **Workload**: 16 application connections, closed loop, 80% point SELECT,
  15% single-row UPDATE, 5% INSERT, after an unmeasured 30 s warm-up. Phase A
  has no agents; phase B adds 8 agent sessions over 4 tasks; phase C adds 32
  sessions over 32 tasks. Each agent session loops: an unfiltered sorted SELECT
  (rows-scanned limit), a protected UPDATE of a row the harness holds locked
  (lock wait, 55P03), and keyset pages over its block until the rows-returned
  cap. Phases run 120 s each.
- **Backoff** (R3-404c): agent sessions sleep with jittered exponential
  backoff on 54T01 and 55P03 (10 ms doubling to 200 ms, reset on success),
  like a well-behaved agent; each phase reports `agents.backoffs` and
  `agents.backoff_ms`. The server also paces ceiling refusals: a statement
  refused at `agent.max_concurrent_statements` waits up to
  `agent.ceiling_refusal_delay_ms` (default 25) for a freed slot before 54T01.
- **Bound**: `agent_load_bound.json`. For each application operation
  `p99(B) <= max(1.25 x p99(A), p99(A) + 2 ms)`, the same for C, and zero
  application errors. Change the constants only in a reviewed commit.

The report goes to `compat/reports/<sha8>/agent-load.json` (`--report` to
override): p50, p99, p99.9 and max per operation and phase, agent outcomes per
step and SQLSTATE (54T01 split by budget), and the verdict with each measured
ratio. Exit 0 passes, 2 means application errors, 3 means the bound failed and
1 is a harness failure. Data, logs and client results go to a new
`run-*` folder under `--work` (default `J:/gigglesdb-work/agent-load`, which
must stay under `J:/gigglesdb-work`); the database is deleted afterwards unless
`--keep-data`. Run it alone on the machine: other builds and tests distort p99.

`--smoke` uses 10,000 rows, 5 s phases and a 2 s warm-up, and writes its report
into the run folder. `compat/tests/test_agent_load.py` runs it when
`GIGGLES_SERVER_BIN` names a built server (`AGENT_LOAD_WORK` sets the work
folder). A smoke run is too short to judge the bound, so the test accepts exit 0
or 3 and requires zero application errors and every agent limit reached.

Until CREATE TASK runs over SQL (R3-110), pass `--task-tool TOOL` (or set
`AGENT_LOAD_TASK_TOOL` for the test): after the setup server stops, the harness
runs `TOOL <database> postgres <file of CREATE TASK statements>` to create the
tasks in the stopped database.

## R3 safeguards worked example

Run the 23-step safeguards scenario against a real server process:
`python -I -B compat/r3_safeguards.py --server-bin <exe> --work J:/gigglesdb-work/<task>`.
The default `--drivers psycopg,pgx` runs both pinned drivers (psycopg 3.2.9 and
pgx v5.7.6); pass either name to select one. Use `--psycopg-python <exe>` for
the psycopg environment, `--expiry-seconds 300` for the real-clock expiry, and
`--no-workload` for isolation. `--go <exe>` selects the Go toolchain (defaults
to `C:/Users/beach/go/go1.24.5/bin/go.exe` when available, otherwise `go`).
The runner builds pgx once into the run folder with read-only modules and
reuses Go caches under `<work>/go/`. Child TEMP/TMP use `<run>/tmp`.
Each driver gets a separate database under a unique run folder. Docker runs
four concurrent pinned PostgreSQL psql loops in one container, with disjoint
merchant subsets and insert IDs. The runner kills and restarts the server
at step 18 and always removes its workload container. Reports are
`report.md` and `report.json`, with per-step status, attempts/retries,
deviations, and workload counts/errors. Exit codes: 0 all PASS, 2 scenario or
unexpected workload failure, 1 harness error. Driver JSON and restart protocol
are documented in the runner module docstring.

Select `--suite exit` to run the five exit scenarios through both drivers:
last-slot race (ten sessions), stale approvals and approver separation, the
operation-outcome table, lost-reply recovery on a new connection, and partial
undo with interleaved application changes. Each scenario uses an independent
task and merchant (51..55), with one-hour task authorizations and no expiry wait.
The application workload continues throughout. The default `--suite story`
retains the 23-step worked example above.

```
python -I -B compat/r3_safeguards.py --suite exit --server-bin <exe> --work J:/gigglesdb-work/<task>
```
