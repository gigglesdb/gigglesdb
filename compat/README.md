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
by glob; IDs are checked globally for duplicates before filtering. `--no-corpus`
skips this stage; `--no-checks` runs the corpus without client checks.

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
psql metacommands in corpus source are forbidden. Tables persist between cases; use case-prefixed names to avoid interference.

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
marked with `-- @error`. Informational `-- expect-sqlstate: CODE` comments record
the expected PostgreSQL code; the harness currently compares observed server
states and does not enforce that comment as a separate assertion.

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
reparse points. Successful commit builds retain the executable, `build.json`, and
`build.log`, while deleting their owned source export and temporary directory.
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
