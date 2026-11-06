# Actual PostgreSQL driver conformance

This standalone Rust CLI runs checked-in asserting probes against **its own**
server processes and copies of an explicitly supplied, offline TEST fixture.
It never connects to an existing listener. It freezes the supplied executable
and pgJDBC JAR before starting. Failures and generated test credentials remain
in the new output directory; protect these as test artifacts. Nothing is removed.

The scope is the executed client cases, not full DataGrip/IDE or production
compatibility. A successful connection does not certify catalog browsing.
Default pgJDBC and `assumeMinServerVersion=14` are separate profiles. The latter
moves initialization settings into startup; it is not a metadata implementation.
The wrapper uses the fixture EC client key, validates its CA and uses pgJDBC
`verify-full` hostname validation against `localhost`.

## Run

All paths in the manifest must be absolute. The output directory must not exist,
its parent must exist, and it must be outside the fixture. Only pass a retained,
closed TEST fixture; do not pass a live database or credentials. The fixture
must contain an empty `app(id BIGINT PRIMARY KEY, body TEXT, flag BOOLEAN,
small SMALLINT, num INTEGER, data BYTEA)` and an unauthorized `other` table,
with the `app` client certificate bound to `app` only. Reserved test IDs are
8000, 8101..8113 and 9000000000. Each profile receives a separate copied database.

An existing server network test generates this contract:

```powershell
$env:CARGO_TARGET_DIR = 'K:/Projects/gigglesdb/target-gd48-conformance'
cargo test --locked --manifest-path crates/giggles-server/Cargo.toml -j1 --test network executable_server_starts_and_native_client_connects -- --exact --nocapture
```

Use the printed `retained_fixture` resolved relative to `crates/giggles-server`.
`config_base` resolves the source server config's relative paths (normally that
server crate). The CLI copies only the database, server cert/key/CA and app
cert/key. Configured file paths must resolve inside the explicit fixture;
external `bindings_file` is rejected. Inline authority bindings are preserved.
Both listeners are always rewritten to `127.0.0.1:0` in the owned config.

Example manifest (replace fixture and version hashes with actual values):

```json
{
  "server": "K:/Projects/gigglesdb/target-datagrip/debug/giggles-server.exe",
  "fixture": "K:/Projects/gigglesdb/crates/giggles-server/artifacts/network-fixture-EXACT",
  "server_config": "K:/Projects/gigglesdb/crates/giggles-server/artifacts/network-fixture-EXACT/server.TEST-ONLY.json",
  "config_base": "K:/Projects/gigglesdb/crates/giggles-server",
  "output": "K:/Projects/gigglesdb/crates/giggles-pg-conformance/artifacts/run-UNIQUE",
  "java": "C:/Program Files/Eclipse Adoptium/jdk-21.0.7.6-hotspot/bin/java.exe",
  "javac": "C:/Program Files/Eclipse Adoptium/jdk-21.0.7.6-hotspot/bin/javac.exe",
  "jdbc_jar": "C:/Users/beach/AppData/Roaming/JetBrains/DataGrip2026.2/jdbc-drivers/PostgreSQL/42.7.3/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar",
  "timeout_seconds": 90
}
```

Optional `expected_server_sha256` and `expected_jar_sha256` enforce a frozen
identity. All observed hashes are recorded even when expected hashes are omitted.
Omit Java paths to record JDBC as UNTESTED. The pgJDBC runtime version must be
exactly 42.7.3; tokio-postgres is locked to 0.7.15 in Cargo.lock.

```powershell
cargo run --locked --manifest-path crates/giggles-pg-conformance/Cargo.toml --target-dir target-gd48-conformance -j1 -- K:/absolute/manifest.json
cargo test --locked --manifest-path crates/giggles-pg-conformance/Cargo.toml --target-dir target-gd48-conformance -j1
cargo clippy --locked --manifest-path crates/giggles-pg-conformance/Cargo.toml --target-dir target-gd48-conformance -j1 --all-targets -- -D warnings
cargo fmt --manifest-path crates/giggles-pg-conformance/Cargo.toml --check
```

## Evidence and limitations

`report.json` contains per-case expected/actual PASS/FAIL/UNTESTED, profile
integrity errors, exact executable/JAR/runtime/source identities and a truthful
aggregate verdict. The checked-in probes own the assertions; the Rust parser
accepts only their canonical PASS record after successful assertion completion.
It rejects missing, duplicate, malformed, unknown and oversized records, wrong
driver versions, capture overflow, nonzero exits and timeouts. Probe stdout is
a private protocol, not arbitrary text that can be interpreted as success.
Per-process stdout/stderr and exit/timeout/capture status are retained separately.
Each stream stores at most 256 KiB while draining further bytes, and any overflow
fails integrity. Manifests are at most 64 KiB, configs 1 MiB, cert/key files
64 KiB and fixture DBs 512 MiB total. Database directories are limited to 1024
entries and eight levels; links, escapes and special files are rejected.
Probe records and actual text have smaller limits.
Server readiness is bounded at 10 seconds; shutdown requests `shutdown` on
owned stdin and waits 10 seconds before killing/reaping that owned child.
Probe timeouts are 1..300 seconds. Processes launched here do not launch child
process trees; kill-on-drop covers early returns.

JDBC: connect/init, isValid, simple/prepared queries, all six supported types,
and table-free `SELECT 1` keep-alive queries through Statement/PreparedStatement,
SQL NULL/wasNull, repeated queries crossing prepare threshold, commit visibility,
rollback, failed transaction recovery, authorization, close, and actual metadata
tables/columns/PK/type-info/hidden-column probes. Metadata assertions require
the real visible fixture catalog; a fabricated empty catalog fails. Type mapping
expects pinned pgJDBC `BIT` for BOOL and `BINARY` for BYTEA; an intentional alternate driver mapping
must be reviewed as a contract change, never silently accepted after a failure.
The error recovery contract expects 23505 for duplicate PK and 25P02 until rollback.

Tokio is a three-case connection/simple/prepared baseline, not the entire JDBC
matrix. psql and Python psycopg/libpq are inventoried without connecting; they
remain UNTESTED until an actual equivalent probe is implemented and executed.
Missing tools are recorded with concrete spawn/import availability diagnostics.
Schema-change fencing and cancellation remain explicit UNTESTED JDBC cases:
they need a bounded owned admin/long-running query fixture. Thus the complete
matrix currently cannot claim PASS, even if all implemented cases pass.

Schema 2 reports identify the actual emitted Java source and compiled probe classes.
`observed_repository_sources` contains separately labeled checkout observations;
these may differ from the source embedded in an older runner executable. They
are not build provenance. Frozen executable identities are not proof
that an arbitrary supplied server executable matches a repository revision.
Keep its build command/source attribution beside the report when publishing it.

The `datagrip-discovery` profile executes the captured DataGrip 2026.2.5
`IsSuperUser`, `ListDatabases`, `ListSchemas`, `ListExistentLanguages`,
`ListLanguages` and `RetrieveEventTriggers` SQL through pgJDBC 42.7.3, with exact expected
identity, privilege, and database facts. The complete installed resource and its
98-section inventory live in `probes/datagrip-2026.2.5`; frozen copies and hashes
are retained in each report directory. Remaining templates are explicitly
UNTESTED. This profile does not execute DataGrip's GUI or its template engine.

JDBC also asserts current database/schema/user/session identity, getSchema,
SERIALIZABLE isolation, filtered schema/catalog/table identities, column order
and nullability, PK sequence, and named type mappings. Composite PK and multiple
authorized table cases remain UNTESTED because the offline fixture grants only
`app`; the real hidden `other` table remains an executed negative control.

pgJDBC 42.7.3 intentionally reports NULL TABLE_CAT/TABLE_CATALOG in schemas,
tables, columns and primary-key results; the probes require NULL plus wasNull.
getCatalogs instead returns the actual `giggles` database name.

The DataGrip discovery profile also exercises the installed connection helper's
`version()` query, stable service startup epoch, and pgJDBC's secondary `xid`
column-type lookup. These are actual JDBC calls, not a claim of complete GUI
introspection support. Optional PostgreSQL lock-age/MVCC probes remain unsupported.

PostgreSQL query errors include a static `phase` and SHA256 `query_id` in the
ErrorResponse detail and server console. The hash identifies the original SQL
bytes across prepared execution; SQL text and bound values are not logged.

The latest captured discovery batch retains fourteen expanded statements and raw
SQL SHA-256 identities in `latest-expanded-queries.json`. Each required
`dg_Batch_*` case replays the exact statement through both simple and prepared
JDBC execution, checks every result column label and metadata type, and checks
independent fixture facts: one application role, no timezone database, one database,
two authorized schemas, and no invented foreign objects, extensions, casts, or
role grants. Empty results still resolve all column metadata, including the
secondary pgJDBC custom-type queries. The fixture does not model PostgreSQL role
inheritance, connection-limit policy, validity, configuration, or ACLs; metadata
must preserve false/NULL as specified by the probe.

`dg_StateNumber` and `dg_CurrentXid` assert the explicit unsupported SQLSTATE
`0A000` and remain UNTESTED. An unexpected success or different rejection is a
failure. An expected rejection never becomes a passing client-support claim.
The full discovery profile remains UNTESTED while unreplayed templates remain.

Timezone catalog rows are empty because the engine has no timezone conversion
database. A startup TimeZone label and SHOW response do not establish timezone
conversion support.

Seven additional `dg_App_*` cases execute installed table/column/constraint/index
queries against the six-column `app` fixture. They derive schema/table/index/PK
OIDs from the real catalog, substitute the schema OID into the pinned PostgreSQL
14 full-schema expansions, and assert both simple and prepared result metadata,
column order/types/nullability, PK membership and object links. Every case
rejects disclosure of the real hidden `other` table. The installed templates,
expansions, hashes and independent fixture contract are frozen with each matrix.
The bounded catalog normalizes `varchar` casts to its TEXT representation; the
probes explicitly check TEXT metadata for those casts. Index option/collation/
opclass IDs are zero where no PostgreSQL access-method mapping exists; absent
method capabilities remain NULL.
