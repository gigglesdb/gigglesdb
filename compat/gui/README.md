# gui

A compatibility harness for contributors comparing GUI catalog requests against PostgreSQL.

**Status:** Exercises pgAdmin REST requests and a DBeaver source-derived replay; it does not qualify full GUI interaction.

Part of [GigglesDB](../../README.md).

Run from the repository root with a prebuilt default-feature server:

```powershell
python -I compat/gui/run.py --server-bin <work-dir>/target/debug/giggles-server.exe --tool all
```

`--tool pgadmin` or `--tool dbeaver` selects one lane. Each run writes
`report.md`, `report.json`, SQL manifests, raw JDBC observations and redacted
server/oracle logs beneath a unique run directory in the runner's fixed Windows work root. There is currently no output-root flag.
Exit codes: 0 = all comparisons pass, 1 = product gaps, 2 = harness/oracle error.
Requires Docker Desktop, Python 3, and JDK 21 on PATH. No Python packages are
installed. The server runs v2 with a fresh database and random password.

`compat/pins.json` pins pgAdmin 4 9.10 and PostgreSQL 18.6 by immutable image
digest. `gui/pins.json` pins pgJDBC 42.7.3 by SHA-256 and records tool versions.
The runner may pull these exact images. Passwords enter child commands through
environment variables. Container ports bind only to host loopback. PostgreSQL
data uses a 512 MB tmpfs and pgAdmin uses a 128 MB tmpfs; containers and network
are removed in `finally`. Per-run server data, compiled Java and frozen server
copies are removed after reports are written.

pgAdmin uses its actual browser REST endpoints in desktop mode, including
registration, password connection, database/schema/table/column/index and
four constraint trees, table properties and generated SQL. The oracle log
segment for each endpoint retains the exact SQL it executed. Unparameterized
queries are replayed against both databases; sample object OIDs are adapted
for GigglesDB and both SQL forms are retained in the observations. Parameterized
queries remain in the raw log rather than being guessed during replay.

DBeaver is a source-derived replay, not an application UI test. `queries.json`
contains the SQL emitted by the PostgreSQL model caches at DBeaver 24.3.0,
with source URLs, source-file hashes and line ranges. Branches select PostgreSQL
10+ partition fields, sequence dependencies, constraint expressions and relation
size calculation. The ordered browse path is schemas, tables in `gui_sales`,
columns of `child`, constraints and indexes of `child`. Parameters are resolved
independently on each target and bound through pgJDBC. Additional pgJDBC
DatabaseMetaData calls cover schemas, tables, columns, indexes, PKs and FKs;
their actual oracle SQL is recorded under each result.

PASS means request success and matching result shape, including result column
labels/JDBC types and sample row counts. The complete schema inventory count is
recorded but excluded because the targets have different system namespaces.
Raw rows and API bodies remain available for semantic inspection. This bounded
harness does not qualify full GUI interaction or all metadata values.
