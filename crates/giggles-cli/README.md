# giggles-cli

The `giggles-sql` command provides embedded SQL sessions, JSON/table output and a small REPL for local database owners.

**Status:** Opens databases through trusted local Admin authority; it does not provide remote access or task authentication.

Part of [GigglesDB](../../README.md).

A bounded embedded SQL CLI with a persistent session, JSON/table output and a
small REPL. It opens GigglesDB directly through trusted local `Admin` authority.
Access to the database files is authority; this tool neither accepts task
credentials nor grants remote access. It is not a network security boundary.

Build from the repository root in PowerShell:

```powershell
$env:CARGO_TARGET_DIR = "$PWD\crates\giggles-cli\target"
$env:CARGO_BUILD_JOBS = '2'
cargo build --manifest-path crates/giggles-cli/Cargo.toml -j2
$sql = "$env:CARGO_TARGET_DIR\debug\giggles-sql.exe"
& $sql --help
```

Create, insert, query and reopen an actual database. The unique path makes this
example repeatable without replacing a previous database:

```powershell
$db = Join-Path ([IO.Path]::GetTempPath()) ('giggles-demo-' + [Guid]::NewGuid())
& $sql --create $db --sql 'CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT)'
& $sql --open $db --sql "INSERT INTO notes VALUES(1,'hello; world')"
& $sql --open $db --sql 'SELECT * FROM notes LIMIT 10'
& $sql --open $db --format json --sql 'SELECT body FROM notes WHERE id = 1'
```

`--create` requires a fresh path, even an existing empty directory rejects.
`--open` requires an existing database and never initializes a missing one.
Exactly one of these modes and exactly one input source is required:

```text
giggles-sql (--create PATH | --open PATH)
  (--sql SQL | --file PATH | --stdin | --repl)
  [--format table|json] [--atomic] [--output NEW_PATH]
  [--params-file PATH] [--expected-version POSITIVE_U64]
```

Typed parameters are separate JSON, never substituted into SQL strings. The
array uses core `Value` encoding: `"Null"`, `{"Bool":true}`, `{"Int":-7}`,
`{"Text":"hello"}`, `{"Bytes":[0,255]}`. Parameters and CAS are available only
with exactly one `--sql` statement. Core checks types, parameter counts and the
supported SQL grammar. A stale CAS fails instead of overwriting another change:

```powershell
$params = Join-Path ([IO.Path]::GetTempPath()) ('giggles-params-' + [Guid]::NewGuid() + '.json')
[IO.File]::WriteAllText($params, '[{"Int":2},{"Text":"Unicode 雪; quotes '' stay data"}]')
& $sql --open $db --sql 'INSERT INTO notes VALUES($1,$2)' --params-file $params
$result = (& $sql --open $db --format json --sql 'SELECT * FROM notes WHERE id = 2') | ConvertFrom-Json
$version = [string]$result.result.rows[0].version
& $sql --open $db --sql "UPDATE notes SET body = 'updated' WHERE id = 2" --expected-version $version
```

For exact 64-bit integers/versions, machine consumers must use a lossless JSON
parser. PowerShell `ConvertFrom-Json` behavior depends on its runtime/version;
the small versions in this local example fit exactly.

Scripts require semicolon terminators, including the last statement. `--sql`
permits one final statement without a semicolon. Empty scripts succeed.
Semicolons in single-quoted literals, doubled quotes and multiline strings are
handled by the lexer. Backslashes are not SQL quote escapes. Comments outside
strings reject explicitly, matching the documented core subset.

```powershell
$script = Join-Path ([IO.Path]::GetTempPath()) ('giggles-script-' + [Guid]::NewGuid() + '.sql')
[IO.File]::WriteAllText($script, "INSERT INTO notes VALUES(3,'three'); INSERT INTO notes VALUES(4,'four');")
& $sql --open $db --file $script --atomic --format json
'SELECT * FROM notes LIMIT 10;' | & $sql --open $db --stdin
```

Ordinary scripts autocommit each statement. The first failure stops execution;
earlier autocommits remain. `--atomic` wraps the whole script in BEGIN/COMMIT,
rejects explicit BEGIN/COMMIT/ROLLBACK anywhere in that script, and rolls back
on SQL or output failure before commit. Output emitted during an atomic script
is provisional until the process succeeds. No automatic retry occurs. An IO
failure or lost commit response can leave an unknown outcome: reopen and inspect
application state before retrying non-idempotent work.

Start a persistent interactive session:

```powershell
& $sql --open $db --repl
```

Then enter:

```text
BEGIN;
INSERT INTO notes VALUES(5,'still staged');
SELECT * FROM notes LIMIT 10;
ROLLBACK;
.mode json
.schema
.help
.quit
```

BEGIN/COMMIT/ROLLBACK persist across input lines. EOF or `.quit` with an open
transaction rolls it back and exits 3. EOF with pending SQL rolls back any
transaction and exits 2. Any failure stops the run. Metacommands work only on
standalone lines between SQL statements; there are no shell escapes or file
loading metacommands. `.schema` reports the committed Admin catalog, so staged
DDL appears after commit. Prompts appear only on stderr for terminal stdin.
`--atomic` is unavailable for REPL; use SQL transaction commands there.

Table output quotes text, distinguishes `NULL` from `""`, prints bytes as hex,
escapes control characters and shows `@version`. Alignment padding is capped at
80 characters per column; longer values print in full and may extend past the
aligned column. Escaped Unicode uses Rust
escape notation; alignment counts escaped characters, not terminal graphemes.
JSON writes one envelope per SQL result:

```json
{"version":1,"result":{"columns":["id","body"],"rows":[{"values":[{"Int":1},{"Text":"hello; world"}],"version":1}],"affected":0}}
```

The shown row version is illustrative; actual versions come from core. Catalog
output has `{"version":1,"catalog":{"epoch":...,"tables":[...]}}`.
Diagnostics go to stderr. `--output` reserves a new file before database opening
or execution and never replaces an existing file, including an input file or
hardlink. It must be outside the database directory. Failed runs can leave a
reserved/partial output file; retain it for inspection and choose a new path.
Prior autocommits cannot be undone when output fails.

| Exit | Meaning |
| --- | --- |
| 0 | Completed successfully; no pending transaction |
| 2 | Arguments, input bounds/encoding/lexer or metacommand error |
| 3 | Core SQL/storage error, conflict, stale CAS, unfinished transaction |
| 4 | File, stream or output IO error |

Inputs must be UTF-8 without a BOM or NUL. A script/interactive run is bounded
to 1MiB and 256 nonempty SQL statements; one statement is bounded to 64KiB
including its surrounding whitespace but excluding its terminator. Interactive
lines are also bounded to 64KiB. Parameter JSON is at most 64KiB and 256 values.
Core independently enforces row/scan/SQL limits; see [SQL.md](../giggles-core/SQL.md).
This CLI documents and tests the common supported CRUD subset, and does not
promise joins or SQL compatibility beyond the current core contract.

## Usage from sh

From the repository root:

```sh
cargo run --manifest-path crates/giggles-cli/Cargo.toml -- --open /path/to/database --sql 'SELECT * FROM notes LIMIT 10'
```
