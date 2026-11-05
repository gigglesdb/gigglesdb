# giggles-language

`embedded-trusted-native-0.1` is a finite trusted embedded native language over
`giggles-core`'s durable SQL catalog and explicit transactions. It provides a
parser, typed AST/bindings, executor and persistent local CLI. It is not a task
authorization layer: independent `Admin` and file owners are trusted.

From the repository root in PowerShell:

```powershell
$env:CARGO_BUILD_JOBS='1'
$env:CARGO_TARGET_DIR='K:/Projects/gigglesdb/crates/giggles-language/target-native'
cargo build --manifest-path crates/giggles-language/Cargo.toml -j1
$cli='crates/giggles-language/target-native/debug/giggles-language.exe'
& $cli capabilities
& $cli init example.db 'create table work (id bigint primary key, state text not null, note text)'
& $cli run example.db 'insert work set id=1,state="ready"'
& $cli run example.db 'work where state=$state order by id desc limit 10 select id,state' '{"state":{"Text":"ready"}}'
& $cli run example.db 'update work where id=1 when state="ready" set note="checked"'
& $cli run example.db 'claim work where state="ready" order by id set state="taken"'
& $cli run example.db 'delete many work where state="taken" at most 1'
```

Use a fresh path for `init`; it accepts one explicit trusted core SQL schema
statement, specifically `CREATE TABLE`. A failed initialization retains its fresh
database directory; use a new path for another attempt. Reopening with `run` uses the existing core database; no separate
datastore or background service exists. PowerShell 5's native argument quoting
can alter embedded double quotes; on such hosts use `ProcessStartInfo.ArgumentList`
from PowerShell 7 or invoke the library. The subprocess tests pass exact argument
vectors without shell interpolation.

```rust,no_run
use giggles_core::{Admin, Limits};
use giggles_language::{Bindings, Engine, Value};
# fn example() -> Result<(), Box<dyn std::error::Error>> {
let admin = Admin::open("example.db", Limits::default())?;
let engine = Engine::new(admin);
let bindings = Bindings::from([("state".into(), Value::Text("ready".into()))]);
let result = engine.execute_text("work where state=$state limit 10 select id", &bindings)?;
assert!(result.rows.len() <= 10);
# Ok(()) }
```

The public `parse` and `Engine::execute` APIs expose the same typed AST as text.
All identifiers and trees are checked before effects. Bindings are exact: missing
or unused names fail. JSON bindings use core serde tags (`{"Int":1}`,
`{"Text":"x"}`, `{"Bool":true}`, `{"Bytes":[0,255]}`, `"Null"`). Names
fold ASCII lowercase. Text literals use JSON double quotes and escapes, with no
interpolation. `--` comments, whitespace, and a final semicolon are supported.
Only one statement is accepted. Quoted/qualified identifiers are unsupported.

Supported types are core boolean, smallint, integer, bigint, text, bytes and NULL.
Integer range and column nullability are checked. Bytes require typed bindings.
Expressions support fields, values, comparisons (`= != < <= > >=`), `is [not]
null`, `not`, `and`, `or` and parentheses, with SQL three-valued logic. WHERE
retains true; WHEN requires true. Arithmetic and functions are rejected.

Reads use `table [where ...] [order by field [asc|desc],...] [limit value]
[select field,...|*]`, with default page 100 and maximum 1,024. Missing primary
key sort fields are appended ascending. NULL sorts last ascending and first
descending. `complete` says whether all matching rows were returned; `matched`
counts the full matching set. No cursors or historical snapshot API is offered.

Writes use:

```text
insert [into] table set field=value,...
update table where predicate [when guard] set field=value,...
delete table where predicate [when guard]
update many table where predicate set field=value,... at most value
delete many table where predicate at most value
claim table where predicate order by field [asc|desc],... set field=value,...
```

Singular writes count the entire WHERE set before testing WHEN: zero gives
`NotFound`, two or more `AmbiguousMatch`. Bulk caps are mandatory and atomic:
cap+1 gives `TooManyMatches`, zero effects. A claim chooses only the first ordered
candidate and requires its postimage to leave eligibility, or returns
`ClaimNotConsumed`. Empty claim succeeds with zero counts. No lease or worker
finalization protocol is provided. Primary key updates are rejected. Omitted
insert columns are NULL, subject to required-column checks; no defaults exist.

Every call scans and validates in one explicit core transaction, stages all
effects and commits once. This native profile supports tables with at most 1,024
rows. Core itself can store larger tables: a reverse primary-key probe verifies
that the forward scan was complete, and an oversized table returns `Limit`
before effects, even for a small read or point mutation. Native insert rejects
a table already at 1,024 rows. Full scans can additionally hit the 1 MiB result
limit and then fail with zero effects. Core also limits rows to 64 KiB, tables to 128, columns to 64 and
concurrent explicit sessions to 8. Native input is at most 64 KiB, 4,096 tokens
and expression depth 32. Scans and sorting are in memory inside the transaction;
no performance qualification is claimed. Whole-generation core certification
can conflict on unrelated writes. There is no automatic retry.

Mutation counts distinguish `matched`, `affected` and `changed`: a successful
no-op update affects one row and changes zero; it skips SQL DML and preserves
the row version. Insert/delete change each affected row. A successful claim returns
the selected full postimage and its actual core version, so a worker can identify
its claimed work. Empty claim returns table columns and no rows. Other mutations
return counts only. Reads affect/change zero. Counts are disclosed because this is a
trusted profile. CLI success prints JSON; errors print tagged JSON on stderr and
exit nonzero. `Conflict` proves no publish for this call. `Unknown` means a
storage error occurred during COMMIT and effects may have persisted. **Do not
blindly retry Unknown.** Inspect/reopen and reconcile application state. There
are no durable native receipts, once keys or outcome lookup.

The larger native tour/writes/permissions contracts remain design targets.
This crate rejects once, atomic blocks, schemas/jobs, decimal/UUID/temporal/JSON
document types, joins, aggregation, functions, task policies and service/distributed
operations. This profile does not qualify the reference's complete “Local core”
bundle. No server integration, PostgreSQL comparator, security, cloud, production
or performance claim is made. Tests retain unique temporary databases for
inspection; no fixture cleanup occurs.
