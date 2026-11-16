# giggles-language

`embedded-trusted-native-0.1` is a finite trusted embedded native language over
`giggles-core`'s common typed operation engine, durable catalog and explicit
transactions. Native text and typed ASTs lower directly into engine requests;
they do not generate SQL or invoke the SQL parser. SQL remains a supported
frontend over the same catalog, mutation primitives and storage. This crate
provides a parser, typed AST/bindings, adapter and persistent local CLI. It is not a task
authorization layer: independent `Admin` and file owners are trusted.
Version 0.1 remains the default. Explicit version 0.2 selects
`embedded-trusted-native-0.2`, adding checked integer arithmetic in predicates.
Explicit version 0.3 selects `embedded-trusted-native-0.3`, adding computed
update/claim assignments and aliased projections. Explicit version 0.4 selects
`embedded-trusted-native-0.4`, adding standalone write return projections.
Explicit version 0.5 selects `embedded-trusted-native-0.5`, adding native
transaction blocks, expression-based VALUES inserts and bulk guards.
Explicit version 0.6 selects `embedded-trusted-native-0.6`, adding capped
multirow VALUES inserts with atomic preflight and optional return projections.
Explicit version 0.7 selects `embedded-trusted-native-0.7`, adding standalone
safe table creation through separate typed catalog APIs.
Explicit version 0.8 selects `embedded-trusted-native-0.8`, adding the fixed
COALESCE, ABS and LENGTH scalar functions.
Explicit version 0.9 selects `embedded-trusted-native-0.9`, adding searched/simple
CASE and backtick syntax for existing canonical lowercase ASCII identifiers.
Explicit version 0.10 selects `embedded-trusted-native-0.10`, adding atomic
ON CONFLICT handling over the existing full primary key.
Explicit version 0.11 selects `embedded-trusted-native-0.11`, adding trusted Rust
once execution and outcome lookup with explicit original scope and a certified
clock. The CLI rejects these two forms before opening a database; ordinary 0.11
commands remain usable.
Explicit version 0.12 selects `embedded-trusted-native-0.12`, adding local
capability/limit discovery and trusted committed catalog inspection. It inherits
0.11 once support in Rust; CLI once/outcome still require unavailable context.
Explicit version 0.13 selects `embedded-trusted-native-0.13`, adding bounded
trusted static explain for simple reads.
Explicit version 0.14 adds native CREATE INDEX text; 0.15 adds Numeric parameters;
0.16 adds unconstrained Numeric column declarations. Version 0.17 selects
`embedded-trusted-native-0.17`, inheriting 0.16 and adding standalone
`drop table NAME [cascade|restrict]` and `drop index NAME [cascade|restrict]`.
RESTRICT is the default. SQL dependency behavior is shared; DROP INDEX CASCADE
retains SQL's Unsupported result. Legacy drops require empty business state;
v2 permits populated state.
Explicit version 0.18 selects `embedded-trusted-native-0.18`, inheriting 0.17
and accepting `schema.table` in reads, writes, blocks, CREATE TABLE, CREATE
INDEX targets and DROP TABLE/INDEX. Each component follows the existing
identifier/backtick rules; bare tables resolve through the default public search
path. CREATE INDEX names remain unqualified. Create schemas through SQL.
Explicit version 0.19 selects `embedded-trusted-native-0.19`, inheriting 0.18 and
adding, each checked against the same SQL on legacy and v2:
- typed reads: `orders where qty > 1 group by item having count(*) > $n order by item
  limit 10 select item, count(*) as n, sum(qty)` and two-source joins
  `items i left join orders o on i.id = o.item select i.id, o.qty`
  (count/sum/min/max/avg, GROUP BY, HAVING, ORDER BY, LIMIT; bare table names,
  `select` required, standalone statements only);
- subquery predicates `n [not] in (u where ... select n)` and `[not] exists (u ...)`
  over uncorrelated single-source reads, and `insert into dst(id, v) from src where
  v > 1 select id, v`. Subqueries inside JOIN ON and HAVING also match SQL;
- `alter table t add [column] c [nullable] int64`, `drop [column] c [cascade|restrict]`,
  `rename [column] a to b` and `rename to t2` (populated tables are accepted like SQL on both engines);
- `savepoint a`, `rollback to [savepoint] a` and `release [savepoint] a` inside a
  `transaction { ... }` block of plain statements (no let/require/emit; a failing step
  rolls back the whole block; such blocks cannot be prepared or used with once);
- real, double precision, date, time, timetz, timestamp, timestamptz and interval
  parameters wherever parameters are accepted; uuid/jsonb text parameters use SQL
  coercion. Numeric parameters remain assignment-only.
Views, constraints and paged reads are not in 0.19.
Profile 0.20 selects `embedded-trusted-native-0.20`, inheriting 0.19:
- views through `execute_catalog_text("create view v as orders select id,qty")`,
  then `execute_text("v where qty > 1 select id", &bindings)` on V2.
  `execute_catalog_optional_text("drop view if exists v")` returns `None` for an
  absent view; `execute_catalog_optional(&statement)` is the corresponding tree API.
- constraints: `alter table child add constraint uq unique(code)`,
  `alter table child add check(qty > 0)` and `alter table child add constraint fk
  foreign key(parent_id) references parent(id) on delete cascade` through catalog
  execution. Use `alter table child drop constraint uq`, or the optional catalog
  API for `alter table child drop constraint if exists uq`.
- keyset paging through `read_page_text("page orders order by id limit 10 after
  ($last) select id", &bindings)`. Feed the returned `next` values into the next
  page; ordering must end in the primary key. Paging uses its dedicated API.
- savepoint recovery: `transaction { insert into t(id) values(1); savepoint a;
  insert into t(id) values(1); rollback to a; insert into t(id) values(2) }`
  commits IDs 1 and 2. A failing step must be immediately followed by a valid
  `rollback to` for recovery; rolled-back steps disappear from summaries.
- typed reads and INSERT FROM inside blocks: `transaction { let g=(orders group
  by item select item,count(*) as n); insert into dst from orders where qty > 1
  select id,item,qty; emit @g.rows }`. Typed row results are emitted as `@g.rows`;
  typed blocks cannot be prepared or used with once.
- frozen handles: `let bound = engine.prepare_bound_text("orders select count(*)",
  &bindings)?; engine.execute_bound(&bound)?;` and `prepare_bound_text("insert into
  dst from orders select id,item,qty", &bindings)`. Preparation freezes arguments
  and catalog identity, describes columns without scanning rows, and execution
  observes fresh rows. Foreign databases, changed catalogs and older profiles
  reject the handle. Plain projected view reads can also be prepared on V2.
- typed explanations through `explain_query_text("explain orders group by item
  select item,count(*) as n", &bindings)` return sources and columns without
  executing. The simple-read `explain_text` API keeps its prior shape.
- qualified typed statements support `app.orders group by item select item,count(*)`,
  `app.orders o join orders p on o.id=p.id select o.id`, and
  `orders where item in (app.orders select item) select id`, matching SQL for
  schema-qualified sources, including V2 views.
  Qualified targets work: `insert into app.dst from orders select id,item,qty`,
  with explicit or omitted target columns and frozen handles. Aliases and
  `alias.column` remain supported; `schema.table.column` is outside this grammar.

Asserted native/SQL differences and shared limits in 0.20:
- V2 plain view reads require supported explicit projections. Arithmetic view
  projections (`v select qty*2 as d`) and omitted projections (`v`) return native
  42P01 while SQL succeeds.
  Legacy view DDL returns 0A000 in both frontends. Filtered and joined views are
  also a shared 0A000 gap versus PostgreSQL 18.6.
- Native cross-column CHECK (`qty > id`) returns `Unsupported` while SQL
  succeeds. Native DML retains `Duplicate` for
  SQLSTATE 23505. DROP CONSTRAINT CASCADE returns shared 42601.
- Paging `matched` counts eligible rows before LIMIT; SQL exposes returned rows.
  Native tuple keyset paging works where core SQL row-value comparison returns
  0A000; equivalent scalar disjunctions match its rows.
- Without immediate valid recovery, native returns the original failure and
  rolls back the block. SQL can first report 25P02 on a later statement or 3B001
  on an unknown savepoint, then recover later with a valid ROLLBACK TO. Version
  0.19 always rolls back on the original failure.
- Typed block rows require `emit @name.rows`; scalar/row emits, read-only typed
  writes, prepared typed blocks, once and generic `lower_text` stay unsupported.
  Typed explanation uses `explain_query_text`; typed INSERT FROM explanations
  remain unsupported. Bound handles are local native APIs, with no SQL handle
  equivalence claimed.
Profile 0.21 selects `embedded-trusted-native-0.21`, inheriting 0.20:
- column options in CREATE TABLE: `create table t (id serial, code int64 default
  7, name nullable text default 'x', n int64 default nextval(s), primary key (id))`;
  `bigserial`, `generated always as identity` and `generated by default as
  identity`. Defaults are literals or `nextval(seq)` and go through SQL's
  validation, stored default text and owned sequences (`t_id_seq`).
- foreign keys in CREATE TABLE: column `p int64 default 1 [constraint fk]
  references parent(id) on delete set default` and table `[constraint fk] foreign
  key(a) references parent [(id)] [on delete|on update action]`. They run as the
  table plus one ADD CONSTRAINT per key in one atomic catalog commit (core's
  CreateTable carries no constraints); `prepare_catalog_bound` is Unsupported.
- sequences: `execute_sequence_catalog_text("create sequence [if not exists]
  app.s")` and `"drop sequence [if exists] s [cascade|restrict]"` return the
  catalog epoch, or `None` when IF [NOT] EXISTS skipped. `execute_sequence_text(
  "nextval(s)" | "currval(s)" | "setval(s, $v [, false])", &bindings)` returns the
  value; each standalone call is a fresh session, so standalone `currval` gives
  55000. Inside blocks, `nextval(s)` and `let a = currval(s)` (emit `@a.rows`)
  share the block's session. The table catalog APIs refuse sequence DDL.
- full savepoint recovery: after a failing step, later statements, SAVEPOINT and
  RELEASE are skipped without summaries (SQL reports 25P02 for each) until a
  `rollback to` an existing savepoint recovers; `rollback to` an unknown name
  keeps the block aborted (SQL 3B001). A block that ends aborted fails with the
  original error and commits nothing.
Asserted native/SQL differences in 0.21:
- `default '5'` on int64 is a typed text default (42804); SQL coerces the
  unknown literal. Repo SQL accepts `SERIAL NULL` (PG and native: 42601) and
  rejects `DEFAULT .. GENERATED` order with 0A000. No clock defaults natively
  (native_column_defaults21_parity).
- Read-only blocks refuse nextval/setval up front (Unsupported; PG gives 25006
  at the call, repo SQL has no BEGIN READ ONLY). Standalone currval is 55000.
  `drop sequence if exists nope.s` returns `None`; repo SQL reports 42P01 where
  PG skips (native_sequence21_parity).
- Repo SQL COMMIT of an aborted transaction reports 25P02 (PG reports
  ROLLBACK); native returns the original error. Skipped `let` locals cannot be
  emitted (Unsupported) (native_recovery21_parity).
- On V2, CREATE TABLE foreign keys get ALTER TABLE ADD CONSTRAINT object IDs;
  names, columns and actions match SQL CREATE TABLE (native_create_fk21_parity).
No version implements the complete proposed native v1 language.

From the repository root in PowerShell:

```powershell
$env:CARGO_BUILD_JOBS='1'
$env:CARGO_TARGET_DIR='K:/Projects/gigglesdb/crates/giggles-language/target-native'
cargo build --manifest-path crates/giggles-language/Cargo.toml -j1
$cli='crates/giggles-language/target-native/debug/giggles-language.exe'
& $cli capabilities
& $cli capability-manifest
& $cli capability-manifest --language-version 0.2
& $cli capability-manifest --language-version 0.3
& $cli capability-manifest --language-version 0.4
& $cli capability-manifest --language-version 0.5
& $cli capability-manifest --language-version 0.6
& $cli capability-manifest --language-version 0.7
& $cli capability-manifest --language-version 0.8
& $cli capability-manifest --language-version 0.9
& $cli capability-manifest --language-version 0.10
& $cli capability-manifest --language-version 0.11
& $cli capability-manifest --language-version 0.12
& $cli init example.db 'create table work (id bigint primary key, state text not null, note text)'
& $cli run example.db 'insert work set id=1,state="ready"'
& $cli run example.db 'work where state=$state order by id desc limit 10 select id,state' '{"state":{"Text":"ready"}}'
& $cli run example.db 'update work where id=1 when state="ready" set note="checked"'
& $cli run example.db 'claim work where state="ready" order by id set state="taken"'
& $cli run example.db 'delete many work where state="taken" at most 1'
& $cli run --language-version 0.2 example.db 'work where id + 1 = 2'
& $cli run --language-version 0.3 example.db 'work select id,id+1 as next_id'
& $cli run --language-version 0.4 example.db 'insert work set id=2,state="ready" return id,state'
& $cli run --language-version 0.5 example.db 'transaction { let created=(insert into work(id,state) values(3,"ready") return id,state); require @created.changed=1; emit @created.row }'
```

Use a fresh path for `init`; it accepts one explicit trusted core SQL schema
statement, specifically `CREATE TABLE`. A failed initialization retains its fresh
database directory; use a new path for another attempt. Reopening with `run` uses the existing core database; no separate
datastore or background service exists. PowerShell 5's native argument quoting
can alter embedded double quotes; on such hosts use `ProcessStartInfo.ArgumentList`
from PowerShell 7 or invoke the library. The subprocess tests pass exact argument
vectors without shell interpolation.

`capabilities` retains the existing compact profile output. The separate
`capability-manifest` command, also available as Rust `capability_manifest()`,
returns inventory schema version 1 for the selected finite language version. Each
entry is keyed by language version, profile, Rust text/Rust tree/CLI interface,
trusted embedded deployment and feature. `implemented` names executable checks;
`rejected` names requests or typed-tree combinations that must fail before
effects; `unverified` makes no qualification claim. These test references are
not release attestations. The manifest publishes actual admission limits and
pins all six GD-0042 reference revisions. It does not advertise proposed native
v1, PostgreSQL 18 compatibility, server support or performance guarantees.
Unavailable text syntax has no corresponding Rust tree variant unless an
explicit tree rejection entry describes it.

The versioned CLI grammar is `run --language-version VERSION PATH NATIVE
[BINDINGS_JSON]`; `capabilities` and `capability-manifest` also accept
`--language-version VERSION`. Versions are exactly `0.1` through `0.12`; other values
return `Unsupported` before opening a database. Unversioned commands retain
0.1 behavior. Rust callers select `LanguageVersion::Embedded02` through `Embedded012` using
`Engine::with_version(admin, version)` and `parse_with_version(text, version)`;
`profile_for(version)` and `capability_manifest_for(version)` describe that
selection. `Engine::new`, `parse`, `profile` and `capability_manifest` retain
0.1 defaults. The version check also applies to hand-built trees and preparation.

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
`execute` and `execute_text` bind and execute in one transaction. Rust callers
can separately use `prepare_bound_text(text, bindings)` or
`prepare_bound(tree, bindings)`, followed by `execute_bound(&operation)`. All
real argument values are required and frozen at preparation; this is not a
template accepting different parameters on each execution. Preparation binds
names/types against a fresh committed catalog snapshot and discards that read
transaction. It does not scan rows, evaluate business conditions or change
durable state. Each execution checks database domain, catalog epoch and stable
table/column identities, then evaluates fresh rows. Schema drift or a restored
database rejects an old handle with `Core("Sql(StaleSchema)")` before effects.

The immutable handle exposes `language_version()`, `profile()` and
`description()` with the engine contract version and typed output columns.
Handles are local Rust values without a serialization format, SQL session,
network endpoint or authority credential; the executing engine still requires
trusted `Admin`. Cloning or repeating a handle starts another request, with no
deduplication or automatic retry. The CLI does not expose prepared handles.
A bound handle requires the same selected language version and profile when
executed; cross-version execution returns `Unsupported` before effects.

All identifiers and trees are checked before effects. Bindings are exact: missing
or unused names fail. JSON bindings use core serde tags (`{"Int":1}`,
`{"Text":"x"}`, `{"Bool":true}`, `{"Bytes":[0,255]}`, `"Null"`). Names
fold ASCII lowercase. Text literals use JSON double quotes and escapes, with no
interpolation. `--` comments, whitespace, and a final semicolon are supported.
The statement API accepts one statement. Qualified identifiers remain unsupported.
Profiles 0.1 through 0.8 reject quoted identifiers; 0.9 accepts only the canonical
backtick subset documented below.

Binding admission now rejects inputs the earlier executor left unbounded.
There may be at most 256 supplied named bindings, with at most 64 KiB of supplied
value payload in total. Text counts its UTF-8 bytes, bytes count their length,
and each integer, boolean or NULL counts 9 bytes. A separate 64 KiB budget counts
every explicit resolved literal or parameter occurrence in the operation:
reusing a parameter charges its payload again, including uses in a limit or cap.
Implicit defaults such as the default page size do not consume that budget.
Supplied-map checks follow request validation and precede catalog/transaction
admission; expanded-value checks happen before cloning each resolved value.
Overflow returns `Limit` with zero effects for both execution and preparation.
These limits apply after the typed map exists; they do not bound CLI JSON
decoding, total heap use or transport allocation. Under-budget missing/unused
bindings and type mismatches retain their existing errors.

Supported types are core boolean, smallint, integer, bigint, text, bytes and NULL.
Integer range and column nullability are checked. Bytes require typed bindings.
Expressions support fields, values, comparisons (`= != < <= > >=`), `is [not]
null`, `not`, `and`, `or` and parentheses, with SQL three-valued logic. WHERE
retains true; WHEN requires true. Version 0.1 rejects arithmetic. Versions before
0.8 reject scalar calls; 0.8 and later support only the three fixed functions below.

Version 0.2 adds unary minus and binary `+`, `-`, `*`, `/`, `%` in WHERE, WHEN and
claim predicates only. Unary minus binds before multiplication/division/remainder,
then addition/subtraction, then comparisons/NULL tests, NOT, AND and OR. Binary
arithmetic associates left; parentheses override precedence. Unary plus is
unsupported. `--` still starts a line comment: use `id - -1` to subtract a
negative number. The signed minimum literal `-9223372036854775808` is accepted;
its positive magnitude is not an integer value.

Arithmetic operates on checked signed 64-bit values, including values read
from smallint/integer columns, without changing those columns' stored types or
write range checks. Division truncates toward zero; remainder has the dividend's
sign. Negating the signed minimum or dividing it by `-1` returns `Overflow`;
its remainder by `-1` is zero. Division or remainder by zero returns
`DivisionByZero`. After evaluating operands, a NULL operand yields NULL, so
`null / 0` is NULL while `null + (1 / 0)` fails. Boolean operands are evaluated
eagerly too; false AND does not conceal a runtime error. Binding checks every
operand even on empty input. Preparation binds types without evaluating
arithmetic; execution evaluates predicates over complete input before paging,
so LIMIT 0 does not hide an error in an existing row.

Computed projections and SET expressions remain rejected in versions 0.1 and
0.2. All versions reject arithmetic sort keys, arithmetic in LIMIT or `at most`,
computed INSERT SET values, decimal/float arithmetic, casts and constructors.
Versions before 0.5 also reject the column-list VALUES insert form.
SQL syntax and its existing type and aggregate contracts remain unchanged.

Version 0.3 accepts expressions in update and claim assignments, including field
copies, checked integer arithmetic, comparisons and boolean expressions. Every
right-hand side reads the old row: `set a=b,b=a` swaps values. All assignments
are evaluated before applying the row's postimage. Target type, integer range,
nullability and row-byte checks still apply; primary-key writes remain rejected.
Cardinality/caps precede guard and assignment evaluation. The engine validates
all selected postimages before staging writes, including the claim's requirement
to leave eligibility. A later arithmetic or size error produces zero effects.
No-op updates still preserve row versions.

Computed mutations have a 1 MiB aggregate payload budget across every selected
postimage, including no-ops; each stored postimage also stays within 64 KiB.
The aggregate budget counts full resulting rows, including unchanged columns,
using the same text/bytes/scalar accounting as binding admission. It is separate
from the 64 KiB bound-value budget and is not a total heap-use guarantee.

Version 0.3 also accepts `select id as item_id,n+1 as next_n`. Every non-field
projection requires `as name`; field aliases are optional, and output names must
be unique. Aliased field outputs retain their source column ID and data type.
Computed outputs use column ID 0 and their bound expression type. An untyped
NULL projection is rejected because it has no output type. Projection aliases
do not become input fields for another expression or an ordering clause.
All eligible projected values are evaluated and charged against a 1 MiB total
before paging, so LIMIT 0 and later pages cannot conceal expression errors or
an oversized result. Projected output has no additional 64 KiB per-row ceiling;
stored rows and computed mutation postimages still do.

The public 0.3 AST appends `Statement::UpdateExpressions` and
`Statement::ReadExpressions`, with `Projection { expression, alias }`. Text with
only plain literal/parameter assignments or unaliased field projections keeps
the existing `Update`/`Read` variants, including ordinary negative assignment
literals. Versions 0.1 and 0.2 reject the new variants even when they contain only
simple values or fields. Preparation freezes parameters and typed output
metadata without evaluating expressions; execution uses fresh rows. The 0.3
manifest adds `computed_mutation_bytes` and `computed_projection_bytes`; these
optional limit fields are omitted from the older versions' JSON.
Versions 0.4 through 0.10 retain the 0.3 computed-expression limits, with the
explicit scalar additions described below.

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

Version 0.4 allows one standalone write to end with `return projection` or
`return *`, after its `at most` cap if present. For example,
`update work where id=2 set state="done" return id,state` returns the final row's
selected values. DELETE returns preimages; INSERT, update and claim return
postimages. Successful no-op updates return matched rows with unchanged versions.
Changed rows carry their actual stored versions; deleted rows retain their
pre-delete versions. Bulk returned rows have primary-key order. Empty bulk or
claim results still report the requested columns, with no rows. Counts keep
their existing meanings and `complete` is true: return results have no paging.

Return expressions use the same field, type and alias rules as 0.3 projections.
The engine evaluates them once and checks the complete 1 MiB output payload
before staging writes. A return expression or output-size error leaves zero
effects; commit failure still follows the existing `Conflict`/`Unknown` rules.
There is no additional 64 KiB limit on each returned output row. The manifest
publishes `return_result_bytes` for version 0.4 and later. A return clause creates no
receipt, idempotency key or automatic retry behavior.

The AST appends `Statement::Returning { statement, projection }`; its inner
statement must be one direct write. An empty projection vector means explicit
`return *`. Text must spell a projection: bare `return`, mixed star/list,
repeated clauses, reads with return, and nested wrappers are rejected. Wrapper
shape is checked before recursive inspection, and body plus return expressions
share the existing input, node and binding budgets. Versions 0.1 through 0.3
reject the new clause and wrapper. `return` is contextual, so table, field and
alias identifiers named `return` remain valid. SQL's `returning` spelling is
unsupported. Unwrapped statements retain their existing output behavior.

Version 0.5 adds `insert into work(id,state) values($id,"ready")`, with one tuple
of scalar expressions. INSERT expressions have no target-row field scope; they
can use literals, parameters, and block locals when inside a transaction. The
existing `insert work set ...` spelling keeps its literal/parameter-only rules.
Version 0.5 also permits WHEN on update/delete many. The complete match set must
fit the cap before every guard is checked; any failed guard aborts the whole
operation before assignments or effects. Claim guards remain unsupported.
Versions 0.1 through 0.4 retain their earlier refusals, including wrapped writes.

The finite 0.5 block grammar is:

```text
transaction [read only] {
  read_or_write;
  let name = (read_or_write);
  require predicate;
  emit scalar, @name.row, @page.rows
}
```

Read/write, let and require items may repeat. Emit is optional, appears at most
once, and must be last. Items are separated by semicolons; one trailing
semicolon is allowed. An emit-only block is valid, but an empty block or empty
emit list is rejected. Blocks exclude claim, DDL, nesting, per-item once, savepoints and
recovery. All names and types bind before any item runs. Later items see earlier
staged writes at the same transaction snapshot; one final commit publishes a
write block. Any failure discards the whole attempt and its provisional output.
Read-only blocks reject writes upfront and finish without publishing a durable
generation. Block keywords remain contextual; a table named `transaction` still
parses as a read unless the tokens form a block opening.

Locals are immutable and cannot be referenced before their let item. A singular
write with RETURN exposes `@name.row.field` and whole `@name.row`; write locals
expose `@name.changed` and the finite status string `"success"`. Read locals
expose `@name.count`, the number of materialized rows, and whole `@name.rows`.
Rows emissions retain completeness, so a default page of 100 from 101 matches
is still an incomplete page. Whole row/rows values are allowed only in emit.
Local references do not add qualified table fields or locals to LIMIT, caps or
other literal/parameter-only value clauses. Require accepts true; false or NULL
returns `RequirementFailed` and aborts the block.

Statement `execute_text`/`execute` and `Outcome` remain unchanged. Use
`parse_transaction_with_version`, `execute_transaction_text` or
`execute_transaction` for blocks. `prepare_transaction_bound_text` and
`prepare_transaction_bound` freeze the actual parameters and typed local slots;
`execute_transaction_bound` uses a fresh snapshot and revalidates all referenced
table identities. `BoundTransaction` exposes the selected language/profile and
a description with all tables and typed emit shapes. It has no serialization,
authority credential or receipt. Scalar emit metadata uses `None` for untyped
NULL; nullable columns retain their declared type even when their value is NULL.
The CLI uses `parse_input_with_version` to dispatch a tokenized input to the
appropriate API, preserving the existing statement JSON shape.

`TransactionOutcome` contains read/write step summaries with their item indexes
and tagged scalar/row/rows emissions. Require items produce no summary. Without
emit, only summaries are returned. Each block has at most 64 items including
final emit, 64 locals and 64 emitted values. Source/tree size, token/node/depth
and binding limits apply across the whole block. Runtime local scalar copies
consume the remaining 64 KiB resolved-value budget once per bound occurrence,
not once per scanned row. Retained local results have a shared 1 MiB payload
budget, and emitted values have a separate 1 MiB budget charged on every emitted
occurrence before copying. Existing scan/mutation/result limits remain per
operation. These are finite admission bounds, not total heap, task-lifetime,
time or performance guarantees. They appear as optional 0.5 manifest limits.

Version 0.6 retains the 0.5 APIs and adds:

```text
insert many into work(id,state)
values (3,"ready"),(4,"ready")
at most 2
return id,state
```

`many`, `into`, the column list, a nonempty VALUES tuple list and `at most` are
required; return is optional and follows the cap. A cap is a literal or parameter,
not an expression or local reference. Multiple tuples without `many` are rejected.
Existing single-tuple VALUES retains `Statement::InsertExpressions`; the new
tree is `Statement::InsertMany { table, columns, rows, cap }`. Versions 0.1 through
0.5 reject it, including within return wrappers or transaction blocks. Prepared
statement and block handles still require exact language/profile equality.

Every tuple and return expression binds upfront with shared input/node/binding
limits. After binding, execution checks the input tuple count against the cap
before local materialization, complete table scans or tuple expression evaluation.
Preparation binds real values and metadata without deciding the execution cap.
Tuple expressions have no target-row field scope; block locals are allowed as
scalar operands. All proposed stored rows are checked before any writes, including
types, nullability, integer ranges and primary-key identities. Repeated proposed
primary keys return `DuplicateTarget`; conflicts with existing keys keep
`Core("Sql(Duplicate)")`. A row error, duplicate, capacity violation or return error
aborts the entire insert and its enclosing block.

The tuple limit is 1,024 on legacy and has no fixed row count on V2; the column
limit is 64 and every tuple must match the column count. Each proposed stored row
fits 64 KiB, and all proposed full rows share a 1 MiB payload budget (on V2 the
tuple literals are charged to that budget rather than the 64 KiB resolved-value
bound). On legacy the resulting table must remain within 1,024 rows; V2 has no
table ceiling. Like SQL INSERT, text that begins with `insert` is bounded by SQL's
statement byte budget (`insert_text_bytes`) and flat-tuple token budget
(`insert_text_tokens`) instead of the 64 KiB / 4,096-token text bounds, and each
tuple has its own 4,096-node budget. The manifest exposes separate
`insert_input_rows` and `insert_proposed_bytes` fields for 0.6 and later; the
legacy-valued row fields are kept, and `engine_rows.legacy` / `engine_rows.v2` give
per-engine row ceilings (`null`: no fixed ceiling). On V2, filtered native reads,
updates and deletes retain only matching rows while streaming, and a filter that
fixes the full primary key with literals reads that one key.

An empty typed `rows` vector is valid after checking columns, cap and return
metadata; there are no row-level required-column checks when no row is proposed.
Text does not accept an empty tuple list or an empty tuple. Empty execution returns
zero counts and, when requested, typed return columns with no rows. Nonempty
return results are complete and ordered by primary key, with actual stored row
versions. An INSERT MANY block local exposes `status` and `changed`, including
when only one tuple is supplied; it has no singular `.row` or read `.rows` member.
Source-fed inserts, conflict actions and upserts remain unsupported.

Version 0.7 adds safe standalone table creation without requiring SQL provisioning:

```powershell
& $cli init --engine legacy native-schema.db
& $cli run --language-version 0.7 native-schema.db 'create table work (id int64, state text, note nullable text, primary key (id))'
& $cli run --language-version 0.7 native-schema.db 'insert into work(id,state) values(1,"ready")'
```

`init [--engine legacy|v2] PATH [SQL_SCHEMA]` selects the engine for a fresh
database. The default is `v2`, enabled by this crate's default `engine-rocksdb`
feature. Use `init --engine legacy native.db` to create a legacy database in
both builds. With `--no-default-features`, plain `init` and `init --engine v2`
fail before filesystem effects and direct callers to `--engine legacy`; there
is no automatic fallback. Explicit `init --engine v2 native.db` is also accepted
when `engine-rocksdb` is enabled.
Unknown engine names and unsupported language versions reject before filesystem
effects. `--engine` is init-only: `run` detects the existing database's engine
automatically, and repeated `init` refuses an existing path even with a different
engine flag. The existing `init PATH SQL_SCHEMA` form remains supported. Native catalog
requests use `create table name (column,...,primary key (name,...))`, with one
explicit table-level primary key. Column syntax is `name [nullable] type`;
types are exactly `bool`, `int32`, `int64`, `text` and `bytes`. Fields are required
by default. Omitted nullable fields become NULL on insert; explicit NULL in a
required field fails. An explicitly nullable primary-key column is rejected.
SQL retains nullable-by-default columns and its existing smallint type; native
creation does not add SQL type aliases or a smallint spelling.

The separate AST is `CatalogStatement::CreateTable { table, columns, primary_key }`,
with `ColumnSpec { name, data_type: CatalogType, nullable }`. Use
`parse_catalog_with_version(text, version)`, `execute_catalog_text(text)` or
`execute_catalog(&statement)`. Catalog requests have no value bindings; the CLI
rejects a nonempty bindings map before effects. `ParsedInput::Catalog` lets the
CLI dispatch a tokenized CREATE TABLE while the existing statement and block
APIs retain their result shapes. Earlier profiles reject catalog text, trees and
handles. DDL is also rejected inside data blocks and return wrappers.

Successful execution returns `CatalogOutcome { catalog_epoch, table }`, containing
the committed epoch and real assigned table/column IDs. This finite native path
permits creates only when business state is empty: all relational tables have no
rows, and built-in rows, tasks, decisions/receipts, approvals and accounting are
empty. Existing empty tables are allowed; counters, time and restore-origin
metadata do not themselves block creation. A valid create on populated state
returns `PlanRequired` with no catalog effects. SQL CREATE retains its existing
behavior. This is a synchronous safe-create capability, not a migration job API.

`prepare_catalog_bound_text(text)` and `prepare_catalog_bound(&statement)` retain
an immutable ID-free definition and its database domain/catalog epoch without
allocating IDs or deciding the population policy. `execute_catalog_bound(&handle)`
requires the same selected version/profile, revalidates identity, checks current
business-state emptiness and commits once. Handles expose `language_version()`,
`profile()` and `description()` and have no serialization format or authority
credential. A different domain or catalog epoch rejects the handle; newly added
data with an unchanged catalog returns `PlanRequired`. Failed definitions leave
IDs and epochs unchanged. Commit errors keep the existing Conflict/Unknown rules.

Table and field names use the same finite canonical identifier rules as data
requests. Definitions have at most 64 columns and 64 primary-key names; the
catalog remains limited to 128 tables. Text/tree input limits still apply.
The manifest adds `catalog_primary_key_columns` for 0.7 and later. Schema-qualified names,
defaults, inline primary keys, additional constraints, indexes, partitioning,
create-if-missing, alters and drops remain unsupported; unsupported clauses are
never silently ignored.

Version 0.8 accepts `coalesce(a,b,...)`, `abs(value)` and `length(value)` in the
existing expression scopes: predicates, computed SET/projections, VALUES tuples,
return expressions, and transaction require/emit. Function names fold ASCII case
and are recognized only when followed by `(`. Bare `abs`, `length` and `coalesce`
remain ordinary table/field/alias identifiers. Through 0.8, CASE grammar is
unchanged, including existing fields named `case` and the write form
`where case when guard`. Version 0.9 requires that keyword field to be quoted.

COALESCE requires at least two compatible arguments. It evaluates arguments
left-to-right until the first non-NULL result, so `coalesce(1,1/0)` succeeds.
Every argument still binds and consumes its input budget: an unknown field, missing
parameter or incompatible type fails even in an argument that will not run.
There are no implicit conversions between integer, text, bytes and boolean families.
COALESCE is untyped only when every argument has unknown NULL type, as in
`coalesce(null,null)`: a projected output is rejected, scalar emit metadata has
no data type, and typed outer or assignment contexts keep their checks. Known
argument types survive runtime NULL values; `coalesce(abs(null),length(null))`
has BigInt type and returns NULL.

ABS accepts exactly one integer or NULL. It returns the checked absolute value;
the signed minimum returns `Overflow`. LENGTH accepts exactly one text, bytes or
NULL value. Text length counts Unicode code points, including combining marks
separately, while bytes length counts bytes. It does not normalize text or count
graphemes. ABS(NULL) and LENGTH(NULL) return NULL with BigInt output metadata.
Call outputs use synthetic column ID 0; integer results use BigInt, and all call
output metadata is conservatively nullable even when particular arguments are not.
Existing destination integer ranges and required-column checks still apply.

The AST appends `Expr::Call { function, arguments }` with
`ScalarFunction::{Coalesce,Abs,Length}`. Calls have at most 64 arguments:
too many returns `Limit` before argument traversal, and invalid arity returns
`Type`. The manifest reports `function_arguments` for 0.8 and later. All arguments
share the existing expression node/depth, input and resolved-value limits.
Transaction local occurrences in every argument are materialized and charged
before scalar evaluation, including arguments COALESCE will skip. Selected text
and bytes results remain borrowed until existing output/mutation admission.

Preparation binds all arguments and freezes supplied values without evaluating
the functions. Text, trees and prepared execution use the same core evaluator;
earlier profiles reject call nodes in every scope, including nested Require/Emit.
Exact prepared-handle version/profile matching remains enforced. Functions do not
extend value-only LIMIT/caps or legacy INSERT SET. CASE requires 0.9; LOWER/UPPER,
arbitrary functions, casts and other scalar families remain unsupported. SQL
grammar is unchanged.

Version 0.9 adds searched and simple CASE in every existing expression scope:

```text
items select case when n < 0 then -n else n end as magnitude
items select case state when "ready" then 1 when "done" then 2 end as stage
transaction { emit case when true then "selected" else "other" end }
```

Searched CASE accepts boolean or NULL conditions and selects the first TRUE arm.
Simple CASE evaluates its operand once and tests successive WHEN expressions
with ordinary equality: NULL never matches NULL. A NULL operand still evaluates
WHEN expressions, so `case null when 1/0 then 1 else 2 end` fails. Evaluation
stops at the selected arm and evaluates only its result; if none matches, it
uses ELSE or NULL. Conditions and results in every arm still bind before any
execution. Simple-CASE operand/WHEN types unify separately from result/ELSE
types, with no implicit conversion between scalar families. Unknown fields,
missing bindings and incompatible types in skipped branches remain errors.

The AST appends `Expr::Case { operand, arms, else_expr }`. It admits 1 to 64 arms;
zero arms returns `Type` and too many returns `Limit` before branch traversal.
Every explicit operand, WHEN, THEN and ELSE is a child expression and shares
existing node/depth, input and bound-value limits. An omitted ELSE consumes no
input node or value budget. Transaction local occurrences in every branch are
materialized and charged upfront, including branches scalar evaluation skips.
Selected text/bytes results remain borrowed until output or mutation admission.
CASE output metadata is conservatively nullable, uses synthetic column ID 0 and
BigInt for integer results. A result whose branches all have unknown NULL type
is rejected as a projected column but can be a scalar emit with no data type.
Known result types survive runtime NULL values. Computed projections still
require explicit aliases. Preparation binds without evaluating CASE; prior
profiles reject its typed nodes in every scope, including nested Require/Emit.

In 0.9, `case`, `then`, `else` and `end` are syntax keywords. Existing catalog
names do not change, but text references to those names require backticks:

```text
-- Up to 0.8: update items where case when guard set n=1
update items where `case` when guard set n=1
items select case `case` when true then 1 else 0 end as result
items select `null`, `coalesce`, n as `end`
transaction { let case=(insert items set id=1,n=2 return n as `end`); emit @case.row.`end` }
```

Backticks accept only names already in the canonical catalog domain:
`[a-z][a-z0-9_]{0,62}`. They can spell syntax keywords without folding or
normalizing names. Quoted `null` is a field, not a literal; quoted `coalesce`
is a field, not a function call. There are no escapes or embedded backticks.
Unterminated quoting returns `Syntax`, a decoded name over 63 bytes returns
`Limit`, and other unsupported names return `Unsupported`. This finite syntax
does not support the reference's broader case-preserving or Unicode names,
schema qualification, or a new durable name format. Protected core catalog
names remain rejected. Typed AST names are unquoted canonical strings; the
selected version applies the corresponding semantic name checks.

LET names and the name immediately after `@` remain bare canonical variables.
Keyword variables such as `let case` and `@case` are allowed in 0.9; quoted
variable names and leading underscores are rejected. The final field of
`@local.row.field` uses the ordinary catalog/output name parser, so keyword
fields need backticks. Fixed local selectors and `$` parameter rules are
unchanged. The manifest publishes `case_arms` and `quoted_identifier_bytes`
for 0.9 and later. Exact prepared-handle version/profile matching remains required.

Version 0.10 adds explicit conflict handling to column-list VALUES inserts:

```text
insert into items(id,n) values(1,2)
on conflict(id) do nothing
return id,n

insert many into items(id,n) values(1,2),(2,3)
on conflict(id) do update when existing.n < incoming.n
set n=existing.n+incoming.n
at most 2
return id,n
```

The conflict clause follows VALUES and precedes the mandatory MANY cap and
optional RETURN. The target must name exactly the table's full primary-key
column set, without duplicates; reordered target names are accepted. This
finite path has no secondary unique indexes or nullable primary keys. Legacy
`insert items set ...` text does not accept a conflict clause. Keywords `on`,
`conflict`, `do` and `nothing` remain contextual; no identifier is newly reserved.
Bare `existing` and `incoming` remain ordinary names. An unquoted prefix followed
by a dot selects conflict scope; a quoted prefix remains an ordinary field.
The field after the dot uses the normal canonical name parser, including
backticks for keyword fields.

`existing.field` and `incoming.field` bind only in DO UPDATE guards and SET
expressions. Bare row-field references there are rejected: spell the intended
source explicitly. Parameters, literals, block locals and existing scalar
expressions remain available. The incoming row is the admitted proposal,
including NULL for omitted nullable columns. The existing row is the winner's
preimage. Assignments evaluate against those same two rows and become visible
simultaneously; assigning a primary key or assigning a destination twice fails.
RETURN sees final target fields, not conflict scopes. A new-key insertion does
not evaluate the conflict guard or SET expressions, although every expression
still binds upfront.

Caps count every proposed tuple before conflict handling, including tuples that
will be skipped. Every incoming tuple must satisfy the stored-column checks and
64 KiB row limit. The full proposal shares the existing 1 MiB input-row budget.
Duplicate proposal identities abort with `DuplicateTarget`, even when every
input would otherwise DO NOTHING. All existing-winner guards must be TRUE
before any conflict SET expression evaluates; false or NULL yields
`ConditionFailed` and rolls back the entire operation. Every non-skipped final
row obeys the 64 KiB row limit. Inserted, updated and no-op final rows share one
combined 1 MiB budget, separate from the budget for all incoming proposals.
RETURN and retained local results are admitted before staging.
The legacy 1,024-row table ceiling counts actual new inserts, so a full table can still
skip or update existing rows. No new work, transport-decoding or total-heap
bound is implied.

DO NOTHING returns no row for a skipped input. Successful inserts and updates
return postimages in primary-key order; no-op updates return the winner with
its unchanged row version. Changed rows use their actual stored versions.
`matched` counts all inputs, `affected` counts non-skipped successes and
`changed` counts actual inserts or updates. `Outcome.conflict_results` is
`Some(Vec<ConflictTupleOutcome>)` only for ON CONFLICT operations. It has one
entry per input in input order: `input_index`, `status` (`success` or `skipped`
in JSON), `changed`, and `returned_row`, an optional index into the PK-ordered
RETURN rows. With no RETURN, row indices are absent; skips never have one.
An empty typed MANY request produces `Some([])` after normal binding and cap
checks. Non-conflict results omit the field from JSON, and deserializing older
JSON defaults it to None. Rust callers constructing Outcome literals must
account for the added optional field. Transaction step summaries expose the
same optional conflict metadata. A summary's `returned_row` indexes that
operation's RETURN rows, not the block's final emitted values.

A singular skipped block local has `status="skipped"` and `changed=0`.
Its `.row` and `.row.field` are absent: dereferencing them returns
`Bind("local row")` and aborts the block. Use status/changed output when a skip is
acceptable, or a preceding REQUIRE when the rest of the block requires a row:

```text
transaction {
  let created=(insert into items(id,n) values(1,2) on conflict(id) do nothing return id,n);
  require @created.status="success";
  emit @created.row
}
```

CASE and COALESCE cannot mask a missing local row because all local occurrences
are resolved and charged before scalar evaluation. No synthetic NULL row is
created. MANY locals retain aggregate `status="success"` and expose their
per-input results through transaction summary metadata; `.row` and `.rows`
remain unavailable for MANY writes.

The AST adds `Statement::OnConflict { statement, target, action }`,
`ConflictAction::{Nothing,Update{guard,values}}` and
`Expr::ConflictField { source: ConflictSource::{Existing,Incoming}, field }`.
Typed wrappers accept direct legacy/value-expression/MANY insert variants.
Only an outer RETURN may enclose ON CONFLICT; reversed, repeated and non-insert
wrappers reject before recursive traversal or serialization. All input/action/
RETURN expressions share the existing node, depth and resolved-value budgets.
The manifest adds `conflict_target_columns` (64) and `conflict_input_outcomes`
(1,024) in 0.10. Preparation freezes parameters and catalog identity without
choosing winners, deciding caps or evaluating guards. Execution observes fresh
rows, with the same exact version/profile fence and Conflict/Unknown rules.
Profiles 0.1 through 0.9 reject the new wrapper and scoped expressions, including
nested transaction expressions. SQL grammar remains unchanged. The 0.10 conflict
increment does not implement secondary uniqueness, once/receipts, authorization
or complete native v1 write semantics.

Versions 0.11 and 0.12 provide separate trusted Rust once APIs over the core's unchanged
Once V1 contract. A standalone write places `once` after its ordinary clauses
and cap, before `return`; a writing block places it in the transaction header:

```text
update items where id=$id set n=n+1 once $key for 1h return id,n
insert many into items(id,n) values(2,10),(3,20) at most 2 once $key for duration("24h") return id,n
transaction once $key for 24h {
  let changed=(update items where id=$id set n=n+1 return id,n);
  require @changed.changed=1;
  emit @changed.row
}
outcome $key
```

Use `execute_once_text(text, bindings, scope, clock)` or
`execute_once(envelope, bindings, scope, clock)` for a new attempt or explicit
retry. Use `outcome_text(text, bindings, scope)` or `outcome(query, bindings,
scope)` for lookup; lookup needs no clock. `parse_once_with_version` and
`parse_outcome_with_version` expose typed `OnceEnvelope` and `OutcomeQuery`, and
`parse_input_with_version` returns `ParsedInput::Once` or `ParsedInput::Outcome`.
These trees carry their exact selected language version: a 0.11 tree cannot
execute through a 0.12 engine or vice versa. Profiles 0.1 through 0.10 reject
them. Ordinary execute/prepare methods do not execute
envelopes, and there is no prepared once handle or automatic retry.

The caller must persist the original `OnceScope { database_domain, tenant,
namespace }`, selected language version, request, key and bindings before the
first attempt. Never replace the original domain with a reopened/restored
database's current domain. Scope labels partition identity; they grant no tenant,
user or task authority. Keys are nonempty Text/Bytes values of at most 256 bytes;
Text uses its exact UTF-8 bytes, so an equal Bytes key denotes the same identity.
Namespaces are nonempty and at most 63 UTF-8 bytes. There is no key normalization.

The clock is supplied by the trusted caller and certifies a real Unix-time
interval for its entire remaining monotonic lease, at most 30 seconds. A plain
`SystemTime` sample does not provide that certification. The provider is called
after evaluation under the publication gate, must be bounded and non-reentrant,
and must not call this database, including `Admin::certify_server_time`. For
example, this helper accepts an already persisted scope and a caller's certified
time provider; it supplies no clock or identity defaults:

```rust,no_run
use giggles_language::{Bindings, Engine, OnceScope, Value};
use giggles_language::once::{
    OnceClockUnavailable, OnceReceipt, OnceResult, OnceTime,
};

fn submit_original(
    engine: &Engine, // Explicitly constructed with LanguageVersion::Embedded011.
    persisted_scope: &OnceScope,
    original_key: &[u8],
    certify_current_interval: &dyn Fn() -> Result<OnceTime, OnceClockUnavailable>,
) -> OnceResult<OnceReceipt> {
    // The provider must certify lower/upper Unix seconds through valid_until.
    // It is invoked by the engine at publication; no SystemTime fallback exists.
    let clock = || certify_current_interval();
    let bindings = Bindings::from([
        ("id".into(), Value::Int(1)),
        ("key".into(), Value::Bytes(original_key.to_vec())),
    ]);
    engine.execute_once_text(
        "update items where id=$id set n=n+1 once $key for 1h return id,n",
        &bindings,
        persisted_scope,
        &clock,
    )
}
```

Retention accepts an unsigned integer quantity followed by case-sensitive `us`,
`ms`, `s`, `m`, `h` or `d`, or the same spelling inside `duration("...")`.
The typed header uses `DurationLiteral { microseconds: i64 }`. The duration must
represent an exact positive whole number of seconds, from 1 through 604,800;
for example `1000ms` works and `1001ms` fails. There is no rounding, duration
parameter or general duration column/scalar type. Reads, read-only blocks,
catalog operations, nested/per-item once and receipts inside expressions remain
unsupported.

The entire native envelope retains the 64 KiB input bound. All supplied bindings
share the 256-name/64 KiB payload admission before header-only parameters are
separated from the body; missing and extra names are checked across their union.
A key parameter also referenced in the body remains a body parameter. Header
admission does not reduce the body's existing 64 KiB explicit-value/runtime-local
allowance. Core canonical intent is additionally capped at 256 KiB, each encoded
receipt at 2 MiB, and the durable ledger at 1,024 identities and 8 MiB within the
existing 16 MiB database envelope. These keyed-output limits are additional
admission bounds, including result metadata and structural overhead, and can
reject an otherwise valid unkeyed request before publication.

A returned receipt contains the accepted fingerprint, minimum retry deadline,
replay flag, original outcome and `OnceDescription` with the accepting catalog
epoch and typed operation columns or transaction emit descriptions. The five
terminal business failures `NotFound`, `AmbiguousMatch`, `ConditionFailed`,
`TooManyMatches` and `RequirementFailed` are retained in the receipt's outcome
with zero business effects. Success, skipped and no-op results are also retained.
Admission errors, transient conflicts and other execution errors do not consume
the identity. Matching retained submissions replay before live catalog binding;
changed intent under the same identity conflicts. Receipt metadata remains from
the accepting snapshot even after the live schema changes.

Lookup reports `Absent`, `InProgress`, `Retained` or `Expired` without changing
durable generation. Replay and lookup need no clock. The retry deadline is the
certified upper Unix bound at the publication decision plus the requested
retention; retained payloads remain usable beyond that minimum until explicit
core administrative compaction. Compaction requires a fresh certified lower
bound strictly beyond the deadline and leaves permanent tombstones: keys are
never reused. Restore retains old-domain records as inaccessible forensic data
that still consumes capacity. Compaction remains a separate core administrative
API; this native adapter does not expose it.

For `OnceError::Engine(core_once::OnceError::Unknown)`, reopen/recover and resolve
the original persisted identity with outcome lookup or an explicit identical
submission. No provisional receipt is returned on uncertain publication.
`InProgress` also triggers no automatic retry. The native API and its Rust/serde
layouts are not a stable network protocol; protocol adapters must define their
own versioned wire DTOs and authorization. The CLI has neither original scope
nor a certified clock and returns `Unsupported` for valid once/outcome input
before database open, including under 0.11 and 0.12. It fabricates neither context.

Version 0.12 adds four standalone inspection commands through separate
`Engine::inspect_text(text)` and `Engine::inspect(&InspectionStatement)` APIs.
They accept no bindings and cannot appear in transaction blocks, once envelopes,
data projections or prepared operations. `parse_inspection_with_version` returns
the typed statement, and general parsing returns `ParsedInput::Inspection`.
Profiles 0.1 through 0.11 reject the new command forms. `show` and `describe`
remain ordinary table names when the inspection command prefix is absent.

```powershell
# These describe the selected native library profile; the path is not opened.
& $cli run --language-version 0.12 missing.db 'show capabilities'
& $cli run --language-version 0.12 missing.db 'show limits'
# These inspect an existing database's committed catalog.
& $cli run --language-version 0.12 example.db 'show tables'
& $cli run --language-version 0.12 example.db 'describe table work'
```

`show capabilities` returns `InspectionOutcome::Capabilities` containing the
selected version's full `CapabilityManifest`, and `show limits` returns
`InspectionOutcome::Limits` containing its `CapabilityLimits`. These are local
library facts, not observations of a database, live server or caller's grants.
The CLI returns them before opening even a missing or locked path. Nonempty
inspection bindings fail before database open for all four commands. The
existing CLI `capabilities` command retains its compact profile output;
`capability-manifest` retains its direct manifest output without the inspection
wrapper. Unversioned commands still select 0.1.

`show tables` returns `InspectionOutcome::Catalog` with a complete list of
`TableSummary { id, name, column_count, primary_key_columns }`; both count fields
count columns, and neither is a row count. `describe table name` returns the exact
`TableDef`, including stable column IDs, declared types, nullability and
primary-key indexes. SQL-created `SmallInt` columns remain `SmallInt`. Names use
the existing canonical lowercase ASCII identifier rules; 0.12 also accepts
canonical backtick names. Qualified names and table-name parameters are not
part of this slice. A missing table returns `Bind("table")`.

Every catalog result contains `contract_version`, `database_domain`,
`catalog_epoch`, `complete` and `result` (`Tables` or `Table`). The Rust domain is
`u128`; its serde/CLI representation is a canonical decimal **string** so the full
128-bit value survives JSON conversion. Catalog inspection reads one fresh
committed view without executing SQL, scanning business rows, publishing a
generation or copying the whole database state. Uncommitted DDL in another
session stays invisible. Result descriptors are immutable copies of that view;
they confer no execution authority and are not a continuing snapshot handle.

The manifest advertises only `local_discovery` and `catalog_inspection` for this
increment. `inspection_metadata_bytes` is 65,536, the core's logical catalog
metadata accounting limit checked before cloning output. It is **not** a JSON,
protobuf or total heap size bound. Table/column catalog limits remain in force.
No data rows, row counts, indexes, schemas, dependencies, EXPLAIN or policy
inspection are added. This is trusted embedded inspection under `Admin`; it
does not qualify the reference's full Local core bundle, authenticated service
discovery or task authorization, and changes no server/protocol API.

Singular writes count the entire WHERE set before testing WHEN: zero gives
`NotFound`, two or more `AmbiguousMatch`. Bulk caps are mandatory and atomic:
cap+1 gives `TooManyMatches`, zero effects. A claim chooses only the first ordered
candidate and requires its postimage to leave eligibility, or returns
`ClaimNotConsumed`. Empty claim succeeds with zero counts. No lease or worker
finalization protocol is provided. Primary key updates are rejected. Omitted
insert columns are NULL, subject to required-column checks; no defaults exist.

Each statement or block uses one explicit core transaction. Statements and writing
blocks stage their effects and commit once; read-only blocks release their snapshot
without a durable commit. On legacy, this native profile supports tables with at
most 1,024 rows; on V2, reads and writes (point and many, insert, upsert,
`insert into ... from`) accept tables of any size and caps beyond 1,024, and
once writes past 1,024 rows record a GDOU version 2 receipt (u32 counts; the
first one durably sets the V2 `wide_once_receipts` feature bit, so older
binaries refuse the database). Legacy once receipts stay version 1 and bounded
at 1,024 rows; returned rows stay bounded by the 2 MiB receipt. Core itself can store larger tables: a reverse primary-key probe verifies
that the forward scan was complete, and an oversized table returns `Limit`
before effects, even for a small read or point mutation. A nonempty native insert
rejects a table already at 1,024 rows. Full scans can additionally hit the 1 MiB result
limit and then fail with zero effects. Core also limits rows to 64 KiB, tables to 128, columns to 64 and
concurrent explicit sessions to 8. Native input is at most 64 KiB, 4,096 tokens
and expression depth 32. Scans and sorting are in memory inside the transaction;
no performance qualification is claimed. Whole-generation core certification
can conflict on unrelated writes. There is no automatic retry.

Mutation counts distinguish `matched`, `affected` and `changed`: a successful
no-op update affects one row and changes zero; it stages no mutation and preserves
the row version. Insert/delete change each affected row. A successful claim returns
the selected full postimage and its actual core version, so a worker can identify
its claimed work. Without an explicit return clause, empty claim returns table
columns and no rows, and other mutations return counts only. Reads affect/change
zero. Counts are disclosed because this is a
trusted profile. CLI success prints JSON; errors print tagged JSON on stderr and
exit nonzero. `Conflict` rejects staged data changes; a commit conflict may still
advance the core's trusted decision/generation metadata. `Unknown` means a
storage error occurred during COMMIT and effects may have persisted. **Do not
blindly retry Unknown.** Unkeyed operations require application-state
reconciliation; explicitly keyed 0.11/0.12 Rust calls use the original identity
resolution described above. Ordinary APIs and profiles 0.1 through 0.10 do not
provide receipts. The 0.11/0.12 adapter delegates its durable decisions to the trusted
core [`operations::once` API](../giggles-core/OPERATIONS.md).

The larger native tour/writes/permissions contracts remain design targets.
This crate rejects per-item once, recovery/nested blocks, schemas/jobs, decimal/UUID/temporal/JSON
document types, joins, aggregation, other scalar functions, task policies and service/distributed
operations. This profile does not qualify the reference's complete “Local core”
bundle. No server integration, PostgreSQL comparator, security, cloud, production
or performance claim is made. Tests retain unique temporary databases for
inspection; no fixture cleanup occurs.

Run the coordinated core and native verification from PowerShell with
`./crates/giggles-language/verify.ps1 -ArtifactDirectory <new-directory>`.
The script runs formatting, Clippy, debug/release tests and rustdoc tests with
one build job, records output and tool versions, and compares input hashes
before and after the run. Choose a fresh artifact directory; the script keeps
its evidence and test databases for review.


## Explicit trusted native 0.13 static explain

Select `LanguageVersion::Embedded013` (CLI `--language-version 0.13`) to use
`explain SIMPLE_READ`. Defaults and profiles 0.1 through 0.12 retain their behavior,
including table/column identifiers such as `explain` and `analyze`.
`parse_explain_with_version` produces a separate `ExplainStatement { read }`;
`Engine::explain_text(text, &bindings)` and `Engine::explain(&tree, &bindings)`
return core's typed `ReadExplainOutcome`. Only `Statement::Read` with field
projections is accepted. Computed projections, writes, blocks, once, analyze,
inspection and nested explain are rejected; explain is not a block item or a
prepared credential. Each call validates actual supplied parameters against a
fresh committed catalog, including missing/extra/type errors.

```text
giggles-language run --language-version 0.13 PATH 'explain items where id=$id order by id desc limit $page select id' '{"id":{"Int":1},"page":{"Int":10}}'
```

CLI JSON directly contains contract version, canonical decimal-string database
domain, catalog epoch, table ID/name, projection column IDs, order column IDs and
directions, filter presence, typed stages and limits. Estimates are unknown
(`null`). Malformed explain syntax and unsupported trees reject before CLI
database open. JSON output has an exact 65,536-byte preflight before output copies;
this is separate from core's logical metadata budget. No human renderer is added.

Legacy explain performs a whole-state physical storage read and validation;
v2 explain inspects a rows-free catalog snapshot.
It performs no logical business-row read, mutation or commit. The described read
path on legacy materializes within a 1024-row ceiling and certifies completeness
with a reverse-primary-key probe when full. V2 streams complete table input
without that row ceiling; explicit read limits may exceed 1024. Both retain the
default output page of 100 and filter/order/project in memory. V2 explain uses
`input_rows: usize::MAX` to denote no fixed row ceiling, and its first stage
names the access path: `PrimaryKeyLookup`, `StreamingFilter` (only matching
rows retained) or `StreamingCompleteTableMaterialization`. A transaction block
that contains an INSERT has the INSERT text bound. Mutation and once-receipt limits are unchanged. It does not imply indexed
optimization, task authorization or an access credential. The manifest separately
advertises `bounded_simple_read_static_explain`; full reference explain,
`local_core_bundle` and `index_creation` remain unqualified.
