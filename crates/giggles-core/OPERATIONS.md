# Typed operation engine

`giggles_core::operations` provides the trusted embedded operation boundary for
frontends. `Engine::execute(&Request, &Bindings)` binds a typed request against
one transaction snapshot, evaluates its intent, stages mutations, and commits
once. Requests contain values and expressions; no SQL strings are generated.

The native frontend in `giggles-language` parses text or accepts its existing
public AST, then structurally lowers it to these requests. The core owns binding,
expression evaluation, ordering, cardinality, guards, caps, claims and mutation
execution. Native grammar and reserved words remain in the frontend.

SQL remains supported through `SqlSession`. Its parser produces prepared
relational statements. Typed operation requests construct those same internal
statements directly, using the same catalog, row validation, version checks,
admission, cancellation, transaction staging and durable publication path.
The current extraction does not move every SQL query form into the public
`operations::Request` enum: SQL joins, aggregation and DDL retain their existing
typed relational implementation. Native operations need no equivalent SQL syntax.

| Contract | Typed engine request | Current behavior |
| --- | --- | --- |
| Read | `Request::Read` | Filter, stable ordering, projection, bounded page and completeness |
| Computed read | `Request::ReadExpressions` | Named scalar projections, typed output metadata and full eligible-output admission |
| Insert | `Request::Insert` | Typed values, required columns and constraints |
| Computed insert | `Request::InsertExpressions` | Scalar values without a source-row scope, checked before staging |
| Capped tuple insertion | `Request::InsertMany` | Full input cap, canonical duplicate checks and complete preflight before staging |
| Atomic block | `Transaction` | Upfront binding, immutable locals, read-your-writes, optional final emit and one decision |
| Computed update | `Request::UpdateExpressions` | Simultaneous preimage expressions with complete postimage checks before staging |
| Returned write output | `Request::Returning` | Complete projected preimages/postimages admitted before staging, with actual row versions |
| Singular mutation | `Intent::One` or uncapped delete | Count the full predicate set before evaluating the guard |
| Capped mutation | `Intent::Many` or capped delete | Cap exceeded returns `TooManyMatches` with zero effects |
| Claim | `Intent::Claim` | Choose the first ordered match; its postimage must leave eligibility |

For example, `Many { cap: Literal(Int(2)) }` against three matching rows rejects
the entire operation. It does not apply a two-row page. A singular update with
two matches returns `AmbiguousMatch` even if its guard would accept only one.

Mutation policy remains explicit. Native-intent no-op updates affect the match
count while preserving row versions. Ordinary SQL updates retain their existing
matched-update behavior, including advancing the version for equal values.
The typed engine returns the actual stored postimage/version for a claim.

Every request uses the same staged catalog and rows. Full-primary-key mutations
use expected row versions, and commit certifies the shared database generation.
Concurrent SQL, native or repair writes can therefore reject a stale commit.
Errors discard staged changes. A commit certification `Conflict` preserves the
existing SQL kernel policy: publication may advance trusted decision/generation
metadata even though none of this request's staged rows, row versions, catalog
or allocator changes are published. Conflict therefore does not promise a
byte-identical canonical export. Commit storage errors return `Unknown`; this
profile has no receipt or outcome lookup to resolve an ambiguous commit.

Cardinality and cap evaluation require an internal `CompleteRelation`, which
ordinary scan pages cannot construct. The storage layer creates it only after
proving complete input in an active transaction. Consumption checks the database
domain, catalog epoch, table descriptor, exact transaction and staged revision.
A successful staged write invalidates earlier materializations, including a
delete or no-op SQL update; rollback and transaction reuse cannot revive them.
This revision is in memory only and does not change the database format.

`Engine::prepare_bound(&Request, &Bindings)` freezes real argument values into
an opaque process-local `BoundOperation`. Its description records the engine
contract version, database domain, catalog epoch, stable table/column IDs and
typed output columns. Preparation binds against a temporary snapshot and rolls
it back without a logical row scan or advancing durable state. Beginning the
snapshot still clones bounded database state. Preparation does not decide
cardinality, guards or caps at preparation time.

`Engine::execute_bound(&BoundOperation)` revalidates the catalog and domain in
a fresh transaction, then evaluates the frozen intent against current rows.
Schema changes and a different/restored database reject before effects. The
handle is not serialized authority, a receipt, or a template accepting new
parameters. It still requires a trusted `Engine`, and a repeated execution is
a new operation with the same values. There is no automatic retry after Unknown.
Ordinary `execute` continues to bind and run in one transaction.

Both direct execution and preparation admit at most 256 supplied bindings and
65,536 bytes of supplied value payload before opening a transaction. Binding
also limits the combined payload of explicit literal and parameter occurrences
to 65,536 bytes before copying values. Repeated references count repeatedly;
explicit limit/cap arguments count too. Text and bytes use their byte length,
and scalar/NULL values count as nine bytes, matching existing SQL payload
accounting. Generated defaults and structural metadata have their own finite
node/column limits. These are value-payload budgets, not total heap or transport
decoding guarantees; a caller owns its input map before admission.

This tightens earlier native behavior: oversized binding maps, payloads and
expanded references return `Limit` before catalog lookup or value cloning.
Under-budget binding diagnostics and ordinary SQL behavior are preserved.

Scalar expressions support checked signed-64-bit integer negation, addition,
subtraction, multiplication, division and remainder through `Expr::Negate` and
`Expr::Arithmetic`. Integer column operands, including SQL SMALLINT and INTEGER,
evaluate in this signed-64-bit expression domain. Stored column width checks
remain unchanged. Division truncates toward zero; remainder has the dividend's
sign. `i64::MIN % -1` is zero, while `i64::MIN / -1` and negating `i64::MIN`
return `Overflow`. Dividing or taking a remainder by zero returns
`DivisionByZero`. Any other unrepresentable integer result returns `Overflow`.

All operands bind before execution, including on empty tables. Arithmetic and
boolean evaluation is fallible and eager: both arithmetic operands are
evaluated before NULL propagation, and AND/OR do not hide errors in a boolean
branch. There is no
constant folding during preparation. Filters evaluate against the complete
bounded input before output limits, so a small or zero page limit cannot hide
an error on another row. Guards still follow cardinality/cap checks, and claims
evaluate eligibility again on the proposed postimage before staging a write.
Any evaluation error discards the request's staged changes.

The original `Request::Update` keeps literal/parameter assignments and its eager
column range/nullability checks during binding. `Request::Read` keeps its field
projection shape. Their serialized forms and existing admission policies remain
unchanged. Limits and caps still take integer value arguments. Decimal, floating
and temporal arithmetic are not part of this slice. SQL SUM shares the engine's
checked integer-add helper while
retaining its existing SQL grammar, NULL behavior and overflow diagnostic.
The helper introduces no stored value variants or database-format changes.

`Expr::Call { function, arguments }` supports the fixed `ScalarFunction` values
`Coalesce`, `Abs` and `Length`. `MAX_FUNCTION_ARGUMENTS` is 64; a larger argument
vector returns `Limit` before traversal or serialization. COALESCE requires at
least two arguments; ABS and LENGTH require exactly one. Other arities and
incompatible argument families return `Type`. Every explicit argument counts
toward the shared expression depth, node and resolved-value limits.

COALESCE binds all arguments to one compatible scalar family, allowing untyped
NULLs alongside that family. It evaluates arguments left to right and returns
the first non-NULL value without evaluating later scalar expressions. If all
values are NULL, its result is NULL. Only arguments with uniformly unknown NULL
type, such as `coalesce(null,null)`, retain an untyped result:
projected output requires a known type, while scalar emit can describe it with
`data_type: None`. A typed enclosing expression or nullable assignment can use
that result. Integer families share the existing signed-64-bit expression
domain; there are no implicit conversions between unrelated families.

ABS accepts an integer or NULL and returns its checked signed-64-bit absolute
value; `ABS(i64::MIN)` returns `Overflow`. LENGTH accepts text, bytes or NULL.
It counts Unicode code points in text, without normalization or grapheme
clustering, and raw bytes in byte values. Both functions propagate NULL and
have BigInt result type even for an untyped NULL argument. Computed call output
uses synthetic column ID zero and conservative nullable metadata; integer
results use BigInt even when their source was SMALLINT or INTEGER.

Only COALESCE's scalar computation is lazy. Every argument still binds and
admits literal/parameter occurrences, including an argument that will never be
evaluated. In transaction blocks, every local occurrence in every argument is
also admitted and frozen before the step executes. These rules preserve the
shared 64 KiB frozen/runtime value budget. Function evaluation checks
cancellation. The selected COALESCE text/bytes value remains borrowed until
the existing output or postimage budget admits a copy; ABS and LENGTH produce
only scalar results. Existing eager arithmetic and boolean behavior, complete
input evaluation before output paging, and atomic rollback rules remain intact.
These calls add no SQL grammar, stored types or other scalar functions.

`Expr::Case { operand, arms, else_expr }` supports searched and simple CASE.
Each arm is a `(when, then)` expression pair. `MAX_CASE_ARMS` is 64; larger
vectors return `Limit` before traversal or serialization, and an empty arm
vector returns `Type`. Every explicit operand, WHEN, THEN and ELSE expression
shares the existing depth, node and value budgets. An omitted ELSE adds no
synthetic input node or value charge.

Without an operand, every WHEN must bind as Boolean or untyped NULL. With an
operand, its type and every WHEN type must unify into one compatible scalar
family, even when the operand is untyped NULL. THEN and explicit ELSE types
unify separately into the result family. All branches bind, including ones
whose conditions or results will never execute; unknown references, missing
parameters and incompatible types fail before effects. No branch is folded
during preparation. Stored integer width and required-column constraints still
apply to the selected result when constructing a proposed row.

Searched CASE evaluates WHENs in order and chooses the first true condition;
false and NULL do not select an arm. Simple CASE evaluates its operand once,
then successive WHEN expressions using ordinary equality. NULL never matches
NULL, and a NULL operand still evaluates successive WHEN expressions, which
can fail. Both forms stop at the first match, evaluate only its THEN expression,
and skip later conditions and results. If no arm matches, evaluate ELSE or
return NULL when it is omitted. Errors in an evaluated condition or selected
result abort the operation; errors confined to unevaluated scalar expressions
are skipped. Existing AND/OR evaluation remains eager within an evaluated
expression.

An implicit or explicit NULL result retains the statically inferred result
family. CASE with only unknown NULL result types remains untyped: computed
projection rejects it, scalar emit describes it with `data_type: None`, and
typed enclosing expressions or nullable assignments can use it. Computed CASE
outputs use conservative nullable metadata, synthetic column ID zero and
BigInt for integer families, including branches sourced from SMALLINT fields.

Binding charges every explicit literal/parameter occurrence, and transaction
steps resolve and charge every local occurrence across all CASE branches before
execution. These admissions stay eager despite lazy condition/result
computation. CASE checks cancellation and returns the selected value without
copying borrowed text/bytes. Existing result, mutation and retained-local byte
checks still precede payload copies and publication. CASE adds no persisted
types or SQL syntax and does not complete the broader relational-service profile.

`Request::UpdateExpressions` takes `(column, Expr)` assignments with the existing
`One`, `Many` or `Claim` intent. Binding resolves every reference and scalar type,
rejects duplicate targets and primary-key assignments, and freezes argument
values. Execution identifies the complete target set, checks cardinality or cap,
then checks every guard before evaluating assignments. Each right-hand side sees
the original row: `a=b, b=a` swaps the fields. Target integer width, nullability
and final stored-row payload are checked against the simultaneous postimage.
These row checks happen at execution, including for literal expressions, while
incompatible scalar families still fail binding. No constant folding or row
evaluation occurs during preparation. Many and capped Delete guards are supported:
the complete target cap is checked first, then every guard must pass before any
RHS or RETURN evaluation. Claim with an explicit guard remains unsupported.

Every computed postimage must fit the stored-row 64 KiB payload limit. The sum
of full selected postimages must also fit `MAX_MUTATION_BYTES` (1 MiB), including
unchanged fields and no-op rows. Payload accounting uses text/bytes length and
nine bytes for each scalar or NULL. This additional mutation budget applies to
`UpdateExpressions`; it does not change the legacy value-only update contract.
All computed rows pass evaluation, constraints, budgets and claim consumption
checks before any write is staged. No-op updates preserve versions, and claims
return their actual stored postimage/version. Cancellation is polled during row
evaluation and before staging. Precommit failures discard staged data changes;
commit uncertainty and certification metadata follow the policy above.

`Request::ReadExpressions` requires 1–64 `Projection { expression, alias }`
items. A plain field may omit its alias; every other expression needs an explicit
alias, and all resulting names must be unique. Repeated source fields are allowed
with distinct aliases. Aliases name output only; filters and ordering still bind
to source fields. There are no expression sort keys, nested objects or mixed star
projections in this surface. An untyped NULL projection fails binding; a typed
nullable expression such as integer arithmetic with NULL has a known output type.

Preparation describes field projections with their original stable column ID,
declared type and nullability, changing only the output name for an alias.
Computed outputs use synthetic column ID zero: integer values/arithmetic are
BigInt, comparisons/Boolean expressions are Boolean, and text/bytes values retain
their scalar type. Nullability is conservative; `IS NULL` is always non-null.
The output row keeps the source row version.

Every eligible projected row is evaluated and charged before output paging,
including rows beyond a small or zero LIMIT. This finite profile therefore does
not hide projection errors or byte exhaustion beyond the returned page. Combined
projected payload must fit the existing 1 MiB result limit. Output rows may exceed
64 KiB by repeating source fields, provided the combined result fits; the 64 KiB
row limit governs stored/mutated rows. Expression leaves borrow field and frozen
argument values through evaluation and byte admission. Large text/bytes values
are copied only after the applicable payload budget admits them; mutation work
retains per-row assignments without duplicate full postimage copies.

`Request::Returning { request, projection }` wraps one Insert, InsertExpressions,
InsertMany, Update, UpdateExpressions or Delete request. It can also wrap one
OnConflict over a direct insert, as described below. Every other wrapper nesting
and read body returns `Unsupported` before recursive traversal, serialization
or transaction admission.
The inner write and return expressions share one binder and the existing input,
expression-node and explicit-value budgets. At most 64 projection items are
accepted before building traversal work. An empty projection is the wrapper-only
all-column form (`return *`); it does not change ReadExpressions' nonempty rule.
Explicit return items use the same alias, unique-name and output-type rules as
computed reads. Preparation freezes return-only parameters and describes the
requested output without executing the write or evaluating any row.

Delete returns selected preimages. Insert, update and claim return postimages;
unchanged updates still return their matched rows with unchanged versions.
Bulk output includes all selected rows in full-primary-key order, without an
implicit 100-row page. Empty bulk or claim results retain their output columns
and report complete output. A wrapped claim returns its requested projection;
an unwrapped claim keeps its existing full-row result. Matched, affected and
changed counts, cardinality/cap/guard ordering and claim consumption are unchanged.

All requested return expressions are evaluated once, and their combined raw
payload must fit the 1 MiB result budget before any large result value is copied
or any write is staged. Proposed stored rows first pass type, nullability and
64 KiB row checks, including wrapped inserts and legacy value-only updates.
Unwrapped legacy writes retain their existing validation behavior.
No-op rows count and can fail output evaluation or byte
admission. As with computed reads, there is no additional 64 KiB output-row cap.
Return output and computed-mutation postimages have separate budgets; wrapping a
legacy value-only update does not introduce the computed-mutation budget.

The admitted output is cached while the writes are staged. Changed insert/update
row versions are then obtained from the same active snapshot through a checked
private version lookup, without re-evaluating return expressions or cloning the
database state. Delete and no-op outputs retain their preimage versions. These
cached values match the current engine's explicit writes; this surface adds no
triggers, generated values or other postimage transformations. No result escapes
until commit succeeds. A precommit failure rolls back all effects; a commit
storage error still returns `Unknown`, without a speculative returned result.

`Request::InsertExpressions` supplies one tuple of scalar expressions. Assignment
names bind to target columns, but the RHS has no source-row scope: bare fields
fail binding. Literals, parameters, scalar operators and transaction locals are
allowed. Duplicate columns, incompatible scalar families and omitted required
columns fail binding. Supplied expressions evaluate at execution without constant
folding; target width, nullability and the complete 64 KiB stored-row payload are
checked before staging. The original `Insert` value-only behavior is unchanged.

`Request::InsertMany { table, columns, rows, cap }` inserts explicit scalar
expression tuples under a mandatory cap. `columns` is a nonempty unique list
of at most 64 target names; every tuple must have matching arity. There are at
most `MAX_INSERT_ROWS` (1,024) tuples on legacy; V2 has no tuple-count ceiling
beyond the input, expression and byte budgets. Structural vector/arity bounds and the
combined tuple/RETURN root count are checked before constructing traversal
work. All expression nodes, frozen values, cap and RETURN share the ordinary
request or enclosing block budgets. RHS expressions have no source-row field
scope; block locals are allowed only inside a transaction.

Preparation binds every tuple and output expression but does not decide the
input cap or evaluate row expressions. Execution checks the supplied tuple
count before local value materialization, source-table scanning, RHS evaluation
or duplicate work. Cap overflow returns `TooManyMatches`, even when a later
runtime expression would fail. Static binding failures still reject upfront.
Every proposed row then passes expression evaluation, type/nullability and the
64 KiB stored-row bound. All full proposed rows share `MAX_INSERT_BYTES` (1 MiB),
including omitted nullable fields; the existing input/value budgets can impose
a tighter effective bound in this explicit-tuple slice.

The canonical complete-primary-key codec defines proposal identity. Repeated
input identities return `DuplicateTarget`, including identical tuples. This
check follows all proposed-row validation and precedes existing-key checks.
An existing key retains the ordinary `Core("Sql(Duplicate)")` diagnostic.
After key checks, a legacy table must contain at most 1,024 rows (V2: no row
ceiling). There is
no replacement, source query or partial acceptance. Conflict handling requires
the explicit OnConflict wrapper; the plain insert variants keep duplicate errors.
All proposed rows and requested output are validated before any insertion is
staged. Ordinary `Insert`, `InsertExpressions` and SQL insertion keep their
existing behavior.

RETURN on InsertMany includes every inserted row in full-primary-key order,
with actual staged versions and complete output; there is no default page.
All return expressions, the 1 MiB output budget and any retained-local budget
pass before output payload copies or staging. Proposed full-row values remain
borrowed until admission and are moved/copied once into each engine insert.
Cancellation and any late engine failure discard the enclosing attempt.

Typed trees may supply zero tuples. They still bind the table, unique column
names, cap and RETURN metadata, but no nonexistent-row required-field checks
run. Counts are zero and requested output is empty with its column metadata.
The native VALUES spelling requires at least one tuple. In blocks, InsertMany
is always a nonsingular write, including zero/one-row inputs: local `.changed`
and `.status` are available, while singular `.row` access and read `.rows`
access reject. Input caps are checked only when the step executes, after the
whole block has bound successfully. No cumulative task mutation allowance is
introduced by this trusted operation.

`Request::OnConflict { request, target, action }` wraps a direct Insert,
InsertExpressions or InsertMany. Its only permitted outer wrapper is Returning.
A bounded normalizer checks this shape, target length and action assignment
length before any recursive walk, serialization or transaction admission.
`target` must name every column of the table's ready primary key exactly once;
order may differ, and matching uses canonical catalog PK order. Empty, duplicate
or nonexact targets fail binding. There are no secondary/partial conflict keys,
implicit targets or replacement semantics in this slice.

`ConflictAction::Nothing` skips an existing target. `ConflictAction::Update`
contains an optional guard and `(column, Expr)` assignments. Its expressions use
`Expr::ConflictField { source: ConflictSource::Existing | Incoming, field }`.
These two scopes refer to the winner's preimage and the full proposed incoming
row. Bare fields are rejected in the conflict action, and scoped conflict fields
are rejected outside it, including VALUES and RETURN. Assignment targets remain
ordinary column names. Duplicate assignments and PK changes reject. Every RHS
sees the same two immutable rows; assignments take effect simultaneously. RETURN
uses ordinary fields against the final target row. All input, action and output
expressions bind upfront and share the existing explicit-value/node/input limits.

Execution checks the full input count before resolving block locals or scanning,
even if every input will be skipped. It evaluates and validates every proposed
incoming row, including skipped inputs, under the 64 KiB row and aggregate
1 MiB `MAX_INSERT_BYTES` bounds. Canonical duplicate input PKs return
`DuplicateTarget` before existing-row conflict handling. The complete snapshot
then determines inserts, updates and skips. Capacity counts only new keys: a
1,024-row legacy table can still update or skip an existing key; V2 has no
table capacity. Every conflicting
winner must pass its guard before any conflict SET RHS evaluates; a false or
NULL guard returns `ConditionFailed` and aborts the entire statement.

Final inserted, updated and unchanged rows each pass their stored-row constraints
and share one `MAX_MUTATION_BYTES` (1 MiB) full-postimage budget. Skipped existing
rows do not consume that final-row budget. This budget is independent of the
incoming proposal budget, which includes skipped tuples. Existing/incoming
payloads remain borrowed through guard, assignment, postimage and output
admission. The executor retains assignment overlays instead of copied complete
postimages; no synthetic combined row or catalog descriptor supplies the scopes.
No writes stage until all final rows, requested RETURN values and retained-local
output pass. Unchanged updates preserve versions. Changed writes use the same
typed engine insert/update paths and actual staged row-version lookup.

Outcome and StepSummary append `conflict_results: Option<Vec<ConflictTupleOutcome>>`.
Legacy operations use None and omit the field from JSON. An upsert uses Some,
including Some(empty) for zero typed inputs. Entries stay in input order and
carry `input_index`, `status`, `changed` and `returned_row`. ConflictStatus is
Success or Skipped, serialized as `success` or `skipped`. Successful inserts,
updates and no-ops have Success; DO NOTHING conflicts have Skipped and changed
false. Counts are matched=input count, affected=non-skipped count and changed=
actual inserts/updates. RETURN omits skipped inputs and does not evaluate their
output expressions. Successful rows have canonical full-PK order with no page
cut. `returned_row` indexes this operation's RETURN rows, or is None if skipped
or RETURN was absent; it never indexes a block's final emitted values.

A singular upsert local reports status `skipped` on DO NOTHING conflict and
`success` otherwise. MANY retains aggregate status `success`; its per-input
metadata reports individual skips. Missing singular `.row` or `.row.field`
access remains `Bind("local row")`, not a fabricated NULL row. Local resolution
is eager, so CASE/COALESCE cannot hide a missing row reference. A preceding
`require @result.status="success"` can abort before a later step or emit uses
that row. No new optional-row DTO or relaxed local admission is introduced.
Output/metadata escapes only after commit; failure and Unknown disclose no
provisional tuple results. This slice adds no persisted formats, SQL grammar,
authentication, server protocol, outcome receipts or task authorization.

Atomic blocks use a separate `Transaction { read_only, steps, emit }` API.
`Engine::execute_transaction` binds and executes one block;
`prepare_transaction_bound` freezes a process-local `BoundTransaction` without
a logical row scan or durable publication; beginning its snapshot still clones
bounded state. `execute_transaction_bound`
executes that intent against a fresh snapshot. Its description records every
referenced table, domain, catalog epoch, read-only mode and typed emitted shape.
Execution checks all exact descriptors and fences before scanning or effects.
An emit-only or require-only block may have no table dependencies.

A step is `Execute(Request)`, `Let { name, request }` or `Require(Expr)`. All
steps and final output bind before any step executes, using one catalog snapshot
and one parameter set. Locals have immutable lexical slots: duplicate, forward,
self and unknown references fail binding. There are no fake parameter values,
sentinel rows or runtime name/type rebinding. `Expr::Local` is available only
inside blocks; caps and limits retain literal/parameter `ValueExpr` arguments.
Before each step scans rows, typed local references become admitted frozen
values once per syntactic occurrence. The ordinary scalar evaluator then runs
unchanged, without multiplying local admission by the number of candidate rows.

A singular write with RETURN exposes `.row.field` and can be emitted as a row.
Every write exposes `.changed` and `.status` (the successful status is `success`).
Read locals expose `.count` and can be emitted as `.rows`. Count is the number
of rows in the materialized page, never proof of total cardinality; emitted read
rows preserve the page's `complete` flag. Local values stay fixed when later
steps change the underlying rows. A new request reads the current staged state
and obtains its own complete-relation certificate. Claim in a block is rejected.
Writes in a read-only block are rejected before execution.

`Require` accepts only true; false or NULL returns `RequirementFailed`. Final
emit is optional and is represented separately from steps, so it cannot occur
mid-block or more than once. Its values are scalars, singular returned rows or
materialized read rows. Scalar metadata preserves a direct local field's exact
declared type and nullability; arithmetic describes BigInt. An untyped scalar
NULL has `data_type: None` and `nullable: true`, without adding a persisted NULL
type. Typed nullable expressions retain their declared type when the value is
NULL. Row output retains typed columns and actual row versions.

`TransactionOutcome` returns summaries of executed requests and only explicitly
emitted values. Requirements have no request summary. With no emit, there is
no row output and no fabricated receipt. All scalar/output failures roll back
the block, including previous successful steps and allocated row versions.
Writing blocks make one commit decision; commit storage failures return Unknown
without speculative results. A certification Conflict discards this block's
staged data changes but may advance trusted decision/generation metadata under
the existing SQL publication policy. Read-only success releases its pinned snapshot
through rollback and does not advance durable generation. Neither form retries
automatically. Ordinary SQL and standalone operation behavior remain separate.

Block admission limits the whole serialized input to 64 KiB and all expression
roots together to 4,096 nodes and depth 32. There are at most 64 items, counting
final emit as one, at most 64 locals, and at most 64 expressions in emit. Empty
blocks and empty explicit emits are rejected; emit-only blocks are valid.
The supplied parameter-map limits apply once to the whole block. Frozen explicit
values and runtime local scalar occurrences share a 64 KiB payload budget;
repeated references count repeatedly, and values are admitted before copying.

Retained local results have an independent 1 MiB aggregate payload budget,
charged before copying output into immutable slots. Final emitted payload has
its own 1 MiB aggregate budget, checked for every occurrence before any output
payload is copied. Emitting the same row collection twice charges it twice.
These budgets count raw value payload, not total heap or metadata overhead.
Existing per-operation complete-input, projection, mutation, RETURN and page
limits still apply. In particular, a computed read checks every eligible output
row, while the retained-local budget counts only the materialized page. Each
computed update keeps its own 1 MiB postimage budget; there is no new cumulative
task-mutation budget or task authorization in this trusted API.

Catalog creates use separate `CatalogRequest::CreateTable` and `Engine` methods:
`execute_catalog`, `prepare_catalog_bound`, and `execute_catalog_bound`. A
`ColumnSpec` contains a canonical name, an existing relational `DataType` and
an explicit `not_null` flag; it has no object ID. The request carries an explicit
nonempty primary key naming unique declared columns. Both required fields and
nullable fields are represented directly. Catalog intents reject nullable PK
columns; SQL CREATE retains its existing nullable defaults and PK strengthening.
All six stored types, including SmallInt, remain available to the common engine.
This introduces no new value variants, catalog layout or persisted format.

Catalog requests admit at most 64 columns and 64 key names before copying or
serialization. Names must be canonical lowercase identifiers, and the shared
catalog validator rejects built-in/system reserved names, duplicate fields or
keys and unknown key fields. Input still fits the 64 KiB request bound. There
are no schema parameters, defaults, secondary indexes, constraints beyond the
existing PK/nullability/type checks, or implicit create-if-missing behavior.
Duplicate table names and catalog capacity preserve existing engine diagnostics.

An opaque `BoundCatalogOperation` freezes the ID-free request, engine version,
database domain and catalog epoch. Preparation validates the definition and
catalog availability, then releases its snapshot without allocating IDs or
advancing durable state. It deliberately does not decide empty/populated status.
Execution opens a fresh snapshot and rejects stale domain/epoch before staging;
it checks empty-database eligibility at execution, including data added after
preparation without a catalog change. Handles are process-local, reusable only
while their fences remain valid, and carry no authority or receipt contract.

Synchronous native creates require no relational rows, no built-in business
rows, and no retained tasks, operations, approvals or task-accounting entries.
Existing empty tables are permitted. Logical clock/allocator counters and restore
origin metadata alone do not make the database populated. A populated snapshot
returns `PlanRequired` without effects; no migration job is fabricated. This
finite policy applies to catalog creates and drops and does not restrict
ordinary SQL CREATE. Native ALTER TABLE and ADD/DROP CONSTRAINT run SQL's DDL
executor and, like SQL, succeed on populated Legacy tables. Catalog intents
cannot appear inside data transaction blocks or RETURN.

The private typed schema constructor enters the same internal CREATE executor,
statement admission, cancellation, full-envelope validation and transaction
staging path used by SQL. Stable table/column IDs and the new epoch are allocated
only in the private staged copy. A failure discards that definition and its ID
allocations. Commit certifies the same database generation, so an intervening SQL
write or catalog decision prevents publication. The previously documented
certification metadata policy still applies to Conflict. `CatalogOutcome`
contains the actual allocated `TableDef` and new catalog epoch only after a
successful commit; commit storage uncertainty returns Unknown with no speculative
descriptor. SQL parsing, public SQL dispatch and SQL prepared-description APIs
are not used by this path.

These common operations support explicitly selected finite native profiles.
The engine does not infer a native version or change ordinary SQL update policy.
On legacy it accepts tables with at most 1,024 rows, checks completeness using
a reverse scan of the full primary key, and rejects larger tables before
effects. On V2, reads and writes stream the snapshot-pinned table one decoded row
at a time into complete input (64 MiB retained-input budget), accept any table
size, and accept explicit caps/limits beyond 1,024; writes stage row by row
through the SQL V2 write path. Once receipts keep their 1,024-row bound. The
1 MiB scan result limit and existing row, input, expression and transaction
bounds still apply. A small point request does not bypass these table limits.
Task authority, native schema jobs, server opcodes and distributed operations
remain separate from this trusted execution surface. The Rust-only durable
retry contract below extends it without changing native text profiles.

A protobuf or HTTP adapter can call the separate data, transaction and catalog
APIs, but the current `Engine` requires a trusted `Admin`. It is not an
ordinary-user or task authorization boundary. Define versioned wire request and
result DTOs explicitly: internal Rust enums and serde layouts are not a stable
wire protocol, and opaque prepared handles have no serialization contract.
Return results only after these APIs report successful commit (or successful
read-only snapshot release). Unknown means publication could not be determined;
an adapter must not turn it into a confirmed rollback or automatically retry it.

`operations::once` defines a separate `OnceRequest` with an explicit
`contract_version = 1`, `OnceIdentity`, requested retention and a typed write or
writing transaction body. `Engine::execute_once` returns a `OnceReceipt` only
after the data and encoded outcome are published together. Its `outcome` member
contains either the original successful operation/block result or a retained
business error: `NotFound`, `AmbiguousMatch`, `ConditionFailed`, `TooManyMatches`
or `RequirementFailed`. A late retained block failure discards every earlier
staged write and allocator change. Other execution errors, rejected inputs,
cancellation that wins before the decision and insufficient receipt capacity
do not consume the key.
Standalone reads, catalog requests and declared read-only blocks have no once
contract. Existing SQL and unkeyed operations retain their existing behavior.

Persist the original identity before the first attempt: database domain, tenant
label, stable application namespace and opaque key. The namespace is a nonempty
case-sensitive UTF-8 string of at most 63 bytes; the key contains 1–256 arbitrary
bytes. These labels partition identity under trusted Admin authority. They do
not authorize a tenant, task or remote caller. Both `execute_once` and
`Engine::outcome` require the original database domain. Restore creates a new
domain, so an old identity returns `DomainMismatch`; it never becomes an absent
key eligible for automatic re-execution. Old-domain records remain inaccessible
forensic state and count toward capacity.

The V1 fingerprint resolves parameter leaves to typed values while preserving
the explicit request structure. A literal and a parameter with the same value
can match after ordinary input admission. Omitted clauses remain distinct from
explicit equivalents; local names, aliases and tuple, assignment, expression
and projection order remain significant. Duplicate-free conflict target order
is normalized. Other structurally different requests conflict even when they
would produce the same effects. Scope labels are separate from this body hash.
The accepted fingerprint additionally includes stable table/column IDs, types
and result descriptors from the execution snapshot. These codecs use explicit
tags and versions, independently of Rust enum layouts and serde. Future versions
must preserve the V1 retry contract.

A retained matching submission returns its original rows, versions, aliases,
completion flags, block outputs and conflict tuple mappings before live catalog
binding or business evaluation. `OnceReceipt::description` retains the accepting
catalog epoch and complete output descriptors, including standalone column
types/nullability and scalar emit types even when their values are NULL. These
descriptors are stored with terminal business outcomes too. Successful result
shapes and values are checked against them before encoding. Replay never infers
old types from live catalog objects or returned values.
Later row changes, missing or renamed tables, and reused
names do not change that receipt. Changed intent under the same key returns
`IdempotencyConflict`. An active identical owner returns `InProgress`; concurrent
attempts cannot publish twice. Replay and lookup do not advance database
generation or require a fresh clock. Prepared once handles and native `once`
text syntax are not implemented by this Rust-only version.

`OnceError::Unknown` means the publication response cannot establish the result.
Reopen a fenced database after a storage error, then resolve the original
identity with `outcome` or retry that same submission. A new key would describe
new work and can duplicate an already committed effect. Successful publication
does not depend on delivery of its acknowledgment.

Retention is requested in whole seconds, from 1 through 604,800. A trusted
`OnceClock` must certify a Unix-time interval valid throughout a monotonic lease
of at most 30 seconds. There is no default wall-clock implementation. The pure,
bounded provider runs after evaluation while publication is serialized and must
not call back into this database. In particular, `Admin::certify_server_time`
uses a separate repair watermark and cannot implement this callback.
`retry_until_seconds` is the certified upper bound plus requested retention,
checked for overflow. The lease is checked on both sides of the logical decision
barrier, before committing. Retention begins at that logical decision; it does
not promise a full new duration after acknowledgment or an arbitrarily slow
storage flush. Invalid, expired or regressing certificates refuse publication.

`Engine::compact_outcomes` may replace retained payloads with permanent
tombstones only when a fresh certified lower time bound is strictly beyond
their deadlines. Until explicit compaction, a receipt can remain replayable
beyond its guaranteed retention. Uncertainty never permits early removal. A
tombstone produces `KeyExpired` on matching retry and `Expired` on lookup;
altered intent still conflicts. This version never deletes or reuses identities.
No-op compaction publishes no generation or watermark. If no native ledger
exists, it returns zero without consulting the clock; an existing ledger still
requires certification before deciding nothing has expired. If compaction
returns Unknown, reopen and repeat it safely. A later zero count does not reveal
how many payloads the ambiguous attempt compacted.

Once adds explicit admission limits: 256 KiB canonical intent, 2 MiB encoded
result, 1,024 identities and 8 MiB encoded ledger, within the existing 16 MiB
database envelope. Encoding counts descriptors, row envelopes, summary and
conflict metadata, and even empty value cells. Ordinary value-payload limits
alone do not bound this representation: a valid unkeyed block with repeated
empty row emissions can exceed the receipt cap. Such a keyed request returns
`RetentionUnavailable` without durable business effects or key consumption.
Compaction can reclaim payload bytes but cannot reclaim identity slots.

The first retained outcome atomically adds namespace 7 and adopts storage/export
format 4. Databases without native outcomes keep format-3 bytes; legacy formats
1, 2 and 3 remain readable. Format 4 persists even after all results become
tombstones. Native receipts make the safe synchronous catalog-create check
populated. Replica images and protected/distributed paths explicitly reject
native outcome state until their retry-authority contract is implemented.

The `test-sql-parser-trap` feature exposes scoped, thread-local test instrumentation
that panics on SQL parsing and public SQL prepare, describe and execution calls,
including previously prepared SQL handles. Typed operations enter the private
engine execution method directly. Architecture tests show that SQL calls are
caught before effects, then run native text, AST and bound requests in that scope.
Normal builds do not enable this feature. See the [native profile](../giggles-language/README.md)
and the [SQL contract](SQL.md) for the supported syntax and public limits.

## Trusted catalog inspection

`Engine::inspect(&InspectionRequest)` accepts `Tables` or `Table { table }`.
It reads fresh committed catalog metadata under the engine's existing trusted
`Admin` authority. The returned `InspectionOutcome` includes the engine contract
version, database domain, catalog epoch and `complete: true`. `Tables` returns
name-sorted `TableSummary` entries with stable ID, name, column count and primary
key column count. These counts describe the schema; no business row count is
computed or returned. `Table` returns the exact `TableDef`, including column IDs,
all six existing scalar types, nullability, column order and primary-key order.

Domain, epoch and descriptors come from one borrowed committed snapshot. No SQL
parser, SQL execution, row query, explicit transaction or publication occurs.
Inspection does not consume a SQL transaction slot or advance generation, row
versions or catalog allocators. It does not see another session's staged DDL.
The storage kernel still loads and validates its bounded whole state, including
stored rows; this is not metadata-only physical I/O or a new scan-work guarantee.
There is no extra whole-state clone or relation materialization for inspection.

Requests use canonical lowercase names of at most 63 bytes. Oversized names
return `Limit`; other invalid names return `Bind("identifier")`; an absent table
returns `Bind("table")`. Inspection has no parameters, prepared handle, receipt,
data-block step, DDL mutation or remote/task authority contract. It cannot inspect
built-in repair state, roles, secondary indexes, jobs or topology. Cancellation
and storage/admission failures expose no partial metadata. Since there is no
commit, these failures are not an ambiguous business decision (`Unknown`).

Metadata is admitted before copying any returned names or descriptors, with at
most 128 summaries and 64 columns/primary-key components in a table descriptor.
`MAX_INSPECTION_METADATA_BYTES` is 65,536. The logical metric charges 30 bytes for
the outcome header (4 contract + 16 domain + 8 epoch + 1 completion + 1 result
tag), 8 for every ID, collection length, count and primary-key index, 8 plus the
UTF-8 length for each returned name, and 1 each for a column's data-type tag and
nullability flag. Thus a summary costs `32 + name.len()` after its vector length;
a table costs `32 + name.len() + sum(18 + column.name.len()) + 8 * pk.len()` after
the outcome header. These are admission units, not a persistent/network codec,
serialized JSON size or total heap bound. Current maximum valid result shapes
fit below this byte ceiling; helper tests also exercise exact admission bounds.

`database_domain` is a `u128` in Rust and canonical decimal **text** in serialized
inspection results, preserving values above `u64::MAX` through JSON values and
CLI output. Deserialization accepts only checked decimal strings without signs,
whitespace or leading zeroes (except `"0"`). Other numeric fields retain their
usual serialization. Reopen preserves catalog/domain identity; restore reports
the restored database's new domain with the preserved catalog descriptors.
Inspection adds no persisted schema, value, storage or export format.

## Finite trusted read explain

`Engine::explain_read(&Request, &Bindings) -> Result<ReadExplainOutcome>` accepts
only `Request::Read`, the existing simple field projection read. Other variants
return `Unsupported` before catalog acquisition or binding. Actual supplied
values, parameter membership, expression types, column names, order and limit
are validated with the existing binder against one current committed catalog
snapshot. No prepared or bound handle acts as a credential.

The serde outcome reports contract version, canonical decimal text database
domain, catalog epoch, table ID/name, projected column IDs, ordered column IDs
and directions, filter presence, typed execution stages and limits. Cardinality
and cost estimates are `None` (unknown). Literal and parameter values are absent.
The stages describe `execute_operation`: whole-state transaction snapshot,
bounded complete-table materialization and reverse-primary-key completeness
certification, then in-memory filter/order/field projection/output page. There
is no claimed indexed optimization. Legacy input and output retain their 1024-row
ceilings. V2 streams complete table input and accepts nonnegative read limits
that fit `usize`; output still defaults to 100. V2 explain reports
`input_rows: usize::MAX` (no fixed row ceiling) and the access path as its
first stage: `PrimaryKeyLookup` (the filter fixes the full primary key with
literals of bool, integer, text, uuid, bytes or numeric keys),
`StreamingFilter` (rows stream through the filter and only matching rows are
retained) or `StreamingCompleteTableMaterialization` (no filter). V2 inserts and
upserts read only the existing rows that share a proposed primary key. Filter/order evaluation still materializes complete input in memory;
byte budgets and once receipt limits remain; V2 mutations use the same input. Limits also report the Admin admission/selected-key
configuration and engine byte/expression/binding ceilings.

Explain itself borrows committed catalog metadata and does not call `run`,
`complete_table_engine`, a SQL parser/routine, mutation or commit. Legacy storage
still decodes and validates whole bounded state including stored rows; v2
borrows a rows-free catalog snapshot. There is no extra transaction-state clone,
logical business-row scan, relation materialization or publication for explain.
It sees committed catalog changes on every call, not staged DDL in another
session. Trusted Admin access is not generic task authorization.

Before binder/output clones, metadata admission charges `280 + table_name.len()
+ 8 * projection_count + 9 * order_count` logical bytes, bounded by
`MAX_EXPLAIN_METADATA_BYTES = 65_536`. The fixed charge reserves the header,
limits, stage tags and collection prefixes. Counts use resolved all-column
projection width when the request projection is empty. Checked arithmetic fails
closed. These units are not JSON size or total heap accounting. Exact-bound and
one-over admission are tested. No text renderer is supplied in this core slice.
This prerequisite does not claim the full local-core/reference explain bundle,
analyze, expression projections, writes, blocks, once or remote authorization.
Index creation remains separate; no persisted format changes are introduced.
