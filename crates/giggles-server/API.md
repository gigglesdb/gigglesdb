# HTTP and gRPC interfaces

The optional interfaces expose `giggles_core::operations` as typed requests and
atomic blocks. Clients send structured operations, expressions, or native language text; SQL
is an additional interface. Both use the same database, persistent transaction,
mTLS certificate bindings, task accounting and admission limits as the native
wire and PostgreSQL listeners. HTTP supports HTTP/1.1 and HTTP/2 over TLS. gRPC requires
HTTP/2 over TLS. This remains a single-host preview.

The same requests also run over the native wire protocol, which costs much less
server CPU per request and supports pipelining, prepared handles and batches.
See the [native wire protocol reference](../../docs/reference/native-wire-protocol.md)
and [choosing a transport](../../docs/release-notes/native-wire.md#choosing-a-transport).

Design: [DD-0056](https://app.archdocs.io/design-docs/f82329f8-0d03-4077-b584-9b2579b941d9).
The executable contract is [giggles.proto](proto/giggles/v1/giggles.proto), with
[OpenAPI 3.1](docs/http-api.openapi.json) for HTTP clients.

## Enable listeners

Add either or both addresses to the existing server JSON configuration:

```json
{
  "http": "127.0.0.1:7434",
  "grpc": "127.0.0.1:7435",
  "api_limits": { "sessions": 32 }
}
```

These are additional fields, not a complete configuration. Omitted listeners
stay disabled. All listeners use the existing `certificate`, `private_key`,
`client_ca`, `bindings`, `connections` and `service_limits`. Port zero is supported;
startup prints the actual bound addresses only after every requested socket has
bound successfully. A later bind failure starts no request processing.

Only CA-verified client certificates with a current fingerprint binding have
authority. HTTP headers, URL paths, SQL and gRPC metadata cannot choose a role.
Use application identities for typed data operations, transaction controls and
parameterized SQL. The legacy Preview, Execute, RepairSql and Status requests
are retired from the published API and rejected with INVALID_ARGUMENT by default.
Neither API exposes provisioning, grants, approval creation, administrative SQL,
remote bound handles or trusted core `once`/`outcome` handles. Typed operations
check every referenced table, including wrappers and later block steps, against
the application binding before any step runs.

Operators may also provision an existing SQL LOGIN role in the same binding
configuration: `{"fingerprint":"<64 lowercase SHA256 hex characters>",
"role":{"role":"Database","name":"app_user"}}`. The role name comes only from
this configuration. Unknown roles and roles without LOGIN reject validation and
session opening. These identities can run typed native operations, native text,
atomic blocks and transaction controls using the SQL role's live table grants
(and owner/superuser authority). Every block target is authorized before any step
runs; GRANT and REVOKE apply on the next request. Native privilege requirements
match the current GigglesDB SQL policy, including its existing WHERE, RETURNING
and ON CONFLICT privilege limitations. SQL requests on these API sessions still
return Scope (`42501`).

A certificate-bound Database session checks both its fingerprint binding epoch
and its SQL role generation on every request. Removing or replacing the binding,
dropping/recreating the role, or revoking LOGIN invalidates existing sessions
(`28000`). The server promptly ends invalidated sessions: pgwire connections are
closed without a message (as for any revoked binding) and their open
transaction is rolled back; HTTP/gRPC sessions are aborted and their handles
answer 401/UNAUTHENTICATED. Unchanged configuration reloads preserve bindings;
removing and readding one never revives old sessions. Password-only SQL changes use the same
generation aliases as authenticated PostgreSQL sessions.

## Persistent sessions

| Operation | HTTP | gRPC method |
| --- | --- | --- |
| Open | `POST /v1/sessions`, body `{}`, status 201 | `giggles.v1.Giggles/OpenSession` |
| Execute | `POST /v1/session/execute`, status 200 after dispatch | `giggles.v1.Giggles/Execute` |
| Close | `DELETE /v1/session`, empty body, status 204 | `giggles.v1.Giggles/CloseSession` |

Open returns `sessionId`, `idleTimeoutSeconds`, `maxAgeSeconds` and
`transactionTimeoutSeconds`. Send the handle as exactly one `x-giggles-session`
header/metadata value on Execute and Close. Do not send it on Open. Handles are
64 lowercase hexadecimal characters, generated from 256 random bits. Treat them
as secrets: keep them out of URLs, logs, cookies and copied command transcripts.

One real engine session persists across requests, new TLS connections, and both
API protocols. Native `begin`, writes, reads, `commit` and `rollback` retain the
engine's normal transaction semantics. SQL `BEGIN`/`COMMIT`/`ROLLBACK` and typed
controls use that same transaction. A native write followed by a SQL read sees
the staged write; rollback through either interface discards both kinds of work.
Only the same certificate fingerprint
and unchanged role/binding epoch can resume it; another certificate with the same
role cannot. Removing and readding a binding never revives old connections or
handles. Restart invalidates every handle.

One Execute may own a session at a time, including queued work and response
delivery. Concurrent Execute returns HTTP 409 / gRPC `ABORTED`. Idle sessions
hold no physical socket permit. They expire after 60 seconds since the last
completed operation, after one hour total, or after 60 seconds in an explicit
transaction. Pings cannot extend the transaction lifetime. Close, expiry,
revocation and shutdown discard uncommitted staging and invalidate the handle.
Already-started decisions retain their ownership until they finish.

## Typed operations and atomic blocks

Native operation and text table names accept `schema.table`; bare names use the retained SQL
session search_path. Authorization checks the resolved SQL table identity for all
block targets before binding or executing any target.

Send one `operation` with a typed `request` and named `bindings`. For example,
insert an application row with typed parameters:

```json
{
  "operation": {
    "request": {"insert": {"table": "app", "values": [
      {"column": "id", "value": {"parameter": "id"}},
      {"column": "body", "value": {"parameter": "body"}}
    ]}},
    "bindings": [
      {"name": "id", "value": {"intValue": "9"}},
      {"name": "body", "value": {"textValue": "hello"}}
    ]
  },
  "budgetMs": 10000
}
```

`{"begin":{}}`, `{"commit":{}}` and `{"rollback":{}}` use the same Execute
endpoint and handle. These are typed controls and do not invoke the SQL parser.
Alternatively send native language text over HTTP or gRPC:

```json
{"text":{"text":"insert app set id=$id","bindings":[{"name":"id","value":{"intValue":"7"}}],"languageVersion":"0.16"}}
```

The `text` arm (protobuf field 12, `NativeTextRequest`) accepts `text`, optional
`bindings` in the same format as structured operations, and optional
`languageVersion` (`language_version` in protobuf). Versions `0.1` through `0.16`
are accepted; omission uses `0.1`, matching the CLI default. Text is limited to
65,536 UTF-8 bytes and lowered without catalog access. Every lowered table is
allowlist-authorized before catalog binding or execution, including every block
step. Data statements and atomic blocks use the same outcomes and retained
session transaction as their structured equivalents; use the existing `begin`,
`commit`, and `rollback` arms around text calls. Catalog, inspection, explain,
and once/receipt text are refused. Unknown versions, parse/lowering failures,
and invalid wire bindings yield HTTP 400 / gRPC InvalidArgument before session
leasing; diagnostics retain the fixed `invalid request` message. Engine binding
and evaluation errors retain the structured operation's existing result path.
The request variants are:

| Variant | Data |
| --- | --- |
| `read` | Table, optional filter, order, limit and column projection |
| `insert` | Table and literal/parameter assignments |
| `update` | Table, filter, optional guard, assignments and `one`/`many`/`claim` intent |
| `delete` | Table, filter, optional guard and cap |
| `updateExpressions` | Update with computed assignments |
| `readExpressions` | Read with computed projections and optional aliases |
| `returning` | Mutation wrapper returning actual row images; empty projection means all columns |
| `insertExpressions` | Insert with computed assignments |
| `insertMany` | Column names, expression rows and an explicit cap |
| `onConflict` | Insert wrapper, conflict target and `nothing` or guarded `update` action |

Expressions have explicit `value`, `field`, `compare`, `isNull`, `notExpr`,
`andExpr`, `orExpr`, `negate`, `arithmetic`, `local`, `call`, `caseExpr` and
`conflictField` variants. A `value` contains either a `literal` typed value or a
`parameter` name. Comparisons and arithmetic use the protobuf enum names, such as
`NATIVE_COMPARISON_EQ` and `NATIVE_ARITHMETIC_OPERATOR_ADD`. Functions are
`NATIVE_FUNCTION_COALESCE`, `NATIVE_FUNCTION_ABS` and `NATIVE_FUNCTION_LENGTH`.
Local references expose `field`, `status`, `changed` or `count`; conflict fields
select `NATIVE_CONFLICT_SOURCE_EXISTING` or `NATIVE_CONFLICT_SOURCE_INCOMING`.
The protobuf/OpenAPI definitions specify every field and presence rule.

An atomic block can execute requests, bind immutable local outcomes, require a
Boolean condition, and emit scalars, one row or all rows from a local. For example,
this read-only block emits up to ten rows without publishing its own snapshot:

```json
{
  "transaction": {
    "transaction": {
      "readOnly": true,
      "steps": [{"letBinding": {"name": "rows", "request": {"read": {
        "table": "app", "order": [{"field": "id", "descending": false}],
        "limit": {"literal": {"intValue": "10"}}, "projection": ["id", "body"]
      }}}}],
      "emit": {"values": [{"rows": "rows"}]}
    },
    "bindings": []
  }
}
```

With no explicit transaction, a successful writable block publishes once; a
read-only block discards its snapshot. An ordinary standalone native operation,
including `read`, follows the engine's owned-transaction commit behavior. Such a
read can advance the database generation and conflict with another staged
transaction; use a read-only block when a read must avoid publication.
Inside an explicit transaction, either kind of block joins the existing staging
and leaves it open. A block never commits or rolls back a successful outer
transaction. Engine validation, binding or evaluation failure aborts the outer
transaction; issue `rollback` before continuing. Wire/schema rejection occurs
before leasing the session and leaves the current transaction unchanged.

Native semantics are preserved: cardinality is checked before a mutation guard;
caps reject the whole operation; equal-value updates report `changed: "0"` and
preserve row versions. `claim` must consume the selected row and is unavailable
inside blocks. `onConflict.nothing` reports skipped inputs, while a false or NULL
update guard fails the operation. Conflict metadata is in input order; a
`returnedRow` index refers to the returned rows in canonical primary-key order.
All input rows must fit the core's 1024-row completeness bound even when a filter
or result limit would return fewer rows. A page with `complete: false` is not
proof that the rest of the input was omitted from validation.

## JSON and protobuf

HTTP requests require `Content-Type: application/json` (optional UTF-8 charset).
Compression is unsupported. Field names are lowerCamelCase; enum values are
their protobuf names. Exactly one request variant is required. SQL remains
available through the same session, including parameters:

```json
{"sql":{"sql":"INSERT INTO app (id,body) VALUES ($1,$2)","params":[{"intValue":"9"},{"textValue":"hello"}]},"budgetMs":10000}
```

`{"sql":{"sql":"BEGIN"}}`, `{"sql":{"sql":"COMMIT"}}` and
`{"sql":{"sql":"ROLLBACK"}}` use the same endpoint and handle.

Native and SQL values use exactly one tag: `nullValue`, `boolValue`, `intValue`,
`textValue` or `bytesValue`. NULL is `{"nullValue":"SQL_NULL"}`; false and zero are
explicit values. Bytes use canonical padded base64. Every 64-bit integer,
including operation IDs, row versions, counts and affected rows, is a canonical
decimal **string** in JSON. Leading zeros, `+`, `-0`, exponent notation and
out-of-range values reject. Protobuf carries the corresponding typed integers.
`budgetMs` is a JSON number because its range is 0 through 10000.

Unknown or duplicate JSON fields, multiple oneof alternatives, excessive nesting
(over 128 containers), trailing content and missing required business fields
reject before dispatch. Protobuf follows normal unknown-field compatibility;
the adapter checks required logical presence and rejects unknown business enum
values. Native expression depth is separately capped at 24 edges, with 4096
aggregate AST nodes; protobuf's own decoding recursion bound is retained.
Table, column and local identifiers match `[a-z][a-z0-9_]{0,62}`; parameter names
also permit a leading underscore. Duplicate bindings reject, and
explicit Boolean presence is required for `readOnly`, sort `descending` and
`isNull.negated`. Open and Close have empty protobuf messages. gRPC accepts exactly one
uncompressed message per call, with a 256 KiB payload cap.

Successful dispatch returns `ExecuteReply`, including `version: 1`, `code` and
`transactionState`, plus `operationOutcome`, `transactionOutcome`, `catalogOutcome`, `nativeError`,
or optional SQL/redacted task fields as appropriate. The three
states are `TRANSACTION_STATE_IDLE`, `TRANSACTION_STATE_IN_TRANSACTION` and
`TRANSACTION_STATE_ABORTED`. Domain errors such as `23505`, `42501`, `25P02`,
`57014` or `GD001` remain HTTP 200 / gRPC OK: always inspect `code`.
Task responses never recover denied fields from the submitted request.

SQL results in `sql` contain column names in `columns` and PostgreSQL type OIDs
in `column_types`, parallel to those names and equal to the PostgreSQL listener's
RowDescription OIDs. HTTP encodes `column_types` as an array of unsigned numbers;
gRPC uses repeated uint32. These are type OIDs only, without typmods (for example,
VARCHAR length or NUMERIC precision/scale). An empty query result still includes
its column names and types. DDL and DML without RETURNING have no columns and no
types. Older replies that omit `column_types` decode as an empty list.

`operationOutcome` preserves column names, typed rows and their real versions,
`matched`, `affected`, `changed`, `complete`, and optional `conflictResults`.
`transactionOutcome` has per-step summaries and typed emitted values; summary
`itemIndex` refers to the original step index and excludes `require` steps.
Emitted row sets include column IDs, names, data types, nullability and
completeness. Ordinary operation results contain column names, not invented
column type metadata. Every emitted value is checked against the reply limit.

Native failures use `code: "native_error"` and `nativeError.kind`, with an optional
syntax offset and an optional `sqlstate` string carrying the underlying SQLSTATE.
The `NATIVE_ERROR_CLASS_` suffixes are `SYNTAX`, `UNSUPPORTED`,
`LIMIT`, `BIND`, `TYPE`, `NOT_FOUND`, `AMBIGUOUS_MATCH`, `CONDITION_FAILED`,
`TOO_MANY_MATCHES`, `CLAIM_NOT_CONSUMED`, `CONFLICT`, `CORE`, `UNKNOWN`, `OVERFLOW`,
`DIVISION_BY_ZERO`, `REQUIREMENT_FAILED`, `DUPLICATE_TARGET`, `PLAN_REQUIRED`,
`DUPLICATE` (tag 19, SQLSTATE 23505) and `SQL` (tag 20). Existing tags 1 through
19 are unchanged; `NativeError.sqlstate` is optional protobuf field 3.
`SQL` preserves errors such as unknown column (42703) and FK violation (23503).
Bind/core diagnostics do not expose raw engine strings. Unknown native decisions
use `code: "08007"` and `NATIVE_ERROR_CLASS_UNKNOWN`, without successful outcomes.
Authorization, transaction state and cancellation retain the existing `42501`,
`25P02` and `57014` codes. Always check `code` before using a result.

Transport errors use a fixed HTTP body such as
`{"error":{"code":"SESSION_BUSY","message":"session busy"}}`.

| Condition | HTTP | gRPC |
| --- | --- | --- |
| Malformed input or missing session header | 400 | INVALID_ARGUMENT |
| Unbound/revoked identity | 401 | UNAUTHENTICATED |
| Unknown, expired or differently owned handle | 404 | NOT_FOUND |
| Session busy | 409 | ABORTED |
| Capacity/rate exhausted | 429 | RESOURCE_EXHAUSTED |
| Input too large | 413 | RESOURCE_EXHAUSTED |
| Unsupported encoding | 415 | UNIMPLEMENTED |
| Deadline before dispatch | 408 | DEADLINE_EXCEEDED |
| Stopping service | 503 | UNAVAILABLE |
| Internal failure | 500 | INTERNAL |

Unknown HTTP routes return 404 and wrong methods return 405 with `Allow`.
All application responses use `Cache-Control: no-store`. Fixed diagnostics do
not echo SQL, handles, certificate bytes or submitted metadata.

## Deadlines and unknown outcomes

`budgetMs` defaults to 10000 and starts when the complete request is received.
Zero cannot submit a business decision. A valid gRPC `grpc-timeout` starts at
header receipt; its earlier deadline also bounds request-body reception. The
effective decision deadline includes the session and transaction lifetimes.
Each invocation has a fresh cancellation context, including retries of a task
operation. Input, TLS and output stages have separate ten-second bounds.

Cancellation can prevent a decision only before the core commit fence. A lost
response, timeout, Close or disconnect cannot undo a committed result. A
post-dispatch encoding failure returns the existing `08007` ambiguity code if a
safe reply can be sent. Clients must not automatically retry native or SQL writes,
writable blocks or COMMIT: these application operations have no durable operation
ID in this profile. Reconcile the database
before deciding what to do next. For scoped task operations, query Status using
the original operation ID; durable receipts retain the existing deduplication
contract. A timed-out retry says nothing about an earlier invocation's outcome.

## Limits and protocol driver

| Resource | Bound |
| --- | --- |
| Logical API sessions | 32 default, configurable 1..128 |
| API calls, across HTTP/gRPC runtimes on one Service | 128 |
| Physical sockets / blocking workers | Existing shared Service caps / 32 |
| Request payload / encoded reply | 256 KiB / 8 MiB |
| Native expression depth / aggregate AST nodes | 24 edges / 4096 |
| Bindings / raw binding bytes | 256 / 64 KiB |
| Core serialized native input / resolved value occurrences | 64 KiB / 64 KiB |
| Columns, assignments, order, projections, conflict targets, function args, CASE arms | 64 each |
| Native block steps plus one item when emit is present / locals / emitted values | 64 / 64 / 64 |
| Native insert rows / complete input rows | 1024 / 1024 |
| Native local, emitted, mutation and result raw values | Core 1 MiB bounds |
| HTTP/2 concurrent streams per connection | 16 |
| Decoded header list / header count | 16 KiB / 64 |
| HPACK dynamic table | 4 KiB |
| HTTP/2 frame / stream receive window | 16 KiB / 64 KiB |
| HTTP/2 connection receive window | 1 MiB, adaptive growth disabled |
| HTTP/2 send buffer target / emitted DATA chunk | 64 KiB / 16 KiB |
| Pending accepted resets / local error resets | 16 / 16 |

The shared accept loop uses Hyper's maintained HTTP driver for both interfaces;
generated tonic services run behind it without a second socket/TLS stack.
The pinned implementation is Hyper 1.12.0, hyper-util 0.1.21, h2 0.4.20,
tonic/tonic-prost 0.14.9 and prost 0.14.4 (see Cargo.lock). The h2 decoder starts
with its 4096-byte HPACK table and rejects excessive table updates. Its encoded
header continuation count is bounded to five nonfinal frames plus one final
continuation at the configured header/frame sizes. The builder sets the remaining
bounds explicitly in `api/net.rs`.
The encoded partial-header block can retain up to seven 16 KiB frames (112 KiB);
the pinned Huffman decoder can reserve approximately 180 KiB for its expanded
string. Allocator rounding adds overhead. The 16 KiB decoded-header acceptance
limit is therefore not a peak parser-memory claim. Reset history is bounded to
50 entries for one second by the pinned h2 default.
HTTP/2 PING does not refresh application activity; an idle or stalled connection
expires after ten seconds. For an incomplete HTTP/2 header block while prior
calls remain active, there is no separate ten-second header timer. The decoder
rejects interleaved input, and the existing stage deadlines provide a conservative
source-derived bound of 50 seconds: up to ten for prior bodies, twenty for
decision/cleanup, ten for writes, then ten idle. New requests cannot extend an
unfinished header block. This is a logical bound, not a measured scheduling or
availability guarantee. No compression, server reflection or retry middleware is
enabled.

Admission happens before body buffering/decoding, including unknown routes and
malformed calls. Socket and request attempts consume the same existing token
bucket as native and PostgreSQL traffic. Output buffers carry the session and
permit guards through Hyper/h2 send queues; dropping the request future does not
release a running worker's ownership. Shutdown signals all listeners, closes
handles, cancels unsealed work and joins supervised workers. Broken storage can
delay a started synchronous decision; there is no hard shutdown-time guarantee.

For 128 calls, input reservations total at most 32 MiB and encoded response
reservations at most 1024 MiB; these overlap with bounded engine results and
decoded representations. The separate logical session cap does not multiply
the database's existing transaction cap (eight staged snapshots on legacy, 128
v2 transactions). Typed operations and SQL share
those snapshots; an atomic block inside a session does not allocate a second
outer transaction. The core's raw-value byte limits are not heap-object limits:
many empty values can still produce substantial object and wire overhead. The
separate encoded-reply cap prevents successful partial or oversized outputs.
HTTP/2 flow-control windows,
TLS/TCP buffers, decoded objects and allocator overhead add memory. These are
capacity dimensions, not measured RSS. See
[CREDENTIALS_ADMISSION.md](CREDENTIALS_ADMISSION.md) for the full worksheet.

## Runnable examples

The database must already contain an authorized `app(id BIGINT PRIMARY KEY,
body TEXT)` table. Generated test fixtures provide it. Examples select existing
rows by default; insertion is explicit so repeated demos do not duplicate IDs.

```powershell
./examples/http-native.ps1 -Endpoint https://localhost:7434 -CaCertificate CA.pem -ClientCertificate app.pem -ClientKey app.key.TEST-ONLY
cargo run --example grpc_native -- https://localhost:7435 CA.pem app.pem app.key.TEST-ONLY
```

Add `-Insert -Id 9 -Text hello` to the HTTP command, or `9 hello` to the Rust
command, for a native parameterized insert inside the transaction. Both examples
use typed BEGIN/COMMIT, a native read and a read-only block emitting rows, with no
SQL text. Both close the session explicitly and attempt rollback on failure
without retrying mutations. The additional `http-transaction.ps1` and
`grpc_transaction` examples demonstrate the optional parameterized SQL interface
with the same arguments.
The HTTP example requires PowerShell 5.1+ and curl with PEM TLS support. On Windows,
pass `-CurlPath PATH/TO/curl.exe` for a build using OpenSSL or LibreSSL if the system
curl uses Schannel and cannot load PEM client keys; check `curl.exe --version`.
Supply
paths and endpoint hostnames matching the certificate; do not bypass TLS checks.

Export a descriptor without an installed protoc:

```powershell
cargo run --example export_descriptor -- artifacts/giggles-v1.bin
grpcurl -cacert CA.pem -cert app.pem -key app.key.TEST-ONLY -protoset artifacts/giggles-v1.bin -d '{}' localhost:7435 giggles.v1.Giggles/OpenSession
```

Store the returned handle securely. The following uses a placeholder and exposes
it to local process inspection; use a trusted workstation and avoid command logs:

```powershell
'{"begin":{}}' | grpcurl -cacert CA.pem -cert app.pem -key app.key.TEST-ONLY -protoset artifacts/giggles-v1.bin -H 'x-giggles-session: SESSION_HANDLE' -d '@' localhost:7435 giggles.v1.Giggles/Execute
```

Use the same metadata for subsequent Execute and CloseSession calls. Reflection
is intentionally unavailable. The build uses vendored protoc, includes generated
Rust client/server types, and exposes the descriptor as
`api::proto::FILE_DESCRIPTOR_SET`.

## Typed aggregate and join reads

`ExecuteRequest.typed_read` (protobuf field 13), JSON `typedRead`, reads a table
with optional INNER/LEFT join, WHERE, GROUP BY, HAVING, ORDER BY and LIMIT.
Table/column/source names are exact identifiers. Each source requires SELECT
for Database roles or inclusion in the Application allowlist, before catalog
binding. Task roles are refused. These reads use the retained session, including
uncommitted SQL changes and native begin/commit/rollback controls.

```json
{"typedRead":{"table":"app","projection":[
  {"expression":{"aggregate":{"function":"count_star"}},"alias":"n"}
]}}
```

Column expressions use `{"column":{"name":"id","source":"a"}}` (source is
optional). Other scalar arms are `literal` (the existing Value profile) and
`aggregate`: `count_star` has no column; `count`, `sum`, `min`, `max`, `avg`
require a column object. Predicates use `compare` (left/comparison/right with
existing NATIVE_COMPARISON enums), `isNull` (expression/negated), `notExpr`,
`andExpr` and `orExpr` (left/right). The OpenAPI schema describes all fields.
Single-source groupBy accepts unqualified columns; join groupBy accepts scalar
expressions. Each expression tree has depth at most 24; total nodes at most
4096; projection/groupBy/orderBy each at most 64. Projection must be nonempty.

Add `join: {"table":"other","leftAlias":"a","rightAlias":"b","left":true,
"on":{"compare":{"left":{"column":{"source":"a","name":"id"}},
"comparison":"NATIVE_COMPARISON_EQ","right":{"column":{"source":"b","name":"id"}}}}}`
inside typedRead for a LEFT JOIN (`left:false` selects INNER JOIN).
`orderBy` entries require expression and descending, with optional nullsFirst;
`limit` is a canonical signed 64-bit decimal string. Results use
`operationOutcome` columns/rows (matched=row count, affected=changed=0,
complete=true). Binding/execution failures use nativeError kind SQL with the underlying SQLSTATE.
Malformed shapes return HTTP 400/gRPC InvalidArgument before leasing the
session; an execution failure aborts an open transaction.

### Session search_path

Execute accepts `{"searchPath":{}}` to read the retained session path, or
`{"searchPath":{"set":true,"schemas":["app","public"]}}` to replace it.
The protobuf oneof field is `search_path = 14` (`NativeSearchPathRequest`).
Omitted/false `set` reads; nonempty schemas require true. True with no schemas
clears the path. Names must be nonempty, NUL-free, at most 63 UTF-8 bytes,
and the list at most 64 entries. Unknown JSON fields are rejected.
Application and Database sessions may use this operation; Task sessions may not.
The `ok` reply contains SQL columns `["search_path"]`, one text row, and the
normal transaction state. The default is `"$user", public`. Simple lowercase
identifiers display bare; other names are double quoted with doubled quotes;
empty paths display `""`. Keyword quoting currently differs from PostgreSQL:
`select` displays bare here, versus `"select"` in PostgreSQL 18.6.
Native operations, native text 0.18, and SQL reads share the retained path.
SQL API `SET search_path` currently returns `0A000` (unsupported). Session path changes commit with a successful transaction and restore the prior path
on rollback. COMMIT in an aborted transaction remains refused with `25P02`; the retained path is restored when ROLLBACK ends the block.

### Native savepoints

Execute accepts `{"savepoint":{"name":"a"}}`, `{"rollbackTo":{"name":"a"}}`,
and `{"release":{"name":"a"}}` (gRPC fields 15-17, `NativeSavepointRequest`).
Names are required, nonempty, case-sensitive strings without NUL; unknown JSON
fields are rejected. Invalid shapes return HTTP 400 / gRPC INVALID_ARGUMENT.
Long names reach core and truncate at the 63-byte UTF-8 boundary. Duplicate
names resolve to the latest savepoint. These controls use the existing native
transaction control replies, binding checks, and role gates.

Outside a transaction they return SQLSTATE 25P01; unknown names return 3B001.
An aborted transaction rejects savepoint/release with 25P02. `rollbackTo` can
recover it; an aborted commit remains 25P02 until recovery or full rollback.
Native savepoints capture the retained API search path. Rollback restores the
latest matching capture, discards later captures, and retains its target;
release discards its target and later captures while keeping the current path.
Transaction end clears captures. SQL savepoint controls do not maintain this
API path stack; mixing SQL and native savepoints can therefore differ in path
restoration.

## Native catalog DDL

Execute accepts `catalog` (protobuf field 18, `NativeCatalogRequest`) with
exactly one request below. Each example is a complete Execute JSON body.

```json
{"catalog":{"createTable":{"table":"public.items","columns":[{"name":"id","dataType":"BigInt","notNull":true},{"name":"body","dataType":"Text"}],"primaryKey":["id"]}}}
{"catalog":{"createIndex":{"table":"public.items","name":"items_body","columns":["body"]}}}
{"catalog":{"dropTable":{"table":"public.items","cascade":false}}}
{"catalog":{"dropIndex":{"name":"public.items_body","cascade":false}}}
{"catalog":{"alterTable":{"table":"public.items","addColumn":{"name":"note","dataType":"Text","notNull":false}}}}
{"catalog":{"alterTable":{"table":"public.items","dropColumn":{"name":"note","cascade":false}}}}
{"catalog":{"alterTable":{"table":"public.items","renameColumn":{"from":"body","to":"description"}}}}
{"catalog":{"alterTable":{"table":"public.items","renameTable":{"to":"products"}}}}
```

`table`, index `name`, column `name`/`dataType`, rename `from`/`to`, and
exactly one alter action are required where shown. `columns` and `primaryKey`
default to empty lists and each accepts at most 1600 entries; core validates
their meaning. Native table creation requires a primary key and returns
`NATIVE_ERROR_CLASS_BIND` without one; SQL can create a heap table without a
primary key. `notNull` and `cascade` default to false. Unknown fields, duplicate
keys, nulls and multiple request/action alternatives are rejected.
The exact, case-sensitive `dataType` names are `Boolean`, `SmallInt`, `Integer`,
`BigInt`, `Text`, `Bytes`, `Numeric`, `Real`, `DoublePrecision`, `Date`, `Time`,
`TimeTz`, `Timestamp`, `TimestampTz`, `Interval`, `Varchar`, `Bpchar`, `Uuid`,
`Jsonb`, `IntArray`, `BigIntArray`, `TextArray` and `UuidArray`; SQL type spellings and
parameterized type declarations are not accepted here.

Create-table columns additionally accept `default` with exactly one of
`{"Value": <native Value>}`, `{"Nextval":"sequence_name"}` or
`{"Clock":{"kind":"Now"}}`, and `generated`: `IdentityAlways`,
`IdentityByDefault` or `Serial`. Clock kinds are `Now`, `TransactionTimestamp`,
`StatementTimestamp`, `ClockTimestamp`, `CurrentDate`, `CurrentTime`,
`CurrentTimestamp`, `LocalTime`, `LocalTimestamp`. Only the last four accept
optional unsigned 32-bit numeric `precision`; execution validates its meaning.
Unknown keys/kinds are malformed. Protobuf column tags are default=4 and
generated=5; default oneof tags are value=1, clock=2, nextval=3, and clock tags
are kind=1, precision=2. Native ALTER addColumn rejects either new field as a
malformed request on both transports; SQL ALTER TABLE ADD COLUMN supports DEFAULT/IDENTITY.
Catalog outcomes do not echo default/generated definitions.

Successful replies use `code: "ok"` and `catalogOutcome` (protobuf field 11):
`catalogEpoch` is an unsigned 64-bit decimal string, `table` is the canonical
schema-qualified table name (the new name after rename), `columns` contains
logical columns in table order with `name`, `dataType` and `notNull`, and
`primaryKey` contains logical key column names. Hidden row IDs are omitted.
`indexId`, an unsigned 64-bit decimal string, is present for index
creation/deletion. Drop outcomes
describe the removed table or the index's owning table.

Catalog changes execute synchronously in the retained session. Outside an open
transaction, each successful DDL request commits automatically. Native and SQL
DDL share transaction visibility and rollback, including native savepoints.
Unqualified names use the retained `search_path`; creation uses its creation
schema. The legacy engine returns `NATIVE_ERROR_CLASS_PLAN_REQUIRED` (tag 18)
on a populated database, where SQL DDL can succeed.
Malformed requests return HTTP 400 / gRPC INVALID_ARGUMENT; execution failures
use the existing native error mapping above, including `NATIVE_ERROR_CLASS_SQL`
with preserved SQLSTATEs, and abort an open transaction. An aborted transaction
requires rollback or rollback-to-savepoint recovery before further DDL.

Authorization follows SQL DDL rules: Application and Task roles cannot perform
any of the five catalog operations. Database roles need schema CREATE, schema
ownership or superuser for CREATE TABLE and CREATE INDEX, and table ownership
(or superuser) for ALTER/DROP TABLE and DROP INDEX. Table grants alone do not
authorize DDL. CREATE INDEX follows GigglesDB SQL's schema CREATE rule even on
another role's table; PostgreSQL 18.6 instead requires table ownership. After
the session binding and role gate, authorization precedes validation, column
binding and classification (E939), so an unauthorized request with a missing
column returns `42501`. Unauthorized DROP/ALTER TABLE and CREATE/DROP INDEX
also return `42501` for missing objects, where PostgreSQL 18.6 returns `42P01`
(relations) or `42704` (DROP INDEX). A missing creation schema returns `3F000`.
Live privilege revocation is checked on each request, and a denied request
inside BEGIN aborts the transaction (subsequent requests return `25P02`).

Constraint catalog requests use additive oneof tags `add_constraint=6` and
`drop_constraint=7`.
```json
{"catalog":{"addConstraint":{"table":"public.items","name":"items_unique","unique":{"columns":["body"]}}}}
{"catalog":{"addConstraint":{"table":"public.items","name":"items_check","check":{"expression":{"compare":{"left":{"field":"id"},"comparison":"NATIVE_COMPARISON_GT","right":{"value":{"literal":{"intValue":"0"}}}}}}}}}
{"catalog":{"addConstraint":{"table":"public.items","name":"items_fk","foreignKey":{"columns":["parent"],"refTable":"public.parents","refColumns":["id"],"refColumnsSet":true,"onDelete":"Cascade","onUpdate":"NoAction"}}}}
{"catalog":{"dropConstraint":{"table":"public.items","name":"items_unique","cascade":false}}}
```
`name` is optional on ADD. Exactly one of `unique`, `check`, `foreignKey` is
required. CHECK uses the existing bounded native expression wire message;
execution currently supports only a column compared with an int, text, bool
or NULL literal (other shapes return `NATIVE_ERROR_CLASS_UNSUPPORTED` without
a SQLSTATE, even where SQL accepts the expression). FK actions are
`NoAction`, `Restrict`, `Cascade`, `SetNull`, `SetDefault` (default `NoAction`).
`refColumnsSet` absent/false infers the referenced primary key and ignores
`refColumns`; true preserves the array, including empty. Table names use the
retained search_path. Outcomes name the constrained table, without `indexId`.
ADD/DROP require table ownership; REFERENCES also requires referenced-table
ownership under the shared SQL rule. Application roles cannot perform DDL.
Authorization precedes column and constraint binding. DROP CONSTRAINT CASCADE
succeeds in native and SQL alike, matching PostgreSQL 18.6.
Duplicate named UNIQUE reports 42P07 for the existing backing-index name,
matching PostgreSQL 18.6.
Legacy native catalog requests on populated databases retain the existing
`NATIVE_ERROR_CLASS_PLAN_REQUIRED` gate, while SQL DDL executes synchronously.

View catalog requests use additive oneof tags `create_view=8`
(`NativeCreateView {name=1, read=2}`, where `read` is the field-13
`NativeTypedRead`, single source or join) and `drop_view=9`
(`NativeDropView {name=1, cascade=2}`).
```json
{"catalog":{"createView":{"name":"app.recent","read":{"table":"app.items","projection":[{"expression":{"column":{"name":"id"}}}]}}}}
{"catalog":{"dropView":{"name":"app.recent","cascade":false}}}
```
Views run through core's SQL view executor, so rows, dependencies and
SQLSTATEs equal the matching `CREATE VIEW name AS SELECT ...` / `DROP VIEW`
(42P01 missing relation, 3F000 missing schema, 2BP01 for a dependent view
without CASCADE). Known differences shared with SQL: a duplicate view name
returns `23505` on V2 (PG18: `42P07`); join views return `0A000` in
both SQL and native (PG18 accepts them); the Legacy engine has no views
(`0A000` in both). Names resolve through the retained
search_path; the outcome names the canonical `schema.view` without `indexId`.
Authorization equals SQL: CREATE VIEW needs CREATE on (or ownership of) the
creation schema plus SELECT on every read relation; DROP VIEW needs ownership
of the view; Application and Task roles are refused. Authorization precedes
binding, so a refused role reading a missing table gets `42501`. The creator
owns the view, as with SQL. Views inside BEGIN follow the transaction.

### Native keyset paging

`NativeOperationRequest` has an additive field `page = 3` (`NativePage {repeated Value after = 1}`)
and `NativeOutcome` has `next_page = 8`. With `page` present, a read request runs through core
`read_page`: the read's `order` is the keyset and its `limit` is the page size. Send `"page":{}`
for the first page and then the previous reply's `nextPage` until a reply has none (its
`complete` is then true). Without `page`, operations behave exactly as before.
```json
{"operation":{"request":{"read":{"table":"app","order":[{"field":"id","descending":false}],"limit":{"literal":{"intValue":"100"}},"projection":["id","body"]}},"page":{}}}
{"operation":{"request":{"read":{"table":"app","order":[{"field":"id","descending":false}],"limit":{"literal":{"intValue":"100"}},"projection":["id","body"]}},"page":{"after":[{"intValue":"100"}]}}}
```
Authorization is the same as for the unpaged read and precedes the continuation check. Errors
are core's: an invalid continuation (wrong arity or type) and the order/limit rules of
`read_page`; paging a non-read request returns `NATIVE_ERROR_CLASS_UNSUPPORTED`. Pages run in the
retained session, so inside BEGIN they see the transaction's own writes. Continuations are
plain key values: rows committed between pages appear if they sort after the continuation.

### Native sequences

Catalog requests gain additive oneof tags `create_sequence=10`
(`NativeCreateSequence {name=1}`) and `drop_sequence=11`
(`NativeDropSequence {name=1, cascade=2}`); the reply is `catalogOutcome` with
the canonical `schema.sequence` and empty `columns`/`primaryKey`.
`ExecuteRequest` gains `sequence=19` (`NativeSequenceCall`, oneof
`nextval=1`/`currval=2` with `NativeSequenceName {name=1}`, or
`setval=3` with `NativeSetval {name=1, value=2, is_called=3}`).
```json
{"catalog":{"createSequence":{"name":"app.ids"}}}
{"sequence":{"nextval":{"name":"app.ids"}}}
{"sequence":{"setval":{"name":"app.ids","value":"100","isCalled":false}}}
{"catalog":{"dropSequence":{"name":"app.ids","cascade":false}}}
```
A call's reply carries the same `sql` result as SQL `SELECT nextval('app.ids')`
(one row, one bigint column named `nextval`, `currval` or `setval`). Names
resolve through the retained search_path. Execution uses core's SQL sequence
executor in the retained session: CREATE/DROP SEQUENCE are transactional,
nextval/setval are not undone by ROLLBACK, currval is per session (55000
before the first nextval), and errors are SQL's (42P07, 42P01, 3F000, 2BP01).
Authorization equals SQL and precedes binding: CREATE SEQUENCE needs
superuser or CREATE on schema `public` (the existing SQL rule); DROP SEQUENCE
needs ownership; calls need ownership of the sequence or of the table that
owns it (Application roles: a table in their allowlist). Refused requests,
including ones naming a missing sequence, return `42501`. The creator owns a
natively created sequence, as with SQL.

### Write fence holder

The server JSON accepts `"write_fence_token_file": "/secure/holder-token"`.
When set, the server reads at most 1 MiB from that file, removes one trailing
newline (LF or CRLF), rejects an empty token, and opens the database as the
write fence holder. Keep the file private. The token is never a CLI value or
logged. This option cannot be combined with cluster or replication mode.
Without the option, a fenced database still permits reads; writes fail with
SQLSTATE `25006`, `database is write-fenced (epoch N)`. A wrong token also
leaves writes fenced. Omit the option for ordinary startup.
