# HTTP and gRPC interfaces

The optional interfaces expose `giggles_core::operations` as typed requests and
atomic blocks. Clients send structured operations, expressions, or native language text; SQL
is an additional interface. Both use the same database, persistent transaction,
mTLS certificate bindings, task accounting and admission limits as the native
and PostgreSQL listeners. HTTP supports HTTP/1.1 and HTTP/2 over TLS. gRPC requires
HTTP/2 over TLS. This remains a single-host preview.

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
parameterized SQL, and task identities for the
existing scoped Preview, Execute, RepairSql and Status operations. Neither API
exposes provisioning, grants, approval creation, catalog DDL, administrative SQL,
remote bound handles or trusted core `once`/`outcome` handles. Typed operations
check every referenced table, including wrappers and later block steps, against
the application binding before any step runs. Task identities cannot invoke
native data operations, atomic blocks or native transaction controls.

Operators may also provision an existing SQL LOGIN role in the same binding
configuration: `{"fingerprint":"<64 lowercase SHA256 hex characters>",
"role":{"role":"Database","name":"app_user"}}`. The role name comes only from
this configuration. Unknown roles and roles without LOGIN reject validation and
session opening. These identities can open sessions, but typed native operations,
transaction controls and SQL requests currently return Scope (`42501`). Native
SQL-role privilege enforcement will be enabled separately.

A certificate-bound Database session checks both its fingerprint binding epoch
and its SQL role generation on every request. Removing or replacing the binding,
dropping/recreating the role, or revoking LOGIN invalidates existing sessions
(`28000`). Unchanged configuration reloads preserve bindings; removing and
readding one never revives old sessions. Password-only SQL changes use the same
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
`transactionState`, plus `operationOutcome`, `transactionOutcome`, `nativeError`,
or optional SQL/redacted task fields as appropriate. The three
states are `TRANSACTION_STATE_IDLE`, `TRANSACTION_STATE_IN_TRANSACTION` and
`TRANSACTION_STATE_ABORTED`. Domain errors such as `23505`, `42501`, `25P02`,
`57014` or `GD001` remain HTTP 200 / gRPC OK: always inspect `code`.
Task responses never recover denied fields from the submitted request.

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
the database's existing eight staged snapshots. Typed operations and SQL share
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
