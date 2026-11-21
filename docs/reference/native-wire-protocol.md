# Native wire protocol

## Overview and listeners

GigglesDB native wire v1 (GD-0091) carries binary Protocol Buffers over a
connection-oriented byte stream. It uses the same execution API as native HTTP
and gRPC. One signed-in session runs on each connection. The client can send
several requests before reading their replies.

All message names below are in the `giggles.v1` package. Use the field numbers
and types in [giggles.proto](../../crates/giggles-server/proto/giggles/v1/giggles.proto)
to generate or implement the message codec.

| Config key | Listener | Transport | Sign-in |
| --- | --- | --- | --- |
| `wire` | Ordinary, optional bind address | TLS over TCP with a required client certificate | Certificate binding in the ordinary runtime |
| `agent.wire` | Agent, optional bind address in the `agent` section | TLS over TCP, with a certificate required for sign-in | Certificate binding in the agent runtime, with an optional task selector |
| `role_api.wire` | SQL role, optional bind address in the `role_api` section | Configurable TLS or plain TCP | SQL-role authentication |
| `native` | Deprecated alias for `wire` | Same as `wire` | Same as `wire` |

`native` is a deprecated alias for `wire`. Setting only `native` starts the
ordinary wire listener on that address and prints a deprecation warning to
stderr. Setting both `native` and `wire` is a startup error. The legacy
JSON-frame listener no longer exists.

All v1 wire listeners are subject to shared admission and a per-listener
connection limit. The limit accepts 1 through 128. Top-level `connections`
applies to `wire`, and `agent.connections` and `role_api.connections` each
default to 32. Wire TLS listeners advertise no ALPN protocols. Complete TLS
before sending the preface on a TLS connection.

### SQL-role listener

Set `role_api.wire` to the bind address. Omitting it leaves this listener
disabled. The optional `role_api.wire_tls` override requires a wire address
and defaults to `role_api.tls`. The `role_api.tls` key is required and accepts
`disabled` or `required`. Only `role_api.wire_tls` also accepts `optional`.

| Effective wire TLS mode | Accepted transport | Sign-in methods |
| --- | --- | --- |
| `disabled` | Plain TCP | SCRAM-SHA-256 or configured loopback trust |
| `required` | TLS over TCP | Mapped client certificate, SCRAM-SHA-256, `credential.password`, or configured loopback trust |
| `optional` | TLS and plain TCP on the same port | TLS connections use the `required` methods. Plain connections use the `disabled` methods. |

Wire TLS modes `required` and `optional` use `role_api.certificate` and
`role_api.private_key`. The TLS handshake can complete without a client
certificate. To enable certificate sign-in, configure `role_api.client_ca`
and map lowercase SHA-256 client certificate fingerprints to SQL roles in
`role_api.certificates`. The `role_api.databases` allowlist defaults to
`["postgres"]`, and `role_api.loopback_trust` defaults to `false`.

On an `optional` port the server peeks at the first byte without consuming
it. `0x16` selects a TLS handshake, and `G` (`0x47`) selects plaintext
preface parsing. Any other first byte, EOF before the first byte, or a sniff
timeout closes the connection without a wire error. The sniff and any TLS
handshake share one 10-second deadline. The plaintext preface read gets its
own 10-second timeout after sniffing. A complete but invalid preface is
handled as described below.

At startup, the server prints a warning to standard error for a
non-loopback bind in `disabled` or `optional` mode. It includes the bound
address and the effective mode and says that plaintext sessions cross the
network in clear.

## Preface

The client sends eight bytes before the first frame. The server's first output
is a frame.

| Offset | Bytes | Value |
| --- | --- | --- |
| 0 | 4 | ASCII `GGLN` |
| 4 | 2 | Big-endian `u16` major version `1` |
| 6 | 2 | Big-endian `u16` minor version `0` |

The v1.0 bytes are `47 47 4c 4e 00 01 00 00` in hexadecimal. The server accepts
any minor version under major `1` and uses the same framing. An unsupported
major gets `CONN_ERROR(0)` with `supported_major = [1]` before close. Wrong
magic also gets `CONN_ERROR(0)`, with an empty `supported_major`. The preface
and the complete `HELLO` frame each have a separate `IO_TIMEOUT` of 10 seconds.
An I/O failure, including a timeout or an incomplete preface followed by EOF,
can end the connection without an error frame.

## Frame header

Every frame has a 12-byte header followed by its body. The length counts the
eight header bytes after the length field plus the body.

| Offset | Type | Field | Meaning |
| --- | --- | --- | --- |
| 0 | Big-endian `u32` | `length` | `8 + body_len`. The body is at most `MAX_FRAME` (256 KiB). |
| 4 | `u8` | `kind` | One of the frame kinds below. |
| 5 | `u8` | `flags` | `MORE = 0x01`, `SKIPPED = 0x02`. All other bits are reserved. |
| 6 | Big-endian `u16` | `reserved` | Must be zero. |
| 8 | Big-endian `u32` | `request_id` | Client-chosen ID or the ID echoed by the server. Zero is reserved for connection-level errors. |
| 12 | Byte sequence | Body | `length - 8` bytes. |

A body may be empty when the kind permits it. The minimum `length` is `8`.
Validate the header, direction and body length before allocating or reading
based on the claimed size.

Client frames use zero flags. Server frames use zero flags except when
`REPLY` carries `MORE` or `SKIPPED`. `MORE` means another frame will finish or
abort the same reply. `SKIPPED` is valid only on an empty, terminal batch-entry
`REPLY`. The two bits cannot be combined.

## Frame kinds

| Value | Kind | Direction | Body |
| --- | --- | --- | --- |
| 1 | `HELLO` | Client to server | `OpenSessionRequest` |
| 2 | `AUTH` | Both | `ScramChallenge` from the server, `ScramContinue` from the client |
| 3 | `READY` | Server to client | `WireReady` |
| 4 | `EXECUTE` | Client to server | `ExecuteRequest` |
| 5 | `PREPARE` | Client to server | `WirePrepare` |
| 6 | `PREPARED` | Server to client | `WirePrepared` |
| 7 | `RUN` | Client to server | `WireRun` |
| 8 | `CLOSE_HANDLE` | Client to server | `WireCloseHandle` |
| 9 | `BATCH` | Client to server | `WireBatch` |
| 10 | `REPLY` | Server to client | Bytes of a `WireReply` with an `ExecuteReply`, or an empty body with `SKIPPED` |
| 11 | `CANCEL` | Client to server | `WireCancel` |
| 12 | `GOODBYE` | Client to server | Empty body |
| 13 | `CONN_ERROR` | Server to client | `WireConnError` |

## Sign-in

After the preface, send `HELLO` with zero flags and a nonzero ID. Wait for
`READY`, `AUTH` or a connection-level error before sending requests.

- On `wire`, present a certificate trusted by the listener and bound to an
  ordinary service identity. Send an empty `OpenSessionRequest`.
- On `agent.wire`, present a certificate trusted by the listener and bound in
  the agent certificate map. Send `OpenSessionRequest.task` to select one of
  its tasks. Omit `task` only when the binding has exactly one task. The agent
  TLS configuration allows the handshake to finish without a client
  certificate, but a valid `HELLO` on that connection gets `UNAUTHENTICATED`.
  A bearer credential alone cannot sign in.
- Both listeners use only the certificate-binding request form. A role,
  database or password/SCRAM credential in `HELLO` is invalid. Agent sessions
  require TLS. A plaintext connection to `agent.wire` closes at the TLS
  handshake without a wire error.

On `role_api.wire`, include `role` and `database` in `HELLO`. The role must
have `LOGIN`, and the database must be in `role_api.databases`. The role
listener refuses task-only roles and the agent `task` request form.

- For a mapped client certificate or configured loopback trust, omit
  `credential`. Certificate sign-in requires TLS and a fingerprint mapped
  to the requested role. The server checks the certificate mapping first,
  then applies loopback trust only if `role_api.loopback_trust` is enabled
  and the TCP peer IP is loopback. Either method can return `READY` directly.
- For password-based sign-in, use SCRAM-SHA-256 or send `credential.password`
  over TLS. On plain TCP, a valid `credential.password` request gets
  `UNAUTHENTICATED` before role lookup or password derivation, even for
  trusted loopback peers.

When the listener selects password authentication, start the SCRAM-SHA-256
exchange as follows.

1. Send `HELLO` with `OpenSessionRequest.role`, `database` and
   `credential.scram_start.client_first`.
2. Receive `AUTH` with `ScramChallenge.exchange` and `server_first`. The
   server echoes the ID of the client frame that produced the challenge.
3. Compute the SCRAM client proof and send `AUTH` with
   `ScramContinue.exchange` copied from the challenge and `client_final`
   containing the proof. Use a nonzero ID. The reference client chooses a new
   ID for this frame.
4. On success, `READY` echoes the last client handshake ID. Verify
   `WireReady.session.scram_server_final` before trusting the signed-in
   connection.

The server allows at most `MAX_AUTH_ROUNDS = 4` challenge/continuation pairs
and applies a fresh 10-second read timeout to each client `AUTH`. The
reference client's SCRAM implementation expects one challenge. An unexpected
handshake kind, zero ID, nonzero flags or another requested challenge after
four rounds is a fatal `protocol` error.

## READY

`WireReady` contains `session` (`OpenSessionReply`) and four limits.

| Field | Value on the current listeners | Meaning |
| --- | --- | --- |
| `max_in_flight` | 64 | Maximum admitted requests, counting batch entries individually |
| `max_frame` | 262,144 | Maximum bytes in a frame body |
| `max_handles` | 256 | Maximum live prepared handles per connection |
| `max_shapes` | 256 | Maximum committed shape definitions per connection |

`session` contains a nonempty `session_id` and the session lifetime fields
`idle_timeout_seconds`, `max_age_seconds` and `transaction_timeout_seconds`.
Native wire requests use the session already attached to the connection.
They carry no session token. `WireReady` does not advertise `MAX_RESPONSE`,
the pending-reply cap or the batch entry limit.

## Request IDs

Choose nonzero client `request_id` values. Keep each outstanding request's ID
unique, including batch entry IDs. The server releases a reserved ID when
it queues the terminal reply. Wait until you receive that reply before
reusing the ID. Handshake IDs belong to the handshake and can be reused after
`READY`.

The server processes reply-bearing requests and emits replies in request
order, including entries within a batch. A multipart response occupies that
position until the final frame. `CANCEL`, `GOODBYE` and the `BATCH` header
require a nonzero ID distinct from IDs already reserved by outstanding
requests. Those header IDs receive no replies and are not themselves reserved.
A fatal connection error uses ID zero and can end the ordered sequence early.

## Pipelining and backpressure

The server admits at most `max_in_flight` requests per connection, executes
one at a time and promotes deferred requests in arrival order. It can read
ahead with all slots occupied to find control frames such as `CANCEL` and
`GOODBYE`. It defers additional requests instead of admitting them immediately.
Lookahead is bounded by `max_in_flight` source frames and a shared byte budget
of 1 MiB plus `MAX_FRAME` for held request data and unread decoder bytes. A
batch with deferred entries occupies one source-frame position until its last
deferred entry is admitted. A client that fills the lookahead window can delay
control frames behind that traffic.

The server checks the pending-reply cap against queued frame bytes plus bytes
in the write in progress. At or above the cap it pauses request reads,
admission and the production of additional reply frames. A frame queued just
below the threshold can take the total over by up to one frame. A client that
never reads eventually stalls the socket write and the write/flush deadline
closes the connection. Keep draining replies while sending a pipeline,
particularly when either requests or replies are large.

## Replies and reassembly

An `EXECUTE`, `RUN` or `CLOSE_HANDLE` normally returns a `REPLY` for its ID.
`ExecuteReply.code` carries the execution outcome, including SQLSTATEs for
SQL failures. A `CONN_ERROR` carries a transport, request or session error.

To reassemble a reply, append each same-ID `REPLY` body in order. Continue
while `MORE` is set and decode the concatenated bytes as one `WireReply` when
the final `REPLY` clears `MORE`. A frame boundary can fall inside a protobuf
field. A spilled result can also contribute several serialized `WireReply`
messages to that sequence. Their singular message-valued `reply` fields
merge under protobuf decoding, appending repeated rows. Only the head
contains column metadata. Expand shapes after decoding the complete reply.

If `CONN_ERROR` arrives for that ID, discard all buffered bytes for the
reply. `CONN_ERROR(0)` or an interrupted connection abandons all incomplete
replies. `REPLY` with `SKIPPED` is a complete batch outcome with no protobuf
body.

`MAX_RESPONSE` bounds the dispatcher's materialized `ExecuteReply` encoding.
It is not a total byte limit on a streamed wire reply. Ordinary sessions can
stream spilled rows across multiple parts. Task sessions use the
non-streaming execution path. An oversized materialized result is replaced
by an `ExecuteReply` with code `08007`, since execution may already have
taken effect.

## Column shapes

Each connection starts with an empty table of ordered column-name lists. A
`WireShapeRef` holds a `position`, an optional `shape_id` and repeated
`columns`. The server visits these fixed positions in order and moves each
nonempty list from its reply field into a ref.

| Position | Field within `ExecuteReply` |
| --- | --- |
| 0 | `sql.columns` (`SqlResult.columns`) |
| 1 | `operation_outcome.columns` (`NativeOutcome.columns`) |

An absent or empty field produces no ref and does not shift subsequent
positions. Typed `NativeColumn` lists in transaction emits stay inline.

| List state | `shape_id` | `columns` | Client action |
| --- | --- | --- | --- |
| First occurrence with space in the table | Present, next ID starting at `0` | Full list | Append the definition to the table and restore the list at `position`. |
| Previously defined list | Present, existing ID | Empty | Look up the ID and restore the list. |
| New list at the cap | Absent | Full list | Restore the list without caching it. |

Test presence of `shape_id` explicitly because zero is a valid definition
or reference. Process refs in order, restoring each to its existing,
empty destination field. Reject an invalid or repeated position, an unknown
ID or a definition whose ID is not the next table index. A definition earlier
in the same reply can supply a later ref. Commit new definitions only after
accepting the whole reply. Definitions added during a failed expansion are
rolled back.

Committed shape IDs are never evicted or reused on the connection. If a
reply aborts during delivery, the server rolls back any tentative definitions
it added for that reply. An ID from an aborted reply can be assigned by a
later complete reply. Discard incomplete replies without applying their refs.

For example, two SQL replies with columns `["id", "name"]`, on a connection
with an empty shape table, carry these message fields. Rows are omitted here.

~~~text
First WireReply
  reply.sql.columns = []
  shapes = [{position: 0, shape_id: 0, columns: ["id", "name"]}]

Second WireReply
  reply.sql.columns = []
  shapes = [{position: 0, shape_id: 0, columns: []}]
~~~

After decoding the first reply, store `["id", "name"]` at index `0` and restore
`reply.sql.columns`. On the second reply, copy the same list from the table.

## Prepared handles

Send `PREPARE` with `WirePrepare.text` and optionally `language_version`.
On success, `PREPARED` returns `WirePrepared.handle` under the request's ID.
The server checks syntax and lowering and caches the parsed and lowered
native data operation or transaction block and its language version. The
cached form is catalog-free and captures no binding values, authority,
catalog resolution or execution plan. Task DDL and text outside the supported
data-operation subset are refused with `INVALID_ARGUMENT`.

Send `RUN` with `WireRun.handle` and `bindings`, and optionally `budget_ms`
and `operation_id`, which have the same meanings as for `EXECUTE`. Each run
validates parameter admission against its own bindings, applies the statement
budget and operation ID rules, checks live session identity and normal
execution admission, and resolves names, types and authorization against the
current catalog and session state. A handle does not preserve earlier
privileges or schema state.

Handles are connection-local slot numbers, starting at zero. The server
reuses the most recently closed slot before allocating a new one. At
`max_handles`, `PREPARE` returns `RESOURCE_EXHAUSTED` without eviction. `RUN`
and `CLOSE_HANDLE` on a missing or closed slot return `INVALID_ARGUMENT`.
Once a slot is reused, its number denotes the new operation.

Send `CLOSE_HANDLE` with `WireCloseHandle.handle`. Its success reply is a
`WireReply` with an `ExecuteReply` containing `version = 1` and `code = "ok"`,
an unspecified `transaction_state` and no shapes. Closing a handle only
changes the handle table. Lookups and table changes follow request order, so
a pipelined `RUN` uses the slot contents when that entry executes and a
following `CLOSE_HANDLE` cannot affect it retroactively.

## Batches

A `BATCH` contains one `WireBatch`, with a `mode` and 1 to
`MAX_BATCH_ENTRIES` `entries`. The frame body must still fit `max_frame`.
Each `WireBatchEntry` supplies its own `request_id` and exactly one decoded
`entry` variant, `execute` (`ExecuteRequest`) or `run` (`WireRun`).

| Mode | Value | Behavior |
| --- | --- | --- |
| `WIRE_BATCH_MODE_ALL` (`ALL`) | 0 | Execute each entry in order, continuing after individual errors while the connection is live. This is the protobuf default. |
| `WIRE_BATCH_MODE_STOP_ON_ERROR` (`STOP_ON_ERROR`) | 1 | Execute until the first failed entry, then send empty `REPLY` frames with `SKIPPED` for subsequent entries in this batch. |

A batch adds no transaction boundary. Each entry counts toward admission and
gets its own reply in entry order. The header ID is checked, but only entry
IDs are reserved and used in replies. All entry IDs must be nonzero, distinct
within the batch and clear of any existing outstanding ID. Use a separate
header ID for clarity. The decoder permits the header ID to equal an entry
ID because the header itself is never reserved.

In `STOP_ON_ERROR`, a per-entry `CONN_ERROR` or an unsuccessful
`ExecuteReply.code` marks the batch failed. The success codes are `ok` and
the legacy task success codes `preview`, `committed` and `absent`. A fatal
session error can close the connection before remaining entries get replies.
A structurally valid entry whose request is invalid gets the same per-request
error as a standalone request. A malformed batch envelope, unknown mode,
empty or oversized entry list, missing entry variant or invalid entry ID is
a fatal `protocol` error for the connection.

## CANCEL

Send `WireCancel.target_request_id` in a `CANCEL` frame with its own valid
header ID. The server does not send an acknowledgment. Cancellation is
best effort and applies to the target ID, including a batch entry.

| Target state when the server processes `CANCEL` | Outcome |
| --- | --- |
| Queued or deferred | The target does not run. It normally returns `CONN_ERROR` with `DEADLINE_EXCEEDED`, or `SKIPPED` if an earlier entry stopped its batch. |
| Running | The server signals cancellation and returns the execution path's outcome. A cancelled SQL statement can report SQLSTATE `57014` in `ExecuteReply.code`. |
| Reply still being produced | The server abandons the remaining reply, rolls back tentative shapes and normally terminates the target with `CONN_ERROR` containing `DEADLINE_EXCEEDED`. |
| Unknown or already finished, including a fully queued reply | The server ignores the cancellation. |

A cancel frame can sit behind request traffic or backpressure. Cancellation
of reply delivery gives no information about whether the operation committed.

## GOODBYE

Send `GOODBYE` with a valid nonzero ID, zero flags and an empty body. The
server sends no acknowledgment. When it processes the frame, it stops
admitting requests, drops queued work and any unfinished replies, and waits
for a running call to return without signalling cancellation. Drain any
replies you need before sending `GOODBYE`.

## CONN_ERROR

`WireConnError` contains `code`, `message` and repeated `supported_major`.
Only the unsupported-major error populates `supported_major`. `CONN_ERROR`
always has zero flags. Code spelling is case-sensitive.

| Code | `message` | Request ID and effect |
| --- | --- | --- |
| `protocol` | `invalid wire preface`, `unsupported wire version`, `invalid handshake frame`, `unexpected handshake frame`, `too many authentication rounds`, `invalid frame header`, `invalid client frame ID or flags`, `invalid BATCH frame`, `invalid CANCEL frame`, `unexpected client frame` | `0`, fatal. Covers preface, framing, handshake and control-frame violations. |
| `INVALID_ARGUMENT` | `invalid request` | `0` and fatal for invalid sign-in arguments or a SCRAM start when a mapped certificate or loopback trust was selected. Echoed ID for a malformed or refused request or invalid handle. |
| `UNAUTHENTICATED` | `authentication required` | `0` for failed sign-in (including bad passwords or SCRAM proofs, passwords on plain TCP, or a refused role or database) or idle identity loss. Echoed ID for identity loss found while processing a request. Fatal. |
| `SESSION_NOT_FOUND` | `session not found` | `0` for expiry detected while idle, or echoed ID for a missing or expired session detected during a request. Fatal. |
| `SESSION_BUSY` | `session busy` | Echoed ID if the session lease is already held. |
| `RESOURCE_EXHAUSTED` | `resource limit` | `0` and fatal if sign-in runs out of session slots or pending SCRAM exchange slots. Echoed ID for an execution/admission limit, a full handle table or a spill resource error. |
| `DEADLINE_EXCEEDED` | `deadline exceeded` | Echoed ID for request timeout or cancellation reported at the transport layer. |
| `UNAVAILABLE` | `service unavailable` | `0` and fatal when sign-in cannot open a session, or echoed ID for unavailable execution or response delivery. |
| `INTERNAL` | `internal error; outcome may be ambiguous` | `0` and fatal for an internal sign-in failure, or echoed ID for an internal request or response failure. The session may have been invalidated. |

Echoed-ID failures are per-request unless marked fatal or accompanied by a
failed identity recheck. The server drains the fatal echoed-ID error and
then closes. A worker can invalidate the session while reporting `INTERNAL`,
so the next session check can report session loss. `UNSUPPORTED_MEDIA_TYPE`
belongs to other transports and has no emitter on native wire. `PayloadTooLarge`
shares the `RESOURCE_EXHAUSTED` API code, but oversized materialized execution
results follow the `08007` reply path described above.

## Connection close and session expiry

The server checks for expiry while the connection is idle, once a second
or when bindings change. It also checks liveness for request execution and
before releasing reply frames. Ordinary and agent sessions expire after 60
seconds idle, 3,600 seconds of age or 60 seconds in an open transaction.
Role sessions also have a 60-second transaction timeout. Their idle timeout
defaults to 60 seconds and their maximum age defaults to 3,600 seconds when
`role_api.tls` is `required` or 900 seconds when it is `disabled`. The
`role_api.idle_timeout_seconds` and `role_api.max_age_seconds` settings override
those role session defaults. `READY` reports the actual limits. An idle
session error uses ID zero. A session failure found during request handling
uses the request ID and closes the connection.

Closing the connection closes its session and rolls back any open
transaction. EOF, a socket error or a fatal protocol error signals
cancellation to a running call and waits for it to return. Admission refusal
before sign-in, TLS failure and some I/O failures can close the connection
without a wire error. A fatal protocol or idle error can discard unwritten
replies. After close, reconnect and sign in again. The new connection starts
with fresh request tracking, handles and shapes. A lost reply does not
establish that an already-started operation had no effect.

## Example exchange

This ordinary TLS connection selects from an existing `shipments` table.
The notation shows message fields, omitting unrelated reply fields. The client
pipelines the two runs and the batch once it has the handle.

~~~text
C -> S   TLS handshake with a bound client certificate
C -> S   GGLN 00 01 00 00
C -> S   HELLO(1) {}
S -> C   READY(1) {session: {...}, max_in_flight: 64, max_frame: 262144,
                   max_handles: 256, max_shapes: 256}

C -> S   PREPARE(2) {text: "shipments where id=$id select id"}
S -> C   PREPARED(2) {handle: 0}
C -> S   RUN(3) {handle: 0, bindings: [{name: "id", value: {int_value: 1}}]}
C -> S   RUN(4) {handle: 0, bindings: [{name: "id", value: {int_value: 2}}]}
C -> S   BATCH(5) {mode: WIRE_BATCH_MODE_ALL, entries: [
           {request_id: 6, execute: {ping: {}}},
           {request_id: 7, run: {handle: 0,
             bindings: [{name: "id", value: {int_value: 3}}]}}]}
S -> C   REPLY(3) {reply: {code: "ok", operation_outcome: {columns: [], ...}},
                   shapes: [{position: 1, shape_id: 0, columns: ["id"]}]}
S -> C   REPLY(4) {reply: {code: "ok", operation_outcome: {columns: [], ...}},
                   shapes: [{position: 1, shape_id: 0, columns: []}]}
S -> C   REPLY(6) {reply: {code: "ok"}}
S -> C   REPLY(7) {reply: {code: "ok", operation_outcome: {columns: [], ...}},
                   shapes: [{position: 1, shape_id: 0, columns: []}]}
C -> S   GOODBYE(8) <empty>
~~~

## Limits summary

| Limit | Value | Where it applies |
| --- | --- | --- |
| `MAX_FRAME` | 256 KiB (262,144 bytes) | Frame body, advertised as `max_frame` |
| `MAX_RESPONSE` | 8 MiB (8,388,608 bytes) | Materialized `ExecuteReply` encoding |
| `max_in_flight` | 64 | Admitted requests per connection, including batch entries |
| Pending reply threshold | `2 × MAX_RESPONSE` (16 MiB) | Queued and writing frame bytes, with up to one frame of overshoot |
| Lookahead | `max_in_flight` source frames while full, with a 1 MiB + `MAX_FRAME` byte budget | Deferred input and unread bytes |
| `max_handles` | 256 | Live handle slots per connection |
| `max_shapes` | 256 | Committed shape IDs per connection |
| `MAX_BATCH_ENTRIES` | 1,024 | Entries per batch, subject to the frame body limit |
| `MAX_AUTH_ROUNDS` | 4 | Challenge/continuation pairs in the server handshake |
| `IO_TIMEOUT` | 10 seconds | TLS handshake (shared with sniffing on an optional port). Separate deadlines apply to the preface read, each handshake frame read and each write/flush. |
| `budget_ms` | 10,000 ms default and maximum | Execution budget from frame receipt, including time in the queue |
| Session lifetime | 60 seconds idle, 3,600 seconds total, 60 seconds in a transaction | Current ordinary and agent session defaults |

## Known gaps

- The connection-loop plan describes stopping reads at the admitted request
  limit. The server uses bounded lookahead at that limit so it can see control
  frames while requests are deferred.
- The shape traversal plan included transaction column lists. Those lists use
  typed `NativeColumn` messages and stay inline. The implementation has two
  fixed positions for column-name lists.
