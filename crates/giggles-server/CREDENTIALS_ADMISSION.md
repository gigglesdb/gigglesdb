# Trusted credential lifecycle and shared admission

GD13: https://app.archdocs.io/design-docs/9beab8f8-a971-4d68-9d26-c27fedf37382

## Run and reload

Start the existing executable with its JSON configuration. Add these optional
fields (old configurations still start with the documented defaults):

```json
{
  "bindings_file": "K:/trusted/giggles/bindings.json",
  "service_limits": {"sessions": 16, "burst": 4096, "per_second": 4096}
}
```

The file contains the existing JSON array of Binding records. When configured,
this file supplies startup bindings instead of the inline `bindings` array (the
existing inline field remains required for configuration compatibility). Type
`reload` followed by Enter on the process's trusted local stdin to read and
validate that same file and atomically replace bindings. Success prints
`bindings reloaded epoch=N`; rejection prints `reload rejected; prior bindings
retained` on stderr. Type `shutdown` or press Ctrl-C to stop. EOF disables stdin
control and leaves Ctrl-C available. There is no network control or provisioning
endpoint. Existing offline Admin/issuer/approver workflows remain separate.

Both startup config and bindings files are read through a 1MiB+1 bounded reader;
files over 1MiB, malformed JSON and unknown fields reject. Commands retain at most
16 bytes; overlong lines are discarded until newline. One reload runs at a time
on a blocking thread. Validate all records before swap: at most1024 bindings,
canonical lowercase64-hex SHA256 fingerprints, unique fingerprints, current
kernel domain, nonempty application sets of at most128 table names each1..128bytes.
Invalid configs, conflicting authority or exhausted epoch arithmetic leave the
entire prior registry unchanged. Empty bindings intentionally revoke everyone.
Only bindings are hot-reloaded; listener addresses, service limits, clock config,
server certificate/private key and client CA require process restart. Editing the
main config while running does not change these settings.

## Authority and synchronization

Exact identical roles may share certificates during rotation: same application
domain and *identical* table set, or same task/domain/principal. Application roles
have no separate username/principal in this preview. An identical table set
explicitly designates the same authority, not a distinct isolated tenant. Any
nonidentical overlapping table sets reject. To rotate, add a new CA-issued leaf
fingerprint with the identical role, reload, switch clients, then remove the old
fingerprint and reload. Old active sessions then fail closed on both listeners.

Each new/changed binding gets a checked monotonic u64 identity epoch. Unchanged
exact records keep their epoch. Removing then restoring a fingerprint always
allocates a new epoch: old sessions, SQL staging, prepared handles, cached portal
pages and backend keys never revive. No tombstone map grows with reload count.

Synchronous execution takes a concurrent authority read lease, checks current
fingerprint/role/epoch, and retains that lease through the actual kernel decision,
response construction and task disclosure reservation. Reload takes the exclusive
writer lease. Thus a completed reload fences later execution; a decision already
holding a lease may commit/reserve before reload completes. The kernel still
checks current task/domain/principal, approval, revoke/expiry/time and durable
accounting. Native and all pgwire frames, including cached application pages,
re-enter this lease. Explicit COMMIT requires a fresh lease, even when BEGIN and
staging preceded revocation. No mutex is held across socket await.

A final current-binding check immediately before starting each response send
withholds responses invalidated since construction. This check is the output
release authorization point: a reload ordered after it cannot recall bytes
already authorized for an in-flight bounded write. Already delivered bytes cannot
be recalled. Revocation cannot undo a committed decision, refund reserved task
output or imply rollback when a socket closes. Recover original task operations
through trusted forensic status or a currently authorized rotated task identity.
Application SQL has no durable operation envelope: reconcile ambiguous writes.

CancelRequest checks current binding and backend epoch under the credential
registry lock through signaling; it does not wait for authority leases or
blocking32. Removed/changed/readded identities cannot use old backend keys. Same
unchanged identity retains cancellation across harmless reloads. Cancellation
still cannot undo a sealed decision. Backend registry historical PIDs remain
capped65536 per Service lifetime, live entries at most that number; new backend
registration fails closed at exhaustion until restart. Epochs are process-local;
backend registries do not survive process restart.

Rustls WebPkiClientVerifier performs mandatory chain/signature/time verification
at TLS handshake. Wrong-CA, expired and unbound clients are tested. Established
TLS connections are governed by live binding epochs; certificate validity is not
reverified cryptographically per frame and certificate expiry/CA changes alone
do not revoke an established session. Remove its binding to revoke it. Server/CA
hot rotation and external PKI/CRL/OCSP or security-audit qualification are absent.

## Shared service limits

| Limit | Default | Accepted config / behavior |
| --- | --- | --- |
| Aggregate sessions |128| `service_limits.sessions`1..128; both listeners and detached workers share permits |
| Request burst |4096| `burst`1..1000000 tokens |
| Refill |4096/second| `per_second`1..1000000, monotonic Instant, fractional nanosecond credit |
| Per listener socket cap |existing `connections`|1..128, additional cap within aggregate budget |
| Submitted blocking work |32|Existing fixed shared runner; permit remains until real completion |
| Binding registry |1024 records|Whole reload rejects above cap |
| Backend PID history |65536|No PID reuse within Service; exhaustion requires restart |

One token is charged on each accepted socket *before* TLS/SSLRequest or binding
lookup, and another on each native frame or pg startup/post-startup frame before
processing, including malformed/denied requests and protocol controls. Socket
attempts rejected by aggregate/per-listener caps still spend the socket token.
Oversized/incomplete frames close within existing bounds/timeouts; repeated
reconnect attempts still spend socket tokens. Rate or socket saturation closes the
connection silently with no SQL/task execution. No per-IP/identity state exists;
reconnect, switching listeners and a new certificate cannot reset service caps.
A new Service/process starts a fresh transient bucket; durable GD12 task caps
remain in the kernel across restart. Trusted direct in-process Session APIs are
not a network bypass boundary and do not spend network tokens.

CancelRequest bypasses blocking work but shares the socket and token caps. At
session or rate saturation a cancellation connection can be rejected; this is
explicitly tested and no reserved cancellation capacity is promised. Existing
native Deadline also remains available on an admitted request. No fairness,
per-tenant latency or adversarial availability guarantee is made. Refill uses
checked wide arithmetic and clamps at burst; clock regression grants no token
and never moves the refill anchor backward. There is no wall-clock token expiry.

## Finite capacity worksheet

Core persisted State is at most16MiB encoded. Eight staged SQL snapshots share
one database cap, at most8x16=128MiB in encoded-state equivalents; decoded Rust
objects, candidate copies, storage buffers and allocator overhead add memory.
Core SQL bounds remain1024rows/1MiB raw result values,64KiB row,64columns,
256parameters and128tables. A result can expand when serialized (control-character
TEXT up to6x), and an attempted bounded result exists transiently before cache
admission. Cached application results are now admitted against the8MiB aggregate
*before* assigning cache or emitting any DataRow, including suspended pages.

For S admitted sessions, queued input frames are at most Sx256KiB (32MiB at128),
outputs at most Sx8MiB, encoded retained portal parameters at most Sx8MiB, cached
results at most Sx8MiB. Each session has32 statements and32 cloned portal plans;
64x64KiB gives4MiB of SQL-source equivalents per session. Summing those encoded
reservations/source equivalents gives Sx28.25MiB, or3616MiB at128, plus shared
staged-state equivalents and bounded transient work in at most32 workers. These
are conservative capacity dimensions, *not* a strict measured RSS bound: decoded
AST/rows/parameter objects, TLS/TCP buffers, allocator and redb/cache/candidate
copies differ from serialized sizes. Set a smaller sessions cap for a constrained
local host. No low-memory, physical I/O, CPU, latency or overload qualification is
claimed. Idle sessions time out under existing10s IO timeout; their task caps do
not reset. Detached workers retain both blocking and aggregate session admission
until they actually finish, even after socket abort/shutdown.

## Optional HTTP and gRPC interfaces

Both API listeners share the aggregate physical-socket pool, rate bucket and
blocking32 runner above with native and PostgreSQL. A further Service-owned pool
admits at most128 API calls before buffering/decoding, including malformed routes.
There is no reserved Close capacity. Logical API sessions are separate:32 by
default, configurable1..128, with no socket held while idle. Their real SQL
sessions still share the existing database-wide eight staged snapshots.

Typed native operations and atomic blocks use these same call, worker and
snapshot pools. Native and SQL work in one API session share one transaction;
a block joining that transaction does not allocate an independent outer
snapshot. Admission precedes AST decoding. Native requests additionally cap
expression depth at24 edges, aggregate AST nodes at4096, bindings at256/64KiB,
serialized core input and resolved value occurrences at64KiB each, per-field
lists at64, insert/complete-input rows at1024, and block steps plus one item when
emit is present at64; emitted values are separately capped at64. The core's1MiB
local/emitted/mutation/result bounds count raw value bytes,
not Rust object overhead; many empty strings or repeated emitted rows can still
expand substantially. The existing8MiB encoded-reply guard applies to all native
outcomes and prevents partial success output. These finite bounds add no claim
of a measured process-memory or execution-time guarantee.

At128 admitted API calls, bounded request payloads account for32MiB and encoded
responses for1024MiB. One worker may transiently hold native, typed and serialized
response representations. Each HTTP/2 connection has at most16 streams, a64KiB
stream receive window,1MiB connection receive window,16KiB decoded header list,
4KiB HPACK table and64KiB send-buffer target. Output is chunked at16KiB and carries
its session/call/socket ownership through the protocol send queue. These values
are conservative capacity dimensions; they do not establish a process RSS bound.
The pinned HTTP/2 parser can retain112KiB of encoded partial-header frames and
reserve approximately180KiB for Huffman expansion, plus allocator overhead,
before rejecting a decoded header list over16KiB. Reset history is50entries for
one second. The accepted header-size cap is not a peak parser-memory bound.

API session idle/absolute/transaction expiry is60/3600/60seconds. PING-only HTTP/2
traffic does not refresh the separate10second application I/O watchdog. Binding
epochs fence handles across protocol changes and new TLS connections. Close,
expiry and shutdown discard staging but cannot undo a sealed decision. See
[API.md](API.md) for the exact wire contract, deadlines and unknown-outcome rules.
