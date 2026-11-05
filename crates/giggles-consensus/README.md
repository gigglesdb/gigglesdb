# bounded durable consensus prototype

[GD-0014](https://app.archdocs.io/design-docs/69d83cfc-6a14-4531-a748-4f4f52fbba1b)
implements a single fixed range with full-data voters **1, 2, 3** using pinned
TiKV `raft = 0.7.0`. This is runnable local implementation work. It has no cloud,
production, HA release, general SQL compatibility or superiority qualification.

## Ownership and entry points

Each `giggles-node` process owns one redb file, one `RawNode<Store>` and one actor.
The redb writer lock excludes other processes. Cloned Store handles share a mutex
and one database owner. No embedded Admin handle or independent authoritative core
database exists inside a consensus process. The P2 server is unchanged and cannot
open this consensus file as a local database.

The network listener accepts **Raft peer traffic only**. All peers authenticate
through maintained rustls WebPKI chain/signature/time verification, `localhost`
server-name verification and an exact SHA256 leaf fingerprint mapped to node ID.
A CA-valid certificate without a membership binding has no authority. A peer may
not claim another sender, send local messages, forward proposals or alter
membership. Dynamic membership, learners, remote Admin and remote SQL are rejected
or have no listener. Node config and keys are trusted local provisioning.

The bounded native command interface is a trusted **stdin control interface**.
`Command::Task` binds explicit task/principal/trusted interval and uses existing
current-authority kernel paths. It is not an authenticated external task-client
protocol. `Command::Admin` is for the trusted process owner only. The current
native implementation does not accept a remote task principal, client-chosen
time, or trusted Admin over TLS. Network task sessions/P2 pgwire routing are exact
remaining P3 work, rather than an implicit claim that every P2 path is replicated.

## State and durability

`giggles_core::replica::Image` holds every canonical kernel namespace: application
catalog/data/indexes, task grants/revocations, approval dependencies and consumed
state, mutation budgets, dedupe and receipts, read/disclosure counters, logical
time and identity metadata. Evaluation opens transient **in-memory redb**, invokes
the existing kernel policy/SQL transitions, captures the resulting image and drops
every handle. It has no file/network/time/random side effects. Identity is explicit
bootstrap input; time intervals and request IDs are explicit logged inputs.

The outer consensus store is the only durable source. A redb Immediate transaction
publishes hard state, log records, exact kernel image and applied index/term.
Ready entries/hard state/snapshots are persisted before outbound messages; the
LightReady commit/application boundary is also persisted before release.
Application success is emitted only after indexed state is durable. Restart uses
the durable applied index to avoid double apply, replaying the committed tail.
Truncation and snapshot compaction replace logical map records; no filesystem
deletion is performed. Partial/uncommitted paths are not reinitialized on open.

Snapshots carry format, domain, fixed membership, index, term, exact image,
length bounds and SHA256 checksum. Metadata is checked against its checksummed
body and committed local terms; stale/conflicting/wrong-domain/corrupt snapshots
reject before durable replacement. Snapshot installation and index publication
are one transaction. Checksums detect corruption; they are not authentication
against a privileged disk owner. mTLS authenticates peer transport.
Every peer frame also carries a versioned cluster-domain envelope, so reuse of
node certificates across a wrong domain cannot silently merge Raft traffic.

Current diagnostic reads use safe library ReadIndex and explicitly require local
applied >= barrier. Task preview/status reserve resources and therefore use a
committed replicated command: the log decision supplies their quorum serialization
point and applies current authority plus accounting before response. Isolated
leaders cannot produce a new successful current read or task decision.

Retry a task with its **original OperationId and canonical request**; same payload
recovers the original receipt, different payload rejects. Each successful output
attempt consumes a new disclosure reservation, including a lost response; failed
delivery is never refunded. Revocation/expiry still gates remote-style task output.
Proposal contexts only correlate output; they are not application dedupe tokens.
Administrative SQL/provisioning commands have no general retry guarantee.

Cancellation before proposal may prevent submission. Once submitted, timeout,
disconnect or process death means **indeterminate**, never rollback. Resolve the
original task operation through a quorum and current authority. This slice has no
external cancellation protocol.

## Running a real three-process demonstration

Run from this crate:

```powershell
cargo test --test process real_three_process_mtls_failover_snapshot_and_lost_ack -- --nocapture
cargo test --test process subprocess_crash_before_and_after_atomic_image_index_commit -- --nocapture
```

These launch the actual `giggles-node` executable, generate a CA and three distinct
node certificates, retain configs/keys/databases/invocations/stdout/stderr/checker
outputs under unique `artifacts/real-mtls-*` directories, transfer a snapshot to a
restarted voter, kill the leader, retry the original operation and enforce budgets.
Test-only keys are local disposable fixtures; directories are retained.

Manual startup uses retained config examples:

```powershell
cargo run --bin giggles-node -- K:/path/to/node-1.json
```

Start one process for each fixed voter. Each config names its own database path,
listen address, leaf/key/CA files, domain and all three `{address,fingerprint}` peer
bindings. Every node must use the same domain, voters and fingerprints. Database
creation is explicit create-new; existing files use open and validate identity.
Stdin accepts newline-delimited JSON; output is newline-delimited JSON:

```json
{"op":"diagnostics"}
{"op":"read","id":400}
{"op":"checkpoint"}
{"op":"propose","id":401,"command":{"Admin":[123,34,99,111,109,109,97,110,100,34,58,34,99,97,116,97,108,111,103,34,125]}}
```

The final example encodes trusted `{"command":"catalog"}` bytes. Programmatic
controllers should serialize `Command`, as the integration tests do. Output from
each node includes local apply events, including follower applies; a controller
must correlate against the node and proposal context it submitted. Remote clients
must not be given stdin, process-owner permissions or these node credentials.

## Bounds and limits

Commands <=1MiB serialized, proposal context <=64 bytes, core image <=16MiB,
whole durable envelope <=64MiB, peer frame <=24MiB checked before allocation.
Transport has 8 inbound sessions, 8 concurrent outbound sessions, inbound queue64,
outbound queue128 and a two-second connection/handshake/frame deadline. Overload
drops traffic and Raft retries; it grants no additional authority. Read contexts
are capped at32. Raft has 8 inflight messages/peer and 8MiB uncommitted log cap.
Application commands and durable images are copied; performance is a prototype
limit, and disk growth has no automatic retention policy. Checkpointing is manual.
The transport `ok` is a queue receipt, **not a Raft replication acknowledgment**;
only later library responses released after persistence count toward consensus.

No dynamic changes, multi-range work, snapshot streaming/resumption, process
controller retry API, peer certificate hot rotation/CRL, external task-client listener,
automatic checkpoint policy or operational
deployment is implemented. An out-of-space state-image/log error fails closed and
requires operator recovery; graceful capacity admission needs more P3 work.

## Verification

`./verify.ps1 -ArtifactDirectory artifacts/UNIQUE-NAME` runs fmt, Clippy with
warnings denied, debug/release tests and doctests, retaining exact commands/exits,
outputs and source hashes. Tests include the actual library Storage API, durable
votes/restart, seed-driven drops/duplicates/reordering, independent numeric task
oracle and serialized history checker, old isolated leader, no majority, lost ACK,
conflicting IDs, approvals/revocation, catalog image round trips, snapshot+tail,
snapshot rejection and real process crashes around atomic image/index publication.
Debug/release repeat schedules; they are not independent campaigns. The history
checker checks observed sequential histories, not arbitrary concurrent schedules
or a proof. This is not the upstream Raft test suite or release qualification.

`Store::simulated` uses the same transaction adapter on in-memory redb; explicit
durable model bytes survive simulated restart. Injected errors before/after commit
test atomic publication and ambiguous-response fencing. This storage simulation
models atomic transactions and does not simulate device flush internals. A separate
independent exhaustive checker searches serial orders for six overlapping proposals
with identical/conflicting IDs and budget2 (seeds40..49). It covers that bounded
history class, not arbitrary concurrent SQL.


## internal authority/time lifecycle

[Design documentation](https://app.archdocs.io/design-docs/d4bf94d5-2625-414d-8e76-b1b9b9984ceb) documents the new
replicated credential reload epochs, automatic trusted leader OS clock, committed
watermark/time-term fences and opaque authenticated decision/held-output release
APIs used by the actual trusted stdin runtime. No external task listener was added.
The historical explicit principal/time controls above remain trusted process-owner
only; future listeners must use the new authority API and must not dispatch them.

Snapshots now use body version2 and an extended durable prototype schema. No old
store upgrade or rolling compatibility is implemented. Time correctness depends
on explicit calibrated host/skew/pause assumptions; asynchronous consensus does
not enforce the five-second sample-to-decision bound. See the design documentation's precise
expiry/release semantics and remaining physical delivery-pause limitation.
