# giggles-outbox

A bounded trusted local outbox backed by real `giggles-core` SQL transactions.
Application mutations and enqueue commit together. The library and CLI never
send notifications, email, webhooks, or other external effects.

```powershell
$env:CARGO_TARGET_DIR='K:/Projects/gigglesdb/crates/giggles-outbox/target-outbox'
cargo run --manifest-path crates/giggles-outbox/Cargo.toml -j2 --example orders -- C:/scratch/fresh-orders-db
```

The example creates an order and its notification in one transaction, reopens the
database, then queries both. Supply a fresh path; creation refuses an existing DB.

## Transaction API

```no_run
use giggles_outbox::*;
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let outbox = Outbox::create("fresh-local-db", Limits::default())?;
let mut tx = outbox.begin()?;
tx.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, status TEXT NOT NULL)", &[], None)?;
tx.execute("INSERT INTO orders VALUES (1,'accepted')", &[], None)?;
let receipt = tx.enqueue(EnqueueIntent {
    stream: "shop", effect_id: "order-1-accepted",
    destination_kind: "notification", payload: b"order 1 accepted",
})?;
match tx.commit()? {
    CommitOutcome::Committed => println!("queued: {}", receipt.effect_id),
    CommitOutcome::Unknown => { /* Drop handle, reopen, inspect application AND effect identity. */ }
}
# Ok(()) }
```

`OutboxTransaction` owns one explicit core transaction. Drop or `rollback()`
discards it. Every execute/enqueue error poisons the whole transaction. There is
no automatic retry. Concurrent transactions can return a definite `Conflict`.
Mutation SQL is parsed by core `Prepared`; only CREATE TABLE, INSERT, UPDATE, and
DELETE with an application target are allowed. Transaction controls are refused.
The `outbox_internal_` namespace is reserved by this wrapper. SELECT/EXPLAIN are
refused because core's first-table metadata cannot authorize every join source;
use `scan_application(Scan)` for bounded single-table application reads.

The wrapper privately owns Admin. Trusted raw Admin or filesystem owners can
bypass it. This is module protection, not enginewide SQL privileges or tenant
security. Open validates the exact internal table shape, singleton state,
canonical encoding, version, identity/digest and lifecycle invariants; it never
silently repairs malformed data.

## Immutable identities and unknown commits

Persist a stable `(stream, effect_id)` before submitting. Payloads are opaque
canonical bytes; equivalent JSON with different byte formatting is different.
The SHA256 digest includes domain separation and length-prefixed stream, effect
ID, destination kind and payload. Same ID with identical bytes returns the same
permanent enqueue receipt, including after acknowledgment. Different payload or
destination conflicts. Batch enqueue is atomic, including duplicate IDs inside
the batch. There is no automatic expiration, GC, eviction, or reuse of IDs.

**Effect dedupe does not make arbitrary application SQL exactly once.** A retry
that queues an existing effect still executes the application statements supplied
in that new transaction. Use an immutable unique application operation key or
caller-idempotent compare-and-set with a previously read core row version
(`execute(..., Some(version))`). After `Unknown`, discard the fenced handle,
reopen, inspect both the business operation key/version and outbox status, then
explicitly decide what is safe to do. Do not replay SQL or use a fresh effect ID
merely because a reply was lost. A status read never replays application work.
An enqueue receipt returned while staging is provisional. Only a paired
`Committed` result or a successful later status read establishes database
acceptance. Downstream acknowledgments are separate evidence.

## Claims and external delivery

`claim(stream, owner, time, ttl, limit)` is an explicit trusted payload disclosure.
It returns `ClaimOutcome::Committed(Vec<Delivery>)` or `Unknown` without payload.
The receiver runs outside this library. After receiver success call `ack` with
the token; `renew` and `release` are separate durable operations. Tokens include
stream, effect ID, bounded owner, and monotonic generation. Reclaim increments
generation and attempts. A stale generation/owner/stream cannot ack, release or
renew. Ack is terminal; even repeated ack with the old token is stale. Renew
never shortens a lease. Ready and provably expired claims are selected in
lexicographic effect-ID order within a stream; this is not a delivery ordering
guarantee. Every claim scans at most 128 retained entries, including acked IDs.

The caller certifies `TrustedTime { lower, upper }` in a **shared monotonic epoch
that remains valid across process restart**. The library does not manufacture
this certificate from wall-clock time. The interval must be ordered, lower must
not regress below the persisted watermark, and uncertainty must not exceed the
configured bound. TTL must exceed interval uncertainty, remain within max TTL,
and fit checked `u64` addition. New deadline is `upper + ttl`. Expiry is proven
only when `lower >= deadline`; a live token operation requires `upper < deadline`.
Uncertainty overlapping expiry refuses acknowledgment/renewal/release. Checked
generation, attempt, and deadline overflow fail closed without partial writes.

Lease expiry never proves an external effect failed. Crash after receiver success
and before ack causes redelivery of the same stable effect ID. Use receiver-side
durable idempotency keyed by that ID or reconcile manually. A non-idempotent
receiver can execute the effect twice. Restoring an older database timeline can
resurrect delivery work: fence the restored timeline and reconcile externally.

## Bounds

Default and hard caps: 128 retained effect IDs; 16KiB total retained payload;
4096 bytes per payload; 32 effects per enqueue/claim batch; 64 application SQL
statements and 64KiB cumulative SQL/parameter bytes per transaction; 48KiB encoded
outbox state. Identifiers are 1–64 ASCII alphanumeric or `_ - . :`. Limits are
persisted and immutable on reopen. Acknowledged entries still consume quotas.
Admission failure rolls back all staged application work. Capacity is finite:
there is no implicit retention policy that weakens dedupe. Core also imposes its
own row, result, database, session and resource caps.

The implementation uses one bounded serialized state row and core's optimistic
whole-database conflict detection. It is intended for a small local workflow,
with contention and permanent capacity visible to the caller. It provides no
general CDC, cross-range ordering, cloud, security, performance or production
qualification.

## Trusted local JSON CLI

Run `giggles-outbox DATABASE [REQUEST_FILE|-]`, or `cargo run --manifest-path
crates/giggles-outbox/Cargo.toml -j2 -- DATABASE [REQUEST_FILE|-]`. Omit the final
argument to read stdin. Exactly one JSON request, at most 64KiB, is accepted.
Unknown fields are refused. Output is JSON; exit 0 means command processed,
exit 2 is a sanitized typed error, exit 3 is stdout failure. Inspect `commit` or
`claim` for `Unknown` even on exit 0. Status/reconcile omit payload. Only claim
returns payload bytes. No command sends anything externally.

```json
{"command":"create"}
{"command":"enqueue","stream":"shop","effect_id":"order-1","destination_kind":"notification","payload":"accepted","application":[{"sql":"INSERT INTO orders VALUES (1,'accepted')"}]}
{"command":"status","stream":"shop","effect_id":"order-1"}
{"command":"claim","stream":"shop","owner":"worker-1","now":{"lower":100,"upper":101},"ttl":30,"limit":1}
{"command":"ack","token":{"stream":"shop","effect_id":"order-1","owner":"worker-1","generation":1},"now":{"lower":102,"upper":103}}
{"command":"reconcile"}
```

Each line above is a separate invocation. Create application tables first in a
transaction or include CREATE TABLE in a first enqueue's `application` list.
Statements accept `params` using core's serde Value representation and optional
`expected_version`. CLI payload is a UTF-8 string, converted to exact bytes.

Tests use actual core persistence, independent randomized lifecycle histories,
concurrent claim races, abrupt commit/receiver subprocess termination and a local
durable fake receiver, with idempotent and non-idempotent controls. Scratch
fixtures are uniquely named and retained in the operating-system temp directory.
