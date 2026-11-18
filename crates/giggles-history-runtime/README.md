# giggles-history-runtime

An archived capture harness for contributors studying the former three-node consensus prototype.

**Status:** Retired: no runnable target binary remains; server replication histories now use giggles-history.

Part of [GigglesDB](../../README.md).

> **Retired.** The former replication prototype and its process test were removed.
> Server histories are now captured by `crates/giggles-server/tests/replication_history.rs`
> and checked with `giggles-history`; see [replication release notes](../../docs/release-notes/R4.md).
> The library remains for reference and tooling reuse.

## Archived internals

The following describes the former harness, not a runnable CLI on main.

The public Rust entry point is
`run(binary: &Path, artifact_parent: &Path, seed: u64) -> Result<Value, Box<dyn Error + Send + Sync>>`.
The supplied executable is trusted. The harness owns only its spawned child
handles, stops them using `Child::kill`/`wait`, and joins output collectors.
Readiness requires observed ready output and current leader authority diagnostics;
there are no fixed sleeps used to establish state. All reads require the node's
committed read barrier. Response deadlines bound execution, never establish abort.

## Scope and faithful mapping

This is **trusted owner stdin transport**, not an external task-client security
test. Consensus peer traffic uses real mTLS sockets. There is one consensus
domain; no cross-range or distributed SQL claim is made.

Bootstrap eight known tenant42 rows, task1/principal2 and ample task resources.
Take a complete barrier inspection before any modeled request. The modeled
keyspace is the complete status projection of tenant42, rows0 through7; absence
is absence. Failed/Queued/Succeeded map to integers0/1/2. The decoder rejects
other tenants, keys, statuses, duplicate keys, unexpected fixed row fields or
missing barrier evidence. Versions, receipts, accounting, authority and other
namespaces are retained in raw replies but are **outside the checker model**.

Only explicit-key task status assignments and a separate trusted seed reset
are modeled as atomic point/multirow writes. All transitions needed by the
campaign are authorized, rows exist, and keys are unique. This restriction
makes assignment semantics faithful even when a row already has the desired
status. Complete barrier inspection maps to a full `[0,8)` scan. No partial,
filtered or paginated scans are accepted. No generic SQL, predicates, deletes,
transfers, value increments or cross-range operations are translated.

The harness constructs write operations from the submitted request, before
receiving the reply. It checks only observed successful owner responses for
commit status; no intent, timeout or diagnostic applied index becomes durable
evidence. Every checker `durable`/`decision_at` field is null. Retries use one
immutable task operation ID/body and distinct attempt IDs/intervals.

`events.jsonl` records invoke, raw wire reply, client delivery, fault and process
events under one mutex with strictly increasing ticks and elapsed monotonic time.
Checker timestamps are invocation/client-delivery ticks in that same domain.
Raw wire arrival alone is not client success: the proxy-discard event injects a
lost client reply while retaining the original wire bytes for audit. The lost
attempt remains indeterminate. Unrelated/out-of-order ID replies are retained
for their callers. There is no wall-clock synchronization assumption.

## Actual campaign

Two outstanding independent-key assignments; overlapping multirow assignments;
complete reads; acknowledged state surviving leader process loss; a proxy-lost
reply and retry after another leader loss; an intervening reset followed by
another retry to expose duplicate logical effects; checkpoint and full retained
restart; peer isolation with no successful write in a bounded minority window;
healing/retry and a final complete read. A timeout during isolation stays unknown:
the request may commit after healing. It is not labeled a definite abort.

Seed parity orders independent-key callers. This is a bounded scripted fault
campaign, not randomized scheduling or broad seed coverage. Outstanding requests
overlap at the client boundary but execute through one leader control stream.

`history.jsonl` is the untouched original, replayable with giggles-history.
`checker.json` includes the bounded verdict and witness/diagnostics. Three
in-memory copies change an externally observed scan to simulate a stale read,
loss of part of an acknowledged multirow write, and a duplicate logical effect
after reset/retry. Copies and verdicts are saved as `negative-*.jsonl` and
`negative-*.verdict.json`. The original is retained. Assignment duplicates that
leave identical values are inherently invisible; the reset makes this selected
duplicate observable. These controls validate detection, not injected runtime
storage corruption.

`runtime-checks.json` separately records equality of the full exposed inspection
payload across leader loss and checkpoint/full restart, retry-after-reset and
minority-window denial. Inspection equality covers rows including versions,
tasks, operation decisions and approvals, but not unexposed kernel namespaces.
These direct observation checks can fail the campaign even if its narrower
checker projection passes. An original FAIL or INCONCLUSIVE skips negative
controls and retains its verdict unchanged.

`manifest.json` records seed, scope, binary SHA256 hashes and paths; node JSON
configs, certificates, stderr, stdout and event traces are retained. Checkpoint
files preserve all runtime namespaces, but the checker checks only statuses.
Finite checker PASS is an explanation of these observations, not qualification,
proof of arbitrary durability, security, HA or strict cross-range semantics.
PostgreSQL comparative CONTROL remains deferred.

## Verification

```powershell
cargo fmt --manifest-path crates/giggles-history-runtime/Cargo.toml --check
cargo clippy --manifest-path crates/giggles-history-runtime/Cargo.toml -j 2 --all-targets -- -D warnings
cargo test --manifest-path crates/giggles-history-runtime/Cargo.toml -j 2 --doc
cargo doc --manifest-path crates/giggles-history-runtime/Cargo.toml -j 2 --no-deps
```

The process integration test, which built `giggles-node` from
`crates/giggles-consensus` and ran the harness CLI for seeds 42 and 43, was
removed with the prototype.
