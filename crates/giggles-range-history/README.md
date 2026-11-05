# External multi-range history capture

This bounded local harness launches three actual `giggles-ranges` processes with
peer mTLS, a real directory split and two transactional balance accounts. Owner
stdin is trusted control. It is not an application authentication interface.

From the repository root in PowerShell:

```powershell
$env:CARGO_BUILD_JOBS='1'
$env:CARGO_TARGET_DIR='K:/Projects/gigglesdb/crates/giggles-range-history/target'
cargo test --manifest-path crates/giggles-range-history/Cargo.toml -j1
cargo test --manifest-path crates/giggles-range-history/Cargo.toml -j1 --release
cargo run --manifest-path crates/giggles-range-history/Cargo.toml -- K:/Projects/gigglesdb/crates/giggles-range-history/target/owner/debug/giggles-ranges.exe $env:TEMP 42
```

For direct CLI use, supply an absolute owner binary path (the tests build it at
`crates/giggles-range-history/target/owner/{debug,release}/giggles-ranges.exe`).
Arguments are `NODE_BINARY ARTIFACT_PARENT SEED`. Exit 0 requires original checker
PASS, all runtime assertions true and all three corrupted copies FAIL. Exit 1 is
an original checker FAIL, 2 is INCONCLUSIVE or an execution error, 64 is usage.

Fixtures use unique retained directories. No files are cleaned up. Each retains
exact owner requests/stdout, stderr, event ticks and elapsed times, generated
certificates/private keys, configs, stores, history JSONL, verdicts and dependency
hashes. Keep these local test credentials with the fixture. Every attempt uses
external invocation/completion observations. Missing responses remain unknown;
durable status evidence is recorded separately. Complete scans come from stdout.

The two account keys are tenant42/row1 and tenant42/row60. Only Failed rows with
failed_at103 are modeled; attempt_count encodes a nonnegative u32 balance. Exact
before-images include runtime versions for CAS. The independent history checker
models validated transfers and complete scans, not arbitrary Row fields or CAS
precondition logic. Stale CAS and conflicting-body rejection have separate actual
runtime assertions. The seed selects the transfer amounts; it does not claim
randomized network scheduling.

The campaign stages real prepared transactions, overlaps a competing transfer,
terminates/restarts the coordinator before and after a durable decision, retains
the original transaction home/IDs, delays participant finalization, observes
survivor election and retries immutable requests. Copied stale-read, partial-write
and double-effect witnesses are deliberate checker controls, not storage faults.
Pending calls are indeterminate even after their durable outcome is learned.
Finite checker PASS is an existential explanation, not system qualification.

The retained campaign uses eight owner transactions, including observation
transactions. Encoded retained-state capacity can bind before the nominal
transaction-count ceiling. The harness drives prepared stages and survives the
faults; termination targets the actual process hosting the original durable
transaction home. Normal transfer and immutable retries exercise automatic
`transact`. The response deadline is 60 seconds; expiration remains inconclusive.
Status projection validates the original identity and body digest and preserves
the earliest externally observed durable bound.

Recovery retries only the same original home and transaction identity, at most
eight times within one 60-second deadline including routing and response waits.
Every refusal remains in the event trace. It never retries a fresh transaction.
The independent trace validator reconciles plan identity/body/home, response
ownership, exact scans and earliest explicit durable-status observations.

The continuation also sends actual automatic `transact` commands through two
distinct owner process pipes before receiving either response. This establishes
overlapping external invocation intervals; local leadership requirements may
refuse both calls. It does not establish simultaneous successful coordinators.
The original staged logical overlap remains a separate part of the profile.

While a committed participant is still pinned locked/unpublished after restart,
the harness sends an actual `Observe` to another owner, polls its response, and
drives immutable recovery on an available owner channel. `read_barrier` and
`read_capability` retain whether a call actually remained pending or immediately
refused, its exact response, and whether a complete read followed. The current
local-leader surface can refuse a follower with `leader/read context`; that is
capability evidence with no partial rows, not a parked-read liveness proof.
No pending request is simulated by an unpolled variable. Poll timeout preserves
response ownership; an unresolved final response makes the campaign inconclusive.

Only three local voters and bounded schedules are covered. No SQL, protected task,
four-voter replacement, cloud, security, performance or production qualification
is claimed. PostgreSQL comparison and broader integration remain deferred.
