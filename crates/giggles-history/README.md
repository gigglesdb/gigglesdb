# giggles-history

Independent Rust library and CLI for **bounded strict serializability** of
transactional ordered integer-key histories. It imports no GigglesDB runtime,
model, storage or transaction evaluator. Version 1 describes a finite synthetic
model suitable for feeding instrumented runtime histories after integration.
The tests here validate the checker; they do **not** qualify a distributed
system, production durability, cloud behavior or security.

```powershell
cargo run --manifest-path crates/giggles-history/Cargo.toml -- generate 42 8
cargo run --manifest-path crates/giggles-history/Cargo.toml -- check history.jsonl
cargo run --manifest-path crates/giggles-history/Cargo.toml -- replay history.jsonl --nodes 1000000 --memory 67108864
cargo run --manifest-path crates/giggles-history/Cargo.toml -- minimize failing.jsonl
```

`generate` emits JSONL on stdout; save using a UTF-8 writer (Windows PowerShell
5 redirection can produce UTF-16, which this parser rejects). `check` and
`replay` are equivalent and emit one JSON verdict. Use `-` for stdin.
`control <stale-read|stale-final-read|aborted-but-visible|duplicate-apply> <FILE|->` emits mutated JSONL.
Exit 0 means applied; exit 3 means not applicable (message on stderr); check the mutation independently.
`minimize` emits reduced JSONL on stdout and a reduction summary on stderr.
Its successful exit is 0; a non-FAIL source or invalid input exits 64. Reduction
removes complete logical transaction groups, retaining initial state,
invariants and FAIL. It is bounded greedy reduction, not a minimum-size proof.

| check/replay exit | Meaning |
| --- | --- |
| 0 | `PASS`: a legal serial explanation exists in this finite model |
| 1 | `FAIL`: exhaustive search rejected every legal explanation |
| 2 | `INCONCLUSIVE`: node or accounted model-memory limit exceeded |
| 64 | Invalid input, unsupported schema, arguments or I/O error |

There is no timeout-to-success path. `--nodes` bounds search nodes (default
1,000,000); `--memory` bounds a conservative accounted model allocation estimate
(default 64 MiB). This is **not an operating-system RSS limit**: parser,
allocator, executable and library overhead are outside that estimate. Input
size bounds apply before search. Finite search is exponential; rejecting a
large history on limits is expected and gives no consistency verdict.

## JSONL v1

Exactly one `header` first, then up to 128 `attempt` records. Every displayed
field is required except nullable `Option` fields, which may be omitted.
Unknown fields, duplicate struct fields, malformed UTF-8 and blank lines are
rejected. The final newline is optional. Numeric map-key ambiguity is avoided
by encoding initial state and scan rows as **ordered, unique `[key,value]`
pairs**. Keys/values are signed 64-bit integers; times are unsigned 64-bit
ticks on one instrumented clock/order domain.

```jsonl
{"record":"header","version":1,"horizon":10,"initial":[],"invariants":{"conserved_keys":[],"nonnegative_keys":[],"budgets":{},"require_receipts":false}}
{"record":"attempt","attempt":{"id":"a1","logical_id":"request1","invoke":0,"complete":2,"outcome":"committed","durable":"committed","decision_at":1,"ops":[{"op":"write","key":7,"value":12}],"observations":[],"receipt":null}}
{"record":"attempt","attempt":{"id":"a2","logical_id":"request2","invoke":3,"complete":5,"outcome":"committed","durable":null,"decision_at":null,"ops":[{"op":"scan","start":0,"end":10}],"observations":[{"observation":"scan","rows":[[7,12]]}],"receipt":null}}
```

`initial` is the **complete** starting state for the modeled keyspace, not a
sample. Keys not present are absent. Model the whole relevant keyspace and all
intervening writes; partial instrumentation can invalidate any inference.

Operations run in program order on a private transactional state:

* `{"op":"read","key":7}` produces `{"observation":"read","value":12}`;
  absent produces `value:null`.
* `{"op":"scan","start":0,"end":10}` returns **every** modeled row in
  `[start,end)`, including read-your-writes and deletes. Bounds must increase.
  These are full predicate scans, with no pagination, filtering or truncation.
* `{"op":"write","key":7,"value":12}` inserts/replaces.
* `{"op":"delete","key":7}` removes; deleting absence is allowed.
* `{"op":"transfer","from":0,"to":1,"amount":3}` requires distinct
  existing accounts, positive amount, sufficient source funds and checked
  integer arithmetic. Both changes commit atomically. It produces no read
  observation; add explicit reads/scans to witness balances.

Observations follow the read/scan operation order. Committed client responses
must include all read results. Aborted or indeterminate responses can provide
a prefix. A selected committing origin and every independently durable-committed
retry must match their observed prefixes; all committed responses for that
logical ID must match the full original result.

## Outcomes, intervals and retries

Each physical attempt has a unique `id`, scoped immutable `logical_id`,
`invoke`, nullable `complete`, client `outcome`, independent nullable `durable`
decision and nullable `decision_at`. Supported client outcomes are `committed`,
`aborted` and `indeterminate`. Durable evidence is either `committed` or
`aborted`; `null` means unknown. A non-null `decision_at` requires a durable
decision and is a timestamp **by which** it existed. Timestamps must be within
the attempt invocation and history horizon. Decisive client responses require
a completion. A pending indeterminate attempt may use `complete:null`.

A successful response forces exactly one logical commit at or before its
completion. Independently durable commit evidence also forces that commit,
at or before `decision_at` when supplied. `decision_at:null` supplies no
additional temporal bound. Durable commit without a successful response still
requires the effects. A timed-out/indeterminate response may or may not commit,
and **its client completion does not cap commit time**: the operation may finish
after the timeout up to the finite horizon. Horizon is an observation/model
boundary, not a promise that the live system has stopped processing.

Response-only `aborted` excludes that physical attempt's effects; another
attempt of the same logical request can retry successfully. Durable `aborted`
is a terminal **logical** decision and forbids any commit for that ID. Do not
use it for a retryable per-attempt conflict. Contradictory commit/abort evidence
is an exhaustive `FAIL`, not a guessed resolution. Aborted/omitted attempt
observations do not require opacity or isolation of aborted execution.

All retries share identical operations and receipt metadata. Reusing an ID
with a different body is invalid input. A selected origin is one non-aborted
attempt; its invocation bounds the earliest serialization time. Every successful
retry returns the original observations without applying effects again. The
earliest successful response or durable decision bounds the commit even if
later retry intervals extend further. Unique operation IDs alone cannot reveal
an observationally identical duplicate assignment: use transfers, counters,
receipts or external witnesses to expose duplicate effects.

The checker searches permutations and choices of unknown outcomes, propagating
the earliest feasible serialization timestamp. Committed transactions whose
completion is **strictly before** another origin's invocation must precede it.
Equal endpoints permit either order at that instant. Instrumentation must use
a coherent time/order domain; independent unsynchronized wall clocks are not
adequate. Returned `serialization` lists logical IDs, origin attempt IDs and
feasible times. `omitted` lists excluded logical IDs. A passing witness includes
`final_state`. FAIL includes explored count and bounded rejected-edge diagnostic
samples; diagnostics alone are not an unsatisfiability certificate. Replay the
same bounded source for the exhaustive verdict.

## Independent optional invariants

`conserved_keys` keeps their initial i128 sum (absence counts as zero).
`nonnegative_keys` must remain nonnegative. These hold initially and after
**every** selected logical commit, so a later compensating transaction cannot
hide an invariant violation. A `receipt` has `id`, `budget`, and unsigned `cost`.
When supplied it commits once with the logical effects, must be globally unique
within the history, must name a configured budget and cannot exceed its cap.
`require_receipts:true` requires a receipt on every selected commit, including
read-only transactions. Represent external receipt/status observations as
ordinary modeled reads/writes if checking their materialization is required.
This metadata model does not prove authority, approval, lifetime accounting
outside the supplied history, or external side effects. Budget consumption
starts at zero for this history; provide remaining allowance as its cap.

## Bounds and public API

v1 bounds: 4 MiB total JSONL; 1 MiB per line; 128 attempts; 256 operations per
attempt; 8,192 operations total; 4,096 distinct modeled/observed keys; 16,384
scan-result cells total; 4,096 rows per scan; 128-byte nonempty/control-free
identities; 128 budget IDs; 4,096 entries per invariant list (no duplicates).
Serde's nesting bound also applies. Bad schema is an error, not inconclusive.
The same model bounds protect direct API calls via `validate`/`check`.

Public entry points are `parse_jsonl(BufRead)`, `write_jsonl(&History, Write)`,
`validate(&History)`, `check(&History, Limits)`, `generate(seed, count)` and
`minimize(&History, Limits, max_checks)`. They return `Result<_, String>`;
`Check`/`Verdict`/`Step` and schema types are exported with serde support.
The JSONL representation is the versioned interchange contract; direct serde
serialization of `History` is a convenience with a different top-level shape.
Within v1, new operations or semantics require a schema version change.

```rust
use giggles_history::{check, generate, Limits, Verdict};
let history = generate(42, 8)?;
let result = check(&history, Limits::default())?;
assert_eq!(result.verdict, Verdict::Pass);
# Ok::<(), String>(())
```

Runtime adapters should record observations outside the evaluator, give each
retry attempt its own interval, retain request identity/body, preserve every
failure/unknown outcome, and append explicit complete-state reads or range
witnesses after recovery. Lost acknowledged writes and partial commits are
detectable when later observations contradict their required effects; a checker
cannot discover silent unobserved loss or validate the truth of a fabricated
durable-evidence field. Current keys, transactions, receipts and transfer
semantics are intentionally narrower than SQL or a full financial ledger.

## Testing

```powershell
cargo fmt --manifest-path crates/giggles-history/Cargo.toml --check
cargo clippy --manifest-path crates/giggles-history/Cargo.toml -j 2 --all-targets -- -D warnings
cargo test --manifest-path crates/giggles-history/Cargo.toml -j 2
cargo test --manifest-path crates/giggles-history/Cargo.toml -j 2 --release
cargo test --manifest-path crates/giggles-history/Cargo.toml -j 2 --doc
cargo doc --manifest-path crates/giggles-history/Cargo.toml -j 2 --no-deps
```

Tests include paired valid/corrupted anomaly fixtures, 23,840 exhaustive tiny
histories checked against separately written subset/permutation/timestamp and
vector-state enumeration (without retries), seeded metamorphic campaigns,
retry-specific behavioral fixtures, and real child-process CLI tests.
CLI tests preserve exact inputs, invocation arguments, exit codes and raw
stdout/stderr in unique `giggles-history-cli/cli-*` directories under `TEMP`. These are
synthetic checker tests; system qualification and independent integration
review remain separate work.

## Runtime recording

The record module supplies a thread-safe Recorder with a process-wide clock,
a deterministic TransferWorkload, and window validation with conservation and
nonnegative account invariants. Complete unknown responses as Indeterminate;
resolve attaches independent durable evidence without guessing a decision time.
The controls module returns mutations only when the bounded checker confirms
FAIL; absent candidates or inconclusive mutations return None.
