# giggles-ledger

A finite, trusted embedded financial module backed by the actual durable
GigglesDB public SQL transaction API. It requires no service. It supports one
ledger and asset per dedicated database, immutable scale, accounts, direct
postings, reserve/capture/void/expire, whole reversals, permanent request IDs,
atomic or individually committed batches, status, reconciliation and sealed
export/restore.

This is a bounded local implementation. It has no distributed posting, account
SQL joins, partial capture/reversal, account closure or automatic expiry sweeper.
It makes no performance, security, cloud or production qualification claim.

## Accounting and API

`Ledger::create(path, Definition)` initializes a fresh database;
`Ledger::open(path)` validates an existing one. `create_account` establishes an
immutable account rule and optional maximum outstanding credit balance.
`account` returns separate posted debit/credit and pending debit/credit totals.
`execute(Intent)` performs one operation. `atomic_batch` commits all or none;
`individual_batch` returns separate per-item results and may partially succeed.
`status(id)` returns the original immutable decision, while `position(id)`
returns the current reserve/reversal state. `reconcile()` verifies the schema,
canonical encoding, every receipt digest and transition, and reconstructs all
four totals from immutable decisions before comparing caches.

All amounts are `Amount(u128)`, unsigned integer asset units. An amount is
encoded as exactly **16 big-endian bytes** in SQL BYTEA's canonical versioned
JSON document and in the JSON CLI. No float or signed integer conversion occurs.
Scale is metadata (0..38); the API never rounds decimals. Convert business
decimals to units exactly before submitting. Zero/self transfers, missing
accounts, ledger/asset mismatch and checked cumulative total overflow reject.
Each posting debits and credits exactly the same units. Totals are lifetime
counters, so a reversal can fail on cumulative overflow even when net balance
would fit.

An unrestricted clearing/source account may issue units by debit. A
`DebitsMayNotExceedCredits` account requires
`posted_debits + pending_debits <= posted_credits`; incoming reservations are
not spendable. Optional `max_credit` requires
`max(posted_credits - posted_debits, 0) + pending_credits <= max_credit`;
outgoing holds do not create incoming capacity. All additions are checked.

`Reserve` records pending equal debits/credits. Exactly one full `Capture`,
`Void` or `Expire` resolves it. Capture posts the entire reserve, void/expiry
release it. A new `Reverse` references the original direct posting or captured
reserve and creates a full compensating pair. A second reversal, reversal of a
reversal, capture-receipt alias as original and partial reversals are rejected.
Reversals respect account capacity and funds rules.

## Trusted boundary and durability

`Ledger` keeps `Admin` and `SqlSession` private and exports no raw SQL handle.
Ordinary API callers cannot write financial rows directly. An independently
obtained trusted local Admin or file owner **can bypass this module**. This is
not engine-wide protected DML. The dedicated schema is exactly
`ledger_state(slot BIGINT PRIMARY KEY, payload BYTEA NOT NULL)` with one row at
slot 1 and version 1 payload. Unknown format, foreign/extra schema, extra rows,
noncanonical payloads, inconsistent totals, digests or illegal transitions fail
closed. The posting journal and cached state share one bounded aggregate.

Each change uses `BEGIN`, reads and audits the durable aggregate, applies typed
transitions, performs a row-version-checked `UPDATE`, and `COMMIT`s. Core
generation conflicts cause at most eight attempts. Contention exhaustion is
explicit, not a dropped posting. All receipts/totals/lifecycle changes commit
together. Local durability follows core/redb's documented immediate durability
and the device flush contract; a single device is not disaster recovery.

The audit detects inconsistencies, not a privileged adversary rewriting both
journal and caches consistently. After privileged changes, stop writes,
preserve an export, reconcile against independent trusted audit copies and
external business/settlement records. `export` produces a core sealed export;
`restore` preserves ledger operation IDs and receipts in a new core identity.
An old export omits later financial decisions. Keep restored historical
timelines isolated and fence/reconcile externally before accepting settlement.

## Retry and time contract

The caller must durably persist the complete intent and stable ID **before**
sending. IDs last for the ledger's lifetime. Same ID and canonical operation
payload returns the original receipt, even after reopen/export/restore.
Different payload returns `DuplicateMismatch`. Clock certification is excluded
from the payload digest, allowing a retry to supply fresher time; reserve
deadline, account IDs, amount, asset, ledger and referenced operation IDs are
included. An already recorded request returns its original outcome before
checking the new clock. Rejected decisions do not consume an ID.

`Error::Unknown { ids }` means COMMIT may have persisted. Drop all handles,
reopen, and query `status` for the original IDs; retry the same ID/payload if
absent. Never generate a fresh ID because of a timeout. For ambiguous
initialization/account creation (empty ID list), reopen and inspect definition
or account. A core I/O failure may fence the existing handle.

`Clock { lower, upper }` is an explicitly **trusted monotonic certification**
in caller-defined integer units, not local wall-clock time. Require lower <=
upper and lower >= the last committed lower. Capture requires upper < deadline;
expiry requires lower >= deadline; an overlapping interval rejects as
`TimeUncertain`. Deadline is rechecked inside the committing transaction on
every conflict retry. There is no clock authority or automatic expiry in this
crate. A lying caller defeats real-time expiry guarantees. Units and clock
authority must be fixed consistently by the application; accepted deadlines
are at most 86,400,000 units after the certified lower bound. Void is permitted
at any certified time while still pending, including after the deadline.

## Finite limits

- One ledger/asset per database, IDs 1..64 ASCII letters/digits/underscore/hyphen.
- At most 64 accounts and 128 permanent operation receipts.
- At most 16 intents per batch, 60,000 encoded bytes per aggregate.
- Scale <=38, maximum hold 86,400,000 caller clock units, eight conflict attempts.
- CLI input <=128 KiB; query/audit work scans the bounded journal.

The byte cap can bind before receipt count, especially with long identifiers.
Each pending hold reserves one future receipt slot and 2,048 encoded bytes for
its terminal action. All writes preserve that headroom, so filling remaining
capacity cannot strand a hold for lack of journal space. Capture can still fail
on time, arithmetic or account rules; void/expiry retain their storage budget.
New operations exceeding available capacity fail atomically with `Limit`.
History and dedupe are never pruned. All writes serialize through a bounded aggregate and rewrite
it; use this as a correctness-focused finite module, not a scalable ledger.

## Run the JSON CLI

From the repository root, use `cargo run --manifest-path
crates/giggles-ledger/Cargo.toml --bin giggles-ledger`, feeding one JSON command
on stdin. The process emits `{"ok": ...}` or `{"error": ...}` and exits nonzero
on error. Use a new, absent database path. PowerShell examples:

```powershell
$env:CARGO_TARGET_DIR='K:/Projects/gigglesdb/crates/giggles-ledger/target-ledger'
'{"command":"create","path":"C:/Temp/example-ledger.db","definition":{"ledger":"main","asset":"USD","scale":2}}' | cargo run --manifest-path crates/giggles-ledger/Cargo.toml --bin giggles-ledger
'{"command":"account","path":"C:/Temp/example-ledger.db","definition":{"id":"mint","rule":"Unrestricted","max_credit":null}}' | cargo run --manifest-path crates/giggles-ledger/Cargo.toml --bin giggles-ledger
'{"command":"account","path":"C:/Temp/example-ledger.db","definition":{"id":"alice","rule":"DebitsMayNotExceedCredits","max_credit":null}}' | cargo run --manifest-path crates/giggles-ledger/Cargo.toml --bin giggles-ledger
'{"command":"execute","path":"C:/Temp/example-ledger.db","intent":{"id":"fund-1","clock":{"lower":0,"upper":0},"operation":{"Post":{"ledger":"main","asset":"USD","debit":"mint","credit":"alice","amount":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,25]}}}}' | cargo run --manifest-path crates/giggles-ledger/Cargo.toml --bin giggles-ledger
'{"command":"status","path":"C:/Temp/example-ledger.db","id":"fund-1"}' | cargo run --manifest-path crates/giggles-ledger/Cargo.toml --bin giggles-ledger
'{"command":"reconcile","path":"C:/Temp/example-ledger.db"}' | cargo run --manifest-path crates/giggles-ledger/Cargo.toml --bin giggles-ledger
```

Other operation JSON uses externally tagged enum variants:
`{"Reserve":{"transfer":{...},"deadline":100}}`,
`{"Capture":{"reserve":"hold-1"}}`, `{"Void":{"reserve":"hold-1"}}`,
`{"Expire":{"reserve":"hold-1"}}`, `{"Reverse":{"original":"fund-1"}}`.
Each is wrapped in a fresh stable `Intent`. CLI commands also include
`get_account`, `position`, `atomic_batch`/`individual_batch` (with `intents`),
`export` (with `destination`), and `restore` (with `source` and fresh `path`).
The explicit `inject` command and `execute_with_fault` Rust API are trusted
exclusive-owner recovery-test seams. They can terminate the process or produce
ambiguous I/O and must never be invoked concurrently or by untrusted callers.
No environment variable enables faults.

## Validate

Set crate-local `CARGO_TARGET_DIR` and `CARGO_BUILD_JOBS=2`. Run `cargo fmt
--manifest-path crates/giggles-ledger/Cargo.toml --check`, `cargo clippy
--manifest-path crates/giggles-ledger/Cargo.toml --all-targets -j2 -- -D warnings`,
`cargo test --manifest-path crates/giggles-ledger/Cargo.toml -j2`, the same with
`--release`, `cargo test --manifest-path crates/giggles-ledger/Cargo.toml --doc`,
and `RUSTDOCFLAGS=-D warnings cargo doc --manifest-path
crates/giggles-ledger/Cargo.toml --no-deps`. Fixtures use unique paths in the OS
temp directory, intentionally retained, with no automatic filesystem cleanup.
