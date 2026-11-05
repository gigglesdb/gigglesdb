# GigglesDB durable embedded core (P1 safety slice)

Standalone Rust crate; phase [GD-0002](https://app.archdocs.io/design-docs/f386f1a9-661e-4d62-9d03-8a51e050ebe6).
No remote, push,
release, production, cloud, comparative or general SQL compatibility claim.
P0 is a **dev-only** dependency. Runtime policy code never invokes `Model`.

```powershell
cd K:/Projects/gigglesdb/crates/giggles-core
cargo run --bin giggles -- demo artifacts/my-new-demo
cargo run --bin giggles -- inspect artifacts/my-new-demo
cargo run --bin giggles -- export artifacts/my-new-demo artifacts/my-new-export.gdb
cargo run --bin giggles -- verify artifacts/my-new-export.gdb
cargo run --bin giggles -- restore artifacts/my-new-export.gdb artifacts/my-new-restored
./verify.ps1 -ArtifactDirectory artifacts/my-new-verification
```

All destination directories/files must be new. No cleanup is performed. An error
can leave an artifact; retain it and choose another path. No daemon or listener.

## Integration interface

`Admin::create(path, Limits)` explicitly creates a new database directory.
`Admin::open(path, Limits)` only opens an existing `data.redb`; a missing, empty,
corrupt or newer-format database never initializes a replacement. Clone an admin
or executor to share one local scheduler; opening another writer rejects, even
within the same process. redb supplies OS-held file locking. Tests cover actual
independent processes and hardlink aliases on this Windows environment.

Trusted `Admin` methods: `put`, `grant`, `executor` (rebind after restart),
`revoke`, `bump_schema`, `approve`, `revoke_approval`, `snapshot`,
`authority_metadata`, `export`, `verify_export`, `restore`. `grant` returns an
opaque `Executor`, bound to its database generation, immutable task and principal.
There is no public credential constructor or deserialization of an executor.
`Executor` exposes only `preview`, `execute`, `execute_sql`, and `status`.
Use the shared public typed DTOs in `types.rs` for server integration. The source
API is the initial 0.1 contract; format and canonical payload changes require a
new encoding version, not silently changed DTO ordering. `CoreError` distinguishes
admission/limit/unsupported errors, policy errors, storage errors and fencing.

```text
Executor::preview(&Request, Time) -> Result<Preview, CoreError>
Executor::execute(Request, Time, Option<ApprovalId>) -> Result<Decision, CoreError>
Executor::execute_sql(OperationId, &str, Time, Option<ApprovalId>) -> same
Executor::status(OperationId) -> Result<OperationStatus, CoreError>
Decision = Result<Receipt, Error>
```

An outer error is an API/admission/storage failure. A returned `Decision` is a
durable logical result, including terminal policy denial. An outer `Storage`
error means the outcome may have committed; **do not allocate a new operation ID**.
The whole handle is fenced, including status. Drop all clones, reopen through the
trusted admin, rebind the original executor and resolve the original operation
ID using `status`. A process killed before commit leaves `Absent`; a killed
process after commit leaves `Decided`. An acknowledged result is returned only
after `Durability::Immediate` commit returns. No in-memory result is published
on commit failure. Fault hooks `set_crash_point` are trusted-admin-only test seams.

The first authenticated, admitted logical attempt durably binds `(domain, task,
operation)` to its canonical payload and decision. Same payload replays without
recharging or consuming another approval. Altered payload returns `IdConflict`.
Task expiry/revocation does not prevent status/replay of an existing decision.
Admission/size/unsupported-syntax failures precede logical decision and do not
bind an operation. A corrected terminal denial requires a new operation ID.

## Actual authority and clock boundary

This is a local library capability boundary, not a process sandbox or remote
identity system. The embedding host, filesystem owner and holder of `Admin` are
trusted. `Principal` is a typed local identity, not authentication evidence.
The trusted application must authenticate a remote principal before calling
`Admin::executor` and must never expose that admin or rebind API to an executor.
The CLI is a **trusted local admin tool**, including its repair/status commands;
its principal argument does not authenticate the shell user.

`Time::Certified(n)` is a trusted logical decision-time input. **The host must
supply it; never accept it from a remote executor's request.** `Uncertain` denies
new work, decreasing time denies, and the monotonic floor survives all APIs,
restart and export. Preview/approval attempts that advance time persist that
floor even when subsequently rejected. A caller that can supply arbitrary
certified times can defeat any physical expiry interpretation. This crate does
not prove wall-clock expiry, pause/skew bounds or a trusted time service. Logical
revocation is serialized with writes. A server must inject its own time authority
and authenticate principals rather than forwarding client timestamps.

Receipts contain full effects; previews show only task-selected rows, but there
is no production disclosure quota, redacted forensic split or cryptographic
credential/receipt signature. Do not expose these as a production security API.
Filesystem ownership defeats enforcement. NFS/SMB and cloud-synced live paths are unsupported and are not automatically detected; use trusted local paths. Checksums detect corruption, not a
malicious administrator rewriting a database or export. No issuer-authority
intersection/delegation implementation exists.

## Independent policy and atomic storage

`policy.rs` independently enumerates/validates selected rows, transitions,
incident bounds, exact dependencies and budgets. It is not copied Model methods.
Public DTO/canonical request grammar began from P0 so fixtures can be converted
without semantic reinterpretation. Runtime has no dependency on giggles-model.
The evaluator reads the engine's committed records under the serialization gate,
derives the next decision and writes records through one redb transaction.
It does not serialize a `Model` or maintain an authoritative in-memory cache.

Separate namespace keys store metadata (0), rows (1), tasks plus membership and
counters (2), operation binding plus receipt/denial (3), approvals (4). Business
writes, selected-row versions, task membership/counters, approval consumption,
operation binding and receipt are committed in **one** immediate transaction.
All fallible policy checks precede business mutation. Lifetime distinct identity
and repeated transition charges survive restart; trusted row resets do not reset
budgets. Noops charge zero mutation effects but still validate incident scope and
selected dependencies. Tasks are immutable and IDs cannot be reissued.

Exact approval binds task, database domain, schema, policy, operation/payload,
before rows/versions, after rows and expiry. A supplied approval is checked even
below the threshold. Predicate re-enumeration detects newly matching rows and
removals; an ABA write increments version. Unrelated/nonmatching writes need not
invalidate an exact set. Revoked, consumed, expired, cross-operation and stale
approvals reject. Scope may reject before stale-preview detection, matching P0.

Local serial locking protects full predicate sets. Default admission is 32
in-flight calls (active plus waiting); excess calls get `Busy`. Default selected
key cap is 1,024, including predicate matches. The redb cache is 8 MiB. Total
serialized map payload and export decoding are capped at 16 MiB, including
8 bytes for the map count and16 bytes per record for lengths. Every accepted
state fits the export envelope. SQL input cap is 4,096 bytes.
These are a small bounded safety profile, not a performance/scalability result.
Whole-state traversal and rewriting on each transaction are deliberate initial
costs; no fairness, wait deadline, cancellation, scan CPU quota or disk-reserve
claim. There is no filesystem/device power-loss qualification.

## Version 1 encoding

Database metadata carries format=1, domain u128, schema u64, last_time u64,
next_approval u64 and optional restore origin. Keys are a one-byte namespace
followed by fixed big-endian unsigned u64 components: `(tenant,row)` for rows,
`task` for tasks, `(task,operation)` for decisions, `approval` for approvals.
This is prefix-free within each declared fixed-arity namespace; comparison order
matches unsigned tuples. No signed/NULL/text/collation key type is supported.

Record values use the pinned format-1 DTO declarations and bincode 1.3 fixed-width,
big-endian, bounded decoding with trailing bytes rejected. Enum tags are bincode
u32 discriminants in declaration order. There are no permissive unknown tags.
Global format=1 pins every record layout. redb's format/checksums are an additional
storage layer, not a substitute for GigglesDB version checks. P0 request identity
remains the explicit `GD-P0-REPAIR` byte-1 canonical codec, with sorted keys and
preserved duplicate multiplicity. Operation ID is the map key, not payload bytes.

Export header is 8-byte `GDEXP001`, u32 format=1 big-endian, u64 payload length
big-endian, 32-byte SHA256 of payload, then canonical encoded ordered record map.
Verify checks total size, exact length, version, SHA256, canonical map encoding,
namespace/key shapes, bounded typed values and authority invariants before
returning a snapshot. Unknown or malformed input fails closed. No migration or
salvage is attempted. The current checks do not independently prove every
possible forged history; P0's checker/differential tests provide separate local
behavior evidence, not authentication of an artifact.

## Online export and fenced restore

Export captures a consistent committed cut under the gate, releases the gate,
writes a unique **create-new staging file**, calls `sync_all`, then reads/verifies
it. Atomic no-clobber hard-link creation publishes the requested destination.
Staging remains on success and failure; no artifact is overwritten or deleted.
Writers can proceed during writing/verification; encoding happens under the gate.
Partial staging fails verification, while complete staging is independently
verifiable even after publication fails. Parent-directory persistence under power
loss is not qualified. Export errors do not fence a healthy source database.

Restore verifies first and then creates a fresh destination only. The complete
restored state is installed in its first transaction, avoiding a valid empty
intermediate database. The new random 128-bit database identity differs from
source with cryptographic-random collision probability; all restored tasks and
approvals are revoked. Old rows, cumulative counters, operation bindings and
receipts remain forensic state; old receipts retain their original domain.
Old operations can be resolved by a **trusted new rebind** but new work under
restored tasks denies. Create a new task ID for new authority after reconciling
business history. Metadata preserves `last_time`, `next_approval`, and origin.
No destructive overwrite, source fencing/promotion, replay into old authority,
PITR or distributed reconciliation is implemented. An offline source can still
write: coordinating source/restore deployment ownership is external to this API.

## Finite SQL and CLI

Only these whitespace-tokenized shapes are supported (case-insensitive keywords):

```sql
UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 0
UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND status = 'failed' AND failed_at >= 100 AND failed_at < 107
```

Status literals: `'failed'`, `'queued'`, `'succeeded'`. Numbers are unsigned decimal
u64. Assignment/selection still obeys task policy. Operators require surrounding
whitespace; no trailing semicolon. No comments, joins, subqueries, DDL, bulk,
expressions, functions, implicit casts, placeholders, transactions, SELECT, RETURNING,
pgwire or compatibility promise. Everything else returns `Unsupported` before
binding an ID. CLI commands now cover create, seed, grant, preview, approve, revoke,
revoke_approval, execute, repair, status, inspect, export, verify, restore and demo.
All return protocol-v1 JSON; policy denials exit2 and other errors/absent status
exit3. Demo returns a JSON envelope containing both permitted/forbidden examples.
See [LOCAL.md](LOCAL.md) for exact JSON commands and approval workflow.
The versioned C ABI is implemented in [giggles-ffi](../giggles-ffi/README.md),
with header, ownership contract, stable codes, bounds and an actual C client.

## Verification scope and next P1 work

Tests use real redb files, retained unique directories, actual independent writer
processes and process termination. Windows explicit durability seams use
`TerminateProcess` without Rust destructors; the acknowledged-result child exits
immediately without dropping database handles after syncing an external witness.
Three seams: after staging/before immediate commit, immediately after commit,
and before returning the acknowledgment. The latter two are the same durable
side of the commit boundary. Tests do **not** instrument redb internal page/flush
boundaries or cut device power. Fault hooks separately return IO errors before
and after commit to exercise fencing and status recovery.

Differential tests convert P0 DTOs, replay five public golden histories and compare
100 deterministic seeds x100 actions with the independent P0 Model after **each**
action. P0's independent checker verifies each completed oracle history. Seed
range 0..100, LCG constants and action schedule are in tests/differential.rs;
per-seed traces and SHA256 remain in each database directory. Debug/release run
the same seeds and must not be counted as independent campaigns. Concurrency
includes ten-thread last-slot, identical retry and altered-payload races. Raw
subprocess command/stdout/stderr/status artifacts remain with each database.

Unfinished P1: trusted time
source integration for a remote boundary; redacted output/disclosure accounting;
filesystem path-alias/replacement/symlink/fork/suspend and full-disk campaigns;
redb internal flush/torn-write/power-loss harness,
parent-directory durability qualification; durability/platform
qualification and installation packaging. ## Consecutive P1 interface and recovery slice

[GD-0004](https://app.archdocs.io/design-docs/69c1e508-5416-4aca-87d8-71c4f791c0cf)
and [GD-0005](https://app.archdocs.io/design-docs/973ebf6a-0989-4397-af4c-d8b5bc2a49d8)
extend the embedded core. Exports now write/sync/readback a unique sibling staging file, then publish by
atomic no-clobber hard-link creation on the tested local filesystem. Staging
files are intentionally retained on success and failure; do not edit hard links.
No destination replacement or filesystem deletion occurs. Unsupported link
publication returns an error, with no unsafe fallback. The file-sync contract
does not qualify directory-entry persistence under actual power loss.
The 16MiB bound now includes the serialized-map count/length overhead (8 bytes
plus16 per record), not only raw record bytes; this is checked for live writes
and on open. Previously created states exceeding that envelope now reject with
Limit rather than opening an unexportable state. Format bytes remain version1;
this does not claim compatibility for arbitrarily full older prototype files.

`RecoveryPoint` and `export_with_fault`/`restore_with_fault` are trusted test
surfaces, not wire commands or environment-controlled production behavior.
Returned IO seams cover export write/sync/verify/publication. Restore seams after
engine creation and after staged record insertion reject open after crash/error;
after immediate commit they open only as the full new identity with every old
task/approval revoked and forensic outcomes preserved. Restore does not publish
an empty valid replacement or overwrite existing destinations. Retry a failed
restore at a different fresh path and keep the partial for inspection. Process
crash seams use exact exit86; no internal redb flush or device faults are proven.

Full-state traversal/rewrite remains a prototype limitation. C registry handles
and CLI principal/time fields do not provide remote authentication. Production
redaction/disclosure budgets, trusted physical time integration, issuer delegation,
packaging and platform/durability qualification remain separate work.
