# giggles-model

A standalone, dependency-free Rust reference model for contributors testing bounded agent repairs.

**Status:** An in-memory serial oracle; storage, SQL, networking and production authentication are outside its scope.

Part of [GigglesDB](../../README.md).

```powershell
cd crates/giggles-model
cargo test --all-targets
cargo run --example p0
cargo run --example p0 -- --histories
./verify.ps1 -ArtifactDirectory artifacts/my-unique-run
```

Rust/cargo 1.99.1 was used for development. `verify.ps1` refuses to overwrite
an artifact directory. It records separate raw stdout/stderr, exact commands,
exit codes and SHA256 of source/fixture inputs. It performs no cleanup. Old
artifacts, including failures, remain under `artifacts/`. `target/` is retained.

## Trust and completion boundary

This is an in-memory serial oracle. It does not implement storage, SQL, locks,
networking, production authentication, cryptographic credentials, physical-time
expiry, bounded admission or disclosure accounting. A `Principal` is a typed
fixture identity, not evidence that someone authenticated. Model callers control
the process. Never expose trusted methods as executor endpoints.

`new`, `put`, `grant`, `revoke`, `approve`, `revoke_approval`, `bump_schema`,
`snapshot` and `history` are **trusted controller/test-harness interfaces**.
`execute`, `preview` and `status` take an opaque `Credential` issued by `grant`.
`approve` is a trusted call on behalf of the configured approver and verifies
that the supplied preview equals a fresh recomputation. Issuer and approver
must differ from executor. Delegation/issuer-authority intersection is not
implemented. Snapshot/history expose full fixture data and are not redacted
executor outputs. Receipts also contain full typed effects for the checker;
production receipt redaction and a separate forensic view are later work.

## Public repair interface and typed grammar

All public model types live in `repair.rs`, re-exported at the crate root.
IDs are distinct newtypes over `u64`; database generation/domain is `u128`.
Trusted callers must allocate distinct domains across databases and restores.
Cloning an entire `Model` is a simulation fork, not an independent authority.

```text
Key       = { tenant: TenantId, row: RowId }
Row       = { status: Failed|Queued|Succeeded,
              failed_at: u64, attempt_count: u64, version: u64 }
Policy    = { tenant, columns: set<Column>, transitions: set<(Status,Status)>,
              incident: [from,until), distinct_limit, transition_limit,
              approval_above, valid_from, expires }
Grant     = { task, issuer, executor, approver, policy }
Selection = Keys(vec<Key>)
          | Predicate { tenant, status, from, until }
Request   = { operation, selection, column, after: Status }
Time      = Certified(logical_decision_upper_bound) | Uncertain
```

The only supported write is the status column. Typed column alternatives exist
to test denial; adding them to a policy does not enable those writes. There is no
SQL parser or permissive unknown expression path. Incident and predicate bounds
are half-open. One forbidden selected row rejects the entire request. Predicates
select all matching rows in the specified tenant; no hidden partial eligibility.
Explicit missing identities reject. Duplicate explicit keys reject. No-op status
assignments have no logical effect and consume no mutation budget; this model
does not claim to account for their scan/work cost. Rows outside the incident
still reject even on no-op assignment. `put` models a trusted application write;
it increments the existing version and ignores the caller's version field.

Policies/tasks are immutable and IDs cannot be reissued, even after revocation.
Each actual effect consumes one lifetime transition. Each previously untouched
key consumes one lifetime distinct identity. Repeated changes charge transitions
again, even after trusted application writes reset the row. Both caps, business
effects, receipt, dedupe and approval consumption change at one serial decision.
Ten serial last-slot contenders are tested; this says nothing about thread safety
of a future concurrent implementation.

## Operation ID and ambiguity contract

Uniqueness is `(database domain, task, operation)`. The payload codec is
`GD-P0-REPAIR` + byte `1`, selection tag, fixed-width big-endian `u64` fields,
then column/status tags. Keys use tag `0`, count, sorted `(tenant,row)` pairs.
Predicate uses tag `1`, tenant, status byte, from and until. Column tags are
status=0, failed_at=1, attempt_count=2; status tags are failed=0, queued=1,
succeeded=2. Duplicate key multiplicity remains in the payload and rejects.
Operation ID is the map key, not part of the payload. Reordered unique key lists
have identical payloads. Predicate and explicit-key requests are distinct.
Exact canonical bytes are retained instead of relying on a non-cryptographic
digest; production hashing/encoding requires its own versioned contract.

The first authenticated execute binds its ID and terminal outcome, including
denials such as missing approval, uncertain time or budget failure. A corrected
attempt needs a new ID and, where required, a new preview/approval. Same ID/same
payload returns the original decision without new effects or another receipt.
Same ID/different payload returns `IdConflict` without replacing the binding.
Authentication failure never binds an ID. Approval handle and retry clock are
not payload fields: replay resolves the original decision independent of them.
This is deliberate; changing approval after a terminal denial cannot restart it.

`execute(..., lose_reply=true)` hides the returned decision as `Outcome::Unknown`;
it does not roll it back. `status` returns `Absent` or `Decided(Result<Receipt,
Error>)`. Absent means no bound serial decision in this model, not proof that a
real in-flight distributed request aborted. Status and exact replay work after
expiry/revocation for the originally bound credential; new work fails closed.

## Exact previews and time

Preview enumerates exact before rows, versions, after rows, request, task, policy,
schema and domain. Its dependencies contain all selected rows, including no-ops.
For a predicate, recomputation detects newly matching rows and removals. Explicit
key previews authorize exactly those keys; they do not claim completeness of a
predicate. Unrelated tenant and nonmatching-row changes do not invalidate them.
ABA writes increment versions and invalidate approval. Changes in a selected row
may reject on scope before returning `StalePreview`; both reject all repair
effects. Schema, task revocation, expiry, approval revocation, consumed approval,
operation or payload mismatch also reject. Budget is revalidated at commit.

Approval is required above the policy's effect-count threshold. A supplied
approval is always checked, even below threshold. Approval consumption is atomic
with a successful decision; retries return the prior receipt. Previews have no
business effects and do not bind an operation ID.

`Certified(n)` is supplied by a trusted logical-time fixture. Decisions and
preview issuance conservatively enforce monotonic time and `[valid_from,expires)`.
`Uncertain` fails closed. This models an ordering/fencing rule, not a proof of
wall-clock behavior under pauses or clock skew. No production expiry claim.

## Histories and checker replay

`fixtures::golden_histories()` returns five `GoldenHistory` values:
permitted, unsafe-query, stale-preview, ambiguous-retry, lifetime-budget.
Each contains `{name,domain,events: Vec<Event>,final_state: Snapshot,transcript}`.
`tests/fixtures/golden.txt` is the fixed transcript expectation. The example can
emit the complete typed histories as deterministic Rust Debug text for inspection;
that text is not a stable interchange format. Other tools can depend on this crate by path
and consume its public typed fixtures directly.

`Event` grammar: `Put(Key,Row)`, `Grant(Grant)`, `Revoke(TaskId)`, `Schema(epoch)`,
`Approve(Approval)`, `RevokeApproval(ApprovalId)`, and
`Decision{task,request,time,approval,result}`. Only first logical decisions are
recorded; replay/conflicts are caller observations, not extra committed events.
Trusted put/grant/schema events form the setup and interference fixture stream.
`Snapshot` exposes rows, task counters/membership, operation bindings and approval
states for independent comparison. `BoundDecision` retains payload and result.

`checker::verify(domain, events, expected_snapshot)` reconstructs state using
its own selection, scope, effect, version, budget and approval validation. It
does not call `Model::execute`, `preview`, `materialize` or `budget`. Only typed
records and canonical payload encoding are shared. It rejects malformed
successful commits, mismatched effects/counters, duplicates, stale approvals,
and final-state differences. Negative tests include a self-consistent forged
snapshot/history to ensure validation is more than equality against the model.
It treats trusted setup/approval events as witnesses and does not prove their
real-world authorization or audit authenticity. It checks successful decision
times; it does not reconstruct hidden preview time advancement. It preserves
rejections without claiming each denial was necessary. It has no concurrent
invocation/completion linearizability search.

## Ledger model

`ledger::Ledger` is separate from repairs; there is no claim that calling both
models forms one transaction. An instance represents one ledger/asset with
prefunded accounts, checked `u128` integer amounts and debit-funded available
balances. Operations: `open`, `reserve`, `capture`, `void`, `expire`. A reserve
moves available units to held units; capture posts to destination; void/expiry
release to source. One terminal resolution wins; identical terminal retries
return it. Same reserve ID/payload resolves once; altered payload conflicts.
Zero, same-account, missing account, insufficient funds and overflow reject
without partial effects. Expiry is checked in capture without a sweeper.
Conservation/hold reconstruction is tested independently in generated histories.
This is not financial qualification: immutable posting journals, reverse/direct
posting, multi-asset codecs, authorization and integration with business writes
remain unimplemented.
