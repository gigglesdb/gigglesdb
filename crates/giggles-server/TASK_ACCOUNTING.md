# Durable task accounting v1 / storage format 3

Design and exact local evidence: [GD12](https://app.archdocs.io/design-docs/d1147fc9-5510-4d4e-8dc3-c67bdfdb65c6).
This is a local implementation contract, not release or security qualification.

Task grants retain the original repair/P0 layout. A separate format-3 namespace
stores immutable limits and monotonic used counters for each task. New grants
have **no** remote disclosure allowance until a trusted Admin provisions them.
Formats 1/2 remain readable, migrate on writes, and default to no allowance.
Older binaries reject format 3. Unknown formats, malformed accounting records,
unknown tasks, used > limit and export header/metadata mismatches reject.

Trusted Rust API:

```rust,ignore
admin.provision_task_resources(task, TaskResources {
    read_rows: 1000, disclosed_rows: 100, encoded_bytes: 2 * 1024 * 1024,
})?;
let accounting = admin.task_resources(task)?;
```

Trusted local CLI and C admin JSON dispatch (never remote task dispatch):

```json
{"command":"provision_task_resources","task":1,"limits":{"read_rows":1000,"disclosed_rows":100,"encoded_bytes":2097152}}
```

```json
{"command":"task_resources","task":1}
```

Use `giggles admin PATH @INPUT_FILE`. Provisioning is one-time, even for zero
limits; a second call fails TaskExists. Tasks cannot inspect or provision caps.
Trusted embedded/FFI executor calls remain the original P0-compatible local
model, expose full receipts, and are not the remote untrusted-agent boundary.
Do not expose that dispatcher over a listener.

## Units and order

All three units and arithmetic are u64; no narrowing to usize is used for caps.

| Counter | Conservative reservation |
| --- | --- |
| read_rows | Explicit input keys, at least one; larger cached effect count + one when applicable. Predicate: 3N+1 for the entire repair row set, covering selection-bound scan, predicate scan and selected-row visits. Status: cached effects + one, absent/denied status at least one. |
| disclosed_rows | One redacted decision envelope per release, including an empty preview, absent status, denial or task Ping. It counts logical result envelopes, not underlying changed rows. |
| encoded_bytes | Canonical UTF-8 Response JSON length + 1024 bytes per release. |

Read admission is durably persisted **before** protected evaluation, with the
same core serialization lock held through evaluation so the bound cannot race.
Reads that reject, are cancelled after admission or fail delivery can spend
read allowance. These are logical examinations, not all physical redb reads:
the current engine deserializes its bounded state, including internal metadata.
No physical scan/CPU/time quota is claimed by this counter.

Business mutation, dedupe, exact approvals and original receipt remain their
own durable decision. The service then freezes/serializes its immutable Response
and obtains fresh current credential/time/revoke checks and an atomic durable
disclosure/byte reservation. All native, simple GIGGLES and extended task Execute
paths use this same gate. Retries, status, repeated bound-portal Execute,
reconnect and service restart spend again. A failed reservation never releases
the protected Response. No transport may add protected fields after reservation.

The byte allowance bounds supported native length framing and PostgreSQL decision
row, description, completion and ReadyForQuery overhead. TLS records, TCP/IP,
retransmissions and other physical network overhead are excluded. Fixed protocol
metadata, syntactic errors, invalid bindings and safe rejection envelopes contain
no task outcome and use existing protocol bounds outside protected accounting.
Infinite fixed rejections still need rate controls, which remain separate work.

Failed release returns only a bounded envelope with code 42501, or 57014 for
cancellation / 55000 for uncertain time. It contains no operation/count/digest,
remaining counters or original decision. A read/mutation rejection whose response
is itself successfully reserved may retain its bounded policy code. None of
these responses proves a preceding business operation rolled back.

Cancellation before business decision can abort it; after decision the existing
invocation cancellation fence is sealed. Late cancellation cannot undo business
commit or bypass the mandatory disclosure reservation. A read/status invocation
without a business decision remains cancellable up to its disclosure decision.
Once that decision wins, cancellation and ambiguous socket writes never refund.
Ambiguous persistence fences the handle and produces no protected output.

## Recovery and limits of the boundary

Exports retain accounting and original task identity. Restore preserves counters,
creates a new database domain and revokes inherited tasks/approvals. It does not
grant another spendable allowance. Trusted provisioning cannot reset existing
accounting. A file owner copying/rolling back live database files remains outside
the threat boundary; restoring an earlier timeline requires operational fencing.

No timing-side-channel elimination, physical network quota, certificate lifecycle,
rate-bound, production, cloud, power-loss or comparative qualification is claimed.
Server clock accuracy and storage Immediate durability retain their existing
documented assumptions.
