# Authority and failure boundary

Filesystem ownership, offline config/issuer/approver and host time are trusted.
Mandatory rustls client verification authenticates a CA-issued client leaf, then
the exact leaf fingerprint selects an immutable configured role and database
domain. Client user/database strings, principal integers, role SET and certified
timestamps never confer authority. Empty bindings deny everyone. Config has no
default identity, password or privilege. Provisioning/approval use the existing
trusted local API, never the network JSON dispatcher. Certificate roles are fixed
until restart; revocation/rotation of a certificate binding requires disconnecting
its sessions and restarting with revised offline config. Task/approval revocation
is checked through the shared kernel without restart.

Application tables are exclusively assigned to one configured role. Its entire
table data is readable/writable via the finite inherited grammar. Internal/task
names and network DDL reject. Parsed statement targets are inspected; there is no
substring SQL allowlist. Every connection owns its own SQL staging, prepared
registry and portals; parse, bind, scope and metadata errors abort an active
transaction until ROLLBACK. Disconnect discards staging. Core prepared objects
retain domain, catalog epoch and exact creating-transaction lifetime fences.

Task identities can only preview/execute the finite repair shape or obtain their
own operation status. Scope/budget/exact approval validation remains in the same
atomic redb decision as business changes and receipts. Remote current-authority
APIs recheck task activity and the full trusted time interval under that same
serialization gate even for duplicate IDs/status. Native and task wire share a
service task gate to keep clock issuance in decision order. Existing trusted local
forensic status intentionally remains available after task revoke.

Time uses host UNIX seconds, SystemTime/Instant cross-checking, configured skew0..60s
and maximum gap1..3600s. Each sample persists its conservative upper bound in the
existing metadata last_time, with immediate redb durability. Both lower and upper
bounds must be within grant validity. Decreasing wall time, excess pause, clock
discontinuity, overflow or sample upper below persisted bound fails closed and
latches uncertainty until restart after correcting host time. No request supplies
authority time. Storage format is unchanged. Restart respects the existing bound.
Host accuracy and no excessive unobserved pause between sample and decision are
assumptions. Clock uncertainty is not a certified external time service: real
pause/skew/clock-accuracy production qualification is unfinished. Offline callers
can advance the shared bound; an erroneous future timestamp fences remote tasks.

The cancellation boundary is an atomic pending/cancelled/decided request fence
immediately before the existing redb business commit. A cancellation that wins
prevents publication of that invocation's business rows, task budgets, approval
consumption and operation receipt. Trusted intermediate clock metadata may still
advance. Once decision wins, cancellation cannot overwrite success, the original
policy result or ambiguous storage IO. Recover task writes using the original
OperationId/status after reopening fenced storage; a cancelled retry never proves
that an earlier attempt rolled back. Application SQL has no new durable operation
identifier and still requires application reconciliation after a missing ACK.

Core uses an explicit synchronous Cancellation scope with panic-safe thread-local
restoration. Trusted callers must create a fresh context per invocation and must
not return async work or hold the scope across await. Network workers enter the
scope only inside the owned synchronous closure. Core/service lock waits check
cooperatively; checks do not interrupt redb/OS IO or provide a storage latency bound.
Native Deadline includes queue wait but is discovered by decode after admission.
PostgreSQL cancellation observes waiting admission without submitting a worker;
started workers retain their32-pool permit through actual completion. Socket loss
and shutdown do not set cancellation or claim rollback.

PostgreSQL cancellation requires SSLRequest, mandatory mTLS and an exact configured
leaf fingerprint, identical to the target session binding in the same Service and
database authority. Backend PID/secret are generated from OS crypto randomness;
PID reuse is prohibited during a Service lifetime. Historical PIDs are capped at
65536; new backend registration then fails closed until restart. The32-bit secret
has the protocol's fixed width; certificate authentication is an additional boundary.
Closed socket tasks invalidate registration even if a worker continues. Wrong,
stale, cross-certificate and idle requests have no effect and return no details.
Fresh per-frame contexts prevent inherited cancellation. PostgreSQL has no query
generation in CancelRequest: a delayed request can cancel a later active frame on
the same live backend; applications must coordinate query/cancel reuse themselves.
Pinned tokio-postgres0.7.15 cancel_query/cancel_query_raw/connect_tls sources were
inspected and actual SSLRequest+mTLS cancellation tested. Plaintext and direct TLS
without SSLRequest on the pg listener remain rejected.

Trusted Admin::set_decision_hook is a deterministic test seam, never a network API
or environmental switch. It can pause real lock/decision/commit paths; local race
tests are not independent security or storage qualification.

GD-0009 moves native decode/dispatch/encode and PostgreSQL frame processing
(including prepare/describe, clock and core decisions) into a Service-shared
blocking pool capped at32 submitted jobs across both listeners. Admission awaits
before submission; each connected session retains at most one waiting frame.
The existing per-listener connection limits bound waiting frames. Original frame
receipt time survives queueing for native and simple task-wire deadlines. Session
and protocol state have one owner and are moved into and back out of each job.
Started jobs hold admission until actual completion even after socket shutdown;
shutdown does not wait for detached blocking decisions and does not promise their
rollback. Runtime teardown may wait for them. The synchronous Rust Session API
remains synchronous for trusted embedded callers. This is a count bound, not a
storage latency bound, rate limit, aggregate-memory qualification or fairness
guarantee. Session construction/drop takes no storage mutex; final database
teardown remains the trusted host's responsibility.

Task responses contain only version, stable code, original operation ID, count
and digest, with no full effects/rows/policies/dependencies/issuer identities or
internal error text. SHA256 is supplied by a maintained library, not custom
cryptography. Minimal output still reveals counts, correlation, existence within
the authorized task, timing and repeated observations. No cumulative read/egress
budget, rate-limited security audit, signed receipts or strong timing isolation is
implemented. Do not claim the parent's read/egress accounting contract. The
trusted approver must retrieve and review full exact effects locally; a count/digest
alone is not a human-readable approval review.

Generated test CA/client/server keys are explicitly TEST-ONLY fixtures. Do not use
them for deployment. Logs store no private keys or certificate PEM contents;
private fixture files remain intentionally local under unique artifact folders.
