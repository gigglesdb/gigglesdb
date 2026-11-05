# Tested preview surface and limits

The inherited SQL subset is specified in ../giggles-core/SQL.md. Network schema
creation is deliberately unavailable: create tables through trusted local
provisioning, then assign each entire application table to exactly one certificate
binding. Multiple connections with that certificate can share its tables. There
is no row-level tenant sharing or generic task table grant.

| Surface | Implemented/tested | Rejected or remaining |
| --- | --- | --- |
| TLS/auth | Mandatory mutual TLS, CA chain/time validation by rustls, exact SHA256 leaf binding, TLS hostname validation | Plaintext, password fallback, unbound/wrong-CA/no certificate; CRL/online rotation remain |
| Startup | SSLRequest then v3, fixed giggles user/database, UTF8 and application_name | Direct TLS, GSS, replication, options, alternate users/databases |
| Simple query | One inherited statement; typed result description and text rows; empty query | Multi-statements, COPY, arbitrary SET, catalog/system SQL |
| Extended | Parse/Bind/Describe/Execute/Sync/Close/Flush; named and unnamed objects; pipeline error drain to Sync | Extended task envelopes, COPY, arbitrary PostgreSQL syntax |
| Binding | Inferred or matching explicit OIDs; NULL; text/binary bool,int2,int4,int8,text,bytea | All other OIDs, implicit casts, text bytea escape format (only hex accepted) |
| Describe | Parsed metadata only, without executing any write; domain/schema/transaction fences | Types not inferable from supported columns/LIMIT |
| Transactions | BEGIN / START TRANSACTION, COMMIT, ROLLBACK; abort until rollback, conflict SQLSTATE40001 | Transaction options, isolation-level SET, savepoints, distributed transactions |
| Portals | Bounded materialized rows; Execute max_rows/suspension; close on transaction end or idle Sync | Streaming unbounded results, server cursors across transactions |
| Task wire | Simple GIGGLES JSON envelope with explicit operation/approval IDs; same native response | Generic deliveries/application SQL, task transactions and role rebinding |
| Cancellation | Cooperative native/task Deadline and certificate-bound pg CancelRequest/BackendKeyData; pinned tokio-postgres0.7.15 with SSLRequest+mTLS | Before-decision57014 prevents this invocation publishing; after decision success/ambiguous IO is preserved. No plaintext cancel, no query-generation guarantee, no unsafe IO interruption |
| Outcome | Original task operation ID status/retry; bounded stable decision digest/count | General application idempotent commits/status remain absent; no exactly-once claim |
| Driver | Actual tokio-postgres0.7.15 with tokio-postgres-rustls0.13.0 over local sockets | libpq/psql,JDBC,ODBC,ORMs, other clients or full PostgreSQL compatibility unqualified |

Frames: native request256KiB, response8MiB; pg startup8192bytes, post-startup frame
256KiB including four-byte length, aggregate response8MiB. Validate lengths before
allocation. Per listener1..128 connections, admitted before TLS; two configured
listeners therefore allow at most256 total. Per session32 statements and32portals;
names63bytes, SQL64KiB. Parameter payloads retained by portals and materialized
results each have an8MiB aggregate limit per session, checked before/after their
respective allocation. A single bounded result can temporarily exceed retained
capacity before rejection. Prepared SQL is at most32x64KiB per session. Core
parameters256, row64KiB, result1024rows/1MiB raw values;8 staged transactions shared
by the database; admitted core calls default32; whole persisted State16MiB.
These finite limits are not a low-memory or overload-latency qualification.

SQLSTATE errors are stable categories, with fixed redacted messages. 28000 auth,
42501 scope/revoke/expiry,55000 uncertain time,GD001 approval,GD002 operation ID
conflict,54000 limits/budget,53300 admission,08007 storage/fenced ambiguous outcome,
25P02 aborted transaction,40001 optimistic conflict. Internal storage paths and
trusted receipt/admin exception strings are never returned.

An absent task operation is not a promise a concurrent invocation cannot commit.
After a missing response, retry/status only the original operation ID and original
payload. Revoke/expiry denies even completed remote status/retry; a trusted local
administrator can inspect the forensic outcome. Application writes/COMMIT with a
lost response or08007 have an ambiguous outcome. Drop/reopen or reconnect, inspect
business state with an application-specific reconciliation key or trusted
operator, and decide how to reconcile. Never blindly replay an ambiguous write.

Network fixtures and fault injection establish local behavior only. Device power
loss, real skew/suspend, full-disk stress, independent security/correctness review,
production packaging, wider client/OS and cloud qualification remain separate.

GD-0009: network synchronous work is isolated behind32 shared blocking permits.
Native/task-simple-wire deadlines include blocking admission wait, checked after decoding.
Wire formats, persistence and C ABI are unchanged. Rust helper `pgwire::run` now
takes ownership of `Session` rather than a mutable borrow so it can transfer the
session safely to a blocking worker; direct Rust callers must pass the session
by value. No broader Rust source compatibility promise is made for this0.1 crate.
GD-0010 adds the cancellation decision fence and authenticated CancelRequest,
with a fresh active context per frame and crypto backend keys. See SECURITY.md
for the delayed-packet/query-generation limit and65536-session Service lifetime
cap. Native has no new Cancel endpoint; Deadline is its cooperative equivalent.
Task OperationId keeps its original dedupe/status meaning; cancelled retries may
refer to already committed operations. Prepared task envelopes and durable
disclosure accounting remain unimplemented.
