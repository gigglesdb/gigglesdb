# Tested preview surface and limits

The inherited SQL subset is specified in ../giggles-core/SQL.md. Network schema
creation is deliberately unavailable: create tables through trusted local
provisioning, then assign each entire application table to exactly one certificate
authority. Identical-authority certificates may overlap during trusted local reload. There
is no row-level tenant sharing or generic task table grant.

| Surface | Implemented/tested | Rejected or remaining |
| --- | --- | --- |
| TLS/auth (mTLS listener; the [dev listener](README.md#quick-start-postgresql-clients) uses SCRAM-SHA-256 password authentication) | Mandatory mutual TLS, CA chain/time validation by rustls, exact SHA256 leaf binding, TLS hostname validation | Plaintext and password fallback on the mTLS listener, unbound/wrong-CA/no certificate; CRL/CA/server-certificate hot rotation remain; trusted live binding reload supported |
| Startup | SSLRequest then v3, fixed giggles user/database, UTF8, bounded application_name/DateStyle/TimeZone/extra_float_digits | Direct TLS, GSS, replication, options, alternate users/databases |
| Simple query | One inherited statement; typed rows; empty/comment-only queries; bounded JDBC initialization SET batches | General multi-statements, COPY, arbitrary SET, catalog SQL outside the bounded metadata profile |
| Extended | Parse/Bind/Describe/Execute/Sync/Close/Flush; named and unnamed objects; pipeline error drain to Sync | COPY, arbitrary PostgreSQL syntax |
| Binding | NULL; text/binary bool,int2,int4,int8,text,bytea; varchar for TEXT; integer wire widths with destination range checks; case-insensitive boolean text | Other OIDs, general implicit casts, text bytea escape format (only hex accepted); task OIDs remain exact |
| Describe | Parsed metadata only, without executing any write; domain/schema/transaction fences | Types not inferable from supported columns/LIMIT |
| Transactions | BEGIN / START TRANSACTION, COMMIT, ROLLBACK; implicit application SQL cycle commits at Sync; failure rolls back cycle; COMMIT of aborted transaction rolls back; conflict SQLSTATE40001 | Transaction options, isolation-level SET, savepoints, distributed transactions; task operations retain independent durable decisions |
| Portals | Bounded materialized rows; Execute max_rows/suspension; close on transaction end or idle Sync | Streaming unbounded results, server cursors across transactions |
| Task wire | Simple GIGGLES JSON plus finite typed prepared KEY/RANGE preview/execute and STATUS; same native redaction | Generic deliveries/application SQL, task transactions and role rebinding |
| Cancellation | Cooperative native/task Deadline and certificate-bound pg CancelRequest/BackendKeyData; pinned tokio-postgres0.7.15 with SSLRequest+mTLS | Before-decision57014 prevents this invocation publishing; after decision success/ambiguous IO is preserved. No plaintext cancel, no query-generation guarantee, no unsafe IO interruption |
| Outcome | Original task operation ID status/retry; bounded stable decision digest/count | General application idempotent commits/status remain absent; no exactly-once claim |
| Driver | Actual tokio-postgres0.7.15 with tokio-postgres-rustls0.13.0; actual pgJDBC42.7.3 initialization/query/validation/rollback, identity/schema/isolation and catalog metadata probes | Full DataGrip GUI browsing, libpq/psql, ODBC, ORMs and full PostgreSQL compatibility unqualified |

The read-only catalog adapter supplies actual application table/column/PK definitions
filtered by the current certificate grant. Prepared metadata and cached pages recheck
authority and schema epoch. JDBC metadata methods and a bounded set of captured
DataGrip 2026.2.5 discovery queries are exercised; full GUI introspection remains
unqualified. Unknown catalogs/functions fail explicitly. Empty catalogs are used
only for explicitly modeled absent features, such as procedural languages and
event triggers. See [catalog query limits](../giggles-pg-catalog/README.md). See [the driver conformance runner](../giggles-pg-conformance/README.md)
for reproducible per-case results, including failed and untested cases.

Captured DataGrip discovery includes role/member/ACL and absent-feature catalogs,
plus actual table, column, primary-key constraint and index definitions. `SHOW
DateStyle` reports ISO; `SHOW TimeZone` reports the accepted startup presentation
label. Those labels do not add timestamp or timezone-conversion support. The
startup-time expression returns the hosting service initialization epoch, and
`version()` identifies GigglesDB. PostgreSQL transaction-age/MVCC probes remain
explicitly unsupported; the observed optional StateNumber probe is ignored by
DataGrip. No synthetic transaction ID or lock-state marker is returned.

Catalog integer/OID parameter wire widths may differ only when no SELECT
projection contains parameters, including nested/UNION/CTE projections. Decoding
uses the declared width; the engine checks inferred-type ranges. Projected
parameters retain exact type agreement. Catalog vectors preserve zero-based
indexing and their actual wire OIDs; empty internal `"char"` values are distinct
from SQL NULL.

Stock pgJDBC cancellation sends plaintext CancelRequest and cannot cancel this
server's authenticated mTLS session. Do not rely on JDBC query timeout as proof
that a write stopped. The pinned Rust driver's TLS cancel transport is tested.
Each session accepts at most 1,024 named prepared statements, 1,024 named
portals and 1,024 cursors, and the SQL text of its named statements may total
max(16 MiB, twice the maximum statement size); exceeding either returns 54000
before anything runs. pgJDBC's default 256-entry cache fits; close unused
statements, since concurrent explicitly open statements still count. Ordinary prepared statements survive commits/rollbacks, with schema and
binding checks on reuse.

Joins and uncorrelated `IN (SELECT ...)` subqueries follow PostgreSQL's
per-relation rule: a non-superuser database role needs SELECT (owner or grant) on
every table the statement reads, plus the target's own privilege for UPDATE or
DELETE; otherwise the statement fails with 42501. An application role needs every
read table in its allowlist. The internal `giggles_join` marker still cannot be
granted as a table.

Idle PostgreSQL connections do not expire after ten seconds. Once a frame starts,
its remaining header/body must arrive within ten seconds. Idle sessions still
consume the bounded connection pool.

Frames: native request256KiB, response8MiB; pg startup8192bytes, post-startup frame
256KiB including four-byte length, aggregate response8MiB. Validate lengths before
allocation. Per listener1..128 connections, admitted before TLS; two configured
listeners share at most128 total by default (configurable1..128). Per session32 statements and32portals;
names63bytes, SQL64KiB. Parameter payloads retained by portals and materialized
results share one8MiB aggregate limit per session; results are admitted
before caching or emitting rows. A single bounded result can temporarily exceed retained
capacity before rejection. Prepared SQL is at most32x64KiB per session. Core
parameters256, row64KiB, result1024rows/1MiB raw values;8 staged transactions (legacy) or 128 transactions (v2) shared
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
refer to already committed operations. GD-0011 adds typed prepared task envelopes;
see PREPARED_TASKS.md for grammar, bounds and fresh portal replay semantics.
Durable cumulative task disclosure accounting is implemented; see TASK_ACCOUNTING.md.
GD13 live binding lifecycle/shared rate limits and exact capacity dimensions are
documented in CREDENTIALS_ADMISSION.md.
