# embedded relational contract (GD-0006)

[Design, implementation plan and work log](https://app.archdocs.io/design-docs/14d046e2-cf09-4309-a392-2a12650f000e).
This is a bounded embedded subset, not PostgreSQL or SQLite compatibility. The later P2 service preview is documented in [the server README](../giggles-server/README.md); this document retains the trusted embedded contract.

## Authority and entry points

Only a trusted `Admin` can obtain `sql_session()`. `SqlSession::execute(sql, params, expected_version)` runs one statement. `prepare` returns a process-local `Prepared`; `execute_prepared` binds typed values and checks database domain and catalog epoch, including transaction control handles. Handles prepared inside an explicit transaction are scoped to that exact transaction and expire on commit/rollback/drop; this prevents rollback-branch epoch/ID reuse from rebinding abandoned descriptors. Handles prepared outside transactions can be reused in compatible transactions. `catalog()` returns epoch and descriptors with stable globally allocated table/column IDs. `SqlSession::scan(Scan)` compiles a native ordered access request into the identical relational execution/transaction path.

Task `Executor` has no session, catalog or generic table method. Its existing `execute_sql` only recognizes the finite deliveries repair grammar and retains all scope/budget/dedupe/exact approval checks. Generic task grants, tenant/column table privileges, aliases, transactions and generic SQL are unsupported for Executor. Trusted filesystem/Admin access remains outside the task threat model. Do not expose the local admin protocol or principal/time assertions to remote clients.

CLI: `giggles admin PATH JSON` (or `@request-file`). Admin JSON supports:

```json
{"command":"sql","sql":"INSERT INTO app VALUES($1,$2)","params":[{"Int":-7},{"Text":"hello"}],"expected_version":null}
```

`{"command":"catalog"}` returns descriptors. `sql_batch` accepts up to 256 `{sql,params,expected_version}` objects; parameters default to empty within batch/session requests. Each object contains exactly one SQL statement. Include explicit BEGIN/COMMIT to make a batch atomic:

```json
{"command":"sql_batch","statements":[{"sql":"BEGIN"},{"sql":"CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT NOT NULL)"},{"sql":"INSERT INTO app VALUES($1,$2)","params":[{"Int":-7},{"Text":"hello"}]},{"sql":"COMMIT"}]}
```

One-shot Admin sql cannot retain a transaction: BEGIN returns Transaction and discards staging; use sql_batch or a persistent C/native session for controls. Batch without BEGIN uses autocommit per statement; a later failure does not undo earlier autocommits. An open transaction at end is discarded and returns Transaction. A batch failure drops the session and discards any staged transaction. CLI SQL errors exit3; policy denials exit2. JSON protocol version stays1; SQL codes are additive.

C: `gd_v1_sql_session(admin,&session)` accepts Admin only. Use existing `gd_v1_call(session, json, len, &buffer)` with `{ "sql":"BEGIN", "params":[] }`, subsequent statements and COMMIT/ROLLBACK. Existing buffer ownership, input limits and panic containment apply. Close session to rollback; it retains its Admin reference even if the original Admin handle closes. Executor and response handles cannot create sessions. Native Rust offers prepared handles and `Scan`; the C/CLI surface currently binds parameters per call, without a separate prepared-handle registry or native Scan opcode.

## Accepted grammar and semantics

ASCII unquoted identifiers fold to lowercase, max63bytes. No quoted identifiers. Qualified column references are supported on the bounded query path below. Reserved names include deliveries, tasks, operations, approvals, receipts, repair, internal, system, giggles, information_schema, pg_catalog and giggles_/system_/repair_/internal_/pg_ prefixes. Reserved names cannot shadow protected storage. Semicolon allowed only as a single trailing terminator; comments and multiple statements reject. Table aliases and AS projection aliases are supported on the bounded query path below. String literals preserve UTF8 and doubled quotes; no backslash escapes. Parameters are `$1..$256`, contiguous with no unused supplied arguments. Values use `Null`, `{"Bool":true}`, `{"Int":-1}`, `{"Text":"..."}`, `{"Bytes":[0,255]}`. Types are checked at execution against the descriptor; no implicit coercions or truncation.

| Surface | Supported in this local subset | Rejected or untested |
| --- | --- | --- |
| CREATE TABLE | Columns; inline single PK or table-level composite PK; NOT NULL/NULL | ALTER/DROP, defaults, sequences, secondary/unique/check/FK constraints, schema jobs |
| Types | BOOLEAN/BOOL; SMALLINT signed16; INTEGER/INT signed32; BIGINT signed64; UTF8 TEXT; BYTEA/BYTES; SQL NULL | Decimal, timestamp, date, UUID, JSON, floats and all other types explicitly unsupported |
| INSERT | One VALUES tuple; optional unique column list; omitted nullable columns become NULL | Multirow, INSERT SELECT, RETURNING, UPSERT |
| SELECT | Column list or star; full PK lookup; equality PK prefix and optional next-component lower/upper bound; bounded full ordered index scan | Legacy path rejects non-PK filters; bounded joins and aggregates described below; OR, subqueries, DISTINCT, OFFSET reject |
| Order | Legacy: full PK or remaining PK suffix, uniform direction; extended: bounded stable scalar sort below | Expressions, locale collations |
| UPDATE/DELETE | Full PK equality only; updates to non-PK columns; optional API expected row version | Range/global mutations, PK rewrite, expression updates, unbounded fallback |
| Transactions | BEGIN, COMMIT, ROLLBACK; read your writes, snapshot staging, conservative serial certification | Savepoints, nested transactions, task SQL transactions, distributed transactions |
| Prepared | Rust handle with domain/epoch/transaction fences; typed parameters validated on execute; describe returns parameter and result types | C prepared-handle registry, privilege caching |
| Recovery | Shared immediate redb commit; reopen, versioned export/restore, process-boundary checks | Device power loss/internal redb torn-write qualification |
| Client compatibility | Trusted native Rust, JSON CLI, actual version1 C ABI clients on current Windows host | pgwire, libpq, JDBC, drivers, ORMs, wire compatibility, other OS/filesystem qualification |

Every PK column becomes NOT NULL, including declarations that omit NOT NULL. Duplicate keys reject; INSERT is not replacement. No automatic IDs or gapless sequence promise. Row `version` is a durable globally allocated nonzero u64 changed on every successful INSERT/UPDATE (including equal-value updates). Delete/reinsert gets a later version; rollback need not consume an identifier. Expected version on missing row also rejects Version. CAS is accepted only for UPDATE/DELETE; apply other conditions through full PK predicates. Returned SELECT rows include version independent of projection.

Binary collation: bool false<true; signed integers numeric; TEXT compares UTF8 bytes; BYTES compares unsigned bytes. Canonical key encoding v1 uses type tags, signed integer sign-bit flip + big endian, and NUL escaping `(0,255)` terminated by `(0,0)` for text/bytes. Composite keys concatenate encodings; PK types cannot mix in one column. NULL has a canonical first-sorting tag but cannot appear in PKs. Legacy indexed predicates reject NULL key bounds. On the bounded query path, NULL comparisons never match; IS NULL and arbitrary three-valued expressions remain unsupported.

## Bounds and transactions

64KiB SQL,4096tokens,64columns,128tables,256parameters;64KiB total parameter payload/row;1024rows per scan and1MiB selected raw value bytes. On the legacy simple SELECT path, full PK lookup can omit LIMIT (at most one row); other SELECTs require LIMIT0..1024 (literal or typed integer parameter). The extended path has independent candidate/intermediate caps described below. Mutations never fall back to scanning. Native Scan has the same limits, index bounds, projection, versions and transaction rules. Result limit errors return no partial result. These caps are distinct from the1MiB JSON input and64MiB ABI output envelope.

Whole statement execution has its own admission counter held until the call returns, bounded by configured Limits.admitted (default32), covering transient autocommit state copies. At most8explicit staged transactions per shared Admin kernel; no client call retains a storage mutex or admission ticket. Snapshot BEGIN copies the entire bounded State; a statement evaluates against a private copy and only replaces staged state after success, including export-envelope validation. Any statement/parse/bind/native scan failure aborts an explicit transaction; COMMIT/queries then return Aborted until ROLLBACK (or native `rollback()`/drop). A transport JSON parse failure does not execute a statement. COMMIT compares the durable shared generation under the same gate as repair decisions, so intervening catalog/application/repair decisions conflict. Conflicts abort and require rollback/retry from BEGIN. Abandoned sessions release slots on drop. Autocommit SELECT is a point-in-time read and causes no write. Explicit read-only COMMIT still certifies against the shared generation. Denied repair decisions and failed conflict decisions may also advance generation conservatively.

No predicate/write-skew escape: any intervening decision, including an otherwise unrelated write, conflicts. This is conservative optimistic serial certification, a P1 local implementation choice rather than the parent's proposed distributed locking algorithm. It may starve under sustained contention; no retry fairness or latency claim is made. Do not hold application locks while waiting for other sessions. Admission failure is Busy, not a queued transaction.

## Persistence and integration limits

Live metadata format2 plus namespace5 relational payload format1 contains catalog/epoch/allocators/generation and all relational rows. Existing namespaces0..4 still contain repair metadata, rows, counters and decisions. The same redb immediate transaction persists all of them. SQL DELETE replaces the namespace5 value; no independent sidecar or filesystem cleanup exists. Old format1 live databases and exports remain readable; open does not rewrite logical data. A subsequent durable decision writes format2. Old binaries reject2 before decoding new namespaces. Legacy states at the16MiB envelope boundary may lack room for the added catalog payload: migration/export then rejects Limit without replacing the prior state. Keep originals; there is no downgrade writer.

Restore keeps catalog IDs/epochs/versions/allocators and uses a fresh database domain; old prepared handles reject, old task/approval authority is revoked, and prior receipts remain forensic. This is not multi-cluster identity or remote credential qualification. Export headers accept versions1/2 and verify length/checksum/canonical records; unsupported formats fail closed.

The adapter still loads/validates the entire State and rewrites every current record for each durable decision, with a16MiB complete export-envelope bound. Ordered SQL/native scans seek an in-memory BTreeMap range after that load; they are not selective on-disk reads or scalable storage qualification. Eight staged snapshots (the legacy transaction cap; v2 admits 128 transactions without State copies) plus transient copies remain material memory costs. No general application receipt/idempotency or ambiguous SQL commit-status API exists yet: a storage error fences the handle and SQL outcome is unknown. Reopen and inspect application state; blindly retrying a non-idempotent logical workflow is not qualified. The finite repair API still provides its original operation-ID status/retry contract.

P2 consumers can use native session/prepare/scan APIs after separately implementing authenticated authority, physical time, credential issuance, output disclosure and resource accounting. Generic task SQL remains fail closed until per-table/tenant/column/operation grants and their bypass tests are separately designed. No remote endpoint has been added.

SQL error codes200..216: Unsupported,Syntax,Reserved,UnknownTable,UnknownColumn,Duplicate,Type,Null,Parameters,Limit,Unbounded,Version,Conflict,StaleSchema,Transaction,Aborted,Overflow respectively. Core storage/fencing codes10..16 and existing policy100..117 remain distinct. Errors describe rejection, not PostgreSQL SQLSTATE compatibility.

## Bounded joins, global aggregates and EXPLAIN (GD-0033)

The extended query path accepts qualified columns (`p.id`), explicit or implicit table aliases (`people AS p` or `people p`), and projection aliases with `AS`. Once a source is aliased, its original name is out of scope. Unqualified references must resolve to exactly one source; ambiguous names and duplicate aliases return `Duplicate`. Unknown columns/scopes return `UnknownColumn`. All sources and expressions bind before evaluation, including empty inputs and LIMIT 0. String literal contents never act as SQL keywords.

`INNER JOIN` (or `JOIN`) and `LEFT [OUTER] JOIN` use indexed nested loops. Each right source must have equality bindings for **every primary-key column**, supplied by columns from preceding sources with the same declared type. Conjunctions may include further typed column equalities. Composite keys, repeated foreign keys, self joins with distinct aliases, and chains of up to eight sources work. A nonindexed right join rejects `Unbounded`; cross products, RIGHT/FULL/CROSS joins, inequalities in ON and unsupported expressions reject. NULL never equals NULL. Unmatched LEFT rows project NULL values with nullable column metadata. This is a local snapshot operator, with no distributed execution claim.

```sql
SELECT j.id, p.label AS owner
FROM jobs j LEFT JOIN people p ON p.id = j.person
ORDER BY j.id LIMIT 100;

SELECT COUNT(*), COUNT(j.wage), SUM(j.wage), MIN(j.wage), MAX(j.wage)
FROM jobs j JOIN people p ON p.id = j.person;
```

Global `COUNT(*)`, `COUNT(column)`, `SUM`, `MIN` and `MAX` accept a projection list containing only aggregates. COUNT returns BIGINT and ignores NULL for COUNT(column). SUM accepts integer columns and returns checked signed64 BIGINT; overflow returns `Overflow`, even with LIMIT 0. MIN/MAX preserve the scalar input type and binary ordering. NULL inputs are ignored; empty/all-NULL SUM/MIN/MAX return NULL and COUNT returns zero. GROUP BY, HAVING, DISTINCT, windows and mixed aggregate/nonaggregate projections reject explicitly. Aggregate output aliases are accepted; aggregate ORDER BY is unsupported.

The extended path scans at most1024 retained base/intermediate rows and1MiB raw values per intermediate stage, with a total4096 base-candidate visits plus indexed probes. The cap includes work before WHERE filtering and LIMIT; additional rows or oversized projected output return `Limit` with no partial result. The checked1MiB projected-byte cap applies before LIMIT, including LIMIT 0. Query syntax/parameter caps remain the same. Legacy simple SELECT/native Scan retains its existing ordered-page behavior. The extended path permits omitted LIMIT because it rejects exhausted resource caps; it supports bounded stable sorting by bound source columns, with NULL first ascending and last descending. No arbitrary expression or projection-alias ORDER BY resolution is provided.

Legacy and extended single-source rows retain their durable row version. Joined and aggregate rows use **version0**, identifying a synthetic result with no writable row identity. CAS stays restricted to point UPDATE/DELETE; no C/wire/result struct or persisted format changes were made. All operations use the existing SqlSession snapshot, read-your-writes and conservative generation-conflict rules. Existing cancellation checks run at session admission and staging boundaries; these bounded operators do not add per-row cancellation polling.

`EXPLAIN SELECT ...` binds the same physical plan and returns a `plan` TEXT column with `PrimaryKeyLookup`, `PrimaryKeyRangeScan` or `EmptyInput`, `InnerPrimaryKeyNestedLoop`/`LeftPrimaryKeyNestedLoop`, `GlobalAggregate`, `BoundedStableSort`, `OutputLimit` and resource caps as applicable. It reads no user rows, exposes no cardinality or literal values, and never executes a mutation. `EXPLAIN ANALYZE` and `EXPLAIN` of mutations reject. This is descriptive physical planning, without runtime statistics or cost estimates.

The unchanged `Prepared::table()` API has only one target slot. Distinct-table extended queries return the reserved sentinel `giggles_join`, rather than presenting only one source to existing table allowlists. Self joins return their single table name. Gateways must not authorize a statement from `table()` alone: `Prepared::read_tables()` lists every relation a statement reads (all FROM/JOIN sources and every table read by an uncorrelated `IN (SELECT ...)` subquery, at any depth). The server requires SELECT on each of them, as owner or by grant, for non-superuser database roles, in addition to the target's own privilege; application roles need every read table in their allowlist. A sourceless query that is not table-free still returns the sentinel with no read tables and stays denied to non-superusers.

For persistent CLI use, create a fresh path once (`giggles create PATH`), then run `giggles admin PATH @seed.json` with this request file:

```json
{"command":"sql_batch","statements":[{"sql":"BEGIN"},{"sql":"CREATE TABLE owners (id BIGINT PRIMARY KEY, label TEXT)"},{"sql":"CREATE TABLE entries (id BIGINT PRIMARY KEY, owner BIGINT, amount BIGINT)"},{"sql":"INSERT INTO owners VALUES(1,'persisted')"},{"sql":"INSERT INTO entries VALUES(1,1,12)"},{"sql":"INSERT INTO entries VALUES(2,1,-3)"},{"sql":"COMMIT"}]}
```

Each subsequent `giggles admin PATH @query.json` invocation reopens the database. This request returns typed values `[{"Int":2},{"Int":9}]` in one synthetic row:

```json
{"command":"sql","sql":"SELECT COUNT(*),SUM(e.amount) FROM entries e JOIN owners o ON o.id=e.owner","params":[],"expected_version":null}
```

The subprocess tests exercise these persistent reopen requests and rejected-query rollback through the actual `giggles` binary. These are local functional checks; production, security, cloud, performance and PostgreSQL-comparator qualification remain outside this deliverable.
