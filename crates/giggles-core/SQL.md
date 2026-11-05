# embedded relational contract (GD-0006)

[Design, implementation plan and work log](https://app.archdocs.io/design-docs/14d046e2-cf09-4309-a392-2a12650f000e).
This is a bounded embedded subset, not PostgreSQL or SQLite compatibility. P2 has not started.

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

ASCII unquoted identifiers fold to lowercase, max63bytes. No quoted/qualified names. Reserved names include deliveries, tasks, operations, approvals, receipts, repair, internal, system, giggles, information_schema, pg_catalog and giggles_/system_/repair_/internal_/pg_ prefixes. Reserved names cannot shadow protected storage. Semicolon allowed only as a single trailing terminator; comments, aliases and multiple statements reject. String literals preserve UTF8 and doubled quotes; no backslash escapes. Parameters are `$1..$256`, contiguous with no unused supplied arguments. Values use `Null`, `{"Bool":true}`, `{"Int":-1}`, `{"Text":"..."}`, `{"Bytes":[0,255]}`. Types are checked at execution against the descriptor; no implicit coercions or truncation.

| Surface | Supported in this local subset | Rejected or untested |
| --- | --- | --- |
| CREATE TABLE | Columns; inline single PK or table-level composite PK; NOT NULL/NULL | ALTER/DROP, defaults, sequences, secondary/unique/check/FK constraints, schema jobs |
| Types | BOOLEAN/BOOL; SMALLINT signed16; INTEGER/INT signed32; BIGINT signed64; UTF8 TEXT; BYTEA/BYTES; SQL NULL | Decimal, timestamp, date, UUID, JSON, floats and all other types explicitly unsupported |
| INSERT | One VALUES tuple; optional unique column list; omitted nullable columns become NULL | Multirow, INSERT SELECT, RETURNING, UPSERT |
| SELECT | Column list or star; full PK lookup; equality PK prefix and optional next-component lower/upper bound; bounded full ordered index scan | Non-PK filters, OR, expressions, joins/subqueries, aggregates, DISTINCT, OFFSET |
| Order | Full PK or remaining PK suffix after equality prefix, all ASC or all DESC; default full PK ascending | Arbitrary sorts, mixed directions, locale collations |
| UPDATE/DELETE | Full PK equality only; updates to non-PK columns; optional API expected row version | Range/global mutations, PK rewrite, expression updates, unbounded fallback |
| Transactions | BEGIN, COMMIT, ROLLBACK; read your writes, snapshot staging, conservative serial certification | Savepoints, nested transactions, task SQL transactions, distributed transactions |
| Prepared | Rust handle with parsed statement and domain/epoch fence; typed parameters validated on execute | Parameter type introspection, C prepared-handle registry, privilege caching |
| Recovery | Shared immediate redb commit; reopen, versioned export/restore, process-boundary checks | Device power loss/internal redb torn-write qualification |
| Client compatibility | Trusted native Rust, JSON CLI, actual version1 C ABI clients on current Windows host | pgwire, libpq, JDBC, drivers, ORMs, wire compatibility, other OS/filesystem qualification |

Every PK column becomes NOT NULL, including declarations that omit NOT NULL. Duplicate keys reject; INSERT is not replacement. No automatic IDs or gapless sequence promise. Row `version` is a durable globally allocated nonzero u64 changed on every successful INSERT/UPDATE (including equal-value updates). Delete/reinsert gets a later version; rollback need not consume an identifier. Expected version on missing row also rejects Version. CAS is accepted only for UPDATE/DELETE; apply other conditions through full PK predicates. Returned SELECT rows include version independent of projection.

Binary collation: bool false<true; signed integers numeric; TEXT compares UTF8 bytes; BYTES compares unsigned bytes. Canonical key encoding v1 uses type tags, signed integer sign-bit flip + big endian, and NUL escaping `(0,255)` terminated by `(0,0)` for text/bytes. Composite keys concatenate encodings; PK types cannot mix in one column. NULL has a canonical first-sorting tag but cannot appear in PKs. SQL NULL comparisons/IS NULL and arbitrary three-valued expressions are unsupported; NULL is a value for nullable non-PK columns only.

## Bounds and transactions

64KiB SQL,4096tokens,64columns,128tables,256parameters;64KiB total parameter payload/row;1024rows per scan and1MiB selected raw value bytes. Full PK SELECT can omit LIMIT (at most one row); all other SELECTs require LIMIT0..1024 (literal or typed integer parameter). Mutations never fall back to scanning. Native Scan has the same limits, index bounds, projection, versions and transaction rules. Result limit errors return no partial result. These caps are distinct from the1MiB JSON input and64MiB ABI output envelope.

Whole statement execution has its own admission counter held until the call returns, bounded by configured Limits.admitted (default32), covering transient autocommit state copies. At most8explicit staged transactions per shared Admin kernel; no client call retains a storage mutex or admission ticket. Snapshot BEGIN copies the entire bounded State; a statement evaluates against a private copy and only replaces staged state after success, including export-envelope validation. Any statement/parse/bind/native scan failure aborts an explicit transaction; COMMIT/queries then return Aborted until ROLLBACK (or native `rollback()`/drop). A transport JSON parse failure does not execute a statement. COMMIT compares the durable shared generation under the same gate as repair decisions, so intervening catalog/application/repair decisions conflict. Conflicts abort and require rollback/retry from BEGIN. Abandoned sessions release slots on drop. Autocommit SELECT is a point-in-time read and causes no write. Explicit read-only COMMIT still certifies against the shared generation. Denied repair decisions and failed conflict decisions may also advance generation conservatively.

No predicate/write-skew escape: any intervening decision, including an otherwise unrelated write, conflicts. This is conservative optimistic serial certification, a P1 local implementation choice rather than the parent's proposed distributed locking algorithm. It may starve under sustained contention; no retry fairness or latency claim is made. Do not hold application locks while waiting for other sessions. Admission failure is Busy, not a queued transaction.

## Persistence and integration limits

Live metadata format2 plus namespace5 relational payload format1 contains catalog/epoch/allocators/generation and all relational rows. Existing namespaces0..4 still contain repair metadata, rows, counters and decisions. The same redb immediate transaction persists all of them. SQL DELETE replaces the namespace5 value; no independent sidecar or filesystem cleanup exists. Old format1 live databases and exports remain readable; open does not rewrite logical data. A subsequent durable decision writes format2. Old binaries reject2 before decoding new namespaces. Legacy states at the16MiB envelope boundary may lack room for the added catalog payload: migration/export then rejects Limit without replacing the prior state. Keep originals; there is no downgrade writer.

Restore keeps catalog IDs/epochs/versions/allocators and uses a fresh database domain; old prepared handles reject, old task/approval authority is revoked, and prior receipts remain forensic. This is not multi-cluster identity or remote credential qualification. Export headers accept versions1/2 and verify length/checksum/canonical records; unsupported formats fail closed.

The adapter still loads/validates the entire State and rewrites every current record for each durable decision, with a16MiB complete export-envelope bound. Ordered SQL/native scans seek an in-memory BTreeMap range after that load; they are not selective on-disk reads or scalable storage qualification. Eight staged snapshots plus transient copies remain material memory costs. No general application receipt/idempotency or ambiguous SQL commit-status API exists yet: a storage error fences the handle and SQL outcome is unknown. Reopen and inspect application state; blindly retrying a non-idempotent logical workflow is not qualified. The finite repair API still provides its original operation-ID status/retry contract.

P2 consumers can use native session/prepare/scan APIs after separately implementing authenticated authority, physical time, credential issuance, output disclosure and resource accounting. Generic task SQL remains fail closed until per-table/tenant/column/operation grants and their bypass tests are separately designed. No remote endpoint has been added.

SQL error codes200..216: Unsupported,Syntax,Reserved,UnknownTable,UnknownColumn,Duplicate,Type,Null,Parameters,Limit,Unbounded,Version,Conflict,StaleSchema,Transaction,Aborted,Overflow respectively. Core storage/fencing codes10..16 and existing policy100..117 remain distinct. Errors describe rejection, not PostgreSQL SQLSTATE compatibility.
