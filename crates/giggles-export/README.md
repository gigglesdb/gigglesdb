# giggles-export

A logical relational export and fresh-import library and CLI for trusted local database owners.

**Status:** Uses the bounded `giggles-logical-v1` format; it is not a physical backup or an unbounded migration tool.

Part of [GigglesDB](../../README.md).

Local trusted-owner logical relational export and fresh import, format `giggles-logical-v1`.
This standalone Rust crate uses the public core `Admin`, `SqlSession`, catalog and `Scan` APIs.

```powershell
$env:CARGO_TARGET_DIR='crates\giggles-export\target-export'
$env:CARGO_BUILD_JOBS='1'
cargo run --manifest-path crates/giggles-export/Cargo.toml -j1 -- export SOURCE_DIRECTORY output.jsonl
cargo run --manifest-path crates/giggles-export/Cargo.toml -j1 -- verify output.jsonl
cargo run --manifest-path crates/giggles-export/Cargo.toml -j1 -- import output.jsonl NEW_DIRECTORY
```

Commands report JSON on success and exit nonzero on failure. Export opens an existing database.
Import requires a nonexistent directory and never overwrites or resumes an existing directory.
The source must be accessible through core's exclusive local database open; this is not a remote server command.

The library offers `capture`, `encode`, `decode`, `export`, `verify`, `import` and explicit
`*_with_hook` checkpoint variants for trusted fault testing. `Bounds` can narrow the profile.
`Archive` owns logical schemas and typed values; schema IDs, row versions and authorities are absent.

## Format and limits

One UTF-8 JSONL header gives format version, profile and name-ordered tables, with column names,
core column types, nullability and ordered primary-key column indices. One row record gives its
table index and values. Rows use core memcomparable composite primary-key order, binary text
collation, without normalization. A final manifest gives per-table counts, total count and SHA-256
of every preceding byte (including newlines). `Report.sha256` hashes the entire archive including
the manifest. Neither hash is a signature or an authenticity check.

Cells have explicit tags, e.g. `{"type":"null"}`, `{"type":"int","value":"-9223372036854775808"}`,
`{"type":"bool","value":true}`, `{"type":"text","value":"雪"}` and
`{"type":"bytes","value":"00ff"}`. Empty bytes/text differ from NULL. JSON integers are not used
for relational integer values. Booleans, SMALLINT, INTEGER, BIGINT, TEXT and BYTEA preserve exact
core values and ranges. Columns/schema use the core type names `Boolean`, `SmallInt`, `Integer`,
`BigInt`, `Text`, `Bytes`. Core identifiers are lowercase ASCII, at most 63 bytes. Reserved core
column names reject; table names additionally reject internal application names/prefixes.

Canonical line layout is the order emitted by `encode` (record tag first, then declared fields,
compact serde_json escaping, LF terminated). A decoder compares every line with this canonical
encoding. Unknown/duplicate fields, alternate escaping/whitespace/field order, extra records,
missing footer/newline, unsupported version, malformed hex/decimal, duplicate keys, bad ordering,
schema/type/null/range errors, digest/count mismatches and oversized inputs fail closed. The library
encoder is the reference producer for interoperability; arbitrary JSON serializations are not accepted.

Hard ceilings: 16 MiB file, 512 KiB line including LF, 128 tables, 64 columns/table,
1,024 rows/table, 131,072 rows total, 64 KiB logical row payload. Header size and core's 1 MiB scan
result cap can reject combinations below these ceilings. Full scans at the row cap use same-snapshot
successor probes to reject hidden additional rows. The format is bounded and materialized in memory;
it is not an unbounded streaming migration. Encoded retained capture rows are charged against the file
bound as they are collected; one bounded core scan result and encoding buffers also occupy memory.

## Consistency and failure behavior

Export brackets a single core `BEGIN` with equal catalog epoch/descriptor reads. The epoch is
monotonic in this core profile; equality certifies the catalog belongs to the snapshot. Every table
scan and overflow probe uses that transaction. Concurrent DML/DDL after BEGIN cannot mix table cuts.
A bracket race fails. Export rolls back its read transaction; it never commits captured state back into the source.

Export uses a unique sibling `create_new` staging file, writes and syncs it, decodes and compares it,
then publishes the final path with an atomic no-clobber hard link. The staging file remains even on
success. Hard links require a supporting filesystem. POSIX parent directories are synced; Windows
uses file sync and atomic hard-link publication without a portable parent-directory fsync guarantee.
This is tested process-interruption behavior, not universal power-loss or filesystem qualification.
Never treat `.staging` names as published exports. An error after publication can leave a valid final
archive; verify it explicitly. Existing output files, including empty files and racing creates, reject.

Import validates the whole archive before creating the target, creates schema and rows in one core
transaction, commits, closes/reopens and compares the full typed contents before publishing
`logical-import-complete.json`. The marker is synced and published no-clobber; it contains the report.
A failed install retains its isolated directory, possibly with fully committed data, but no completion
marker. Direct core opens can see that directory; callers must honor the marker contract. Restart by
choosing a new target; there is no automatic resume, overwrite or cleanup. Verification faults before
marker publication never report success. All test fixtures retain unique `std::env::temp_dir()` paths.

## Scope

This is a logical relational copy with a fresh storage domain, descriptor IDs, row versions and
catalog epoch. It does not preserve task grants, approvals, receipts, legacy repair rows, operational
state, ledger/outbox wrapper semantics, snapshots/cursors or replication history. Capture rejects
observed nonempty legacy rows/tasks/operations/approvals and relational internal names including
`ledger_state`, `outbox_internal_state`, task/receipt/approval/operation and consensus/membership
prefixes. These conservative name checks can reject ordinary user tables. Do not rename internal
tables to bypass them; arbitrary application semantics cannot be inferred from a catalog.

Physical backup/restore remains in `giggles-ops`. This crate makes no operational restore equivalence,
Scylla compatibility, real-source migration, distributed-cut, cloud, performance, security or production
qualification claim. PostgreSQL CONTROL remains deferred. Checksums detect corruption, not hostile
replacement, and output contains plaintext application data under host filesystem permissions.

## Usage from sh

From the repository root:

```sh
cargo run --manifest-path crates/giggles-export/Cargo.toml -- verify output.jsonl
```
