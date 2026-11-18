# giggles-migrate

An offline migration rehearsal tool for contributors testing normalized input against a real GigglesDB SQL destination.

**Status:** Supports bounded normalized imports; it does not connect to ScyllaDB, fence writers or transfer authority.

Part of [GigglesDB](../../README.md).

A small **normalized offline migration rehearsal** with actual GigglesDB SQL destination effects. It does not connect to ScyllaDB, fence source writers, transfer authority, run reverse capture, or qualify a production migration.

## Quick start

From the repository root in PowerShell:

```powershell
$env:CARGO_BUILD_JOBS='2'
$env:CARGO_TARGET_DIR="<work-dir>/target"
$manifest='crates/giggles-migrate/examples/manifest.json'
$chunks='crates/giggles-migrate/examples/chunks.json'
$db='<work-dir>/fresh-rehearsal-db'
cargo run --manifest-path crates/giggles-migrate/Cargo.toml -- manifest validate $manifest
cargo run --manifest-path crates/giggles-migrate/Cargo.toml -- reconcile $manifest $chunks
cargo run --manifest-path crates/giggles-migrate/Cargo.toml -- import $manifest $chunks $db
cargo run --manifest-path crates/giggles-migrate/Cargo.toml -- verify $manifest $chunks $db
cargo run --manifest-path crates/giggles-migrate/Cargo.toml -- status $manifest $db
```

The canned trace imports capture before old backfill. Its deleted message stays absent, its expired message stays absent, and its surviving edited message retains an empty byte attachment. Repeating import is idempotent. Files remain at `$db`; this crate never removes test fixtures or rehearsal files.

## Normalized v1 input contract

`Manifest` pins source/table identity, schema and topology epochs, typed partition and clustering keys, clustering direction per component, scalar columns, every stream's inclusive start/end cut, absolute microsecond horizon, tie policy and resource limits. Its typed `cell_semantics` explicitly declares signed microsecond timestamps, absolute exclusive microsecond expiry, permanent rehearsal tombstone retention and the permitted tombstone scope inventory. An undeclared tombstone scope rejects. JSON rejects unknown fields. Every normalized event occupies one unique stream position. A source adapter that emits multiple effects per source offset must assign unique normalized subpositions before using this contract. IDs are opaque strings. They are not parsed as source paging tokens or timestamps.

Supported values are explicit null, boolean, signed i64, UTF-8 text and raw bytes. Keys retain original typed bits and compare lexicographically; clustering components respect their declared directions. Partition components use ascending typed order. Null keys reject. Missing cells have no event, empty text/bytes remain values, and deletion has a separate tombstone mutation. Null is an explicit live normalized value, not an inferred CQL deletion. Unsupported counters, LWT, collections, static cells and other semantics must be inventoried in `unsupported_features` and cause validation failure. This schema cannot express those semantics implicitly.

Every put carries its own source timestamp and optional absolute microsecond expiry. Expiry is exclusive: a value is visible only when `expires_at > horizon`. `expiry_from_ttl` rejects negative durations and checked arithmetic overflow. The adapter is responsible for supplying the correct absolute expiry; the importer never derives it from arrival time or wall clock. Newer expired cells remain barriers against old backfill. Tombstones cover a cell, row, partition, or explicit clustering range with inclusive/exclusive full composite endpoints. Tombstones never expire or undergo garbage collection in this rehearsal. All provenance and older versions are retained.

Only `reject_ambiguous` ties are supported. Equal timestamp puts to the same cell must have identical value **and expiry**, even if overshadowed by a later value or delete. Equal-timestamp tombstones suppress puts. This is a pinned normalized rule, not a claim about any ScyllaDB release. A release-specific validated adapter/rule is future work. Conflicting event IDs or stream positions reject atomically. Identical repeated events are idempotent.

Each event also retains its nonempty original opaque `source_position` token verbatim. The separate numeric `position` is the adapter-supplied normalized contiguous index used only for declared-cut gap detection. Source/schema/topology identities are compared for equality; source tokens never select a conflict winner.

Chunk IDs are permanent. SHA256 covers the ID and canonical full events sorted by encoded bytes, including duplicate multiplicity, source metadata and epochs. Reordering a chunk is equivalent; changing its content under the same ID rejects after reopening as well. A manifest change requires a new rehearsal database.

## SQL durability and verification

The importer uses only public `giggles_core::Admin` and `SqlSession` APIs. `migration_stage` retains normalized provenance/barriers; `migration_rows` stores actual ordered materialized destination rows; `migration_control` stores manifest, state digest, permanent chunk checkpoints and verification binding. SQL parameters carry typed payloads. A single real core SQL transaction updates stage, destination slots and checkpoint. Failure before commit leaves all three unchanged. Surplus slots become inactive so historical artifacts remain retained. No SQL row deletion is needed. Destination rows encode the dynamic scalar schema in validated JSON BYTEA payloads; they are queryable through the independent SQL reader, not projected into application-specific SQL columns.

Verification constructs expected state again from supplied normalized chunks, requires the same provenance/cut, and reads actual destination rows through a separate SQL read path. It rejects invalid sequence, duplicate/wrong key order, invalid types or expired cells. It reports sorted per-partition cryptographic digest, count, first/last clustering keys and bounded exact row differences, with an explicit truncation flag. The digest includes typed values, per-cell timestamps, expiry and fixed horizon. `compare_range` supports exact bounded partition slices. Metadata is checked by the matched provenance binding; destination payload parity is separately checked against actual SQL data.

Durable gate markers are rechecked against actual data on `status`:

| Phase | Local evidence |
|---|---|
| source-only-baseline | Initialized normalized manifest; no verified cut yet. This does not prove external writer inventory or fencing. |
| capture-complete | Every integer position in every **declared** stream interval is retained. |
| reconciled | Supplied expected provenance and matched-horizon destination comparison succeeded. |
| canary-ready | Both declared stream coverage and successful comparison hold. This is readiness for an offline read canary, not permission for a live cutover. |

`source_completeness_proven` is always false. Source writer authority is external and uninspected. A declared-only watermark cannot prove that all real streams, snapshot intervals or source mutations were captured. New chunks invalidate previous verification. There is no command to transfer writer authority. Real fencing, complete source capture, reverse mapping/capture, rollback after destination writes and operator performance/cost gates remain external.

## Bounds and tests

This deliberately uses a full retained-state rewrite, not a production streaming importer. Manifest maxima are 4,096 normalized events, 256 materialized rows/slots, 48 KiB per chunk/state/SQL payload and 32 declared streams; each stream spans fewer than 4,096 positions. Smaller manifest limits apply. Retained tombstones and checkpoints eventually hit explicit limits and fail closed. CLI files are bounded to 48 KiB, with at most 256 chunks per input file. One chunk uses one transaction, at most one transaction per importer operation. More rows require a different scalable staging design.

Tests use unique retained temporary paths, exhaustive permutation and seeded independent-oracle checks, range/TTL/tie/identity negative cases, actual SQL corruption controls, reopen/retry, and subprocess abrupt exits before/after commit and acknowledgment. The SQL destination owner's direct database/file authority is trusted; these checks do not establish a security boundary. No security, cloud, performance or production qualification is claimed. PostgreSQL comparator CONTROL remains deferred.

```powershell
cargo fmt --manifest-path crates/giggles-migrate/Cargo.toml -- --check
cargo clippy --manifest-path crates/giggles-migrate/Cargo.toml --all-targets -j2 -- -D warnings
cargo test --manifest-path crates/giggles-migrate/Cargo.toml -j2
cargo test --manifest-path crates/giggles-migrate/Cargo.toml -j2 --release
cargo test --manifest-path crates/giggles-migrate/Cargo.toml --doc -j2
$env:RUSTDOCFLAGS='-D warnings'
cargo doc --manifest-path crates/giggles-migrate/Cargo.toml --no-deps -j2
```
