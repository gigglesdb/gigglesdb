# giggles-pg-uuid

A UUID representation, parser, formatter and PostgreSQL-style extraction and hashing library for Rust callers.

**Status:** Standalone API tested against PostgreSQL 18.0; generation and hashing limits are described below.

Part of [GigglesDB](../../README.md).

Standalone crate; its own Cargo.lock and workspace. Stable initial API is in src/lib.rs:
Uuid ([u8;16]), FromStr/Display, from_bytes/to_binary/from_binary,
extract_version -> Option<i16>, extract_timestamp -> Option<TimestampTz>,
v4_from_entropy, pg_hash_le, pg_hash_extended_le. Rust Hash is an ordinary
caller-selected Rust hasher; pg_hash_* explicitly supplies PostgreSQL algorithms.
Error carries Text/Binary categories and SQLSTATE. Binary value decoder requires
exactly 16 bytes. PostgreSQL COPY framing can report 08P01 on short input before
the value decoder; overlong payloads report 22P03. No SQL error-message parity claim.

Input is rejected above 41 bytes before any allocation. Parser permits braces,
uppercase, absent hyphens and optional hyphens after each four hex digits.
Embedded NUL is invalid (SQL text excludes NUL). Output is canonical lowercase.
Ordering compares unsigned bytes. Extraction checks RFC variant 10xx first;
version returns any nibble, timestamp only v1/v7. v1 truncates ticks to whole
microseconds before epoch subtraction; v7 ignores fraction bits and returns
whole milliseconds. All encoded v1/v7 timestamps fit the existing temporal range.
No floating point conversion occurs in extraction or the oracle comparator.

## Verification
Run from repository root:

```sh
cargo test --manifest-path crates/giggles-pg-uuid/Cargo.toml --target-dir target-dd68-uuid
```

Opt-in Docker oracle (image must already be local):

```sh
cargo test --manifest-path crates/giggles-pg-uuid/Cargo.toml --target-dir target-dd68-uuid -- --include-ignored
cargo fmt --manifest-path crates/giggles-pg-uuid/Cargo.toml -- --check
cargo clippy --manifest-path crates/giggles-pg-uuid/Cargo.toml --target-dir target-dd68-uuid --all-targets -- -D warnings
```

`tests/conformance.rs` is the stable executable oracle manifest. Seed:
0xdd68956218000001; 768 generated UUIDs + 2 raw extremes + 4 v1/v7 endpoints.
Version/variant dimensions vary independently. Exact SQL and comparators are
retained there. Binary receive uses real PostgreSQL binary COPY framing.
Ignored means untested unless explicitly opted in; never count a skipped oracle
as passing. The fixture owns one process-named container, no host ports, no
network, read-only root, only tmpfs database/socket/temp paths; Drop removes it.

## Sources and licenses
reference/uuid.c and reference/hashfn.c are unmodified official PostgreSQL
REL_18_0 files fetched from https://github.com/postgres/postgres/tree/REL_18_0 .
reference/COPYRIGHT preserves PostgreSQL's license. Parser/extraction logic and
specialized 16-byte little-endian hash implementation are adapted from those files.
Hash mixing originates with Bob Jenkins, as credited in hashfn.c.
RFC 9562 https://www.rfc-editor.org/rfc/rfc9562.html sections 4, 5.1, 5.4, 5.7
provide UUID layout, variant and timestamp context. No RFC prose copied.
source-oracle-manifest.json records immutable hashes and oracle image.

## Explicit gaps
No SQL/native engine, storage, catalog, wire, distributed or export integration;
no new outer format allocated. Parent must coordinate enum and format gates.
No ambient clock/random/network at library runtime. v4_from_entropy only sets
version/variant on caller entropy; it does not implement SQL uuidv4().
No uuidv7 generation, monotonic backend context, shift/interval, negative epoch
or overflow generation contract. No SQL uuidv4/uuidv7 claim.
pg_hash_* is qualified for little-endian PostgreSQL only; big-endian hashing
remains unqualified. Corpus is seeded/adversarial evidence, not exhaustive.
BC extraction is unreachable: UUIDv1 begins 1582-10-15 AD and v7 begins
1970-01-01 AD. Values from other variants/versions return None rather than
inventing BC timestamps. Their encoded upper endpoints are tested against PG18.
