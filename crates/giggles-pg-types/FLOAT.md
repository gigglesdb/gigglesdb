# PostgreSQL 18 floating text conversion

`Float4` and `Float8` keep their existing API and bit-preserving binary encoding,
including NaN payload/sign and signed zero. Their PostgreSQL Eq/Ord remains unchanged.

`to_text()` implements the default `extra_float_digits=1`; all positive settings
use the same shortest output. `to_text_with_extra_float_digits(i32)` adds the PG
configuration range -15..=3, returning `Error::Range` outside it. Nonpositive modes
round to max(1, 6+extra) or max(1, 15+extra) significant digits, ties to even, with
PG's fixed/scientific thresholds. Text canonicalizes every NaN to `NaN`.

The formatter does **not** delegate digit selection to Rust Display. It derives
the exact open legal decimal interval using the `mv`, `mmShift`, and `acceptBounds=false`
rules from REL_18_0 `src/common/f2s.c` and `d2s.c`. It searches decimal grids from
coarse to fine and selects the nearest in-range integer, ties to even. BigUint
arithmetic replaces Ryu's cached powers and digit-removal acceleration. Excluding
both interval endpoints implements PG's midpoint avoidance. Exact comparisons
also select the decimal exponent; no floating logarithm estimate is used.
This is intentionally slower than table-driven Ryu; no performance qualification
has been performed.

Input follows REL_18_0 `src/backend/utils/adt/float.c`: ASCII whitespace, decimal
input rounded directly by Rust's destination-type parser, infinities, signed NaN,
and C99 hex rounded once with exact integer arithmetic to the destination format.
Nonzero values rounded to zero or infinity return Range; literal zero with huge
exponents remains zero. Numeric range errors precede trailing-junk syntax errors.
Existing Syntax/Range/Limit error categories remain; PostgreSQL message strings
are not exposed by this API. MAX_TEXT is still the explicit resource ceiling.

Hex input and extended NaN forms in PostgreSQL depend on the server's C library.
This implementation targets the tested Linux/glibc PG18.0 behavior: optional hex
binary exponent, NaN alphanumeric/underscore payloads, base-0 numeric payload,
overflow saturating to u64::MAX, then quiet-bit insertion and payload masking.
Other PostgreSQL platform C libraries may accept different extended spellings.
No locale-specific decimal separators are accepted (PG's C numeric locale).

Sources read verbatim from https://github.com/postgres/postgres/tree/REL_18_0:

- src/common/f2s.c SHA256 3c875827ec2b63868e6496a980c002dfc1eff2695b5c54cbf24598a4d246f07e
- src/common/d2s.c SHA256 37e9fc007e4f0d49a36b594fdb675b3437f344c46cfe11f75d34e9c21517ee1e
- src/backend/utils/adt/float.c SHA256 58914ae93c06c281e581a4bb2dcf2280e4ae1619f3ceb8f801f50beca87eecd8

Interval derivation attribution: Copyright 2018 Ulf Adams; portions Copyright
2018-2025 PostgreSQL Global Development Group. Ryu-derived rules are used under
Boost Software License 1.0 (FLOAT-BOOST-LICENSE). PostgreSQL source is covered by
FLOAT-POSTGRESQL-LICENSE, including PostgreSQL Global Development Group and
Regents of the University of California notices. The bigint search and hex
rounding are new Rust implementations, not a transcription of the Ryu tables.

Run:

```
cargo test --locked --manifest-path crates/giggles-pg-types/Cargo.toml --test float_pg18 -- --include-ignored
cargo clippy --locked --manifest-path crates/giggles-pg-types/Cargo.toml --all-targets -- -D warnings
```

Live tests require a PostgreSQL 18.0 container; default `dd68-numeric-oracle`,
override `DD68_FLOAT_PG_CONTAINER`. They assert server_version_num=180000.
Deterministic xorshift properties compare 108756 float4/float8 binary-send and text
rows across all 19 modes, plus finite text and exact hex round trips. Fixtures
cover min subnormals, normal boundaries, max finite, signed/payload NaNs, signed
zero, exact input midpoints, exponent transitions and malformed/range SQLSTATEs.
The sampling is reproducible and is not an exhaustive proof over every bit pattern.
