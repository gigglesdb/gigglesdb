# Numeric foundation audit

This file supersedes the numeric compatibility-gap statements in the original
README; float documentation is owned separately.

`Numeric::checked_div` implements PostgreSQL REL_18_0 `select_div_scale`: normalized
base-10000 weight and first digit, 16 significant digits, both input display
scales, and the PostgreSQL 1000 fractional digit cap. Finite division and `round`
use exact integer quotient/remainder rounding, ties away from zero.

Add/subtract/multiply/divide support NaN and both infinities, including infinity
minus itself, zero times infinity, finite over infinity (scale-zero zero), and
infinity over zero (DivisionByZero). NaN propagates before division-zero checks.
Hexadecimal/octal/binary integer text accepts PostgreSQL separators, including
one underscore directly after the radix prefix. Decimal leading zeros do not
consume coefficient precision. Ordering uses decimal magnitude and padded digit
comparison, without coefficient alignment powers.

The deliberate standalone resource limits remain: 16,384 coefficient digits,
16,383 display scale, 32,800 input bytes. PostgreSQL accepts some larger values;
this API returns Limit rather than silently reducing precision. Multiplication
whose combined scale exceeds 16,383 returns Limit (PostgreSQL can round to its
maximum scale). Exponent/round scale bounds are also deliberately restrictive.
No full PostgreSQL numeric compatibility or core persistence gate is claimed.
No serde dependency/API was needed; canonical wire bytes remain the standalone
representation and require separate persistence integration/versioning review.

Input lengths/scales bound all arithmetic scratch coefficients to roughly
49,152 decimal digits. Powers are private and assert the established maximum
exponent bound. Product/quotient certain-overflow checks precede powers/clones;
round scale expansion is checked before powers. Nondecimal significant input
length has a conservative integer lower-bound precision check before BigInt
parsing. Receive validates length/sign/scale/groups, normalizes leading zero
groups, checks resulting coefficient width, and constructs at most bounded
coefficient text rather than exponentiating an attacker-controlled weight.
Hidden fractional groups truncate to dscale, as PostgreSQL receive does.

Primary sources (only):
- https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/numeric.c
  SHA256 c0e106fc5c0f19d6b1f31050754d691c8e7a253953dc2041392bb6e915b9853a
- https://github.com/postgres/postgres/blob/REL_18_0/src/include/utils/numeric.h
  SHA256 15423f637eacc565c0eaf7f94bb9c338336398bb36e7e9346cfa8ec30e7a8717

Validation uses CARGO_TARGET_DIR=target/dd68-numeric and a separate
`dd68-numeric-oracle` container of the exact postgres image digest from README.
`numeric_audit` checks image identity and server_version_num=180000. SQL goes
through stdin to support Windows command limits; binary receive uses actual
binary COPY, not a model of numeric_recv. Opt-in live invocation:

```
cargo test --locked --manifest-path crates/giggles-pg-types/Cargo.toml --test numeric_audit -- --include-ignored --nocapture
```

The existing representation.rs numeric_semantics test had three obsolete
Unimplemented expectations (0xFF, checked_div, NaN addition). With explicit user
ownership extension, only those three assertions were updated to exact observed
PostgreSQL results (255, 0.33333333333333333333, NaN). Independent new regressions
remain in numeric_audit.rs. See numeric-results.json for final outcomes.
