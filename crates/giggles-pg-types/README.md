# giggles-pg-types

PostgreSQL-style numeric, floating-point and temporal representations for Rust callers.

**Status:** Provides bounded standalone type APIs; these checks do not establish complete engine, persistence or wire compatibility.

Part of [GigglesDB](../../README.md).

`Numeric::parse`, `Display`, `scale`, `Ord`/`Eq`, `to_binary`/`from_binary`,
`checked_add/sub/mul/sum`, `round`, `div_at_scale` are public. Finite values use
num-bigint (locked 0.4.8) for exact arbitrary precision coefficients, num-traits
for sign/zero operations. No i128 precision ceiling. proptest is test-only.
Cargo.lock belongs solely to this crate; no workspace/root dependency edits.

Finite coefficient limit: 16,384 decimal digits; display scale 0..16,383;
text input limit 32,800 bytes. Limits are fixed and checked before input copies,
binary digit allocation or exponentiation. Bounded arithmetic scratch values can
reach roughly 49,152 decimal digits. Limit errors never round implicitly.
Numeric wire format is big-endian base10000 with stripped zero groups. Receive
validates length/sign/scale/digits and truncates hidden fractional digits as PG18
does. Negative zero normalizes, display scale survives, NaN equals itself and
sorts above infinity. Stable standalone serialization uses canonical wire bytes;
this does not authorize using that encoding in GigglesDB persistence.

Float4/Float8 retain IEEE bits including NaN payload and negative zero in binary
serialization. SQL equality/order equates NaNs and signed zero, NaN sorts highest.
`parse` rounds directly to f32/f64, rejecting overflow/nonzero underflow to zero.
`to_text` emits a shortest round-trip decimal and PG default exponent layout.

Numeric supports nondecimal integer literals, automatic division scale selection,
and arithmetic on special values; finite division and rounding use ties away
from zero. See [NUMERIC.md](NUMERIC.md) for the deliberate resource limits.
Float input includes C99 hex and signed/payload NaN forms for the tested
Linux/glibc PG18 profile. Output implements midpoint avoidance and all PG
extra_float_digits settings (-15..3); see [FLOAT.md](FLOAT.md) for exact scope,
platform limits and performance caveats. Sampling is not exhaustive equivalence.

The crate also exposes bounded integer, bytea and temporal representations;
see [TEMPORAL.md](TEMPORAL.md), [TEMPORAL_ZONES.md](TEMPORAL_ZONES.md) and
[TEMPORAL_COMPAT.md](TEMPORAL_COMPAT.md). These standalone APIs do not establish
SQL/native type binding, catalog, wire-server or persistence compatibility.
Consumer integration and qualification are maintained separately in GigglesDB.

Primary source: PostgreSQL REL_18_0 `src/backend/utils/adt/numeric.c` and `float.c`
https://github.com/postgres/postgres/tree/REL_18_0/src/backend/utils/adt

Validation:

The full test command currently includes four live calendar tests that require
the existing owned dd68-temporal-calendar-oracle container. Unlike the other
live oracle tests, these four are not marked ignored; an absent container makes
the full suite fail. Passing non-live targets does not qualify that live suite.

```
cargo test --locked --manifest-path crates/giggles-pg-types/Cargo.toml
cargo clippy --locked --manifest-path crates/giggles-pg-types/Cargo.toml --all-targets -- -D warnings
```

Live oracle (explicit opt-in, requires Docker):

```
docker run -d --name dd68-pgtypes-oracle --network none -e POSTGRES_HOST_AUTH_METHOD=trust postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c
cargo test --locked --manifest-path crates/giggles-pg-types/Cargo.toml --test oracle -- --ignored --nocapture
docker rm -f dd68-pgtypes-oracle
```

The live test checks actual image identity and server_version_num=180000 before
comparing 109 numeric text/binary, 300 arithmetic, 25 rounding, 18 float text/binary and 4 float
range errors. The ordinary suite has four properties with 1024 cases each.
