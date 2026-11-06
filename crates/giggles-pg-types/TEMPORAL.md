# Temporal foundation (DD68)

Public API is `giggles_pg_types::temporal::{Date, Time, Timestamp, TimestampTz,
TimeTz, Interval, ParserSettings, DateOrder, IntervalStyle}`. Constructors validate
finite PostgreSQL 18 ranges. `days()` uses 2000-01-01; `microseconds()` uses that
epoch for timestamps and midnight for time. Infinities retain PG binary sentinels.
`from_binary` requires exact network-order lengths; `to_binary` returns fixed
arrays. Interval fields remain independent; `same_components` checks identity,
while Eq/Ord use PG's 30-day-month comparison only. Do not use SQL equality as a
canonical storage-image test. Timetz ordering compares UTC time without wrapping,
then seconds west as a tie-break. Timestamptz is an instant, without stored zone.

`ParserSettings::default()` means YMD, UTC; offsets are seconds **east** of UTC.
Settings never read the local clock, process timezone, locale, filesystem tzdata,
or environment variables. Output is ISO dates/times, UTC timestamptz and PostgreSQL
interval style. Explicit numeric offsets are supported up to 15:59:59.

Supported text: numeric `YYYY-MM-DD` dates (BC suffix, no year zero), slash dates
with explicit YMD/MDY/DMY order (year field at least four digits), `HH:MM[:SS[.fraction]]`, date/time separated by
space or T, numeric colon offsets, Z and UTC. BC follows the whole timestamp.
Fractional seconds round to microseconds with ties to even; 24:00 is preserved
for time and rolls forward for timestamps. Date/timestamp/interval accept signed
infinities (positive spelling `infinity`). Interval grammar supports signed integer
years/mons/days and signed wide HH:MM:SS; components may have independent signs.
`parse_with_style(..., SqlStandard)` applies a lone leading minus to all fields;
other explicit signs suppress that propagation. `parse` uses Postgres semantics.
Every parser rejects input over 256 bytes before copying/tokenizing. No input-sized
allocation occurs in binary decoding. Integer/component overflow is checked.

This is a narrower grammar than PostgreSQL, not full temporal function coverage.
Named IANA zones, DST resolution, timezone abbreviations other than UTC, POSIX
zones, locale month names, AM/PM, relative/current-time keywords, leap seconds,
week/ordinal dates, compact offsets, typmods, ISO8601/SQL year-month interval
syntax, fractional interval units, verbose/ago interval syntax and alternate
output DateStyles/IntervalStyles are not implemented. Named zones fail rather
than being treated as UTC; no timezone dependency has been added. DST and named
zone support must use a maintained pinned timezone database with explicit
ambiguity/gap policy and its own live PG qualification. FullPG remains unfinished.

Verification uses the existing pinned PG18.0 image:
`postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c`.
Create only the owned ephemeral oracle (no ports or host mounts):

```powershell
docker run -d --name dd68-temporal-oracle --network none --tmpfs /var/lib/postgresql -e POSTGRES_HOST_AUTH_METHOD=trust postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c
# Once pg_isready succeeds:
cargo test --locked --manifest-path crates/giggles-pg-types/Cargo.toml --target-dir target-dd68-temporal
cargo test --locked --manifest-path crates/giggles-pg-types/Cargo.toml --target-dir target-dd68-temporal --test temporal -- --include-ignored --nocapture
cargo clippy --locked --manifest-path crates/giggles-pg-types/Cargo.toml --target-dir target-dd68-temporal --all-targets -- -D warnings
cargo fmt --manifest-path crates/giggles-pg-types/Cargo.toml -- --check
docker rm -f dd68-temporal-oracle
```

Both live tests require Docker, a matching pinned container and PG version180000.
They are opt-in ignored tests; ordinary tests do not quietly claim live coverage.
Four properties run 1024 cases each. Results/hashes are in temporal-results.json.
Sources (REL_18_0):
- https://github.com/postgres/postgres/blob/REL_18_0/src/include/datatype/timestamp.h
- https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/date.c
- https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/timestamp.c
- https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/datetime.c

Only the temporal module, tests, this document and one `pub mod temporal;`
declaration are added. No storage/core integration, persistence/wire exposure,
manifest/lock changes, commits or pushes are included.

Subsequent increments: TEMPORAL_ZONES.md and TEMPORAL_COMPAT.md supersede the
historical unsupported-feature list above. The latest compatibility and range
qualification is in TEMPORAL_COMPAT.md; full DD68 remains open.
