# DD68 temporal compatibility continuation

This increment supersedes the named-zone range restriction in TEMPORAL_ZONES.md.
Full DD68 remains open. Numeric/float behavior and adapters are unchanged.

## Zone data and algorithm

`SessionZone` retains its existing public enum and constructors. Named zones now
use tz-rs **=0.7.3**, alloc only, to parse embedded TZif files. chrono-tz remains
only the existing public zone identifier/name table; its truncated transition
arrays no longer determine offsets. Parent must refresh downstream lockfiles for
the added tz-rs dependency. No new core enum or adapter contract is exposed.

The 487 embedded files were read byte-for-byte from `/usr/share/zoneinfo/<name>`
in the ephemeral PG18 oracle image
`postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c`.
The selected names came from pg_timezone_names, excluding posix/ and right/ trees.
`src/temporal/tzdata/manifest.json` records every name/file/SHA256; the image's
`tzdata.zi` reports 2025b. Data is public domain (IANA; Debian tzdata copyright).
The embedded table includes oracle names beyond the existing chrono identifier
API; 484 names are currently recognized by `SessionZone::named`.

Before the first transition, the actual TZif initial local-time type is used.
After the last transition, the TZif footer's POSIX rule is evaluated in the actual
Gregorian year. This is **not** a 400-year surrogate mapping: no year is remapped,
and no synthetic historical transitions are manufactured. RFC9636 section3.3
specifies footer extrapolation beyond the last transition; section3.2 specifies
initial type before the first transition. tz-rs rule.rs computes transition days
and UTC seconds for the requested year. Missing data/parser failure returns an
explicit error, never UTC. Files and parser are pinned, independent of host TZ,
clock, locale, filesystem, network and environment at runtime.

Local conversion enumerates the zone's possible offsets, validates each candidate
against UTC lookup and selects the largest instant in an overlap. For gaps it
locates and validates the actual boundary between candidate offsets and uses its
pre-transition offset. This matches PG18 DetermineTimeZoneOffsetInternal even for
political/date-line shifts. Arithmetic uses signed integer seconds/microseconds;
negative subsecond timestamps use Euclidean division. Finite timestamp range is
no longer constrained to1800..2099. Offset conversion can still overflow the
finite timestamp endpoint and returns Range. Infinities pass through unchanged.

## Additional API and grammar

- `DateStyle::{Iso,Sql,Postgres,German}` and
  `Date::format_with_style(style, order)` implement date-only output, including BC,
  finite endpoints and infinity. Postgres date-only style is numeric hyphen output.
- `IntervalOutputStyle::{Postgres,Iso8601,SqlStandard,PostgresVerbose}` and
  `Interval::format_with_style` preserve independent months/days/microseconds,
  including mixed signs, wide times, zero, and infinities.
- `Interval::with_precision(0..=6)` rounds the time component ties away from zero,
  preserving months/days and infinity. Carry remains in the time component.
- `Interval::with_fields(IntervalField::{Year,Month,Day,Hour,Minute,Second}, precision)`
  implements trailing-field restriction; leading fields remain. This models the
  result for a valid range ending in that field, not SQL typmod syntax validation.
- Interval text adds signed year-month tokens, designator ISO8601 durations and
  decimal year/month/week/day/hour/minute/second/millisecond/microsecond units.
  Supported exact fractions use at most six nonzero fractional positions; larger
  decimal precision and submicrosecond scaled results return Unimplemented.
  Years round to integral months ties-even; months cascade to30-day fractional
  days and days to24-hour time, without normalizing integral components.
- Independent review fixed AM/PM hour zero and24:00 input with a fraction that
  rounds to zero. Existing clock text rounding remains ties-even; typmods use
  ties-away-from-zero.

## Qualification and gaps

`tests/temporal_compat.rs` exercises12,100 live cases (484names x25dates): BC;
1800,1883,1900,1945,1970,2038,2099,2100,2400,10000,294000, January and July.
Each compares UTC text, exact timestamptz_send bytes and reverse local text.
Additional tests cover half-hour DST, political/date-line changes and2100 gaps
and overlaps; all4 date styles x3 orders x5values; interval signs/output styles,
exact component binary images, trailing fields/precision, AMPM and24h carry,
invalid inputs and interval equality/order versus component identity.
This is broad differential evidence, not an exhaustive every-transition proof.

Date/time input decodes compact and delimited dates, English names, ordinal and
Julian days, abbreviations, and case-insensitive named and POSIX zones. The
embedded PostgreSQL 18.6 Default table retains its license in
`src/temporal/timezone_abbreviations.txt`. Dynamic entries resolve against
embedded TZif transitions. Timestamptz input checks the session zone's
abbreviations before that table.

- POSIX session zone strings such as `UTC+3` print a numeric offset in place of the abbreviation in SQL, Postgres, and German styles.
- Timestamptz literals with zone words, and timetz literals whose offset depends on the session zone, are converted at execution time in prepared statements (PostgreSQL converts them at prepare).
- A named statement rebound after DDL on a relation it references keeps the DateOrder it was prepared under; PostgreSQL re-analyses it under the DateOrder current at that point.

Explicit remaining gaps include runtime selection of abbreviation tables,
current-date-dependent input in the clock-free type APIs, alternate timestamp/timetz DateStyle output; ISO alternative
compact/extended interval syntax; bare SQL day-time interval grammar; complete
SQL-standard input sign propagation; submicrosecond fractional-unit parsing and
higher-precision decimals (PG's double-based rounding differs from exact decimal);
full type/SQLSTATE-specific diagnostic mapping and SQL typmod syntax validation.
No fake UTC, float conversion, year surrogate, host-clock fallback or data-version
substitution qualifies these gaps. Binary formats remain unchanged. SQL interval
comparison still equates a month to30days; same_components/binary images retain
component identity. Data-version differences on other PG installations remain an
explicit qualification gap; matching a version label alone proves no equivalence.

Run final full suite with owned containers named dd68-{pgtypes,numeric,temporal,
temporal-zones,temporal-compat}-oracle, all using the pinned image:
`cargo test --locked --manifest-path crates/giggles-pg-types/Cargo.toml --target-dir target-dd68-temporal-compat -- --include-ignored`.
Results/hashes: temporal-compat-results.json. No commit/push performed.

Primary sources:
- https://www.rfc-editor.org/rfc/rfc9636.html#section-3.3
- https://github.com/x-hgg-x/tz-rs/tree/v0.7.3/src/timezone
- https://github.com/postgres/postgres/blob/REL_18_0/src/timezone/localtime.c
- https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/datetime.c
- https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/timestamp.c
- https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/date.c

## Grammar continuation (2026-11-06)

This increment adds case-insensitive SessionZone::named lookup and UTC session
suffix handling without changing the enum or pinned TZif data. It adds bare
integer SQL day-plus-clock input and corrects SQL-standard leading sign propagation:
only signs at the start of subsequent tokens cancel propagation; the delimiter
in a year-month token does not. Complete unsigned integer ISO alternative forms
PYYYYMMDD[Thhmmss] and PYYYY-MM-DD[Thh:mm:ss], plus time-only variants, are
translated into component units; components are durations, not calendar fields.
Partial/signed/fractional alternative forms remain unqualified and may error.

Timestamp::format_with_style(DateStyle, DateOrder) now supplies all four styles,
with PostgreSQL weekday/month text, BC suffix position, infinities and full finite
range. TimeTz::format_with_style supplies the same explicit API; PostgreSQL timetz
output is independent of DateStyle/order. TimestampTz alternate style output
still needs an explicit zone/abbreviation contract and remains a gap.

New tests grammar_continuation_components, pg_grammar_continuation and
pg_grammar_errors_and_endpoints independently compare actual PG18 binary images,
styles and errors. Existing TZif algorithm was audited: initial type/footer rules,
offset enumeration, overlap maximum candidate, validated gap boundary, Euclidean
negative microseconds and checked endpoint arithmetic are retained. This is not
an exhaustive transition proof. All prior rounding, general grammar, relative
context, diagnostics, other tzdata-build qualification gaps remain open.
No dependencies, numeric/float/core/downstream files changed. No commits/pushes.
Latest verification/hashes: temporal-grammar-results.json.
