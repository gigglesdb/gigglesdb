# DD68 temporal zones increment

This extends the baseline in TEMPORAL.md; that baseline document describes the
previous grammar. It is not a claim of full PostgreSQL temporal compatibility.

`SessionSettings { date_order, zone }` has no default: callers choose explicitly.
`SessionZone::named` parses case-sensitive IANA names; `fixed` validates seconds
east of UTC. `TimestampTz::parse_in_session` accepts the baseline timestamp grammar
plus a trailing IANA name, or uses the explicit session zone when none is supplied.
Explicit numeric offsets take precedence. A timestamptz remains a UTC instant;
`to_local`, `to_instant`, and `format_in_zone` provide explicit conversion.
`TimeTz::in_zone_on_date` requires a date and time; it never invents today's date.
No environment, host clock, network, or filesystem timezone access occurs.

Pinned dependencies: chrono =0.4.42 (std, no clock) and chrono-tz =0.10.4, embedding
IANA 2025b. Parent integration must refresh dependent crate lockfiles. PG18 oracle
image remains the digest in TEMPORAL.md. Its pg_config specifies system tzdata in
/usr/share/zoneinfo; tzdata.zi reports 2025b. New York tzfile SHA256 is
 e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95.
There is no observed data-version divergence in that image. Matching labels do
not establish equivalence across every zone and historical transition.

PG18 DetermineTimeZoneOffsetInternal selects the pre-transition offset for gaps
and post-transition offset for overlaps. This also handles non-DST political
changes (Apia 2011, Moscow 2014); choosing by isdst alone is incorrect. chrono-tz
LocalResult::Ambiguous selects the later instant; GapInfo supplies the offset
before a gap. No guessed fallback UTC is used.

Non-UTC named-zone conversion explicitly returns Unimplemented outside civil
years 1800 through 2099. This conservative qualification boundary avoids library
prebuilt transitions ending with fixed offsets and does not wrap years onto
surrogate dates. Historical second offsets inside that boundary are retained.
BC and larger future dates remain available for fixed offsets and canonical UTC,
but are an explicit named-zone compatibility gap. Infinity bypasses finite zone
conversion and is preserved. Aliases other than canonical UTC retain the named
range restriction.

Additional grammar: case-insensitive AM/PM after clock fields; compact HHMM
numeric offsets (HHMMSS deliberately rejected after a live-PG mismatch); @/ago
intervals with checked component negation. Existing DateOrder ISO output roundtrips
are independently checked for every order. `with_precision(0..=6)` on Time,
TimeTz, Timestamp, TimestampTz performs PG typmod ties-away-from-zero rounding;
this differs from ties-even fractional text parsing. Overflow and invalid
precision return errors; time carry preserves 24:00. Typmod is not persisted in
the value. DateOrder still controls input, not alternate output DateStyle.

Remaining work: ISO8601 intervals, fractional interval units, SQL year-month
intervals, interval typmods and output styles, general DateStyle output,
month names, PostgreSQL abbreviation tables/POSIX zones, case-insensitive zone
lookup, full PG named-zone range/BC support, full text grammar and integration
into core/server. Cancellation tokens are not exposed by the baseline scalar
API: inputs remain bounded to 256 bytes with bounded library lookups. These
checks do not qualify SQL cancellation or integration behavior.

Verification: `cargo test --locked --manifest-path crates/giggles-pg-types/Cargo.toml
--target-dir target-dd68-temporal-zones`; use the same manifest/target for clippy
--all-targets -- -D warnings. New opt-in live tests in tests/temporal_zones.rs use
owned container dd68-temporal-zones-oracle (same pinned image). Run that test with
-- --include-ignored. Baseline temporal live tests use dd68-temporal-oracle.

Primary references:
- https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/datetime.c
- https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/timestamp.c
- https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/date.c
- https://docs.rs/chrono-tz/0.10.4/chrono_tz/struct.GapInfo.html
- https://docs.rs/chrono/0.4.42/chrono/offset/enum.LocalResult.html

See temporal-zones-results.json for final checks and source/artifact hashes.

## Superseding compatibility increment

TEMPORAL_COMPAT.md supersedes the1800..2099 guard and remaining-work list above.
Named-zone offsets now use pinned embedded TZif data with actual future footer
rules. See that document for APIs, qualification and explicit remaining gaps.
