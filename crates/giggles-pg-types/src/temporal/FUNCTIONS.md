# Typed temporal function foundation

Standalone typed functions; no SQL/core/storage/wire integration claim while
GD42 shared hooks remain active. No host clock, implicit zone, or locale.

Date/Time/TimeTz/Timestamp/TimestampTz/Interval::extract returns
Result<Option<Numeric>, TemporalFunctionError>; None is PostgreSQL NULL for
oscillating infinite fields. Exact Numeric uses the frozen decimal API, never
float8 as a substitute. date_part returns Option<Float8>.

TemporalField implements FromStr with PostgreSQL REL_18_0 DecodeUnits and
DecodeSpecial aliases, ASCII case folding and the PostgreSQL ten-byte token
comparison limit. An alias identifies a field, not its availability on a type.
NumericOverflow (float8 multiplication overflow in make_interval),
UnsupportedField, Range, InvalidStride, InfiniteStride and InfiniteOrigin remain
standalone typed errors, not a SQLSTATE mapping. SQL NULL propagation belongs
to callers. Oracle tests independently check PostgreSQL result types and errors.

Timestamp/TimestampTz::date_bin takes an Interval and same-type origin.
Timestamp/TimestampTz/Interval::date_trunc takes a TemporalField. Timestamptz
operations require SessionZone. Interval::justify_hours, justify_days and
justify_interval preserve PostgreSQL component normalization and checked overflow.

Existing make_date(year:i32,month:u32,day:u32), make_time(hour:u32,minute:u32,
second_microseconds:u32), and make_timestamp(year,month,day,hour,minute,
second_microseconds) retain their exact integer-microsecond contract (seconds
strictly below 60). New make_time_float8(hour:i32,minute:i32,seconds:f64) and
make_timestamp_float8(year:i32,month:u32,day:u32,hour:i32,minute:i32,seconds:f64)
apply PostgreSQL's float8 multiplication and ties-to-even microsecond rounding
before range checks, including the 60-second boundary. Nonfinite seconds fail.
make_interval(years:i32,months:i32,weeks:i32,days:i32,hours:i32,minutes:i32,
seconds:f64) checks each PostgreSQL intermediate component and finite result.
Negative constructor years denote BC, with no year zero.

Date::add_interval/sub_interval return Timestamp. Timestamp::add_interval/
sub_interval return Timestamp; TimestampTz versions also require SessionZone.
Calendar months clamp to month end, then calendar days are added, then elapsed
microseconds. Timestamptz re-resolves local time after each nonzero calendar
stage, retaining PostgreSQL gap/overlap resolution. Infinities propagate unless
the operation is indeterminate. Negation and arithmetic overflow are checked.

Timestamp::age(other) and TimestampTz::age(other,zone) return the symbolic
interval for self minus other. Month borrowing uses the earlier operand's month
length, matching PostgreSQL rather than elapsed-duration arithmetic. Timestamptz
uses local fields and instant ordering. There is no one-argument age or relative
function: any future such API must require explicit current-time context.

Primary references: PostgreSQL REL_18_0 date.c, timestamp.c, datetime.c.
Previous source copies are in target-dd68-temporal-functions; datetime.c and
new logs/source/API/test hashes are in target-dd68-temporal-calendar. The prior
manifest's tzdata_manifest_sha256 label was corrected to the actual manifest;
the old hash is retained as tzdata_hashes_txt_sha256. Data is unchanged.

The calendar suite compares binary interval/timestamp results, text values,
result types and SQLSTATE against the pinned owned PostgreSQL 18 image.
Sampled cases cover BC, month ends, ISO years, infinities, endpoint arithmetic,
negative normalization, rounding ties/submicroseconds/nonfinite seconds, and
New York/Lord Howe/Apia transitions. Existing temporal suites remain required.
These are broad sampled comparisons, not exhaustive transition qualification.
Parser limits remain in TEMPORAL_COMPAT.md. SQL binding, core temporal types,
storage/wire integration, relative functions and timezone abbreviation support
remain outside this foundation.