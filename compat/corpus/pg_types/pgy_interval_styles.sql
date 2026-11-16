-- case: pgy_interval_styles
-- title: interval styles
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: interval precision and field qualifiers; types.txt / Date and time types
SET intervalstyle='postgres_verbose';
SELECT '1 year 2 mons 3 days 04:05:06.7'::interval;
SET intervalstyle='sql_standard';
SELECT '1 year 2 mons 3 days 04:05:06.7'::interval;
SET intervalstyle='iso_8601';
SELECT '1 year 2 mons 3 days 04:05:06.7'::interval;
RESET intervalstyle;
