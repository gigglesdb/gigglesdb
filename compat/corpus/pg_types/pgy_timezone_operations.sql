-- case: pgy_timezone_operations
-- title: timezone operations
-- owner: DD-0068
-- expect: compatible
-- source: Data types; types.txt
SET TIME ZONE 'America/New_York';
SELECT '2024-07-01 12:00'::timestamptz AT TIME ZONE 'UTC';
SELECT '2024-01-01 12:00'::timestamp AT TIME ZONE 'America/New_York';
SELECT EXISTS (SELECT 1 FROM pg_timezone_names WHERE name='America/New_York');
RESET timezone;
SET TIME ZONE 'UTC+3';
SELECT '2024-01-01 00:00+00'::timestamptz;
RESET timezone;
SET TIME ZONE '+05:30';
SELECT '2024-01-01 00:00+00'::timestamptz;
RESET timezone;
-- @error
SET TIME ZONE 'Not/AZone';
RESET timezone;
