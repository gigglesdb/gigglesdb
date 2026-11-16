-- case: pgf_datetime_current
-- title: Current datetime and clocks as properties
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-datetime.html
BEGIN;
SELECT now() IS NOT NULL;
SELECT transaction_timestamp() IS NOT NULL;
SELECT current_timestamp IS NOT NULL;
SELECT current_date IS NOT NULL;
SELECT current_time IS NOT NULL;
SELECT localtime IS NOT NULL;
SELECT localtimestamp IS NOT NULL;
SELECT current_timestamp(0) IS NOT NULL;
SELECT current_time(0) IS NOT NULL;
SELECT localtime(0) IS NOT NULL;
SELECT localtimestamp(0) IS NOT NULL;
SELECT statement_timestamp() IS NOT NULL;
SELECT clock_timestamp() IS NOT NULL;
SELECT pg_typeof(timestamp with time zone '2024-01-01 00:00+00');
SELECT pg_typeof(localtimestamp);
SELECT age(d)=interval '0 days' FROM (VALUES (current_date)) AS input(d);
SELECT age(NULL::timestamp) IS NULL;
COMMIT;
