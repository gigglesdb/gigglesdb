-- case: pgf_datetime_timezone
-- title: Timezone operators and explicit conversion
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-datetime.html
CREATE TABLE r0c_pgf_datetime_timezone (id integer PRIMARY KEY, t timestamp);
INSERT INTO r0c_pgf_datetime_timezone VALUES (1,'2024-03-10 06:30'),(2,'2024-11-03 05:30'),(3,NULL);
SELECT id, t AT TIME ZONE 'UTC' FROM r0c_pgf_datetime_timezone ORDER BY id;
SELECT id, t::timestamptz AT TIME ZONE 'America/New_York' FROM r0c_pgf_datetime_timezone ORDER BY id;
SELECT id, t AT LOCAL FROM r0c_pgf_datetime_timezone ORDER BY id;
SELECT id, t::timestamptz AT LOCAL FROM r0c_pgf_datetime_timezone ORDER BY id;
SELECT id, timezone('UTC',t) FROM r0c_pgf_datetime_timezone ORDER BY id;
SELECT id, time with time zone '12:00+02' AT TIME ZONE interval '-05:00' FROM r0c_pgf_datetime_timezone ORDER BY id;
SELECT id, timestamp '2024-01-01' AT TIME ZONE 'Asia/Kolkata' FROM r0c_pgf_datetime_timezone ORDER BY id;
DROP TABLE r0c_pgf_datetime_timezone;
