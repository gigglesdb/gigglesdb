-- case: pgf_datetime_truncate
-- title: Truncation bucketing and finite dates
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-datetime.html
CREATE TABLE r0c_pgf_datetime_truncate (id integer PRIMARY KEY, t timestamp);
INSERT INTO r0c_pgf_datetime_truncate VALUES (1,'2024-05-06 07:08:09'),(2,'2024-01-01 00:07:00'),(3,NULL);
SELECT id, date_trunc('month',t) FROM r0c_pgf_datetime_truncate ORDER BY id;
SELECT id, date_trunc('week',t::date) FROM r0c_pgf_datetime_truncate ORDER BY id;
SELECT id, date_trunc('day',t::timestamptz,'America/New_York') FROM r0c_pgf_datetime_truncate ORDER BY id;
SELECT id, date_trunc('hour',interval '2 days 03:45:06') FROM r0c_pgf_datetime_truncate ORDER BY id;
SELECT id, date_bin(interval '15 minutes',t,timestamp '2024-01-01') FROM r0c_pgf_datetime_truncate ORDER BY id;
SELECT id, date_bin('15 minutes',t,timestamp '2024-01-01') FROM r0c_pgf_datetime_truncate ORDER BY id;
SELECT id, isfinite(t) FROM r0c_pgf_datetime_truncate ORDER BY id;
SELECT id, isfinite(date 'infinity') FROM r0c_pgf_datetime_truncate ORDER BY id;
SELECT id, isfinite(interval '1 day') FROM r0c_pgf_datetime_truncate ORDER BY id;
SELECT id, to_timestamp(1700000000) FROM r0c_pgf_datetime_truncate ORDER BY id;
DROP TABLE r0c_pgf_datetime_truncate;
