-- case: pgf_datetime_clock_fields
-- title: Clock epoch and timezone extraction fields
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-datetime.html
CREATE TABLE r0c_pgf_datetime_clock_fields (id integer PRIMARY KEY, t timestamptz);
INSERT INTO r0c_pgf_datetime_clock_fields VALUES (1,'2024-02-29 13:04:05.123456+00'),(2,'2000-01-01 00:00+00'),(3,NULL);
SELECT id, extract(epoch FROM t) FROM r0c_pgf_datetime_clock_fields ORDER BY id;
SELECT id, extract(hour FROM t) FROM r0c_pgf_datetime_clock_fields ORDER BY id;
SELECT id, extract(microseconds FROM t) FROM r0c_pgf_datetime_clock_fields ORDER BY id;
SELECT id, extract(milliseconds FROM t) FROM r0c_pgf_datetime_clock_fields ORDER BY id;
SELECT id, extract(minute FROM t) FROM r0c_pgf_datetime_clock_fields ORDER BY id;
SELECT id, extract(second FROM t) FROM r0c_pgf_datetime_clock_fields ORDER BY id;
SELECT id, extract(timezone FROM t) FROM r0c_pgf_datetime_clock_fields ORDER BY id;
SELECT id, extract(timezone_hour FROM t) FROM r0c_pgf_datetime_clock_fields ORDER BY id;
SELECT id, extract(timezone_minute FROM t) FROM r0c_pgf_datetime_clock_fields ORDER BY id;
SELECT id, date_part('epoch',t) FROM r0c_pgf_datetime_clock_fields ORDER BY id;
SELECT id, date_part('year',t) FROM r0c_pgf_datetime_clock_fields ORDER BY id;
SELECT id, extract(epoch FROM interval '2 days 03:04:05') FROM r0c_pgf_datetime_clock_fields ORDER BY id;
DROP TABLE r0c_pgf_datetime_clock_fields;
