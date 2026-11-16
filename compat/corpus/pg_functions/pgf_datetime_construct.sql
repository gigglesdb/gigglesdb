-- case: pgf_datetime_construct
-- title: Date time constructors and interval justification
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-datetime.html
CREATE TABLE r0c_pgf_datetime_construct (id integer PRIMARY KEY, y integer);
INSERT INTO r0c_pgf_datetime_construct VALUES (1,2024),(2,2000),(3,NULL);
SELECT id, make_date(y,2,29) FROM r0c_pgf_datetime_construct ORDER BY id;
SELECT id, make_time(12,34,56.5) FROM r0c_pgf_datetime_construct ORDER BY id;
SELECT id, make_timestamp(y,2,29,12,34,56.5) FROM r0c_pgf_datetime_construct ORDER BY id;
SELECT id, make_timestamptz(y,2,29,12,34,56.5,'UTC') FROM r0c_pgf_datetime_construct ORDER BY id;
SELECT id, make_interval(y,2,1,3,4,5,6.5) FROM r0c_pgf_datetime_construct ORDER BY id;
SELECT id, make_interval(days => 3, hours => 4) FROM r0c_pgf_datetime_construct ORDER BY id;
SELECT id, justify_days(interval '35 days') FROM r0c_pgf_datetime_construct ORDER BY id;
SELECT id, justify_hours(interval '49 hours') FROM r0c_pgf_datetime_construct ORDER BY id;
SELECT id, justify_interval(interval '1 mon -1 hour') FROM r0c_pgf_datetime_construct ORDER BY id;
SELECT id, age(timestamp '2024-03-01',timestamp '2024-01-15') FROM r0c_pgf_datetime_construct ORDER BY id;
SELECT id, age(NULL::timestamp) FROM r0c_pgf_datetime_construct ORDER BY id;
DROP TABLE r0c_pgf_datetime_construct;
