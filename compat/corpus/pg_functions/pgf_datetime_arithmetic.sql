-- case: pgf_datetime_arithmetic
-- title: Date time interval arithmetic and overlaps
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-datetime.html
CREATE TABLE r0c_pgf_datetime_arithmetic (id integer PRIMARY KEY, d date, t timestamp, i interval);
INSERT INTO r0c_pgf_datetime_arithmetic VALUES (1,'2024-02-29','2024-02-29 12:00','2 days 3 hours'),(2,'2000-01-01','2000-01-01','-1 hour'),(3,NULL,NULL,NULL);
SELECT id, d+2 FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, d-2 FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, d-date '2024-01-01' FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, t+i FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, t-i FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, t-timestamp '2024-01-01' FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, time '12:00'+i FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, time '12:00'-time '10:30' FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, d+i FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, d+time '01:00' FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, i*2 FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, i/2 FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, -i FROM r0c_pgf_datetime_arithmetic ORDER BY id;
SELECT id, (t,t+i) OVERLAPS (timestamp '2024-02-28',timestamp '2024-03-02') FROM r0c_pgf_datetime_arithmetic ORDER BY id;
DROP TABLE r0c_pgf_datetime_arithmetic;
