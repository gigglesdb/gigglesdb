-- case: pgf_format_datetime
-- title: Timestamp date and interval formatting patterns
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-formatting.html
CREATE TABLE r0c_pgf_format_datetime (id integer PRIMARY KEY, t timestamp);
INSERT INTO r0c_pgf_format_datetime VALUES (1,'2024-02-29 13:04:05.123456'),(2,'2000-01-01'),(3,NULL);
SELECT id, to_char(t,'YYYY-MM-DD HH24:MI:SS.US') FROM r0c_pgf_format_datetime ORDER BY id;
SELECT id, to_char(t,'IYYY-IW-ID DDD Q CC') FROM r0c_pgf_format_datetime ORDER BY id;
SELECT id, to_char(t,'FMDay FMMonth DDth YYYY') FROM r0c_pgf_format_datetime ORDER BY id;
SELECT id, to_char(t,'HH12:MI AM') FROM r0c_pgf_format_datetime ORDER BY id;
SELECT id, to_char(t,'J WW W') FROM r0c_pgf_format_datetime ORDER BY id;
SELECT id, to_char(t::date,'YYYY/MM/DD') FROM r0c_pgf_format_datetime ORDER BY id;
SELECT id, to_char(interval '1 year 2 months 3 days 04:05:06','YYYY MM DD HH24 MI SS') FROM r0c_pgf_format_datetime ORDER BY id;
SELECT id, to_date('2024-060','YYYY-DDD') FROM r0c_pgf_format_datetime ORDER BY id;
SELECT id, to_timestamp('2024-02-29 13:04:05','YYYY-MM-DD HH24:MI:SS') FROM r0c_pgf_format_datetime ORDER BY id;
SELECT id, to_timestamp(0) FROM r0c_pgf_format_datetime ORDER BY id;
SELECT id, to_date(NULL::text,'YYYY-MM-DD') FROM r0c_pgf_format_datetime ORDER BY id;
DROP TABLE r0c_pgf_format_datetime;
