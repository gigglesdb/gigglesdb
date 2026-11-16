-- case: pgf_datetime_fields
-- title: Calendar extraction all calendar fields
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-datetime.html
CREATE TABLE r0c_pgf_datetime_fields (id integer PRIMARY KEY, t timestamptz);
INSERT INTO r0c_pgf_datetime_fields VALUES (1,'2024-02-29 13:04:05.123456+00'),(2,'2000-01-01 00:00+00'),(3,NULL);
SELECT id, extract(century FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(day FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(decade FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(dow FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(doy FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(isodow FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(isoyear FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(julian FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(millennium FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(month FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(quarter FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(week FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(year FROM t) FROM r0c_pgf_datetime_fields ORDER BY id;
SELECT id, extract(julian FROM date '2024-05-06') FROM r0c_pgf_datetime_fields ORDER BY id;
DROP TABLE r0c_pgf_datetime_fields;
