-- case: pgf_set_series
-- title: Integer numeric and timestamp generate series
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-srf.html
CREATE TABLE r0c_pgf_set_series (id integer PRIMARY KEY, n integer);
INSERT INTO r0c_pgf_set_series VALUES (1,3),(2,0),(3,NULL);
SELECT id,v FROM r0c_pgf_set_series CROSS JOIN LATERAL generate_series(1,n) AS g(v) ORDER BY id,v;
SELECT v FROM generate_series(1,5,2) AS g(v) ORDER BY v;
SELECT v FROM generate_series(5,1,-2) AS g(v) ORDER BY v;
SELECT v FROM generate_series(0.5::numeric,2::numeric,0.5::numeric) AS g(v) ORDER BY v;
SELECT v FROM generate_series(timestamp '2024-02-28',timestamp '2024-03-01',interval '1 day') AS g(v) ORDER BY v;
SELECT v FROM generate_series(timestamptz '2024-03-09 12:00+00',timestamptz '2024-03-11 12:00+00',interval '1 day','America/New_York') AS g(v) ORDER BY v;
SELECT v,n FROM generate_series(1,3) WITH ORDINALITY AS g(v,n) ORDER BY n;
DROP TABLE r0c_pgf_set_series;
