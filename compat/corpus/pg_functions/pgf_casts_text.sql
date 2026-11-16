-- case: pgf_casts_text
-- title: Text casts to common scalar structured and temporal types
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-string.html
CREATE TABLE r0c_pgf_casts_text (id integer PRIMARY KEY, s text);
INSERT INTO r0c_pgf_casts_text VALUES (1,'2024-02-29'),(2,'2000-01-01'),(3,NULL);
SELECT id, s::date FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, s::timestamp FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, s::timestamptz FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, '12:34:56'::time FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, '12:34:56+00'::timetz FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, '2 days'::interval FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, '550e8400-e29b-41d4-a716-446655440000'::uuid FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, '{1,2}'::integer[] FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, '{"a":1}'::json FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, '{"a":1}'::jsonb FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, '12.34'::money FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, 'abc'::varchar(2) FROM r0c_pgf_casts_text ORDER BY id;
SELECT id, 'abc'::char(5) FROM r0c_pgf_casts_text ORDER BY id;
DROP TABLE r0c_pgf_casts_text;
