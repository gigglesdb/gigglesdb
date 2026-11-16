-- case: pgf_math_random
-- title: Volatile random functions checked by properties
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-math.html
SELECT setseed(0.125) IS NULL;
CREATE TABLE r0c_pgf_math_random (id integer PRIMARY KEY, lo integer, hi integer);
INSERT INTO r0c_pgf_math_random VALUES (1,2,5),(2,-5,-2),(3,NULL,NULL);
SELECT id, random() >= 0 AND random() < 1 FROM r0c_pgf_math_random ORDER BY id;
SELECT id, random(lo,hi) BETWEEN lo AND hi FROM r0c_pgf_math_random ORDER BY id;
SELECT id, random(lo::bigint,hi::bigint) BETWEEN lo AND hi FROM r0c_pgf_math_random ORDER BY id;
SELECT id, random(lo::numeric,hi::numeric) BETWEEN lo AND hi FROM r0c_pgf_math_random ORDER BY id;
SELECT random(7,7)=7;
SELECT random(7::bigint,7::bigint)=7;
SELECT random(7::numeric,7::numeric)=7;
SELECT pg_typeof(x) FROM (VALUES (random())) AS input(x);
DROP TABLE r0c_pgf_math_random;
