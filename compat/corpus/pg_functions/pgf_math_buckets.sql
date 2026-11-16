-- case: pgf_math_buckets
-- title: Width buckets numeric and threshold forms
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-math.html
CREATE TABLE r0c_pgf_math_buckets (id integer PRIMARY KEY, x numeric);
INSERT INTO r0c_pgf_math_buckets VALUES (1,2),(2,10),(3,NULL);
SELECT id, width_bucket(x,0,10,5) FROM r0c_pgf_math_buckets ORDER BY id;
SELECT id, width_bucket(x,10,0,5) FROM r0c_pgf_math_buckets ORDER BY id;
SELECT id, width_bucket(x,ARRAY[0,3,6,9]::numeric[]) FROM r0c_pgf_math_buckets ORDER BY id;
SELECT id, width_bucket(-1,0,10,5) FROM r0c_pgf_math_buckets ORDER BY id;
SELECT id, width_bucket(10,0,10,5) FROM r0c_pgf_math_buckets ORDER BY id;
SELECT id, width_bucket(5.0::double precision,0,10,5) FROM r0c_pgf_math_buckets ORDER BY id;
DROP TABLE r0c_pgf_math_buckets;
