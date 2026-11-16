-- case: pgf_array_unnest
-- title: Single multiple and ordinal array expansion
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-array.html
CREATE TABLE r0c_pgf_array_unnest (id integer PRIMARY KEY, a integer[], b text[]);
INSERT INTO r0c_pgf_array_unnest VALUES (1,ARRAY[1,2],ARRAY['a','b','c']),(2,ARRAY[]::integer[],ARRAY[]::text[]),(3,NULL,NULL);
SELECT id,x,y,n FROM r0c_pgf_array_unnest CROSS JOIN LATERAL unnest(a,b) WITH ORDINALITY AS u(x,y,n) ORDER BY id,n;
SELECT id,unnest(a) AS x FROM r0c_pgf_array_unnest ORDER BY id,x;
SELECT x,n FROM unnest(ARRAY[3,NULL,1]) WITH ORDINALITY AS u(x,n) ORDER BY n;
SELECT array_agg(x ORDER BY n) FROM (VALUES (3,1),(NULL::integer,2),(1,3)) AS u(x,n);
DROP TABLE r0c_pgf_array_unnest;
