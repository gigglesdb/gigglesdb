-- case: pgf_casts_numeric
-- title: Numeric casts round and text coercions
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-math.html
CREATE TABLE r0c_pgf_casts_numeric (id integer PRIMARY KEY, x numeric);
INSERT INTO r0c_pgf_casts_numeric VALUES (1,2.5),(2,-2.5),(3,NULL);
SELECT id, CAST(x AS integer) FROM r0c_pgf_casts_numeric ORDER BY id;
SELECT id, x::bigint FROM r0c_pgf_casts_numeric ORDER BY id;
SELECT id, x::smallint FROM r0c_pgf_casts_numeric ORDER BY id;
SELECT id, x::real FROM r0c_pgf_casts_numeric ORDER BY id;
SELECT id, x::double precision FROM r0c_pgf_casts_numeric ORDER BY id;
SELECT id, x::numeric(8,1) FROM r0c_pgf_casts_numeric ORDER BY id;
SELECT id, x::text FROM r0c_pgf_casts_numeric ORDER BY id;
SELECT id, '42'::integer FROM r0c_pgf_casts_numeric ORDER BY id;
SELECT id, '2.5'::numeric::integer FROM r0c_pgf_casts_numeric ORDER BY id;
SELECT id, '-2.5'::numeric::integer FROM r0c_pgf_casts_numeric ORDER BY id;
SELECT id, CAST('true' AS boolean) FROM r0c_pgf_casts_numeric ORDER BY id;
SELECT id, '255'::oid FROM r0c_pgf_casts_numeric ORDER BY id;
DROP TABLE r0c_pgf_casts_numeric;
