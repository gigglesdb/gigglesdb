-- case: pgf_math_rounding
-- title: Numeric rounding quotient and scale
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-math.html
CREATE TABLE r0c_pgf_math_rounding (id integer PRIMARY KEY, x numeric);
INSERT INTO r0c_pgf_math_rounding VALUES (1,12.3450),(2,-12.3450),(3,NULL);
SELECT id, abs(x) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, ceil(x) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, ceiling(x) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, floor(x) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, round(x,2) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, trunc(x,2) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, mod(x,5) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, div(x,5) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, sign(x) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, scale(x) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, min_scale(x) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, trim_scale(x) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, round(2.5::numeric) FROM r0c_pgf_math_rounding ORDER BY id;
SELECT id, trunc(-2.9::numeric) FROM r0c_pgf_math_rounding ORDER BY id;
DROP TABLE r0c_pgf_math_rounding;
