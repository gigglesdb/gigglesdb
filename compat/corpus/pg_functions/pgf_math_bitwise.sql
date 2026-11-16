-- case: pgf_math_bitwise
-- title: Integer bitwise operators
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-math.html
CREATE TABLE r0c_pgf_math_bitwise (id integer PRIMARY KEY, n integer);
INSERT INTO r0c_pgf_math_bitwise VALUES (1,5),(2,-3),(3,NULL);
SELECT id, n & 3 FROM r0c_pgf_math_bitwise ORDER BY id;
SELECT id, n | 8 FROM r0c_pgf_math_bitwise ORDER BY id;
SELECT id, n # 2 FROM r0c_pgf_math_bitwise ORDER BY id;
SELECT id, ~ n FROM r0c_pgf_math_bitwise ORDER BY id;
SELECT id, n << 2 FROM r0c_pgf_math_bitwise ORDER BY id;
SELECT id, n >> 1 FROM r0c_pgf_math_bitwise ORDER BY id;
SELECT id, 5 & 3 FROM r0c_pgf_math_bitwise ORDER BY id;
SELECT id, 5 | 8 FROM r0c_pgf_math_bitwise ORDER BY id;
SELECT id, 5 # 2 FROM r0c_pgf_math_bitwise ORDER BY id;
SELECT id, ~ 5 FROM r0c_pgf_math_bitwise ORDER BY id;
SELECT id, 5 << 2 FROM r0c_pgf_math_bitwise ORDER BY id;
SELECT id, 5 >> 1 FROM r0c_pgf_math_bitwise ORDER BY id;
DROP TABLE r0c_pgf_math_bitwise;
