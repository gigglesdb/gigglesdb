-- case: pgf_math_operators
-- title: Exponent absolute root and remainder operators
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-math.html
CREATE TABLE r0c_pgf_math_operators (id integer PRIMARY KEY, x double precision, n integer);
INSERT INTO r0c_pgf_math_operators VALUES (1,8,9),(2,27,-8),(3,NULL,NULL);
SELECT id, x ^ 2 FROM r0c_pgf_math_operators ORDER BY id;
SELECT id, @ n FROM r0c_pgf_math_operators ORDER BY id;
SELECT id, |/ x FROM r0c_pgf_math_operators ORDER BY id;
SELECT id, ||/ x FROM r0c_pgf_math_operators ORDER BY id;
SELECT id, n % 3 FROM r0c_pgf_math_operators ORDER BY id;
SELECT id, 2.0 ^ 3.0 FROM r0c_pgf_math_operators ORDER BY id;
SELECT id, @ (-4.5) FROM r0c_pgf_math_operators ORDER BY id;
SELECT id, |/ 16.0 FROM r0c_pgf_math_operators ORDER BY id;
SELECT id, ||/ 64.0 FROM r0c_pgf_math_operators ORDER BY id;
SELECT id, abs(n) FROM r0c_pgf_math_operators ORDER BY id;
SELECT id, factorial((CASE WHEN n<0 THEN -n ELSE n END)::bigint) FROM r0c_pgf_math_operators ORDER BY id;
DROP TABLE r0c_pgf_math_operators;
