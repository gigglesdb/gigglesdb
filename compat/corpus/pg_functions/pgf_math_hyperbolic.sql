-- case: pgf_math_hyperbolic
-- title: Printed hyperbolic functions
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-math.html
CREATE TABLE r0c_pgf_math_hyperbolic (id integer PRIMARY KEY, x double precision);
INSERT INTO r0c_pgf_math_hyperbolic VALUES (1,0.25),(2,0.5),(3,NULL);
SELECT id, sinh(x) FROM r0c_pgf_math_hyperbolic ORDER BY id;
SELECT id, cosh(x) FROM r0c_pgf_math_hyperbolic ORDER BY id;
SELECT id, tanh(x) FROM r0c_pgf_math_hyperbolic ORDER BY id;
SELECT id, asinh(x) FROM r0c_pgf_math_hyperbolic ORDER BY id;
SELECT id, acosh(x+1) FROM r0c_pgf_math_hyperbolic ORDER BY id;
SELECT id, atanh(x) FROM r0c_pgf_math_hyperbolic ORDER BY id;
SELECT id, cosh(2.0) FROM r0c_pgf_math_hyperbolic ORDER BY id;
SELECT id, acosh(2.0) FROM r0c_pgf_math_hyperbolic ORDER BY id;
SELECT id, atanh(0.5) FROM r0c_pgf_math_hyperbolic ORDER BY id;
DROP TABLE r0c_pgf_math_hyperbolic;
