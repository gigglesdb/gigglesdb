-- case: pgf_math_transcendental
-- title: Printed roots logarithms and exponentials
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-math.html
CREATE TABLE r0c_pgf_math_transcendental (id integer PRIMARY KEY, x double precision);
INSERT INTO r0c_pgf_math_transcendental VALUES (1,2),(2,8),(3,NULL);
SELECT id, sqrt(x) FROM r0c_pgf_math_transcendental ORDER BY id;
SELECT id, cbrt(x) FROM r0c_pgf_math_transcendental ORDER BY id;
SELECT id, exp(x) FROM r0c_pgf_math_transcendental ORDER BY id;
SELECT id, ln(x) FROM r0c_pgf_math_transcendental ORDER BY id;
SELECT id, log(x) FROM r0c_pgf_math_transcendental ORDER BY id;
SELECT id, log10(x) FROM r0c_pgf_math_transcendental ORDER BY id;
SELECT id, power(x,3) FROM r0c_pgf_math_transcendental ORDER BY id;
SELECT id, log(2::numeric,8::numeric) FROM r0c_pgf_math_transcendental ORDER BY id;
SELECT id, sqrt(16::numeric) FROM r0c_pgf_math_transcendental ORDER BY id;
SELECT id, cbrt(27::double precision) FROM r0c_pgf_math_transcendental ORDER BY id;
SELECT id, gcd(24,18) FROM r0c_pgf_math_transcendental ORDER BY id;
SELECT id, lcm(6,8) FROM r0c_pgf_math_transcendental ORDER BY id;
DROP TABLE r0c_pgf_math_transcendental;
