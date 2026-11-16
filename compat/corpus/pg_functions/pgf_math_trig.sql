-- case: pgf_math_trig
-- title: Printed trigonometric radians and degrees
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-math.html
CREATE TABLE r0c_pgf_math_trig (id integer PRIMARY KEY, x double precision);
INSERT INTO r0c_pgf_math_trig VALUES (1,0.25),(2,0.5),(3,NULL);
SELECT id, sin(x) FROM r0c_pgf_math_trig ORDER BY id;
SELECT id, cos(x) FROM r0c_pgf_math_trig ORDER BY id;
SELECT id, tan(x) FROM r0c_pgf_math_trig ORDER BY id;
SELECT id, cot(x) FROM r0c_pgf_math_trig ORDER BY id;
SELECT id, asin(x) FROM r0c_pgf_math_trig ORDER BY id;
SELECT id, acos(x) FROM r0c_pgf_math_trig ORDER BY id;
SELECT id, atan(x) FROM r0c_pgf_math_trig ORDER BY id;
SELECT id, atan2(x,2) FROM r0c_pgf_math_trig ORDER BY id;
SELECT id, pi() FROM r0c_pgf_math_trig ORDER BY id;
SELECT id, degrees(x) FROM r0c_pgf_math_trig ORDER BY id;
SELECT id, radians(90.0) FROM r0c_pgf_math_trig ORDER BY id;
DROP TABLE r0c_pgf_math_trig;
