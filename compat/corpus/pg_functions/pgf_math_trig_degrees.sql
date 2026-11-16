-- case: pgf_math_trig_degrees
-- title: Printed degree trigonometric functions
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-math.html
CREATE TABLE r0c_pgf_math_trig_degrees (id integer PRIMARY KEY, x double precision);
INSERT INTO r0c_pgf_math_trig_degrees VALUES (1,0.25),(2,0.5),(3,NULL);
SELECT id, sind(x) FROM r0c_pgf_math_trig_degrees ORDER BY id;
SELECT id, cosd(x) FROM r0c_pgf_math_trig_degrees ORDER BY id;
SELECT id, tand(x) FROM r0c_pgf_math_trig_degrees ORDER BY id;
SELECT id, cotd(x) FROM r0c_pgf_math_trig_degrees ORDER BY id;
SELECT id, asind(x) FROM r0c_pgf_math_trig_degrees ORDER BY id;
SELECT id, acosd(x) FROM r0c_pgf_math_trig_degrees ORDER BY id;
SELECT id, atand(x) FROM r0c_pgf_math_trig_degrees ORDER BY id;
SELECT id, atan2d(x,2) FROM r0c_pgf_math_trig_degrees ORDER BY id;
SELECT id, sind(30) FROM r0c_pgf_math_trig_degrees ORDER BY id;
SELECT id, cosd(60) FROM r0c_pgf_math_trig_degrees ORDER BY id;
DROP TABLE r0c_pgf_math_trig_degrees;
