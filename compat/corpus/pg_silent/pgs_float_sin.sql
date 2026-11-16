-- case: pgs_float_sin
-- title: Floating point sin over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point sin over spread of inputs
CREATE TABLE r0c_pgs_float_sin (id int PRIMARY KEY, x double precision);
INSERT INTO r0c_pgs_float_sin VALUES (1, -100), (2, -3.141592653589793), (3, -1), (4, -0.1), (5, 0), (6, 0.1), (7, 1), (8, 3.141592653589793), (9, 100);
SELECT id, sin(x) FROM r0c_pgs_float_sin ORDER BY id;
