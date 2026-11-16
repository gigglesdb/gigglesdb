-- case: pgs_float_other
-- title: Floating point other over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point other over spread of inputs
CREATE TABLE r0c_pgs_float_other (id int PRIMARY KEY, x double precision);
INSERT INTO r0c_pgs_float_other VALUES (1, -10), (2, -1), (3, -0.1), (4, 0), (5, 0.1), (6, 1), (7, 10);
SELECT id, cos(x) FROM r0c_pgs_float_other ORDER BY id;
SELECT id, tanh(x) FROM r0c_pgs_float_other ORDER BY id;
SELECT id, atan(x) FROM r0c_pgs_float_other ORDER BY id;
SELECT id, asinh(x) FROM r0c_pgs_float_other ORDER BY id;
SELECT id, exp(x) FROM r0c_pgs_float_other ORDER BY id;
SELECT id, sqrt(abs(x)) FROM r0c_pgs_float_other ORDER BY id;
SELECT id, ln(abs(x)+1) FROM r0c_pgs_float_other ORDER BY id;
SELECT id, log(abs(x)+1) FROM r0c_pgs_float_other ORDER BY id;
SELECT id, power(abs(x)+1, 0.3::float8) FROM r0c_pgs_float_other ORDER BY id;
