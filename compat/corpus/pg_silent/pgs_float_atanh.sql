-- case: pgs_float_atanh
-- title: Floating point atanh over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point atanh over spread of inputs
CREATE TABLE r0c_pgs_float_atanh (id int PRIMARY KEY, x double precision);
INSERT INTO r0c_pgs_float_atanh VALUES (1, -0.99), (2, -0.5), (3, -0.1), (4, 0), (5, 0.1), (6, 0.5), (7, 0.99);
SELECT id, atanh(x) FROM r0c_pgs_float_atanh ORDER BY id;
