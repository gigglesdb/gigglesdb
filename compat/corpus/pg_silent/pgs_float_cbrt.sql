-- case: pgs_float_cbrt
-- title: Floating point cbrt over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point cbrt over spread of inputs
CREATE TABLE r0c_pgs_float_cbrt (id int PRIMARY KEY, x double precision);
INSERT INTO r0c_pgs_float_cbrt VALUES (1, -1000), (2, -27), (3, -2), (4, -0.1), (5, 0), (6, 0.1), (7, 2), (8, 27), (9, 1000);
SELECT id, cbrt(x) FROM r0c_pgs_float_cbrt ORDER BY id;
