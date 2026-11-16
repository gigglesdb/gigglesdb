-- case: pgs_float_cosh
-- title: Floating point cosh over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point cosh over spread of inputs
CREATE TABLE r0c_pgs_float_cosh (id int PRIMARY KEY, x double precision);
INSERT INTO r0c_pgs_float_cosh VALUES (1, -10), (2, -1), (3, -0.1), (4, 0), (5, 0.1), (6, 1), (7, 10);
SELECT id, cosh(x) FROM r0c_pgs_float_cosh ORDER BY id;
