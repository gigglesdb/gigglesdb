-- case: pgs_float_acosh
-- title: Floating point acosh over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point acosh over spread of inputs
CREATE TABLE r0c_pgs_float_acosh (id int PRIMARY KEY, x double precision);
INSERT INTO r0c_pgs_float_acosh VALUES (1, 1), (2, 1.0000001), (3, 1.1), (4, 2), (5, 10), (6, 1000);
SELECT id, acosh(x) FROM r0c_pgs_float_acosh ORDER BY id;
