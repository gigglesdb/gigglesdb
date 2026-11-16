-- case: pgs_float_acosh
-- title: Floating point acosh over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point acosh over spread of inputs
SELECT x, acosh(x) FROM (VALUES (1::float8),(1.0000001::float8),(1.1::float8),(2::float8),(10::float8),(1000::float8)) AS inputs(x) ORDER BY x;
