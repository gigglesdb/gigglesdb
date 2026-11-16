-- case: pgs_float_cosh
-- title: Floating point cosh over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point cosh over spread of inputs
SELECT x, cosh(x) FROM (VALUES (-10::float8),(-1::float8),(-0.1::float8),(0::float8),(0.1::float8),(1::float8),(10::float8)) AS inputs(x) ORDER BY x;
