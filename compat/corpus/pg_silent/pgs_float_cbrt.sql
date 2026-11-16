-- case: pgs_float_cbrt
-- title: Floating point cbrt over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point cbrt over spread of inputs
SELECT x, cbrt(x) FROM (VALUES (-1000::float8),(-27::float8),(-2::float8),(-0.1::float8),(0::float8),(0.1::float8),(2::float8),(27::float8),(1000::float8)) AS inputs(x) ORDER BY x;
