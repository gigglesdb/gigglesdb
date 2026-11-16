-- case: pgs_float_sin
-- title: Floating point sin over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point sin over spread of inputs
SELECT x, sin(x) FROM (VALUES (-100::float8),(-3.141592653589793::float8),(-1::float8),(-0.1::float8),(0::float8),(0.1::float8),(1::float8),(3.141592653589793::float8),(100::float8)) AS inputs(x) ORDER BY x;
