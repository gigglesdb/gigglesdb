-- case: pgs_float_other
-- title: Floating point other over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point other over spread of inputs
SELECT x, cos(x), tanh(x), atan(x), asinh(x), exp(x), sqrt(abs(x)), ln(abs(x)+1), log(abs(x)+1), power(abs(x)+1, 0.3::float8) FROM (VALUES (-10::float8),(-1::float8),(-0.1::float8),(0::float8),(0.1::float8),(1::float8),(10::float8)) AS inputs(x) ORDER BY x;
