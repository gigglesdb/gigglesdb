-- case: pgs_float_atanh
-- title: Floating point atanh over spread of inputs
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Floating point atanh over spread of inputs
SELECT x, atanh(x) FROM (VALUES (-0.99::float8),(-0.5::float8),(-0.1::float8),(0::float8),(0.1::float8),(0.5::float8),(0.99::float8)) AS inputs(x) ORDER BY x;
