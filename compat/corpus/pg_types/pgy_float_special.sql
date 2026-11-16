-- case: pgy_float_special
-- title: float special
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
SELECT 'NaN'::float4,'Infinity'::float8,'-Infinity'::float8,0.1::float8+0.2::float8;
SET extra_float_digits=0;
SELECT 0.1::float8+0.2::float8;
RESET extra_float_digits;
