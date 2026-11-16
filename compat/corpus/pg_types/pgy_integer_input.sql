-- case: pgy_integer_input
-- title: integer input
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
SELECT ' 7 '::int,'1_000'::int,'0x10'::int,'0o17'::int,'0b101'::int,1_000;
-- @error
SELECT '32768'::smallint;
-- @error
SELECT 2147483647+1;
-- @error
SELECT '9223372036854775808'::bigint;
-- @error
SELECT 'bad'::int;
