-- case: pgf_errors
-- title: Common scalar errors retain PostgreSQL SQLSTATE
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-math.html
-- @error
SELECT 1/0;
-- @error
SELECT sqrt(-1::numeric);
-- @error
SELECT ln(0::numeric);
-- @error
SELECT width_bucket(1,0,10,0);
-- @error
SELECT substring('abc' FROM 1 FOR -1);
-- @error
SELECT regexp_match('abc','[');
-- @error
SELECT 'bad'::integer;
-- @error
SELECT to_date('2024-13-01','YYYY-MM-DD');
