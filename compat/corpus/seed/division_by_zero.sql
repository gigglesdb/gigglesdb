-- case: division_by_zero
-- title: Integer division by zero reports an error
-- owner: E2
-- expect: compatible
-- @error
-- expect-sqlstate: 22012
SELECT 1/0;
