-- case: immutable_overflow_error
-- title: Immutable overflow error
-- owner: E2
-- expect: compatible
-- @error
-- expect-sqlstate: 22003
SELECT abs(-2147483648);
