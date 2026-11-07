-- case: composite_type
-- title: Composite type
-- owner: E2
-- expect: compatible
CREATE TYPE r0c_composite_type_t AS (a int, b text);
SELECT ROW(1,'x')::r0c_composite_type_t;
