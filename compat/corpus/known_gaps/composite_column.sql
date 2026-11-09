-- case: composite_column
-- title: Composite-typed columns
-- owner: E2
-- expect: intentional Composite-typed columns
CREATE TYPE r0c_composite_column_type AS (a int);
-- expect-sqlstate: 0A000
CREATE TABLE r0c_composite_column (id int PRIMARY KEY, v r0c_composite_column_type);
