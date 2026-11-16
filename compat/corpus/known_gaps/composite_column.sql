-- case: composite_column
-- title: Composite-typed columns
-- owner: E2
-- expect: compatible
-- source: Differences: Published differences / composite column
CREATE TYPE r0c_composite_column_type AS (a int);
CREATE TABLE r0c_composite_column (id int PRIMARY KEY, v r0c_composite_column_type);
INSERT INTO r0c_composite_column VALUES (1, ROW(7));
SELECT (v).a FROM r0c_composite_column;
UPDATE r0c_composite_column SET v.a = 9 WHERE id = 1;
SELECT (v).a FROM r0c_composite_column;
DROP TABLE r0c_composite_column;
DROP TYPE r0c_composite_column_type;
