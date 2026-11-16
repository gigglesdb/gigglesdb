-- case: pgy_column_varbit
-- title: column varbit
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_varbit (id int PRIMARY KEY, v varbit);
INSERT INTO r0c_pgy_column_varbit VALUES (1, B'1010'), (2, B'1010');
SELECT id, v, v=v, v || B'1' FROM r0c_pgy_column_varbit ORDER BY v, id;
