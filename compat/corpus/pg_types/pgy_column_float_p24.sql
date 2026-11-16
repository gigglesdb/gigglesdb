-- case: pgy_column_float_p24
-- title: column float p24
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_float_p24 (id int PRIMARY KEY, v float(24));
INSERT INTO r0c_pgy_column_float_p24 VALUES (1, 7), (2, 7);
SELECT id, v, v = v, v + 1 FROM r0c_pgy_column_float_p24 ORDER BY v, id;
