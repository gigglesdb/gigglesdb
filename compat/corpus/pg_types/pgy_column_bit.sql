-- case: pgy_column_bit
-- title: column bit
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_bit (id int PRIMARY KEY, v bit(4));
INSERT INTO r0c_pgy_column_bit VALUES (1, B'1010'), (2, B'1010');
SELECT id, v, v=v, v & B'1100' FROM r0c_pgy_column_bit ORDER BY v, id;
