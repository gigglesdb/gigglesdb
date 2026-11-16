-- case: pgy_column_money
-- title: column money
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_money (id int PRIMARY KEY, v money);
INSERT INTO r0c_pgy_column_money VALUES (1, '12.34'), (2, '12.34');
SELECT id, v, v=v, v + v FROM r0c_pgy_column_money ORDER BY v, id;
