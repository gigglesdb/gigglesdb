-- case: pgy_column_inet
-- title: column inet
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_inet (id int PRIMARY KEY, v inet);
INSERT INTO r0c_pgy_column_inet VALUES (1, '192.0.2.7/24'), (2, '192.0.2.7/24');
SELECT id, v, v=v, v << '192.0.2.0/24'::inet FROM r0c_pgy_column_inet ORDER BY v, id;
