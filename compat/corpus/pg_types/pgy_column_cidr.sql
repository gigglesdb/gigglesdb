-- case: pgy_column_cidr
-- title: column cidr
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_cidr (id int PRIMARY KEY, v cidr);
INSERT INTO r0c_pgy_column_cidr VALUES (1, '192.0.2.0/24'), (2, '192.0.2.0/24');
SELECT id, v, v=v, v >>= '192.0.2.7'::inet FROM r0c_pgy_column_cidr ORDER BY v, id;
