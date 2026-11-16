-- case: pgy_column_macaddr
-- title: column macaddr
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_macaddr (id int PRIMARY KEY, v macaddr);
INSERT INTO r0c_pgy_column_macaddr VALUES (1, '08:00:2b:01:02:03'), (2, '08:00:2b:01:02:03');
SELECT id, v, v=v, trunc(v) FROM r0c_pgy_column_macaddr ORDER BY v, id;
