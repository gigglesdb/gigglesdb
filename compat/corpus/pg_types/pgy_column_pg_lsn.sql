-- case: pgy_column_pg_lsn
-- title: column pg lsn
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_pg_lsn (id int PRIMARY KEY, v pg_lsn);
INSERT INTO r0c_pgy_column_pg_lsn VALUES (1, '0/10'), (2, '0/10');
SELECT id, v, v=v, v - '0/0'::pg_lsn FROM r0c_pgy_column_pg_lsn ORDER BY v, id;
