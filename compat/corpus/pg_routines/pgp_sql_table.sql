-- case: pgp_sql_table
-- title: SQL RETURNS TABLE report
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_table(n integer) RETURNS TABLE(id integer, label text) LANGUAGE sql AS $f$SELECT g, 'item-' || g FROM generate_series(1,n) AS g$f$;
SELECT * FROM r0c_pgp_sql_table(2) ORDER BY id;
DROP FUNCTION r0c_pgp_sql_table(n integer);
