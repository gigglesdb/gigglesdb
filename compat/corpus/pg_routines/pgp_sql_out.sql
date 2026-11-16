-- case: pgp_sql_out
-- title: SQL output parameters
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_out(IN x integer, OUT doubled integer, OUT label text) LANGUAGE sql AS $$SELECT x*2, 'ok'::text$$;
SELECT * FROM r0c_pgp_sql_out(3);
DROP FUNCTION r0c_pgp_sql_out(integer);
