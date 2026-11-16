-- case: pgp_pl_return_query
-- title: PLpgSQL RETURN QUERY
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_return_query() RETURNS TABLE(id integer) LANGUAGE plpgsql AS $f$BEGIN RETURN QUERY SELECT generate_series(1,3); END;$f$;
SELECT * FROM r0c_pgp_pl_return_query() ORDER BY id;
DROP FUNCTION r0c_pgp_pl_return_query();
