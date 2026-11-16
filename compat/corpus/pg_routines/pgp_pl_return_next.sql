-- case: pgp_pl_return_next
-- title: PLpgSQL RETURN NEXT
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_return_next() RETURNS SETOF integer LANGUAGE plpgsql AS $f$BEGIN RETURN NEXT 2; RETURN NEXT 1; RETURN; END;$f$;
SELECT * FROM r0c_pgp_pl_return_next() ORDER BY 1;
DROP FUNCTION r0c_pgp_pl_return_next();
