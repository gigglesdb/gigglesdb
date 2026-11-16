-- case: pgp_pl_dynamic
-- title: Dynamic EXECUTE USING parameters
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_dynamic(x integer) RETURNS integer LANGUAGE plpgsql AS $f$DECLARE y integer; BEGIN EXECUTE 'SELECT $1 + 7' INTO y USING x; RETURN y; END;$f$;
SELECT r0c_pgp_pl_dynamic(5);
DROP FUNCTION r0c_pgp_pl_dynamic(x integer);
