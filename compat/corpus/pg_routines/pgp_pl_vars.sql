-- case: pgp_pl_vars
-- title: PLpgSQL local variables
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_vars(x integer) RETURNS integer LANGUAGE plpgsql AS $f$DECLARE y integer := x+3; BEGIN y := y*2; RETURN y; END;$f$;
SELECT r0c_pgp_pl_vars(4);
DROP FUNCTION r0c_pgp_pl_vars(x integer);
