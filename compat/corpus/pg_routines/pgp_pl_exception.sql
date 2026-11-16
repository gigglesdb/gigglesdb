-- case: pgp_pl_exception
-- title: PLpgSQL exception recovery
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_exception() RETURNS text LANGUAGE plpgsql AS $f$BEGIN RAISE EXCEPTION USING ERRCODE='22012', MESSAGE='application failure'; EXCEPTION WHEN division_by_zero THEN RETURN SQLSTATE; END;$f$;
SELECT r0c_pgp_pl_exception();
DROP FUNCTION r0c_pgp_pl_exception();
