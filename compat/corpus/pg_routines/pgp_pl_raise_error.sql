-- case: pgp_pl_raise_error
-- title: PLpgSQL propagated SQLSTATE
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_raise_error() RETURNS integer LANGUAGE plpgsql AS $f$BEGIN RAISE EXCEPTION USING ERRCODE='P0001', MESSAGE='rejected'; END;$f$;
-- @error
SELECT r0c_pgp_pl_raise_error();
DROP FUNCTION r0c_pgp_pl_raise_error();
