-- case: pgp_pl_notice
-- title: RAISE NOTICE leaves deterministic result
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_notice() RETURNS integer LANGUAGE plpgsql AS $f$BEGIN RAISE NOTICE 'application notice'; RETURN 7; END;$f$;
SELECT r0c_pgp_pl_notice();
DROP FUNCTION r0c_pgp_pl_notice();
