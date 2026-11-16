-- case: pgp_pl_diagnostics
-- title: PLpgSQL GET DIAGNOSTICS row count
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_diagnostics() RETURNS bigint LANGUAGE plpgsql AS $f$DECLARE n bigint; BEGIN PERFORM g FROM generate_series(1,4) g; GET DIAGNOSTICS n = ROW_COUNT; RETURN n; END;$f$;
SELECT r0c_pgp_pl_diagnostics();
DROP FUNCTION r0c_pgp_pl_diagnostics();
