-- case: pgp_pl_cursor
-- title: PLpgSQL cursor open fetch close
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_cursor() RETURNS integer LANGUAGE plpgsql AS $f$DECLARE c CURSOR FOR SELECT 7; x integer; BEGIN OPEN c; FETCH c INTO x; CLOSE c; RETURN x; END;$f$;
SELECT r0c_pgp_pl_cursor();
DROP FUNCTION r0c_pgp_pl_cursor();
