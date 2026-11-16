-- case: pgp_pl_while
-- title: PLpgSQL while loop
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_while() RETURNS integer LANGUAGE plpgsql AS $f$DECLARE s integer := 0; i integer; BEGIN WHILE s < 3 LOOP s := s+1; END LOOP; RETURN s; END;$f$;
SELECT r0c_pgp_pl_while();
DROP FUNCTION r0c_pgp_pl_while();
