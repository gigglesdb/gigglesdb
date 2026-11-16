-- case: pgp_pl_for
-- title: PLpgSQL for loop
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_for() RETURNS integer LANGUAGE plpgsql AS $f$DECLARE s integer := 0; i integer; BEGIN FOR i IN 1..3 LOOP s := s+i; END LOOP; RETURN s; END;$f$;
SELECT r0c_pgp_pl_for();
DROP FUNCTION r0c_pgp_pl_for();
