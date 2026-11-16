-- case: pgp_pl_foreach
-- title: PLpgSQL foreach loop
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_foreach() RETURNS integer LANGUAGE plpgsql AS $f$DECLARE s integer := 0; i integer; BEGIN FOREACH i IN ARRAY ARRAY[2,4,6] LOOP s := s+i; END LOOP; RETURN s; END;$f$;
SELECT r0c_pgp_pl_foreach();
DROP FUNCTION r0c_pgp_pl_foreach();
