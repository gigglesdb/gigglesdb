-- case: pgp_pl_if_case
-- title: PLpgSQL IF and CASE
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_pl_if_case(x integer) RETURNS text LANGUAGE plpgsql AS $f$BEGIN IF x < 0 THEN RETURN 'negative'; END IF; CASE x WHEN 0 THEN RETURN 'zero'; ELSE RETURN 'positive'; END CASE; END;$f$;
SELECT r0c_pgp_pl_if_case(-1);
SELECT r0c_pgp_pl_if_case(0);
SELECT r0c_pgp_pl_if_case(1);
DROP FUNCTION r0c_pgp_pl_if_case(x integer);
