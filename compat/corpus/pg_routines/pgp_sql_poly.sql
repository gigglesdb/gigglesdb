-- case: pgp_sql_poly
-- title: Polymorphic anyelement identity
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_poly(x anyelement) RETURNS anyelement LANGUAGE sql AS $f$SELECT x$f$;
SELECT r0c_pgp_sql_poly(42);
SELECT r0c_pgp_sql_poly('text'::text);
DROP FUNCTION r0c_pgp_sql_poly(x anyelement);
