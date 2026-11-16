-- case: pgp_sql_variadic
-- title: Variadic SQL argument array
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_variadic(VARIADIC xs integer[]) RETURNS integer LANGUAGE sql AS $f$SELECT sum(v)::integer FROM unnest(xs) v$f$;
SELECT r0c_pgp_sql_variadic(1,2,3);
SELECT r0c_pgp_sql_variadic(VARIADIC ARRAY[4,5]);
DROP FUNCTION r0c_pgp_sql_variadic(VARIADIC xs integer[]);
