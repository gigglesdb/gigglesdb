-- case: pgp_function_drop
-- title: DROP overloaded signature and dependent view cascade
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_function_drop(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1$$;
CREATE FUNCTION r0c_pgp_function_drop(text) RETURNS text LANGUAGE sql AS $$SELECT $1$$;
CREATE VIEW r0c_pgp_function_drop_v AS SELECT r0c_pgp_function_drop(1) AS id;
-- @error
DROP FUNCTION r0c_pgp_function_drop(integer);
DROP FUNCTION r0c_pgp_function_drop(integer) CASCADE;
SELECT r0c_pgp_function_drop('survives'::text);
DROP FUNCTION r0c_pgp_function_drop(text);
