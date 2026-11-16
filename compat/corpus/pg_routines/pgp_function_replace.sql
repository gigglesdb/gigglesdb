-- case: pgp_function_replace
-- title: CREATE OR REPLACE retains identity changes body
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_function_replace(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1+1$$;
SELECT r0c_pgp_function_replace(1);
CREATE OR REPLACE FUNCTION r0c_pgp_function_replace(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1+2$$;
SELECT r0c_pgp_function_replace(1);
-- @error
CREATE OR REPLACE FUNCTION r0c_pgp_function_replace(integer) RETURNS text LANGUAGE sql AS $$SELECT $1::text$$;
DROP FUNCTION r0c_pgp_function_replace(integer);
