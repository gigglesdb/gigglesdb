-- case: pgp_cast
-- title: User composite cast to text
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE TYPE r0c_pgp_cast AS (v integer);
CREATE FUNCTION r0c_pgp_cast_f(r0c_pgp_cast) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT 'value=' || ($1).v$$;
CREATE CAST (r0c_pgp_cast AS text) WITH FUNCTION r0c_pgp_cast_f(r0c_pgp_cast) AS ASSIGNMENT;
SELECT ROW(7)::r0c_pgp_cast::text;
DROP CAST (r0c_pgp_cast AS text);
DROP FUNCTION r0c_pgp_cast_f(r0c_pgp_cast);
DROP TYPE r0c_pgp_cast;
