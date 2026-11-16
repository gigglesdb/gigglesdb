-- case: pgp_language
-- title: Procedural language alias lifecycle
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE TRUSTED LANGUAGE r0c_pgp_language HANDLER plpgsql_call_handler INLINE plpgsql_inline_handler VALIDATOR plpgsql_validator;
CREATE FUNCTION r0c_pgp_language_f() RETURNS integer LANGUAGE r0c_pgp_language AS $$BEGIN RETURN 3; END;$$;
SELECT r0c_pgp_language_f();
ALTER LANGUAGE r0c_pgp_language RENAME TO r0c_pgp_language_renamed;
DROP FUNCTION r0c_pgp_language_f();
DROP LANGUAGE r0c_pgp_language_renamed;
