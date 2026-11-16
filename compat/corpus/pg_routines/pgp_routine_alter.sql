-- case: pgp_routine_alter
-- title: ALTER and DROP ROUTINE resolve a function
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_routine_alter(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1$$;
ALTER ROUTINE r0c_pgp_routine_alter(integer) RENAME TO r0c_pgp_routine_alter_renamed;
SELECT r0c_pgp_routine_alter_renamed(8);
DROP ROUTINE r0c_pgp_routine_alter_renamed(integer);
