-- case: pgp_create_routine
-- title: Unsupported grammar has PostgreSQL SQLSTATE
-- owner: unowned
-- expect: compatible
-- source: W7 command grammar: PostgreSQL has no CREATE ROUTINE
-- @error
CREATE ROUTINE r0c_pgp_create_routine() LANGUAGE sql AS $$SELECT 1$$;
