-- case: pgp_alter_transform
-- title: Unsupported grammar has PostgreSQL SQLSTATE
-- owner: unowned
-- expect: compatible
-- source: W7 command grammar: PostgreSQL has no ALTER TRANSFORM
-- @error
ALTER TRANSFORM FOR integer LANGUAGE plpgsql OWNER TO postgres;
