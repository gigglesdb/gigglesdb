-- case: pgp_alter_cast
-- title: Unsupported grammar has PostgreSQL SQLSTATE
-- owner: unowned
-- expect: compatible
-- source: W7 command grammar: PostgreSQL has no ALTER CAST
-- @error
ALTER CAST (integer AS text) OWNER TO postgres;
