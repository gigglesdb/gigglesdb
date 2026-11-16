-- case: pgp_alter_access_method
-- title: Unsupported grammar has PostgreSQL SQLSTATE
-- owner: unowned
-- expect: compatible
-- source: W7 command grammar: PostgreSQL has no ALTER ACCESS_METHOD
-- @error
ALTER ACCESS METHOD r0c_pgp_alter_access_method OWNER TO postgres;
