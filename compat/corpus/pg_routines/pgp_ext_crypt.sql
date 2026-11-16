-- case: pgp_ext_crypt
-- title: Extension pgcrypto deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "pgcrypto";
SELECT crypt('password','$1$fixed123$');
DROP EXTENSION "pgcrypto";
