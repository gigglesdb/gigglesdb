-- case: pgp_ext_digest
-- title: Extension pgcrypto deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "pgcrypto";
SELECT encode(digest('abc','sha256'),'hex');
DROP EXTENSION "pgcrypto";
