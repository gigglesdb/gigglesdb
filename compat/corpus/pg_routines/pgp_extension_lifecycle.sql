-- case: pgp_extension_lifecycle
-- title: Extension IF NOT EXISTS schema relocation and drop
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE SCHEMA r0c_pgp_extension_lifecycle;
CREATE SCHEMA r0c_pgp_extension_lifecycle_dest;
CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA r0c_pgp_extension_lifecycle;
CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA r0c_pgp_extension_lifecycle;
ALTER EXTENSION hstore SET SCHEMA r0c_pgp_extension_lifecycle_dest;
SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='hstore';
DROP EXTENSION hstore;
DROP SCHEMA r0c_pgp_extension_lifecycle_dest;
DROP SCHEMA r0c_pgp_extension_lifecycle;
