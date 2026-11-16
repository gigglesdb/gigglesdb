-- case: pgp_ext_uuid
-- title: Extension uuid-ossp deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "uuid-ossp";
SELECT uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8','example.com');
DROP EXTENSION "uuid-ossp";
