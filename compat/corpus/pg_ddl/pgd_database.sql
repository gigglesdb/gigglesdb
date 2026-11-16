-- case: pgd_database
-- title: database
-- owner: G
-- expect: compatible
-- source: Differences: CREATE DATABASE; W4 ALTER DROP DATABASE
CREATE DATABASE r0c_pgd_database TEMPLATE template0;
ALTER DATABASE r0c_pgd_database RENAME TO r0c_pgd_database_new;
ALTER DATABASE r0c_pgd_database_new OWNER TO postgres;
ALTER DATABASE r0c_pgd_database_new CONNECTION LIMIT 5;
COMMENT ON DATABASE r0c_pgd_database_new IS 'migration database';
SELECT datname,datconnlimit,shobj_description(oid,'pg_database') FROM pg_database WHERE datname='r0c_pgd_database_new';
DROP DATABASE r0c_pgd_database_new;
