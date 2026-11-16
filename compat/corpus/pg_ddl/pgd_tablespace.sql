-- case: pgd_tablespace
-- title: tablespace
-- owner: G
-- expect: compatible
-- source: Differences: CREATE TABLESPACE; W4 ALTER DROP TABLESPACE
-- @error
CREATE TABLESPACE r0c_pgd_tablespace LOCATION '/r0c_pgd_tablespace_missing';
-- @error
ALTER TABLESPACE r0c_pgd_tablespace RENAME TO r0c_pgd_tablespace_new;
-- @error
ALTER TABLESPACE r0c_pgd_tablespace OWNER TO postgres;
-- @error
DROP TABLESPACE r0c_pgd_tablespace;
SELECT count(*) FROM pg_tablespace WHERE spcname='r0c_pgd_tablespace';
