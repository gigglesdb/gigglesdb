-- case: pgy_collation_lifecycle
-- title: collation lifecycle
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: COLLATE; W5 CREATE/ALTER/DROP COLLATION
CREATE COLLATION r0c_pgy_collation_lifecycle_c (provider=libc,locale='C');
ALTER COLLATION r0c_pgy_collation_lifecycle_c REFRESH VERSION;
ALTER COLLATION r0c_pgy_collation_lifecycle_c OWNER TO postgres;
ALTER COLLATION r0c_pgy_collation_lifecycle_c RENAME TO r0c_pgy_collation_lifecycle_new;
CREATE SCHEMA r0c_pgy_collation_lifecycle_s;
ALTER COLLATION r0c_pgy_collation_lifecycle_new SET SCHEMA r0c_pgy_collation_lifecycle_s;
SELECT 'a' COLLATE r0c_pgy_collation_lifecycle_s.r0c_pgy_collation_lifecycle_new < 'b' COLLATE r0c_pgy_collation_lifecycle_s.r0c_pgy_collation_lifecycle_new;
DROP COLLATION r0c_pgy_collation_lifecycle_s.r0c_pgy_collation_lifecycle_new;
DROP SCHEMA r0c_pgy_collation_lifecycle_s;
