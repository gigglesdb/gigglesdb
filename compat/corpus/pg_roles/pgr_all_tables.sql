-- case: pgr_all_tables
-- title: Bulk grants on all tables in a schema
-- owner: unowned
-- expect: compatible
-- source: Roles: GRANT ON ALL TABLES SEQUENCES FUNCTIONS IN SCHEMA
CREATE ROLE r0c_pgr_all_tables_u;
CREATE SCHEMA r0c_pgr_all_tables;
GRANT USAGE ON SCHEMA r0c_pgr_all_tables TO r0c_pgr_all_tables_u;
CREATE TABLE r0c_pgr_all_tables.r0c_pgr_all_tables_t (id integer PRIMARY KEY);
INSERT INTO r0c_pgr_all_tables.r0c_pgr_all_tables_t VALUES (7);
SET ROLE r0c_pgr_all_tables_u;
-- @error
SELECT * FROM r0c_pgr_all_tables.r0c_pgr_all_tables_t ORDER BY id;
RESET ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA r0c_pgr_all_tables TO r0c_pgr_all_tables_u;
SET ROLE r0c_pgr_all_tables_u;
SELECT * FROM r0c_pgr_all_tables.r0c_pgr_all_tables_t ORDER BY id;
RESET ROLE;
REVOKE SELECT ON ALL TABLES IN SCHEMA r0c_pgr_all_tables FROM r0c_pgr_all_tables_u;
DROP TABLE r0c_pgr_all_tables.r0c_pgr_all_tables_t;
DROP SCHEMA r0c_pgr_all_tables;
DROP ROLE r0c_pgr_all_tables_u;
