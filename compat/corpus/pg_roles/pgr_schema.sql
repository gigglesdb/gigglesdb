-- case: pgr_schema
-- title: Schema USAGE and CREATE independently control access
-- owner: unowned
-- expect: compatible
-- source: Roles: privileges on non-public schemas
CREATE ROLE r0c_pgr_schema_u;
CREATE SCHEMA r0c_pgr_schema;
CREATE TABLE r0c_pgr_schema.r0c_pgr_schema_t (id integer PRIMARY KEY);
INSERT INTO r0c_pgr_schema.r0c_pgr_schema_t VALUES (1);
GRANT SELECT ON r0c_pgr_schema.r0c_pgr_schema_t TO r0c_pgr_schema_u;
SET ROLE r0c_pgr_schema_u;
-- @error
SELECT * FROM r0c_pgr_schema.r0c_pgr_schema_t;
RESET ROLE;
GRANT USAGE ON SCHEMA r0c_pgr_schema TO r0c_pgr_schema_u;
SET ROLE r0c_pgr_schema_u;
SELECT * FROM r0c_pgr_schema.r0c_pgr_schema_t ORDER BY id;
-- @error
CREATE TABLE r0c_pgr_schema.r0c_pgr_schema_new (id integer PRIMARY KEY);
RESET ROLE;
GRANT CREATE ON SCHEMA r0c_pgr_schema TO r0c_pgr_schema_u;
SET ROLE r0c_pgr_schema_u;
CREATE TABLE r0c_pgr_schema.r0c_pgr_schema_new (id integer PRIMARY KEY);
DROP TABLE r0c_pgr_schema.r0c_pgr_schema_new;
RESET ROLE;
DROP TABLE r0c_pgr_schema.r0c_pgr_schema_t;
DROP SCHEMA r0c_pgr_schema;
DROP ROLE r0c_pgr_schema_u;
