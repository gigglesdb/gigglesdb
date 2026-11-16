-- case: pgr_view_create
-- title: CREATE VIEW succeeds without SELECT but reading it fails
-- owner: unowned
-- expect: compatible
-- source: Roles: CREATE VIEW source SELECT checked at execution
CREATE ROLE r0c_pgr_view_create_u;
CREATE SCHEMA r0c_pgr_view_create AUTHORIZATION r0c_pgr_view_create_u;
CREATE TABLE r0c_pgr_view_create.r0c_pgr_view_create_t (id integer PRIMARY KEY);
INSERT INTO r0c_pgr_view_create.r0c_pgr_view_create_t VALUES (1);
SET ROLE r0c_pgr_view_create_u;
CREATE VIEW r0c_pgr_view_create.r0c_pgr_view_create_v AS SELECT id FROM r0c_pgr_view_create.r0c_pgr_view_create_t;
-- @error
SELECT * FROM r0c_pgr_view_create.r0c_pgr_view_create_v;
RESET ROLE;
GRANT SELECT ON r0c_pgr_view_create.r0c_pgr_view_create_t TO r0c_pgr_view_create_u;
SET ROLE r0c_pgr_view_create_u;
SELECT * FROM r0c_pgr_view_create.r0c_pgr_view_create_v ORDER BY id;
RESET ROLE;
DROP VIEW r0c_pgr_view_create.r0c_pgr_view_create_v;
DROP TABLE r0c_pgr_view_create.r0c_pgr_view_create_t;
DROP SCHEMA r0c_pgr_view_create;
DROP ROLE r0c_pgr_view_create_u;
