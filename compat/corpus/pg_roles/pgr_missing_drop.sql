-- case: pgr_missing_drop
-- title: Non-superuser missing view and sequence drops issue notices
-- owner: unowned
-- expect: compatible
-- source: Roles: DROP VIEW IF EXISTS DROP SEQUENCE IF EXISTS missing objects
CREATE ROLE r0c_pgr_missing_drop_u;
SET ROLE r0c_pgr_missing_drop_u;
DROP VIEW IF EXISTS r0c_pgr_missing_drop_v;
DROP SEQUENCE IF EXISTS r0c_pgr_missing_drop_s;
RESET ROLE;
DROP ROLE r0c_pgr_missing_drop_u;
