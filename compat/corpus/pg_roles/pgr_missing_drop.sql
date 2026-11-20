-- case: pgr_missing_drop
-- title: Non-superuser missing view and sequence drops issue notices
-- owner: unowned
-- expect: compatible
-- source: Roles: DROP VIEW IF EXISTS DROP SEQUENCE IF EXISTS missing objects
CREATE ROLE r0c_pgr_missing_drop_u;
SET ROLE r0c_pgr_missing_drop_u;
DROP VIEW IF EXISTS r0c_pgr_missing_drop_v;
DROP SEQUENCE IF EXISTS r0c_pgr_missing_drop_s;
DROP TABLE IF EXISTS r0c_pgr_missing_drop_t;
DROP INDEX IF EXISTS r0c_pgr_missing_drop_i;
DROP VIEW IF EXISTS r0c_pgr_missing_drop_absent.v;
DROP SEQUENCE IF EXISTS r0c_pgr_missing_drop_absent.s;
DROP TABLE IF EXISTS r0c_pgr_missing_drop_absent.t;
DROP INDEX IF EXISTS r0c_pgr_missing_drop_absent.i;
-- @error
DROP VIEW r0c_pgr_missing_drop_v;
RESET ROLE;
DROP ROLE r0c_pgr_missing_drop_u;
