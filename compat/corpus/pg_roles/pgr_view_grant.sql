-- case: pgr_view_grant
-- title: GRANT on a view allows view-only readers through owner rights
-- owner: unowned
-- expect: compatible
-- source: Roles: privileges on a view; reading checks view owner privileges
CREATE ROLE r0c_pgr_view_grant_owner;
CREATE ROLE r0c_pgr_view_grant_u;
CREATE SCHEMA r0c_pgr_view_grant AUTHORIZATION r0c_pgr_view_grant_owner;
GRANT USAGE ON SCHEMA r0c_pgr_view_grant TO r0c_pgr_view_grant_u;
SET ROLE r0c_pgr_view_grant_owner;
CREATE TABLE r0c_pgr_view_grant.r0c_pgr_view_grant_t (id integer PRIMARY KEY, value text);
INSERT INTO r0c_pgr_view_grant.r0c_pgr_view_grant_t VALUES (1, 'view-visible');
CREATE VIEW r0c_pgr_view_grant.r0c_pgr_view_grant_v WITH (security_invoker = false) AS SELECT id, value FROM r0c_pgr_view_grant.r0c_pgr_view_grant_t;
GRANT SELECT ON r0c_pgr_view_grant.r0c_pgr_view_grant_v TO r0c_pgr_view_grant_u;
RESET ROLE;
SET ROLE r0c_pgr_view_grant_u;
SELECT * FROM r0c_pgr_view_grant.r0c_pgr_view_grant_v ORDER BY id;
-- @error
SELECT * FROM r0c_pgr_view_grant.r0c_pgr_view_grant_t;
RESET ROLE;
DROP VIEW r0c_pgr_view_grant.r0c_pgr_view_grant_v;
DROP TABLE r0c_pgr_view_grant.r0c_pgr_view_grant_t;
DROP SCHEMA r0c_pgr_view_grant;
DROP ROLE r0c_pgr_view_grant_owner, r0c_pgr_view_grant_u;
