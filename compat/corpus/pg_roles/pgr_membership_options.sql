-- case: pgr_membership_options
-- title: NOINHERIT and membership SET and ADMIN options
-- owner: unowned
-- expect: compatible
-- source: Roles: membership NOINHERIT SET ADMIN; W6 GRANT REVOKE
CREATE ROLE r0c_pgr_membership_options_g;
CREATE ROLE r0c_pgr_membership_options_u NOINHERIT;
CREATE ROLE r0c_pgr_membership_options_peer;
CREATE TABLE r0c_pgr_membership_options (id integer PRIMARY KEY);
INSERT INTO r0c_pgr_membership_options VALUES (1);
GRANT SELECT ON r0c_pgr_membership_options TO r0c_pgr_membership_options_g;
GRANT r0c_pgr_membership_options_g TO r0c_pgr_membership_options_u WITH INHERIT FALSE, SET TRUE, ADMIN TRUE;
SET SESSION AUTHORIZATION r0c_pgr_membership_options_u;
SELECT pg_has_role(current_user, 'r0c_pgr_membership_options_g', 'MEMBER'), pg_has_role(current_user, 'r0c_pgr_membership_options_g', 'USAGE'), pg_has_role(current_user, 'r0c_pgr_membership_options_g', 'SET');
-- @error
SELECT * FROM r0c_pgr_membership_options;
GRANT r0c_pgr_membership_options_g TO r0c_pgr_membership_options_peer;
SET ROLE r0c_pgr_membership_options_g;
SELECT * FROM r0c_pgr_membership_options ORDER BY id;
RESET SESSION AUTHORIZATION;
REVOKE r0c_pgr_membership_options_g FROM r0c_pgr_membership_options_peer;
-- @error
REVOKE ADMIN OPTION FOR r0c_pgr_membership_options_g FROM r0c_pgr_membership_options_u;
REVOKE ADMIN OPTION FOR r0c_pgr_membership_options_g FROM r0c_pgr_membership_options_u CASCADE;
SELECT pg_has_role('r0c_pgr_membership_options_peer', 'r0c_pgr_membership_options_g', 'MEMBER') AS dependent_membership;
GRANT r0c_pgr_membership_options_g TO r0c_pgr_membership_options_u WITH SET FALSE;
SET SESSION AUTHORIZATION r0c_pgr_membership_options_u;
-- @error
SET ROLE r0c_pgr_membership_options_g;
-- @error
GRANT r0c_pgr_membership_options_g TO r0c_pgr_membership_options_peer;
RESET SESSION AUTHORIZATION;
DROP TABLE r0c_pgr_membership_options;
DROP ROLE r0c_pgr_membership_options_u, r0c_pgr_membership_options_peer, r0c_pgr_membership_options_g;
