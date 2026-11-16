-- case: pgr_membership
-- title: Inherited table access and IN ROLE membership
-- owner: unowned
-- expect: compatible
-- source: Roles: role membership INHERIT IN ROLE; W6 GRANT REVOKE SET ROLE
CREATE ROLE r0c_pgr_membership_g;
CREATE ROLE r0c_pgr_membership_u INHERIT IN ROLE r0c_pgr_membership_g;
CREATE TABLE r0c_pgr_membership (id integer PRIMARY KEY);
INSERT INTO r0c_pgr_membership VALUES (1);
GRANT SELECT ON r0c_pgr_membership TO r0c_pgr_membership_g;
SET ROLE r0c_pgr_membership_u;
SELECT * FROM r0c_pgr_membership ORDER BY id;
SELECT current_user, pg_has_role(current_user, 'r0c_pgr_membership_g', 'MEMBER'), pg_has_role(current_user, 'r0c_pgr_membership_g', 'USAGE'), pg_has_role(current_user, 'r0c_pgr_membership_g', 'SET');
RESET ROLE;
REVOKE r0c_pgr_membership_g FROM r0c_pgr_membership_u;
SET ROLE r0c_pgr_membership_u;
-- @error
SELECT * FROM r0c_pgr_membership;
RESET ROLE;
DROP TABLE r0c_pgr_membership;
DROP ROLE r0c_pgr_membership_u, r0c_pgr_membership_g;
