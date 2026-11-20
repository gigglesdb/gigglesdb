-- case: pgr_grant_atomic
-- title: Multi-target role membership statements are atomic
-- owner: unowned
-- expect: compatible
-- source: PostgreSQL 18.6 GrantRole oracle
CREATE ROLE r0c_pgr_grant_atomic_a;
CREATE ROLE r0c_pgr_grant_atomic_b;
CREATE ROLE r0c_pgr_grant_atomic_x;
CREATE ROLE r0c_pgr_grant_atomic_y;
-- @error
GRANT r0c_pgr_grant_atomic_a, r0c_pgr_grant_atomic_b TO r0c_pgr_grant_atomic_x, r0c_pgr_grant_atomic_a;
SELECT pg_has_role('r0c_pgr_grant_atomic_x','r0c_pgr_grant_atomic_a','MEMBER');
GRANT r0c_pgr_grant_atomic_a, r0c_pgr_grant_atomic_b TO r0c_pgr_grant_atomic_x WITH ADMIN OPTION;
-- @error
REVOKE r0c_pgr_grant_atomic_a, r0c_pgr_grant_atomic_missing FROM r0c_pgr_grant_atomic_x;
-- @error
REVOKE ADMIN OPTION FOR r0c_pgr_grant_atomic_a, r0c_pgr_grant_atomic_b FROM r0c_pgr_grant_atomic_x, r0c_pgr_grant_atomic_missing;
SELECT pg_has_role('r0c_pgr_grant_atomic_x','r0c_pgr_grant_atomic_a','MEMBER WITH ADMIN OPTION'), pg_has_role('r0c_pgr_grant_atomic_x','r0c_pgr_grant_atomic_b','MEMBER WITH ADMIN OPTION');
GRANT r0c_pgr_grant_atomic_a TO r0c_pgr_grant_atomic_x, r0c_pgr_grant_atomic_x;
REVOKE r0c_pgr_grant_atomic_a, r0c_pgr_grant_atomic_b FROM r0c_pgr_grant_atomic_y;
DROP ROLE r0c_pgr_grant_atomic_a, r0c_pgr_grant_atomic_b, r0c_pgr_grant_atomic_x, r0c_pgr_grant_atomic_y;
