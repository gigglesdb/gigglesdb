-- case: pgr_has_role
-- title: Role privilege signatures, graph options, lists and errors
-- owner: unowned
-- expect: compatible
-- source: PostgreSQL 18.6 role privilege oracle
CREATE ROLE r0c_pgr_has_role_a;
CREATE ROLE r0c_pgr_has_role_b;
CREATE ROLE r0c_pgr_has_role_c NOINHERIT;
GRANT r0c_pgr_has_role_a TO r0c_pgr_has_role_b WITH ADMIN OPTION, INHERIT FALSE;
GRANT r0c_pgr_has_role_b TO r0c_pgr_has_role_c;
SELECT pg_has_role('r0c_pgr_has_role_c','r0c_pgr_has_role_a','MEMBER'), pg_has_role('r0c_pgr_has_role_c','r0c_pgr_has_role_a','USAGE'), pg_has_role('r0c_pgr_has_role_c','r0c_pgr_has_role_a','SET'), pg_has_role('r0c_pgr_has_role_c','r0c_pgr_has_role_a','USAGE WITH ADMIN OPTION');
SELECT pg_has_role('r0c_pgr_has_role_a','r0c_pgr_has_role_a','MEMBER WITH ADMIN OPTION'), pg_has_role('r0c_pgr_has_role_b','r0c_pgr_has_role_a','member with grant option, usage');
SELECT pg_has_role(oid,'r0c_pgr_has_role_a','MEMBER'), pg_has_role('r0c_pgr_has_role_c',oid,'MEMBER'), pg_has_role(oid,oid,'USAGE'), pg_has_role(oid,'USAGE') FROM pg_roles WHERE rolname='r0c_pgr_has_role_c';
SELECT pg_has_role(999999::oid,'r0c_pgr_has_role_a','MEMBER'), pg_has_role('r0c_pgr_has_role_a',999999::oid,'USAGE'), pg_has_role(999999::oid,'USAGE');
SELECT pg_has_role(NULL,'r0c_pgr_has_role_a','MEMBER'), pg_has_role('r0c_pgr_has_role_a',NULL::name,'SET'), pg_has_role('r0c_pgr_has_role_a','r0c_pgr_has_role_b',NULL);
-- @error
SELECT pg_has_role('public','r0c_pgr_has_role_a','USAGE');
-- @error
SELECT pg_has_role('CURRENT_USER','r0c_pgr_has_role_a','USAGE');
-- @error
SELECT pg_has_role('r0c_pgr_has_role_a','USAGE,');
-- @error
SELECT pg_has_role('r0c_pgr_has_role_a','');
-- @error
SELECT pg_has_role('r0c_pgr_has_role_a','SELECT');
SELECT pg_has_role('r0c_pgr_has_role_c','pg_read_all_data','USAGE'), pg_has_role('r0c_pgr_has_role_c','pg_database_owner','MEMBER');
GRANT r0c_pgr_has_role_b TO r0c_pgr_has_role_c WITH SET FALSE;
SET ROLE r0c_pgr_has_role_c;
SELECT pg_has_role('r0c_pgr_has_role_a','USAGE'), pg_has_role('r0c_pgr_has_role_a','MEMBER'), pg_has_role('r0c_pgr_has_role_a','SET WITH ADMIN OPTION');
RESET ROLE;
DROP ROLE r0c_pgr_has_role_a, r0c_pgr_has_role_b, r0c_pgr_has_role_c;
