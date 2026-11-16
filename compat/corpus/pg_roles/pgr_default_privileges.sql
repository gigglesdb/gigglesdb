-- case: pgr_default_privileges
-- title: Defaults for a creator role apply only to future objects
-- owner: unowned
-- expect: compatible
-- source: Roles: ALTER DEFAULT PRIVILEGES FOR ROLE IN SCHEMA
CREATE ROLE r0c_pgr_default_privileges_owner;
CREATE ROLE r0c_pgr_default_privileges_u;
CREATE SCHEMA r0c_pgr_default_privileges AUTHORIZATION r0c_pgr_default_privileges_owner;
GRANT USAGE ON SCHEMA r0c_pgr_default_privileges TO r0c_pgr_default_privileges_u;
SET ROLE r0c_pgr_default_privileges_owner;
CREATE TABLE r0c_pgr_default_privileges.r0c_pgr_default_privileges_old (id integer PRIMARY KEY);
RESET ROLE;
ALTER DEFAULT PRIVILEGES FOR ROLE r0c_pgr_default_privileges_owner IN SCHEMA r0c_pgr_default_privileges GRANT SELECT ON TABLES TO r0c_pgr_default_privileges_u;
ALTER DEFAULT PRIVILEGES FOR ROLE r0c_pgr_default_privileges_owner IN SCHEMA r0c_pgr_default_privileges GRANT USAGE ON SEQUENCES TO r0c_pgr_default_privileges_u;
SET ROLE r0c_pgr_default_privileges_owner;
CREATE TABLE r0c_pgr_default_privileges.r0c_pgr_default_privileges_new (id integer PRIMARY KEY);
INSERT INTO r0c_pgr_default_privileges.r0c_pgr_default_privileges_new VALUES (5);
CREATE SEQUENCE r0c_pgr_default_privileges.r0c_pgr_default_privileges_s;
RESET ROLE;
SET ROLE r0c_pgr_default_privileges_u;
SELECT * FROM r0c_pgr_default_privileges.r0c_pgr_default_privileges_new ORDER BY id;
SELECT nextval('r0c_pgr_default_privileges.r0c_pgr_default_privileges_s');
-- @error
SELECT * FROM r0c_pgr_default_privileges.r0c_pgr_default_privileges_old;
RESET ROLE;
ALTER DEFAULT PRIVILEGES FOR ROLE r0c_pgr_default_privileges_owner IN SCHEMA r0c_pgr_default_privileges REVOKE SELECT ON TABLES FROM r0c_pgr_default_privileges_u;
ALTER DEFAULT PRIVILEGES FOR ROLE r0c_pgr_default_privileges_owner IN SCHEMA r0c_pgr_default_privileges REVOKE USAGE ON SEQUENCES FROM r0c_pgr_default_privileges_u;
DROP TABLE r0c_pgr_default_privileges.r0c_pgr_default_privileges_new, r0c_pgr_default_privileges.r0c_pgr_default_privileges_old;
DROP SEQUENCE r0c_pgr_default_privileges.r0c_pgr_default_privileges_s;
DROP SCHEMA r0c_pgr_default_privileges;
DROP OWNED BY r0c_pgr_default_privileges_owner, r0c_pgr_default_privileges_u;
DROP ROLE r0c_pgr_default_privileges_owner, r0c_pgr_default_privileges_u;
