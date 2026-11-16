-- case: pgr_attributes
-- title: Role attributes and USER and GROUP aliases
-- owner: unowned
-- expect: compatible
-- source: Roles: membership and attributes; W6 CREATE ALTER DROP ROLE USER GROUP
CREATE ROLE r0c_pgr_attributes_u LOGIN CREATEDB CREATEROLE NOINHERIT CONNECTION LIMIT 3 VALID UNTIL '2099-01-01 00:00:00+00';
SELECT rolname, rolcanlogin, rolcreatedb, rolcreaterole, rolinherit, rolconnlimit, rolvaliduntil = '2099-01-01 00:00:00+00'::timestamptz AS expiry FROM pg_roles WHERE rolname = 'r0c_pgr_attributes_u';
ALTER ROLE r0c_pgr_attributes_u NOLOGIN NOCREATEDB NOCREATEROLE INHERIT CONNECTION LIMIT -1 VALID UNTIL 'infinity';
SELECT rolcanlogin, rolcreatedb, rolcreaterole, rolinherit, rolconnlimit, rolvaliduntil = 'infinity'::timestamptz AS expiry FROM pg_roles WHERE rolname = 'r0c_pgr_attributes_u';
CREATE USER r0c_pgr_attributes_alias NOLOGIN;
ALTER USER r0c_pgr_attributes_alias CONNECTION LIMIT 2;
CREATE GROUP r0c_pgr_attributes_g;
ALTER GROUP r0c_pgr_attributes_g ADD USER r0c_pgr_attributes_alias;
SELECT pg_has_role('r0c_pgr_attributes_alias', 'r0c_pgr_attributes_g', 'MEMBER');
ALTER GROUP r0c_pgr_attributes_g DROP USER r0c_pgr_attributes_alias;
DROP GROUP r0c_pgr_attributes_g;
DROP USER r0c_pgr_attributes_alias;
DROP ROLE r0c_pgr_attributes_u;
