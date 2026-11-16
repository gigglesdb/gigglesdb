-- case: pgr_rls_force
-- title: FORCE applies to owner while BYPASSRLS bypasses policies
-- owner: unowned
-- expect: compatible
-- source: Roles: RLS FORCE ENABLE BYPASSRLS
CREATE ROLE r0c_pgr_rls_force_u;
CREATE ROLE r0c_pgr_rls_force_bypass BYPASSRLS;
CREATE TABLE r0c_pgr_rls_force (id integer PRIMARY KEY);
INSERT INTO r0c_pgr_rls_force VALUES (1), (2);
ALTER TABLE r0c_pgr_rls_force OWNER TO r0c_pgr_rls_force_u;
GRANT SELECT ON r0c_pgr_rls_force TO r0c_pgr_rls_force_bypass;
ALTER TABLE r0c_pgr_rls_force ENABLE ROW LEVEL SECURITY;
SET ROLE r0c_pgr_rls_force_u;
SELECT id FROM r0c_pgr_rls_force ORDER BY id;
RESET ROLE;
ALTER TABLE r0c_pgr_rls_force FORCE ROW LEVEL SECURITY;
SET ROLE r0c_pgr_rls_force_u;
SELECT id FROM r0c_pgr_rls_force ORDER BY id;
RESET ROLE;
SET ROLE r0c_pgr_rls_force_bypass;
SELECT id FROM r0c_pgr_rls_force ORDER BY id;
RESET ROLE;
ALTER ROLE r0c_pgr_rls_force_bypass NOBYPASSRLS;
SET ROLE r0c_pgr_rls_force_bypass;
SELECT id FROM r0c_pgr_rls_force ORDER BY id;
RESET ROLE;
ALTER TABLE r0c_pgr_rls_force NO FORCE ROW LEVEL SECURITY;
ALTER TABLE r0c_pgr_rls_force DISABLE ROW LEVEL SECURITY;
DROP TABLE r0c_pgr_rls_force;
DROP ROLE r0c_pgr_rls_force_u, r0c_pgr_rls_force_bypass;
