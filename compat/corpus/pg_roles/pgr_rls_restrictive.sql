-- case: pgr_rls_restrictive
-- title: Restrictive policies combine with permissive policies and ALTER POLICY
-- owner: unowned
-- expect: compatible
-- source: Roles: RLS PERMISSIVE RESTRICTIVE; W6 ALTER POLICY
CREATE ROLE r0c_pgr_rls_restrictive_u;
CREATE TABLE r0c_pgr_rls_restrictive (id integer PRIMARY KEY, tenant text);
INSERT INTO r0c_pgr_rls_restrictive VALUES (1, 'r0c_pgr_rls_restrictive_u'), (2, 'r0c_pgr_rls_restrictive_u'), (3, 'other');
GRANT SELECT ON r0c_pgr_rls_restrictive TO r0c_pgr_rls_restrictive_u;
ALTER TABLE r0c_pgr_rls_restrictive ENABLE ROW LEVEL SECURITY;
CREATE POLICY r0c_pgr_rls_restrictive_allow ON r0c_pgr_rls_restrictive AS PERMISSIVE FOR SELECT TO r0c_pgr_rls_restrictive_u USING (tenant = current_user);
CREATE POLICY r0c_pgr_rls_restrictive_limit ON r0c_pgr_rls_restrictive AS RESTRICTIVE FOR SELECT TO r0c_pgr_rls_restrictive_u USING (id < 2);
SET ROLE r0c_pgr_rls_restrictive_u;
SELECT id FROM r0c_pgr_rls_restrictive ORDER BY id;
RESET ROLE;
ALTER POLICY r0c_pgr_rls_restrictive_limit ON r0c_pgr_rls_restrictive TO r0c_pgr_rls_restrictive_u USING (id < 3);
SET ROLE r0c_pgr_rls_restrictive_u;
SELECT id FROM r0c_pgr_rls_restrictive ORDER BY id;
RESET ROLE;
DROP POLICY r0c_pgr_rls_restrictive_limit ON r0c_pgr_rls_restrictive;
DROP POLICY r0c_pgr_rls_restrictive_allow ON r0c_pgr_rls_restrictive;
DROP TABLE r0c_pgr_rls_restrictive;
DROP ROLE r0c_pgr_rls_restrictive_u;
