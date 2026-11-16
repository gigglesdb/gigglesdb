-- case: pgr_rls_select
-- title: RLS SELECT policies enforce tenant boundaries
-- owner: unowned
-- expect: compatible
-- source: Roles: row-level security; W6 CREATE DROP POLICY ENABLE ROW LEVEL SECURITY
CREATE ROLE r0c_pgr_rls_select_u;
CREATE TABLE r0c_pgr_rls_select (id integer PRIMARY KEY, tenant text);
INSERT INTO r0c_pgr_rls_select VALUES (1, 'r0c_pgr_rls_select_u'), (2, 'other');
GRANT SELECT, INSERT, UPDATE, DELETE ON r0c_pgr_rls_select TO r0c_pgr_rls_select_u;
ALTER TABLE r0c_pgr_rls_select ENABLE ROW LEVEL SECURITY;
CREATE POLICY r0c_pgr_rls_select_policy ON r0c_pgr_rls_select FOR SELECT TO r0c_pgr_rls_select_u USING (tenant = current_user);
SET ROLE r0c_pgr_rls_select_u;
SELECT id, tenant FROM r0c_pgr_rls_select ORDER BY id;
RESET ROLE;
SELECT id, tenant FROM r0c_pgr_rls_select ORDER BY id;
DROP POLICY r0c_pgr_rls_select_policy ON r0c_pgr_rls_select;

DROP TABLE r0c_pgr_rls_select;
DROP ROLE r0c_pgr_rls_select_u;
