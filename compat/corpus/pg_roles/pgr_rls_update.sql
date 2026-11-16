-- case: pgr_rls_update
-- title: RLS UPDATE policies enforce tenant boundaries
-- owner: unowned
-- expect: compatible
-- source: Roles: row-level security; W6 CREATE DROP POLICY ENABLE ROW LEVEL SECURITY
CREATE ROLE r0c_pgr_rls_update_u;
CREATE TABLE r0c_pgr_rls_update (id integer PRIMARY KEY, tenant text);
INSERT INTO r0c_pgr_rls_update VALUES (1, 'r0c_pgr_rls_update_u'), (2, 'other');
GRANT SELECT, INSERT, UPDATE, DELETE ON r0c_pgr_rls_update TO r0c_pgr_rls_update_u;
ALTER TABLE r0c_pgr_rls_update ENABLE ROW LEVEL SECURITY;
CREATE POLICY r0c_pgr_rls_update_read ON r0c_pgr_rls_update FOR SELECT TO r0c_pgr_rls_update_u USING (true);
CREATE POLICY r0c_pgr_rls_update_policy ON r0c_pgr_rls_update FOR UPDATE TO r0c_pgr_rls_update_u USING (tenant = current_user) WITH CHECK (tenant = current_user);
SET ROLE r0c_pgr_rls_update_u;
UPDATE r0c_pgr_rls_update SET tenant = current_user WHERE id = 1 RETURNING id, tenant;
UPDATE r0c_pgr_rls_update SET tenant = current_user WHERE id = 2;
-- @error
UPDATE r0c_pgr_rls_update SET tenant = 'other' WHERE id = 1;
RESET ROLE;
SELECT id, tenant FROM r0c_pgr_rls_update ORDER BY id;
DROP POLICY r0c_pgr_rls_update_policy ON r0c_pgr_rls_update;
DROP POLICY r0c_pgr_rls_update_read ON r0c_pgr_rls_update;
DROP TABLE r0c_pgr_rls_update;
DROP ROLE r0c_pgr_rls_update_u;
