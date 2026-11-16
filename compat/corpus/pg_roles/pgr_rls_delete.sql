-- case: pgr_rls_delete
-- title: RLS DELETE policies enforce tenant boundaries
-- owner: unowned
-- expect: compatible
-- source: Roles: row-level security; W6 CREATE DROP POLICY ENABLE ROW LEVEL SECURITY
CREATE ROLE r0c_pgr_rls_delete_u;
CREATE TABLE r0c_pgr_rls_delete (id integer PRIMARY KEY, tenant text);
INSERT INTO r0c_pgr_rls_delete VALUES (1, 'r0c_pgr_rls_delete_u'), (2, 'other');
GRANT SELECT, INSERT, UPDATE, DELETE ON r0c_pgr_rls_delete TO r0c_pgr_rls_delete_u;
ALTER TABLE r0c_pgr_rls_delete ENABLE ROW LEVEL SECURITY;
CREATE POLICY r0c_pgr_rls_delete_read ON r0c_pgr_rls_delete FOR SELECT TO r0c_pgr_rls_delete_u USING (true);
CREATE POLICY r0c_pgr_rls_delete_policy ON r0c_pgr_rls_delete FOR DELETE TO r0c_pgr_rls_delete_u USING (tenant = current_user);
SET ROLE r0c_pgr_rls_delete_u;
DELETE FROM r0c_pgr_rls_delete WHERE id = 2 RETURNING id;
DELETE FROM r0c_pgr_rls_delete WHERE id = 1 RETURNING id;
RESET ROLE;
SELECT id, tenant FROM r0c_pgr_rls_delete ORDER BY id;
DROP POLICY r0c_pgr_rls_delete_policy ON r0c_pgr_rls_delete;
DROP POLICY r0c_pgr_rls_delete_read ON r0c_pgr_rls_delete;
DROP TABLE r0c_pgr_rls_delete;
DROP ROLE r0c_pgr_rls_delete_u;
