-- case: pgr_rls_insert
-- title: RLS INSERT policies enforce tenant boundaries
-- owner: unowned
-- expect: compatible
-- source: Roles: row-level security; W6 CREATE DROP POLICY ENABLE ROW LEVEL SECURITY
CREATE ROLE r0c_pgr_rls_insert_u;
CREATE TABLE r0c_pgr_rls_insert (id integer PRIMARY KEY, tenant text);
INSERT INTO r0c_pgr_rls_insert VALUES (1, 'r0c_pgr_rls_insert_u'), (2, 'other');
GRANT SELECT, INSERT, UPDATE, DELETE ON r0c_pgr_rls_insert TO r0c_pgr_rls_insert_u;
ALTER TABLE r0c_pgr_rls_insert ENABLE ROW LEVEL SECURITY;
CREATE POLICY r0c_pgr_rls_insert_policy ON r0c_pgr_rls_insert FOR INSERT TO r0c_pgr_rls_insert_u WITH CHECK (tenant = current_user);
SET ROLE r0c_pgr_rls_insert_u;
INSERT INTO r0c_pgr_rls_insert VALUES (3, current_user);
-- @error
INSERT INTO r0c_pgr_rls_insert VALUES (4, 'other');
RESET ROLE;
SELECT id, tenant FROM r0c_pgr_rls_insert ORDER BY id;
DROP POLICY r0c_pgr_rls_insert_policy ON r0c_pgr_rls_insert;

DROP TABLE r0c_pgr_rls_insert;
DROP ROLE r0c_pgr_rls_insert_u;
