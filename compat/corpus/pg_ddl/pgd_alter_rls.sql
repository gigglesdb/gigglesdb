-- case: pgd_alter_rls
-- title: alter rls
-- owner: G
-- expect: compatible
-- source: PG18 ALTER TABLE actions
CREATE TABLE r0c_pgd_alter_rls (id int PRIMARY KEY,v int);
ALTER TABLE r0c_pgd_alter_rls ENABLE ROW LEVEL SECURITY;
ALTER TABLE r0c_pgd_alter_rls FORCE ROW LEVEL SECURITY;
SELECT relrowsecurity,relforcerowsecurity FROM pg_class WHERE oid='r0c_pgd_alter_rls'::regclass;
ALTER TABLE r0c_pgd_alter_rls NO FORCE ROW LEVEL SECURITY;
ALTER TABLE r0c_pgd_alter_rls DISABLE ROW LEVEL SECURITY;
SELECT relrowsecurity,relforcerowsecurity FROM pg_class WHERE oid='r0c_pgd_alter_rls'::regclass;
