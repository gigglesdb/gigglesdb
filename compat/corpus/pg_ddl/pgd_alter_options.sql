-- case: pgd_alter_options
-- title: alter options
-- owner: G
-- expect: compatible
-- source: PG18 ALTER TABLE actions
CREATE TABLE r0c_pgd_alter_options (id int PRIMARY KEY,v int);
ALTER TABLE r0c_pgd_alter_options SET (autovacuum_enabled=false);
SELECT reloptions FROM pg_class WHERE oid='r0c_pgd_alter_options'::regclass;
ALTER TABLE r0c_pgd_alter_options RESET (autovacuum_enabled);
SELECT reloptions IS NULL FROM pg_class WHERE oid='r0c_pgd_alter_options'::regclass;
