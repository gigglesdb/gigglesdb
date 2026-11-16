-- case: pgd_alter_replica
-- title: alter replica
-- owner: G
-- expect: compatible
-- source: PG18 ALTER TABLE replica identity and triggers
CREATE TABLE r0c_pgd_alter_replica (id int PRIMARY KEY, v int);
ALTER TABLE r0c_pgd_alter_replica REPLICA IDENTITY FULL;
ALTER TABLE r0c_pgd_alter_replica DISABLE TRIGGER ALL;
ALTER TABLE r0c_pgd_alter_replica ENABLE TRIGGER ALL;
CREATE RULE r0c_pgd_alter_replica_r AS ON INSERT TO r0c_pgd_alter_replica DO ALSO NOTHING;
ALTER TABLE r0c_pgd_alter_replica DISABLE RULE r0c_pgd_alter_replica_r;
ALTER TABLE r0c_pgd_alter_replica ENABLE ALWAYS RULE r0c_pgd_alter_replica_r;
ALTER TABLE r0c_pgd_alter_replica ENABLE REPLICA RULE r0c_pgd_alter_replica_r;
ALTER TABLE r0c_pgd_alter_replica ENABLE RULE r0c_pgd_alter_replica_r;
DROP RULE r0c_pgd_alter_replica_r ON r0c_pgd_alter_replica;
SELECT relreplident FROM pg_class WHERE oid='r0c_pgd_alter_replica'::regclass;
ALTER TABLE r0c_pgd_alter_replica REPLICA IDENTITY DEFAULT;
