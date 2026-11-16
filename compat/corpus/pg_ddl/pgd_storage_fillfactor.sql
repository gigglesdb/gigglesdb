-- case: pgd_storage_fillfactor
-- title: storage fillfactor
-- owner: G
-- expect: compatible
-- source: Differences: storage parameters
CREATE TABLE r0c_pgd_storage_fillfactor (id int PRIMARY KEY,v text) WITH(fillfactor=70);
ALTER TABLE r0c_pgd_storage_fillfactor SET(fillfactor=80);
SELECT reloptions FROM pg_class WHERE oid='r0c_pgd_storage_fillfactor'::regclass;
ALTER TABLE r0c_pgd_storage_fillfactor RESET(fillfactor);
SELECT reloptions IS NULL FROM pg_class WHERE oid='r0c_pgd_storage_fillfactor'::regclass;
