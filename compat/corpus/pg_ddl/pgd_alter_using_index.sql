-- case: pgd_alter_using_index
-- title: alter using index
-- owner: E1
-- expect: compatible
-- source: PG18 ALTER TABLE ADD CONSTRAINT USING INDEX
CREATE TABLE r0c_pgd_alter_using_index (id int PRIMARY KEY,v int);
CREATE UNIQUE INDEX r0c_pgd_alter_using_index_u ON r0c_pgd_alter_using_index(v);
ALTER TABLE r0c_pgd_alter_using_index ADD CONSTRAINT r0c_pgd_alter_using_index_uq UNIQUE USING INDEX r0c_pgd_alter_using_index_u;
INSERT INTO r0c_pgd_alter_using_index VALUES(1,5);
-- @error
INSERT INTO r0c_pgd_alter_using_index VALUES(2,5);
ALTER TABLE r0c_pgd_alter_using_index DROP CONSTRAINT r0c_pgd_alter_using_index_uq;
SELECT * FROM r0c_pgd_alter_using_index ORDER BY id;
