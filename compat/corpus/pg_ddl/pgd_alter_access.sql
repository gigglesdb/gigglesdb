-- case: pgd_alter_access
-- title: alter access
-- owner: E1
-- expect: compatible
-- source: PG18 ALTER TABLE access method tablespace and logged
CREATE TABLE r0c_pgd_alter_access (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_alter_access VALUES(1,2);
ALTER TABLE r0c_pgd_alter_access SET ACCESS METHOD heap;
ALTER TABLE r0c_pgd_alter_access SET TABLESPACE pg_default;
SELECT * FROM r0c_pgd_alter_access ORDER BY id;
