-- case: pgd_inheritance
-- title: inheritance
-- owner: E1
-- expect: compatible
-- source: Differences: inheritance
CREATE TABLE r0c_pgd_inheritance (id int PRIMARY KEY, v text);
CREATE TABLE r0c_pgd_inheritance_child (PRIMARY KEY(id),extra int) INHERITS(r0c_pgd_inheritance);
INSERT INTO r0c_pgd_inheritance VALUES(1,'parent');
INSERT INTO r0c_pgd_inheritance_child VALUES(2,'child',8);
SELECT id,v FROM r0c_pgd_inheritance ORDER BY id;
SELECT id,v FROM ONLY r0c_pgd_inheritance ORDER BY id;
ALTER TABLE r0c_pgd_inheritance_child NO INHERIT r0c_pgd_inheritance;
SELECT count(*) FROM r0c_pgd_inheritance;
ALTER TABLE r0c_pgd_inheritance_child INHERIT r0c_pgd_inheritance;
DROP TABLE r0c_pgd_inheritance_child;
