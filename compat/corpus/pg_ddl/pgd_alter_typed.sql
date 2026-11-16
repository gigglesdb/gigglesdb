-- case: pgd_alter_typed
-- title: alter typed
-- owner: E1
-- expect: compatible
-- source: PG18 ALTER TABLE OF NOT OF
CREATE TYPE r0c_pgd_alter_typed_type AS (id int,v text);
CREATE TABLE r0c_pgd_alter_typed OF r0c_pgd_alter_typed_type (PRIMARY KEY(id));
INSERT INTO r0c_pgd_alter_typed VALUES(1,'typed');
ALTER TABLE r0c_pgd_alter_typed NOT OF;
ALTER TABLE r0c_pgd_alter_typed OF r0c_pgd_alter_typed_type;
SELECT * FROM r0c_pgd_alter_typed ORDER BY id;
DROP TABLE r0c_pgd_alter_typed;
DROP TYPE r0c_pgd_alter_typed_type;
