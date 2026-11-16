-- case: pgd_alter_owner_schema
-- title: alter owner schema
-- owner: E1
-- expect: compatible
-- source: Differences: OWNER TO and SET SCHEMA
CREATE TABLE r0c_pgd_alter_owner_schema (id int PRIMARY KEY, v int);
CREATE SCHEMA r0c_pgd_alter_owner_schema_s;
ALTER TABLE r0c_pgd_alter_owner_schema OWNER TO postgres;
ALTER TABLE r0c_pgd_alter_owner_schema SET SCHEMA r0c_pgd_alter_owner_schema_s;
INSERT INTO r0c_pgd_alter_owner_schema_s.r0c_pgd_alter_owner_schema VALUES(1,5);
SELECT * FROM r0c_pgd_alter_owner_schema_s.r0c_pgd_alter_owner_schema ORDER BY id;
DROP TABLE r0c_pgd_alter_owner_schema_s.r0c_pgd_alter_owner_schema;
DROP SCHEMA r0c_pgd_alter_owner_schema_s;
