-- case: pgd_schema
-- title: schema
-- owner: E1
-- expect: compatible
-- source: Differences: ALTER SCHEMA
CREATE SCHEMA r0c_pgd_schema;
ALTER SCHEMA r0c_pgd_schema RENAME TO r0c_pgd_schema_new;
ALTER SCHEMA r0c_pgd_schema_new OWNER TO postgres;
CREATE TABLE r0c_pgd_schema_new.r0c_pgd_schema (id int PRIMARY KEY);
INSERT INTO r0c_pgd_schema_new.r0c_pgd_schema VALUES(1);
SELECT * FROM r0c_pgd_schema_new.r0c_pgd_schema ORDER BY id;
DROP TABLE r0c_pgd_schema_new.r0c_pgd_schema;
DROP SCHEMA r0c_pgd_schema_new;
