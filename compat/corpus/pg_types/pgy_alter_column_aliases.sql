-- case: pgy_alter_column_aliases
-- title: alter column aliases
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_alter_column_aliases (id int PRIMARY KEY);
ALTER TABLE r0c_pgy_alter_column_aliases ADD COLUMN a int2;
ALTER TABLE r0c_pgy_alter_column_aliases ADD COLUMN b int4;
ALTER TABLE r0c_pgy_alter_column_aliases ADD COLUMN c int8;
ALTER TABLE r0c_pgy_alter_column_aliases ADD COLUMN d float;
ALTER TABLE r0c_pgy_alter_column_aliases ADD COLUMN e float(24);
INSERT INTO r0c_pgy_alter_column_aliases VALUES (1,2,3,4,5.5,6.5);
SELECT a,b,c,d,e FROM r0c_pgy_alter_column_aliases ORDER BY id;
