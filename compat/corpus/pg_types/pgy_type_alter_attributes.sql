-- case: pgy_type_alter_attributes
-- title: type alter attributes
-- owner: E2
-- expect: compatible
-- source: W5 CREATE/ALTER/DROP TYPE; types.txt
CREATE TYPE r0c_pgy_type_alter_attributes_c AS (a int);
ALTER TYPE r0c_pgy_type_alter_attributes_c ADD ATTRIBUTE b text CASCADE;
ALTER TYPE r0c_pgy_type_alter_attributes_c ALTER ATTRIBUTE a TYPE bigint CASCADE;
ALTER TYPE r0c_pgy_type_alter_attributes_c RENAME ATTRIBUTE b TO label;
SELECT (ROW(9,'hello')::r0c_pgy_type_alter_attributes_c).label;
ALTER TYPE r0c_pgy_type_alter_attributes_c DROP ATTRIBUTE label RESTRICT;
ALTER TYPE r0c_pgy_type_alter_attributes_c OWNER TO postgres;
ALTER TYPE r0c_pgy_type_alter_attributes_c RENAME TO r0c_pgy_type_alter_attributes_new;
CREATE SCHEMA r0c_pgy_type_alter_attributes_s;
ALTER TYPE r0c_pgy_type_alter_attributes_new SET SCHEMA r0c_pgy_type_alter_attributes_s;
SELECT ROW(9)::r0c_pgy_type_alter_attributes_s.r0c_pgy_type_alter_attributes_new;
DROP TYPE r0c_pgy_type_alter_attributes_s.r0c_pgy_type_alter_attributes_new;
DROP SCHEMA r0c_pgy_type_alter_attributes_s;
