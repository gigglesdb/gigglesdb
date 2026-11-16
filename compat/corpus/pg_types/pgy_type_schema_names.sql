-- case: pgy_type_schema_names
-- title: type schema names
-- owner: E2
-- expect: compatible
-- source: W5 CREATE/ALTER/DROP TYPE; types.txt
CREATE SCHEMA r0c_pgy_type_schema_names_a;
CREATE SCHEMA r0c_pgy_type_schema_names_b;
CREATE TYPE r0c_pgy_type_schema_names_a.r0c_pgy_type_schema_names_e AS ENUM ('a');
CREATE TYPE r0c_pgy_type_schema_names_b.r0c_pgy_type_schema_names_e AS ENUM ('b');
SELECT 'a'::r0c_pgy_type_schema_names_a.r0c_pgy_type_schema_names_e,'b'::r0c_pgy_type_schema_names_b.r0c_pgy_type_schema_names_e;
DROP TYPE r0c_pgy_type_schema_names_a.r0c_pgy_type_schema_names_e;
DROP TYPE r0c_pgy_type_schema_names_b.r0c_pgy_type_schema_names_e;
DROP SCHEMA r0c_pgy_type_schema_names_a;
DROP SCHEMA r0c_pgy_type_schema_names_b;
