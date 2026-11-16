-- case: global_type_name
-- title: Same type name in two schemas
-- owner: E2
-- expect: compatible
-- source: Differences: Published differences / two types with same name
CREATE SCHEMA r0c_global_type_name_a;
CREATE SCHEMA r0c_global_type_name_b;
CREATE TYPE r0c_global_type_name_a.r0c_global_type_name AS ENUM ('a');
CREATE TYPE r0c_global_type_name_b.r0c_global_type_name AS ENUM ('b');
CREATE TABLE r0c_global_type_name_a.r0c_global_type_name_t (id int PRIMARY KEY, v r0c_global_type_name_a.r0c_global_type_name);
CREATE TABLE r0c_global_type_name_b.r0c_global_type_name_t (id int PRIMARY KEY, v r0c_global_type_name_b.r0c_global_type_name);
INSERT INTO r0c_global_type_name_a.r0c_global_type_name_t VALUES (1,'a');
INSERT INTO r0c_global_type_name_b.r0c_global_type_name_t VALUES (1,'b');
SELECT v FROM r0c_global_type_name_a.r0c_global_type_name_t;
SELECT v FROM r0c_global_type_name_b.r0c_global_type_name_t;
DROP TABLE r0c_global_type_name_a.r0c_global_type_name_t;
DROP TABLE r0c_global_type_name_b.r0c_global_type_name_t;
DROP TYPE r0c_global_type_name_a.r0c_global_type_name;
DROP TYPE r0c_global_type_name_b.r0c_global_type_name;
DROP SCHEMA r0c_global_type_name_a;
DROP SCHEMA r0c_global_type_name_b;
