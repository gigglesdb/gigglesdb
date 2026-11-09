-- case: global_type_name
-- title: Type names are globally unique across schemas
-- owner: E2
-- expect: intentional Type names are globally unique across schemas
CREATE SCHEMA r0c_global_type_name_a;
CREATE SCHEMA r0c_global_type_name_b;
CREATE TYPE r0c_global_type_name_a.r0c_global_type_name AS ENUM ('a');
-- expect-sqlstate: 42710
CREATE TYPE r0c_global_type_name_b.r0c_global_type_name AS ENUM ('b');
