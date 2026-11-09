-- case: global_relation_name
-- title: Relation names are globally unique across schemas
-- owner: E2
-- expect: intentional Relation names are globally unique across schemas
CREATE SCHEMA r0c_global_relation_name_a;
CREATE SCHEMA r0c_global_relation_name_b;
CREATE TABLE r0c_global_relation_name_a.r0c_global_relation_name (id int PRIMARY KEY);
-- expect-sqlstate: 42P07
CREATE TABLE r0c_global_relation_name_b.r0c_global_relation_name (id int PRIMARY KEY);
