-- case: enum_type
-- title: Enum type
-- owner: E2
-- expect: compatible
CREATE TYPE r0c_enum_type_e AS ENUM ('first','second');
CREATE TABLE r0c_enum_type_t (id int PRIMARY KEY, value r0c_enum_type_e);
INSERT INTO r0c_enum_type_t VALUES (1, 'second');
INSERT INTO r0c_enum_type_t VALUES (2, 'first');
SELECT value FROM r0c_enum_type_t ORDER BY value;
