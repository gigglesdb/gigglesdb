-- case: enum_type
-- title: Enum type
-- owner: E2
-- expect: compatible
CREATE TYPE r0c_enum_type_e AS ENUM ('first','second');
CREATE TABLE r0c_enum_type_t (value r0c_enum_type_e);
INSERT INTO r0c_enum_type_t VALUES ('second');
INSERT INTO r0c_enum_type_t VALUES ('first');
SELECT value FROM r0c_enum_type_t ORDER BY value;
