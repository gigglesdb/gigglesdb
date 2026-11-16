-- case: pgf_enum
-- title: Enum declaration order helpers and extrema
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-enum.html
CREATE TYPE r0c_pgf_enum_e AS ENUM ('zebra','apple','middle');
CREATE TABLE r0c_pgf_enum (id integer PRIMARY KEY, e r0c_pgf_enum_e);
INSERT INTO r0c_pgf_enum VALUES (1,'zebra'),(2,'apple'),(3,'middle'),(4,NULL);
SELECT id, e, enum_first(e) FROM r0c_pgf_enum ORDER BY id;
SELECT id, e, enum_last(e) FROM r0c_pgf_enum ORDER BY id;
SELECT id, e, enum_range(e) FROM r0c_pgf_enum ORDER BY id;
SELECT id, e, e<'middle'::r0c_pgf_enum_e FROM r0c_pgf_enum ORDER BY id;
SELECT id, e, e>'zebra'::r0c_pgf_enum_e FROM r0c_pgf_enum ORDER BY id;
SELECT min(e) FROM r0c_pgf_enum;
SELECT max(e) FROM r0c_pgf_enum;
SELECT enum_range('apple'::r0c_pgf_enum_e,'middle'::r0c_pgf_enum_e) FROM r0c_pgf_enum;
SELECT enum_range(NULL::r0c_pgf_enum_e,'apple'::r0c_pgf_enum_e) FROM r0c_pgf_enum;
DROP TABLE r0c_pgf_enum;
DROP TYPE r0c_pgf_enum_e;
