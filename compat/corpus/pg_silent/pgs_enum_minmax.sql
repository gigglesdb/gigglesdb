-- case: pgs_enum_minmax
-- title: Enum min and max follow declaration order
-- owner: E2
-- expect: compatible
-- source: Differences: Read first / Enum min and max follow declaration order
CREATE TYPE r0c_pgs_enum_minmax AS ENUM ('z','a','m');
CREATE TABLE r0c_pgs_enum_minmax_t (id int PRIMARY KEY, v r0c_pgs_enum_minmax);
INSERT INTO r0c_pgs_enum_minmax_t VALUES (1,'z'),(2,'a'),(3,'m');
SELECT min(v), max(v) FROM r0c_pgs_enum_minmax_t;
DROP TABLE r0c_pgs_enum_minmax_t;
DROP TYPE r0c_pgs_enum_minmax;
