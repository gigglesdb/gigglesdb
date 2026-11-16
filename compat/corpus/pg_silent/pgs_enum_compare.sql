-- case: pgs_enum_compare
-- title: Enum comparison follows declaration order
-- owner: E2
-- expect: compatible
-- source: Differences: Read first / Enum comparison follows declaration order
CREATE TYPE r0c_pgs_enum_compare AS ENUM ('z','a','m');
CREATE TABLE r0c_pgs_enum_compare_t (id int PRIMARY KEY, v r0c_pgs_enum_compare);
INSERT INTO r0c_pgs_enum_compare_t VALUES (1,'z'),(2,'a'),(3,'m');
SELECT id, v < 'a', v > 'a', v <= 'a', v >= 'a' FROM r0c_pgs_enum_compare_t ORDER BY id;
DROP TABLE r0c_pgs_enum_compare_t;
DROP TYPE r0c_pgs_enum_compare;
