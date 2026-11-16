-- case: pgl_twelve_indexes
-- title: Twelve secondary indexes on one table
-- owner: E1
-- expect: compatible
-- source: Indexes: more than 8 secondary indexes
CREATE TABLE r0c_pgl_twelve_indexes (id integer PRIMARY KEY, value integer);
CREATE INDEX r0c_pgl_twelve_indexes_i1 ON r0c_pgl_twelve_indexes (value);
CREATE INDEX r0c_pgl_twelve_indexes_i2 ON r0c_pgl_twelve_indexes (value);
CREATE INDEX r0c_pgl_twelve_indexes_i3 ON r0c_pgl_twelve_indexes (value);
CREATE INDEX r0c_pgl_twelve_indexes_i4 ON r0c_pgl_twelve_indexes (value);
CREATE INDEX r0c_pgl_twelve_indexes_i5 ON r0c_pgl_twelve_indexes (value);
CREATE INDEX r0c_pgl_twelve_indexes_i6 ON r0c_pgl_twelve_indexes (value);
CREATE INDEX r0c_pgl_twelve_indexes_i7 ON r0c_pgl_twelve_indexes (value);
CREATE INDEX r0c_pgl_twelve_indexes_i8 ON r0c_pgl_twelve_indexes (value);
CREATE INDEX r0c_pgl_twelve_indexes_i9 ON r0c_pgl_twelve_indexes (value);
CREATE INDEX r0c_pgl_twelve_indexes_i10 ON r0c_pgl_twelve_indexes (value);
CREATE INDEX r0c_pgl_twelve_indexes_i11 ON r0c_pgl_twelve_indexes (value);
CREATE INDEX r0c_pgl_twelve_indexes_i12 ON r0c_pgl_twelve_indexes (value);
INSERT INTO r0c_pgl_twelve_indexes VALUES (1, 42);
SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'r0c_pgl_twelve_indexes';
SELECT id FROM r0c_pgl_twelve_indexes WHERE value = 42 ORDER BY id;
DROP TABLE r0c_pgl_twelve_indexes;
