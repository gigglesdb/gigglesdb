-- case: pgl_drop_recreate
-- title: Dropped tables and indexes can be recreated and reused
-- owner: E1
-- expect: compatible
-- source: Maintenance: space from dropped tables and indexes; physical reclamation timing not compared
CREATE TABLE r0c_pgl_drop_recreate (id integer PRIMARY KEY, payload text);
CREATE INDEX r0c_pgl_drop_recreate_idx ON r0c_pgl_drop_recreate (payload);
INSERT INTO r0c_pgl_drop_recreate SELECT g, repeat('x', 1000) FROM generate_series(1, 1000) AS g;
DROP INDEX r0c_pgl_drop_recreate_idx;
DROP TABLE r0c_pgl_drop_recreate;
CREATE TABLE r0c_pgl_drop_recreate (id integer PRIMARY KEY, payload text);
CREATE INDEX r0c_pgl_drop_recreate_idx ON r0c_pgl_drop_recreate (payload);
INSERT INTO r0c_pgl_drop_recreate VALUES (1, 'reused');
SELECT * FROM r0c_pgl_drop_recreate ORDER BY id;
DROP TABLE r0c_pgl_drop_recreate;
