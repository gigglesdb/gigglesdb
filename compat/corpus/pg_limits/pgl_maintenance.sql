-- case: pgl_maintenance
-- title: Maintenance commands accepted for ordinary application tables
-- owner: E1
-- expect: compatible
-- source: Maintenance: VACUUM ANALYZE CHECKPOINT
CREATE TABLE r0c_pgl_maintenance (id integer PRIMARY KEY, value text);
INSERT INTO r0c_pgl_maintenance VALUES (1, 'before');
UPDATE r0c_pgl_maintenance SET value = 'after' WHERE id = 1;
VACUUM r0c_pgl_maintenance;
VACUUM ANALYZE r0c_pgl_maintenance;
ANALYZE r0c_pgl_maintenance;
CHECKPOINT;
SELECT * FROM r0c_pgl_maintenance ORDER BY id;
DROP TABLE r0c_pgl_maintenance;
