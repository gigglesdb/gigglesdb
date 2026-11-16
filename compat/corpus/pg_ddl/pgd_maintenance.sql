-- case: pgd_maintenance
-- title: maintenance
-- owner: E1
-- expect: compatible
-- source: W4 VACUUM ANALYZE CHECKPOINT
CREATE TABLE r0c_pgd_maintenance (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_maintenance VALUES(1,10),(2,20);
DELETE FROM r0c_pgd_maintenance WHERE id=2;
VACUUM (ANALYZE) r0c_pgd_maintenance;
ANALYZE r0c_pgd_maintenance (v);
CHECKPOINT;
SELECT * FROM r0c_pgd_maintenance ORDER BY id;
