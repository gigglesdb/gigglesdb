-- case: pgd_partition_attach
-- title: partition attach
-- owner: E1
-- expect: compatible
-- source: PG18 ALTER TABLE ATTACH DETACH PARTITION
CREATE TABLE r0c_pgd_partition_attach (id int PRIMARY KEY,v text) PARTITION BY RANGE(id);
CREATE TABLE r0c_pgd_partition_attach_p (id int PRIMARY KEY,v text);
INSERT INTO r0c_pgd_partition_attach_p VALUES(1,'a');
ALTER TABLE r0c_pgd_partition_attach ATTACH PARTITION r0c_pgd_partition_attach_p FOR VALUES FROM(0) TO(10);
SELECT * FROM r0c_pgd_partition_attach ORDER BY id;
ALTER TABLE r0c_pgd_partition_attach DETACH PARTITION r0c_pgd_partition_attach_p;
SELECT count(*) FROM r0c_pgd_partition_attach;
SELECT * FROM r0c_pgd_partition_attach_p ORDER BY id;
DROP TABLE r0c_pgd_partition_attach_p;
DROP TABLE r0c_pgd_partition_attach;
