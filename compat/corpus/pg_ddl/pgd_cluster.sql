-- case: pgd_cluster
-- title: cluster
-- owner: E1
-- expect: compatible
-- source: W4 CLUSTER and ALTER TABLE CLUSTER ON
CREATE TABLE r0c_pgd_cluster (id int PRIMARY KEY, v int);
CREATE INDEX r0c_pgd_cluster_i ON r0c_pgd_cluster(v);
INSERT INTO r0c_pgd_cluster VALUES(1,20),(2,10);
ALTER TABLE r0c_pgd_cluster CLUSTER ON r0c_pgd_cluster_i;
CLUSTER r0c_pgd_cluster;
CLUSTER r0c_pgd_cluster USING r0c_pgd_cluster_i;
SELECT * FROM r0c_pgd_cluster ORDER BY v,id;
ALTER TABLE r0c_pgd_cluster SET WITHOUT CLUSTER;
DROP INDEX r0c_pgd_cluster_i;
