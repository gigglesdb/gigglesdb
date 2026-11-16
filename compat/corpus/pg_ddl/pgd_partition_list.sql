-- case: pgd_partition_list
-- title: partition list
-- owner: E1
-- expect: compatible
-- source: Differences: declarative partitioning and pruning results
CREATE TABLE r0c_pgd_partition_list (id int PRIMARY KEY,v text) PARTITION BY LIST(id);
CREATE TABLE r0c_pgd_partition_list_a PARTITION OF r0c_pgd_partition_list FOR VALUES IN(1,3) /* PRIMARY KEY inherited from partitioned parent */;
CREATE TABLE r0c_pgd_partition_list_b PARTITION OF r0c_pgd_partition_list FOR VALUES IN(2,4) /* PRIMARY KEY inherited from partitioned parent */;
CREATE TABLE r0c_pgd_partition_list_d PARTITION OF r0c_pgd_partition_list DEFAULT /* PRIMARY KEY inherited from partitioned parent */;
INSERT INTO r0c_pgd_partition_list VALUES(1,'a'),(2,'b'),(30,'default');
SELECT tableoid::regclass::text,id,v FROM r0c_pgd_partition_list ORDER BY id;
SELECT id,v FROM r0c_pgd_partition_list WHERE id=2 ORDER BY id;
DROP TABLE r0c_pgd_partition_list CASCADE;
