-- case: pgd_partition_range
-- title: partition range
-- owner: E1
-- expect: compatible
-- source: Differences: declarative partitioning and pruning results
CREATE TABLE r0c_pgd_partition_range (id int PRIMARY KEY,v text) PARTITION BY RANGE(id);
CREATE TABLE r0c_pgd_partition_range_a PARTITION OF r0c_pgd_partition_range FOR VALUES FROM(0) TO(10) /* PRIMARY KEY inherited from partitioned parent */;
CREATE TABLE r0c_pgd_partition_range_b PARTITION OF r0c_pgd_partition_range FOR VALUES FROM(10) TO(20) /* PRIMARY KEY inherited from partitioned parent */;
CREATE TABLE r0c_pgd_partition_range_d PARTITION OF r0c_pgd_partition_range DEFAULT /* PRIMARY KEY inherited from partitioned parent */;
INSERT INTO r0c_pgd_partition_range VALUES(1,'a'),(2,'b'),(30,'default');
SELECT tableoid::regclass::text,id,v FROM r0c_pgd_partition_range ORDER BY id;
SELECT id,v FROM r0c_pgd_partition_range WHERE id=2 ORDER BY id;
DROP TABLE r0c_pgd_partition_range CASCADE;
