-- case: pgd_partition_hash
-- title: partition hash
-- owner: E1
-- expect: compatible
-- source: Differences: declarative partitioning and pruning results
CREATE TABLE r0c_pgd_partition_hash (id int PRIMARY KEY,v text) PARTITION BY HASH(id);
CREATE TABLE r0c_pgd_partition_hash_a PARTITION OF r0c_pgd_partition_hash FOR VALUES WITH(MODULUS 2,REMAINDER 0) /* PRIMARY KEY inherited from partitioned parent */;
CREATE TABLE r0c_pgd_partition_hash_b PARTITION OF r0c_pgd_partition_hash FOR VALUES WITH(MODULUS 2,REMAINDER 1) /* PRIMARY KEY inherited from partitioned parent */;
INSERT INTO r0c_pgd_partition_hash VALUES(1,'a'),(2,'b'),(30,'default');
SELECT tableoid::regclass::text,id,v FROM r0c_pgd_partition_hash ORDER BY id;
SELECT id,v FROM r0c_pgd_partition_hash WHERE id=2 ORDER BY id;
DROP TABLE r0c_pgd_partition_hash CASCADE;
