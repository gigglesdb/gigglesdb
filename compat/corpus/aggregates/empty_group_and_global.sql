-- case: empty_group_and_global
-- title: Empty grouped and global aggregates
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_empty_group_and_global_t (id int PRIMARY KEY, k int, v int);
SELECT k, count(*), sum(v), min(v), max(v) FROM r0c_empty_group_and_global_t GROUP BY k ORDER BY k;
SELECT count(*), count(v), sum(v), min(v), max(v) FROM r0c_empty_group_and_global_t;
SELECT count(*) FROM r0c_empty_group_and_global_t HAVING count(*) = 0;
SELECT count(*) FROM r0c_empty_group_and_global_t HAVING count(*) > 0;
