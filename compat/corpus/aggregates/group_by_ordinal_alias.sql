-- case: group_by_ordinal_alias
-- title: Grouping by ordinals and output aliases
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_group_by_ordinal_alias_t (id int PRIMARY KEY, k int, v int);
INSERT INTO r0c_group_by_ordinal_alias_t VALUES (1,1,10),(2,1,20),(3,2,30);
SELECT k + 1 AS bucket, count(*) FROM r0c_group_by_ordinal_alias_t GROUP BY 1 ORDER BY bucket;
SELECT k + 1 AS bucket, sum(v) FROM r0c_group_by_ordinal_alias_t GROUP BY bucket ORDER BY bucket;
