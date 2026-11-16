-- case: pgd_wide_row
-- title: wide row
-- owner: E1
-- expect: compatible
-- source: Differences: rows larger than 64 KiB
CREATE TABLE r0c_pgd_wide_row (id int PRIMARY KEY, v text);
INSERT INTO r0c_pgd_wide_row VALUES (1,repeat('abcdefgh',10000));
SELECT id,length(v),substr(v,79993) FROM r0c_pgd_wide_row ORDER BY id;
