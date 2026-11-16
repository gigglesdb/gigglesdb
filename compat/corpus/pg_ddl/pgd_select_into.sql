-- case: pgd_select_into
-- title: select into
-- owner: E1
-- expect: compatible
-- source: Differences: CREATE TABLE AS and SELECT INTO
CREATE TABLE r0c_pgd_select_into (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_select_into VALUES (1,8);
SELECT * INTO r0c_pgd_select_into_copy FROM r0c_pgd_select_into;
ALTER TABLE r0c_pgd_select_into_copy ADD PRIMARY KEY(id);
SELECT * FROM r0c_pgd_select_into_copy ORDER BY id;
DROP TABLE r0c_pgd_select_into_copy;
