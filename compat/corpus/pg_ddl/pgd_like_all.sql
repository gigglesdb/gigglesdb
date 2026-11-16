-- case: pgd_like_all
-- title: like all
-- owner: E1
-- expect: compatible
-- source: Differences: CREATE TABLE LIKE
CREATE TABLE r0c_pgd_like_all (id int PRIMARY KEY, v int DEFAULT 7, CHECK(v>0));
CREATE TABLE r0c_pgd_like_all_copy (LIKE r0c_pgd_like_all INCLUDING ALL) /* PRIMARY KEY copied by INCLUDING ALL */;
INSERT INTO r0c_pgd_like_all_copy(id) VALUES (1);
SELECT * FROM r0c_pgd_like_all_copy ORDER BY id;
-- @error
INSERT INTO r0c_pgd_like_all_copy VALUES (2,-1);
DROP TABLE r0c_pgd_like_all_copy;
