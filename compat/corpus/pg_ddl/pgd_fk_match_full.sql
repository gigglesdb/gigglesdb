-- case: pgd_fk_match_full
-- title: fk match full
-- owner: E1
-- expect: compatible
-- source: Differences: MATCH FULL
CREATE TABLE r0c_pgd_fk_match_full (a int,b int,PRIMARY KEY(a,b));
CREATE TABLE r0c_pgd_fk_match_full_child(id int PRIMARY KEY,a int,b int,FOREIGN KEY(a,b) REFERENCES r0c_pgd_fk_match_full(a,b) MATCH FULL);
INSERT INTO r0c_pgd_fk_match_full VALUES(1,2);
INSERT INTO r0c_pgd_fk_match_full_child VALUES(1,1,2),(2,NULL,NULL);
-- @error
INSERT INTO r0c_pgd_fk_match_full_child VALUES(3,1,NULL);
SELECT * FROM r0c_pgd_fk_match_full_child ORDER BY id;
DROP TABLE r0c_pgd_fk_match_full_child;
