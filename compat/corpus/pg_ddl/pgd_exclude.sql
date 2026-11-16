-- case: pgd_exclude
-- title: exclude
-- owner: E1
-- expect: compatible
-- source: Differences: EXCLUDE constraints
CREATE TABLE r0c_pgd_exclude (id int PRIMARY KEY, v int4range);
ALTER TABLE r0c_pgd_exclude ADD CONSTRAINT r0c_pgd_exclude_ex EXCLUDE USING gist (v WITH &&);
INSERT INTO r0c_pgd_exclude VALUES (1,'[1,5)'),(2,'[5,8)');
-- @error
INSERT INTO r0c_pgd_exclude VALUES (3,'[4,6)');
SELECT * FROM r0c_pgd_exclude ORDER BY id;
