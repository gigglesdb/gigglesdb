-- case: pgd_view_replace
-- title: view replace
-- owner: E1
-- expect: compatible
-- source: PG18 CREATE OR REPLACE VIEW and column lists
CREATE TABLE r0c_pgd_view_replace (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_view_replace VALUES (1,4);
CREATE VIEW r0c_pgd_view_replace_v (key,value) AS SELECT id,v FROM r0c_pgd_view_replace;
CREATE OR REPLACE VIEW r0c_pgd_view_replace_v (key,value) AS SELECT id,v*3 FROM r0c_pgd_view_replace;
SELECT * FROM r0c_pgd_view_replace_v ORDER BY key;
ALTER VIEW r0c_pgd_view_replace_v RENAME COLUMN value TO amount;
ALTER VIEW r0c_pgd_view_replace_v RENAME TO r0c_pgd_view_replace_renamed;
ALTER VIEW r0c_pgd_view_replace_renamed OWNER TO postgres;
SELECT key,amount FROM r0c_pgd_view_replace_renamed ORDER BY key;
DROP VIEW r0c_pgd_view_replace_renamed;
