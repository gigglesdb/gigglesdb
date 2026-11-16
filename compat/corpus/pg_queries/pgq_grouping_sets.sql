-- case: pgq_grouping_sets
-- title: grouping sets
-- owner: E3
-- expect: compatible
-- source: Differences: grouping sets; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_grouping_sets (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_grouping_sets VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT dept,parent,GROUPING(dept,parent),sum(amount) FROM r0c_pgq_grouping_sets GROUP BY GROUPING SETS ((dept),(parent),()) ORDER BY GROUPING(dept,parent),dept NULLS LAST,parent NULLS LAST;
