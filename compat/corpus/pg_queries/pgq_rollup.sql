-- case: pgq_rollup
-- title: rollup
-- owner: E3
-- expect: compatible
-- source: Differences: rollup; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_rollup (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_rollup VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT dept,parent,GROUPING(dept,parent),count(*),sum(amount) FROM r0c_pgq_rollup GROUP BY ROLLUP (dept,parent) ORDER BY GROUPING(dept,parent),dept NULLS LAST,parent NULLS LAST;
