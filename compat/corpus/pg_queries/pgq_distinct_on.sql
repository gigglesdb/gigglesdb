-- case: pgq_distinct_on
-- title: distinct on
-- owner: E3
-- expect: compatible
-- source: Differences: distinct on; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_distinct_on (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_distinct_on VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT DISTINCT ON (dept) dept,id,amount FROM r0c_pgq_distinct_on ORDER BY dept,amount DESC NULLS LAST,id;
