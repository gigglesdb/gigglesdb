-- case: pgq_all_array
-- title: all array
-- owner: E3
-- expect: compatible
-- source: Differences: ANY, ALL, SOME
CREATE TABLE r0c_pgq_all_array (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_all_array VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT t.id,t.amount = ALL (ARRAY[10,20,NULL]::int[]) AS matches FROM r0c_pgq_all_array t ORDER BY t.id;
