-- case: pgq_explain_analyze_update
-- title: explain analyze update
-- owner: E1
-- expect: compatible
-- source: Differences: EXPLAIN ANALYZE of data changes
CREATE TABLE r0c_pgq_explain_analyze_update (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_explain_analyze_update VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) UPDATE r0c_pgq_explain_analyze_update SET amount=11 WHERE id=1;
