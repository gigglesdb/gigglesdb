-- case: pgq_join_full
-- title: join full
-- owner: E3
-- expect: compatible
-- source: Differences: RIGHT/FULL/CROSS/comma joins; JOIN USING/NATURAL; derived joins; LATERAL; more than 8 tables
CREATE TABLE r0c_pgq_join_full (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_join_full VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT a.id,b.id FROM r0c_pgq_join_full a FULL JOIN r0c_pgq_join_full b ON a.id=b.parent ORDER BY a.id NULLS LAST,b.id NULLS LAST;
