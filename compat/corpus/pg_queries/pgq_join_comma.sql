-- case: pgq_join_comma
-- title: join comma
-- owner: E3
-- expect: compatible
-- source: Differences: RIGHT/FULL/CROSS/comma joins; JOIN USING/NATURAL; derived joins; LATERAL; more than 8 tables
CREATE TABLE r0c_pgq_join_comma (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_join_comma VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT a.id,b.id FROM r0c_pgq_join_comma a,r0c_pgq_join_comma b WHERE a.id=b.parent ORDER BY a.id,b.id;
