-- case: pgq_join_nine
-- title: join nine
-- owner: E3
-- expect: compatible
-- source: Differences: RIGHT/FULL/CROSS/comma joins; JOIN USING/NATURAL; derived joins; LATERAL; more than 8 tables
CREATE TABLE r0c_pgq_join_nine (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_join_nine VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT a.id FROM r0c_pgq_join_nine a JOIN r0c_pgq_join_nine b ON b.id=a.id JOIN r0c_pgq_join_nine c ON c.id=b.id JOIN r0c_pgq_join_nine d ON d.id=c.id JOIN r0c_pgq_join_nine e ON e.id=d.id JOIN r0c_pgq_join_nine f ON f.id=e.id JOIN r0c_pgq_join_nine g ON g.id=f.id JOIN r0c_pgq_join_nine h ON h.id=g.id JOIN r0c_pgq_join_nine i ON i.id=h.id ORDER BY a.id;
