-- case: pgq_lateral_subquery
-- title: lateral subquery
-- owner: E3
-- expect: compatible
-- source: Differences: RIGHT/FULL/CROSS/comma joins; JOIN USING/NATURAL; derived joins; LATERAL; more than 8 tables
CREATE TABLE r0c_pgq_lateral_subquery (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_lateral_subquery VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT t.id,s.id FROM r0c_pgq_lateral_subquery t LEFT JOIN LATERAL (SELECT c.id FROM r0c_pgq_lateral_subquery c WHERE c.parent=t.id ORDER BY c.id LIMIT 1) s ON true ORDER BY t.id;
