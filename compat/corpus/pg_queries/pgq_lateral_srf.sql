-- case: pgq_lateral_srf
-- title: lateral srf
-- owner: E3
-- expect: compatible
-- source: Differences: RIGHT/FULL/CROSS/comma joins; JOIN USING/NATURAL; derived joins; LATERAL; more than 8 tables
CREATE TABLE r0c_pgq_lateral_srf (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_lateral_srf VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT t.id,s.n FROM r0c_pgq_lateral_srf t CROSS JOIN LATERAL generate_series(1,t.id) AS s(n) ORDER BY t.id,s.n;
