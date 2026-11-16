-- case: pgq_join_derived_alias
-- title: join derived alias
-- owner: E3
-- expect: compatible
-- source: Differences: RIGHT/FULL/CROSS/comma joins; JOIN USING/NATURAL; derived joins; LATERAL; more than 8 tables
CREATE TABLE r0c_pgq_join_derived_alias (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_join_derived_alias VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT d.child,d.name FROM (SELECT c.id,p.label FROM r0c_pgq_join_derived_alias c JOIN r0c_pgq_join_derived_alias p ON c.parent=p.id) AS d(child,name) ORDER BY d.child;
