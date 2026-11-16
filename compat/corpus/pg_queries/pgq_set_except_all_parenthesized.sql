-- case: pgq_set_except_all_parenthesized
-- title: set except all parenthesized
-- owner: E3
-- expect: compatible
-- source: PostgreSQL 18 SELECT set operations
CREATE TABLE r0c_pgq_set_except_all_parenthesized (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_set_except_all_parenthesized VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
(SELECT amount FROM r0c_pgq_set_except_all_parenthesized ORDER BY amount NULLS LAST LIMIT 2) EXCEPT ALL (SELECT amount FROM r0c_pgq_set_except_all_parenthesized ORDER BY amount DESC NULLS LAST LIMIT 2) ORDER BY amount NULLS LAST;
