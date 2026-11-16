-- case: pgq_truncate_only
-- title: truncate only
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 TRUNCATE ONLY / CONTINUE IDENTITY / RESTRICT
CREATE TABLE r0c_pgq_truncate_only (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_truncate_only VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
TRUNCATE ONLY r0c_pgq_truncate_only CONTINUE IDENTITY RESTRICT;
SELECT count(*) FROM r0c_pgq_truncate_only;
