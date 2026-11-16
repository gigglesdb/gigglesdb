-- case: pgq_truncate_multiple
-- title: truncate multiple
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 TRUNCATE forms
CREATE TABLE r0c_pgq_truncate_multiple (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_truncate_multiple VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
CREATE TABLE r0c_pgq_truncate_multiple_other (id int PRIMARY KEY);
INSERT INTO r0c_pgq_truncate_multiple_other VALUES (1);
TRUNCATE r0c_pgq_truncate_multiple,r0c_pgq_truncate_multiple_other;
SELECT (SELECT count(*) FROM r0c_pgq_truncate_multiple),(SELECT count(*) FROM r0c_pgq_truncate_multiple_other);
