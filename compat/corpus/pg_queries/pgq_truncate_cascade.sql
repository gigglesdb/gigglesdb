-- case: pgq_truncate_cascade
-- title: truncate cascade
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 TRUNCATE forms
CREATE TABLE r0c_pgq_truncate_cascade (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_truncate_cascade VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
CREATE TABLE r0c_pgq_truncate_cascade_child (id int PRIMARY KEY, parent int REFERENCES r0c_pgq_truncate_cascade(id));
INSERT INTO r0c_pgq_truncate_cascade_child VALUES (1,1);
TRUNCATE r0c_pgq_truncate_cascade CASCADE;
SELECT count(*) FROM r0c_pgq_truncate_cascade_child;
