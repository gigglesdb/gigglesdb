-- case: pgq_copy_from_file
-- title: copy from file
-- owner: E1
-- expect: compatible
-- source: Differences: COPY server files/programs; COPY FROM HEADER/QUOTE
CREATE TABLE r0c_pgq_copy_from_file (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_copy_from_file VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
-- @error
COPY r0c_pgq_copy_from_file FROM '/r0c_pgq_copy_from_file/missing.txt';
