-- case: pgq_copy_from_quote
-- title: copy from quote
-- owner: E1
-- expect: compatible
-- source: Differences: COPY server files/programs; COPY FROM HEADER/QUOTE
CREATE TABLE r0c_pgq_copy_from_quote (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_copy_from_quote VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
-- @error
COPY r0c_pgq_copy_from_quote FROM '/r0c_pgq_copy_from_quote/missing.csv' WITH (FORMAT CSV, QUOTE '~');
