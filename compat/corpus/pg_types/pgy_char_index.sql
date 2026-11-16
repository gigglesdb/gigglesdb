-- case: pgy_char_index
-- title: char index
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: string functions on char(n); types.txt / Character types
CREATE TABLE r0c_pgy_char_index (id int PRIMARY KEY,v char(3) UNIQUE);
CREATE INDEX r0c_pgy_char_index_idx ON r0c_pgy_char_index(v);
INSERT INTO r0c_pgy_char_index VALUES (1,'ab');
-- @error
INSERT INTO r0c_pgy_char_index VALUES (2,'ab ');
SELECT v FROM r0c_pgy_char_index ORDER BY v,id;
