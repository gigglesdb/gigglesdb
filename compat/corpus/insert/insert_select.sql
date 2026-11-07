-- case: insert_select
-- title: Insert rows selected from another table
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_insert_select_src (id integer, score integer);
CREATE TABLE r0c_insert_select_dst (id integer, score integer);
INSERT INTO r0c_insert_select_src VALUES (2, 20);
INSERT INTO r0c_insert_select_src VALUES (1, 10);
INSERT INTO r0c_insert_select_src VALUES (3, 30);
INSERT INTO r0c_insert_select_dst SELECT id, score + 1 FROM r0c_insert_select_src WHERE id <= 2 ORDER BY id;
SELECT id, score FROM r0c_insert_select_dst ORDER BY id;
