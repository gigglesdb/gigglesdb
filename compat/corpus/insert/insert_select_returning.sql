-- case: insert_select_returning
-- title: Return one row inserted from a select
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_insert_select_returning_src (id integer PRIMARY KEY, name text);
CREATE TABLE r0c_insert_select_returning_dst (id integer, name text);
INSERT INTO r0c_insert_select_returning_src VALUES (1, 'Ada');
INSERT INTO r0c_insert_select_returning_src VALUES (2, 'Bea');
INSERT INTO r0c_insert_select_returning_dst SELECT id, name FROM r0c_insert_select_returning_src WHERE id = 1 RETURNING id, name;
SELECT id, name FROM r0c_insert_select_returning_dst ORDER BY id;
