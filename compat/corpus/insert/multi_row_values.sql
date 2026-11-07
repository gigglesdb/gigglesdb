-- case: multi_row_values
-- title: Insert multiple rows in one values clause
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_multi_row_values_t (id integer PRIMARY KEY, name text);
INSERT INTO r0c_multi_row_values_t VALUES (3, 'Cia'), (1, 'Ada'), (2, 'Bea');
SELECT id, name FROM r0c_multi_row_values_t ORDER BY id;
